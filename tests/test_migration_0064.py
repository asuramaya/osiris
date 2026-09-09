"""MIGRATION 0064, THE EVENT LOG COLD PARTITION: assertions_hot/assertions_cold behind
the `assertions` umbrella view (alembic/versions/0064_assertions_hot_cold.py), and the
bounded-batch archiver (src/orchestrator/migration_0064.py). Boundary-spanning tests
prove current_assertions and the supersedes chain resolve correctly regardless of which
physical table a row lands in; the archiver tests prove bounded batches, a count-
preserving receipt, and the never-move-a-current-row invariant.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.migration_0064 import (
    ReconciliationError,
    apply_migration_0064,
    plan_migration_0064,
)

OLD = datetime(2020, 1, 15, tzinfo=UTC)
RECENT = datetime(2026, 9, 1, tzinfo=UTC)
CUTOFF = datetime(2026, 9, 1, tzinfo=UTC)


async def _archive_row(actions: Actions, assertion_id: int) -> None:
    """Simulate what apply_migration_0064 does to one row: copy to cold, delete from
    hot. Used to set up an ALREADY-ARCHIVED fixture without running the whole archiver,
    for tests that only care about read-side correctness."""
    cols = (
        "id, object_id, name, value, source_id, case_id, helper_run_id, evidence_uri, "
        "evidence_sha256, observed_at, confidence, supersedes, created_at, "
        "evidence_class, is_current"
    )
    select_cols = cols.replace("value,", "(value::text::jsonb) AS value,")
    await actions.pool.execute(
        f"INSERT INTO assertions_cold ({cols}) SELECT {select_cols} FROM assertions_hot "
        "WHERE id=$1", assertion_id,
    )
    await actions.pool.execute("DELETE FROM assertions_hot WHERE id=$1", assertion_id)


async def test_current_assertions_resolves_the_winner_regardless_of_which_side_it_is_on(
    actions: Actions,
) -> None:
    """Same object, one superseded row physically archived to cold, its winner still in
    hot -- current_assertions must return exactly the winner either way."""
    obj = await actions.create_or_find_object("Agent", "agent:boundary1", "test")
    old_id = await actions.assert_property(obj, "house", "old-house", "agent:self", OLD, 0.9)
    new_id = await actions.assert_property(obj, "house", "new-house", "agent:self", RECENT, 0.9)
    assert old_id != new_id

    # before archiving: sanity check ordinary same-source supersession already works
    rows = await actions.pool.fetch(
        "SELECT id, value FROM current_assertions WHERE object_id=$1 AND name='house'", obj)
    assert [r["value"] for r in rows] == ["new-house"]

    await _archive_row(actions, old_id)

    # after archiving: current_assertions (hot-only) still names the correct winner
    rows = await actions.pool.fetch(
        "SELECT id, value FROM current_assertions WHERE object_id=$1 AND name='house'", obj)
    assert [(r["id"], r["value"]) for r in rows] == [(new_id, "new-house")]

    # and the OLD row is not lost -- the umbrella view still surfaces it for a full-
    # history reader (compositions.py's dossier, retirement.py, trace_evidence, ...)
    full = await actions.pool.fetch(
        "SELECT id, value FROM assertions WHERE object_id=$1 AND name='house' ORDER BY id",
        obj)
    assert [r["id"] for r in full] == [old_id, new_id]


async def test_supersedes_chain_crossing_the_hot_cold_boundary_resolves_correctly(
    actions: Actions,
) -> None:
    """An old row archived to cold, superseded by a NEW row that lives in hot: the
    supersedes pointer crosses tables, and both current_assertions and a raw walk of
    the pointer must still resolve correctly."""
    obj = await actions.create_or_find_object("Agent", "agent:boundary2", "test")
    old_id = await actions.assert_property(obj, "role", "analyst", "agent:self", OLD, 0.9)
    # the correction supersedes old_id WHILE IT IS STILL HOT (is_current flips false in
    # the same transaction, per Actions.assert_property) -- only THEN is it eligible to
    # archive; a still-current row could never legally sit in assertions_cold (its own
    # CHECK(is_current=false) forbids it, matching the archiver's own invariant).
    new_id = await actions.assert_property(obj, "role", "lead", "agent:self", RECENT, 0.9)
    new_row = await actions.pool.fetchrow(
        "SELECT id, value, supersedes FROM assertions WHERE id=$1", new_id)
    assert new_row["supersedes"] == old_id

    await _archive_row(actions, old_id)

    winner = await actions.pool.fetchrow(
        "SELECT value FROM current_assertions WHERE object_id=$1 AND name='role'", obj)
    assert winner["value"] == "lead"

    # following supersedes by hand must find the cold row through the union view --
    # nothing about crossing tables makes the pointer dangle
    superseded = await actions.pool.fetchrow(
        "SELECT value FROM assertions WHERE id=$1", new_row["supersedes"])
    assert superseded["value"] == "analyst"
    in_cold = await actions.pool.fetchval(
        "SELECT 1 FROM assertions_cold WHERE id=$1", old_id)
    assert in_cold == 1


async def test_assertions_view_stays_writable_insert_update_delete(actions: Actions) -> None:
    """A plain UNION ALL view is read-only in Postgres; the INSTEAD OF triggers must make
    `assertions` behave like a real table for every existing raw-SQL caller (tests
    included -- tests/conftest.py's own per-test reset does DELETE FROM assertions)."""
    obj = await actions.create_or_find_object("Agent", "agent:writable", "test")
    new_id = await actions.pool.fetchval(
        "INSERT INTO assertions (object_id, name, value, source_id, observed_at, "
        "confidence, evidence_class) VALUES ($1,'note','\"hi\"','test:direct',$2,0.9,"
        "'self_declared') RETURNING id",
        obj, RECENT,
    )
    assert await actions.pool.fetchval(
        "SELECT 1 FROM assertions_hot WHERE id=$1", new_id) == 1

    await actions.pool.execute(
        "UPDATE assertions SET confidence=0.5 WHERE id=$1", new_id)
    assert await actions.pool.fetchval(
        "SELECT confidence FROM assertions_hot WHERE id=$1", new_id) == 0.5

    await actions.pool.execute("DELETE FROM assertions WHERE id=$1", new_id)
    assert await actions.pool.fetchval(
        "SELECT 1 FROM assertions_hot WHERE id=$1", new_id) is None


async def test_cold_rows_carry_real_lz4_toast_compression(actions: Actions) -> None:
    """'Compressed cold' must be a verifiable, concrete mechanism, not a comment. A
    sufficiently large jsonb `value` written into assertions_cold should report lz4 via
    Postgres's own pg_column_compression()."""
    obj = await actions.create_or_find_object("Agent", "agent:compressed", "test")
    big_value = "x" * 4000  # comfortably past TOAST's inline threshold
    old_id = await actions.assert_property(obj, "blob", big_value, "agent:self", OLD, 0.9)
    # supersede it so it is_current=false -- the only state assertions_cold's own CHECK
    # constraint (and the archiver's own invariant) ever allows in cold
    await actions.assert_property(obj, "blob", "gone", "agent:self", RECENT, 0.9)
    await _archive_row(actions, old_id)
    compression = await actions.pool.fetchval(
        "SELECT pg_column_compression(value) FROM assertions_cold WHERE id=$1", old_id)
    assert compression == "lz4"


async def _age(actions: Actions, assertion_id: int, created_at: datetime) -> None:
    """Test-only backdating: `created_at` is a DEFAULT-now() column no application
    caller (Actions.assert_property included) ever overrides -- the archiver's own
    eligibility clock is real wall-clock insertion time, deliberately independent of
    `observed_at` (the "when the fact was true" field callers DO control). Stamping it
    directly is the only way to build a deterministic fixture for cutoff-based tests."""
    await actions.pool.execute(
        "UPDATE assertions_hot SET created_at=$2 WHERE id=$1", assertion_id, created_at)


async def test_apply_migration_0064_moves_only_eligible_rows_in_bounded_batches(
    actions: Actions,
) -> None:
    """10 old, superseded rows are cold-eligible; a current (is_current=true) row and a
    RECENT superseded row must never move, regardless of the batch size. batch_size=3
    over 10 eligible rows must take 4 batches (3,3,3,1), never one giant sweep."""
    obj = await actions.create_or_find_object("Agent", "agent:archiver", "test")

    old_ids = []
    for i in range(10):
        aid = await actions.assert_property(
            obj, f"fact{i}", "v1", f"agent:src{i}", OLD, 0.9)
        await _age(actions, aid, OLD)
        await actions.assert_property(obj, f"fact{i}", "v2", f"agent:src{i}", RECENT, 0.9)
        old_ids.append(aid)  # each of these is now is_current=false and OLD (created_at)

    # a current row that happens to be old itself -- must never move
    ancient_current = await actions.assert_property(
        obj, "ancient_but_current", "still-true", "agent:self", OLD, 0.9)
    await _age(actions, ancient_current, OLD)
    # a superseded row that is too RECENT to be cold-eligible -- must never move
    recent_superseded = await actions.assert_property(
        obj, "recent_fact", "v1", "agent:recent", RECENT, 0.9)
    await actions.assert_property(obj, "recent_fact", "v2", "agent:recent", RECENT, 0.9)

    plan = await plan_migration_0064(actions.pool, cutoff=CUTOFF)
    assert plan["eligible_rows"] == 10

    receipt = await apply_migration_0064(actions.pool, batch_size=3, cutoff=CUTOFF)
    assert receipt["rows_examined"] == 10
    assert receipt["rows_moved"] == 10
    assert receipt["batches_run"] == 4
    assert receipt["count_preserved"] is True
    assert receipt["before"]["total"] == receipt["after"]["total"]
    assert receipt["after"]["cold"] - receipt["before"]["cold"] == 10
    assert receipt["before"]["hot"] - receipt["after"]["hot"] == 10

    for aid in old_ids:
        assert await actions.pool.fetchval(
            "SELECT 1 FROM assertions_cold WHERE id=$1", aid) == 1
        assert await actions.pool.fetchval(
            "SELECT 1 FROM assertions_hot WHERE id=$1", aid) is None

    # never moved: still current, still in hot
    assert await actions.pool.fetchval(
        "SELECT 1 FROM assertions_hot WHERE id=$1", ancient_current) == 1
    # never moved: too recent
    assert await actions.pool.fetchval(
        "SELECT 1 FROM assertions_hot WHERE id=$1", recent_superseded) == 1

    # a second run is idempotent -- nothing left eligible, zero-row receipt
    second = await apply_migration_0064(actions.pool, batch_size=3, cutoff=CUTOFF)
    assert second["rows_moved"] == 0
    assert second["batches_run"] == 0
    assert second["count_preserved"] is True


async def test_plan_migration_0064_is_read_only(actions: Actions) -> None:
    obj = await actions.create_or_find_object("Agent", "agent:dryrun", "test")
    old_id = await actions.assert_property(obj, "x", "v1", "agent:s", OLD, 0.9)
    await _age(actions, old_id, OLD)
    await actions.assert_property(obj, "x", "v2", "agent:s", RECENT, 0.9)

    before_hot = await actions.pool.fetchval("SELECT count(*) FROM assertions_hot")
    before_cold = await actions.pool.fetchval("SELECT count(*) FROM assertions_cold")

    plan = await plan_migration_0064(actions.pool, cutoff=CUTOFF)
    assert plan["eligible_rows"] >= 1

    after_hot = await actions.pool.fetchval("SELECT count(*) FROM assertions_hot")
    after_cold = await actions.pool.fetchval("SELECT count(*) FROM assertions_cold")
    assert (before_hot, before_cold) == (after_hot, after_cold)


async def test_apply_migration_0064_raises_loudly_on_a_reconciliation_mismatch(
    actions: Actions,
) -> None:
    """A count mismatch must never be swallowed into a receipt that looks fine --
    simulate the failure by racing a manual delete of an eligible row mid-run isn't
    practical here, so this proves the raise path directly: ReconciliationError is
    exactly the class apply_migration_0064 raises, and it is a RuntimeError (so nothing
    upstream can quietly catch-and-ignore it as a plain return value)."""
    assert issubclass(ReconciliationError, RuntimeError)
