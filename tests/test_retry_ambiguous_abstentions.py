"""WAVE 5, THE OTHER HALF OF THE RETRY DOOR (thread 6001): `retryable_abstentions` is
deliberately scoped to candidate_count=0 only — a 2+-candidate abstention needs a
DIFFERENT, safer re-scan: not a fresh re-derivation, but a check of whether the ORIGINAL
recorded candidates have since been eliminated (merged, retired, invalidated) down to
exactly one live survivor. Lane-agnostic, since it never re-derives anything.
"""
from __future__ import annotations

import uuid

from src.actions.core import Actions
from src.ontology.catalog import ensure_type
from src.orchestrator import capture


async def _mint_bare(actions: Actions, type_name: str) -> uuid.UUID:
    await ensure_type(actions, name=type_name, kind="object", actor="test")
    return await actions.create_or_find_object(
        type_name, f"{type_name.lower()}:{uuid.uuid4()}", "test")


async def test_retryable_ambiguous_finds_nothing_while_both_candidates_are_active(
    actions: Actions,
) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    t1 = await _mint_bare(actions, "GateWidget")
    t2 = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [t1, t2], "test")
    out = await capture.retryable_ambiguous_abstentions(actions.pool, "implements")
    assert out["count"] == 0
    assert out["sample"] == []


async def test_retryable_ambiguous_finds_a_row_once_one_candidate_is_eliminated(
    actions: Actions,
) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    t1 = await _mint_bare(actions, "GateWidget")
    t2 = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [t1, t2], "test")
    await actions.pool.execute("UPDATE objects SET status='retired' WHERE id=$1", t2)
    out = await capture.retryable_ambiguous_abstentions(actions.pool, "implements")
    assert out["count"] == 1
    row = out["sample"][0]
    assert row["from_id"] == str(orphan)[:8]
    assert row["original_candidate_count"] == 2
    assert row["surviving_candidate"]["id"] == str(t1)[:8]


async def test_retryable_ambiguous_never_returns_a_row_with_two_live_survivors(
    actions: Actions,
) -> None:
    """Structural guarantee: even pooling every link_type (link_type=None), a case with
    2+ STILL-ACTIVE candidates never appears — the SQL's own WHERE clause, not
    application logic a later edit could widen."""
    orphan = await _mint_bare(actions, "GateWidget")
    t1 = await _mint_bare(actions, "GateWidget")
    t2 = await _mint_bare(actions, "GateWidget")
    t3 = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "confirms", [t1, t2, t3], "test")
    await actions.pool.execute("UPDATE objects SET status='retired' WHERE id=$1", t3)
    out = await capture.retryable_ambiguous_abstentions(actions.pool, None)
    assert str(orphan)[:8] not in {r["from_id"] for r in out["sample"]}


async def test_retryable_ambiguous_names_eliminated_to_zero_separately_never_folded_in(
    actions: Actions,
) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    t1 = await _mint_bare(actions, "GateWidget")
    t2 = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [t1, t2], "test")
    await actions.pool.execute(
        "UPDATE objects SET status='retired' WHERE id = ANY($1)", [t1, t2])
    out = await capture.retryable_ambiguous_abstentions(actions.pool, "implements")
    assert out["count"] == 0
    assert out["eliminated_to_zero"] == 1
    assert out["sample"] == []


async def test_retry_ambiguous_dry_run_writes_nothing(actions: Actions) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    t1 = await _mint_bare(actions, "GateWidget")
    t2 = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [t1, t2], "test")
    await actions.pool.execute("UPDATE objects SET status='retired' WHERE id=$1", t2)
    out = await capture.retry_ambiguous_abstentions(
        actions, actor="retrier", dry_run=True, link_type="implements")
    assert out["to_mint"] == 1
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='implements'", orphan)
    assert n == 0


async def test_retry_ambiguous_mints_the_surviving_candidate(actions: Actions) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    t1 = await _mint_bare(actions, "GateWidget")
    t2 = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [t1, t2], "original")
    await actions.pool.execute("UPDATE objects SET status='retired' WHERE id=$1", t2)
    out = await capture.retry_ambiguous_abstentions(
        actions, actor="retrier", dry_run=False, because="test authorization",
        link_type="implements")
    assert out["to_mint"] == 1
    linked = await actions.pool.fetchval(
        "SELECT to_id FROM links WHERE from_id=$1 AND type='implements'", orphan)
    assert linked == t1
    row = await actions.pool.fetchrow(
        "SELECT properties, source_id FROM links WHERE from_id=$1 AND type='implements'",
        orphan)
    assert row["properties"]["retried"] is True
    assert row["source_id"] == "retrier"
    # the stale abstention (recorded under "original") must be RETIRED, not left
    # coexisting beside the resolved marker (Wave 5's own cross-source finding)
    resolved = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='derivation_abstained_implements'", orphan)
    assert resolved["resolved"] is True
    assert resolved["resolved_to"] == str(t1)


async def test_retry_ambiguous_is_idempotent(actions: Actions) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    t1 = await _mint_bare(actions, "GateWidget")
    t2 = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [t1, t2], "test")
    await actions.pool.execute("UPDATE objects SET status='retired' WHERE id=$1", t2)
    first = await capture.retry_ambiguous_abstentions(
        actions, actor="retrier", dry_run=False, because="test authorization",
        link_type="implements")
    second = await capture.retry_ambiguous_abstentions(
        actions, actor="retrier", dry_run=False, because="test authorization",
        link_type="implements")
    assert first["to_mint"] == 1
    assert second["scanned"] == 0  # already resolved, no longer in the retryable surface
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='implements'", orphan)
    assert n == 1


async def test_retry_ambiguous_requires_a_because_to_execute(actions: Actions) -> None:
    orphan = await _mint_bare(actions, "GateWidget")
    t1 = await _mint_bare(actions, "GateWidget")
    t2 = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, orphan, "implements", [t1, t2], "test")
    await actions.pool.execute("UPDATE objects SET status='retired' WHERE id=$1", t2)
    out = await capture.retry_ambiguous_abstentions(
        actions, actor="retrier", dry_run=False, link_type="implements")
    assert "error" in out
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='implements'", orphan)
    assert n == 0


async def test_retry_ambiguous_is_lane_agnostic_across_link_types_at_once(
    actions: Actions,
) -> None:
    """No lane-specific lookup is ever re-run — only the stored candidate ids' status —
    so one call with link_type=None serves every lane's ambiguous abstentions together."""
    await ensure_type(actions, name="gate_rel_b", kind="link", actor="test")
    o1 = await _mint_bare(actions, "GateWidget")
    o2 = await _mint_bare(actions, "GateWidget")
    t1 = await _mint_bare(actions, "GateWidget")
    t2 = await _mint_bare(actions, "GateWidget")
    t3 = await _mint_bare(actions, "GateWidget")
    t4 = await _mint_bare(actions, "GateWidget")
    await capture.derive_or_abstain(actions, o1, "implements", [t1, t2], "test")
    await capture.derive_or_abstain(actions, o2, "gate_rel_b", [t3, t4], "test")
    await actions.pool.execute("UPDATE objects SET status='retired' WHERE id=$1", t2)
    await actions.pool.execute("UPDATE objects SET status='retired' WHERE id=$1", t4)
    out = await capture.retry_ambiguous_abstentions(
        actions, actor="retrier", dry_run=False, because="test authorization")
    assert out["to_mint"] == 2
    linked1 = await actions.pool.fetchval(
        "SELECT to_id FROM links WHERE from_id=$1 AND type='implements'", o1)
    linked2 = await actions.pool.fetchval(
        "SELECT to_id FROM links WHERE from_id=$1 AND type='gate_rel_b'", o2)
    assert linked1 == t1
    assert linked2 == t3
