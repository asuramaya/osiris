"""retire_assertion — the cross-source supersede (thread 52911d2a, found diagnosing
b9aa7326, decision d28d1459): assert_property's own supersession is scoped to the SAME
source only, so a peer's correction of another agent's bad self-declaration can never
retire it through the ordinary path — both rows stay simultaneously "current". This is the
guarded orchestrator layer (friendly error dicts, never a bare exception) around
Actions.supersede_assertion.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.retirement import (
    list_assertions,
    repair_stale_current_flags,
    retire_assertion,
    retire_link,
    stale_current_flags,
)

NOW = datetime(2026, 7, 27, tzinfo=UTC)


async def test_list_assertions_requires_name(actions: Actions) -> None:
    await actions.create_or_find_object("Agent", "agent:x", "test")
    out = await list_assertions(actions, ref="agent:x", name="  ")
    assert "error" in out


async def test_list_assertions_unknown_ref_is_an_honest_error(actions: Actions) -> None:
    out = await list_assertions(actions, ref="agent:does-not-exist", name="house")
    assert "error" in out


async def test_list_assertions_empty_when_the_property_was_never_asserted(
    actions: Actions,
) -> None:
    await actions.create_or_find_object("Agent", "agent:x", "test")
    out = await list_assertions(actions, ref="agent:x", name="never-asserted")
    assert out["assertions"] == []


async def test_list_assertions_exposes_the_id_retire_assertion_needs(actions: Actions) -> None:
    """The exact 382067d9 gap: two CURRENT contradicting values, and retire_assertion's
    superseded_id can only come from somewhere that names the row, not just the value."""
    obj = await actions.create_or_find_object("Agent", "agent:ad1a1cb0-g40-xxiv", "test")
    wrong = await actions.assert_property(
        obj, "seat_generation", "2", "agent:ad1a1cb0-g40-xxiv", NOW, 0.9,
        evidence_class="self_declared")
    right = await actions.assert_property(
        obj, "seat_generation", "58", "agent:aad6603a-iv", NOW, 0.9,
        evidence_class="self_declared")

    out = await list_assertions(actions, ref="agent:ad1a1cb0-g40-xxiv", name="seat_generation")
    ids = {a["id"]: a["value"] for a in out["assertions"]}
    assert ids == {wrong: "2", right: "58"}

    # and it is exactly what retire_assertion accepts, end to end
    retired = await retire_assertion(
        actions, ref="agent:ad1a1cb0-g40-xxiv", name="seat_generation",
        superseded_id=wrong, value="58", because="closing the loop",
        actor="agent:c38f8f3b-vi")
    assert retired["retired"]["id"] == wrong

    # retire_assertion always ASSERTS its own new row rather than deleting anything
    # (append-only) — the wrong VALUE is gone, but a second "58" (the corrector's own
    # stamp) now sits beside the original one; both current, neither wrong.
    after = await list_assertions(actions, ref="agent:ad1a1cb0-g40-xxiv", name="seat_generation")
    assert {a["value"] for a in after["assertions"]} == {"58"}
    assert wrong not in {a["id"] for a in after["assertions"]}


async def test_requires_because(actions: Actions) -> None:
    obj = await actions.create_or_find_object("Agent", "agent:x", "test")
    row_id = await actions.assert_property(obj, "seat_generation", "2", "agent:x", NOW, 0.9)
    out = await retire_assertion(actions, ref="agent:x", name="seat_generation",
                                 superseded_id=row_id, value="58", because="   ",
                                 actor="agent:corrector")
    assert "error" in out
    assert "because" in out["error"]


async def test_unknown_ref_is_an_honest_error(actions: Actions) -> None:
    out = await retire_assertion(actions, ref="agent:does-not-exist", name="seat_generation",
                                 superseded_id=1, value="58", because="fix",
                                 actor="agent:corrector")
    assert "error" in out


async def test_wrong_name_is_refused(actions: Actions) -> None:
    obj = await actions.create_or_find_object("Agent", "agent:x", "test")
    row_id = await actions.assert_property(obj, "seat_generation", "2", "agent:x", NOW, 0.9)
    out = await retire_assertion(actions, ref="agent:x", name="not-a-real-name",
                                 superseded_id=row_id, value="58", because="fix",
                                 actor="agent:corrector")
    assert "error" in out


async def test_unknown_assertion_id_is_refused(actions: Actions) -> None:
    await actions.create_or_find_object("Agent", "agent:x", "test")
    out = await retire_assertion(actions, ref="agent:x", name="seat_generation",
                                 superseded_id=999999999, value="58", because="fix",
                                 actor="agent:corrector")
    assert "error" in out


async def test_already_superseded_is_refused_not_double_retired(actions: Actions) -> None:
    obj = await actions.create_or_find_object("Agent", "agent:x", "test")
    row_id = await actions.assert_property(obj, "seat_generation", "2", "agent:x", NOW, 0.9)
    first = await retire_assertion(actions, ref="agent:x", name="seat_generation",
                                   superseded_id=row_id, value="58", because="fix",
                                   actor="agent:corrector")
    assert "error" not in first
    second = await retire_assertion(actions, ref="agent:x", name="seat_generation",
                                    superseded_id=row_id, value="99", because="again",
                                    actor="agent:corrector-2")
    assert "error" in second
    assert "already superseded" in second["error"]


async def test_the_live_case(actions: Actions) -> None:
    """The exact acceptance shape (decision d28d1459): a self-declared low value vs a peer's
    correct one, neither superseding the other until retire_assertion runs."""
    obj = await actions.create_or_find_object("Agent", "agent:ad1a1cb0-g40-xxiv", "test")
    wrong = await actions.assert_property(
        obj, "seat_generation", "2", "agent:ad1a1cb0-g40-xxiv", NOW, 0.9,
        evidence_class="self_declared")
    await actions.assert_property(
        obj, "seat_generation", "58", "agent:aad6603a-iv", NOW, 0.9,
        evidence_class="self_declared")
    vals = await actions.current_values(obj, "seat_generation")
    assert {v["value"] for v in vals} == {"2", "58"}  # both current — the live bug shape

    out = await retire_assertion(
        actions, ref="agent:ad1a1cb0-g40-xxiv", name="seat_generation", superseded_id=wrong,
        value="58", because="operator-approved repair, diagnosis d28d1459",
        actor="agent:c38f8f3b-vi")
    assert out["retired"]["value"] == "2"
    assert out["retired"]["id"] == wrong
    assert out["now_current"]["value"] == "58"
    assert out["because"] == "operator-approved repair, diagnosis d28d1459"

    vals = await actions.current_values(obj, "seat_generation")
    assert {v["value"] for v in vals} == {"58"}  # only the correct value survives


async def test_the_mcp_tool_wraps_the_orchestrator(actions: Actions) -> None:
    """srv._pool swap (mirrors test_succession.py's own pattern) — proves the ACTUAL MCP
    tool delegates correctly. No mount context here, so it refuses with the mount-first
    message rather than a bare crash — the identity gate is exercised, not bypassed."""
    from src import mcp_server as srv

    obj = await actions.create_or_find_object("Agent", "agent:mcp-x", "test")
    row_id = await actions.assert_property(obj, "seat_generation", "2", "agent:mcp-x", NOW, 0.9)

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.retire_assertion(
            ref="agent:mcp-x", name="seat_generation", superseded_id=row_id, value="9",
            because="test")
    finally:
        srv._pool = saved_pool
    assert "error" in out


# ═══ stale_current_flags — the read door (thread 09bde57e, khepri's own live specimen:
# a real supersedes FK exists but is_current was never flipped for the row it excludes) ═══

async def test_stale_current_flags_is_empty_on_a_clean_supersession(actions: Actions) -> None:
    """The ordinary, correctly-flipped shape (via assert_property's own same-source
    supersession, or retire_assertion) must NEVER show up here."""
    obj = await actions.create_or_find_object("Agent", "agent:clean01", "test")
    await actions.assert_property(obj, "house", "old", "agent:clean01", NOW, 0.9)
    await actions.assert_property(obj, "house", "new", "agent:clean01", NOW, 0.9)
    out = await stale_current_flags(actions)
    ids = {s["stale_id"] for s in out["sample"]}
    old_id = await actions.pool.fetchval(
        "SELECT id FROM assertions WHERE object_id=$1 AND value #>> '{}' = 'old'", obj)
    assert old_id not in ids


async def test_stale_current_flags_surfaces_a_row_whose_flip_never_landed(
    actions: Actions,
) -> None:
    """Reproduces khepri's own live specimen directly: a real `supersedes` FK exists (a raw
    INSERT bypassing the code path that would have flipped `is_current` in the same
    transaction — exactly the historical-write shape suspected for pre-fix rows) while the
    superseded row's own `is_current` stays true. `count` names the TRUE total; `sample`
    carries this exact row with both sides' provenance."""
    obj = await actions.create_or_find_object("Agent", "agent:stale01", "test")
    stale_id = await actions.assert_property(obj, "house", "cultural-infrastructure",
                                             "agent:mistake", NOW, 0.9)
    # bypass Actions entirely — a raw INSERT with a real supersedes FK but no is_current flip,
    # the exact shape a pre-0047-fix write (or any write outside the two flipping call
    # sites) leaves behind
    await actions.pool.execute(
        "INSERT INTO assertions (object_id, name, value, source_id, observed_at, confidence, "
        "supersedes, evidence_class) VALUES ($1,'house','\"tony\"','agent:revert', $2, 0.9, "
        "$3, 'self_declared')", obj, NOW, stale_id)

    out = await stale_current_flags(actions)
    hit = next((s for s in out["sample"] if s["stale_id"] == stale_id), None)
    assert hit is not None, out
    assert hit["value"] == "cultural-infrastructure"
    assert hit["superseding_source"] == "agent:revert"
    assert out["count"] >= 1


async def test_stale_current_flags_count_is_never_capped_by_the_sample_limit(
    actions: Actions,
) -> None:
    """`count` must report the TRUE population size even when `sample` is capped small —
    a caller measuring the live population must never be told a bounded sample's own size
    is the whole truth."""
    obj = await actions.create_or_find_object("Agent", "agent:manystale", "test")
    for i in range(3):
        stale_id = await actions.assert_property(
            obj, f"prop{i}", "old", f"agent:src{i}", NOW, 0.9)
        await actions.pool.execute(
            "INSERT INTO assertions (object_id, name, value, source_id, observed_at, "
            "confidence, supersedes, evidence_class) VALUES "
            f"($1,'prop{i}','\"new\"','agent:revert', $2, 0.9, $3, 'self_declared')",
            obj, NOW, stale_id)

    out = await stale_current_flags(actions, limit=1)
    assert len(out["sample"]) == 1
    assert out["count"] >= 3


async def test_the_stale_current_flags_mcp_tool_wraps_the_orchestrator(
    actions: Actions,
) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.stale_current_flags(limit=5)
    finally:
        srv._pool = saved_pool
    assert "count" in out and "sample" in out


# ═══ repair_stale_current_flags — thread 09bde57e piece (d), the backfill for the
# population stale_current_flags measures (123,914 of 267,305 rows, d8225e71) ═══


async def test_repair_dry_run_lists_without_writing(actions: Actions) -> None:
    obj = await actions.create_or_find_object("Agent", "agent:dryrun", "test")
    stale_id = await actions.assert_property(obj, "house", "old", "agent:mistake", NOW, 0.9)
    await actions.pool.execute(
        "INSERT INTO assertions (object_id, name, value, source_id, observed_at, confidence, "
        "supersedes, evidence_class) VALUES ($1,'house','\"new\"','agent:revert', $2, 0.9, "
        "$3, 'self_declared')", obj, NOW, stale_id)

    out = await repair_stale_current_flags(actions, dry_run=True)
    assert out["dry_run"] is True
    assert stale_id in out["sample_ids"]
    assert out["would_repair"] >= 1
    # nothing written — the row is still flagged current
    assert await actions.pool.fetchval(
        "SELECT is_current FROM assertions WHERE id=$1", stale_id) is True
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM audit_log WHERE action='repair_stale_current_flags'") == 0


async def test_repair_dry_run_is_the_default(actions: Actions) -> None:
    obj = await actions.create_or_find_object("Agent", "agent:defaultdry", "test")
    stale_id = await actions.assert_property(obj, "house", "old", "agent:mistake", NOW, 0.9)
    await actions.pool.execute(
        "INSERT INTO assertions (object_id, name, value, source_id, observed_at, confidence, "
        "supersedes, evidence_class) VALUES ($1,'house','\"new\"','agent:revert', $2, 0.9, "
        "$3, 'self_declared')", obj, NOW, stale_id)

    out = await repair_stale_current_flags(actions)  # no dry_run kwarg at all
    assert out["dry_run"] is True
    assert await actions.pool.fetchval(
        "SELECT is_current FROM assertions WHERE id=$1", stale_id) is True


async def test_repair_execute_flips_the_flag_and_records_who(actions: Actions) -> None:
    obj = await actions.create_or_find_object("Agent", "agent:execute", "test")
    stale_id = await actions.assert_property(obj, "house", "old", "agent:mistake", NOW, 0.9)
    await actions.pool.execute(
        "INSERT INTO assertions (object_id, name, value, source_id, observed_at, confidence, "
        "supersedes, evidence_class) VALUES ($1,'house','\"new\"','agent:revert', $2, 0.9, "
        "$3, 'self_declared')", obj, NOW, stale_id)

    out = await repair_stale_current_flags(actions, dry_run=False, actor="agent:operator")
    assert out["dry_run"] is False
    assert stale_id in out["repaired_ids"]
    assert out["total_stale_remaining"] == out["total_stale_before"] - out["repaired"]
    assert await actions.pool.fetchval(
        "SELECT is_current FROM assertions WHERE id=$1", stale_id) is False
    assert await actions.pool.fetchval(
        "SELECT actor FROM audit_log WHERE action='repair_stale_current_flags' "
        "ORDER BY id DESC LIMIT 1") == "agent:operator"


async def test_repair_execute_is_idempotent(actions: Actions) -> None:
    obj = await actions.create_or_find_object("Agent", "agent:idempotent", "test")
    stale_id = await actions.assert_property(obj, "house", "old", "agent:mistake", NOW, 0.9)
    await actions.pool.execute(
        "INSERT INTO assertions (object_id, name, value, source_id, observed_at, confidence, "
        "supersedes, evidence_class) VALUES ($1,'house','\"new\"','agent:revert', $2, 0.9, "
        "$3, 'self_declared')", obj, NOW, stale_id)

    first = await repair_stale_current_flags(actions, dry_run=False, actor="agent:operator")
    assert stale_id in first["repaired_ids"]
    second = await repair_stale_current_flags(actions, dry_run=False, actor="agent:operator")
    assert stale_id not in second["repaired_ids"]
    assert second["repaired"] == 0


async def test_the_repair_mcp_tool_refuses_an_unmounted_execute(actions: Actions) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.repair_stale_current_flags(dry_run=False)
    finally:
        srv._pool = saved_pool
    assert "error" in out


async def test_the_repair_mcp_tool_allows_an_unmounted_dry_run(actions: Actions) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.repair_stale_current_flags(dry_run=True)
    finally:
        srv._pool = saved_pool
    assert "error" not in out
    assert out["dry_run"] is True


# --- retire_link (thread badb4040) --------------------------------------------------


async def test_retire_link_requires_because(actions: Actions) -> None:
    a = await actions.create_or_find_object("Agent", "agent:linka", "test")
    b = await actions.create_or_find_object("Agent", "agent:linkb", "test")
    await actions.create_link(a, b, "works_in", "test", NOW, 0.9, evidence_class="self_declared")
    out = await retire_link(actions, from_ref="agent:linka", to_ref="agent:linkb",
                            link_type="works_in", because="  ", actor="test")
    assert "error" in out


async def test_retire_link_requires_link_type(actions: Actions) -> None:
    await actions.create_or_find_object("Agent", "agent:linkc", "test")
    await actions.create_or_find_object("Agent", "agent:linkd", "test")
    out = await retire_link(actions, from_ref="agent:linkc", to_ref="agent:linkd",
                            link_type="  ", because="oops", actor="test")
    assert "error" in out


async def test_retire_link_unresolved_refs_are_honest_errors(actions: Actions) -> None:
    await actions.create_or_find_object("Agent", "agent:linke", "test")
    out = await retire_link(actions, from_ref="agent:linke", to_ref="agent:does-not-exist",
                            link_type="works_in", because="oops", actor="test")
    assert "error" in out
    out2 = await retire_link(actions, from_ref="agent:does-not-exist", to_ref="agent:linke",
                             link_type="works_in", because="oops", actor="test")
    assert "error" in out2


async def test_retire_link_no_active_link_refuses_rather_than_silent_success(
    actions: Actions,
) -> None:
    a = await actions.create_or_find_object("Agent", "agent:linkf", "test")
    b = await actions.create_or_find_object("Agent", "agent:linkg", "test")
    # a real link exists, but of a DIFFERENT type than the one being retired
    await actions.create_link(a, b, "works_in", "test", NOW, 0.9, evidence_class="self_declared")
    out = await retire_link(actions, from_ref="agent:linkf", to_ref="agent:linkg",
                            link_type="governs", because="wrong type entirely", actor="test")
    assert "error" in out
    assert "nothing to retire" in out["error"]


async def test_retire_link_deactivates_never_deletes(actions: Actions) -> None:
    """THE LIVE INCIDENT this thread was opened for: a fuzzy-substring resolves= mis-
    citation minted a spurious `answers` link — this closes it cleanly, event-sourced,
    the row still there just no longer current."""
    thread_obj = await actions.create_or_find_object("Thread", "thread:spurious1", "test")
    decision_obj = await actions.create_or_find_object("Decision", "decision:spurious1", "test")
    await actions.create_link(thread_obj, decision_obj, "answers", "test", NOW, 0.9,
                              evidence_class="self_declared")

    out = await retire_link(actions, from_ref="thread:spurious1", to_ref="decision:spurious1",
                            link_type="answers", because="fuzzy-substring mis-citation, "
                            "unrelated thread", actor="agent:fixer")

    assert out["retired"]["count"] == 1
    assert out["because"] == "fuzzy-substring mis-citation, unrelated thread"
    live = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND to_id=$2 AND type='answers' "
        "AND valid_until IS NULL", thread_obj, decision_obj)
    assert live == 0
    # NEVER A DELETE: the row is still there, just no longer current
    total = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND to_id=$2 AND type='answers'",
        thread_obj, decision_obj)
    assert total == 1
    # THE COMPENSATING EVENT carries the reason
    audit_reason = await actions.pool.fetchval(
        "SELECT payload->>'reason' FROM audit_log WHERE action='invalidate_link' "
        "ORDER BY id DESC LIMIT 1")
    assert audit_reason == "fuzzy-substring mis-citation, unrelated thread"
    outbox_reason = await actions.pool.fetchval(
        "SELECT payload->>'reason' FROM outbox WHERE event_type='link_invalidated' "
        "ORDER BY id DESC LIMIT 1")
    assert outbox_reason == "fuzzy-substring mis-citation, unrelated thread"


async def test_retire_link_is_idempotent_a_second_call_finds_nothing_active(
    actions: Actions,
) -> None:
    a = await actions.create_or_find_object("Agent", "agent:linkh", "test")
    b = await actions.create_or_find_object("Agent", "agent:linki", "test")
    await actions.create_link(a, b, "works_in", "test", NOW, 0.9, evidence_class="self_declared")

    first = await retire_link(actions, from_ref="agent:linkh", to_ref="agent:linki",
                              link_type="works_in", because="test cleanup", actor="test")
    assert first["retired"]["count"] == 1
    second = await retire_link(actions, from_ref="agent:linkh", to_ref="agent:linki",
                               link_type="works_in", because="already gone", actor="test")
    assert "error" in second  # nothing currently active left to retire


async def test_retire_link_mcp_tool_refuses_when_unmounted(actions: Actions) -> None:
    from src import mcp_server as srv

    a = await actions.create_or_find_object("Agent", "agent:linkj", "test")
    b = await actions.create_or_find_object("Agent", "agent:linkk", "test")
    await actions.create_link(a, b, "works_in", "test", NOW, 0.9, evidence_class="self_declared")

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.retire_link(from_ref="agent:linkj", to_ref="agent:linkk",
                                    link_type="works_in", because="test")
    finally:
        srv._pool = saved_pool
    assert "error" in out
    assert "mount first" in out["error"]
