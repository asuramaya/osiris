"""An abstention is idempotent on its meaning: the same link type over the same candidate
set never supersedes itself, whatever the reason text or the writing actor says."""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.capture import (
    apply_provenance_sweep_heartbeat,
    backfill_lineage_repo_links,
    backfill_lineage_repo_links_at_write_time,
    derive_or_abstain,
    open_thread,
)

_ROWS = ("SELECT count(*) FROM assertions WHERE object_id=$1 "
         "AND name='derivation_abstained_in_repo'")


async def test_same_candidates_with_new_reason_writes_nothing(actions: Actions) -> None:
    obj = await actions.create_or_find_object("Decision", "idem-a", "test")
    cand = await actions.create_or_find_object("SoftwareProject", "repo:idem-a1", "test")
    other = await actions.create_or_find_object("SoftwareProject", "repo:idem-a2", "test")
    first = await derive_or_abstain(actions, obj, "in_repo", [cand, other], "one",
                                    why_if_ambiguous="first wording")
    again = await derive_or_abstain(actions, obj, "in_repo", [other, cand], "two",
                                    why_if_ambiguous="second wording")
    assert "unchanged" not in first
    assert again["unchanged"] is True
    assert await actions.pool.fetchval(_ROWS, obj) == 1


async def test_changed_candidate_set_still_writes(actions: Actions) -> None:
    obj = await actions.create_or_find_object("Decision", "idem-b", "test")
    a = await actions.create_or_find_object("SoftwareProject", "repo:idem-b1", "test")
    b = await actions.create_or_find_object("SoftwareProject", "repo:idem-b2", "test")
    c = await actions.create_or_find_object("SoftwareProject", "repo:idem-b3", "test")
    await derive_or_abstain(actions, obj, "in_repo", [a, b], "one")
    changed = await derive_or_abstain(actions, obj, "in_repo", [a, b, c], "one")
    assert "unchanged" not in changed
    assert await actions.pool.fetchval(_ROWS, obj) == 2


async def _ambiguous_thread(actions: Actions, tag: str) -> object:
    now = datetime.now(UTC)
    g1 = await actions.create_or_find_object("Agent", f"agent:{tag}", "test")
    g2 = await actions.create_or_find_object("Agent", f"agent:{tag}-ii", "test")
    for gen, name in ((g1, "a"), (g2, "b")):
        proj = await actions.create_or_find_object("SoftwareProject", f"repo:{tag}{name}", "test")
        await actions.create_link(gen, proj, "works_in", "test", now, 0.9,
                                  evidence_class="self_declared")
    return await open_thread(actions, "lineage that disagrees with itself",
                             source=f"agent:{tag}-iii")


async def test_both_lanes_run_twice_leave_one_abstention_row(actions: Actions) -> None:
    thread = await _ambiguous_thread(actions, "idemlane")
    for _ in range(2):
        await backfill_lineage_repo_links(
            actions, actor="test", dry_run=False, because="test authorization")
        await backfill_lineage_repo_links_at_write_time(
            actions, actor="test", dry_run=False, because="test authorization")
    assert await actions.pool.fetchval(_ROWS, thread) == 1


async def test_sweep_twice_leaves_one_abstention_row(actions: Actions) -> None:
    thread = await _ambiguous_thread(actions, "idemsweep")
    await apply_provenance_sweep_heartbeat(actions)
    await apply_provenance_sweep_heartbeat(actions)
    assert await actions.pool.fetchval(_ROWS, thread) == 1
