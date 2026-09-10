"""PROVENANCE SWEEP, WAVE 15, DECISION/THREAD LANE REFINEMENT (mail 8840): the finer rung
named as follow-up scope by `backfill_lineage_repo_links` itself (decision 69277bd3) — an
object left abstained by that lane's CURRENT-unanimous check can still resolve against what
its own lineage's works_in looked like AT THE OBJECT'S OWN write time, via
`lineage_works_in_at` (agents.py). Never a guess: still cardinality-1-or-abstain.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.actions.core import Actions
from src.orchestrator.capture import backfill_lineage_repo_links_at_write_time, open_thread


async def test_resolves_when_the_lineage_was_unanimous_at_write_time_but_not_now(
    actions: Actions,
) -> None:
    """The exact shape the plain lane leaves behind: a lineage that had ONE project live
    when the object was captured, and only PICKED UP a second project afterward — current-
    unanimous abstains (2 live now), write-time resolves (1 live then)."""
    earlier = datetime.now(UTC) - timedelta(days=10)
    later = datetime.now(UTC) + timedelta(days=1)
    gen1 = await actions.create_or_find_object("Agent", "agent:wt1", "test")
    proj_a = await actions.create_or_find_object("SoftwareProject", "repo:wtproja", "test")
    await actions.create_link(gen1, proj_a, "works_in", "test", earlier, 0.9,
                              evidence_class="self_declared")
    thread = await open_thread(actions, "captured while the lineage had one project",
                               source="agent:wt1-ii")
    # the lineage only picks up a SECOND project after this thread was already written
    proj_b = await actions.create_or_find_object("SoftwareProject", "repo:wtprojb", "test")
    gen2 = await actions.create_or_find_object("Agent", "agent:wt1-iii", "test")
    await actions.create_link(gen2, proj_b, "works_in", "test", later, 0.9,
                              evidence_class="self_declared")

    out = await backfill_lineage_repo_links_at_write_time(
        actions, actor="test", dry_run=False, because="test authorization")
    assert out["to_mint"] == 1
    linked = await actions.pool.fetchval(
        "SELECT p.canonical FROM links l JOIN objects p ON p.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='in_repo'", thread)
    assert linked == "repo:wtproja"
    ec = await actions.pool.fetchval(
        "SELECT evidence_class FROM links WHERE from_id=$1 AND type='in_repo'", thread)
    assert ec == "direct_observation"


async def test_still_abstains_when_ambiguous_even_at_write_time(actions: Actions) -> None:
    now = datetime.now(UTC)
    gen1 = await actions.create_or_find_object("Agent", "agent:wt2", "test")
    proj_a = await actions.create_or_find_object("SoftwareProject", "repo:wt2proja", "test")
    proj_b = await actions.create_or_find_object("SoftwareProject", "repo:wt2projb", "test")
    await actions.create_link(gen1, proj_a, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")
    gen2 = await actions.create_or_find_object("Agent", "agent:wt2-ii", "test")
    await actions.create_link(gen2, proj_b, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")
    thread = await open_thread(actions, "genuinely ambiguous even at its own write time",
                               source="agent:wt2-iii")

    out = await backfill_lineage_repo_links_at_write_time(
        actions, actor="test", dry_run=False, because="test authorization")
    assert out["to_abstain"] == 1
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='in_repo'", thread)
    assert n == 0
    reason = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='derivation_abstained_in_repo'", thread)
    assert reason is not None
    assert reason["candidate_count"] == 2


async def test_abstains_when_the_lineage_had_no_works_in_yet_at_write_time(
    actions: Actions,
) -> None:
    """A project link that only appears AFTER the object's own observed_at must not count
    — the window is a strict `first_seen <= at`, not "ever, eventually"."""
    now = datetime.now(UTC)
    later = now + timedelta(days=5)
    thread = await open_thread(actions, "written before the lineage had a project at all",
                               source="agent:wt3-i")
    gen2 = await actions.create_or_find_object("Agent", "agent:wt3-ii", "test")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:wt3proj", "test")
    await actions.create_link(gen2, proj, "works_in", "test", later, 0.9,
                              evidence_class="self_declared")

    out = await backfill_lineage_repo_links_at_write_time(
        actions, actor="test", dry_run=False, because="test authorization")
    assert out["to_abstain"] == 1
    reason = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='derivation_abstained_in_repo'", thread)
    assert reason["candidate_count"] == 0


async def test_dry_run_writes_nothing(actions: Actions) -> None:
    earlier = datetime.now(UTC) - timedelta(days=1)
    gen1 = await actions.create_or_find_object("Agent", "agent:wt4", "test")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:wt4proj", "test")
    await actions.create_link(gen1, proj, "works_in", "test", earlier, 0.9,
                              evidence_class="self_declared")
    thread = await open_thread(actions, "dry run only", source="agent:wt4-ii")

    out = await backfill_lineage_repo_links_at_write_time(actions, actor="test")
    assert out["dry_run"] is True
    assert out["to_mint"] == 1
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='in_repo'", thread)
    assert n == 0


async def test_requires_a_because_to_execute(actions: Actions) -> None:
    out = await backfill_lineage_repo_links_at_write_time(actions, actor="test", dry_run=False)
    assert "error" in out


async def test_mint_supersedes_a_live_abstention_from_the_plain_lane(actions: Actions) -> None:
    """The plain lane's own abstention (its current-unanimous check having already found
    nothing) must not sit "current" forever once this finer rung resolves the same object."""
    earlier = datetime.now(UTC) - timedelta(days=1)
    gen1 = await actions.create_or_find_object("Agent", "agent:wt5", "test")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:wt5proj", "test")
    await actions.create_link(gen1, proj, "works_in", "test", earlier, 0.9,
                              evidence_class="self_declared")
    thread = await open_thread(actions, "carries a stale abstention from the plain lane",
                               source="agent:wt5-ii")
    await actions.assert_property(
        thread, "derivation_abstained_in_repo",
        {"link_type": "in_repo", "candidate_count": 0,
         "reason": "no project found anywhere across this lineage's own works_in"},
        "backfill_lineage_repo_links:test", datetime.now(UTC), 0.6,
        evidence_class="direct_observation")

    out = await backfill_lineage_repo_links_at_write_time(
        actions, actor="test", dry_run=False, because="test authorization")
    assert out["to_mint"] == 1
    resolved = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='derivation_abstained_in_repo'", thread)
    assert resolved.get("resolved") is True
    assert resolved.get("resolved_to") == str(proj)
