"""WAVE 4 HISTORICAL BACKFILL (thread 72cd8e3c, decision c1073f00): resolve_repo_default's
ladder is write-time-only by design (Lane 3, thread 79e785d1) — it never touches a Decision/
Thread that already existed before it deployed. Link every zero-live-link one authored by a
real Agent lineage to its project, via the SAME rung-3 lineage-wide works_in lookup a NEW
write already gets, through derive_or_abstain — never a guess.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.capture import backfill_lineage_repo_links, open_thread


async def test_backfill_dry_run_writes_nothing(actions: Actions) -> None:
    now = datetime.now(UTC)
    gen1 = await actions.create_or_find_object("Agent", "agent:hist1", "test")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:histproj", "test")
    await actions.create_link(gen1, proj, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")
    thread = await open_thread(actions, "an orphan minted before Lane 3 existed",
                               source="agent:hist1-iii")
    out = await backfill_lineage_repo_links(actions, actor="test", dry_run=True)
    assert out["scanned"] == 1
    assert out["to_mint"] == 1
    assert out["plan"][0]["to"] == "histproj"
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='in_repo'", thread)
    assert n == 0


async def test_backfill_mints_in_repo_via_lineage_widening(actions: Actions) -> None:
    now = datetime.now(UTC)
    gen1 = await actions.create_or_find_object("Agent", "agent:hist2", "test")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:histproj2", "test")
    await actions.create_link(gen1, proj, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")
    # the WRITER is a later, orphaned generation of the same lineage with no works_in of
    # its own — exactly the shape a historical object left behind before Lane 3 existed
    thread = await open_thread(actions, "authored by an orphan generation",
                               source="agent:hist2-iv")
    out = await backfill_lineage_repo_links(
        actions, actor="test", dry_run=False, because="test authorization")
    assert out["to_mint"] == 1
    linked = await actions.pool.fetchval(
        "SELECT p.canonical FROM links l JOIN objects p ON p.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='in_repo'", thread)
    assert linked == "repo:histproj2"
    ec = await actions.pool.fetchval(
        "SELECT evidence_class FROM links WHERE from_id=$1 AND type='in_repo'", thread)
    assert ec == "direct_observation"  # mechanically recovered, never self_declared


async def test_backfill_abstains_on_lineage_ambiguity_never_guessing(
    actions: Actions,
) -> None:
    now = datetime.now(UTC)
    gen1 = await actions.create_or_find_object("Agent", "agent:hist3", "test")
    proj_a = await actions.create_or_find_object("SoftwareProject", "repo:histproja", "test")
    proj_b = await actions.create_or_find_object("SoftwareProject", "repo:histprojb", "test")
    await actions.create_link(gen1, proj_a, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")
    gen2 = await actions.create_or_find_object("Agent", "agent:hist3-ii", "test")
    await actions.create_link(gen2, proj_b, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")
    thread = await open_thread(actions, "authored under a lineage that disagrees with itself",
                               source="agent:hist3-iii")
    out = await backfill_lineage_repo_links(
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
    assert set(reason["candidates"]) == {str(proj_a), str(proj_b)}


async def test_backfill_abstains_when_the_lineage_has_no_works_in_at_all(
    actions: Actions,
) -> None:
    thread = await open_thread(actions, "authored by a lineage with no works_in anywhere",
                               source="agent:hist4-i")
    out = await backfill_lineage_repo_links(
        actions, actor="test", dry_run=False, because="test authorization")
    assert out["to_abstain"] == 1
    reason = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='derivation_abstained_in_repo'", thread)
    assert reason is not None
    assert reason["candidate_count"] == 0
    assert reason["reason"] == "no project found anywhere across this lineage's own works_in"


async def test_backfill_is_idempotent(actions: Actions) -> None:
    now = datetime.now(UTC)
    gen1 = await actions.create_or_find_object("Agent", "agent:hist5", "test")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:histproj5", "test")
    await actions.create_link(gen1, proj, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")
    await open_thread(actions, "one more historical orphan", source="agent:hist5-ii")
    first = await backfill_lineage_repo_links(
        actions, actor="test", dry_run=False, because="test authorization")
    second = await backfill_lineage_repo_links(
        actions, actor="test", dry_run=False, because="test authorization")
    assert first["to_mint"] == 1
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE to_id=$1 AND type='in_repo'", proj)
    assert n == 1
    assert second["scanned"] == 0  # the object is no longer zero-live-link


async def test_backfill_never_touches_an_already_linked_object(actions: Actions) -> None:
    now = datetime.now(UTC)
    gen1 = await actions.create_or_find_object("Agent", "agent:hist6", "test")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:histproj6", "test")
    other_proj = await actions.create_or_find_object("SoftwareProject", "repo:histother",
                                                      "test")
    await actions.create_link(gen1, proj, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")
    thread = await open_thread(actions, "already linked by hand", source="agent:hist6-ii")
    await actions.create_link(thread, other_proj, "in_repo", "test", now, 0.9,
                              evidence_class="self_declared")
    out = await backfill_lineage_repo_links(actions, actor="test", dry_run=True)
    assert out["scanned"] == 0


async def test_backfill_ignores_bare_session_sourced_objects(actions: Actions) -> None:
    """A `session`-sourced object (the un-mounted back-compat writer) has no lineage to
    walk at all — `resolve_repo_default`'s own precondition — so this population is
    scoped to `agent:`-sourced summaries only, same as the live ladder."""
    await open_thread(actions, "written by the bare session source, never a real agent",
                      source="session")
    out = await backfill_lineage_repo_links(actions, actor="test", dry_run=True)
    assert out["scanned"] == 0


async def test_backfill_requires_a_because_to_execute(actions: Actions) -> None:
    now = datetime.now(UTC)
    gen1 = await actions.create_or_find_object("Agent", "agent:hist7", "test")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:histproj7", "test")
    await actions.create_link(gen1, proj, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")
    thread = await open_thread(actions, "gated on because", source="agent:hist7-ii")
    out = await backfill_lineage_repo_links(actions, actor="test", dry_run=False)
    assert "error" in out
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='in_repo'", thread)
    assert n == 0


async def test_backfill_mint_supersedes_a_live_abstention_from_another_lane(
    actions: Actions,
) -> None:
    """Thoth's own question (msg 5978): if an object this backfill mints for ALREADY
    carries a live abstention from a DIFFERENT source, does the mint clear it? Answer:
    yes, but not for free — `derive_or_abstain`'s own supersede step only fires when it
    performs the mint itself (this backfill's successful path mints via `link_repo`
    instead, matching the live write path), AND `assert_property`'s own supersession is
    same-source-only, so a naive re-assert under this call's own actor would leave the
    original, different-source abstention "current" beside it. This backfill explicitly
    retires it via `supersede_assertion`, the one legitimate cross-source door."""
    now = datetime.now(UTC)
    gen1 = await actions.create_or_find_object("Agent", "agent:hist8", "test")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:histproj8", "test")
    await actions.create_link(gen1, proj, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")
    thread = await open_thread(actions, "carries a stale abstention from a different lane",
                               source="agent:hist8-ii")
    # simulate a DIFFERENT lane having already abstained on in_repo for this object
    await actions.assert_property(
        thread, "derivation_abstained_in_repo",
        {"link_type": "in_repo", "candidate_count": 0,
         "reason": "a different lane found nothing at the time"},
        "some-other-lane", now, 0.6, evidence_class="direct_observation")
    out = await backfill_lineage_repo_links(
        actions, actor="test", dry_run=False, because="test authorization")
    assert out["to_mint"] == 1
    linked = await actions.pool.fetchval(
        "SELECT to_id FROM links WHERE from_id=$1 AND type='in_repo'", thread)
    assert linked == proj
    resolved = await actions.pool.fetchval(
        "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='derivation_abstained_in_repo'", thread)
    assert resolved is not None
    assert resolved.get("resolved") is True
    assert resolved.get("resolved_to") == str(proj)
