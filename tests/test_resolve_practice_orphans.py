"""The provenance sweep's Practice lane resolve in_repo for a
Practice carrying no live in_repo edge of its own, from the DISTINCT projects its own
live `witnesses` edges already name ("the decisions that confirm or refute it"),
cardinality-1-or-abstain via derive_or_abstain, never a guess.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.capture import (
    link_repo,
    open_thread,
    record_decision,
    resolve_practice_orphans,
)

_NOW = datetime.now(UTC)


async def _practice_witnessing(actions: Actions, canonical: str, *decision_ids: object) -> object:
    p = await actions.create_or_find_object("Practice", canonical, "test")
    await actions.assert_property(p, "statement", "a standing rule", "test", _NOW, 0.8,
                                  evidence_class="direct_observation")
    for d in decision_ids:
        await actions.create_link(p, d, "witnesses", "test", _NOW, 0.8,  # type: ignore[arg-type]
                                  evidence_class="direct_observation")
    return p


async def test_a_practice_witnessing_one_projects_worth_of_decisions_mints(
    actions: Actions,
) -> None:
    d1 = await record_decision(actions, "a ruling under repo:witproj", source="agent:wp1")
    await link_repo(actions, d1, "witproj", _NOW)
    d2 = await record_decision(actions, "another ruling, same repo", source="agent:wp1")
    await link_repo(actions, d2, "witproj", _NOW)
    practice = await _practice_witnessing(actions, "practice:solo", d1, d2)

    out = await resolve_practice_orphans(actions, dry_run=False, because="test")

    [entry] = [p for p in out["plan"] if p["canonical"] == "practice:solo"]
    assert entry["verdict"] == "mint"
    linked = await actions.pool.fetchval(
        "SELECT p.canonical FROM links l JOIN objects p ON p.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='in_repo'", practice)
    assert linked == "repo:witproj"


async def test_a_practice_witnessing_two_different_projects_abstains(actions: Actions) -> None:
    d1 = await record_decision(actions, "ruling under proj a", source="agent:wp2")
    await link_repo(actions, d1, "wpa", _NOW)
    d2 = await record_decision(actions, "ruling under proj b", source="agent:wp2")
    await link_repo(actions, d2, "wpb", _NOW)
    practice = await _practice_witnessing(actions, "practice:split", d1, d2)

    out = await resolve_practice_orphans(actions, dry_run=False, because="test")

    [entry] = [p for p in out["plan"] if p["canonical"] == "practice:split"]
    assert entry["verdict"] == "abstain"
    assert entry["candidate_count"] == 2
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='in_repo'", practice)
    assert n == 0


async def test_a_practice_with_no_witnesses_abstains_with_zero_candidates(
    actions: Actions,
) -> None:
    p = await actions.create_or_find_object("Practice", "practice:lonely", "test")
    await actions.assert_property(p, "statement", "unwitnessed", "test", _NOW, 0.8,
                                  evidence_class="direct_observation")

    out = await resolve_practice_orphans(actions, dry_run=False, because="test")

    [entry] = [e for e in out["plan"] if e["canonical"] == "practice:lonely"]
    assert entry["verdict"] == "abstain"
    assert entry["candidate_count"] == 0


async def test_witnesses_pointing_at_an_unlinked_target_contributes_nothing(
    actions: Actions,
) -> None:
    """A witnessed Thread/Decision/Practice that itself carries no in_repo yet is not an
    error, and not a guess; it just names no candidate, same as any other empty vote."""
    thread = await open_thread(actions, "a thread with no repo of its own",
                               source="agent:wp3")
    practice = await _practice_witnessing(actions, "practice:emptyvote", thread)

    out = await resolve_practice_orphans(actions, dry_run=False, because="test")

    [entry] = [e for e in out["plan"] if e["canonical"] == "practice:emptyvote"]
    assert entry["verdict"] == "abstain"
    assert entry["candidate_count"] == 0
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='in_repo'", practice)
    assert n == 0


async def test_dry_run_never_writes(actions: Actions) -> None:
    d1 = await record_decision(actions, "a ruling under repo:dryprac", source="agent:wp4")
    await link_repo(actions, d1, "dryprac", _NOW)
    practice = await _practice_witnessing(actions, "practice:dryonly", d1)

    out = await resolve_practice_orphans(actions)  # dry_run=True default
    assert out["dry_run"] is True
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='in_repo'", practice)
    assert n == 0


async def test_apply_without_because_refuses(actions: Actions) -> None:
    out = await resolve_practice_orphans(actions, dry_run=False)
    assert "error" in out


async def test_a_practice_already_linked_is_never_rescanned(actions: Actions) -> None:
    d1 = await record_decision(actions, "a ruling", source="agent:wp5")
    await link_repo(actions, d1, "alreadylinked", _NOW)
    practice = await _practice_witnessing(actions, "practice:already", d1)
    await link_repo(actions, practice, "alreadylinked", _NOW)  # type: ignore[arg-type]

    out = await resolve_practice_orphans(actions)
    assert out["scanned"] == 0


async def test_a_retired_link_is_held_and_the_heartbeat_stops_re_planning_it(
    actions: Actions,
) -> None:
    """Four live practices read to_mint 4 on every tick: each had a link that was deliberately
    invalidated, which derive_or_abstain never recreates. The result says held, and the
    heartbeat's scan (skip_judged) leaves them out; a manual or dry-run scan still sees them."""
    d1 = await record_decision(actions, "a ruling under repo:heldproj", source="agent:wp9")
    await link_repo(actions, d1, "heldproj", _NOW)
    practice = await _practice_witnessing(actions, "practice:held", d1)
    first = await resolve_practice_orphans(actions, dry_run=False, because="test")
    assert "practice:held" in {p["canonical"] for p in first["plan"] if p["verdict"] == "mint"}
    project = await actions.pool.fetchval(
        "SELECT to_id FROM links WHERE from_id=$1 AND type='in_repo'", practice)
    await actions.invalidate_link(practice, project, "in_repo", "test", _NOW,  # type: ignore[arg-type]
                                  reason="retired on purpose")

    again = await resolve_practice_orphans(actions, dry_run=False, because="test")
    [entry] = [p for p in again["plan"] if p["canonical"] == "practice:held"]
    assert entry["verdict"] == "already_linked" and again["held"] >= 1
    beat = await resolve_practice_orphans(
        actions, dry_run=False, because="test", skip_judged=True)
    assert "practice:held" not in {p["canonical"] for p in beat["plan"]}
    manual = await resolve_practice_orphans(actions)  # dry run
    assert "practice:held" in {p["canonical"] for p in manual["plan"]}


async def test_the_heartbeat_does_not_walk_a_practice_it_already_abstained_on(
    actions: Actions,
) -> None:
    practice = await _practice_witnessing(actions, "practice:judgedonce")  # no witnesses
    await resolve_practice_orphans(actions, dry_run=False, because="test", skip_judged=True)
    beat = await resolve_practice_orphans(
        actions, dry_run=False, because="test", skip_judged=True)
    assert "practice:judgedonce" not in {p["canonical"] for p in beat["plan"]}
    assert practice is not None
