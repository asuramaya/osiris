"""The provenance sweep's Superstition lane (wave 15, mail 8840): resolve in_repo for a
Superstition with no live in_repo edge of its own, from ITS OWN `killed_by` property (the
decision/commit that killed it) — one hop, cardinality-1-or-abstain via derive_or_abstain.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.capture import (
    kill_superstition,
    link_repo,
    record_decision,
    resolve_superstition_orphans,
)

_NOW = datetime.now(UTC)


async def test_a_superstition_killed_by_a_linked_decision_mints(actions: Actions) -> None:
    killer = await record_decision(actions, "the fix that killed the workaround",
                                   source="agent:sup1")
    await link_repo(actions, killer, "supproj", _NOW)
    s = await kill_superstition(actions, "NEVER DO THE THING", killed_by=str(killer))

    out = await resolve_superstition_orphans(actions, dry_run=False, because="test")

    [entry] = [e for e in out["plan"] if e["id"] == str(s)]
    assert entry["verdict"] == "mint"
    linked = await actions.pool.fetchval(
        "SELECT p.canonical FROM links l JOIN objects p ON p.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='in_repo'", s)
    assert linked == "repo:supproj"


async def test_a_superstition_whose_killer_has_no_repo_yet_abstains(actions: Actions) -> None:
    killer = await record_decision(actions, "a fix with no repo of its own",
                                   source="agent:sup2")
    s = await kill_superstition(actions, "ANOTHER DEAD WORKAROUND", killed_by=str(killer))

    out = await resolve_superstition_orphans(actions, dry_run=False, because="test")

    [entry] = [e for e in out["plan"] if e["id"] == str(s)]
    assert entry["verdict"] == "abstain"
    assert entry["candidate_count"] == 0


async def test_a_superstition_with_an_unresolvable_killed_by_abstains(actions: Actions) -> None:
    s = await kill_superstition(actions, "KILLED BY A GHOST", killed_by="not-a-real-id")

    out = await resolve_superstition_orphans(actions, dry_run=False, because="test")

    [entry] = [e for e in out["plan"] if e["id"] == str(s)]
    assert entry["verdict"] == "abstain"
    assert entry["candidate_count"] == 0


async def test_dry_run_never_writes(actions: Actions) -> None:
    killer = await record_decision(actions, "a dry-run-only fix", source="agent:sup3")
    await link_repo(actions, killer, "dryproj", _NOW)
    s = await kill_superstition(actions, "DRY RUN WORKAROUND", killed_by=str(killer))

    out = await resolve_superstition_orphans(actions)  # dry_run=True default
    assert out["dry_run"] is True
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='in_repo'", s)
    assert n == 0


async def test_apply_without_because_refuses(actions: Actions) -> None:
    out = await resolve_superstition_orphans(actions, dry_run=False)
    assert "error" in out


async def test_an_already_linked_superstition_is_never_rescanned(actions: Actions) -> None:
    killer = await record_decision(actions, "a fix, already linked", source="agent:sup4")
    await link_repo(actions, killer, "alreadyproj", _NOW)
    s = await kill_superstition(actions, "ALREADY LINKED WORKAROUND", killed_by=str(killer),
                                repo="alreadyproj")

    out = await resolve_superstition_orphans(actions)
    assert all(e["id"] != str(s) for e in out["plan"])
