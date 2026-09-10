"""The provenance sweep's SoftwareProject lane (Thoth mail 9054, from Sekhmet's
multi-phase pass 9047): SoftwareProject has no single mint door (five separate
auto-vivifying create_or_find_object sites) and had no sweep lane at all. Unlike this
module's other orphan lanes, this is NOT a derive_or_abstain candidate lookup -- same
shape as Sekhmet's own resolve_seat_orphans (her branch, not yet merged): a friendless
SoftwareProject has no ambiguous candidate set, it is either genuinely connected or it
is not, so to_mint is always 0 and to_abstain counts real hatch confessions."""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.capture import resolve_project_orphans

_NOW = datetime.now(UTC)


async def test_a_friendless_project_confesses_with_no_evidence_named(
    actions: Actions,
) -> None:
    proj = await actions.create_or_find_object(
        "SoftwareProject", "repo:lonelyproj", "fleet-observer")

    out = await resolve_project_orphans(actions, dry_run=False, because="test")

    [entry] = [p for p in out["plan"] if p["canonical"] == "repo:lonelyproj"]
    assert entry["verdict"] == "abstain"
    assert entry["named_by"] == 0
    because = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='unlinked_because'", proj)
    kind = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='unlinked_because_kind'", proj)
    assert because is not None and "no commit, decision, or reference" in because
    assert kind == "standalone"


async def test_a_friendless_project_named_in_prose_still_confesses_but_says_so(
    actions: Actions,
) -> None:
    proj = await actions.create_or_find_object(
        "SoftwareProject", "repo:mentionedproj", "fleet-observer")
    commit = await actions.create_or_find_object(
        "Commit", "commit:deadbeef00000000000000000000000000cafe", "fleet-observer")
    await actions.assert_property(commit, "subject", "fix mentionedproj's boot alarm",
                                  "fleet-observer", _NOW, 0.9, evidence_class="direct_observation")

    out = await resolve_project_orphans(actions, dry_run=False, because="test")

    [entry] = [p for p in out["plan"] if p["canonical"] == "repo:mentionedproj"]
    assert entry["verdict"] == "abstain"
    assert entry["named_by"] >= 1
    because = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='unlinked_because'", proj)
    assert because is not None and "named by" in because


async def test_a_project_with_a_live_link_is_never_scanned(actions: Actions) -> None:
    proj = await actions.create_or_find_object(
        "SoftwareProject", "repo:connectedproj", "fleet-observer")
    agent = await actions.create_or_find_object("Agent", "agent:connectedguy", "fleet-observer")
    await actions.create_link(agent, proj, "works_in", "fleet-observer", _NOW, 0.9,
                              evidence_class="self_declared")

    out = await resolve_project_orphans(actions, dry_run=False, because="test")

    assert not [p for p in out["plan"] if p["canonical"] == "repo:connectedproj"]
    because = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='unlinked_because'", proj)
    assert because is None


async def test_dry_run_never_writes(actions: Actions) -> None:
    proj = await actions.create_or_find_object(
        "SoftwareProject", "repo:dryproj", "fleet-observer")

    out = await resolve_project_orphans(actions)  # dry_run=True default
    assert out["dry_run"] is True
    because = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='unlinked_because'", proj)
    assert because is None


async def test_apply_without_because_refuses(actions: Actions) -> None:
    out = await resolve_project_orphans(actions, dry_run=False)
    assert "error" in out


async def test_is_idempotent_a_second_run_finds_nothing_left(actions: Actions) -> None:
    await actions.create_or_find_object("SoftwareProject", "repo:idemproj2", "fleet-observer")

    first = await resolve_project_orphans(actions, dry_run=False, because="test")
    second = await resolve_project_orphans(actions, dry_run=False, because="test")

    assert first["to_abstain"] >= 1
    assert second["scanned"] == 0
