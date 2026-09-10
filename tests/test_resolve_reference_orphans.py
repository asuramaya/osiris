"""The provenance sweep's Reference lane (wave 15, mail 8840): resolve in_repo for a
zero-live-link Reference from its own `topic` property, matched against a real
SoftwareProject's NAME as a mechanical prefix (`bootstrap_project` always writes
`topic=f"{project}-{topic}"`) — never content inference, cardinality-1-or-abstain via
derive_or_abstain.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.capture import resolve_reference_orphans

_NOW = datetime.now(UTC)


async def _orphan_ref(actions: Actions, canonical: str, topic: str) -> None:
    r = await actions.create_or_find_object("Reference", canonical, "ref:osiris")
    await actions.assert_property(r, "topic", topic, "ref:osiris", _NOW, 0.7,
                                  evidence_class="self_declared")


async def test_a_project_prefixed_topic_mints_in_repo(actions: Actions) -> None:
    await actions.create_or_find_object("SoftwareProject", "repo:heinrich", "test")
    await _orphan_ref(actions, "ref:heinrich-history-genesis", "heinrich-history")

    out = await resolve_reference_orphans(actions, dry_run=False, because="test")

    [entry] = [p for p in out["plan"] if p["canonical"] == "ref:heinrich-history-genesis"]
    assert entry["verdict"] == "mint"
    linked = await actions.pool.fetchval(
        "SELECT p.canonical FROM links l JOIN objects r ON r.id=l.from_id "
        "JOIN objects p ON p.id=l.to_id WHERE r.canonical=$1 AND l.type='in_repo'",
        "ref:heinrich-history-genesis")
    assert linked == "repo:heinrich"


async def test_a_bare_topic_with_no_project_prefix_abstains_never_guessing(
    actions: Actions,
) -> None:
    """The real, live shape: 56 of 56 measured orphans (2026-09-09) carry a bare topic
    like 'history'/'design' — no project name prefixes it, so nothing is minted, even
    though every one of them is plausibly this repo's own doc. Content is not evidence."""
    await actions.create_or_find_object("SoftwareProject", "repo:heinrich", "test")
    await _orphan_ref(actions, "ref:history-genesis", "history")

    out = await resolve_reference_orphans(actions, dry_run=False, because="test")

    [entry] = [p for p in out["plan"] if p["canonical"] == "ref:history-genesis"]
    assert entry["verdict"] == "abstain"
    assert entry["candidate_count"] == 0
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects r ON r.id=l.from_id "
        "WHERE r.canonical=$1 AND l.type='in_repo'", "ref:history-genesis")
    assert n == 0


async def test_a_topic_prefix_matching_two_projects_abstains(actions: Actions) -> None:
    await actions.create_or_find_object("SoftwareProject", "repo:a-team", "test")
    await actions.create_or_find_object("SoftwareProject", "repo:a-team-west", "test")
    # "a-team-west-history" starts with both "a-team-" and "a-team-west-"
    await _orphan_ref(actions, "ref:ateam-ambiguous", "a-team-west-history")

    out = await resolve_reference_orphans(actions, dry_run=False, because="test")

    [entry] = [p for p in out["plan"] if p["canonical"] == "ref:ateam-ambiguous"]
    assert entry["verdict"] == "abstain"
    assert entry["candidate_count"] == 2


async def test_no_topic_property_at_all_abstains_with_zero_candidates(
    actions: Actions,
) -> None:
    r = await actions.create_or_find_object("Reference", "ref:notopic", "ref:osiris")
    await actions.assert_property(r, "name", "no topic here", "ref:osiris", _NOW, 0.7,
                                  evidence_class="self_declared")

    out = await resolve_reference_orphans(actions, dry_run=False, because="test")

    [entry] = [p for p in out["plan"] if p["canonical"] == "ref:notopic"]
    assert entry["verdict"] == "abstain"
    assert entry["candidate_count"] == 0


async def test_dry_run_never_writes(actions: Actions) -> None:
    await actions.create_or_find_object("SoftwareProject", "repo:dryproj", "test")
    await _orphan_ref(actions, "ref:dryproj-history-x", "dryproj-history")

    out = await resolve_reference_orphans(actions)  # dry_run=True default
    assert out["dry_run"] is True
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects r ON r.id=l.from_id "
        "WHERE r.canonical=$1 AND l.type='in_repo'", "ref:dryproj-history-x")
    assert n == 0


async def test_apply_without_because_refuses(actions: Actions) -> None:
    out = await resolve_reference_orphans(actions, dry_run=False)
    assert "error" in out


async def test_already_linked_reference_is_never_rescanned(actions: Actions) -> None:
    proj = await actions.create_or_find_object("SoftwareProject", "repo:linkedproj", "test")
    r = await actions.create_or_find_object("Reference", "ref:linkedproj-history-x",
                                            "ref:osiris")
    await actions.assert_property(r, "topic", "linkedproj-history", "ref:osiris", _NOW, 0.7,
                                  evidence_class="self_declared")
    await actions.create_link(r, proj, "in_repo", "test", _NOW, 0.9,
                              evidence_class="self_declared")

    out = await resolve_reference_orphans(actions, dry_run=True)
    assert out["scanned"] == 0
