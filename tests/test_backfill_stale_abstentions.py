"""`stale_in_repo_abstentions`: an `in_repo` abstention on an object that already has a live
`in_repo` link is a stale confession, and the abstention miner must not keep proposing a
project for it. Real Postgres through the `actions` fixture."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator import capture
from src.orchestrator.abstention_miner import _LANES, _next_abstained_object
from src.orchestrator.backfill import run_backfill
from src.orchestrator.capture import backfill_stale_in_repo_abstentions


async def _decision(actions: Actions) -> uuid.UUID:
    from src.ontology.catalog import ensure_type

    await ensure_type(actions, name="Decision", kind="object", actor="test")
    d = await actions.create_or_find_object("Decision", f"decision:{uuid.uuid4()}", "test")
    await capture.derive_or_abstain(actions, d, "in_repo", [], "test")
    return d


async def _link(actions: Actions, d: uuid.UUID, repo: str) -> None:
    project = await actions.create_or_find_object("SoftwareProject", f"repo:{repo}", "test")
    await actions.create_link(d, project, "in_repo", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")


async def _live_abstentions(actions: Actions, d: uuid.UUID) -> int:
    return int(await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions WHERE object_id=$1 "
        "AND name='derivation_abstained_in_repo' AND NOT (value ? 'resolved')", d))


async def test_the_dry_run_counts_and_writes_nothing(actions: Actions) -> None:
    linked = await _decision(actions)
    await _link(actions, linked, "widgetfactory")
    gap = await _decision(actions)

    out = await backfill_stale_in_repo_abstentions(actions, actor="test")

    assert out["dry_run"] is True and out["to_retire"] == 1 and out["retired"] == 0
    assert out["by_type"] == {"Decision": 1}
    assert out["sample"][0]["project"] == "widgetfactory"
    assert await _live_abstentions(actions, linked) == 1       # nothing written
    assert await _live_abstentions(actions, gap) == 1


async def test_apply_retires_the_stale_confession_and_leaves_the_real_gap(
    actions: Actions,
) -> None:
    linked = await _decision(actions)
    await _link(actions, linked, "widgetfactory")
    gap = await _decision(actions)

    refused = await backfill_stale_in_repo_abstentions(actions, actor="test", dry_run=False)
    assert "error" in refused and await _live_abstentions(actions, linked) == 1

    out = await backfill_stale_in_repo_abstentions(
        actions, actor="test", dry_run=False, because="the object already has its project")
    assert out["retired"] == 1
    assert await _live_abstentions(actions, linked) == 0
    assert await _live_abstentions(actions, gap) == 1          # a real gap stays a gap
    resolved = await actions.pool.fetchval(
        "SELECT value->>'resolved_to' FROM current_assertions WHERE object_id=$1 "
        "AND name='derivation_abstained_in_repo'", linked)
    project = await actions.pool.fetchval(
        "SELECT id::text FROM objects WHERE canonical='repo:widgetfactory'")
    assert resolved == project
    assert await actions.pool.fetchval(            # the link itself is untouched
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='in_repo' "
        "AND valid_until IS NULL", linked) == 1
    again = await backfill_stale_in_repo_abstentions(
        actions, actor="test", dry_run=False, because="rerun")
    assert again["retired"] == 0 and again["to_retire"] == 0   # idempotent


async def test_an_object_linked_to_two_projects_is_reported_and_left_alone(
    actions: Actions,
) -> None:
    both = await _decision(actions)
    await _link(actions, both, "widgetfactory")
    await _link(actions, both, "sidecar")

    out = await backfill_stale_in_repo_abstentions(
        actions, actor="test", dry_run=False, because="sweep")

    assert out["retired"] == 0
    assert [r["projects"] for r in out["left_alone_ambiguous"]] == [
        ["repo:sidecar", "repo:widgetfactory"]]
    assert await _live_abstentions(actions, both) == 1


async def test_it_is_reachable_through_the_backfill_verb(actions: Actions) -> None:
    linked = await _decision(actions)
    await _link(actions, linked, "widgetfactory")
    out = await run_backfill(actions.pool, "stale_in_repo_abstentions", actor="test")
    assert out["to_retire"] == 1 and out["dry_run"] is True


async def test_the_miner_never_picks_an_object_that_already_has_its_project(
    actions: Actions,
) -> None:
    linked = await _decision(actions)
    await _link(actions, linked, "widgetfactory")
    lane = next(lane for lane in _LANES if lane.object_type == "Decision")
    assert await _next_abstained_object(actions.pool, lane) is None
    gap = await _decision(actions)
    assert await _next_abstained_object(actions.pool, lane) == gap
