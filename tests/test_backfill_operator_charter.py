"""THE OPERATOR CHARTER BACKFILL (thread 1d5b9773, "authority by charter"): mints a
`governs` link from `person:operator` to every active SoftwareProject it doesn't already
govern — this is what makes "the single operator today is chartered over every project,
so behaviour does not change" literally true.
"""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.capture import backfill_operator_charter, ensure_operator_person
from src.orchestrator.charter import operator_charter_of


async def test_backfill_dry_run_reports_the_plan_and_writes_nothing(actions: Actions) -> None:
    await actions.create_or_find_object("SoftwareProject", "repo:proja", "test")
    await actions.create_or_find_object("SoftwareProject", "repo:projb", "test")
    out = await backfill_operator_charter(actions, actor="test", dry_run=True)
    assert out["dry_run"] is True
    repos = {row["repo"] for row in out["plan"]}
    assert {"proja", "projb"} <= repos
    assert await operator_charter_of(actions.pool, "person:operator") == []


async def test_backfill_requires_a_because_to_write(actions: Actions) -> None:
    await actions.create_or_find_object("SoftwareProject", "repo:projc", "test")
    out = await backfill_operator_charter(actions, actor="test", dry_run=False)
    assert "error" in out
    assert await operator_charter_of(actions.pool, "person:operator") == []


async def test_backfill_live_run_mints_governs_to_every_active_project(
    actions: Actions,
) -> None:
    await actions.create_or_find_object("SoftwareProject", "repo:projd", "test")
    await actions.create_or_find_object("SoftwareProject", "repo:proje", "test")
    retired = await actions.create_or_find_object(
        "SoftwareProject", "repo:retiredproj", "test")
    await actions.pool.execute(
        "UPDATE objects SET status='retired' WHERE id=$1", retired)
    out = await backfill_operator_charter(
        actions, actor="test", dry_run=False, because="wave-21 authority-by-charter rollout")
    assert set(out["minted"]) == {"projd", "proje"}
    charter = await operator_charter_of(actions.pool, "person:operator")
    assert charter == ["projd", "proje"]  # the retired project never gets a charter link


async def test_backfill_is_idempotent(actions: Actions) -> None:
    await actions.create_or_find_object("SoftwareProject", "repo:projf", "test")
    await backfill_operator_charter(actions, actor="test", dry_run=False, because="first pass")
    out = await backfill_operator_charter(actions, actor="test", dry_run=False,
                                          because="second pass, should find nothing left")
    assert out["minted"] == []
    assert out["already_chartered"] == ["projf"]


async def test_backfill_never_touches_a_project_already_chartered_by_a_prior_run(
    actions: Actions,
) -> None:
    """A project already governed by person:operator (e.g. via a direct charter_for call,
    not just this backfill) is recognized as covered and never re-linked."""
    person_id = await ensure_operator_person(actions, source="test")
    proj_id = await actions.create_or_find_object("SoftwareProject", "repo:projg", "test")
    from datetime import UTC, datetime

    await actions.create_link(person_id, proj_id, "governs", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared", actor="test")
    out = await backfill_operator_charter(actions, actor="test", dry_run=True)
    assert "projg" not in {row["repo"] for row in out["plan"]}
    assert out["already_chartered"] == ["projg"]
