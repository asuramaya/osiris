"""Case is not identity for a project label: a pin of `Lilguy` is the project stored as
`repo:lilguy`. Every place a pin, charter or label is compared or resolved accepts it, and
answers with the stored canonical's own case (a project stored as `repo:Like-Us` stays so).
Nothing in the seat's data is edited."""
from __future__ import annotations

from pathlib import Path

import pytest
from src.actions.core import Actions
from src.orchestrator.capture import _resolve_repo
from src.orchestrator.charter import set_charter
from src.orchestrator.projects import AmbiguousProjectRef, _resolve_software_project
from src.orchestrator.seats import ensure_seat, roster


async def _project(actions: Actions, canonical: str) -> str:
    return await actions.create_or_find_object("SoftwareProject", canonical, "test")


async def _lilguy_seat(actions: Actions, tmp_path: Path, pin: str, charter: list[str]) -> str:
    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text(f'project = "{pin}"\n')
    seat = await ensure_seat(actions, house="lilguy", handle="Lilguy", source="test",
                             anchor_cwd=str(office))
    await set_charter(actions, seat["seat_id"], charter, actor="test")
    return str(seat["seat_id"])


async def test_a_capitalised_pin_agrees_with_a_lowercase_charter_on_the_roster_row(
    actions: Actions, tmp_path: Path,
) -> None:
    """The live specimen: pin 'Lilguy', charter ['lilguy'], project repo:lilguy. Roster said
    pin_charter_agreement 'disagree' and triage_bucket 'no-such-project'."""
    await _project(actions, "repo:lilguy")
    seat = await _lilguy_seat(actions, tmp_path, "Lilguy", ["lilguy"])
    row = next(r for r in (await roster(actions.pool))["seats"] if r["seat"] == seat)
    assert row["pin"]["declared"] == "Lilguy"            # the seat's data is untouched
    assert row["chartered_repos"] == ["lilguy"]
    assert row["pin_charter_agreement"] == "agree"
    assert row["pin"]["triage_bucket"] != "no-such-project"


async def test_a_pin_that_really_differs_still_disagrees_and_a_missing_project_still_misses(
    actions: Actions, tmp_path: Path,
) -> None:
    await _project(actions, "repo:lilguy")
    seat = await _lilguy_seat(actions, tmp_path, "Elsewhere", ["lilguy"])
    row = next(r for r in (await roster(actions.pool))["seats"] if r["seat"] == seat)
    assert row["pin_charter_agreement"] == "disagree"
    assert row["pin"]["triage_bucket"] == "no-such-project"


async def test_the_roster_repo_lookup_and_repo_minting_stay_strict_about_case(
    actions: Actions, tmp_path: Path,
) -> None:
    """Two contracts stay as they were, on purpose: roster(repo=) reports a case variant as
    a near miss rather than a match, and a write that names a project in another case does
    not silently bind to it (set_charter / _resolve_repo). Only the READ-SIDE comparisons
    (pin vs charter agreement, the triage bucket, launch's project check, label
    resolution) are case-insensitive."""
    await _project(actions, "repo:lilguy")
    await _lilguy_seat(actions, tmp_path, "Lilguy", ["lilguy"])
    out = await roster(actions.pool, repo="LILGUY")
    assert out["agreement"] == "no-match"
    assert await _resolve_repo(actions.pool, "Lilguy") is None


async def test_a_label_resolves_to_the_stored_canonical_whatever_its_case(
    actions: Actions,
) -> None:
    await _project(actions, "repo:Like-Us")
    row = await _resolve_software_project(actions.pool, "like-us")
    assert row is not None and row["canonical"] == "repo:Like-Us"   # stored case kept
    row = await _resolve_software_project(actions.pool, "REPO:LIKE-US")
    assert row is not None and row["canonical"] == "repo:Like-Us"


async def test_an_exact_spelling_always_wins_and_two_case_twins_stay_ambiguous(
    actions: Actions,
) -> None:
    a = await _project(actions, "repo:twin")
    await _project(actions, "repo:Twin")
    row = await _resolve_software_project(actions.pool, "twin")
    assert row is not None and row["id"] == a                       # exact spelling wins
    with pytest.raises(AmbiguousProjectRef):
        await _resolve_software_project(actions.pool, "TWIN")       # neither is exact


async def test_launch_accepts_a_capitalised_pin_against_a_lowercase_charter(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.trigger import _resolve_launch_project

    await _project(actions, "repo:lilguy")
    seat = await _lilguy_seat(actions, tmp_path, "Lilguy", ["lilguy"])
    office = str(tmp_path / "office")
    out = await _resolve_launch_project(actions.pool, seat_id=seat, office=office, ancestor=None)
    assert out["refuse"] is None
    assert str(out["project"]).lower() == "lilguy"
    # a pin that names a different project is still refused
    (tmp_path / "office" / ".osiris").write_text('project = "other"\n')
    await _project(actions, "repo:other")
    refused = await _resolve_launch_project(actions.pool, seat_id=seat, office=office,
                                            ancestor=None)
    assert refused["refuse"] is not None
