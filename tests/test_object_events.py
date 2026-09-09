"""object_events — the witness surface dossier() deliberately hides (thread 085039cc,
Thoth DM 2469): merge/unmerge/split events plus same_as links for one object."""
from __future__ import annotations

import uuid

from src.actions.core import Actions
from src.orchestrator.dossier import object_events


async def test_merge_then_unmerge_both_show_up_for_both_objects(
    actions: Actions, case_id: str,
) -> None:
    winner = await actions.create_or_find_object("Person", "oe-winner", "analyst:test", case_id)
    loser = await actions.create_or_find_object("Person", "oe-loser", "analyst:test", case_id)

    await actions.merge_objects(winner, loser, "same DOB + email", "analyst:test", case_id)

    winner_view = await object_events(actions.pool, winner)
    loser_view = await object_events(actions.pool, loser)

    # the merge event is one row, visible from EITHER side despite the asymmetric
    # object_id/related_id columns (winner is object_id, loser is related_id)
    for view in (winner_view, loser_view):
        [merge_ev] = [e for e in view["events"] if e["event_type"] == "merge"]
        assert merge_ev["object_canonical"] == "oe-winner"
        assert merge_ev["related_canonical"] == "oe-loser"
        assert merge_ev["payload"] == {"justification": "same DOB + email"}
        assert merge_ev["actor"] == "analyst:test"

    # the same_as link is visible from either endpoint too
    for view in (winner_view, loser_view):
        [link] = view["same_as_links"]
        assert link["type"] == "same_as"
        assert link["from_canonical"] == "oe-loser"
        assert link["to_canonical"] == "oe-winner"

    assert loser_view["object_status"] == "merged"
    assert loser_view["merged_into"] == str(winner)
    assert winner_view["object_status"] == "active"
    assert winner_view["merged_into"] is None

    # unmerge adds a SECOND event, still visible from both sides, and clears the
    # projection — but the merge event and same_as link stay as witnesses (never
    # deleted, per Actions.unmerge_objects's own docstring)
    await actions.unmerge_objects(loser, "wrong pair", "analyst:test", case_id)
    loser_view2 = await object_events(actions.pool, loser)
    event_types = {e["event_type"] for e in loser_view2["events"]}
    assert {"merge", "unmerge"} <= event_types
    assert len(loser_view2["same_as_links"]) == 1
    assert loser_view2["object_status"] == "active"
    assert loser_view2["merged_into"] is None


async def test_event_type_filter_narrows_to_one_kind(
    actions: Actions, case_id: str,
) -> None:
    winner = await actions.create_or_find_object("Person", "oe-filter-winner", "analyst:test",
                                                  case_id)
    loser = await actions.create_or_find_object("Person", "oe-filter-loser", "analyst:test",
                                                 case_id)
    await actions.merge_objects(winner, loser, "dup", "analyst:test", case_id)
    await actions.unmerge_objects(loser, "oops", "analyst:test", case_id)

    only_unmerge = await object_events(actions.pool, loser, event_type="unmerge")
    assert [e["event_type"] for e in only_unmerge["events"]] == ["unmerge"]


async def test_an_object_with_no_merge_history_has_no_merge_events_or_links(
    actions: Actions, case_id: str,
) -> None:
    """create_or_find_object writes its own 'create' event — this checks the
    absence of merge-shaped activity specifically, not a bare empty list."""
    lone = await actions.create_or_find_object("Person", "oe-lone", "analyst:test", case_id)
    out = await object_events(actions.pool, lone)
    assert {e["event_type"] for e in out["events"]} == {"create"}
    assert out["same_as_links"] == []
    assert out["object_status"] == "active"


async def test_missing_object_returns_empty_dict(actions: Actions) -> None:
    assert await object_events(actions.pool, uuid.uuid4()) == {}
