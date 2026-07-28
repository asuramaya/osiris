"""THE INBOX's pure builders (task #71) — inbox.py's build_inbox() turns the SAME
live-desk composition /live-desk already renders into a typed Block tree. These tests
assert on the TREE, never on any HTML string (that's test_inbox_catalog.py's job)."""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.compositions import LIVE_DESK, save_composition


async def _seed_live_desk(actions: Actions) -> None:
    await save_composition(actions.pool, "live-desk", LIVE_DESK)


async def test_build_inbox_maps_each_section_to_its_item_kind(actions: Actions) -> None:
    from src.orchestrator.capture import open_thread
    from src.orchestrator.deploy_guard import alarm_schema_drift
    from src.orchestrator.mailbox import send_message

    await open_thread(actions, "operator must pick a direction", owner="operator",
                      source="agent:me")
    await send_message(actions.pool, from_agent="agent:x", from_project="osiris",
                       to_project="operator", body="which design should we ship?",
                       desk_kind="decision")
    await alarm_schema_drift(actions.pool, "code expects '0036', DB is at '0034'",
                             service="osiris-worker")
    await _seed_live_desk(actions)

    from src.api.inbox.inbox import build_inbox
    inbox_list = await build_inbox(actions.pool)

    by_kind = {i.item_kind: i for i in inbox_list.items}
    assert "operator must pick a direction" in by_kind["review"].title
    assert "which design should we ship" in by_kind["question"].title
    assert "SCHEMA DRIFT" in by_kind["notify"].title


async def test_build_inbox_action_row_carries_the_real_action(actions: Actions) -> None:
    from src.orchestrator.capture import open_thread
    from src.orchestrator.mailbox import send_message

    await open_thread(actions, "an owed obligation", owner="operator", source="agent:me")
    await send_message(actions.pool, from_agent="agent:x", from_project="osiris",
                       to_project="operator", body="a real decision", desk_kind="decision")
    await _seed_live_desk(actions)

    from src.api.inbox.inbox import build_inbox
    inbox_list = await build_inbox(actions.pool)

    by_kind = {i.item_kind: i for i in inbox_list.items}
    review_buttons = by_kind["review"].actions.buttons
    assert review_buttons[0].action == "resolve_thread"
    assert review_buttons[0].style == "primary"
    question_buttons = by_kind["question"].actions.buttons
    assert question_buttons[0].action == "settle"


async def test_build_inbox_is_empty_on_a_clean_desk(actions: Actions) -> None:
    await _seed_live_desk(actions)

    from src.api.inbox.inbox import build_inbox
    inbox_list = await build_inbox(actions.pool)

    assert inbox_list.items == []
    assert inbox_list.empty_label == "Inbox clear."


async def test_build_inbox_degrades_honestly_on_an_unseeded_composition(
    actions: Actions,
) -> None:
    """A fresh/unseeded DB has no 'live-desk' composition saved — run_composition returns
    {"error": ...}, not {"items": ...} (the same case /live-desk's own route already
    degrades gracefully on). Crashing here would be the exact KeyError this test pins
    against; the fix names the real reason rather than a misleading bare 'Inbox clear.'"""
    # deliberately NOT seeding live-desk

    from src.api.inbox.inbox import build_inbox
    inbox_list = await build_inbox(actions.pool)

    assert inbox_list.items == []
    assert "not seeded" in inbox_list.empty_label or "isn't seeded" in inbox_list.empty_label


async def test_build_inbox_never_lists_a_resolved_thread(actions: Actions) -> None:
    from src.orchestrator.capture import open_thread, resolve_thread

    stale = await open_thread(actions, "an old operator debt, now closed", owner="operator",
                              source="agent:me")
    await resolve_thread(actions, str(stale), because="handled", source="agent:me")
    await _seed_live_desk(actions)

    from src.api.inbox.inbox import build_inbox
    inbox_list = await build_inbox(actions.pool)

    assert all("now closed" not in i.title for i in inbox_list.items)


async def test_short_title_truncates_long_summaries_honestly() -> None:
    from src.api.inbox.inbox import _TITLE_CAP, _short_title

    long_summary = "word " * 200
    title = _short_title(long_summary)
    assert len(title) <= _TITLE_CAP
    assert title.endswith("…")

    short_summary = "a short one-liner"
    assert _short_title(short_summary) == short_summary


def test_age_formats_minutes_hours_and_days() -> None:
    from datetime import UTC, datetime, timedelta

    from src.api.inbox.inbox import _age

    now = datetime.now(UTC)
    assert _age(now, now - timedelta(minutes=5)) == "5m ago"
    assert _age(now, now - timedelta(hours=3)) == "3h ago"
    assert _age(now, now - timedelta(days=2)) == "2d ago"
