"""open_or_update_thread: one open thread per condition, keyed on a stable prefix rather
than the summary text. open_thread alone hashes the text into the thread's identity, so a
summary carrying a count, a date or a citing set minted a sibling on every change."""
from __future__ import annotations

import uuid

from src.actions.core import Actions
from src.orchestrator.capture import open_or_update_thread, open_thread, resolve_thread

KEY = "Widget watch for gizmo:"


async def _open_ids(actions: Actions, key: str) -> list[uuid.UUID]:
    rows = await actions.pool.fetch(
        "SELECT o.id FROM objects o WHERE o.type='Thread' "
        "AND EXISTS (SELECT 1 FROM current_assertions s WHERE s.object_id=o.id "
        "  AND s.name='summary' AND starts_with(s.value #>> '{}', $1)) "
        "AND (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) = 'open' "
        "ORDER BY o.created_at", key)
    return [r["id"] for r in rows]


async def _effective(actions: Actions, tid: uuid.UUID) -> str:
    return str(await actions.pool.fetchval(
        "SELECT COALESCE("
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "  AND a.name='corrected_summary' ORDER BY a.confidence DESC, a.observed_at DESC "
        "  LIMIT 1),"
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "  AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1))",
        tid))


async def _thread_count(actions: Actions) -> int:
    return int(await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type='Thread'"))


async def test_opens_a_thread_when_nothing_stands_for_the_key(actions: Actions) -> None:
    out = await open_or_update_thread(
        actions, f"{KEY} 3 hits as of today", key=KEY, kind="question", source="agent:test")
    assert out["action"] == "opened" and out["superseded"] == []
    assert await _open_ids(actions, KEY) == [out["id"]]


async def test_the_same_text_writes_nothing(actions: Actions) -> None:
    text = f"{KEY} 3 hits as of today"
    first = await open_or_update_thread(actions, text, key=KEY, kind="question",
                                        source="agent:test")
    before = await _thread_count(actions)
    again = await open_or_update_thread(actions, text, key=KEY, kind="question",
                                        source="agent:test")
    assert again["action"] == "unchanged" and again["id"] == first["id"]
    assert await _thread_count(actions) == before


async def test_changed_text_is_corrected_in_place_with_no_sibling(actions: Actions) -> None:
    first = await open_or_update_thread(
        actions, f"{KEY} 3 hits as of day one", key=KEY, kind="question", source="agent:test")
    before = await _thread_count(actions)

    moved = await open_or_update_thread(
        actions, f"{KEY} 4 hits as of day two", key=KEY, kind="question", source="agent:test")
    assert moved["action"] == "corrected" and moved["id"] == first["id"]
    assert await _thread_count(actions) == before
    assert await _effective(actions, first["id"]) == f"{KEY} 4 hits as of day two"
    assert await _open_ids(actions, KEY) == [first["id"]]


async def test_existing_duplicates_collapse_onto_the_oldest(actions: Actions) -> None:
    oldest = await open_thread(actions, f"{KEY} 1 hit", kind="question", source="agent:test")
    newer = await open_thread(actions, f"{KEY} 2 hits", kind="question", source="agent:test")
    assert await _open_ids(actions, KEY) == [oldest, newer]

    out = await open_or_update_thread(
        actions, f"{KEY} 5 hits", key=KEY, kind="question", source="agent:test")
    assert out["id"] == oldest and out["action"] == "corrected"
    assert out["superseded"] == [str(newer)]
    assert await _open_ids(actions, KEY) == [oldest]


async def test_prefers_the_thread_already_carrying_the_text(actions: Actions) -> None:
    older = await open_thread(actions, f"{KEY} 1 hit", kind="question", source="agent:test")
    current = await open_thread(actions, f"{KEY} 2 hits", kind="question", source="agent:test")

    out = await open_or_update_thread(
        actions, f"{KEY} 2 hits", key=KEY, kind="question", source="agent:test")
    assert out["id"] == current and out["action"] == "unchanged"
    assert out["superseded"] == [str(older)]
    assert await _open_ids(actions, KEY) == [current]


async def test_the_owner_scopes_the_key(actions: Actions) -> None:
    mine = await open_or_update_thread(
        actions, f"{KEY} for one", key=KEY, kind="question", owner="operator",
        source="agent:test")
    other = await open_or_update_thread(
        actions, f"{KEY} for another", key=KEY, kind="question", owner="agent:test",
        source="agent:test")
    assert mine["id"] != other["id"] and other["action"] == "opened"

    again = await open_or_update_thread(
        actions, f"{KEY} for one, moved", key=KEY, kind="question", owner="operator",
        source="agent:test")
    assert again["id"] == mine["id"] and again["action"] == "corrected"
    assert await _effective(actions, other["id"]) == f"{KEY} for another"


async def test_wildcard_characters_in_a_key_match_literally(actions: Actions) -> None:
    key = "Miner x_y% throttled:"
    decoy = await open_thread(actions, "Miner xzy-anything throttled: not the same subject",
                              kind="question", source="agent:test")
    out = await open_or_update_thread(
        actions, f"{key} 2 rejections", key=key, kind="question", source="agent:test")
    assert out["action"] == "opened" and out["id"] != decoy


async def test_a_resolved_thread_is_not_matched(actions: Actions) -> None:
    first = await open_or_update_thread(
        actions, f"{KEY} 1 hit", key=KEY, kind="question", source="agent:test")
    await resolve_thread(actions, str(first["id"]), because="handled", source="agent:test")

    again = await open_or_update_thread(
        actions, f"{KEY} 9 hits, a new episode", key=KEY, kind="question",
        source="agent:test")
    assert again["action"] == "opened" and again["id"] != first["id"]
