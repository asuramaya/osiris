"""PROVENANCE PIECE 2, TRANSCRIPT PROVENANCE FOR MINED FACTS: a mined Thread's
`possible_upstream` edges to the exact ids its own transcript's preceding tool results
actually produced, never a text-similarity guess.
Hermetic: synthetic JSONL lines, real Postgres (never mocked) for the object/link reads.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from src.actions.core import Actions
from src.ingest.sessions import SessionYield, distill, emit_yield, parse_session_yield


def _line(kind: str, content: Any, **extra: Any) -> str:
    d: dict[str, Any] = {"type": kind, "cwd": "/home/someone/code/testrepo",
                          "message": {"content": content}}
    d.update(extra)
    return json.dumps(d)


def _tool_result(content: Any) -> str:
    return _line("user", [{"type": "tool_result", "content": content}])


def _claude(text: str) -> str:
    return _line("assistant", [{"type": "text", "text": text}])


# --- distill(tag_lines=...) ----------------------------------------------------------

def test_distill_default_output_is_unchanged() -> None:
    lines = [_line("user", "hello"), _claude("hi back")]
    assert distill(lines) == distill(lines, tag_lines=False)


def test_distill_tag_lines_prefixes_the_original_line_index() -> None:
    lines = [_tool_result("irrelevant"), _line("user", "hello"), _claude("hi back")]
    text, _cwd = distill(lines, tag_lines=True)
    # the tool_result line (index 0) is never distilled at all, only the surviving
    # user/assistant lines get a tag, carrying THEIR OWN original index (1 and 2).
    assert "[L1] OPERATOR: hello" in text
    assert "[L2] CLAUDE: hi back" in text
    assert "[L0]" not in text


# --- parse_session_yield's new optional `line` field ----------------------------------

def test_parse_session_yield_extracts_source_line() -> None:
    raw = json.dumps({"threads_opened": [
        {"summary": "a real abandoned thread here", "class": "question", "line": 4},
        {"summary": "one with no line pointer at all", "class": "commitment"},
    ]})
    y = parse_session_yield(raw)
    assert y.threads_opened[0]["source_line"] == 4
    assert y.threads_opened[1]["source_line"] is None


def test_parse_session_yield_rejects_a_non_int_line() -> None:
    raw = json.dumps({"threads_opened": [
        {"summary": "a real abandoned thread here", "class": "question", "line": "four"},
    ]})
    y = parse_session_yield(raw)
    assert y.threads_opened[0]["source_line"] is None


# --- end-to-end: emit_yield mints possible_upstream from the transcript's own lines ----

async def _links_from(actions: Actions, from_id: Any) -> list[dict[str, Any]]:
    rows = await actions.pool.fetch(
        "SELECT to_id, properties FROM links WHERE from_id=$1 AND type='possible_upstream'",
        from_id)
    return [dict(r) for r in rows]


async def _thread_id_by_summary(actions: Actions, summary: str) -> Any:
    return await actions.pool.fetchval(
        "SELECT o.id FROM objects o WHERE o.type='Thread' AND EXISTS ("
        "  SELECT 1 FROM current_assertions a WHERE a.object_id=o.id AND a.name='summary' "
        "  AND a.value#>>'{}' = $1)", summary)


async def test_emit_yield_mints_possible_upstream_to_a_mail_message_id(
    actions: Actions,
) -> None:
    now = datetime.now(UTC)
    msg_id = await actions.create_or_find_object("Message", "message:42424", "mailbox")
    await actions.assert_property(msg_id, "grade", "ask", "mailbox", now, 0.9)

    lines = [
        _tool_result(json.dumps({"sent": 42424, "from": "agent:x", "dm_to": "agent:y"})),
        _claude("noted: the operator asked us to keep the receipt on file"),
    ]
    summary = "the operator's receipt request is still unaddressed after this session"
    y = SessionYield(threads_opened=[
        {"summary": summary, "class": "question", "source_line": 1},
    ])
    await emit_yield(actions, y, repo="testrepo", lines=lines)

    tid = await _thread_id_by_summary(actions, summary)
    assert tid is not None
    links = await _links_from(actions, tid)
    assert any(link["to_id"] == msg_id for link in links)
    hit = next(link for link in links if link["to_id"] == msg_id)
    assert hit["properties"]["door"] == "session-miner:tool_result:message"
    assert "read_at" in hit["properties"]


async def test_emit_yield_mints_possible_upstream_to_a_canonical_ref(
    actions: Actions,
) -> None:
    now = datetime.now(UTC)
    other = await actions.create_or_find_object("Decision", "decision:abc123def456", "x")
    await actions.assert_property(other, "summary", "an earlier ruling", "x", now, 0.9)

    lines = [
        _tool_result(json.dumps({"hits": [{"canonical": "decision:abc123def456"}]})),
        _claude("that search result is now abandoned"),
    ]
    summary = "the search hit for the earlier ruling was never followed up"
    y = SessionYield(threads_opened=[
        {"summary": summary, "class": "question", "source_line": 1},
    ])
    await emit_yield(actions, y, repo="testrepo", lines=lines)

    tid = await _thread_id_by_summary(actions, summary)
    links = await _links_from(actions, tid)
    assert any(link["to_id"] == other for link in links)


async def test_emit_yield_mints_a_url_object_from_a_webfetch_result(
    actions: Actions,
) -> None:
    lines = [
        _tool_result("Fetched https://example.com/report#section-2 — nothing usable"),
        _claude("the fetched report turned out to be a dead end, never revisited"),
    ]
    summary = "the fetched report's dead end was never revisited by anyone"
    y = SessionYield(threads_opened=[
        {"summary": summary, "class": "question", "source_line": 1},
    ])
    await emit_yield(actions, y, repo="testrepo", lines=lines)

    tid = await _thread_id_by_summary(actions, summary)
    links = await _links_from(actions, tid)
    assert len(links) == 1
    url_row = await actions.pool.fetchrow(
        "SELECT canonical FROM objects WHERE id=$1", links[0]["to_id"])
    assert url_row is not None
    assert url_row["canonical"].startswith("https://example.com/report")


async def test_emit_yield_with_no_source_line_mints_nothing(actions: Actions) -> None:
    lines = [
        _tool_result(json.dumps({"sent": 99999})),
        _claude("an abandoned thread with no line pointer given at all"),
    ]
    summary = "an abandoned thread with no line pointer given at all here"
    y = SessionYield(threads_opened=[
        {"summary": summary, "class": "question", "source_line": None},
    ])
    await emit_yield(actions, y, repo="testrepo", lines=lines)

    tid = await _thread_id_by_summary(actions, summary)
    assert tid is not None
    assert await _links_from(actions, tid) == []


async def test_emit_yield_walk_back_is_bounded_by_window(actions: Actions) -> None:
    """A message id sitting further back than the window's own reach is never linked:
    the ruling's own "small N of prior tool results", never the whole transcript."""
    now = datetime.now(UTC)
    far_msg = await actions.create_or_find_object("Message", "message:11111", "mailbox")
    await actions.assert_property(far_msg, "grade", "fyi", "mailbox", now, 0.9)

    lines = [_tool_result(json.dumps({"sent": 11111}))]
    # bury it behind more tool_result HITS than the default window scans, filler
    # assistant turns alone would never push it out, since the window counts
    # tool_result-bearing lines, not lines in general.
    lines += [_tool_result(json.dumps({"ok": True})) for _ in range(10)]
    lines.append(_claude("the real, most-recent turn"))
    summary = "a filler-buried thread that nothing upstream should link to"
    y = SessionYield(threads_opened=[
        {"summary": summary, "class": "question", "source_line": len(lines) - 1},
    ])
    await emit_yield(actions, y, repo="testrepo", lines=lines)

    tid = await _thread_id_by_summary(actions, summary)
    links = await _links_from(actions, tid)
    assert far_msg not in {link["to_id"] for link in links}


async def test_emit_yield_out_of_range_source_line_mints_nothing(actions: Actions) -> None:
    lines = [_claude("only one real line here")]
    summary = "an out-of-range source line thread that mints nothing at all here"
    y = SessionYield(threads_opened=[
        {"summary": summary, "class": "question", "source_line": 99},
    ])
    await emit_yield(actions, y, repo="testrepo", lines=lines)

    tid = await _thread_id_by_summary(actions, summary)
    assert tid is not None
    assert await _links_from(actions, tid) == []
