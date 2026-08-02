"""recall(ref) — the full, untruncated record for a Thread or Decision (thread d6ed2f17)."""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.capture import (
    amend_decision,
    annotate_thread,
    open_thread,
    record_decision,
)
from src.orchestrator.recall import recall

_LONG_SUMMARY = (
    "A" * 200 + " — this summary is well past the 160-char cap task #60 applies in "
    "terse mode, and recall() must hand it back whole, not truncated."
)


async def test_recall_finds_a_thread_by_short_id_prefix(actions: Actions) -> None:
    tid = await open_thread(actions, _LONG_SUMMARY, kind="obligation",
                            owner="operator", source="agent:me")
    short = str(tid)[:8]

    out = await recall(actions.pool, short)

    assert out["type"] == "Thread"
    assert out["summary"] == _LONG_SUMMARY
    assert out["kind"] == "obligation" and out["owner"] == "operator"
    assert out["id"] == short


async def test_recall_finds_a_thread_by_full_uuid(actions: Actions) -> None:
    tid = await open_thread(actions, "a real duty", source="agent:me")

    out = await recall(actions.pool, str(tid))

    assert out["type"] == "Thread" and out["summary"] == "a real duty"


async def test_recall_finds_a_thread_by_summary_substring(actions: Actions) -> None:
    await open_thread(actions, "an unmistakably distinct phrase for substring matching",
                      source="agent:me")

    out = await recall(actions.pool, "unmistakably distinct phrase")

    assert out["type"] == "Thread"
    assert "unmistakably distinct phrase" in out["summary"]


async def test_recall_auto_detects_a_decision_when_no_thread_matches(
    actions: Actions,
) -> None:
    did = await record_decision(actions, _LONG_SUMMARY, kind="ruling",
                                rationale="the reasoning, kept whole", source="agent:me")
    short = str(did)[:8]

    out = await recall(actions.pool, short)

    assert out["type"] == "Decision"
    assert out["summary"] == _LONG_SUMMARY
    assert out["rationale"] == "the reasoning, kept whole"


async def test_recall_respects_an_explicit_kind_hint(actions: Actions) -> None:
    """A kind hint skips the OTHER type's query entirely — asking for kind='decision' on a
    ref that only matches a Thread must refuse, never silently fall through to it."""
    tid = await open_thread(actions, "only a thread has this exact phrase", source="agent:me")

    out = await recall(actions.pool, str(tid)[:8], kind="decision")

    assert "error" in out
    assert "decision" in out["error"] and "thread" not in out["error"]


async def test_recall_refuses_an_invalid_kind(actions: Actions) -> None:
    out = await recall(actions.pool, "whatever", kind="reference")
    assert "error" in out and "must be" in out["error"]


async def test_recall_refuses_when_nothing_matches_either_type(actions: Actions) -> None:
    out = await recall(actions.pool, "00000000-0000-0000-0000-000000000000")
    assert "error" in out
    assert "thread/decision" in out["error"]
    assert "search(" in out["error"]


async def test_recall_returns_the_object_canonical_and_short_id(actions: Actions) -> None:
    import hashlib

    tid = await open_thread(actions, "canonical check", source="agent:me")

    out = await recall(actions.pool, str(tid)[:8])

    assert out["id"] == str(tid)[:8]
    # the miner's canonical scheme (capture._canon): thread:<sha1(summary)[:12]> —
    # content-derived, independent of the object's own uuid `id`
    expected = "thread:" + hashlib.sha1(b"canonical check").hexdigest()[:12]
    assert out["canonical"] == expected


# ═══════════ notes / addenda (Thoth DM 3278, thread 1f4dcc03) ═══════════
# annotate_thread/amend_decision write via `_append_property_name` — before this fix,
# neither `note:%` nor `addendum:%` rows surfaced anywhere a reader would think to look.

async def test_recall_folds_thread_notes_oldest_first(actions: Actions) -> None:
    tid = await open_thread(actions, "a thread that will collect notes", source="agent:me")
    await annotate_thread(actions, str(tid), "first observation", source="agent:a")
    await annotate_thread(actions, str(tid), "second observation", source="agent:b")

    out = await recall(actions.pool, str(tid))

    assert [n["note"] for n in out["notes"]] == ["first observation", "second observation"]
    assert out["notes"][0]["source"] == "agent:a"
    # the raw note:<hash> property must not ALSO leak into the flat per-name dump
    assert not any(k.startswith("note:") for k in out)


async def test_recall_thread_notes_is_an_empty_list_not_an_absent_key(
    actions: Actions,
) -> None:
    tid = await open_thread(actions, "never annotated", source="agent:me")
    out = await recall(actions.pool, str(tid))
    assert out["notes"] == []


async def test_recall_folds_decision_addenda_oldest_first(actions: Actions) -> None:
    did = await record_decision(actions, "a decision that will collect addenda",
                                kind="ruling", source="agent:me")
    await amend_decision(actions, str(did), "the number was wrong by 9.2x", source="agent:a")
    await amend_decision(actions, str(did), "corrected again after re-measuring",
                         source="agent:b")

    out = await recall(actions.pool, str(did))

    assert [a["addendum"] for a in out["addenda"]] == [
        "the number was wrong by 9.2x", "corrected again after re-measuring",
    ]
    assert out["addenda"][0]["source"] == "agent:a"
    assert not any(k.startswith("addendum:") for k in out)


async def test_recall_decision_addenda_is_an_empty_list_not_an_absent_key(
    actions: Actions,
) -> None:
    did = await record_decision(actions, "never amended", kind="ruling", source="agent:me")
    out = await recall(actions.pool, str(did))
    assert out["addenda"] == []


async def test_recall_addendum_observed_at_survives_real_json_serialization(
    actions: Actions,
) -> None:
    """THE ACTUAL RISK: decision_addenda/thread_notes return a raw asyncpg datetime, and
    every other datetime bound for the MCP wire in this codebase is stringified at its own
    call site (no blanket encoder — mcp_server.py has none). json.dumps must not raise."""
    import json

    did = await record_decision(actions, "json safety check", kind="ruling", source="agent:me")
    await amend_decision(actions, str(did), "must serialize cleanly", source="agent:me")

    out = await recall(actions.pool, str(did))

    assert isinstance(out["addenda"][0]["observed_at"], str)
    json.dumps(out)  # raises TypeError on a live datetime object; must not raise


# ═══════════ THE MCP TOOL LAYER ═══════════
# doors()'s own wrapper test is the precedent: recall(), like doors(), needs no mounted
# identity (a pure read), so the tool function is called directly with only srv._pool swapped.

async def test_the_mcp_tool_wrapper_delegates_to_recall(actions: Actions) -> None:
    import src.mcp_server as srv

    tid = await open_thread(actions, "wrapper delegation check", source="agent:me")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.recall(str(tid)[:8])
    finally:
        srv._pool = saved_pool
    assert out["type"] == "Thread" and out["summary"] == "wrapper delegation check"
