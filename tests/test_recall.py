"""recall(ref) — the full, untruncated record for a Thread or Decision (thread d6ed2f17)."""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.capture import open_thread, record_decision
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
