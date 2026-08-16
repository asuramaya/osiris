"""recall(ref) — the full, untruncated record for a Thread or Decision (thread d6ed2f17)."""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.capture import (
    amend_decision,
    annotate_thread,
    mint_bears_on,
    open_thread,
    record_decision,
)
from src.orchestrator.recall import recall
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

NOW = datetime(2026, 8, 1, tzinfo=UTC)

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


# ═══════════ bears_on's own read-back (898840dc, Thoth msg 4828) ═══════════
# mint_bears_on() mints an `answers` edge from a Decision INTO a Thread without closing
# it — before this fix, nothing ever read that edge back onto the thread's own surface.

async def test_recall_surfaces_decisions_that_bear_on_a_thread_oldest_first(
    actions: Actions,
) -> None:
    tid = await open_thread(actions, "a stale row two decisions will speak to",
                            source="agent:me")
    d1 = await record_decision(actions, "first finding that bears on it", kind="ruling",
                               source="agent:a")
    d2 = await record_decision(actions, "second finding that bears on it", kind="ruling",
                               source="agent:b")
    await mint_bears_on(actions, d1, tid)
    await mint_bears_on(actions, d2, tid)

    out = await recall(actions.pool, str(tid))

    assert [b["id"] for b in out["bears_on_from"]] == [str(d1)[:8], str(d2)[:8]]
    assert out["bears_on_from"][0]["summary"] == "first finding that bears on it"
    # the row itself must stay open — bears_on is cite-only, never a close, by construction
    assert out["status"] == "open"


async def test_recall_bears_on_from_is_an_empty_list_not_an_absent_key(
    actions: Actions,
) -> None:
    tid = await open_thread(actions, "never cited by anything", source="agent:me")
    out = await recall(actions.pool, str(tid))
    assert out["bears_on_from"] == []


async def test_recall_bears_on_from_excludes_a_retracted_answers_edge(
    actions: Actions,
) -> None:
    """A live edge only — an unmerge/retraction of the citing Decision must not go on
    claiming it still speaks to this row."""
    tid = await open_thread(actions, "a row whose citation gets retracted", source="agent:me")
    d1 = await record_decision(actions, "a finding later retracted", kind="ruling",
                               source="agent:a")
    await mint_bears_on(actions, d1, tid)
    await actions.pool.execute(
        "UPDATE links SET valid_until=now() WHERE from_id=$1 AND to_id=$2 AND type='answers'",
        d1, tid)

    out = await recall(actions.pool, str(tid))
    assert out["bears_on_from"] == []


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

# ═══════════ legacy_task_ref lookup (msg 4429's acceptance test: "#150" must resolve) ══

async def _stamp_legacy_ref(actions: Actions, oid, task_id: str, store: str) -> None:
    conf = confidence_for(EvidenceClass.SELF_DECLARED)
    await actions.assert_property(
        oid, "legacy_task_ref", {"store": store, "id": task_id}, "roadmap_migration", NOW,
        conf, evidence_class=EvidenceClass.SELF_DECLARED.value,
    )


async def test_recall_resolves_a_bare_hash_number_via_legacy_task_ref(
    actions: Actions,
) -> None:
    did = await record_decision(actions, "migrated row #150", kind="decision",
                                rationale="root-caused and half-fixed", source="agent:me")
    await _stamp_legacy_ref(actions, did, "150", "store-a")

    out = await recall(actions.pool, "#150")

    assert out["type"] == "Decision" and out["id"] == str(did)[:8]


async def test_recall_resolves_the_same_number_without_the_hash_prefix(
    actions: Actions,
) -> None:
    tid = await open_thread(actions, "migrated row 168", kind="obligation", source="agent:me")
    await _stamp_legacy_ref(actions, tid, "168", "store-a")

    out = await recall(actions.pool, "168")

    assert out["type"] == "Thread" and out["id"] == str(tid)[:8]


async def test_recall_refuses_a_legacy_task_id_present_in_two_stores(
    actions: Actions,
) -> None:
    did = await record_decision(actions, "store A's own #171", kind="decision",
                                source="agent:me")
    tid = await open_thread(actions, "store B's own, unrelated #171", source="agent:other")
    await _stamp_legacy_ref(actions, did, "171", "store-a")
    await _stamp_legacy_ref(actions, tid, "171", "store-b")

    out = await recall(actions.pool, "#171")

    assert "error" in out
    assert "171" in out["error"] and "store-a" in out["error"] and "store-b" in out["error"]


async def test_recall_falls_through_to_the_ordinary_ladder_when_no_legacy_ref_matches(
    actions: Actions,
) -> None:
    # "#999" never migrated, but a Thread's own summary happens to quote it — unchanged,
    # pre-existing substring-match behavior must still work.
    await open_thread(actions, "still waiting on #999 to land", source="agent:me")

    out = await recall(actions.pool, "#999")

    assert out.get("type") == "Thread"


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
