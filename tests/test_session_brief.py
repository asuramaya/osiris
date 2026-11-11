"""A cross-account resume is a real continuation: the brief is built from the WHOLE stored
session, and the old transcript stays readable on demand from osiris's own copy.

The fixture is a transcript another Claude account owns, ingested into the soul store. The
hard line these tests pin: nothing is rewritten, the old owner stamps stay exactly as they
were, and the brief is plain text handed to a new session, never a transcript."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.ingest.soul_store import SoulStore
from src.orchestrator.resume_account import store_account_mismatch, store_briefing
from src.orchestrator.session_brief import TOTAL_CHARS, SessionDigest, build_session_brief

OLD = "74d4ddbd-67aa-4830-aaee-388b128e19d5"
NEW = "2e54b530-8738-4bee-86c9-56a49e200cd2"
SID = "feed5678"


def _u(text: Any, **extra: Any) -> str:
    return json.dumps({"type": "user", "ownerAccountUuid": OLD, "timestamp":
                       "2026-09-01T10:00:00.000Z", **extra, "message": {"content": text}})


def _a(blocks: list[dict[str, Any]], model: str = "claude-opus-5") -> str:
    return json.dumps({"type": "assistant", "ownerAccountUuid": OLD, "timestamp":
                       "2026-09-01T10:05:00.000Z",
                       "message": {"model": model, "content": blocks}})


def _use(name: str, **inp: Any) -> dict[str, Any]:
    return {"type": "tool_use", "name": name, "input": inp}


def _text(t: str) -> dict[str, Any]:
    return {"type": "text", "text": t}


def _result(text: str) -> str:
    return json.dumps({"type": "user", "ownerAccountUuid": OLD, "message": {"content": [
        {"type": "tool_result", "content": [{"type": "text", "text": text}]}]}})


def _inbox_dm(mid: int, who: str, body: str) -> str:
    return _result(json.dumps({"project": "p", "messages": [
        {"id": mid, "from": who, "dm": True, "grade": "ask", "body": body},
        {"id": mid + 1, "from": "deploy:disconnect-warning", "dm": False, "grade": "fyi",
         "body": "deploy restarting"}]}))


def _session_lines(*, long_summary: bool = False) -> list[str]:
    summary = ("This session is being continued from a previous conversation that ran out "
               "of context.\n\nSummary:\n1. Primary Request and Intent: build the widget.\n"
               + ("   filler detail line\n" * (2500 if long_summary else 3))
               + "7. Pending Tasks: ship the widget.\n9. Next Step: run the gate on branch "
                 "widget-lane and report the hash.")
    return [
        _u("<system-reminder>harness noise</system-reminder>"),                      # 0
        _u(summary, isCompactSummary=True),                                          # 1
        _u("please also keep the tests hermetic"),                                   # 2
        _u("┌─ osiris mail · DM #77 ─\n│ from : thoth\n│ « preview only »"),         # 3
        _inbox_dm(77, "agent:ad1a1cb0-g129", "FULL BODY: do the widget lane, one branch."),  # 4
        _a([_text("Starting the widget lane now, on a branch from the deployed sha."),
            _use("mcp__osiris__record_decision", summary="Widget uses a ring buffer"),
            _use("mcp__osiris__open_thread", summary="Widget needs a soak test",
                 kind="obligation"),
            _use("mcp__osiris__thread", action="resolve", ref="abc12345",
                 because="superseded by the widget lane"),
            _use("mcp__osiris__send", to_agent="agent:ad1a1cb0-g129",
                 body="tip is on its way"),
            _use("Edit", file_path="/repo/src/widget.py"),
            _use("Bash", command="git checkout -b widget-lane origin/main"),
            _use("Bash", command='git commit -m "Widget ring buffer drops the oldest"')]),  # 5
        _result("[widget-lane 1a2b3c4] Widget ring buffer drops the oldest"),        # 6
        _a([_text("The gate is green on widget-lane; next I report the hash to Thoth "
                  "and then wait for the deploy.")]),                                # 7
    ]


async def _ingest(actions: Any, tmp_path: Path, lines: list[str]) -> None:
    t = tmp_path / f"{SID}-0000-4000-8000-000000000000.jsonl"
    t.write_text("\n".join(lines) + "\n")
    await SoulStore(actions.pool).ingest_path(str(t), SID)


def test_the_digest_extracts_each_kind_with_its_line_index() -> None:
    d = SessionDigest()
    for i, raw in enumerate(_session_lines()):
        d.feed(i, raw.encode())
    assert d.lines == 8 and d.compactions == 1 and d.compact is not None and d.compact[0] == 1
    assert [t for _i, t in d.operator] == ["please also keep the tests hermetic"]
    assert d.dms[77][0] == 4 and d.dms[77][2].startswith("FULL BODY")
    assert 78 not in d.dms                                   # deploy notices are not asks
    assert d.decisions[-1] == (5, "Widget uses a ring buffer")
    assert d.opened[-1][1] == "Widget needs a soak test"
    assert "superseded by the widget lane" in d.resolved[-1][1]
    assert d.sent[-1][1].startswith("to agent:ad1a1cb0-g129: tip is on its way")
    assert d.files == {"/repo/src/widget.py": 5}
    assert list(d.branches) == ["widget-lane"]
    assert d.commits[-1][1:3] == ("widget-lane", "1a2b3c4")
    assert d.said[-1][0] == 7


async def test_the_brief_is_built_from_the_whole_stored_session(
    actions: Any, tmp_path: Path,
) -> None:
    await _ingest(actions, tmp_path, _session_lines())
    brief = await build_session_brief(
        actions.pool, handle="Widgeteer", anchor_sid=SID,
        finding={"detail": "history belongs to another Claude account"})
    assert brief is not None
    assert "You are Widgeteer, in a NEW session that continues an earlier one" in brief
    assert "another Claude account" in brief and "8 stored lines" in brief
    assert "LAST COMPACTION SUMMARY (L1)" in brief and "9. Next Step: run the gate" in brief
    assert "L4 DM #77 from agent:ad1a1cb0-g129, verbatim: FULL BODY: do the widget lane" in brief
    assert "L2 operator, verbatim: please also keep the tests hermetic" in brief
    assert "L5: Widget uses a ring buffer" in brief
    assert "Widget needs a soak test" in brief and "superseded by the widget lane" in brief
    assert "widget-lane" in brief and "/repo/src/widget.py" in brief
    assert "The gate is green on widget-lane" in brief          # what it was doing
    assert "cite_transcript(" in brief and "read_citation(" in brief
    assert "lines 0 to 7" in brief
    assert "harness noise" not in brief and "deploy restarting" not in brief


async def test_a_long_summary_keeps_its_ends_and_the_brief_stays_compaction_sized(
    actions: Any, tmp_path: Path,
) -> None:
    await _ingest(actions, tmp_path, _session_lines(long_summary=True))
    brief = await build_session_brief(
        actions.pool, handle="Widgeteer", anchor_sid=SID, finding=None)
    assert brief is not None
    assert "characters omitted from the middle; the whole summary is at L1" in brief
    assert "1. Primary Request and Intent: build the widget." in brief   # the start
    assert "9. Next Step: run the gate on branch widget-lane" in brief    # the end
    assert len(brief) <= TOTAL_CHARS + 4000


async def test_building_the_brief_changes_nothing_and_the_old_owner_stands(
    actions: Any, tmp_path: Path,
) -> None:
    """The hard line: no owner stamp is rewritten, no transcript is synthesized. The store's
    lines and their chain are exactly what was ingested, and the account question still
    reads the OLD owner after the brief exists."""
    lines = _session_lines()
    await _ingest(actions, tmp_path, lines)
    before = await actions.pool.fetchval(
        "SELECT count(*) FROM soul_lines WHERE harness='claude-code' AND anchor_sid=$1", SID)
    finding = await store_account_mismatch(actions.pool, SID, current=NEW)
    assert finding is not None and finding["history_owner"] == OLD
    brief = await store_briefing(
        actions.pool, handle="Widgeteer", anchor_sid=SID, finding=finding)
    assert "ownerAccountUuid" not in brief and OLD not in brief
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM soul_lines WHERE harness='claude-code' AND anchor_sid=$1",
        SID) == before
    assert await SoulStore(actions.pool).verify_chain(SID) is True
    again = await store_account_mismatch(actions.pool, SID, current=NEW)
    assert again is not None and again["history_owner"] == OLD
    assert not list(tmp_path.glob("*.jsonl.new")) and len(list(tmp_path.glob("*"))) == 1


async def test_an_old_accounts_lines_are_readable_on_demand_by_the_agent_canonical(
    actions: Any, tmp_path: Path,
) -> None:
    """The brief names the old agent and tells the new session to cite and read lines. Prove
    that road works on a session another account owns: the lineage canonical resolves, and
    the cited line comes back verified from the store."""
    from src.orchestrator import capture
    from src.parsers.base import EvidenceClass
    from src.parsers.evidence import confidence_for

    await _ingest(actions, tmp_path, _session_lines())
    agent = await actions.create_or_find_object("Agent", "agent:feed5678-ii", "test")
    do = EvidenceClass.DIRECT_OBSERVATION
    await actions.assert_property(agent, "session", SID, "test", datetime.now(UTC),
                                  confidence_for(do), evidence_class=do.value)
    brief = await build_session_brief(
        actions.pool, handle="Widgeteer", anchor_sid=SID, finding=None)
    assert brief is not None and "agent='agent:feed5678-ii'" in brief

    thread = await actions.create_or_find_object("Thread", "thread:0123456789ab", "test")
    out = await capture.mint_transcript_citation(
        actions, thread, "agent:feed5678-ii", 4, "recalling the earlier session")
    assert out["line_idx"] == 4
    got = await capture.read_transcript_citation(actions.pool, thread, "agent:feed5678-ii")
    assert got["line_idx"] == 4 and "FULL BODY: do the widget lane" in got["raw_line"]
    # a line index past the end refuses, never invents
    try:
        await capture.mint_transcript_citation(
            actions, thread, "agent:feed5678-ii", 999, "past the end")
    except ValueError as exc:
        assert "no soul_lines row" in str(exc)
    else:
        raise AssertionError("a missing line must refuse")


async def test_a_session_the_store_does_not_hold_has_no_brief(actions: Any) -> None:
    assert await build_session_brief(
        actions.pool, handle="Nobody", anchor_sid="00000000", finding=None) is None


def test_earlier_not_shown_is_the_true_total_not_what_the_cap_kept() -> None:
    """The digest keeps only the newest of each kind and the brief shows fewer still; the
    "(+N earlier not shown)" note must be everything the session had minus what is shown."""
    from src.orchestrator.session_brief import render_brief

    d = SessionDigest()
    n = 0
    for i in range(90):                      # far past the digest's own cap of 40
        d.feed(n, _a([_use("mcp__osiris__record_decision",
                           summary=f"decision number {i}")]).encode())
        n += 1
    for mid in range(100, 112):              # twelve distinct DMs, five are shown
        d.feed(n, _inbox_dm(mid, "agent:ad1a1cb0-g129", f"ask {mid}").encode())
        n += 1
    brief = render_brief(d, handle="X", anchor_sid="abcd1234", agent=None, finding=None,
                         graph_threads=[], handoff=None)
    dec = brief.split("DECISIONS YOU RECORDED:")[1].split("\n\n")[0]
    shown = dec.count("\n- L")
    assert 0 < shown <= 40       # the digest itself keeps at most 40 of the 90
    assert f"(+{90 - shown} earlier not shown)" in dec
    asks = brief.split("LAST INSTRUCTIONS RECEIVED (oldest first):")[1].split("\n\n")[0]
    shown_asks = asks.count("\n- L")
    assert shown_asks == 5
    assert f"(+{12 - shown_asks} earlier not shown)" in asks


def test_a_section_that_shows_everything_claims_nothing_is_missing() -> None:
    from src.orchestrator.session_brief import render_brief

    d = SessionDigest()
    d.feed(0, _a([_use("mcp__osiris__record_decision", summary="only one")]).encode())
    brief = render_brief(d, handle="X", anchor_sid="abcd1234", agent=None, finding=None,
                         graph_threads=[], handoff=None)
    assert "only one" in brief and "earlier not shown" not in brief
