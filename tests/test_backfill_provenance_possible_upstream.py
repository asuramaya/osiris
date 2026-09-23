"""PROVENANCE BACKFILL: back-stamping
`possible_upstream` onto historical Decision/Thread writes by finding each write's own
receipt in its writer's transcript and reusing piece 2's own tool_result scan. Hermetic:
synthetic JSONL files under tmp_path (never the live fleet's own transcript root), real
Postgres for the graph reads/writes."""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.orchestrator.provenance_backfill import backfill_possible_upstream


def _line(kind: str, content: Any, **extra: Any) -> str:
    d: dict[str, Any] = {"type": kind, "message": {"content": content}}
    d.update(extra)
    return json.dumps(d)


def _tool_result(content: Any) -> str:
    return _line("user", [{"type": "tool_result", "content": content}])


async def _mint_agent_with_sid(
    actions: Actions, agent_canon: str, sid: str, tmp_path: Path, lines: list[str],
) -> Path:
    now = datetime.now(UTC)
    obj = await actions.create_or_find_object("Agent", agent_canon, "test")
    await actions.assert_property(
        obj, f"anchor_sid:{sid[:8]}", sid, "test", now, 0.9,
        evidence_class="direct_observation")
    proj_dir = tmp_path / "-home-someone-code-testrepo"
    proj_dir.mkdir(parents=True, exist_ok=True)
    transcript = proj_dir / f"{sid}.jsonl"
    transcript.write_text("\n".join(lines) + "\n")
    return transcript


async def _links_from(actions: Actions, from_id: Any) -> list[dict[str, Any]]:
    rows = await actions.pool.fetch(
        "SELECT to_id, source_id, properties FROM links WHERE from_id=$1 "
        "AND type='possible_upstream'", from_id)
    return [dict(r) for r in rows]


async def test_dry_run_finds_the_writes_own_receipt_and_plans_an_edge(
    actions: Actions, tmp_path: Path,
) -> None:
    now = datetime.now(UTC)
    upstream = await actions.create_or_find_object("Decision", "decision:aaa111bbb222", "x")
    lines = [
        _tool_result(json.dumps({"canonical": "decision:aaa111bbb222", "id": "1"})),
        _tool_result(json.dumps({"canonical": "decision:ccc333ddd444"})),
    ]
    await _mint_agent_with_sid(actions, "agent:writer-a", "sidaaa11deadbeef", tmp_path, lines)
    decision = await actions.create_or_find_object(
        "Decision", "decision:ccc333ddd444", "agent:writer-a")
    await actions.assert_property(
        decision, "summary", "a decision with a real upstream read behind it",
        "agent:writer-a", now, 0.9)

    report = await backfill_possible_upstream(
        actions, dry_run=True, transcript_root=tmp_path)

    assert report["dry_run"] is True
    assert report["edges_to_mint"] == 1
    entry = report["plan"][0]
    assert entry["from"] == "decision:ccc333ddd444"
    assert entry["to"] == str(upstream)
    assert entry["door"].startswith("backfill:")
    # dry run writes nothing
    assert await _links_from(actions, decision) == []


async def test_live_mints_the_edge_sourced_to_the_original_writer_and_is_idempotent(
    actions: Actions, tmp_path: Path,
) -> None:
    now = datetime.now(UTC)
    upstream = await actions.create_or_find_object("Decision", "decision:eee555fff666", "x")
    lines = [
        _tool_result(json.dumps({"canonical": "decision:eee555fff666"})),
        _tool_result(json.dumps({"canonical": "decision:aaa777bbb888"})),
    ]
    await _mint_agent_with_sid(actions, "agent:writer-b", "sidbbb22deadbeef", tmp_path, lines)
    decision = await actions.create_or_find_object(
        "Decision", "decision:aaa777bbb888", "agent:writer-b")
    await actions.assert_property(
        decision, "summary", "another decision with a real upstream read",
        "agent:writer-b", now, 0.9)

    report = await backfill_possible_upstream(
        actions, dry_run=False, because="testing the live path",
        transcript_root=tmp_path)
    assert report["minted"] == 1
    edges = await _links_from(actions, decision)
    assert len(edges) == 1
    assert edges[0]["to_id"] == upstream
    assert edges[0]["source_id"] == "agent:writer-b"  # the ORIGINAL writer, never a miner

    # idempotent: the candidate now carries an edge, so a second pass finds nothing to do
    again = await backfill_possible_upstream(
        actions, dry_run=False, because="second pass", transcript_root=tmp_path)
    assert again["edges_to_mint"] == 0
    assert len(await _links_from(actions, decision)) == 1


async def test_live_refuses_a_blank_because(actions: Actions) -> None:
    report = await backfill_possible_upstream(actions, dry_run=False, because="   ")
    assert "error" in report
    assert "because" in report["error"]


async def test_writer_with_no_anchor_sid_ledger_is_reported_skipped_not_silently_dropped(
    actions: Actions, tmp_path: Path,
) -> None:
    now = datetime.now(UTC)
    decision = await actions.create_or_find_object(
        "Decision", "decision:iii999jjj000", "agent:ledgerless-writer")
    await actions.assert_property(
        decision, "summary", "a decision from a writer with no ledger at all",
        "agent:ledgerless-writer", now, 0.9)

    report = await backfill_possible_upstream(
        actions, dry_run=True, transcript_root=tmp_path)

    assert report["edges_to_mint"] == 0
    assert report["skipped_count"] == 1
    assert report["skipped"][0]["object"] == "decision:iii999jjj000"
    assert "anchor_sid" in report["skipped"][0]["reason"]


# --- newest_first + per-writer summary + runtime ----

async def test_summary_classifies_matched_no_ledger_and_no_transcript(
    actions: Actions, tmp_path: Path,
) -> None:
    now = datetime.now(UTC)
    # matched: a real receipt, findable.
    await actions.create_or_find_object("Decision", "decision:a00000a00000", "x")
    lines = [
        _tool_result(json.dumps({"canonical": "decision:a00000a00000"})),
        _tool_result(json.dumps({"canonical": "decision:aaa000100a2"})),
    ]
    await _mint_agent_with_sid(actions, "agent:writer-matched", "sidmatch1deadbeef",
                               tmp_path, lines)
    matched_d = await actions.create_or_find_object(
        "Decision", "decision:aaa000100a2", "agent:writer-matched")
    await actions.assert_property(matched_d, "summary", "a matched write",
                                  "agent:writer-matched", now, 0.9)

    # no_ledger: writer has zero anchor_sid assertions.
    no_ledger_d = await actions.create_or_find_object(
        "Decision", "decision:noledger00001", "agent:writer-no-ledger")
    await actions.assert_property(no_ledger_d, "summary", "a ledgerless write",
                                  "agent:writer-no-ledger", now, 0.9)

    # no_transcript: writer HAS a ledger sid, but that sid resolves to no receipt for
    # this write (the transcript file exists but never mentions this canonical).
    await _mint_agent_with_sid(
        actions, "agent:writer-no-transcript", "sidnotranscript1",
        tmp_path, [_tool_result(json.dumps({"canonical": "decision:unrelated0001"}))])
    no_transcript_d = await actions.create_or_find_object(
        "Decision", "decision:missingrcpt001", "agent:writer-no-transcript")
    await actions.assert_property(no_transcript_d, "summary", "never finds its receipt",
                                  "agent:writer-no-transcript", now, 0.9)

    report = await backfill_possible_upstream(
        actions, dry_run=True, transcript_root=tmp_path)

    assert report["summary"]["candidates"] == {
        "matched": 1, "no_ledger": 1, "no_transcript": 1, "too_large": 0}
    assert report["summary"]["writers"] == {
        "matched": 1, "no_ledger": 1, "no_transcript": 1, "too_large": 0}
    assert report["edges_by_door"]
    assert sum(report["edges_by_door"].values()) == report["edges_to_mint"]


async def test_newest_first_flips_candidate_order(actions: Actions, tmp_path: Path) -> None:
    older = await actions.create_or_find_object(
        "Decision", "decision:orderolder0001", "agent:writer-order")
    await actions.assert_property(older, "summary", "the older one",
                                  "agent:writer-order", datetime(2020, 1, 1, tzinfo=UTC), 0.9)
    newer = await actions.create_or_find_object(
        "Decision", "decision:ordernewer0001", "agent:writer-order")
    await actions.assert_property(newer, "summary", "the newer one",
                                  "agent:writer-order", datetime(2021, 1, 1, tzinfo=UTC), 0.9)

    oldest_first = await backfill_possible_upstream(
        actions, dry_run=True, limit=1, transcript_root=tmp_path)
    newest_first = await backfill_possible_upstream(
        actions, dry_run=True, limit=1, newest_first=True, transcript_root=tmp_path)

    assert oldest_first["skipped"][0]["object"] == "decision:orderolder0001"
    assert newest_first["skipped"][0]["object"] == "decision:ordernewer0001"
    assert newest_first["newest_first"] is True


async def test_runtime_seconds_is_reported(actions: Actions, tmp_path: Path) -> None:
    report = await backfill_possible_upstream(
        actions, dry_run=True, limit=1, transcript_root=tmp_path)
    assert isinstance(report["runtime_seconds"], float)
    assert report["runtime_seconds"] >= 0


# --- anchor_sid is looked up lineage-wide ---------

async def test_a_successor_generation_matches_via_its_ancestors_anchor_sid(
    actions: Actions, tmp_path: Path,
) -> None:
    """record_session_anchor's own "first writer wins, forever" law means only the FIRST
    generation ever mounted under a shared job_dir gets the anchor_sid ledger entry;
    every successor generation of the SAME lineage must still resolve through it."""
    now = datetime.now(UTC)
    upstream = await actions.create_or_find_object("Decision", "decision:b00000b00000", "x")
    lines = [
        _tool_result(json.dumps({"canonical": "decision:b00000b00000"})),
        _tool_result(json.dumps({"canonical": "decision:bbb1110000a1"})),
    ]
    # the ledger entry lives on the ANCESTOR generation, never the successor's own.
    await _mint_agent_with_sid(actions, "agent:widentest", "sidwide1deadbeef",
                               tmp_path, lines)
    await actions.create_or_find_object("Agent", "agent:widentest-ii", "test")
    decision = await actions.create_or_find_object(
        "Decision", "decision:bbb1110000a1", "agent:widentest-ii")
    await actions.assert_property(
        decision, "summary", "written by the successor generation",
        "agent:widentest-ii", now, 0.9)

    report = await backfill_possible_upstream(
        actions, dry_run=True, transcript_root=tmp_path)

    assert report["edges_to_mint"] == 1
    entry = report["plan"][0]
    assert entry["from"] == "decision:bbb1110000a1"
    assert entry["writer"] == "agent:widentest-ii"  # sourced to the REAL writer
    assert entry["to"] == str(upstream)
    assert report["summary"]["candidates"]["matched"] == 1


# --- THE STALL's own fix -----------------------------

def _sidecar_for(transcript: Path) -> Path:
    return transcript.with_name(transcript.name + ".providx.json")


async def test_a_transcript_over_the_cap_is_skipped_unopened(
    actions: Actions, tmp_path: Path,
) -> None:
    """A 100MB transcript never gets opened at all when it exceeds max_scan_bytes; the
    fix for the 469MB/222MB real transcripts that stalled osiris-mcp for 19 minutes."""
    now = datetime.now(UTC)
    proj_dir = tmp_path / "-home-someone-code-testrepo"
    proj_dir.mkdir(parents=True, exist_ok=True)
    sid = "sidbig001deadbeef"
    big = proj_dir / f"{sid}.jsonl"
    with big.open("wb") as f:
        f.truncate(100 * 1024 * 1024)  # sparse, instant, never actually written
    obj = await actions.create_or_find_object("Agent", "agent:writer-huge", "test")
    await actions.assert_property(
        obj, f"anchor_sid:{sid[:8]}", sid, "test", now, 0.9,
        evidence_class="direct_observation")
    decision = await actions.create_or_find_object(
        "Decision", "decision:cccccc111111", "agent:writer-huge")
    await actions.assert_property(
        decision, "summary", "a write whose only transcript is huge",
        "agent:writer-huge", now, 0.9)

    report = await backfill_possible_upstream(
        actions, dry_run=True, transcript_root=tmp_path, max_scan_bytes=64 * 1024 * 1024)

    assert report["edges_to_mint"] == 0
    assert report["summary"]["candidates"]["too_large"] == 1
    assert "ingest.transcript_scan_max_bytes" in report["skipped"][0]["reason"]
    assert not _sidecar_for(big).exists()  # never opened, so never cached either


async def test_a_settle_minted_write_matches_via_its_short_id(
    actions: Actions, tmp_path: Path,
) -> None:
    """A specimen decision: a Decision minted via
    settle(decisions=[...]) never gets its own `"canonical"` key echoed to the writer's
    transcript, only settle()'s own report does, keyed by the object's SHORT id under
    `accepted.decisions[].id`, never the full canonical string. Real-world specimen
    proved the receipt line genuinely exists (`grep` found it) while the old matcher,
    which only recognized the direct record_decision `"canonical"` shape, missed it,
    this is the fix, not a hypothetical."""
    now = datetime.now(UTC)
    settle_report = json.dumps({
        "complete": True, "accepted": {
            "decisions": [{"id": "1234abcd", "is_handoff": True}],
            "threads_opened": [], "threads_resolved": []},
        "rejected": []})
    lines = [_tool_result(settle_report)]
    await _mint_agent_with_sid(actions, "agent:writer-settle", "sidsettle1deadbeef",
                               tmp_path, lines)
    # A canonical LONGER than the 8-char short id, proving genuine prefix matching,
    # not mere string equality.
    decision = await actions.create_or_find_object(
        "Decision", "decision:1234abcd5678ef90", "agent:writer-settle")
    await actions.assert_property(
        decision, "summary", "minted via settle(), never its own record_decision call",
        "agent:writer-settle", now, 0.9)

    report = await backfill_possible_upstream(
        actions, dry_run=True, transcript_root=tmp_path)

    assert report["summary"]["candidates"]["matched"] == 1
    assert report["summary"]["candidates"]["no_transcript"] == 0


async def test_a_writer_with_three_sessions_matches_via_the_second(
    actions: Actions, tmp_path: Path,
) -> None:
    """"Looked up, not searched": a writer with THREE session
    transcripts, none the freshest, carries the write's own receipt; the per-writer
    receipt index (built once, merging every one of the writer's resolvable files) must
    find it regardless of which sid is tried first, never give up after one miss."""
    now = datetime.now(UTC)
    writer = "agent:writer-three-sessions"
    obj = await actions.create_or_find_object("Agent", writer, "test")

    sid1 = "sidfirst1deadbeef"
    sid2 = "sidsecnd2deadbeef"
    sid3 = "sidthird3deadbeef"
    proj_dir = tmp_path / "-home-someone-code-testrepo"
    proj_dir.mkdir(parents=True, exist_ok=True)
    (proj_dir / f"{sid1}.jsonl").write_text(
        _tool_result(json.dumps({"canonical": "decision:unrelated111111"})) + "\n")
    (proj_dir / f"{sid2}.jsonl").write_text(
        _tool_result(json.dumps({"canonical": "decision:2222222aaaaa"})) + "\n")
    (proj_dir / f"{sid3}.jsonl").write_text(
        _tool_result(json.dumps({"canonical": "decision:unrelated333333"})) + "\n")

    # Distinct, explicitly ordered timestamps: sid1 freshest, sid3 oldest, so
    # `_anchor_sids`' own freshest-first ordering tries sid1, then sid2, then sid3;
    # the receipt sits only in the MIDDLE one.
    await actions.assert_property(
        obj, f"anchor_sid:{sid1[:8]}", sid1, "test",
        now, 0.9, evidence_class="direct_observation")
    await actions.assert_property(
        obj, f"anchor_sid:{sid2[:8]}", sid2, "test",
        now - timedelta(seconds=1), 0.9, evidence_class="direct_observation")
    await actions.assert_property(
        obj, f"anchor_sid:{sid3[:8]}", sid3, "test",
        now - timedelta(seconds=2), 0.9, evidence_class="direct_observation")

    decision = await actions.create_or_find_object(
        "Decision", "decision:2222222aaaaa", writer)
    await actions.assert_property(
        decision, "summary", "a write whose receipt sits in the middle session",
        writer, now, 0.9)

    report = await backfill_possible_upstream(
        actions, dry_run=True, transcript_root=tmp_path)

    assert report["edges_to_mint"] == 0  # decision:2222222aaaaa is itself the candidate
    assert report["summary"]["candidates"]["matched"] == 1
    assert report["summary"]["candidates"]["no_transcript"] == 0


async def test_a_10mb_transcript_streams_and_finds_the_receipt(
    actions: Actions, tmp_path: Path,
) -> None:
    """Under the cap, a real multi-megabyte file still streams correctly (never
    read_text()) and finds a receipt near the end of it."""
    now = datetime.now(UTC)
    upstream = await actions.create_or_find_object("Decision", "decision:1a2b3c4d5e6f", "x")
    filler = _tool_result(json.dumps({"note": "x" * 180})) + "\n"
    lines = [filler] * 55_000  # ~10MB of filler before the real content
    lines.append(_tool_result(json.dumps({"canonical": "decision:1a2b3c4d5e6f"})))
    lines.append(_tool_result(json.dumps({"canonical": "decision:6f5e4d3c2b1a"})))
    proj_dir = tmp_path / "-home-someone-code-testrepo"
    proj_dir.mkdir(parents=True, exist_ok=True)
    sid = "sid10mb01deadbeef"
    transcript = proj_dir / f"{sid}.jsonl"
    transcript.write_text("\n".join(lines) + "\n")
    assert transcript.stat().st_size > 9 * 1024 * 1024  # genuinely multi-MB, not a token file

    obj = await actions.create_or_find_object("Agent", "agent:writer-10mb", "test")
    await actions.assert_property(
        obj, f"anchor_sid:{sid[:8]}", sid, "test", now, 0.9,
        evidence_class="direct_observation")
    decision = await actions.create_or_find_object(
        "Decision", "decision:6f5e4d3c2b1a", "agent:writer-10mb")
    await actions.assert_property(
        decision, "summary", "a write near the end of a big real transcript",
        "agent:writer-10mb", now, 0.9)

    report = await backfill_possible_upstream(
        actions, dry_run=True, transcript_root=tmp_path, max_scan_bytes=64 * 1024 * 1024)

    assert report["edges_to_mint"] == 1
    assert report["plan"][0]["to"] == str(upstream)
    assert _sidecar_for(transcript).exists()  # the streaming pass cached its own index


async def test_the_sidecar_cache_is_used_on_a_second_visit(
    actions: Actions, tmp_path: Path,
) -> None:
    """A second call against an UNCHANGED transcript reuses the sidecar's own index
    instead of re-scanning, proven by corrupting the transcript's own content (while
    preserving its stat signature) between calls and confirming the cached answer
    survives the corruption."""
    now = datetime.now(UTC)
    upstream = await actions.create_or_find_object("Decision", "decision:aa11bb22cc33", "x")
    lines = [
        _tool_result(json.dumps({"canonical": "decision:aa11bb22cc33"})),
        _tool_result(json.dumps({"canonical": "decision:dd44ee55ff66"})),
    ]
    sid = "sidcache1deadbeef"
    transcript = await _mint_agent_with_sid(
        actions, "agent:writer-cache", sid, tmp_path, lines)
    decision = await actions.create_or_find_object(
        "Decision", "decision:dd44ee55ff66", "agent:writer-cache")
    await actions.assert_property(
        decision, "summary", "cached across a second visit",
        "agent:writer-cache", now, 0.9)

    first = await backfill_possible_upstream(actions, dry_run=True, transcript_root=tmp_path)
    assert first["edges_to_mint"] == 1

    import os

    st = transcript.stat()
    garbage = ("no receipt in here at all " * (st.st_size // 27 + 1))[:st.st_size]
    transcript.write_bytes(garbage.encode())  # SAME byte length, size+mtime both preserved
    os.utime(transcript, (st.st_atime, st.st_mtime))  # preserve the cache's own key

    second = await backfill_possible_upstream(actions, dry_run=True, transcript_root=tmp_path)
    assert second["edges_to_mint"] == 1  # still found, via the sidecar, not a re-scan
    assert second["plan"][0]["to"] == str(upstream)


async def test_a_tight_budget_returns_a_partial_receipt(
    actions: Actions, tmp_path: Path,
) -> None:
    """A caller must never be left waiting on a call that could hang;
    an exhausted wall-clock budget stops between candidates and says so, honestly."""
    now = datetime.now(UTC)
    for i in range(3):
        d = await actions.create_or_find_object(
            "Decision", f"decision:budget00000{i}", f"agent:writer-budget-{i}")
        await actions.assert_property(
            d, "summary", f"candidate {i}", f"agent:writer-budget-{i}", now, 0.9)

    report = await backfill_possible_upstream(
        actions, dry_run=True, transcript_root=tmp_path, budget_seconds=0.0)

    assert report["partial"] is True
    assert report["candidates_examined"] < report["candidates_total"]
