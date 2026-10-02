"""THE STALL's own fix, item 1: the provenance
backfill door no longer reads a transcript byte on osiris-mcp's own event loop thread:
`provenance_backfill_job` (src/workers/arq_worker.py) does the real work on
osiris-worker and posts the receipt as a thread annotation when it finishes. Calls the
job function directly, same pattern tests/test_sweep_ledger.py already uses for
`sweep_session`: a bare `{"pool": ...}` ctx, no real arq/redis involved."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.workers.arq_worker import provenance_backfill_job


def _tool_result(content: Any) -> str:
    return json.dumps({"type": "user", "message": {"content": [
        {"type": "tool_result", "content": content}]}})


async def _last_annotation(actions: Actions, thread_id: Any) -> str:
    rows = await actions.pool.fetch(
        "SELECT a.value #>> '{}' AS v FROM current_assertions a "
        "WHERE a.object_id = $1 AND a.name LIKE 'note:%' "
        "ORDER BY a.observed_at DESC LIMIT 1", thread_id)
    assert rows, f"no annotation found on thread {thread_id!r}"
    return str(rows[0]["v"])


async def test_provenance_backfill_job_posts_the_receipt_onto_the_named_thread(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.capture import open_thread

    t = await open_thread(actions, "a receipt lands here (test double)")
    thread_short = str(t)[:8]

    out = await provenance_backfill_job(
        {"pool": actions.pool}, dry_run=True, because=None, limit=5, newest_first=False,
        actor="agent:test-caller", receipt_ref=thread_short)

    assert out["dry_run"] is True
    note = await _last_annotation(actions, t)
    assert "WORKER RECEIPT" in note
    assert "agent:test-caller" in note
    assert "candidates_examined" in note


async def test_provenance_backfill_job_live_pass_reports_minted_in_the_receipt(
    actions: Actions, tmp_path: Path, monkeypatch: Any,
) -> None:
    from src.orchestrator.capture import open_thread

    t = await open_thread(actions, "a live-pass receipt lands here (test double)")
    thread_short = str(t)[:8]
    now = datetime.now(UTC)
    upstream = await actions.create_or_find_object("Decision", "decision:a0a0a0a0a0a0", "x")
    lines = [
        _tool_result(json.dumps({"canonical": "decision:a0a0a0a0a0a0"})),
        _tool_result(json.dumps({"canonical": "decision:b0b0b0b0b0b0"})),
    ]
    proj_dir = tmp_path / "-home-someone-code-testrepo"
    proj_dir.mkdir(parents=True, exist_ok=True)
    sid = "sidjob001deadbeef"
    (proj_dir / f"{sid}.jsonl").write_text("\n".join(lines) + "\n")
    obj = await actions.create_or_find_object("Agent", "agent:writer-job", "test")
    await actions.assert_property(
        obj, f"anchor_sid:{sid[:8]}", sid, "test", now, 0.9,
        evidence_class="direct_observation")
    decision = await actions.create_or_find_object(
        "Decision", "decision:b0b0b0b0b0b0", "agent:writer-job")
    await actions.assert_property(
        decision, "summary", "written by the job's own writer", "agent:writer-job", now, 0.9)

    from src.config import settings as settings_mod

    class _FakeSettings:
        osiris_transcripts = str(tmp_path)

    monkeypatch.setattr(settings_mod, "get_settings", lambda: _FakeSettings())

    out = await provenance_backfill_job(
        {"pool": actions.pool}, dry_run=False, because="testing the job's live pass",
        limit=5, newest_first=False, actor="agent:test-caller", receipt_ref=thread_short)

    assert out["minted"] == 1
    assert out["plan"][0]["to"] == str(upstream)
    note = await _last_annotation(actions, t)
    assert "minted=1" in note
