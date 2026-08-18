"""Cross-channel recovery (task #181, Thoth DM 5320): Ptah measured that during a routing
defect he and Ra sent 3 messages through osiris and ~24 through the harness's own
SendMessage tool — 90% of a day's reasoning existed only in two jsonl files. These tests
prove: the pure parser finds exactly the SendMessage blocks (nothing else); the repair
verb is dry-run-first, idempotent, and refuses an unattributed write; adoption_share
reports "unknown" (never a false zero) for a session that was never recovered.
"""
from __future__ import annotations

import json

import pytest
from src.actions.core import Actions
from src.ingest.cross_channel import (
    adoption_share,
    extract_harness_sends,
    recover_harness_exchanges,
)
from src.orchestrator.mounts import save_mount


def _line(**kw: object) -> str:
    return json.dumps(kw)


_ASSISTANT_WITH_SEND = _line(
    type="assistant", timestamp="2026-08-18T09:00:00Z",
    message={"content": [
        {"type": "text", "text": "routing around it"},
        {"type": "tool_use", "name": "SendMessage",
         "input": {"to": "adbf9df793f4d1264", "summary": "resume", "message": "pick up here"}},
    ]})
_ASSISTANT_PLAIN = _line(
    type="assistant", timestamp="2026-08-18T09:01:00Z",
    message={"content": [{"type": "text", "text": "just thinking out loud"}]})
_USER_TURN = _line(type="user", message={"content": "go"})
_ASSISTANT_MALFORMED_SEND = _line(
    type="assistant", timestamp="2026-08-18T09:02:00Z",
    message={"content": [
        {"type": "tool_use", "name": "SendMessage", "input": {"to": "abc123de"}},  # no message
    ]})


# --- extract_harness_sends: pure, no DB -----------------------------------------------------

def test_extract_finds_exactly_the_sendmessage_blocks() -> None:
    lines = [_ASSISTANT_WITH_SEND, _ASSISTANT_PLAIN, _USER_TURN]
    out = extract_harness_sends(lines)
    assert len(out) == 1
    assert out[0]["turn_index"] == 0
    assert out[0]["to"] == "adbf9df793f4d1264"
    assert out[0]["summary"] == "resume"
    assert out[0]["message"] == "pick up here"
    assert out[0]["observed_at"] == "2026-08-18T09:00:00Z"


def test_extract_skips_a_malformed_call_with_no_message() -> None:
    assert extract_harness_sends([_ASSISTANT_MALFORMED_SEND]) == []


def test_extract_skips_unparseable_lines_without_raising() -> None:
    out = extract_harness_sends(["not json at all", "", _ASSISTANT_WITH_SEND])
    assert len(out) == 1
    assert out[0]["to"] == "adbf9df793f4d1264"


def test_extract_never_reindexes_around_a_skipped_line() -> None:
    """turn_index must track the RAW line position, not the filtered output position —
    otherwise a recovered row's turn_index couldn't be joined back to soul_lines.line_idx."""
    lines = [_USER_TURN, _ASSISTANT_WITH_SEND]
    out = extract_harness_sends(lines)
    assert out[0]["turn_index"] == 1


# --- recover_harness_exchanges: dry-run-first repair verb ------------------------------------

async def _seed_soul(actions: Actions, anchor_sid: str, lines: list[str]) -> None:
    async with actions.pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(
                "INSERT INTO soul_lines (harness, anchor_sid, line_idx, raw_line, line_hash, "
                " prev_hash) VALUES ('claude-code', $1, $2, $3, $4, NULL)",
                [(anchor_sid, i, ln.encode(), f"hash{i}") for i, ln in enumerate(lines)])
            await conn.execute(
                "INSERT INTO soul_sessions (harness, anchor_sid, source_path, last_line_idx, "
                " last_hash) VALUES ('claude-code', $1, 'fake', $2, $3) "
                "ON CONFLICT (harness, anchor_sid) DO NOTHING",
                anchor_sid, len(lines), f"hash{len(lines) - 1}" if lines else None)


async def test_recover_reports_an_error_when_nothing_is_soul_stored(actions: Actions) -> None:
    out = await recover_harness_exchanges(actions.pool, "neverIngested")
    assert "error" in out
    assert "soul-store" in out["error"]


async def test_recover_dry_run_finds_without_writing(actions: Actions) -> None:
    await _seed_soul(actions, "anchorA1", [_ASSISTANT_WITH_SEND, _ASSISTANT_PLAIN])
    out = await recover_harness_exchanges(actions.pool, "anchorA1")
    assert out["found"] == 1
    assert out["would_write"] == 1
    assert out["already_recovered"] == 0
    n = await actions.pool.fetchval("SELECT count(*) FROM harness_messages")
    assert n == 0, "dry_run must never write"


async def test_recover_execute_requires_a_because(actions: Actions) -> None:
    await _seed_soul(actions, "anchorA2", [_ASSISTANT_WITH_SEND])
    out = await recover_harness_exchanges(actions.pool, "anchorA2", dry_run=False)
    assert "error" in out
    n = await actions.pool.fetchval("SELECT count(*) FROM harness_messages")
    assert n == 0


async def test_recover_execute_writes_and_is_idempotent(actions: Actions) -> None:
    await _seed_soul(actions, "anchorA3", [_ASSISTANT_WITH_SEND, _ASSISTANT_PLAIN])
    out = await recover_harness_exchanges(
        actions.pool, "anchorA3", dry_run=False, because="recovering the Ptah/Ra day")
    assert out["written"] == 1
    row = await actions.pool.fetchrow(
        "SELECT anchor_sid, turn_index, to_raw, summary, message FROM harness_messages "
        "WHERE anchor_sid='anchorA3'")
    assert row["turn_index"] == 0
    assert row["to_raw"] == "adbf9df793f4d1264"
    assert row["message"] == "pick up here"

    # re-running (still no new soul lines) writes nothing new — idempotent per (anchor, turn)
    out2 = await recover_harness_exchanges(
        actions.pool, "anchorA3", dry_run=False, because="re-run")
    assert out2["written"] == 0
    assert out2["already_recovered"] == 1
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM harness_messages WHERE anchor_sid='anchorA3'")
    assert n == 1


async def test_recover_resolves_to_raw_against_a_live_mount(actions: Actions) -> None:
    await save_mount(actions.pool, job_dir="/home/x/.osiris/seats/ra/adbf9df7",
                     agent_id="agent:ra-test", project="osiris", cwd="/x",
                     model="claude-sonnet-5", session_key=None)
    await _seed_soul(actions, "anchorA4", [_ASSISTANT_WITH_SEND])
    await recover_harness_exchanges(actions.pool, "anchorA4", dry_run=False, because="test")
    row = await actions.pool.fetchrow(
        "SELECT to_resolved FROM harness_messages WHERE anchor_sid='anchorA4'")
    assert row["to_resolved"] == "agent:ra-test"


# --- adoption_share: honest "unknown", never a false zero ------------------------------------

async def test_adoption_share_is_unknown_with_no_anchor(actions: Actions) -> None:
    out = await adoption_share(actions.pool, from_agent="agent:x", anchor_sid=None)
    assert out["harness_count"] is None
    assert out["recovered"] is False
    assert "share" not in out


async def test_adoption_share_is_unknown_when_the_session_was_never_recovered(
    actions: Actions,
) -> None:
    out = await adoption_share(actions.pool, from_agent="agent:x", anchor_sid="neverRecovered")
    assert out["harness_count"] == 0
    assert out["recovered"] is True  # the table WAS queried, it's genuinely zero
    assert out["osiris_count"] == 0


async def test_adoption_share_computes_the_real_split(actions: Actions) -> None:
    await _seed_soul(actions, "anchorB1",
                     [_ASSISTANT_WITH_SEND, _ASSISTANT_MALFORMED_SEND])
    # give the malformed one a real message too, via direct insert, to get 3 harness sends
    await actions.pool.execute(
        "INSERT INTO harness_messages (anchor_sid, turn_index, to_raw, message) "
        "VALUES ('anchorB1', 5, 'x', 'm1'), ('anchorB1', 6, 'x', 'm2')")
    await recover_harness_exchanges(actions.pool, "anchorB1", dry_run=False, because="test")
    await actions.pool.execute(
        "INSERT INTO fleet_messages (from_agent, to_project, body) "
        "VALUES ('agent:x', 'osiris', 'one osiris send')")
    out = await adoption_share(actions.pool, from_agent="agent:x", anchor_sid="anchorB1")
    assert out["osiris_count"] == 1
    assert out["harness_count"] == 3  # 2 direct-inserted + 1 recovered from _ASSISTANT_WITH_SEND
    assert out["share"] == pytest.approx(0.25)
