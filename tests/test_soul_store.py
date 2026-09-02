"""The soul store — verbatim ingest + hash chain (task #51 piece 1, ruling 62dc6397).

Proves the round trip against real Postgres (testcontainers): every line stored exactly
as written, the hash chain detects a gap or a tamper, incremental ingest resumes the
chain correctly, and re_materialize() reconstructs the source byte-for-byte — the
acceptance test this piece stands or falls on.
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest_asyncio
from src.actions.core import Actions
from src.ingest.soul_store import SoulStore, _chain_hash


def _write_transcript(path: Path, lines: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")
    return path


def _synthetic_lines(n: int) -> list[str]:
    out = []
    for i in range(n):
        ts = datetime(2026, 8, 17, 12, 0, i, tzinfo=UTC).isoformat()
        role = "assistant" if i % 2 else "user"
        out.append(json.dumps({"type": role, "timestamp": ts, "message": {"content": f"line {i}"}}))
    return out


@pytest_asyncio.fixture
async def store(actions: Actions) -> SoulStore:
    return SoulStore(actions.pool)


# --- pure hash chain -----------------------------------------------------

def test_chain_hash_is_deterministic_and_order_sensitive() -> None:
    a = _chain_hash(None, b"line0")
    b = _chain_hash(a, b"line1")
    assert a != b
    assert _chain_hash(None, b"line0") == a  # deterministic
    assert _chain_hash("line1", b"line0") != b  # prev_hash matters, not just content


# --- ingest + read back ---------------------------------------------------

async def test_ingest_stores_every_line_verbatim(store: SoulStore, tmp_path: Path) -> None:
    lines = _synthetic_lines(5)
    p = _write_transcript(tmp_path / "t.jsonl", lines)
    n = await store.ingest_path(str(p), "deadbeef")
    assert n == 5
    rows = await store.pool.fetch(
        "SELECT line_idx, raw_line FROM soul_lines WHERE harness='claude-code' "
        "AND anchor_sid='deadbeef' ORDER BY line_idx")
    assert [bytes(r["raw_line"]).decode() for r in rows] == lines
    assert [r["line_idx"] for r in rows] == list(range(5))


async def test_ingest_is_idempotent(store: SoulStore, tmp_path: Path) -> None:
    lines = _synthetic_lines(3)
    p = _write_transcript(tmp_path / "t.jsonl", lines)
    first = await store.ingest_path(str(p), "cafef00d")
    second = await store.ingest_path(str(p), "cafef00d")
    assert first == 3
    assert second == 0
    rows = await store.pool.fetch(
        "SELECT count(*) AS n FROM soul_lines WHERE harness='claude-code' "
        "AND anchor_sid='cafef00d'")
    assert rows[0]["n"] == 3


async def test_ingest_resumes_the_chain_incrementally(store: SoulStore, tmp_path: Path) -> None:
    """Appending to the source and re-ingesting continues the chain from last_hash,
    never re-hashing already-stored lines."""
    p = _write_transcript(tmp_path / "t.jsonl", _synthetic_lines(2))
    await store.ingest_path(str(p), "feedface")
    fuller = _synthetic_lines(2) + _synthetic_lines(4)[2:]
    p.write_text("\n".join(fuller) + "\n")
    added = await store.ingest_path(str(p), "feedface")
    assert added == 2
    assert await store.verify_chain("feedface") is True
    materialized = await store.re_materialize("feedface")
    assert materialized == "\n".join(fuller)


# --- integrity: verify_chain ----------------------------------------------

async def test_verify_chain_true_on_a_clean_ingest(store: SoulStore, tmp_path: Path) -> None:
    p = _write_transcript(tmp_path / "t.jsonl", _synthetic_lines(6))
    await store.ingest_path(str(p), "0ff1ce00")
    assert await store.verify_chain("0ff1ce00") is True


async def test_verify_chain_false_on_a_tampered_line(store: SoulStore, tmp_path: Path) -> None:
    p = _write_transcript(tmp_path / "t.jsonl", _synthetic_lines(4))
    await store.ingest_path(str(p), "b16b00b5")
    await store.pool.execute(
        "UPDATE soul_lines SET raw_line=E'TAMPERED'::bytea "
        "WHERE harness='claude-code' AND anchor_sid='b16b00b5' AND line_idx=1")
    assert await store.verify_chain("b16b00b5") is False


async def test_verify_chain_false_on_a_gap(store: SoulStore, tmp_path: Path) -> None:
    p = _write_transcript(tmp_path / "t.jsonl", _synthetic_lines(4))
    await store.ingest_path(str(p), "5ca1ab1e")
    await store.pool.execute(
        "DELETE FROM soul_lines WHERE harness='claude-code' AND anchor_sid='5ca1ab1e' "
        "AND line_idx=2")
    assert await store.verify_chain("5ca1ab1e") is False


async def test_verify_chain_true_on_zero_rows(store: SoulStore) -> None:
    assert await store.verify_chain("never-ingested") is True


# --- re_materialize: the acceptance test -----------------------------------

async def test_re_materialize_byte_compares_against_the_source(
    store: SoulStore, tmp_path: Path,
) -> None:
    lines = _synthetic_lines(9)
    p = _write_transcript(tmp_path / "t.jsonl", lines)
    await store.ingest_path(str(p), "decafbad")
    materialized = await store.re_materialize("decafbad")
    assert materialized is not None
    # source carries a trailing newline the join form doesn't reproduce — the documented
    # "mod a trailing newline" equivalence
    assert p.read_text() == materialized + "\n"
    assert materialized == "\n".join(lines)


async def test_re_materialize_none_when_never_ingested(store: SoulStore) -> None:
    assert await store.re_materialize("ghost0000") is None


# --- ingest() via the harness adapter door ----------------------------------

async def test_ingest_via_discover_finds_and_eats_the_real_locator(
    store: SoulStore, tmp_path: Path,
) -> None:
    sid = "aabbccdd"
    projects = tmp_path / "projects"
    proj_dir = projects / "-home-x-code-widget"
    lines = _synthetic_lines(3)
    _write_transcript(proj_dir / f"{sid}-session-uuid.jsonl", lines)
    n = await store.ingest(
        cwd="/home/x/code/widget", job_dir=f"/home/x/.claude/jobs/{sid}", root=projects)
    assert n == 3
    materialized = await store.re_materialize(sid)
    assert materialized == "\n".join(lines)


async def test_ingest_via_discover_returns_zero_when_nothing_found(
    store: SoulStore, tmp_path: Path,
) -> None:
    n = await store.ingest(cwd="/nowhere", job_dir="/x/jobs/deadbeef", root=tmp_path)
    assert n == 0


# --- rematerialize_to_disk (task #51 piece 2) -------------------------------

def _fixture_with_compaction_boundary() -> list[str]:
    """A realistic mix including a compaction-summary line — the exact shape piece 2's
    dispatch named: 1:1 must survive a compaction boundary untouched, no filtering."""
    lines = []
    for i in range(3):
        ts = datetime(2026, 8, 17, 9, 0, i, tzinfo=UTC).isoformat()
        lines.append(json.dumps({"type": "user" if i % 2 == 0 else "assistant",
                                 "timestamp": ts, "message": {"content": f"turn {i}"}}))
    lines.append(json.dumps({
        "type": "user", "isCompactSummary": True,
        "timestamp": datetime(2026, 8, 17, 9, 0, 3, tzinfo=UTC).isoformat(),
        "message": {"content": "the whole history replayed, compacted"}}))
    for i in range(4, 6):
        ts = datetime(2026, 8, 17, 9, 0, i, tzinfo=UTC).isoformat()
        lines.append(json.dumps({"type": "assistant", "timestamp": ts,
                                 "message": {"content": f"turn {i}", "model": "claude-opus-5"}}))
    return lines


async def test_rematerialize_to_disk_is_byte_identical_across_a_compaction_boundary(
    store: SoulStore, tmp_path: Path,
) -> None:
    """THE CONTRACT TEST (piece 2's own acceptance bar): ingest -> rematerialize ->
    sha256 equal, on a fixture including a compaction boundary — 1:1 means no
    filtering, ever, not even around the one line type the OTHER store (harness_turns)
    treats specially for its own token math."""
    lines = _fixture_with_compaction_boundary()
    source = _write_transcript(tmp_path / "source" / "c0mpac7d-session.jsonl", lines)
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()

    n = await store.ingest_path(str(source), "c0mpac7d")
    assert n == len(lines)

    dest = tmp_path / "recovered" / "c0mpac7d-session.jsonl"
    receipt = await store.rematerialize_to_disk("c0mpac7d", dest=str(dest))
    assert "error" not in receipt
    assert receipt["lines"] == len(lines)
    assert dest.read_bytes() == source.read_bytes()
    assert hashlib.sha256(dest.read_bytes()).hexdigest() == source_sha
    assert receipt["sha256"] == hashlib.sha256(dest.read_text().encode()).hexdigest()


async def test_ingest_survives_an_embedded_nul_byte(store: SoulStore, tmp_path: Path) -> None:
    """THE ACCEPTANCE TEST FOR 0052 (thread 173cbf11, Thoth DM 5350): a real transcript
    line carrying a literal NUL byte — Postgres `text` cannot hold 0x00 at all, confirmed
    live on both of Thoth's own named transcripts (Ptah 4780, Ra 18591 NUL bytes).
    Ingest must not raise, the hash chain must verify clean, and rematerialize must
    reproduce the exact bytes, NUL included — `bytea` end to end is the whole fix."""
    lines = _synthetic_lines(2)
    poisoned = json.dumps({"type": "user", "message": {"content": "binary garbage: "}})
    poisoned_bytes = poisoned.encode() + b"\x00\x00\x00binary tail"
    source = tmp_path / "poisoned.jsonl"
    source.parent.mkdir(parents=True, exist_ok=True)
    content = "\n".join(lines).encode() + b"\n" + poisoned_bytes + b"\n"
    source.write_bytes(content)

    n = await store.ingest_path(str(source), "nu11byte")
    assert n == 3
    assert await store.verify_chain("nu11byte") is True

    rows = await store.raw_lines("nu11byte")
    assert rows is not None and rows[2] == poisoned_bytes.decode()

    dest = tmp_path / "recovered.jsonl"
    receipt = await store.rematerialize_to_disk("nu11byte", dest=str(dest))
    assert "error" not in receipt
    assert dest.read_bytes() == content
    assert dest.read_bytes().count(b"\x00") == 3


async def test_rematerialize_to_disk_defaults_dest_to_the_recorded_source_path(
    store: SoulStore, tmp_path: Path,
) -> None:
    """No `dest` given -> writes to soul_sessions' own recorded source_path, the
    harness's projects-slug convention — so `claude --resume` on any host finds it
    where a live session would have."""
    lines = _synthetic_lines(3)
    source = _write_transcript(tmp_path / "orig" / "5ee7f0d5-session.jsonl", lines)
    os.remove(source)  # the ORIGINAL is gone — this IS the "any host" scenario
    # ingest normally via a temp copy, THEN delete it, to exercise the real path
    tmp_copy = tmp_path / "tmp-ingest.jsonl"
    _write_transcript(tmp_copy, lines)
    await store.ingest_path(str(tmp_copy), "5ee7f0d5")
    await store.pool.execute(
        "UPDATE soul_sessions SET source_path=$1 WHERE harness='claude-code' "
        "AND anchor_sid='5ee7f0d5'", str(source))

    receipt = await store.rematerialize_to_disk("5ee7f0d5")
    assert receipt["written"] == str(source)
    assert source.exists()
    assert source.read_text() == "\n".join(lines) + "\n"


async def test_rematerialize_to_disk_refuses_a_live_transcript(
    store: SoulStore, tmp_path: Path,
) -> None:
    """A destination modified more recently than the store's last ingest is a LIVE
    transcript — refuse, name why, touch nothing."""
    lines = _synthetic_lines(3)
    source = _write_transcript(tmp_path / "s" / "a11ce00b-session.jsonl", lines)
    await store.ingest_path(str(source), "a11ce00b")

    dest = tmp_path / "d" / "target.jsonl"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("someone else's newer content\n")
    # force the dest's mtime strictly ahead of last_ingested_at
    future = datetime.now(UTC).timestamp() + 3600
    os.utime(dest, (future, future))

    receipt = await store.rematerialize_to_disk("a11ce00b", dest=str(dest))
    assert receipt.get("error", "").startswith("refused — a LIVE transcript")
    assert dest.read_text() == "someone else's newer content\n"  # untouched


async def test_rematerialize_to_disk_force_overwrites_a_live_transcript(
    store: SoulStore, tmp_path: Path,
) -> None:
    lines = _synthetic_lines(3)
    source = _write_transcript(tmp_path / "s" / "1eaf1eaf-session.jsonl", lines)
    await store.ingest_path(str(source), "1eaf1eaf")

    dest = tmp_path / "d" / "target.jsonl"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("stale content\n")
    future = datetime.now(UTC).timestamp() + 3600
    os.utime(dest, (future, future))

    receipt = await store.rematerialize_to_disk("1eaf1eaf", dest=str(dest), force=True)
    assert "error" not in receipt
    assert dest.read_text() == "\n".join(lines) + "\n"


async def test_rematerialize_to_disk_writes_nothing_on_a_broken_chain(
    store: SoulStore, tmp_path: Path,
) -> None:
    """A break is a NAMED state, never a silent partial file — the destination must not
    even be CREATED when the chain fails verification."""
    lines = _synthetic_lines(5)
    source = _write_transcript(tmp_path / "s" / "b0e0be00-session.jsonl", lines)
    await store.ingest_path(str(source), "b0e0be00")
    await store.pool.execute(
        "UPDATE soul_lines SET raw_line=E'TAMPERED'::bytea WHERE harness='claude-code' "
        "AND anchor_sid='b0e0be00' AND line_idx=2")

    dest = tmp_path / "d" / "target.jsonl"
    receipt = await store.rematerialize_to_disk("b0e0be00", dest=str(dest))
    assert "error" in receipt
    assert receipt["verified_through"] == 1
    assert not dest.exists()


async def test_rematerialize_to_disk_errors_when_nothing_ingested(
    store: SoulStore, tmp_path: Path,
) -> None:
    receipt = await store.rematerialize_to_disk("neverseen", dest=str(tmp_path / "x.jsonl"))
    assert "error" in receipt
    assert not (tmp_path / "x.jsonl").exists()


# --- the MCP tool wrapper (thin passthrough, same shape as correct_pin_value's own
# self-scoped test in test_seats.py) -----------------------------------------------

async def test_rematerialize_mcp_tool_wraps_the_same_verb(
    actions: Actions, tmp_path: Path,
) -> None:
    from src import mcp_server as srv

    lines = _synthetic_lines(2)
    source = _write_transcript(tmp_path / "s" / "mcptool1-session.jsonl", lines)
    await SoulStore(actions.pool).ingest_path(str(source), "mcptool1")

    dest = tmp_path / "d" / "out.jsonl"
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.rematerialize("mcptool1", dest=str(dest))
    finally:
        srv._pool = saved_pool
    assert out["written"] == str(dest)
    assert out["lines"] == 2
    assert dest.read_text() == "\n".join(lines) + "\n"


# --- raw_lines / mining_view (task #51 piece 3) -----------------------------

async def test_raw_lines_matches_splitlines_of_the_source(
    store: SoulStore, tmp_path: Path,
) -> None:
    lines = _synthetic_lines(4)
    source = _write_transcript(tmp_path / "t.jsonl", lines)
    await store.ingest_path(str(source), "ba5eba11")
    assert await store.raw_lines("ba5eba11") == source.read_text().splitlines()


async def test_raw_lines_none_when_never_ingested(store: SoulStore) -> None:
    assert await store.raw_lines("neverseen1") is None


async def test_mining_view_extracts_role_text_and_tool_calls(
    store: SoulStore, tmp_path: Path,
) -> None:
    lines = [
        json.dumps({"type": "user", "message": {"content": "please check the logs"}}),
        json.dumps({"type": "assistant", "message": {"content": [
            {"type": "thinking", "thinking": "let me look"},
            {"type": "tool_use", "name": "Bash", "input": {"command": "ls"}},
            {"type": "text", "text": "found it"},
        ]}}),
        json.dumps({"type": "user", "isSidechain": True,
                   "message": {"content": "a subagent's own turn — out of scope"}}),
        json.dumps({"type": "user", "isCompactSummary": True,
                   "message": {"content": "the whole history replayed"}}),
    ]
    source = _write_transcript(tmp_path / "t.jsonl", lines)
    await store.ingest_path(str(source), "0b5e7ab1")
    view = await store.mining_view("0b5e7ab1")
    assert view is not None
    assert len(view) == 2  # sidechain + compaction-summary skipped
    assert view[0] == {"session": "0b5e7ab1", "turn_index": 0, "role": "user",
                       "text": "please check the logs", "tool_calls": []}
    assert view[1]["role"] == "assistant"
    assert view[1]["turn_index"] == 1
    assert view[1]["text"] == "found it"  # thinking block skipped, matching distill()
    assert view[1]["tool_calls"] == [{"name": "Bash", "input": {"command": "ls"}}]


async def test_mining_view_none_when_never_ingested(store: SoulStore) -> None:
    assert await store.mining_view("neverseen2") is None


# --- backfill (task #51 piece 1, Lane 1 msg 6527/ruling ba329ccb) -----------

async def test_backfill_discovers_and_ingests_every_real_session(
    store: SoulStore, tmp_path: Path,
) -> None:
    projects = tmp_path / "projects"
    lines_a = _synthetic_lines(3)
    lines_b = _synthetic_lines(2)
    _write_transcript(projects / "-home-x-code-widget" / "aaaaaaaa-session.jsonl", lines_a)
    _write_transcript(projects / "-home-x-code-gadget" / "bbbbbbbb-session.jsonl", lines_b)
    counts = await store.backfill(root=projects)
    assert sum(counts.values()) == 2  # two sessions touched, across both project dirs
    assert await store.re_materialize("aaaaaaaa") == "\n".join(lines_a)
    assert await store.re_materialize("bbbbbbbb") == "\n".join(lines_b)


async def test_backfill_is_a_stat_only_noop_on_a_second_call(
    store: SoulStore, tmp_path: Path,
) -> None:
    """The spend gate: a source whose mtime hasn't moved since our own last_ingested_at
    is skipped by stat + row lookup alone, never opened — the same law
    transcript_store.py's sibling backfill already runs on this house's other store."""
    projects = tmp_path / "projects"
    _write_transcript(projects / "-home-x-code-widget" / "cccccccc-session.jsonl",
                       _synthetic_lines(4))
    first = await store.backfill(root=projects)
    assert sum(first.values()) == 1
    second = await store.backfill(root=projects)
    assert sum(second.values()) == 0  # nothing changed — skipped, not re-ingested-to-zero


async def test_backfill_resumes_a_session_that_grew_between_ticks(
    store: SoulStore, tmp_path: Path,
) -> None:
    projects = tmp_path / "projects"
    p = _write_transcript(
        projects / "-home-x-code-widget" / "dddddddd-session.jsonl", _synthetic_lines(2))
    await store.backfill(root=projects)
    fuller = _synthetic_lines(2) + _synthetic_lines(5)[2:]
    p.write_text("\n".join(fuller) + "\n")
    second = await store.backfill(root=projects)
    assert sum(second.values()) == 1  # the grown session counts as touched again
    assert await store.re_materialize("dddddddd") == "\n".join(fuller)


async def test_backfill_survives_one_bad_session_among_several(
    store: SoulStore, tmp_path: Path,
) -> None:
    """A vanished/unreadable file must not abort the sweep — the next locator still
    gets ingested (matches TranscriptStore.backfill's own per-session try/except)."""
    projects = tmp_path / "projects"
    good = _write_transcript(
        projects / "-home-x-code-widget" / "eeeeeeee-session.jsonl", _synthetic_lines(2))
    bad_dir = projects / "-home-x-code-ghost"
    bad_dir.mkdir(parents=True)
    (bad_dir / "ffffffff-session.jsonl").symlink_to(bad_dir / "does-not-exist")
    counts = await store.backfill(root=projects)
    assert sum(counts.values()) == 1
    assert await store.re_materialize("eeeeeeee") == good.read_text().rstrip("\n")
