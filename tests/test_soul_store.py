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
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
import pytest_asyncio
from src.actions.core import Actions
from src.ingest.soul_store import (
    SoulStore,
    _addressable_entries,
    _chain_hash,
    _split_lines,
    verify_jsonl_chain_boundary,
)


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


async def test_a_checkpoint_failure_rolls_back_its_own_batchs_soul_lines_too(
    store: SoulStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE CHECKPOINT RACE FIX (thread ce3ddbb6): before this, a batch's soul_lines
    INSERT and the soul_sessions upsert (`_checkpoint`) ran as two SEPARATE
    transactions on two separate connections — a process death between them left
    orphaned soul_lines rows with no soul_sessions row at all, so the store read the
    session as never-ingested despite holding its content (confirmed live:
    a39e60d9e4fbd5292 sat with 89 orphaned rows for hours). Now they share one
    transaction: forcing the checkpoint to fail must roll back that batch's soul_lines
    rows too — the atomicity guarantee this fix actually buys, proven by making the
    SECOND write fail and confirming the FIRST didn't survive it either."""
    p = _write_transcript(tmp_path / "t.jsonl", _synthetic_lines(3))

    async def _boom(*_a: object, **_kw: object) -> None:
        raise RuntimeError("simulated checkpoint failure")

    monkeypatch.setattr(SoulStore, "_checkpoint", _boom)
    with pytest.raises(RuntimeError, match="simulated checkpoint failure"):
        await store.ingest_path(str(p), "raceboom01")

    lines = await store.pool.fetch(
        "SELECT 1 FROM soul_lines WHERE harness='claude-code' AND anchor_sid=$1",
        "raceboom01")
    assert lines == []  # the batch's own soul_lines rows rolled back with the checkpoint
    session = await store.pool.fetchrow(
        "SELECT 1 FROM soul_sessions WHERE harness='claude-code' AND anchor_sid=$1",
        "raceboom01")
    assert session is None


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


# --- thread 78efd46d item 2: the round-trip proof -----------------------------------

async def test_verify_round_trip_sample_is_clean_on_a_healthy_ingest(
    store: SoulStore, tmp_path: Path,
) -> None:
    p = _write_transcript(tmp_path / "roundtrip1.jsonl", _synthetic_lines(4))
    await store.ingest_path(str(p), "r0undtr1p")
    report = await store.verify_round_trip_sample()
    assert report.failures == []
    assert report.skipped_live == 0
    assert bool(report) is False


async def test_verify_round_trip_sample_skips_a_session_whose_file_is_gone(
    store: SoulStore, tmp_path: Path,
) -> None:
    """Pruned/moved/archived since ingest is item 4's own concern (cache with a
    budget), never proof the store's own content is wrong — the round-trip proof only
    ever speaks to sessions it can actually compare against something."""
    p = _write_transcript(tmp_path / "gone.jsonl", _synthetic_lines(3))
    await store.ingest_path(str(p), "va n1shed0")
    p.unlink()
    report = await store.verify_round_trip_sample()
    assert report.failures == []
    assert report.skipped_live == 0


async def test_verify_round_trip_sample_skips_a_live_session_never_as_a_failure(
    store: SoulStore, tmp_path: Path,
) -> None:
    """A file touched AFTER the store's last ingest is a moving target, not proof of
    a defect (Thoth's ruling off the 2026-09-08 full sweep: 2 of 5 raw mismatches
    were exactly this shape) — counted in skipped_live, never in failures."""
    p = _write_transcript(tmp_path / "live1.jsonl", _synthetic_lines(3))
    await store.ingest_path(str(p), "l1vesess1")
    await store.pool.execute(
        "UPDATE soul_sessions SET last_ingested_at = now() - interval '1 hour' "
        "WHERE harness='claude-code' AND anchor_sid='l1vesess1'")
    report = await store.verify_round_trip_sample()
    assert report.failures == []
    assert report.skipped_live == 1


async def test_verify_round_trip_sample_catches_a_tampered_session(
    store: SoulStore, tmp_path: Path,
) -> None:
    p = _write_transcript(tmp_path / "tampered.jsonl", _synthetic_lines(4))
    await store.ingest_path(str(p), "tamper3d1")
    await store.pool.execute(
        "UPDATE soul_lines SET raw_line=E'TAMPERED'::bytea "
        "WHERE harness='claude-code' AND anchor_sid='tamper3d1' AND line_idx=1")
    report = await store.verify_round_trip_sample()
    failures = report.failures
    assert len(failures) == 1
    assert report.skipped_live == 0
    assert failures[0]["anchor_sid"] == "tamper3d1"
    assert "error" in failures[0]


async def test_hash_file_streamed_matches_a_plain_whole_file_hash(tmp_path: Path) -> None:
    from src.ingest.soul_store import _hash_file_streamed

    p = tmp_path / "x.bin"
    p.write_bytes(b"some content spanning more than one chunk" * 100)
    assert _hash_file_streamed(p, chunk_size=16) == hashlib.sha256(p.read_bytes()).hexdigest()


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


async def test_ingest_never_stores_a_trailing_unterminated_line(
    store: SoulStore, tmp_path: Path,
) -> None:
    """THE LIVE-WRITE SAFETY GUARD (msg 6583, Jesus resumed and appending mid-lane): a
    session mid-write of its own last line leaves that line on disk with no trailing
    `\\n` yet. Ingesting it as if complete would bake a half-written JSON object
    permanently into the chain at that line_idx. The fix is structural (never trust an
    unterminated tail), not an occupancy check."""
    complete = _synthetic_lines(2)
    p = tmp_path / "live.jsonl"
    partial = json.dumps({"type": "user", "message": {"content": "still writ"}})
    p.write_bytes(("\n".join(complete) + "\n").encode() + partial.encode())  # NO trailing \n
    n = await store.ingest_path(str(p), "11ff11ff")
    assert n == 2  # the partial third line is NOT ingested
    rows = await store.raw_lines("11ff11ff")
    assert rows == complete

    # the write "finishes" — the file now ends in a real newline
    full_line = partial + " and now done"
    p.write_bytes(("\n".join(complete) + "\n").encode() + full_line.encode() + b"\n")
    added = await store.ingest_path(str(p), "11ff11ff")
    assert added == 1  # exactly the one completed line, never re-ingesting the first two
    rows = await store.raw_lines("11ff11ff")
    assert rows == [*complete, full_line]
    assert await store.verify_chain("11ff11ff") is True


async def test_ingest_returns_zero_when_the_only_content_is_an_unterminated_line(
    store: SoulStore, tmp_path: Path,
) -> None:
    """A brand-new file, mid-write of its very first line — nothing complete yet."""
    p = tmp_path / "brandnew.jsonl"
    p.write_bytes(b'{"type": "user", "message": {"content": "not done ye')  # no \n at all
    n = await store.ingest_path(str(p), "0a11a11a")
    assert n == 0
    assert await store.raw_lines("0a11a11a") is None


# --- streaming (msg 6583, the 307MB question) --------------------------------

async def test_ingest_streams_across_several_batches(
    store: SoulStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forces a tiny batch size so a modest line count still spans several INSERT +
    checkpoint round trips — proving the batch loop itself, not just its single-batch
    fallback (every other test in this file uses too few lines to exercise it)."""
    import src.ingest.soul_store as soul_store_mod

    monkeypatch.setattr(soul_store_mod, "_INGEST_BATCH_LINES", 3)
    lines = _synthetic_lines(11)  # 4 batches: 3+3+3+2
    p = _write_transcript(tmp_path / "big.jsonl", lines)
    n = await store.ingest_path(str(p), "57ea3157")
    assert n == 11
    assert await store.verify_chain("57ea3157") is True
    assert await store.re_materialize("57ea3157") == "\n".join(lines)

    # growing the file and re-ingesting still resumes correctly across a batch boundary
    fuller = lines + _synthetic_lines(15)[11:]
    p.write_text("\n".join(fuller) + "\n")
    added = await store.ingest_path(str(p), "57ea3157")
    assert added == 4
    assert await store.verify_chain("57ea3157") is True
    assert await store.re_materialize("57ea3157") == "\n".join(fuller)


async def test_rematerialize_to_disk_streams_across_several_pages(
    store: SoulStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forces a tiny page size on the READ side too — proving `_stream_verified_write`'s
    own multi-page loop reconstructs byte-identical content, not just its single-page
    fallback."""
    import src.ingest.soul_store as soul_store_mod

    monkeypatch.setattr(soul_store_mod, "_REMATERIALIZE_PAGE_LINES", 4)
    lines = _synthetic_lines(13)  # 4 pages: 4+4+4+1
    source = _write_transcript(tmp_path / "s" / "fee1600d-session.jsonl", lines)
    await store.ingest_path(str(source), "fee1600d")

    dest = tmp_path / "d" / "target.jsonl"
    receipt = await store.rematerialize_to_disk("fee1600d", dest=str(dest))
    assert receipt["lines"] == 13
    assert dest.read_bytes() == source.read_bytes()
    assert receipt["sha256"] == hashlib.sha256(source.read_bytes()).hexdigest()


async def test_rematerialize_to_disk_leaves_no_temp_file_on_a_broken_chain(
    store: SoulStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The streaming rewrite writes to a sibling temp file before the atomic rename —
    a broken chain must discard that temp file too, not just refuse to create `dest`."""
    import src.ingest.soul_store as soul_store_mod

    monkeypatch.setattr(soul_store_mod, "_REMATERIALIZE_PAGE_LINES", 2)
    lines = _synthetic_lines(6)
    source = _write_transcript(tmp_path / "s" / "7ee7f11e-session.jsonl", lines)
    await store.ingest_path(str(source), "7ee7f11e")
    await store.pool.execute(
        "UPDATE soul_lines SET raw_line=E'TAMPERED'::bytea WHERE harness='claude-code' "
        "AND anchor_sid='7ee7f11e' AND line_idx=4")

    dest_dir = tmp_path / "d"
    dest = dest_dir / "target.jsonl"
    receipt = await store.rematerialize_to_disk("7ee7f11e", dest=str(dest))
    assert "error" in receipt
    assert not dest.exists()
    assert dest_dir.is_dir()  # created for the temp writer, but holds nothing
    assert list(dest_dir.iterdir()) == []


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


# --- heal_seat_transcript (the jesus/chad repair, made a callable MCP door) -------------

class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


def _mount(srv: object, actions: Actions, agent_id: str, session: str) -> object:
    from src.orchestrator.agents import AgentIdentity

    ctx = _Ctx()
    ident = AgentIdentity(agent_id=agent_id, session=session, project="osiris",
                          model="claude-sonnet-5", cwd=None, model_method="job_dir",
                          model_history=("claude-sonnet-5",))
    key = srv._conn_key(ctx)
    srv._agents[key] = ident
    return ctx


async def test_heal_seat_transcript_refuses_without_mount(actions: Actions) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.heal_seat_transcript("someseat", ["/a.jsonl", "/b.jsonl"])
    finally:
        srv._pool = saved_pool
    assert "mount first" in out["error"]


async def test_heal_seat_transcript_refuses_fewer_than_two_sources(
    actions: Actions,
) -> None:
    from src import mcp_server as srv

    ctx = _mount(srv, actions, "agent:heal1", "heal1")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.heal_seat_transcript("someseat", ["/only-one.jsonl"], ctx=ctx)
    finally:
        srv._pool = saved_pool
    assert "at least two fragments" in out["error"]


async def test_heal_seat_transcript_dry_run_reports_clean_preflight_and_writes_nothing(
    actions: Actions, tmp_path: Path,
) -> None:
    from src import mcp_server as srv

    lines = _chained_lines(4, seed="healdry")
    sid = "healdry0-0000-0000-0000-000000000001"  # not a real chained uuid, real filename shape
    a = _write_transcript(tmp_path / f"{sid}.jsonl", lines[:2])
    b = _write_transcript(tmp_path / "b" / "irrelevant-name.jsonl", lines[2:])

    ctx = _mount(srv, actions, "agent:heal2", "heal2")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.heal_seat_transcript("dryhandle", [str(a), str(b)], ctx=ctx)
    finally:
        srv._pool = saved_pool
    assert out["dry_run"] is True
    assert "error" not in out
    assert all(p["clean"] for p in out["preflight"])
    assert not Path(out["office_dest"]).exists()


async def test_heal_seat_transcript_dry_run_reports_a_refused_pair(
    actions: Actions, tmp_path: Path,
) -> None:
    from src import mcp_server as srv

    lines_a = _chained_lines(2, seed="healrefa")
    lines_b = _chained_lines(2, seed="healrefb")  # unrelated — not a real continuation
    sid = "healrefa-0000-0000-0000-000000000002"
    a = _write_transcript(tmp_path / f"{sid}.jsonl", lines_a)
    b = _write_transcript(tmp_path / "b.jsonl", lines_b)

    ctx = _mount(srv, actions, "agent:heal3", "heal3")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.heal_seat_transcript("refusehandle", [str(a), str(b)], ctx=ctx)
    finally:
        srv._pool = saved_pool
    assert "error" in out
    assert any(not p["clean"] for p in out["preflight"])


async def test_heal_seat_transcript_execute_requires_because(
    actions: Actions, tmp_path: Path,
) -> None:
    from src import mcp_server as srv

    lines = _chained_lines(4, seed="healnoreason")
    sid = "healnorea-0000-0000-0000-00000000003"
    a = _write_transcript(tmp_path / f"{sid}.jsonl", lines[:2])
    b = _write_transcript(tmp_path / "b.jsonl", lines[2:])

    ctx = _mount(srv, actions, "agent:heal4", "heal4")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.heal_seat_transcript(
            "noreasonhandle", [str(a), str(b)], dry_run=False, ctx=ctx)
    finally:
        srv._pool = saved_pool
    assert "because is required" in out["error"]


async def test_heal_seat_transcript_execute_splices_and_writes_the_office_slug(
    actions: Actions, tmp_path: Path,
) -> None:
    from src import mcp_server as srv
    from src.orchestrator.offices import _default_office_root

    lines = _chained_lines(6, seed="healexec")
    sid = "healexec-0000-0000-0000-000000000004"
    a = _write_transcript(tmp_path / f"{sid}.jsonl", lines[:3])
    b = _write_transcript(tmp_path / "b.jsonl", lines[3:])

    ctx = _mount(srv, actions, "agent:heal5", "heal5")
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.heal_seat_transcript(
            "ExecHandle", [str(a), str(b)], dry_run=False, because="repair", ctx=ctx)
    finally:
        srv._pool = saved_pool
    assert "error" not in out
    assert out["spliced_lines"] == 6
    assert out["verify_chain"] is True
    dest = _default_office_root() / "exechandle" / f"{sid}.jsonl"
    assert out["rematerialize"]["written"] == str(dest)
    assert dest.read_bytes() == ("\n".join(lines) + "\n").encode()


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


# --- a non-claude-code harness round-trips (thread 6483/6587, the recurring catch) -----
#
# `ingest_path`/`splice_sources` have always accepted a real `harness` — but every
# READ-back door (verify_chain, re_materialize, raw_lines, mining_view,
# _stream_verified_write, rematerialize_to_disk) at one point or another hardcoded the
# module constant `_HARNESS = "claude-code"` instead of taking the caller's own value.
# Found once (Seshat, msg 6483/6587), fixed (Khnum), silently REGRESSED by Imhotep's
# streaming rewrite (verify_chain + the new _stream_verified_write both reverted to the
# constant), found again while reconciling khnum-splice-seek against that rewrite. Zero
# tests asserted the round trip for any harness other than 'claude-code' through any of
# this — every one of the three regressions shipped past a green suite. This is that
# test: ingest under a non-claude-code harness and prove every read-back door honors it.

async def test_non_claude_code_harness_round_trips_through_every_read_door(
    store: SoulStore, tmp_path: Path,
) -> None:
    lines = _synthetic_lines(5)
    source = _write_transcript(tmp_path / "d5h0000d-session.jsonl", lines)
    n = await store.ingest_path(str(source), "d5h0000d", harness="dsh")
    assert n == 5

    assert await store.verify_chain("d5h0000d", harness="dsh") is True
    assert await store.re_materialize("d5h0000d", harness="dsh") == "\n".join(lines)
    assert await store.raw_lines("d5h0000d", harness="dsh") == lines
    mined = await store.mining_view("d5h0000d", harness="dsh")
    assert mined is not None and len(mined) > 0

    dest = tmp_path / "out" / "target.jsonl"
    receipt = await store.rematerialize_to_disk("d5h0000d", dest=str(dest), harness="dsh")
    assert receipt["lines"] == 5
    assert dest.read_bytes() == source.read_bytes()


async def test_non_claude_code_harness_ignored_reports_never_ingested(
    store: SoulStore, tmp_path: Path,
) -> None:
    """THE EXACT LIE the bug tells: a caller that forgets (or a regression that drops)
    the harness kwarg on read-back sees a session that was genuinely, fully ingested as
    though it had never been touched at all — not an error, a silent false negative. This
    locks in the CONTRAST as the regression test: same anchor_sid, same store, the only
    difference is which harness the reader asks for."""
    lines = _synthetic_lines(3)
    source = _write_transcript(tmp_path / "crush01-session.jsonl", lines)
    await store.ingest_path(str(source), "crush01", harness="crush")

    assert await store.verify_chain("crush01", harness="crush") is True
    # the SAME session, read back under the wrong (default) harness — reports exactly
    # as if nothing had ever been ingested, never an error naming the mismatch
    assert await store.verify_chain("crush01") is True  # vacuously — zero rows match
    assert await store.re_materialize("crush01") is None
    assert await store.raw_lines("crush01") is None
    assert await store.mining_view("crush01") is None


async def test_resume_diagnostics_honors_a_non_claude_code_harness(
    store: SoulStore, tmp_path: Path,
) -> None:
    """THE FOURTH `_HARNESS` OCCURRENCE (Thoth dispatch 6715), worse than its five
    siblings: `resume_diagnostics` took no `harness` override at all until this fix —
    a session ingested under any harness but 'claude-code' reported as never-ingested
    (`None`) regardless of what a caller passed, because there was nothing to pass."""
    lines = _synthetic_lines(4)
    source = _write_transcript(tmp_path / "dsh02-session.jsonl", lines)
    await store.ingest_path(str(source), "dsh02", harness="dsh")

    diagnostics = await store.resume_diagnostics("dsh02", harness="dsh")
    assert diagnostics is not None
    count, tail_bytes, tail_lines = diagnostics
    assert count == 0
    assert tail_lines == 4
    assert tail_bytes == sum(len(line.encode()) + 1 for line in lines)


async def test_resume_diagnostics_wrong_harness_reports_never_ingested(
    store: SoulStore, tmp_path: Path,
) -> None:
    """THE CONTRAST, same shape as the round-trip test above: the same session, read
    back under the wrong (default) harness, is indistinguishable from one that was
    never ingested at all."""
    lines = _synthetic_lines(2)
    source = _write_transcript(tmp_path / "crush02-session.jsonl", lines)
    await store.ingest_path(str(source), "crush02", harness="crush")

    assert await store.resume_diagnostics("crush02", harness="crush") is not None
    assert await store.resume_diagnostics("crush02") is None


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


# --- splice_sources / verify_jsonl_chain_boundary / rematerialize_to_disk(upto=...)
# (thread 6483/6534/6540/6543 — the operator's injection thesis landing on a real
# specimen: whole transcripts in, any resume point out, defaulting to latest) ------------

def _chained_lines(n: int, *, seed: str, session: str = "chainsession") -> list[str]:
    """A genuine `user`/`assistant` parentUuid chain — `_synthetic_lines`' own fixture
    carries no uuid/parentUuid at all, the one thing this feature's own tests need.
    Deterministic, readable uuids: f"{seed}-000...-line{i:04d}"."""
    out = []
    parent = None
    for i in range(n):
        u = f"{seed}0000-0000-0000-0000-{i:012d}"
        ts = datetime(2026, 8, 17, 12, 0, i, tzinfo=UTC).isoformat()
        role = "user" if i % 2 == 0 else "assistant"
        out.append(json.dumps({
            "type": role, "uuid": u, "parentUuid": parent, "timestamp": ts,
            "sessionId": session, "cwd": "/some/cwd",
            "message": {"role": role, "content": f"turn {i}"},
        }))
        parent = u
    return out


async def test_verify_jsonl_chain_boundary_clean_join(tmp_path: Path) -> None:
    lines = _chained_lines(6, seed="clean")
    a = _write_transcript(tmp_path / "a.jsonl", lines[:3])
    b = _write_transcript(tmp_path / "b.jsonl", lines[3:])
    assert verify_jsonl_chain_boundary(str(a), str(b)) is None


async def test_verify_jsonl_chain_boundary_broken_join(tmp_path: Path) -> None:
    lines_a = _chained_lines(3, seed="brokea")
    lines_b = _chained_lines(3, seed="brokeb")  # unrelated chain, wrong parentUuid
    a = _write_transcript(tmp_path / "a.jsonl", lines_a)
    b = _write_transcript(tmp_path / "b.jsonl", lines_b)
    reason = verify_jsonl_chain_boundary(str(a), str(b))
    assert reason is not None and "chain broken at the join" in reason


async def test_verify_jsonl_chain_boundary_uuid_overlap(tmp_path: Path) -> None:
    lines = _chained_lines(4, seed="overlap")
    a = _write_transcript(tmp_path / "a.jsonl", lines)
    b = _write_transcript(tmp_path / "b.jsonl", lines[2:])  # shares uuids with a
    reason = verify_jsonl_chain_boundary(str(a), str(b))
    assert reason is not None and "uuid overlap" in reason


async def test_verify_jsonl_chain_boundary_orphan_in_b(tmp_path: Path) -> None:
    lines = _chained_lines(4, seed="orphan")
    a = _write_transcript(tmp_path / "a.jsonl", lines[:2])
    b_lines = lines[2:]
    # corrupt B's SECOND entry's parentUuid to point at nothing real
    d = json.loads(b_lines[1])
    d["parentUuid"] = "no-such-uuid-anywhere"
    b_lines[1] = json.dumps(d)
    b = _write_transcript(tmp_path / "b.jsonl", b_lines)
    reason = verify_jsonl_chain_boundary(str(a), str(b))
    assert reason is not None and "orphan entry" in reason


async def test_verify_jsonl_chain_boundary_refuses_a_file_with_no_uuid_bearing_entries(
    tmp_path: Path,
) -> None:
    a = _write_transcript(tmp_path / "a.jsonl", _synthetic_lines(2))  # no uuid field
    b = _write_transcript(tmp_path / "b.jsonl", _chained_lines(2, seed="lonely"))
    reason = verify_jsonl_chain_boundary(str(a), str(b))
    assert reason is not None and "no uuid-bearing entries" in reason


async def test_verify_jsonl_chain_boundary_sees_through_an_attachment_link(
    tmp_path: Path,
) -> None:
    """THE REGRESSION jesus/chad's live splice caught (thread 6483/6559/6565): the join
    itself is genuinely chained through a `type:"attachment"` line — a real link, never a
    valid seek target, but a link `verify_jsonl_chain_boundary` must still see through or
    it reports a false orphan on the entry right after it."""
    lines = _chained_lines(4, seed="attachlink")
    # splice an attachment line, chained correctly, between B's join point and its
    # next real entry — the exact shape found live
    b_raw = json.loads(lines[2])
    attach_uuid = "attachlink-attach-0000-0000-000000000000"
    attachment = json.dumps({
        "type": "attachment", "uuid": attach_uuid, "parentUuid": b_raw["parentUuid"],
        "sessionId": b_raw["sessionId"],
    })
    # re-point the entry that used to follow directly onto the attachment instead
    b_raw["parentUuid"] = attach_uuid
    lines[2] = json.dumps(b_raw)
    a = _write_transcript(tmp_path / "a.jsonl", lines[:2])
    b = _write_transcript(tmp_path / "b.jsonl", [attachment, *lines[2:]])
    assert verify_jsonl_chain_boundary(str(a), str(b)) is None


async def test_verify_jsonl_chain_boundary_still_refuses_a_real_break_through_an_attachment(
    tmp_path: Path,
) -> None:
    """Widening the walk to see attachment links must not make it MORE permissive on a
    genuine break — Thoth's own instruction (thread 6567): prove the widened verifier
    still refuses, on the same shape the fix just legitimized."""
    lines = _chained_lines(4, seed="attachbreak")
    b_raw = json.loads(lines[2])
    attachment = json.dumps({
        "type": "attachment", "uuid": "attachbreak-attach-0000-0000-000000000000",
        "parentUuid": b_raw["parentUuid"], "sessionId": b_raw["sessionId"],
    })
    # the next real entry's parentUuid points at neither the attachment NOR anything
    # else in A or B — a genuine, unrelated break, even with an attachment link present
    b_raw["parentUuid"] = "points-at-nothing-real-at-all"
    lines[2] = json.dumps(b_raw)
    a = _write_transcript(tmp_path / "a.jsonl", lines[:2])
    b = _write_transcript(tmp_path / "b.jsonl", [attachment, *lines[2:]])
    reason = verify_jsonl_chain_boundary(str(a), str(b))
    assert reason is not None and "orphan entry" in reason


async def test_splice_sources_chains_two_files_into_one(
    store: SoulStore, tmp_path: Path,
) -> None:
    """THE BUG THIS FUNCTION EXISTS TO FIX (caught before shipping the wrong claim that
    two ingest_path calls would chain automatically): ingest_path's own `since` offset
    slices into THIS file's own line list, silently under-ingesting a second, different
    source. splice_sources must ingest every line of every source."""
    lines = _chained_lines(10, seed="splicehap")
    a = _write_transcript(tmp_path / "a.jsonl", lines[:4])
    b = _write_transcript(tmp_path / "b.jsonl", lines[4:])

    n = await store.splice_sources("splicedsid", [str(a), str(b)])
    assert n == 10
    rows = await store.pool.fetch(
        "SELECT line_idx, raw_line FROM soul_lines WHERE harness='claude-code' "
        "AND anchor_sid='splicedsid' ORDER BY line_idx")
    assert [bytes(r["raw_line"]).decode() for r in rows] == lines
    assert [r["line_idx"] for r in rows] == list(range(10))
    assert await store.verify_chain("splicedsid") is True

    dest = tmp_path / "reassembled.jsonl"
    receipt = await store.rematerialize_to_disk("splicedsid", dest=str(dest))
    assert "error" not in receipt
    assert dest.read_bytes() == ("\n".join(lines) + "\n").encode()


async def test_splice_sources_refuses_and_writes_nothing_on_a_broken_pair(
    store: SoulStore, tmp_path: Path,
) -> None:
    lines_a = _chained_lines(3, seed="refusea")
    lines_b = _chained_lines(3, seed="refuseb")  # not a real continuation of A
    a = _write_transcript(tmp_path / "a.jsonl", lines_a)
    b = _write_transcript(tmp_path / "b.jsonl", lines_b)

    with pytest.raises(ValueError, match="splice_sources refused"):
        await store.splice_sources("refusedsid", [str(a), str(b)])
    rows = await store.pool.fetch(
        "SELECT count(*) AS n FROM soul_lines WHERE anchor_sid='refusedsid'")
    assert rows[0]["n"] == 0  # nothing written — the refusal is atomic, not partial


async def test_splice_sources_verify_false_bypasses_the_check(
    store: SoulStore, tmp_path: Path,
) -> None:
    """An explicit opt-out for a caller that already verified continuity itself moments
    earlier — still writes the chain, even though the pair alone wouldn't pass."""
    lines_a = _chained_lines(2, seed="skipa")
    lines_b = _chained_lines(2, seed="skipb")
    a = _write_transcript(tmp_path / "a.jsonl", lines_a)
    b = _write_transcript(tmp_path / "b.jsonl", lines_b)
    n = await store.splice_sources("skipverifysid", [str(a), str(b)], verify=False)
    assert n == 4


async def test_rematerialize_to_disk_upto_emits_a_genuine_prefix_and_confesses(
    store: SoulStore, tmp_path: Path,
) -> None:
    lines = _chained_lines(6, seed="seekconf")
    source = _write_transcript(tmp_path / "s.jsonl", lines)
    await store.ingest_path(str(source), "seekconfsid")

    verified_lines, broken = await store._verified_lines("seekconfsid")
    assert broken is None
    addressable = _addressable_entries(verified_lines)
    seek_uuid = addressable[2][1]  # not the latest — a genuine mid-chain seek

    dest = tmp_path / "seek.jsonl"
    receipt = await store.rematerialize_to_disk("seekconfsid", dest=str(dest), upto=seek_uuid)
    assert "error" not in receipt
    assert receipt["seek"] == seek_uuid
    assert receipt["withheld_entries"] == 3

    out_lines = _split_lines(dest.read_bytes())
    # the emitted content is a genuine prefix of the full chain, PLUS one confession line
    assert out_lines[:3] == [line.encode() for line in lines[:3]]
    assert len(out_lines) == 4
    confession = json.loads(out_lines[-1])
    assert confession["isMeta"] is True
    assert confession["parentUuid"] == seek_uuid
    assert "3 later entries" in confession["message"]["content"]


async def test_rematerialize_to_disk_upto_none_is_unchanged_full_emission(
    store: SoulStore, tmp_path: Path,
) -> None:
    lines = _chained_lines(4, seed="nofilt")
    source = _write_transcript(tmp_path / "s.jsonl", lines)
    await store.ingest_path(str(source), "nofiltsid")
    dest = tmp_path / "out.jsonl"
    receipt = await store.rematerialize_to_disk("nofiltsid", dest=str(dest))
    assert "seek" not in receipt
    assert dest.read_bytes() == ("\n".join(lines) + "\n").encode()


async def test_rematerialize_to_disk_upto_confess_false_omits_the_confession_line(
    store: SoulStore, tmp_path: Path,
) -> None:
    lines = _chained_lines(4, seed="noconf")
    source = _write_transcript(tmp_path / "s.jsonl", lines)
    await store.ingest_path(str(source), "noconfsid")
    verified_lines, _ = await store._verified_lines("noconfsid")
    seek_uuid = _addressable_entries(verified_lines)[0][1]

    dest = tmp_path / "out.jsonl"
    receipt = await store.rematerialize_to_disk(
        "noconfsid", dest=str(dest), upto=seek_uuid, confess=False)
    assert receipt["withheld_entries"] == 3
    assert dest.read_bytes() == (lines[0] + "\n").encode()  # the bare prefix, no extra line


async def test_rematerialize_to_disk_upto_refuses_an_unknown_uuid(
    store: SoulStore, tmp_path: Path,
) -> None:
    lines = _chained_lines(3, seed="badseek")
    source = _write_transcript(tmp_path / "s.jsonl", lines)
    await store.ingest_path(str(source), "badseeksid")

    dest = tmp_path / "out.jsonl"
    receipt = await store.rematerialize_to_disk(
        "badseeksid", dest=str(dest), upto="not-a-real-uuid")
    assert "error" in receipt and "matches no user/assistant entry" in receipt["error"]
    assert not dest.exists()
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


async def test_rematerialize_to_disk_reports_unchanged_when_target_is_its_own_source(
    store: SoulStore, tmp_path: Path,
) -> None:
    """THE CANON IS ALREADY HERE (operator, 2026-09-03 — Chad's live shape: the office held
    the full canon, the store had just ingested it from there, and the resume caller read
    the write's absence as 'no canon at the spawn cwd' and spawned at a stale partial one
    slug over). A dest that IS the session's own last-ingested source, untouched since,
    is a NAMED success (`unchanged`), never an error and never a byte-for-byte rewrite."""
    lines = _synthetic_lines(3)
    source = _write_transcript(tmp_path / "s" / "5e1f5ame-session.jsonl", lines)
    await store.ingest_path(str(source), "5e1f5ame")
    before = source.stat().st_mtime_ns

    receipt = await store.rematerialize_to_disk("5e1f5ame", dest=str(source))
    assert "error" not in receipt
    assert receipt.get("unchanged") is True
    assert receipt["written"] == str(source)
    assert source.stat().st_mtime_ns == before          # not rewritten
    assert source.read_text() == "\n".join(lines) + "\n"


async def test_rematerialize_to_disk_parks_a_stale_copy_instead_of_overwriting_it(
    store: SoulStore, tmp_path: Path,
) -> None:
    """Constitution 3 on disk: a stale, store-unseen copy at the destination is MOVED into
    `.superseded-stubs/` (two levels below the slug root, invisible to the harness's own
    listing and to `locate_current_transcript`'s `*/*.jsonl` glob) — never deleted, never
    silently overwritten. Sekhmet's hand-move for Marquee's shadowing stub (b348e902),
    made the materializer's standing rule."""
    lines = _synthetic_lines(3)
    source = _write_transcript(tmp_path / "s" / "5ee7pa7k-session.jsonl", lines)
    await store.ingest_path(str(source), "5ee7pa7k")

    dest = tmp_path / "d" / "5ee7pa7k-session.jsonl"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("stale partial\n")
    past = datetime.now(UTC).timestamp() - 3600
    os.utime(dest, (past, past))                        # older than the ingest: not live

    receipt = await store.rematerialize_to_disk("5ee7pa7k", dest=str(dest))
    assert "error" not in receipt
    assert dest.read_text() == "\n".join(lines) + "\n"  # canon now at the destination
    parked = list((dest.parent / ".superseded-stubs").glob("5ee7pa7k-session-superseded-*.jsonl"))
    assert len(parked) == 1
    assert parked[0].read_text() == "stale partial\n"   # the old copy survives, parked
    # `locate_current_transcript` globs `<root>/*/*.jsonl` — from this root, the parked
    # copy sits at d/.superseded-stubs/…, one level too deep to ever be picked up again.
    assert sorted(p.name for p in tmp_path.glob("*/*.jsonl")) == [
        "5ee7pa7k-session.jsonl", "5ee7pa7k-session.jsonl"]


async def test_ingest_path_touches_last_ingested_at_on_a_verified_full_sync(
    store: SoulStore, tmp_path: Path,
) -> None:
    """Chad's live shape, 2026-09-03: the office file's mtime moved (a byte-identical
    re-materialize) with NOTHING new in it, `ingest_path` found zero new lines and never
    touched the row, so the target read as LIVE on every resume forever. A full scan that
    finds the file holds exactly the store's lines is a verified sync: the clock moves,
    and the materializer then reports the canon as already there. A SHORTER file (a
    stale partial) never earns the touch."""
    lines = _synthetic_lines(4)
    source = _write_transcript(tmp_path / "s" / "7ouc4c10-session.jsonl", lines)
    assert await store.ingest_path(str(source), "7ouc4c10") == 4
    before = await store.pool.fetchval(
        "SELECT last_ingested_at FROM soul_sessions WHERE anchor_sid='7ouc4c10'")
    moved = datetime.now(UTC).timestamp()
    os.utime(source, (moved, moved))                # mtime moved, content did not

    assert await store.ingest_path(str(source), "7ouc4c10") == 0
    after = await store.pool.fetchval(
        "SELECT last_ingested_at FROM soul_sessions WHERE anchor_sid='7ouc4c10'")
    assert after > before                            # the clock moved: verified sync
    receipt = await store.rematerialize_to_disk("7ouc4c10", dest=str(source))
    assert receipt.get("unchanged") is True          # …and the canon reads as present

    partial = _write_transcript(tmp_path / "p" / "7ouc4c10-session.jsonl", lines[:2])
    assert await store.ingest_path(str(partial), "7ouc4c10") == 0
    again = await store.pool.fetchrow(
        "SELECT last_ingested_at, source_path FROM soul_sessions WHERE anchor_sid='7ouc4c10'")
    assert again is not None
    assert again["last_ingested_at"] == after        # a stale partial never touches it
    assert again["source_path"] == str(source)


# ═══ wave 12 item 2 (thread 78efd46d): the soul store's cold tier ═══════════════════════
# "memory gets tiers not deletion" (operator ruling via decision 64ec1905) — a session
# unread for 30 days folds into one compressed soul_lines_cold row; rematerialize/resume/
# verify_round_trip_sample read through both tiers without knowing which they hit.

async def _backdate(store: SoulStore, anchor_sid: str, *, days: int) -> None:
    from datetime import timedelta
    await store.pool.execute(
        "UPDATE soul_sessions SET last_ingested_at = now() - $1::interval "
        "WHERE anchor_sid=$2", timedelta(days=days), anchor_sid)


async def test_fold_to_cold_tier_moves_content_and_shrinks_storage(
    store: SoulStore, tmp_path: Path,
) -> None:
    lines = _synthetic_lines(50)
    p = _write_transcript(tmp_path / "t.jsonl", lines)
    await store.ingest_path(str(p), "c01d0001")

    result = await store.fold_to_cold_tier("c01d0001")
    assert result["folded"] is True
    assert result["line_count"] == 50
    assert result["compressed_bytes"] < result["total_bytes"]  # the whole point: it shrinks

    hot_rows = await store.pool.fetchval(
        "SELECT count(*) FROM soul_lines WHERE anchor_sid='c01d0001'")
    assert hot_rows == 0  # the per-line rows are gone
    cold_row = await store.pool.fetchrow(
        "SELECT line_count, total_bytes FROM soul_lines_cold WHERE anchor_sid='c01d0001'")
    assert cold_row is not None
    assert cold_row["line_count"] == 50
    assert cold_row["total_bytes"] == result["total_bytes"]


async def test_fold_to_cold_tier_refuses_a_broken_chain(
    store: SoulStore, tmp_path: Path,
) -> None:
    p = _write_transcript(tmp_path / "t.jsonl", _synthetic_lines(4))
    await store.ingest_path(str(p), "c01dbad0")
    await store.pool.execute(
        "UPDATE soul_lines SET raw_line=E'TAMPERED'::bytea "
        "WHERE anchor_sid='c01dbad0' AND line_idx=1")

    result = await store.fold_to_cold_tier("c01dbad0")
    assert result["folded"] is False
    assert "error" in result
    # nothing was deleted or created on a refusal
    assert await store.pool.fetchval(
        "SELECT count(*) FROM soul_lines WHERE anchor_sid='c01dbad0'") == 4
    assert await store.pool.fetchval(
        "SELECT count(*) FROM soul_lines_cold WHERE anchor_sid='c01dbad0'") == 0


async def test_fold_to_cold_tier_is_a_noop_when_already_cold(
    store: SoulStore, tmp_path: Path,
) -> None:
    p = _write_transcript(tmp_path / "t.jsonl", _synthetic_lines(3))
    await store.ingest_path(str(p), "c01da1re")
    first = await store.fold_to_cold_tier("c01da1re")
    assert first["folded"] is True
    second = await store.fold_to_cold_tier("c01da1re")
    assert second == {"anchor_sid": "c01da1re", "folded": False, "note": "already cold"}


async def test_fold_to_cold_tier_is_a_noop_when_never_ingested(store: SoulStore) -> None:
    result = await store.fold_to_cold_tier("never-existed")
    assert result == {"anchor_sid": "never-existed", "folded": False,
                       "note": "nothing ingested"}


async def test_cold_eligible_sessions_respects_idle_days_and_already_cold(
    store: SoulStore, tmp_path: Path,
) -> None:
    fresh = _write_transcript(tmp_path / "fresh.jsonl", _synthetic_lines(2))
    idle = _write_transcript(tmp_path / "idle.jsonl", _synthetic_lines(2))
    already_cold = _write_transcript(tmp_path / "cold.jsonl", _synthetic_lines(2))
    await store.ingest_path(str(fresh), "fre5hs1d0")
    await store.ingest_path(str(idle), "1d1e5e551")
    await store.ingest_path(str(already_cold), "a1readyc0")
    await _backdate(store, "1d1e5e551", days=31)
    await _backdate(store, "a1readyc0", days=31)
    await store.fold_to_cold_tier("a1readyc0")

    eligible = await store.cold_eligible_sessions(idle_days=30, limit=20)
    assert eligible == ["1d1e5e551"]  # fresh is too recent, already-cold is excluded


async def test_fold_cold_tier_batch_folds_only_eligible_sessions(
    store: SoulStore, tmp_path: Path,
) -> None:
    fresh = _write_transcript(tmp_path / "fresh.jsonl", _synthetic_lines(2))
    idle_a = _write_transcript(tmp_path / "idle_a.jsonl", _synthetic_lines(2))
    idle_b = _write_transcript(tmp_path / "idle_b.jsonl", _synthetic_lines(2))
    await store.ingest_path(str(fresh), "batchfre5")
    await store.ingest_path(str(idle_a), "batchid1e")
    await store.ingest_path(str(idle_b), "batchid2e")
    await _backdate(store, "batchid1e", days=31)
    await _backdate(store, "batchid2e", days=31)

    report = await store.fold_cold_tier_batch(idle_days=30, limit=20)
    assert report["candidates"] == 2
    assert {f["anchor_sid"] for f in report["folded"]} == {"batchid1e", "batchid2e"}
    assert report["errors"] == []
    assert await store.pool.fetchval(
        "SELECT count(*) FROM soul_lines WHERE anchor_sid='batchfre5'") == 2  # untouched


async def test_fold_cold_tier_batch_respects_the_limit(
    store: SoulStore, tmp_path: Path,
) -> None:
    for i in range(3):
        sid = f"lim1t000{i}"
        p = _write_transcript(tmp_path / f"{sid}.jsonl", _synthetic_lines(2))
        await store.ingest_path(str(p), sid)
        await _backdate(store, sid, days=31)

    report = await store.fold_cold_tier_batch(idle_days=30, limit=2)
    assert report["candidates"] == 2
    assert len(report["folded"]) == 2


async def test_fold_cold_tier_batch_reports_one_bad_session_without_aborting_the_rest(
    store: SoulStore, tmp_path: Path,
) -> None:
    ok = _write_transcript(tmp_path / "ok.jsonl", _synthetic_lines(3))
    bad = _write_transcript(tmp_path / "bad.jsonl", _synthetic_lines(3))
    await store.ingest_path(str(ok), "batchok01")
    await store.ingest_path(str(bad), "batchbad1")
    await _backdate(store, "batchok01", days=31)
    await _backdate(store, "batchbad1", days=31)
    await store.pool.execute(
        "UPDATE soul_lines SET raw_line=E'TAMPERED'::bytea "
        "WHERE anchor_sid='batchbad1' AND line_idx=1")

    report = await store.fold_cold_tier_batch(idle_days=30, limit=20)
    assert {f["anchor_sid"] for f in report["folded"]} == {"batchok01"}
    assert len(report["errors"]) == 1
    assert report["errors"][0]["anchor_sid"] == "batchbad1"


# --- read-through: every consumer works identically before and after the fold ------------

async def test_re_materialize_reads_through_the_cold_tier(
    store: SoulStore, tmp_path: Path,
) -> None:
    lines = _synthetic_lines(6)
    p = _write_transcript(tmp_path / "t.jsonl", lines)
    await store.ingest_path(str(p), "rtc01mat0")
    before = await store.re_materialize("rtc01mat0")
    await store.fold_to_cold_tier("rtc01mat0")
    after = await store.re_materialize("rtc01mat0")
    assert after == before == "\n".join(lines)


async def test_raw_lines_reads_through_the_cold_tier(store: SoulStore, tmp_path: Path) -> None:
    lines = _synthetic_lines(5)
    p = _write_transcript(tmp_path / "t.jsonl", lines)
    await store.ingest_path(str(p), "rtc0raw01")
    before = await store.raw_lines("rtc0raw01")
    await store.fold_to_cold_tier("rtc0raw01")
    after = await store.raw_lines("rtc0raw01")
    assert after == before == lines


async def test_mining_view_reads_through_the_cold_tier(store: SoulStore, tmp_path: Path) -> None:
    lines = _synthetic_lines(5)
    p = _write_transcript(tmp_path / "t.jsonl", lines)
    await store.ingest_path(str(p), "rtc0min01")
    before = await store.mining_view("rtc0min01")
    await store.fold_to_cold_tier("rtc0min01")
    after = await store.mining_view("rtc0min01")
    assert after == before
    assert after is not None and len(after) == 5


async def test_raw_lines_re_materialize_and_mining_view_refuse_a_tampered_cold_blob(
    store: SoulStore, tmp_path: Path,
) -> None:
    """READ-PATH CONSOLIDATION (Thoth mail 9134, operator ruling on thread 773d633a):
    the deliberate, disclosed widening — before this fold, `_all_raw_lines`'s own cold
    branch never verified the chain at all, so raw_lines/re_materialize/mining_view
    would have silently returned a tampered cold blob's content as if it were clean.
    After folding all three readers onto the shared `_iter_verified_lines`, a tampered
    cold row is caught the same way `rematerialize_to_disk` already caught it."""
    p = _write_transcript(tmp_path / "t.jsonl", _synthetic_lines(5))
    await store.ingest_path(str(p), "rtc0tmp01")
    await store.fold_to_cold_tier("rtc0tmp01")
    await store.pool.execute(
        "UPDATE soul_lines_cold SET last_hash='deadbeef' WHERE anchor_sid='rtc0tmp01'")

    assert await store.raw_lines("rtc0tmp01") is None
    assert await store.re_materialize("rtc0tmp01") is None
    assert await store.mining_view("rtc0tmp01") is None


async def test_verify_chain_true_after_a_fold(store: SoulStore, tmp_path: Path) -> None:
    """THE ACCEPTANCE TEST NAMED IN THE DISPATCH: 'the hash chain verifies across the
    fold' — the same chain that was true before folding must still be true after,
    recomputed from the cold blob and checked against the hash captured at fold time."""
    p = _write_transcript(tmp_path / "t.jsonl", _synthetic_lines(7))
    await store.ingest_path(str(p), "rtc0chn01")
    assert await store.verify_chain("rtc0chn01") is True
    await store.fold_to_cold_tier("rtc0chn01")
    assert await store.verify_chain("rtc0chn01") is True


async def test_verify_chain_false_on_a_corrupted_cold_blob(
    store: SoulStore, tmp_path: Path,
) -> None:
    p = _write_transcript(tmp_path / "t.jsonl", _synthetic_lines(5))
    await store.ingest_path(str(p), "rtc0crpt0")
    await store.fold_to_cold_tier("rtc0crpt0")
    await store.pool.execute(
        "UPDATE soul_lines_cold SET content_gzip=E'\\\\x00'::bytea "
        "WHERE anchor_sid='rtc0crpt0'")
    with pytest.raises(Exception):  # noqa: B017,PT011 — a corrupted gzip stream must not
        # be silently swallowed into a false "verified" result; any decompress failure is
        # an honest failure here, never treated as a clean chain.
        await store.verify_chain("rtc0crpt0")


async def test_resume_diagnostics_matches_before_and_after_a_fold(
    store: SoulStore, tmp_path: Path,
) -> None:
    lines = _fixture_with_compaction_boundary()
    p = _write_transcript(tmp_path / "t.jsonl", lines)
    await store.ingest_path(str(p), "rtc0res01")
    before = await store.resume_diagnostics("rtc0res01")
    await store.fold_to_cold_tier("rtc0res01")
    after = await store.resume_diagnostics("rtc0res01")
    assert after == before is not None


async def test_rematerialize_to_disk_is_byte_identical_after_a_fold(
    store: SoulStore, tmp_path: Path,
) -> None:
    lines = _fixture_with_compaction_boundary()
    source = _write_transcript(tmp_path / "source" / "s.jsonl", lines)
    await store.ingest_path(str(source), "rtc0dsk01")
    await store.fold_to_cold_tier("rtc0dsk01")

    dest = tmp_path / "recovered" / "s.jsonl"
    receipt = await store.rematerialize_to_disk("rtc0dsk01", dest=str(dest))
    assert "error" not in receipt
    assert dest.read_bytes() == source.read_bytes()


async def test_rematerialize_to_disk_writes_nothing_on_a_broken_cold_chain(
    store: SoulStore, tmp_path: Path,
) -> None:
    p = _write_transcript(tmp_path / "t.jsonl", _synthetic_lines(5))
    await store.ingest_path(str(p), "rtc0brk01")
    await store.fold_to_cold_tier("rtc0brk01")
    await store.pool.execute(
        "UPDATE soul_lines_cold SET last_hash='deadbeef' WHERE anchor_sid='rtc0brk01'")

    dest = tmp_path / "should-not-exist.jsonl"
    receipt = await store.rematerialize_to_disk("rtc0brk01", dest=str(dest))
    assert "error" in receipt
    assert not dest.exists()


async def test_rematerialize_to_disk_upto_seek_reads_through_the_cold_tier(
    store: SoulStore, tmp_path: Path,
) -> None:
    """`_verified_lines` (the whole-file seek path `upto=` uses) reads through the cold
    tier too — a seek is a controlled, human-supervised repair operation, but it must
    still work on a session old enough to have been folded."""
    lines = _chained_lines(6, seed="coldseek")
    source = _write_transcript(tmp_path / "s.jsonl", lines)
    await store.ingest_path(str(source), "rtc0seek1")
    seek_uuid = _addressable_entries([line.encode() for line in lines])[2][1]

    await store.fold_to_cold_tier("rtc0seek1")
    dest = tmp_path / "seeked.jsonl"
    receipt = await store.rematerialize_to_disk("rtc0seek1", dest=str(dest), upto=seek_uuid)
    assert "error" not in receipt
    assert receipt["withheld_entries"] == 3
    assert dest.exists()


async def test_verify_round_trip_sample_samples_both_tiers_when_both_exist(
    store: SoulStore, tmp_path: Path,
) -> None:
    hot = _write_transcript(tmp_path / "hot.jsonl", _synthetic_lines(3))
    cold = _write_transcript(tmp_path / "cold.jsonl", _synthetic_lines(3))
    await store.ingest_path(str(hot), "vrts0hot0")
    await store.ingest_path(str(cold), "vrts0c0ld")
    await store.fold_to_cold_tier("vrts0c0ld")

    report = await store.verify_round_trip_sample(n=2)
    assert report.failures == []
    assert report.skipped_live == 0
    cold_hit = await store.pool.fetchval(
        "SELECT count(*) FROM soul_lines_cold WHERE anchor_sid='vrts0c0ld'")
    assert cold_hit == 1  # the cold session really was in the sampling pool


# --- the daily cron shim (wave 12 item 2, Thoth DM 8378) ═══════════════════════════════════

async def test_soul_cold_tier_heartbeat_is_a_no_op_when_the_flag_is_off(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    from types import SimpleNamespace

    import src.config.settings as settings_mod
    from src.config.settings import Settings
    from src.workers.arq_worker import soul_cold_tier_heartbeat

    store = SoulStore(actions.pool)
    p = _write_transcript(tmp_path / "t.jsonl", _synthetic_lines(2))
    await store.ingest_path(str(p), "hb1flagof")
    await _backdate(store, "hb1flagof", days=31)

    monkeypatch.setattr(
        settings_mod, "get_settings",
        lambda: Settings(osiris_soul_cold_tier_enabled=False))
    ctx = {"cascade": SimpleNamespace(actions=actions)}
    assert await soul_cold_tier_heartbeat(ctx) == 0
    assert await store.pool.fetchval(
        "SELECT count(*) FROM soul_lines WHERE anchor_sid='hb1flagof'") == 2  # untouched


async def test_soul_cold_tier_heartbeat_folds_and_briefs_the_desk(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    from types import SimpleNamespace
    from typing import Any

    import src.orchestrator.mailbox as mailbox
    from src.workers.arq_worker import soul_cold_tier_heartbeat

    store = SoulStore(actions.pool)
    p = _write_transcript(tmp_path / "t.jsonl", _synthetic_lines(10))
    await store.ingest_path(str(p), "hb1folded")
    await _backdate(store, "hb1folded", days=31)

    captured: dict[str, Any] = {}

    async def _fake_send(pool: Any, **kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {"sent": 1}

    monkeypatch.setattr(mailbox, "send_message", _fake_send)
    ctx = {"cascade": SimpleNamespace(actions=actions)}
    folded = await soul_cold_tier_heartbeat(ctx)

    assert folded == 1
    assert await store.pool.fetchval(
        "SELECT count(*) FROM soul_lines WHERE anchor_sid='hb1folded'") == 0
    assert await store.pool.fetchval(
        "SELECT count(*) FROM soul_lines_cold WHERE anchor_sid='hb1folded'") == 1
    assert captured["to_project"] == "operator"
    assert captured["desk_kind"] == "fyi"
    assert "folded" in captured["body"]


async def test_soul_cold_tier_heartbeat_is_silent_when_nothing_is_eligible(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    from types import SimpleNamespace
    from typing import Any

    import src.orchestrator.mailbox as mailbox
    from src.workers.arq_worker import soul_cold_tier_heartbeat

    store = SoulStore(actions.pool)
    p = _write_transcript(tmp_path / "t.jsonl", _synthetic_lines(2))
    await store.ingest_path(str(p), "hb1fresh0")  # not backdated — not eligible

    calls: list[Any] = []

    async def _fake_send(pool: Any, **kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {"sent": 1}

    monkeypatch.setattr(mailbox, "send_message", _fake_send)
    ctx = {"cascade": SimpleNamespace(actions=actions)}
    assert await soul_cold_tier_heartbeat(ctx) == 0
    assert calls == []  # nothing to report — no desk noise


# ═══ wave 13 item 2 (thread 78efd46d): crush sessions become canonical ══════════════════
# closes the gap soul store piece 1 named on day one ("Crush is SQLite-backed... needs
# its own verbatim strategy, out of scope here on purpose").

def _make_crush_db(path: Path, *, session_id: str, n: int, start: int = 0) -> Path:
    """A minimal, real crush.db (schema verified live against a real install) — `n`
    messages for one session, `start` offsetting `created_at`/message ids so a second
    call against the SAME path can append genuinely NEW rows (incremental-ingest
    tests)."""
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, "
            " parent_session_id TEXT, title TEXT NOT NULL, "
            " message_count INTEGER NOT NULL DEFAULT 0, "
            " prompt_tokens INTEGER NOT NULL DEFAULT 0, "
            " completion_tokens INTEGER NOT NULL DEFAULT 0, "
            " cost REAL NOT NULL DEFAULT 0.0, updated_at INTEGER NOT NULL, "
            " created_at INTEGER NOT NULL, summary_message_id TEXT, todos TEXT)")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, "
            " session_id TEXT NOT NULL, role TEXT NOT NULL, "
            " parts TEXT NOT NULL DEFAULT '[]', model TEXT, "
            " created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, "
            " finished_at INTEGER, provider TEXT, "
            " is_summary_message INTEGER DEFAULT 0 NOT NULL)")
        conn.execute(
            "INSERT INTO sessions (id, title, message_count, updated_at, created_at) "
            "VALUES (?, 'test session', ?, 1700000000, 1700000000) "
            "ON CONFLICT (id) DO UPDATE SET message_count=excluded.message_count",
            (session_id, start + n))
        for i in range(start, start + n):
            conn.execute(
                "INSERT INTO messages (id, session_id, role, parts, model, "
                " created_at, updated_at) VALUES (?,?,?,?,?,?,?)",
                (f"msg-{i}", session_id, "user" if i % 2 == 0 else "assistant",
                 f'[{{"type":"text","data":{{"text":"line {i}"}}}}]', "test-model",
                 1700000000 + i, 1700000000 + i))
        conn.commit()
    finally:
        conn.close()
    return path


async def test_ingest_crush_session_stores_every_message_verbatim(
    store: SoulStore, tmp_path: Path,
) -> None:
    db = _make_crush_db(tmp_path / "crush.db", session_id="sess-a", n=5)
    n = await store.ingest_crush_session(str(db), "sess-a", "sessa000")
    assert n == 5
    rows = await store.pool.fetch(
        "SELECT line_idx, raw_line FROM soul_lines WHERE harness='crush' "
        "AND anchor_sid='sessa000' ORDER BY line_idx")
    assert [r["line_idx"] for r in rows] == list(range(5))
    decoded = json.loads(bytes(rows[0]["raw_line"]))
    assert decoded["session_id"] == "sess-a"
    assert decoded["id"] == "msg-0"
    assert decoded["parts"] == '[{"type":"text","data":{"text":"line 0"}}]'


async def test_ingest_crush_session_is_idempotent_and_resumes(
    store: SoulStore, tmp_path: Path,
) -> None:
    db = tmp_path / "crush.db"
    _make_crush_db(db, session_id="sess-b", n=3)
    first = await store.ingest_crush_session(str(db), "sess-b", "sessb000")
    assert first == 3
    second = await store.ingest_crush_session(str(db), "sess-b", "sessb000")
    assert second == 0  # nothing new

    _make_crush_db(db, session_id="sess-b", n=2, start=3)  # 2 more messages appended
    third = await store.ingest_crush_session(str(db), "sess-b", "sessb000")
    assert third == 2
    assert await store.verify_chain("sessb000", harness="crush") is True


async def test_ingest_crush_session_none_when_db_or_session_missing(
    store: SoulStore, tmp_path: Path,
) -> None:
    assert await store.ingest_crush_session(
        str(tmp_path / "nope.db"), "sess-x", "anchorx0") == 0
    db = _make_crush_db(tmp_path / "crush.db", session_id="sess-real", n=2)
    assert await store.ingest_crush_session(str(db), "sess-ghost", "anchory0") == 0


async def test_backfill_crush_ingests_every_discovered_session(
    store: SoulStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from typing import Any

    from src.ingest.harness import SessionLocator

    db = _make_crush_db(tmp_path / "crush.db", session_id="sess-bf", n=4)
    locator = SessionLocator(
        anchor_sid="sessbf00", session_id="sess-bf", harness="crush",
        source_path=str(db), cwd=str(tmp_path), project="t", anchored=True)

    def _fake_enumerate(self: Any, *, root: Path | None = None) -> Any:
        yield locator

    monkeypatch.setattr(
        "src.ingest.harness.crush_sqlite.CrushSqliteAdapter.enumerate", _fake_enumerate)

    counts = await store.backfill_crush()
    assert counts == {str(db): 1}
    rows = await store.pool.fetchval(
        "SELECT count(*) FROM soul_lines WHERE harness='crush' AND anchor_sid='sessbf00'")
    assert rows == 4

    # a second sweep with nothing new touches no sessions
    counts2 = await store.backfill_crush()
    assert counts2 == {}


async def test_verify_crush_round_trip_sample_clean_when_db_matches_store(
    store: SoulStore, tmp_path: Path,
) -> None:
    db = _make_crush_db(tmp_path / "crush.db", session_id="sess-rt1", n=4)
    await store.ingest_crush_session(str(db), "sess-rt1", "sess-rt1")
    report = await store.verify_crush_round_trip_sample(n=5)
    assert report.failures == []


async def test_verify_crush_round_trip_sample_catches_a_live_divergence(
    store: SoulStore, tmp_path: Path,
) -> None:
    db = _make_crush_db(tmp_path / "crush.db", session_id="sess-rt2", n=3)
    await store.ingest_crush_session(str(db), "sess-rt2", "sess-rt2")
    # mutate the LIVE db after ingest — the store now disagrees with the source
    conn = sqlite3.connect(str(db))
    conn.execute("UPDATE messages SET parts='[]' WHERE id='msg-0'")
    conn.commit()
    conn.close()
    report = await store.verify_crush_round_trip_sample(n=5)
    assert any(f["anchor_sid"] == "sess-rt2" for f in report.failures)


async def test_verify_crush_round_trip_sample_skips_a_vanished_db(
    store: SoulStore, tmp_path: Path,
) -> None:
    db = _make_crush_db(tmp_path / "crush.db", session_id="sess-rt3", n=2)
    await store.ingest_crush_session(str(db), "sess-rt3", "sess-rt3")
    db.unlink()
    report = await store.verify_crush_round_trip_sample(n=5)
    assert report.failures == []
