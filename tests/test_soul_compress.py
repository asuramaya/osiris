"""Compress before encrypt, and the background re-encode of the rows stored before it.

The format (`soul_crypto.pack_line` / `unpack_line`) is tested without a database; the write
path, every reader, the chain checks, the cold tier and the re-encode pass use real Postgres
through the `actions` fixture, with the clock and sleep injected so no test waits."""
from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import zstandard
from cryptography.fernet import Fernet, MultiFernet
from src.actions.core import Actions
from src.ingest import soul_crypto
from src.ingest.soul_crypto import (
    CODEC_PLAIN,
    CODEC_UNSET,
    CODEC_ZS1,
    LINE_ENVELOPE_ZS1,
    UnknownLineCodec,
    get_soul_fernet,
    is_encrypted,
    open_line,
    pack_line,
    seal_line,
    unpack_line,
)
from src.ingest.soul_store import SoulStore
from src.orchestrator import soul_recompress as rc


def _line(i: int, size: int = 1500) -> str:
    ts = datetime(2026, 9, 1, 12, 0, i % 60, tzinfo=UTC).isoformat()
    body = ("the quick brown fox jumps over the lazy dog " * 60)[:size]
    return json.dumps({"type": "user", "timestamp": ts, "n": i, "message": {"content": body}})


class _Clock:
    def __init__(self, step: float = 0.001) -> None:
        self.now = 0.0
        self.step = step

    def __call__(self) -> float:
        self.now += self.step
        return self.now


async def _no_sleep(_secs: float) -> None:
    return None


# --- the format, no database ----------------------------------------------------------------

def test_a_compressible_line_is_wrapped_and_comes_back_byte_identical() -> None:
    raw = _line(1).encode()
    packed, codec = pack_line(raw)
    assert codec == CODEC_ZS1 and packed.startswith(LINE_ENVELOPE_ZS1)
    assert len(packed) < len(raw) // 3
    assert unpack_line(packed) == raw


def test_a_short_line_is_kept_as_written_and_marked_as_a_decision() -> None:
    raw = b'{"type":"user","n":1}'
    assert pack_line(raw) == (raw, CODEC_PLAIN)
    assert unpack_line(raw) == raw


def test_a_line_that_does_not_shrink_is_never_inflated() -> None:
    raw = os.urandom(2000)
    packed, codec = pack_line(raw)
    assert (packed, codec) == (raw, CODEC_PLAIN)


def test_an_old_plain_line_is_never_mistaken_for_an_envelope() -> None:
    # a transcript line is JSON text: it opens with a brace or whitespace, never a NUL
    for raw in (b'{"a":1}', b'  {"a":1}', b'[1,2]', b'"x"'):
        assert unpack_line(raw) == raw


def test_an_unknown_envelope_version_refuses_loudly() -> None:
    with pytest.raises(UnknownLineCodec):
        unpack_line(b"\x00ZS9" + b"anything")


def test_a_corrupt_frame_raises_instead_of_returning_garbage() -> None:
    with pytest.raises(zstandard.ZstdError):
        unpack_line(LINE_ENVELOPE_ZS1 + b"not a zstd frame")


def test_seal_and_open_round_trip_under_a_fernet_and_a_multifernet() -> None:
    key = Fernet.generate_key()
    for f in (Fernet(key), MultiFernet([Fernet(key)])):
        raw = _line(7).encode()
        token, codec = seal_line(f, raw)
        assert codec == CODEC_ZS1 and is_encrypted(token)
        assert open_line(f, token) == raw
        # a token written the old way (the plain line sealed) still opens through the same call
        assert open_line(f, f.encrypt(raw)) == raw


# --- the write path and every reader ---------------------------------------------------------

async def _ingest(
    actions: Actions, tmp_path: Path, anchor: str, n: int, size: int = 1500,
) -> list[str]:
    lines = [_line(i, size) for i in range(n)]
    p = tmp_path / f"{anchor}.jsonl"
    p.write_text("\n".join(lines) + "\n")
    await SoulStore(actions.pool).ingest_path(str(p), anchor)
    return lines


async def _codecs(actions: Actions, anchor: str) -> list[int]:
    got = await actions.pool.fetch(
        "SELECT codec FROM soul_lines WHERE anchor_sid=$1 ORDER BY line_idx", anchor)
    return [int(r["codec"]) for r in got]


async def _make_legacy(actions: Actions, anchor: str) -> None:
    """Rewrites every row of `anchor` into the shape written before compression existed: the
    plain line sealed, codec 0."""
    fernet = get_soul_fernet()
    rows = await actions.pool.fetch(
        "SELECT line_idx, raw_line FROM soul_lines WHERE anchor_sid=$1", anchor)
    for r in rows:
        line = open_line(fernet, bytes(r["raw_line"]))
        await actions.pool.execute(
            "UPDATE soul_lines SET raw_line=$1, codec=0 WHERE anchor_sid=$2 AND line_idx=$3",
            fernet.encrypt(line), anchor, r["line_idx"])


async def test_new_rows_are_stored_compressed_and_read_back_identical(
    actions: Actions, tmp_path: Path,
) -> None:
    lines = await _ingest(actions, tmp_path, "cmp00001", 6)
    assert await _codecs(actions, "cmp00001") == [CODEC_ZS1] * 6
    store = SoulStore(actions.pool)
    assert await store.raw_lines("cmp00001") == lines
    assert await store.verify_chain("cmp00001") is True
    sizes = await actions.pool.fetchval(
        "SELECT avg(octet_length(raw_line)) FROM soul_lines WHERE anchor_sid='cmp00001'")
    assert sizes < len(lines[0]) * 0.6  # the sealed row is far smaller than the sealed raw line


async def test_rows_stored_the_old_way_still_read_and_verify(
    actions: Actions, tmp_path: Path,
) -> None:
    lines = await _ingest(actions, tmp_path, "cmp00002", 5)
    await _make_legacy(actions, "cmp00002")
    assert await _codecs(actions, "cmp00002") == [CODEC_UNSET] * 5
    store = SoulStore(actions.pool)
    assert await store.raw_lines("cmp00002") == lines
    assert await store.verify_chain("cmp00002") is True


async def test_old_and_new_rows_can_sit_in_one_session(actions: Actions, tmp_path: Path) -> None:
    lines = await _ingest(actions, tmp_path, "cmp00003", 4)
    await _make_legacy(actions, "cmp00003")
    more = [_line(i) for i in range(4, 8)]
    p = tmp_path / "cmp00003.jsonl"
    p.write_text("\n".join(lines + more) + "\n")
    await SoulStore(actions.pool).ingest_path(str(p), "cmp00003")
    assert await _codecs(actions, "cmp00003") == [0] * 4 + [CODEC_ZS1] * 4
    store = SoulStore(actions.pool)
    assert await store.raw_lines("cmp00003") == lines + more
    assert await store.verify_chain("cmp00003") is True


async def test_folding_a_compressed_session_to_the_cold_tier_is_byte_identical(
    actions: Actions, tmp_path: Path,
) -> None:
    lines = await _ingest(actions, tmp_path, "cmp00004", 5)
    store = SoulStore(actions.pool)
    result = await store.fold_to_cold_tier("cmp00004")
    assert result.get("folded") is True, result
    assert await store.raw_lines("cmp00004") == lines


async def test_a_sealed_compressed_line_verifies_for_a_citation(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.capture import _verify_transcript_line

    lines = await _ingest(actions, tmp_path, "cmp00005", 3)
    for i in range(3):
        out = await _verify_transcript_line(actions.pool, "claude-code", "cmp00005", i)
        assert out["verified"] is True, out
        assert out["raw_line"] == lines[i]


async def test_key_rotation_keeps_compressed_rows_readable(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.ingest.soul_store import rewrap_soul_lines_key

    lines = await _ingest(actions, tmp_path, "cmp00006", 4)
    old = get_soul_fernet()
    new = MultiFernet([Fernet(Fernet.generate_key())])
    await rewrap_soul_lines_key(
        actions.pool, old_fernet=old, new_fernet=new, dry_run=False)
    rows = await actions.pool.fetch(
        "SELECT raw_line FROM soul_lines WHERE anchor_sid='cmp00006' ORDER BY line_idx")
    assert [open_line(new, bytes(r["raw_line"])).decode() for r in rows] == lines


# --- the background re-encode ---------------------------------------------------------------

async def test_a_tick_compresses_legacy_rows_and_measures_the_saving(
    actions: Actions, tmp_path: Path,
) -> None:
    lines = await _ingest(actions, tmp_path, "rc000001", 8)
    await _make_legacy(actions, "rc000001")
    before = await actions.pool.fetchval(
        "SELECT sum(octet_length(raw_line)) FROM soul_lines WHERE anchor_sid='rc000001'")
    record = await rc.recompress_tick(
        actions.pool, get_soul_fernet(), clock=_Clock(), sleep=_no_sleep)
    after = await actions.pool.fetchval(
        "SELECT sum(octet_length(raw_line)) FROM soul_lines WHERE anchor_sid='rc000001'")
    assert record["state"] == "complete"
    assert record["rows_done"] == 8 and record["rows_skipped"] == 0
    assert record["bytes_before"] == before and record["bytes_after"] == after
    assert after < before * 0.4
    assert await _codecs(actions, "rc000001") == [CODEC_ZS1] * 8
    store = SoulStore(actions.pool)
    assert await store.raw_lines("rc000001") == lines
    assert await store.verify_chain("rc000001") is True
    shaped = rc.shape_compression(rc.read_progress(), key_present=True)
    assert shaped["state"] == "complete" and shaped["ratio"] > 2.5


async def test_a_second_tick_finds_nothing_and_changes_nothing(
    actions: Actions, tmp_path: Path,
) -> None:
    await _ingest(actions, tmp_path, "rc000002", 4)
    await _make_legacy(actions, "rc000002")
    fernet = get_soul_fernet()
    await rc.recompress_tick(actions.pool, fernet, clock=_Clock(), sleep=_no_sleep)
    stored = [bytes(r["raw_line"]) for r in await actions.pool.fetch(
        "SELECT raw_line FROM soul_lines WHERE anchor_sid='rc000002' ORDER BY line_idx")]
    again = await rc.recompress_tick(actions.pool, fernet, clock=_Clock(), sleep=_no_sleep)
    assert again["state"] == "complete" and again["rows_done"] == 4
    assert [bytes(r["raw_line"]) for r in await actions.pool.fetch(
        "SELECT raw_line FROM soul_lines WHERE anchor_sid='rc000002' ORDER BY line_idx")] == stored


async def test_it_resumes_from_the_saved_cursor_across_ticks(
    actions: Actions, tmp_path: Path,
) -> None:
    lines = await _ingest(actions, tmp_path, "rc000003", 7)
    await _make_legacy(actions, "rc000003")
    fernet = get_soul_fernet()
    first = await rc.recompress_tick(
        actions.pool, fernet, budget_secs=0.0015, batch_size=2,
        clock=_Clock(0.001), sleep=_no_sleep)
    assert first["state"] == "running" and 0 < first["rows_done"] < 7
    assert first["cursor"] is not None
    done = first
    for _ in range(10):
        done = await rc.recompress_tick(
            actions.pool, fernet, budget_secs=0.0015, batch_size=2,
            clock=_Clock(0.001), sleep=_no_sleep)
        if done["state"] == "complete":
            break
    assert done["state"] == "complete" and done["rows_done"] == 7
    assert await SoulStore(actions.pool).raw_lines("rc000003") == lines


async def test_rows_not_yet_sealed_are_left_for_the_encryption_pass(
    actions: Actions, tmp_path: Path,
) -> None:
    lines = await _ingest(actions, tmp_path, "rc000004", 3)
    await actions.pool.execute(
        "UPDATE soul_lines SET raw_line=convert_to($1, 'UTF8'), codec=0 "
        "WHERE anchor_sid='rc000004' AND line_idx=1", lines[1])
    await _make_legacy_except(actions, "rc000004", skip=1)
    record = await rc.recompress_tick(
        actions.pool, get_soul_fernet(), clock=_Clock(), sleep=_no_sleep)
    assert record["rows_done"] == 2
    plain = await actions.pool.fetchval(
        "SELECT raw_line FROM soul_lines WHERE anchor_sid='rc000004' AND line_idx=1")
    assert bytes(plain) == lines[1].encode()
    assert await actions.pool.fetchval(
        "SELECT codec FROM soul_lines WHERE anchor_sid='rc000004' AND line_idx=1") == 0


async def _make_legacy_except(actions: Actions, anchor: str, *, skip: int) -> None:
    fernet = get_soul_fernet()
    for r in await actions.pool.fetch(
            "SELECT line_idx, raw_line FROM soul_lines WHERE anchor_sid=$1", anchor):
        if r["line_idx"] == skip:
            continue
        line = open_line(fernet, bytes(r["raw_line"]))
        await actions.pool.execute(
            "UPDATE soul_lines SET raw_line=$1, codec=0 WHERE anchor_sid=$2 AND line_idx=$3",
            fernet.encrypt(line), anchor, r["line_idx"])


async def test_a_row_the_current_key_cannot_open_is_skipped_never_altered(
    actions: Actions, tmp_path: Path,
) -> None:
    lines = await _ingest(actions, tmp_path, "rc000005", 3)
    await _make_legacy(actions, "rc000005")
    stranger = Fernet(Fernet.generate_key()).encrypt(lines[1].encode())
    await actions.pool.execute(
        "UPDATE soul_lines SET raw_line=$1 WHERE anchor_sid='rc000005' AND line_idx=1", stranger)
    record = await rc.recompress_tick(
        actions.pool, get_soul_fernet(), clock=_Clock(), sleep=_no_sleep)
    assert record["rows_done"] == 2 and record["rows_skipped"] >= 1
    assert record["state"] == "complete"
    assert bytes(await actions.pool.fetchval(
        "SELECT raw_line FROM soul_lines WHERE anchor_sid='rc000005' AND line_idx=1")) == stranger


async def test_short_lines_are_marked_as_decided_so_they_are_never_revisited(
    actions: Actions, tmp_path: Path,
) -> None:
    await _ingest(actions, tmp_path, "rc000006", 3, size=40)
    await _make_legacy(actions, "rc000006")
    await rc.recompress_tick(actions.pool, get_soul_fernet(), clock=_Clock(), sleep=_no_sleep)
    assert await _codecs(actions, "rc000006") == [CODEC_PLAIN] * 3


async def test_a_row_already_wrapped_but_marked_unset_only_has_its_marker_fixed(
    actions: Actions, tmp_path: Path,
) -> None:
    await _ingest(actions, tmp_path, "rc000007", 2)
    before = [bytes(r["raw_line"]) for r in await actions.pool.fetch(
        "SELECT raw_line FROM soul_lines WHERE anchor_sid='rc000007' ORDER BY line_idx")]
    await actions.pool.execute("UPDATE soul_lines SET codec=0 WHERE anchor_sid='rc000007'")
    await rc.recompress_tick(actions.pool, get_soul_fernet(), clock=_Clock(), sleep=_no_sleep)
    after = [bytes(r["raw_line"]) for r in await actions.pool.fetch(
        "SELECT raw_line FROM soul_lines WHERE anchor_sid='rc000007' ORDER BY line_idx")]
    assert after == before
    assert await _codecs(actions, "rc000007") == [CODEC_ZS1] * 2


async def test_a_failing_batch_records_the_error_keeps_the_cursor_and_reraises(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _ingest(actions, tmp_path, "rc000008", 4)
    await _make_legacy(actions, "rc000008")

    async def boom(*_a: Any, **_k: Any) -> dict[str, Any]:
        raise RuntimeError("disk full")

    monkeypatch.setattr(rc, "_batch", boom)
    with pytest.raises(RuntimeError, match="disk full"):
        await rc.recompress_tick(actions.pool, get_soul_fernet(), clock=_Clock(), sleep=_no_sleep)
    record = rc.read_progress()
    assert record["state"] == "error" and "disk full" in record["last_error"]


async def test_the_pass_sleeps_after_every_batch_so_ingest_is_never_starved(
    actions: Actions, tmp_path: Path,
) -> None:
    await _ingest(actions, tmp_path, "rc000009", 5)
    await _make_legacy(actions, "rc000009")
    slept: list[float] = []

    async def sleep(secs: float) -> None:
        slept.append(secs)

    await rc.recompress_tick(
        actions.pool, get_soul_fernet(), batch_size=2, clock=_Clock(0.01), sleep=sleep)
    assert slept and all(s > 0 for s in slept)


def test_the_status_block_reads_without_a_database() -> None:
    assert rc.shape_compression({}, key_present=False)["state"] == "no_key"
    assert rc.shape_compression({}, key_present=True)["state"] == "pending"
    running = rc.shape_compression(
        {"state": "running", "rows_done": 10, "rows_remaining": 90, "bytes_before": 1000,
         "bytes_after": 400, "eta_seconds": 60}, key_present=True)
    assert running["ratio"] == 2.5 and running["eta_seconds"] == 60
    done = rc.shape_compression({"state": "complete", "rows_remaining": 7}, key_present=True)
    assert done["rows_remaining"] == 0 and done["eta_seconds"] is None


def test_the_kill_switch_stops_new_slices(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(rc.DISABLE_ENV, raising=False)
    assert rc.disabled() is False
    for value in ("1", "true", "YES"):
        monkeypatch.setenv(rc.DISABLE_ENV, value)
        assert rc.disabled() is True


async def test_the_worker_job_runs_a_slice_and_respects_the_kill_switch(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    from src.workers.arq_worker import soul_recompress_heartbeat

    await _ingest(actions, tmp_path, "rc000010", 3)
    await _make_legacy(actions, "rc000010")
    ctx = {"cascade": SimpleNamespace(actions=actions)}
    monkeypatch.setenv(rc.DISABLE_ENV, "1")
    assert await soul_recompress_heartbeat(ctx) == 0
    assert await _codecs(actions, "rc000010") == [0] * 3
    monkeypatch.delenv(rc.DISABLE_ENV)
    assert await soul_recompress_heartbeat(ctx) == 3
    assert await _codecs(actions, "rc000010") == [CODEC_ZS1] * 3


def test_the_codec_values_are_stable() -> None:
    # stored in the database: these numbers must never be reused for another meaning
    assert (soul_crypto.CODEC_UNSET, soul_crypto.CODEC_ZS1, soul_crypto.CODEC_PLAIN) == (0, 1, 2)
