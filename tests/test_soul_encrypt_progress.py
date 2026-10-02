"""The background encryption pass (src.orchestrator.soul_encrypt_progress): resumable,
throttled, idempotent, and it never touches a row that already carries ciphertext.
Real Postgres via the `actions` fixture; the clock and sleep are injected so no test waits."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from cryptography.fernet import Fernet, MultiFernet
from src.actions.core import Actions
from src.ingest.soul_crypto import get_soul_fernet, is_encrypted
from src.ingest.soul_store import SoulStore
from src.orchestrator import soul_encrypt_progress as prog


def _lines(n: int) -> list[str]:
    out = []
    for i in range(n):
        ts = datetime(2026, 8, 17, 12, 0, i, tzinfo=UTC).isoformat()
        out.append(json.dumps({"type": "user", "timestamp": ts, "message": {"content": f"l{i}"}}))
    return out


class _Clock:
    """Advances by `step` on every read, so a tick's budget is counted in clock reads."""

    def __init__(self, step: float = 1.0) -> None:
        self.now = 0.0
        self.step = step

    def __call__(self) -> float:
        self.now += self.step
        return self.now


async def _no_sleep(_secs: float) -> None:
    return None


async def _seed_plaintext(actions: Actions, tmp_path: Path, anchor: str, n: int) -> list[bytes]:
    """Ingests `n` lines, then overwrites every row with its plaintext (the shape of a row
    written before any key existed). Returns the plaintext lines in order."""
    store = SoulStore(actions.pool)
    p = tmp_path / f"{anchor}.jsonl"
    p.write_text("\n".join(_lines(n)) + "\n")
    await store.ingest_path(str(p), anchor)
    fernet = get_soul_fernet()
    plain = []
    for i in range(n):
        enc = await actions.pool.fetchval(
            "SELECT raw_line FROM soul_lines WHERE anchor_sid=$1 AND line_idx=$2", anchor, i)
        text = fernet.decrypt(bytes(enc))
        plain.append(text)
        await actions.pool.execute(
            "UPDATE soul_lines SET raw_line=$1 WHERE anchor_sid=$2 AND line_idx=$3",
            text, anchor, i)
    return plain


async def _rows(actions: Actions, anchor: str) -> list[bytes]:
    got = await actions.pool.fetch(
        "SELECT raw_line FROM soul_lines WHERE anchor_sid=$1 ORDER BY line_idx", anchor)
    return [bytes(r["raw_line"]) for r in got]


def test_shape_encryption_states_without_a_database() -> None:
    assert prog.shape_encryption({}, key_present=False)["state"] == "no_key"
    pending = prog.shape_encryption({}, key_present=True, total_estimate=900)
    assert pending["state"] == "pending"
    assert pending["rows_total"] == 900
    assert pending["rows_remaining"] is None
    running = prog.shape_encryption(
        {"state": "running", "rows_done": 10, "rows_remaining": 90, "rows_total": 100,
         "eta_seconds": 30, "rate_per_sec": 5.0}, key_present=True)
    assert running["eta_seconds"] == 30
    done = prog.shape_encryption(
        {"state": "complete", "rows_remaining": 7, "eta_seconds": 5}, key_present=True)
    assert done["rows_remaining"] == 0
    assert done["eta_seconds"] is None


def test_a_missing_or_unreadable_record_reads_as_nothing_started(tmp_path: Path) -> None:
    assert prog.read_progress() == {}
    prog._progress_path().parent.mkdir(parents=True, exist_ok=True)
    prog._progress_path().write_text("{not json")
    assert prog.read_progress() == {}


async def test_a_tick_encrypts_plaintext_rows_and_reports_progress(
    actions: Actions, tmp_path: Path,
) -> None:
    plain = await _seed_plaintext(actions, tmp_path, "encpass01", 5)
    fernet = get_soul_fernet()

    record = await prog.encrypt_tick(
        actions.pool, fernet, clock=_Clock(0.001), sleep=_no_sleep)

    assert record["state"] == "complete"
    assert record["rows_done"] == 5
    assert record["rows_remaining"] == 0
    assert record["rows_total"] == 5
    rows = await _rows(actions, "encpass01")
    assert all(is_encrypted(r) for r in rows)
    assert [fernet.decrypt(r) for r in rows] == plain
    assert prog.read_progress()["state"] == "complete"


async def test_it_resumes_from_the_saved_cursor_across_ticks(
    actions: Actions, tmp_path: Path,
) -> None:
    await _seed_plaintext(actions, tmp_path, "encpass02", 7)
    fernet = get_soul_fernet()

    # a one-batch budget: the clock jumps past it after the first batch
    first = await prog.encrypt_tick(
        actions.pool, fernet, batch_size=3, budget_secs=1.5, clock=_Clock(1.0), sleep=_no_sleep)
    assert first["state"] == "running"
    assert 0 < first["rows_done"] < 7
    assert first["cursor"] is not None
    assert first["rows_remaining"] == 7 - first["rows_done"]

    # a later tick (a fresh call, as after a worker restart) picks up from the record
    final = first
    for _ in range(10):
        final = await prog.encrypt_tick(
            actions.pool, fernet, batch_size=3, budget_secs=1.5, clock=_Clock(1.0),
            sleep=_no_sleep)
        if final["state"] == "complete":
            break
    assert final["state"] == "complete"
    assert final["rows_done"] == 7
    assert all(is_encrypted(r) for r in await _rows(actions, "encpass02"))


async def test_a_finished_pass_is_a_cheap_no_op_until_the_reprobe_interval(
    actions: Actions, tmp_path: Path,
) -> None:
    await _seed_plaintext(actions, tmp_path, "encpass03", 3)
    fernet = get_soul_fernet()
    await prog.encrypt_tick(actions.pool, fernet, clock=_Clock(0.001), sleep=_no_sleep)

    # a plaintext row appears afterwards; within the interval the tick does not even look
    await actions.pool.execute(
        "UPDATE soul_lines SET raw_line=$1 WHERE anchor_sid='encpass03' AND line_idx=0",
        b'{"late": "plaintext"}')
    quiet = await prog.encrypt_tick(actions.pool, fernet, clock=_Clock(0.001), sleep=_no_sleep)
    assert quiet["state"] == "complete"
    assert not is_encrypted((await _rows(actions, "encpass03"))[0])

    # past the interval the re-probe finds it and a fresh pass encrypts it
    record = prog.read_progress()
    record["verified_epoch"] = 0.0
    prog._write_progress(record)
    again = await prog.encrypt_tick(actions.pool, fernet, clock=_Clock(0.001), sleep=_no_sleep)
    assert again["state"] == "complete"
    assert again["rows_done"] == 1
    assert is_encrypted((await _rows(actions, "encpass03"))[0])


async def test_a_row_already_carrying_ciphertext_under_another_key_is_never_touched(
    actions: Actions, tmp_path: Path,
) -> None:
    """The manual migration reads "cannot decrypt" as "plaintext" and would encrypt that
    token a second time; this pass selects on the ciphertext prefix instead."""
    await _seed_plaintext(actions, tmp_path, "encpass04", 2)
    foreign = Fernet(Fernet.generate_key()).encrypt(b"belongs to another key")
    await actions.pool.execute(
        "UPDATE soul_lines SET raw_line=$1 WHERE anchor_sid='encpass04' AND line_idx=1", foreign)

    await prog.encrypt_tick(actions.pool, get_soul_fernet(), clock=_Clock(0.001), sleep=_no_sleep)

    rows = await _rows(actions, "encpass04")
    assert is_encrypted(rows[0])
    assert rows[1] == foreign


async def test_the_pass_sleeps_after_every_batch_so_ingest_is_never_starved(
    actions: Actions, tmp_path: Path,
) -> None:
    await _seed_plaintext(actions, tmp_path, "encpass05", 6)
    slept: list[float] = []

    async def _sleep(secs: float) -> None:
        slept.append(secs)

    await prog.encrypt_tick(
        actions.pool, get_soul_fernet(), batch_size=2, clock=_Clock(0.5), sleep=_sleep)

    assert slept, "a batch was processed but the pass never yielded"
    assert all(s > 0 for s in slept)


async def test_cold_tier_blobs_still_in_plaintext_are_encrypted_too(
    actions: Actions, tmp_path: Path,
) -> None:
    store = SoulStore(actions.pool)
    p = tmp_path / "cold.jsonl"
    p.write_text("\n".join(_lines(4)) + "\n")
    await store.ingest_path(str(p), "encpass06")
    await store.fold_to_cold_tier("encpass06")
    fernet = get_soul_fernet()
    blob = await actions.pool.fetchval(
        "SELECT content_gzip FROM soul_lines_cold WHERE anchor_sid='encpass06'")
    plain_gzip = fernet.decrypt(bytes(blob))
    await actions.pool.execute(
        "UPDATE soul_lines_cold SET content_gzip=$1 WHERE anchor_sid='encpass06'", plain_gzip)

    record = await prog.encrypt_tick(
        actions.pool, fernet, clock=_Clock(0.001), sleep=_no_sleep)

    assert record["state"] == "complete"
    after = bytes(await actions.pool.fetchval(
        "SELECT content_gzip FROM soul_lines_cold WHERE anchor_sid='encpass06'"))
    assert is_encrypted(after)
    assert fernet.decrypt(after) == plain_gzip
    # and the session still reads back through the cold tier
    assert await store.raw_lines("encpass06") is not None


async def test_a_failing_batch_records_the_error_keeps_the_cursor_and_reraises(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _seed_plaintext(actions, tmp_path, "encpass07", 4)
    fernet = get_soul_fernet()

    async def _boom(*_a: Any, **_k: Any) -> Any:
        raise RuntimeError("database went away")

    real_hot_batch = prog._hot_batch
    monkeypatch.setattr(prog, "_hot_batch", _boom)
    with pytest.raises(RuntimeError, match="database went away"):
        await prog.encrypt_tick(actions.pool, fernet, clock=_Clock(0.001), sleep=_no_sleep)
    saved = prog.read_progress()
    assert saved["state"] == "error"
    assert "database went away" in saved["last_error"]

    # restore only the patched function: monkeypatch.undo() would also undo the suite's own
    # autouse state-file redirect and write a stray record into the real state directory
    monkeypatch.setattr(prog, "_hot_batch", real_hot_batch)
    resumed = await prog.encrypt_tick(actions.pool, fernet, clock=_Clock(0.001), sleep=_no_sleep)
    assert resumed["state"] == "complete"
    assert resumed["last_error"] is None


async def test_estimate_total_rows_is_a_catalog_read_never_a_scan(actions: Actions) -> None:
    value = await prog.estimate_total_rows(actions.pool)
    assert value is None or value >= 0


async def test_the_worker_job_runs_a_tick_and_reports_rows_encrypted(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.workers.arq_worker import soul_encrypt_heartbeat

    await _seed_plaintext(actions, tmp_path, "encpass08", 3)
    ctx = {"cascade": SimpleNamespace(actions=actions)}

    assert await soul_encrypt_heartbeat(ctx) == 3
    assert await soul_encrypt_heartbeat(ctx) == 0  # finished: a cheap no-op


async def test_the_worker_job_fails_loudly_when_there_is_no_key(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.ingest import soul_crypto
    from src.workers.arq_worker import soul_encrypt_heartbeat

    def _missing(*_a: Any, **_k: Any) -> MultiFernet:
        raise soul_crypto.SoulKeyMissing("no key")

    # like the boot gate itself: never a quiet zero, the watch sees the failure every tick
    monkeypatch.setattr(soul_crypto, "get_soul_fernet", _missing)
    with pytest.raises(soul_crypto.SoulKeyMissing):
        await soul_encrypt_heartbeat({"cascade": SimpleNamespace(actions=actions)})
    assert prog.read_progress() == {}


class _FakePool:
    """Canned answers for the initial-count queries, so the sampling arithmetic is testable
    without a two-million-row table."""

    def __init__(self, *, reltuples: int | None, seen: int, plain: int, cold: int = 0) -> None:
        self.reltuples, self.seen, self.plain, self.cold = reltuples, seen, plain, cold
        self.fetchval_sql: list[str] = []

    async def fetchval(self, sql: str, *args: Any) -> Any:
        self.fetchval_sql.append(sql)
        if "soul_lines_cold" in sql:
            return self.cold
        if "reltuples" in sql:
            return self.reltuples
        return 12345  # an exact hot count, only reached for a small table

    async def fetchrow(self, sql: str, *args: Any) -> dict[str, int]:
        assert "TABLESAMPLE" in sql
        return {"seen": self.seen, "plain": self.plain}


async def test_a_big_table_is_estimated_from_a_sample_never_counted_in_full() -> None:
    pool = _FakePool(reltuples=2_000_000, seen=10_000, plain=9_800, cold=3)

    total, estimated = await prog._initial_counts(pool)  # type: ignore[arg-type]

    assert estimated is True
    assert total == int(2_000_000 * 0.98) + 3
    # the only full-table count issued is the small cold-tier one
    assert not any("count(*) FROM soul_lines WHERE" in q for q in pool.fetchval_sql)


async def test_a_small_or_never_analyzed_table_is_counted_exactly() -> None:
    for reltuples in (None, 500):
        pool = _FakePool(reltuples=reltuples, seen=0, plain=0, cold=2)
        total, estimated = await prog._initial_counts(pool)  # type: ignore[arg-type]
        assert (total, estimated) == (12345 + 2, False)


async def test_an_empty_sample_is_read_as_all_plaintext_the_safe_way_round() -> None:
    pool = _FakePool(reltuples=1_000_000, seen=0, plain=0)
    total, estimated = await prog._initial_counts(pool)  # type: ignore[arg-type]
    assert (total, estimated) == (1_000_000, True)


async def test_the_first_tick_on_a_big_table_marks_the_totals_as_an_estimate_and_corrects_them(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _seed_plaintext(actions, tmp_path, "encpass09", 3)

    async def _low_estimate(pool: Any, **_k: Any) -> tuple[int, bool]:
        return 1, True  # the estimate ran low: 3 rows really need encrypting

    monkeypatch.setattr(prog, "_initial_counts", _low_estimate)
    first = await prog.encrypt_tick(
        actions.pool, get_soul_fernet(), batch_size=2, budget_secs=1.5, clock=_Clock(1.0),
        sleep=_no_sleep)
    assert first["rows_estimated"] is True
    assert prog.shape_encryption(first, key_present=True)["rows_estimated"] is True

    final = first
    for _ in range(5):
        final = await prog.encrypt_tick(
            actions.pool, get_soul_fernet(), batch_size=2, budget_secs=1.5,
            clock=_Clock(1.0), sleep=_no_sleep)
    assert final["state"] == "complete"
    assert final["rows_total"] >= final["rows_done"] == 3  # never reads "3 of 1"
    assert prog.shape_encryption(final, key_present=True)["rows_estimated"] is False
