"""THE BACKGROUND RE-ENCODE: compress the transcript lines that were stored before lines were
compressed. A soul line is sealed (Fernet) and ciphertext does not compress, so the lines are
now compressed first (`soul_crypto.pack_line`, zstd inside a versioned envelope). New writes
do that themselves; this pass rewrites the roughly two million rows that already exist, by
itself, resumable and throttled, the same shape as `soul_encrypt_progress`.

WORK QUEUE: `soul_lines.codec` (0 = not yet considered). The pass takes sealed rows with
codec 0 in primary-key order, opens each one, compresses it, seals it again and stores the
result with codec 1 (or codec 2 when the line is too small to be worth it or did not shrink,
so it is never visited twice). It never decrypts anything it cannot open: a row the current
key does not open is skipped and counted, never altered. Plain rows (no seal yet) are the
encryption pass's job and are left alone until it has sealed them.

SAFE AGAINST LIVE WRITES AND KEY ROTATION: every UPDATE is compare-and-swap on the exact
sealed bytes the pass read, so a row that a key rotation or another writer changed in the
meantime is simply left for the next sweep, never overwritten with a stale token. The line
itself, its hash and the chain are untouched (the hash is over the plain line).

SPACE: an UPDATE writes a new row version; Postgres reuses the old space for later writes
but only hands it back to the operating system on a table rewrite. The dumps and every
backup shrink at once (they hold live rows only); the live table's file shrinks at the next
VACUUM FULL or pg_repack, which is a deliberate, scheduled step, never this pass's.

PROGRESS: one small JSON record, like the encryption pass, with bytes before and after so
the saving is measured on the real rows, not guessed.

KILL SWITCH: set OSIRIS_SOUL_RECOMPRESS_DISABLED=1 in the worker's environment to stop new
slices (a slice already running finishes). Nothing is lost by stopping: the pass resumes
from its saved cursor."""
from __future__ import annotations

import asyncio
import json
import os
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg
from cryptography.fernet import InvalidToken, MultiFernet

from src.ingest import soul_dicts
from src.ingest.soul_crypto import (
    CODEC_PLAIN,
    CODEC_UNSET,
    CODEC_ZS1,
    CODEC_ZS2,
    FERNET_TOKEN_PREFIX,
    LINE_ENVELOPE_ZS1,
    LINE_ENVELOPE_ZS2,
    UnknownLineCodec,
    active_dictionary_id,
    pack_line,
    unpack_line,
)

_PROGRESS_ENV = "OSIRIS_SOUL_RECOMPRESS_PROGRESS_FILE"
_DEFAULT_PROGRESS_FILE = "~/.local/state/osiris/soul_recompress_progress.json"
DISABLE_ENV = "OSIRIS_SOUL_RECOMPRESS_DISABLED"

TICK_BUDGET_SECS = 20.0
BATCH_SIZE = 500
REPROBE_SECS = 6 * 3600.0
EXACT_COUNT_BELOW = 100_000
SAMPLE_PERCENT = 0.5

_SEALED = "substring(raw_line from 1 for 5) = $1"
DICT_RETRY_SECS = 3600.0     # how long to wait before trying to train a dictionary again


def _pending() -> str:
    """The work queue. Rows not yet considered (codec 0) always; once a trained dictionary is
    active, rows already compressed the plain way (codec 1) too, because the dictionary form is
    about half the size again."""
    codecs = f"codec = {CODEC_UNSET}"
    if active_dictionary_id() is not None:
        codecs = f"(codec = {CODEC_UNSET} OR codec = {CODEC_ZS1})"
    return f"{codecs} AND {_SEALED}"


def disabled() -> bool:
    return os.environ.get(DISABLE_ENV, "").strip().lower() in ("1", "true", "yes", "on")


def _progress_path() -> Path:
    env = os.environ.get(_PROGRESS_ENV)
    return Path(env).expanduser() if env else Path(_DEFAULT_PROGRESS_FILE).expanduser()


def read_progress() -> dict[str, Any]:
    try:
        return dict(json.loads(_progress_path().read_text()))
    except (OSError, ValueError):
        return {}


def _write_progress(record: dict[str, Any]) -> None:
    path = _progress_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(record, indent=2))
    tmp.replace(path)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def shape_compression(record: dict[str, Any], *, key_present: bool) -> dict[str, Any]:
    """The reader-facing block for the status route: pure, no database."""
    if not key_present:
        state = "no_key"
    elif not record:
        state = "pending"
    else:
        state = str(record.get("state") or "pending")
    before, after = int(record.get("bytes_before") or 0), int(record.get("bytes_after") or 0)
    return {
        "state": state,
        "rows_done": record.get("rows_done", 0),
        "rows_remaining": 0 if state == "complete" else record.get("rows_remaining"),
        "rows_skipped": record.get("rows_skipped", 0),
        "bytes_before": before,
        "bytes_after": after,
        "ratio": round(before / after, 2) if before and after else None,
        "eta_seconds": record.get("eta_seconds") if state == "running" else None,
        "updated_at": record.get("updated_at"),
        "last_error": record.get("last_error"),
    }


async def _initial_count(pool: asyncpg.Pool) -> tuple[int, bool]:
    """(rows to re-encode, whether that is an estimate). Small tables are counted; a big one
    is the planner's row estimate times the pending share of a small page sample, so the
    first status after a deploy never waits on a full scan."""
    reltuples = await pool.fetchval(
        "SELECT reltuples::bigint FROM pg_class WHERE oid = 'soul_lines'::regclass")
    if reltuples is None or reltuples < EXACT_COUNT_BELOW:
        n = await pool.fetchval(
            f"SELECT count(*) FROM soul_lines WHERE {_pending()}", FERNET_TOKEN_PREFIX)
        return int(n or 0), False
    sample = await pool.fetchrow(
        f"SELECT count(*) AS seen, count(*) FILTER (WHERE {_pending()}) AS pending "
        f"FROM soul_lines TABLESAMPLE SYSTEM ({SAMPLE_PERCENT})", FERNET_TOKEN_PREFIX)
    seen, pending = int(sample["seen"]), int(sample["pending"])
    share = pending / seen if seen else 1.0
    return int(int(reltuples) * share), True


async def _any_pending(pool: asyncpg.Pool) -> bool:
    return bool(await pool.fetchval(
        f"SELECT 1 FROM soul_lines WHERE {_pending()} LIMIT 1", FERNET_TOKEN_PREFIX))


def _envelope_codec(plain: bytes) -> int:
    """The codec a decrypted payload already is, from its own bytes."""
    if plain.startswith(LINE_ENVELOPE_ZS2):
        return CODEC_ZS2
    if plain.startswith(LINE_ENVELOPE_ZS1):
        return CODEC_ZS1
    return CODEC_PLAIN


async def _batch(
    pool: asyncpg.Pool, fernet: MultiFernet, cursor: list[Any] | None, batch_size: int,
) -> dict[str, Any]:
    """One keyset page after `cursor`. Returns rows rewritten, rows skipped (not openable
    with the current key), bytes before and after for the rewritten rows, the new cursor and
    whether the table is exhausted. A row is only ever rewritten into a form that is strictly
    smaller than the one it holds; otherwise just its marker is brought up to date."""
    await soul_dicts.ensure_dictionaries(pool)
    cols = "SELECT harness, anchor_sid, line_idx, raw_line, codec FROM soul_lines "
    if cursor is None:
        rows = await pool.fetch(
            cols + f"WHERE {_pending()} ORDER BY harness, anchor_sid, line_idx LIMIT $2",
            FERNET_TOKEN_PREFIX, batch_size)
    else:
        rows = await pool.fetch(
            cols + f"WHERE {_pending()} AND (harness, anchor_sid, line_idx) > ($2, $3, $4) "
            "ORDER BY harness, anchor_sid, line_idx LIMIT $5",
            FERNET_TOKEN_PREFIX, cursor[0], cursor[1], cursor[2], batch_size)
    if not rows:
        return {"rewritten": 0, "skipped": 0, "before": 0, "after": 0,
                "cursor": cursor, "exhausted": True}
    updates: list[tuple[bytes, int, str, str, int, bytes, int]] = []
    skipped = before = after = 0
    for r in rows:
        old = bytes(r["raw_line"])
        try:
            plain = fernet.decrypt(old)
            line = unpack_line(plain)
            payload, codec = pack_line(line)
        except (InvalidToken, UnknownLineCodec, LookupError):
            skipped += 1  # a key that does not open it, a newer format, or a missing dictionary
            continue
        if codec == CODEC_PLAIN or len(payload) >= len(plain):
            # nothing smaller is available: keep what the row holds, fix only its marker
            updates.append((old, _envelope_codec(plain), r["harness"], r["anchor_sid"],
                            r["line_idx"], old, r["codec"]))
            continue
        new = fernet.encrypt(payload)
        updates.append((new, codec, r["harness"], r["anchor_sid"], r["line_idx"], old,
                        r["codec"]))
        before += len(old)
        after += len(new)
    if updates:
        async with pool.acquire() as conn:
            await conn.executemany(
                "UPDATE soul_lines SET raw_line = $1, codec = $2 "
                "WHERE harness = $3 AND anchor_sid = $4 AND line_idx = $5 "
                "AND raw_line = $6 AND codec = $7", updates)
    last = rows[-1]
    return {"rewritten": len(updates), "skipped": skipped, "before": before, "after": after,
            "cursor": [last["harness"], last["anchor_sid"], last["line_idx"]],
            "exhausted": len(rows) < batch_size}


async def maybe_train_dictionary(pool: asyncpg.Pool, fernet: MultiFernet) -> dict[str, Any]:
    """Train the compression dictionary once, before the re-encode starts, so the existing rows
    go straight to the dictionary form instead of being rewritten twice. Does nothing when one
    exists, when switched off (OSIRIS_SOUL_DICT_DISABLED), or when the last try was less than
    `DICT_RETRY_SECS` ago (a young install has too few lines yet and simply keeps ZS1). Returns
    the training result or {"trained": False, "reason": ...}."""
    if soul_dicts.disabled():
        return {"trained": False, "reason": "switched off"}
    await soul_dicts.ensure_dictionaries(pool)
    if active_dictionary_id() is not None or await soul_dicts.active_dictionary_row(pool):
        return {"trained": False, "reason": "a dictionary is already active"}
    record = read_progress()
    now = time.time()
    if now - float(record.get("dict_attempt_epoch") or 0.0) < DICT_RETRY_SECS:
        return {"trained": False, "reason": "tried recently"}
    record["dict_attempt_epoch"] = now
    result = await soul_dicts.train_dictionary(pool, fernet)
    record["dict_result"] = result
    _write_progress(record)
    return result


async def recompress_tick(
    pool: asyncpg.Pool, fernet: MultiFernet, *,
    budget_secs: float = TICK_BUDGET_SECS, batch_size: int = BATCH_SIZE,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> dict[str, Any]:
    """One bounded slice of the pass, with the same contract as `encrypt_tick`: resumable
    from the cursor in the record, idempotent (a finished pass only re-probes every
    `REPROBE_SECS`), safe beside live ingest, at most half duty on the database. States:
    `running`, `complete`, `error` (the exception is re-raised so the worker's watch sees
    it; the next tick resumes from the same cursor)."""
    record = read_progress()
    now_wall = time.time()
    if record.get("state") == "complete":
        if now_wall - float(record.get("verified_epoch") or 0.0) < REPROBE_SECS:
            return record
        if not await _any_pending(pool):
            record["verified_epoch"] = now_wall
            record["updated_at"] = _now()
            _write_progress(record)
            return record
        # stragglers (or rows the key could not open): sweep again, keeping the totals
        record.update(state="running", cursor=None, sweep_rewritten=0)
    if not record.get("started_at"):
        total, estimated = await _initial_count(pool)
        record = {
            "state": "running", "started_at": _now(), "rows_total": total,
            "rows_estimated": estimated, "rows_done": 0, "rows_remaining": total,
            "rows_skipped": 0, "bytes_before": 0, "bytes_after": 0, "active_secs": 0.0,
            "cursor": None, "sweep_rewritten": 0, "rate_per_sec": None,
            "eta_seconds": None, "last_error": None,
        }
    record["state"] = "running"
    started = clock()
    try:
        while clock() - started < budget_secs:
            batch_started = clock()
            res = await _batch(pool, fernet, record.get("cursor"), batch_size)
            record["cursor"] = res["cursor"]
            record["rows_done"] = int(record.get("rows_done", 0)) + res["rewritten"]
            record["sweep_rewritten"] = int(record.get("sweep_rewritten", 0)) + res["rewritten"]
            record["rows_skipped"] = int(record.get("rows_skipped", 0)) + res["skipped"]
            record["bytes_before"] = int(record.get("bytes_before", 0)) + res["before"]
            record["bytes_after"] = int(record.get("bytes_after", 0)) + res["after"]
            if record["rows_done"] > int(record.get("rows_total", 0)):
                record["rows_total"] = record["rows_done"]  # an estimate that ran low
            record["rows_remaining"] = max(
                0, int(record.get("rows_total", 0)) - record["rows_done"])
            spent = clock() - batch_started
            record["active_secs"] = float(record.get("active_secs", 0.0)) + spent
            if record["active_secs"] > 0 and record["rows_done"]:
                rate = record["rows_done"] / record["active_secs"]
                record["rate_per_sec"] = round(rate, 1)
                # the duty cycle is one half, so wall-clock ETA is twice the busy time
                record["eta_seconds"] = int(record["rows_remaining"] / rate * 2)
            record["updated_at"] = _now()
            if res["exhausted"]:
                if int(record["sweep_rewritten"]) == 0:
                    # a whole sweep found nothing it could rewrite: done (rows the key cannot
                    # open stay as they are, counted in rows_skipped)
                    record.update(state="complete", rows_remaining=0, eta_seconds=0,
                                  verified_epoch=now_wall, last_error=None, cursor=None)
                    _write_progress(record)
                    break
                record.update(cursor=None, sweep_rewritten=0)  # confirm with one more sweep
            _write_progress(record)
            await sleep(spent)  # at most half the time on the database, ever
    except Exception as exc:
        record.update(state="error", last_error=f"{type(exc).__name__}: {exc}",
                      updated_at=_now())
        _write_progress(record)
        raise
    record["updated_at"] = _now()
    _write_progress(record)
    return record
