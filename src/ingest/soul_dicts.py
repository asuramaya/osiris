"""THE TRAINED COMPRESSION DICTIONARIES for soul lines (the ZS2 format, `soul_crypto`).

A line compressed with a dictionary can only be opened by a process that has that dictionary in
memory. This module owns the two halves of that:

- `ensure_dictionaries(pool)`: every reader of stored lines calls it first. One primary-key query
  fetches only the dictionaries this process has not loaded yet, opens them with the soul key and
  registers them, so a dictionary trained by the worker is picked up by the next read anywhere,
  with no restart and no timer to wait for.
- `train_dictionary(pool, fernet)`: samples real lines, trains a dictionary and stores it SEALED
  in `soul_dicts` (it is derived from private transcripts), then makes it the one new lines use.

A dictionary row is never rewritten once stored; a rotation of the soul key re-seals it with the
lines (`soul_store.rewrap_soul_lines_key`)."""
from __future__ import annotations

import os
from typing import Any

import asyncpg
from cryptography.fernet import InvalidToken, MultiFernet

from src.ingest.soul_crypto import (
    FERNET_TOKEN_PREFIX,
    SoulKeyMissing,
    get_soul_fernet,
    known_dictionary_ids,
    register_dictionary,
    unpack_line,
)

DISABLE_ENV = "OSIRIS_SOUL_DICT_DISABLED"
MIN_SAMPLE_LINES = 5_000       # fewer lines than this and a dictionary would learn noise
SAMPLE_LINES = 20_000          # lines a dictionary is trained on
DICT_BYTES = 112_640           # 110 KiB, zstd's own recommended order of size
MIN_LINE_BYTES = 256           # tiny lines are never compressed, so never train on them


def disabled() -> bool:
    return os.environ.get(DISABLE_ENV, "").strip().lower() in ("1", "true", "yes", "on")


async def ensure_dictionaries(pool: asyncpg.Pool) -> None:
    """Load every stored dictionary this process does not have yet. A cheap no-op (one indexed
    query returning no rows) once everything is loaded; does nothing without a key (nothing
    sealed can be opened either)."""
    rows = await pool.fetch(
        "SELECT id, sealed_dict, active FROM soul_dicts WHERE id <> ALL($1::int[]) ORDER BY id",
        sorted(known_dictionary_ids()))
    if not rows:
        return
    try:
        fernet = get_soul_fernet()
    except SoulKeyMissing:
        return
    for r in rows:
        try:
            raw = fernet.decrypt(bytes(r["sealed_dict"]))
        except InvalidToken:
            continue  # not openable with the configured key: leave it, never guess
        register_dictionary(int(r["id"]), raw, active=bool(r["active"]))


async def active_dictionary_row(pool: asyncpg.Pool) -> int | None:
    value = await pool.fetchval(
        "SELECT id FROM soul_dicts WHERE active ORDER BY id DESC LIMIT 1")
    return int(value) if value is not None else None


async def _sample_lines(pool: asyncpg.Pool, fernet: MultiFernet, want: int) -> list[bytes]:
    reltuples = await pool.fetchval(
        "SELECT reltuples::bigint FROM pg_class WHERE oid = 'soul_lines'::regclass")
    total = max(int(reltuples or 0), want)
    # read roughly three times the wanted lines' worth of pages, spread over the whole table
    pct = min(100.0, max(0.01, want * 3 / total * 100))
    rows = await pool.fetch(
        "SELECT raw_line FROM soul_lines TABLESAMPLE SYSTEM ($1) "
        "WHERE substring(raw_line from 1 for 5) = $2 LIMIT $3",
        pct, FERNET_TOKEN_PREFIX, want * 2)
    lines: list[bytes] = []
    for r in rows:
        try:
            line = unpack_line(fernet.decrypt(bytes(r["raw_line"])))
        except (InvalidToken, ValueError, LookupError):
            continue
        if len(line) >= MIN_LINE_BYTES:
            lines.append(line)
        if len(lines) >= want:
            break
    return lines


async def train_dictionary(
    pool: asyncpg.Pool, fernet: MultiFernet, *, sample_lines: int | None = None,
    dict_bytes: int | None = None, min_lines: int | None = None,
) -> dict[str, Any]:
    """Train and store a dictionary. `{"trained": False, "reason": ...}` when the store has too
    few lines to learn from (a young install keeps ZS1 until it has enough)."""
    import zstandard

    sample_lines = SAMPLE_LINES if sample_lines is None else sample_lines
    dict_bytes = DICT_BYTES if dict_bytes is None else dict_bytes
    min_lines = MIN_SAMPLE_LINES if min_lines is None else min_lines
    await ensure_dictionaries(pool)
    lines = await _sample_lines(pool, fernet, sample_lines)
    if len(lines) < min_lines:
        return {"trained": False, "reason": f"only {len(lines)} usable lines (need {min_lines})"}
    samples: list[bytes | bytearray | memoryview[int]] = list(lines)
    raw = zstandard.train_dictionary(dict_bytes, samples).as_bytes()
    sealed = fernet.encrypt(raw)
    async with pool.acquire() as conn, conn.transaction():
        new_id = await conn.fetchval(
            "INSERT INTO soul_dicts (sealed_dict, trained_rows, sample_bytes, active) "
            "VALUES ($1, $2, $3, true) RETURNING id",
            sealed, len(lines), sum(len(x) for x in lines))
        await conn.execute("UPDATE soul_dicts SET active = false WHERE id <> $1", new_id)
    register_dictionary(int(new_id), raw, active=True)
    return {"trained": True, "id": int(new_id), "lines": len(lines),
            "dict_bytes": len(raw), "sample_bytes": sum(len(x) for x in lines)}
