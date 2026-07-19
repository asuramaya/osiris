"""The transcript store — a normalized index of per-turn harness records.

Eats TurnRows from any adapter into harness_sessions/harness_turns (SQL sidecar), and
reads them back as a ModelReading for identity resolution. The HARNESS FILE stays
authoritative (the adapter records source_ref per turn); the store is a DERIVED
fast-read layer — one evidence grade lower than direct observation, so high-stakes
verdicts (the swap banner shown to the operator) can re-probe the source on demand.

MOUNT-TIME EAT (Slice 1): discover_and_ingest() is called from mount() before
resolve_identity. It tries each adapter in order; the first that discovers a session
ingests its turns and returns the model reading. resolve_identity consumes the reading
preferentially over its own JSONL probe — non-Claude minds mount RESOLVED.

MINER-TICK BACKFILL (Slice 3): the miner will call ingest_since() on a schedule to keep
the store current without a mount. Until then, mount-time is the only ingest trigger.
"""
from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

import asyncpg

from src.ingest.harness import (
    HarnessAdapter,
    ModelReading,
    SessionLocator,
    TurnRow,
)

_SYNTHETIC = "<synthetic>"


def _reading_from_turns(
    turns: Sequence[TurnRow], method: str, anchor_sid: str,
) -> ModelReading:
    """Pure: reduce a turn stream to the model-identity reading."""
    history: list[str] = []
    current: str | None = None
    observed_at: datetime | None = None
    deliberate = False
    for t in turns:
        if t.role != "assistant" or not t.model or t.model == _SYNTHETIC:
            continue
        if t.is_summary:
            continue
        if t.model not in history:
            history.append(t.model)
        current = t.model
        observed_at = t.recorded_at or observed_at
        if t.swap_deliberate:
            deliberate = True
    return ModelReading(
        current=current, history=tuple(history), deliberate=deliberate,
        observed_at=observed_at, method=method, anchor_sid=anchor_sid or None,
    )


class TranscriptStore:
    """The normalized transcript index, fed by adapters, read by identity."""

    def __init__(
        self, pool: asyncpg.Pool, adapters: list[HarnessAdapter] | None = None,
    ) -> None:
        self.pool = pool
        from src.ingest.harness.claude_jsonl import ClaudeJsonlAdapter
        from src.ingest.harness.crush_sqlite import CrushSqliteAdapter
        self._default_adapters = adapters or [
            ClaudeJsonlAdapter(), CrushSqliteAdapter(),
        ]

    async def discover_and_ingest(
        self, *, cwd: str | None, job_dir: str | None,
        adapters: list[HarnessAdapter] | None = None,
    ) -> ModelReading | None:
        """Try each adapter; ingest from the first that discovers a session.

        Returns the model reading for identity resolution (also stored for later async
        queries by Slice 2 readers). None when no adapter recognizes the session — the
        caller falls through to its legacy probe."""
        for adapter in (adapters or self._default_adapters):
            try:
                locator = adapter.discover(cwd=cwd, job_dir=job_dir)
            except Exception:  # noqa: BLE001 — an adapter failure must never block mount
                continue
            if locator is None:
                continue
            turns = list(adapter.read_turns(locator))
            if not turns:
                continue
            await self._upsert(locator, turns)
            return _reading_from_turns(turns, locator.harness, locator.anchor_sid)
        return None

    async def model_of_session(
        self, harness: str, anchor_sid: str,
    ) -> ModelReading | None:
        """Read a model reading back from the store (for Slice 2's async readers)."""
        rows = await self.pool.fetch(
            "SELECT turn_idx, role, model, provider, recorded_at, is_summary, "
            "       swap_deliberate "
            "FROM harness_turns WHERE harness=$1 AND anchor_sid=$2 "
            "ORDER BY turn_idx ASC",
            harness, anchor_sid,
        )
        if not rows:
            return None
        turns = [
            TurnRow(
                turn_idx=r["turn_idx"], role=r["role"], model=r["model"],
                provider=r["provider"], recorded_at=r["recorded_at"],
                is_summary=r["is_summary"], swap_deliberate=r["swap_deliberate"],
            )
            for r in rows
        ]
        return _reading_from_turns(turns, harness, anchor_sid)

    async def last_usage_of_session(
        self, harness: str, anchor_sid: str,
    ) -> dict[str, int | None] | None:
        """The most recent assistant turn's token usage — for context_lens's chrome glance.

        Returns the {input, output, cache_read, cache_write} the harness recorded for the
        latest turn, or None when the store has no usage for this session (a harness that
        doesn't carry per-turn tokens — Crush — or an unobserved session)."""
        row = await self.pool.fetchrow(
            "SELECT tokens_in, tokens_out, cache_read, cache_write "
            "FROM harness_turns WHERE harness=$1 AND anchor_sid=$2 "
            "  AND role='assistant' AND is_summary=false "
            "  AND tokens_in IS NOT NULL "
            "ORDER BY turn_idx DESC LIMIT 1",
            harness, anchor_sid,
        )
        if row is None:
            return None
        return {
            "input": row["tokens_in"],
            "output": row["tokens_out"],
            "cache_read": row["cache_read"],
            "cache_creation": row["cache_write"],
        }

    async def usage_of_session(
        self, harness: str, anchor_sid: str,
    ) -> dict[str, int] | None:
        """Aggregate token usage across all turns — for wake_cost / cost telemetry.

        Sums tokens across every assistant turn. None when the store has no usage for
        this session. The harness's own dollars (where available) stay in the source —
        the store records tokens (the fact), not prices (the invention)."""
        row = await self.pool.fetchrow(
            "SELECT COALESCE(SUM(tokens_in), 0) AS input, "
            "       COALESCE(SUM(tokens_out), 0) AS output, "
            "       COALESCE(SUM(cache_read), 0) AS cache_read, "
            "       COALESCE(SUM(cache_write), 0) AS cache_creation "
            "FROM harness_turns WHERE harness=$1 AND anchor_sid=$2 "
            "  AND role='assistant' AND is_summary=false AND tokens_in IS NOT NULL",
            harness, anchor_sid,
        )
        if row is None or row["input"] == 0:
            return None
        return {
            "input": int(row["input"]),
            "output": int(row["output"]),
            "cache_read": int(row["cache_read"]),
            "cache_creation": int(row["cache_creation"]),
        }

    async def backfill(
        self, *, adapters: list[HarnessAdapter] | None = None,
        limit_per_adapter: int = 0,
    ) -> dict[str, int]:
        """The periodic sweep — eat every harness session on disk into the store.

        This is the "Osiris eats transcripts" primitive: the miner's tick calls it, the
        operator's backfill command calls it. Idempotent (ON CONFLICT DO NOTHING on every
        turn). Returns per-adapter counts of sessions + turns ingested.

        `limit_per_adapter` caps each adapter's sweep (0 = unlimited) — a first run over
        a fleet's whole history should not hog the loop. Free work, but bounded."""
        counts: dict[str, int] = {}
        for adapter in (adapters or self._default_adapters):
            sessions = 0
            for locator in adapter.enumerate():
                turns = list(adapter.read_turns(locator))
                if not turns:
                    continue
                try:
                    await self._upsert(locator, turns)
                    sessions += 1
                except Exception:  # noqa: BLE001 — one bad session must not abort the sweep
                    continue
                if limit_per_adapter and sessions >= limit_per_adapter:
                    break
            counts[adapter.name] = sessions
        return counts

    async def _upsert(
        self, locator: SessionLocator, turns: list[TurnRow],
    ) -> None:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO harness_sessions "
                    "   (anchor_sid, harness, session_id, cwd, project, source_path, "
                    "    last_ingested_at, last_turn_idx) "
                    "VALUES ($1, $2, $3, $4, $5, $6, now(), $7) "
                    "ON CONFLICT (harness, anchor_sid) DO UPDATE "
                    "   SET last_ingested_at = now(), "
                    "       last_turn_idx = GREATEST("
                    "           harness_sessions.last_turn_idx, EXCLUDED.last_turn_idx), "
                    "       cwd = COALESCE(EXCLUDED.cwd, harness_sessions.cwd), "
                    "       project = COALESCE(EXCLUDED.project, harness_sessions.project)",
                    locator.anchor_sid, locator.harness, locator.session_id,
                    locator.cwd, locator.project, locator.source_path,
                    max((t.turn_idx for t in turns), default=0),
                )
                # idempotent: ON CONFLICT skips turns already ingested (re-mount, re-probe)
                await conn.executemany(
                    "INSERT INTO harness_turns "
                    "   (anchor_sid, harness, turn_idx, role, model, provider, "
                    "    tokens_in, tokens_out, cache_read, cache_write, cost_usd, "
                    "    duration_ms, recorded_at, is_summary, swap_deliberate, source_ref) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, "
                    "        $15, $16) "
                    "ON CONFLICT (harness, anchor_sid, turn_idx) DO NOTHING",
                    [
                        (locator.anchor_sid, locator.harness, t.turn_idx, t.role,
                         t.model, t.provider, t.tokens_in, t.tokens_out,
                         t.cache_read, t.cache_write, t.cost_usd, t.duration_ms,
                         t.recorded_at, t.is_summary, t.swap_deliberate, t.source_ref)
                        for t in turns
                    ],
                )
