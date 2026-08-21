"""The transcript store — a normalized index of per-turn harness records.

Eats TurnRows from any adapter into harness_sessions/harness_turns (SQL sidecar), and
reads them back as a ModelReading for identity resolution. The HARNESS FILE stays
authoritative (the adapter records source_ref per turn); the store is a DERIVED
fast-read layer — one evidence grade lower than direct observation, so high-stakes
verdicts (the swap banner shown to the operator) can re-probe the source on demand.

MOUNT-TIME EAT (Slice 1, completed by Slice 2 — the JSONL-fallback removal, task #29):
identity_reading() is called from every identity path (mount, _reattach, the automount
whisper) before resolve_identity. It tries each adapter in order; the first that
discovers a session ingests its turns and returns the model reading. This is the ONLY
model-observation lane — resolve_identity no longer carries a JSONL probe of its own;
non-Claude minds mount RESOLVED and a storeless call resolves no observed model.

MINER-TICK BACKFILL (Slice 3): the miner will call ingest_since() on a schedule to keep
the store current without a mount. Until then, mount-time is the only ingest trigger.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg

from src.ingest.harness import (
    HarnessAdapter,
    ModelReading,
    SessionLocator,
    TurnRow,
)

_SYNTHETIC = "<synthetic>"


async def identity_reading(
    pool: asyncpg.Pool, *, cwd: str | None, job_dir: str | None,
    root: Path | None = None, transcript_path: str | None = None,
) -> ModelReading | None:
    """The identity callers' one door to the store — discover_and_ingest, FAIL-OPEN.

    mount(), _reattach() and the automount whisper all need the same thing: this
    session's model reading if the store can produce one, and NO EXCEPTION otherwise —
    a store failure must never block an identity path (the whisper's own law). Since
    the JSONL-fallback removal (task #29) this is the ONLY observation lane; None
    degrades resolve_identity honestly to the caller's self-report.

    `transcript_path` (thread 7304bfd8) passes a caller-KNOWN location straight through
    to discover_and_ingest's explicit-path lane — previously captured by mount()'s/
    automount()'s own signature and used only for revisit resolution, never consulted
    here at all."""
    try:
        return await TranscriptStore(pool).discover_and_ingest(
            cwd=cwd, job_dir=job_dir, root=root, transcript_path=transcript_path)
    except Exception:  # noqa: BLE001 — identity must resolve even with the store down
        return None


def _stat(source_path: str) -> tuple[datetime | None, int | None]:
    """The source's freshness witness + on-disk size (one bare stat, kept in a sync
    helper for the blocking-call lint). (None, None) when the source vanished between
    discovery and here. The size feeds the overhead lens's byte fallback — neo's honest
    proxy for a channel that reports no token usage."""
    try:
        st = Path(source_path).stat()
        return datetime.fromtimestamp(st.st_mtime, tz=UTC), st.st_size
    except OSError:
        return None, None


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


# The overhead lens's turn aggregates. Token sums take live assistant turns only (a
# summary line quotes the past; counting it would double-bill every compaction);
# reminders ride user turns (NULL elsewhere); compactions count the compact-summary
# lines themselves.
_TURN_FILTER = "role='assistant' AND NOT is_summary"
_TURN_AGG_SQL = (
    "SELECT harness, anchor_sid, "
    f"       COALESCE(SUM(tokens_in)   FILTER (WHERE {_TURN_FILTER}), 0) AS tin, "
    f"       COALESCE(SUM(tokens_out)  FILTER (WHERE {_TURN_FILTER}), 0) AS tout, "
    f"       COALESCE(SUM(cache_read)  FILTER (WHERE {_TURN_FILTER}), 0) AS cread, "
    f"       COALESCE(SUM(cache_write) FILTER (WHERE {_TURN_FILTER}), 0) AS cwrite, "
    f"       COUNT(*) FILTER (WHERE {_TURN_FILTER} AND tokens_in IS NOT NULL) AS calls, "
    "       COALESCE(SUM(reminders), 0) AS reminders, "
    "       COUNT(*) FILTER (WHERE is_compaction) AS compactions "
    "FROM harness_turns"
)
_T_FILTER = "t.role='assistant' AND NOT t.is_summary"
_OVERHEAD_SQL = (
    "SELECT s.anchor_sid, s.channel, s.agent_type, s.session_id, s.source_bytes, "
    f"       COALESCE(SUM(t.tokens_in)   FILTER (WHERE {_T_FILTER}), 0) AS tin, "
    f"       COALESCE(SUM(t.tokens_out)  FILTER (WHERE {_T_FILTER}), 0) AS tout, "
    f"       COALESCE(SUM(t.cache_read)  FILTER (WHERE {_T_FILTER}), 0) AS cread, "
    f"       COALESCE(SUM(t.cache_write) FILTER (WHERE {_T_FILTER}), 0) AS cwrite, "
    f"       COUNT(*) FILTER (WHERE {_T_FILTER} AND t.tokens_in IS NOT NULL) AS calls, "
    "       COALESCE(SUM(t.reminders), 0) AS reminders, "
    "       COUNT(*) FILTER (WHERE t.is_compaction) AS compactions "
    "FROM harness_sessions s "
    "LEFT JOIN harness_turns t ON t.harness=s.harness AND t.anchor_sid=s.anchor_sid"
)


def _overhead_shape(
    primary: list[dict[str, Any]], channels: list[dict[str, Any]],
) -> dict[str, Any]:
    """Pure: neo's session math over per-channel aggregates. Visible = the primary
    window; hidden = every channel beside it. Multiplier and hidden share come from
    reported tokens when anything carries usage, on-disk bytes otherwise — the basis is
    named, and no companion figure is fabricated for the lane that wasn't measured."""
    def tok(rows: list[dict[str, Any]]) -> dict[str, int]:
        tin = sum(int(r["tin"]) for r in rows)
        tout = sum(int(r["tout"]) for r in rows)
        cread = sum(int(r["cread"]) for r in rows)
        cwrite = sum(int(r["cwrite"]) for r in rows)
        return {
            "fresh": tin + cwrite + tout, "cache_read": cread,
            "total": tin + cwrite + cread + tout,
            "calls": sum(int(r["calls"]) for r in rows),
            "bytes": sum(int(r["source_bytes"] or 0) for r in rows),
        }

    vis, hid = tok(primary), tok(channels)
    total = vis["total"] + hid["total"]
    total_bytes = vis["bytes"] + hid["bytes"]
    basis = "tokens" if total > 0 else "bytes"
    if basis == "tokens":
        multiplier = round(total / max(vis["total"], 1), 1)
        hidden_pct = round(hid["total"] / max(total, 1) * 100, 1)
    else:
        multiplier = round(total_bytes / max(vis["bytes"], 1), 1)
        hidden_pct = round(hid["bytes"] / max(total_bytes, 1) * 100, 1)
    return {
        "visible": vis, "hidden": hid,
        "total_tokens": total, "total_bytes": total_bytes,
        "fresh_tokens": vis["fresh"] + hid["fresh"],
        "cache_read_tokens": vis["cache_read"] + hid["cache_read"],
        "cache_read_pct": round(
            (vis["cache_read"] + hid["cache_read"]) / max(total, 1) * 100, 1),
        "hidden_pct": hidden_pct, "multiplier": multiplier, "basis": basis,
        "reminders": sum(int(r["reminders"]) for r in primary + channels),
        "compactions": sum(int(r["compactions"]) for r in primary + channels),
        "sidechains": sum(1 for c in channels if c["channel"] == "sidechain"),
        "workflows": sum(1 for c in channels if c["channel"] == "workflow"),
        "compaction_files": sum(1 for c in channels if c["channel"] == "compaction"),
    }


class TranscriptStore:
    """The normalized transcript index, fed by adapters, read by identity."""

    def __init__(
        self, pool: asyncpg.Pool, adapters: list[HarnessAdapter] | None = None,
    ) -> None:
        self.pool = pool
        from src.ingest.harness.claude_jsonl import ClaudeJsonlAdapter
        from src.ingest.harness.crush_sqlite import CrushSqliteAdapter
        from src.ingest.harness.dsh import DshSessionAdapter
        self._default_adapters = adapters or [
            ClaudeJsonlAdapter(), CrushSqliteAdapter(), DshSessionAdapter(),
        ]

    async def _freshness(
        self, locator: SessionLocator,
    ) -> tuple[bool, int, datetime | None, int | None]:
        """(skip, since_idx, observed_mtime, observed_bytes) — THE SPEND GATE (the house's
        oldest law: every catastrophe is a spend catastrophe, and a store must never re-eat
        what it already holds). skip=True means the source hasn't changed since the last
        ingest — no read at all, serve the stored rows. Otherwise since_idx resumes after
        the turns already ingested, and observed_mtime — taken BEFORE the read — becomes
        the new last_ingested_at, so lines appended DURING a read are never skipped by the
        next pass (stamping now() there would swallow them forever)."""
        row = await self.pool.fetchrow(
            "SELECT last_turn_idx, last_ingested_at FROM harness_sessions "
            "WHERE harness=$1 AND anchor_sid=$2",
            locator.harness, locator.anchor_sid)
        mtime, size = _stat(locator.source_path)
        if row is None:
            return (False, 0, mtime, size)
        if (mtime is not None and row["last_ingested_at"] is not None
                and mtime <= row["last_ingested_at"]):
            return (True, int(row["last_turn_idx"]), mtime, size)
        return (False, int(row["last_turn_idx"]), mtime, size)

    async def discover_and_ingest(
        self, *, cwd: str | None, job_dir: str | None,
        adapters: list[HarnessAdapter] | None = None,
        root: Path | None = None, transcript_path: str | None = None,
    ) -> ModelReading | None:
        """Try each adapter; ingest from the first that discovers a session.

        INCREMENTAL by the spend gate: an unchanged source is never re-read (a stat and a
        row lookup, nothing more), and a changed one is read from the last ingested turn
        on. The reading always comes back from the STORE (full history, however this call's
        delta landed) — never from the delta alone, which would amnesia the swap history.

        `root` overrides the adapters' on-disk search dir (tests inject a tmp root, the
        same override resolve_identity's cwd sid-guess honors). The reading carries the
        locator's `anchored` grade — identity's whole basis for trusting it.

        `transcript_path` is a CALLER-KNOWN location (thread 7304bfd8: mount()'s and
        automount()'s own hook-stamped param, previously captured but never consulted
        here) — tried FIRST via each adapter's own `discover_at`, bypassing the job_dir/
        cwd SEARCH entirely. This is the fix for the bridge-fork specimen: a background-
        job fork's job_dir-based search can find a stub file or nothing, while the caller
        already has the real path in hand. Falls through to ordinary discovery when no
        adapter recognizes the path (or none is given) — never a second, competing lane.

        Returns the model reading for identity resolution, or None when no adapter
        recognizes the session — since the JSONL-fallback removal (task #29) there is no
        legacy probe behind this: no reading means no observed model."""
        if transcript_path:
            p = Path(transcript_path)
            for adapter in (adapters or self._default_adapters):
                discover_at = getattr(adapter, "discover_at", None)
                if discover_at is None:
                    continue
                try:
                    locator = discover_at(p)
                except Exception:  # noqa: BLE001 — an adapter failure must never block mount
                    continue
                if locator is not None:
                    reading = await self._ingest_locator(adapter, locator)
                    if reading is not None:
                        return reading
        for adapter in (adapters or self._default_adapters):
            try:
                locator = adapter.discover(cwd=cwd, job_dir=job_dir, root=root)
            except Exception:  # noqa: BLE001 — an adapter failure must never block mount
                continue
            if locator is None:
                continue
            reading = await self._ingest_locator(adapter, locator)
            if reading is not None:
                return reading
        return None

    async def _ingest_locator(
        self, adapter: HarnessAdapter, locator: SessionLocator,
    ) -> ModelReading | None:
        """The shared body behind BOTH discovery lanes above (explicit-path and search):
        freshness-gated ingest, then read the reading back from the store. None means
        THIS locator yielded nothing (an empty source with no prior rows) — the caller
        tries the next adapter/lane, never a hard failure."""
        skip, since, observed, size = await self._freshness(locator)
        if skip:
            stored = await self.model_of_session(locator.harness, locator.anchor_sid)
            if stored is not None:
                return replace(stored, anchored=locator.anchored)
            skip, since = False, 0  # a session row without turns: eat it whole
        turns = list(adapter.read_turns(locator, since_idx=since))
        if turns:
            await self._upsert(locator, turns, observed_at=observed, source_bytes=size)
        elif since == 0:
            return None  # nothing in the source at all — try the next adapter/lane
        reading = await self.model_of_session(locator.harness, locator.anchor_sid)
        return replace(reading, anchored=locator.anchored) if reading else None

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

    async def overhead_of_session(
        self, harness: str, anchor_sid: str,
    ) -> dict[str, Any] | None:
        """THE OVERHEAD LENS, per session (neo's eye, task #34): what the harness itself
        cost this window — the visible primary vs the hidden channels (subagent
        sidechains, compactions), system-reminder injections, compaction churn, and the
        cache-vs-fresh split that decides the price of all of it.

        Token math is neo's, kept honest: multiplier and hidden share come from
        API-reported usage when any channel carries it; the on-disk byte sizes are the
        fallback basis when none does, and no companion figure is ever fabricated.
        None when the store has never eaten this session."""
        rows = await self.pool.fetch(
            _OVERHEAD_SQL + " WHERE s.harness=$1 AND (s.anchor_sid=$2 OR s.parent_sid=$2) "
            "GROUP BY s.anchor_sid, s.channel, s.agent_type, s.session_id, s.source_bytes",
            harness, anchor_sid,
        )
        if not rows:
            return None
        primary = [dict(r) for r in rows if r["channel"] == "primary"]
        channels = [dict(r) for r in rows if r["channel"] != "primary"]
        out = _overhead_shape(primary, channels)
        out["harness"] = harness
        out["anchor_sid"] = anchor_sid
        out["detail"] = [
            {"channel": c["channel"], "agent_type": c["agent_type"],
             "session_id": c["session_id"],
             "tokens": int(c["tin"] + c["cwrite"] + c["tout"] + c["cread"]),
             "bytes": int(c["source_bytes"] or 0)}
            for c in sorted(
                channels,
                key=lambda c: -(c["tin"] + c["cwrite"] + c["tout"] + c["cread"]))
        ]
        return out

    async def overhead_fleet(self, *, top: int = 15) -> dict[str, Any]:
        """THE OVERHEAD LENS, fleet-wide: totals across every eaten session plus the
        top-N sessions by total tokens — the chrome's /overhead page reads this. Bounded:
        the top list is capped, the totals are aggregates (land on counts, walk in)."""
        rows = await self.pool.fetch(
            "WITH ta AS (" + _TURN_AGG_SQL + " GROUP BY harness, anchor_sid) "
            "SELECT s.harness, COALESCE(s.parent_sid, s.anchor_sid) AS root_sid, "
            "       MAX(s.project) FILTER (WHERE s.channel='primary') AS project, "
            "       COUNT(*) FILTER (WHERE s.channel <> 'primary') AS channel_files, "
            "       COALESCE(SUM(s.source_bytes), 0) AS bytes, "
            "       COALESCE(SUM(s.source_bytes) "
            "                 FILTER (WHERE s.channel='primary'), 0) AS visible_bytes, "
            "       COALESCE(SUM(ta.tin), 0) AS tin, "
            "       COALESCE(SUM(ta.tout), 0) AS tout, "
            "       COALESCE(SUM(ta.cread), 0) AS cread, "
            "       COALESCE(SUM(ta.cwrite), 0) AS cwrite, "
            "       COALESCE(SUM(ta.tin + ta.cwrite + ta.tout + ta.cread) "
            "                 FILTER (WHERE s.channel='primary'), 0) AS visible_tokens, "
            "       COALESCE(SUM(ta.tin + ta.cwrite + ta.tout + ta.cread) "
            "                 FILTER (WHERE s.channel <> 'primary'), 0) AS hidden_tokens, "
            "       COALESCE(SUM(ta.calls), 0) AS calls, "
            "       COALESCE(SUM(ta.reminders), 0) AS reminders, "
            "       COALESCE(SUM(ta.compactions), 0) AS compactions "
            "FROM harness_sessions s "
            "LEFT JOIN ta ON ta.harness=s.harness AND ta.anchor_sid=s.anchor_sid "
            "GROUP BY s.harness, COALESCE(s.parent_sid, s.anchor_sid)",
        )
        sessions = []
        for r in rows:
            total = int(r["tin"] + r["cwrite"] + r["tout"] + r["cread"])
            visible = int(r["visible_tokens"])
            basis = "tokens" if total > 0 else "bytes"
            vis_b, tot_b = int(r["visible_bytes"]), int(r["bytes"])
            sessions.append({
                "harness": r["harness"], "anchor_sid": r["root_sid"],
                "project": r["project"], "channel_files": int(r["channel_files"]),
                "total_tokens": total, "hidden_tokens": int(r["hidden_tokens"]),
                "fresh_tokens": int(r["tin"] + r["cwrite"] + r["tout"]),
                "cache_read_tokens": int(r["cread"]),
                "cache_read_pct": round(int(r["cread"]) / max(total, 1) * 100, 1),
                "hidden_pct": (round(int(r["hidden_tokens"]) / max(total, 1) * 100, 1)
                               if basis == "tokens"
                               else round((tot_b - vis_b) / max(tot_b, 1) * 100, 1)),
                "multiplier": (round(total / max(visible, 1), 1) if basis == "tokens"
                               else round(tot_b / max(vis_b, 1), 1)),
                "basis": basis, "bytes": tot_b, "calls": int(r["calls"]),
                "reminders": int(r["reminders"]),
                "compactions": int(r["compactions"]),
            })
        sessions.sort(key=lambda s: (-s["total_tokens"], -s["bytes"]))
        tok_total = sum(s["total_tokens"] for s in sessions)
        tok_hidden = sum(s["hidden_tokens"] for s in sessions)
        tok_cread = sum(s["cache_read_tokens"] for s in sessions)
        byt_total = sum(s["bytes"] for s in sessions)
        return {
            "totals": {
                "sessions": len(sessions),
                "sessions_with_usage": sum(1 for s in sessions if s["total_tokens"]),
                "channel_files": sum(s["channel_files"] for s in sessions),
                "total_tokens": tok_total, "hidden_tokens": tok_hidden,
                "fresh_tokens": sum(s["fresh_tokens"] for s in sessions),
                "cache_read_tokens": tok_cread,
                "cache_read_pct": round(tok_cread / max(tok_total, 1) * 100, 1),
                "hidden_pct": round(tok_hidden / max(tok_total, 1) * 100, 1),
                "total_bytes": byt_total,
                "calls": sum(s["calls"] for s in sessions),
                "reminders": sum(s["reminders"] for s in sessions),
                "compactions": sum(s["compactions"] for s in sessions),
            },
            "top": sessions[:top],
        }

    async def rederive(self, harness: str, *, channel: str = "primary") -> int:
        """One-time re-derivation after the schema grows a per-turn fact (the overhead
        lens's reminders/is_compaction were NULL/false on every pre-lens row): forget the
        derived turn rows of sessions whose SOURCE still exists on disk and reset their
        ingest progress, so the next sweep re-eats them with the current extraction.

        The store is a DERIVED index — the harness file is authoritative, so this loses
        nothing that exists. The guard is the point: a session whose source has VANISHED
        from disk keeps its rows untouched (they are the only record left; dropping them
        would be the wake-meter's unknowable-ghost mistake again). Returns sessions
        reset; the caller (or the next backfill tick) does the re-eating."""
        rows = await self.pool.fetch(
            "SELECT anchor_sid, source_path FROM harness_sessions "
            "WHERE harness=$1 AND channel=$2", harness, channel)
        reset = [r["anchor_sid"] for r in rows
                 if _stat(r["source_path"])[0] is not None]
        if not reset:
            return 0
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "DELETE FROM harness_turns WHERE harness=$1 AND anchor_sid=ANY($2)",
                    harness, reset)
                await conn.execute(
                    "UPDATE harness_sessions SET last_turn_idx=0, "
                    "  last_ingested_at=to_timestamp(0) "
                    "WHERE harness=$1 AND anchor_sid=ANY($2)",
                    harness, reset)
        return len(reset)

    async def backfill(
        self, *, adapters: list[HarnessAdapter] | None = None,
        limit_per_adapter: int = 0,
    ) -> dict[str, int]:
        """The periodic sweep — eat every harness session on disk into the store.

        This is the "Osiris eats transcripts" primitive: the miner's tick calls it, the
        operator's backfill command calls it. Idempotent (ON CONFLICT DO NOTHING on every
        turn). Returns per-adapter counts of sessions + turns ingested.

        `limit_per_adapter` caps each adapter's sweep (0 = unlimited) — a first run over
        a fleet's whole history should not hog the loop. Free work, but bounded.

        INCREMENTAL by the spend gate: a session whose source hasn't changed since its
        last ingest costs one stat + one row lookup and is never re-read — a steady-state
        sweep over a quiet fleet does no file IO at all. Without this, every tick re-ate
        every transcript on disk in full (the exact miner-walked-everything-forever shape
        the house was burned by, thread 51000597)."""
        counts: dict[str, int] = {}
        for adapter in (adapters or self._default_adapters):
            sessions = 0
            for locator in adapter.enumerate():
                try:
                    skip, since, observed, size = await self._freshness(locator)
                    if skip:
                        continue
                    turns = list(adapter.read_turns(locator, since_idx=since))
                    if not turns:
                        continue
                    await self._upsert(locator, turns, observed_at=observed,
                                       source_bytes=size)
                    sessions += 1
                except Exception:  # noqa: BLE001 — one bad session must not abort the sweep
                    continue
                if limit_per_adapter and sessions >= limit_per_adapter:
                    break
            counts[adapter.name] = sessions
        return counts

    async def _upsert(
        self, locator: SessionLocator, turns: list[TurnRow],
        *, observed_at: datetime | None = None, source_bytes: int | None = None,
    ) -> None:
        """`observed_at` is the source mtime taken BEFORE the read — stamped as
        last_ingested_at so the freshness gate compares against what was actually read,
        not against a later clock that would hide same-moment appends. `source_bytes`
        rides from the same stat (the overhead lens's byte fallback)."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO harness_sessions "
                    "   (anchor_sid, harness, session_id, cwd, project, source_path, "
                    "    last_ingested_at, last_turn_idx, channel, parent_sid, "
                    "    agent_type, source_bytes) "
                    "VALUES ($1, $2, $3, $4, $5, $6, COALESCE($8, now()), $7, "
                    "        $9, $10, $11, $12) "
                    "ON CONFLICT (harness, anchor_sid) DO UPDATE "
                    "   SET last_ingested_at = COALESCE($8, now()), "
                    "       last_turn_idx = GREATEST("
                    "           harness_sessions.last_turn_idx, EXCLUDED.last_turn_idx), "
                    "       cwd = COALESCE(EXCLUDED.cwd, harness_sessions.cwd), "
                    "       project = COALESCE(EXCLUDED.project, harness_sessions.project), "
                    "       channel = EXCLUDED.channel, "
                    "       parent_sid = COALESCE(EXCLUDED.parent_sid, "
                    "                             harness_sessions.parent_sid), "
                    "       agent_type = COALESCE(EXCLUDED.agent_type, "
                    "                             harness_sessions.agent_type), "
                    "       source_bytes = COALESCE(EXCLUDED.source_bytes, "
                    "                               harness_sessions.source_bytes)",
                    locator.anchor_sid, locator.harness, locator.session_id,
                    locator.cwd, locator.project, locator.source_path,
                    max((t.turn_idx for t in turns), default=0),
                    observed_at, locator.channel, locator.parent_sid,
                    locator.agent_type, source_bytes,
                )
                # idempotent: ON CONFLICT skips turns already ingested (re-mount, re-probe)
                await conn.executemany(
                    "INSERT INTO harness_turns "
                    "   (anchor_sid, harness, turn_idx, role, model, provider, "
                    "    tokens_in, tokens_out, cache_read, cache_write, cost_usd, "
                    "    duration_ms, recorded_at, is_summary, swap_deliberate, source_ref, "
                    "    reminders, is_compaction) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, "
                    "        $15, $16, $17, $18) "
                    "ON CONFLICT (harness, anchor_sid, turn_idx) DO NOTHING",
                    [
                        (locator.anchor_sid, locator.harness, t.turn_idx, t.role,
                         t.model, t.provider, t.tokens_in, t.tokens_out,
                         t.cache_read, t.cache_write, t.cost_usd, t.duration_ms,
                         t.recorded_at, t.is_summary, t.swap_deliberate, t.source_ref,
                         t.reminders, t.is_compaction)
                        for t in turns
                    ],
                )
