"""Harness-agnostic transcript adapters (ruling be741d3e, 2026-07-18).

The SEAM between Osiris and whatever CLI the operator is running. Each harness (Claude
Code, Crush, opencode, codex, …) writes its session record in a different native format
— JSONL on disk, SQLite, whatever. An adapter normalizes that into the same TurnRow
stream so the transcript store and every reader above it (identity, swap-detect, cost,
miner backfill) never touch a format-specific file again.

ADDING A HARNESS: implement HarnessAdapter, register it in the default adapter list
(see transcript_store.DEFAULT_ADAPTERS). No other change — the store, the readers, the
swap detector, and the MCP server are harness-agnostic by construction.

The harness's own file stays AUTHORITATIVE (the adapter records source_ref per turn);
the store is a DERIVED index of it, one evidence grade lower — high-stakes verdicts can
re-probe the source on demand.
"""
from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class TurnRow:
    """One assistant/user turn, normalized across harnesses.

    model+provider are first-class on some harnesses (Crush: SQLite columns) and embedded
    in a message envelope on others (Claude: ``message.model`` on the JSONL line). The
    adapter extracts whichever exists; None means the harness didn't record it for this
    turn. Token/cost fields are per-turn where available, None where the harness only
    aggregates at session level."""

    turn_idx: int
    role: str
    model: str | None = None
    provider: str | None = None
    tokens_in: int | None = None
    tokens_out: int | None = None
    cache_read: int | None = None
    cache_write: int | None = None
    cost_usd: float | None = None
    duration_ms: int | None = None
    recorded_at: datetime | None = None
    is_summary: bool = False
    swap_deliberate: bool | None = None
    source_ref: str | None = None
    # THE OVERHEAD FACTS (neo's eye, task #34): reminders = system-reminder blocks the
    # harness injected into this user turn (None = not measured — a harness that doesn't
    # carry them must never read as zero); is_compaction marks the compact-summary line
    # itself, distinct from is_summary (which also covers isMeta and so can't COUNT
    # compactions).
    reminders: int | None = None
    is_compaction: bool = False


@dataclass(frozen=True)
class SessionLocator:
    """Where a harness session LIVES on disk, resolved from cwd + job_dir.

    anchor_sid is the 8-char handle the rest of the identity system keys on (matches
    _job_id's parse of CLAUDE_JOB_DIR and the session UUID prefix). source_path is the
    absolute path to the harness's own record (JSONL file, SQLite DB, …) — kept so the
    store can re-probe the source for high-stakes verdicts without the adapter."""

    anchor_sid: str
    session_id: str
    harness: str
    source_path: str
    cwd: str | None
    project: str | None
    # THE CHANNEL TAXONOMY (neo's, kept whole): 'primary' is the operator-visible window;
    # 'sidechain' a Task-tool subagent's own transcript (agent_type from its meta.json);
    # 'compaction' the ancestor's separate-file layout, kept for old transcripts. A channel
    # names its primary via parent_sid; a primary carries neither.
    channel: str = "primary"
    parent_sid: str | None = None
    agent_type: str | None = None
    # HOW this session was found: True = anchored on the caller's own job/session id (the
    # identity-grade discovery), False = a hottest/newest heuristic (Crush's no-jid fallback).
    # Identity translates this into its anchor vocabulary — an unanchored locator's reading
    # must never confess a swap or claim a confident sid (the cry-wolf class).
    anchored: bool = True


@dataclass(frozen=True)
class ModelReading:
    """The model-identity reading off a harness session — what resolve_identity consumes.

    current is the latest assistant turn's model (the best in-session answer to 'which
    model am I'). history is the distinct-model sequence (length > 1 = a swap).
    deliberate means the operator's own model-change command is on the record (a chosen
    swap, never a sin). method names the harness that produced the reading so the grade
    is auditable. anchor_sid is the join key back to the store. anchored carries the
    locator's discovery grade (see SessionLocator.anchored); a raw store read
    (model_of_session) defaults False — only a discovery can testify to anchoring."""

    current: str | None
    history: tuple[str, ...]
    deliberate: bool
    observed_at: datetime | None
    method: str
    anchor_sid: str | None
    anchored: bool = False


class HarnessAdapter(Protocol):
    """One harness's transcript, normalized into TurnRows.

    discover(): given a cwd and/or job_dir, find THIS session's record on disk. Returns
    None when this adapter doesn't recognize the session (try the next adapter). read_turns():
    stream turns from the record, optionally skipping already-ingested ones (since_idx).
    enumerate(): yield ALL sessions this harness knows about on disk (for the miner's
    backfill — "Osiris eats transcripts" as a periodic sweep, not just mount-time). Adapters
    that can't enumerate (a harness with no on-disk discovery surface) yield nothing.

    Both discover/read_turns are SYNC — they do disk IO (read a file, open a SQLite DB),
    not network or DB. enumerate() is also sync for the same reason."""

    name: str

    def discover(
        self, *, cwd: str | None, job_dir: str | None, root: Path | None = None,
    ) -> SessionLocator | None: ...

    def read_turns(
        self, locator: SessionLocator, *, since_idx: int = 0,
    ) -> Iterator[TurnRow]: ...

    def enumerate(self, *, root: Path | None = None) -> Iterator[SessionLocator]: ...
