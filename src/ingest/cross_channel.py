"""Cross-channel recovery — the harness's OWN cross-session messaging (the SendMessage
tool) lifted out of soul-stored transcripts into typed, attributed, time-threaded rows
(task #181, Thoth DM 5320). Ptah measured that during a routing defect he and Ra sent 3
messages through Osiris and ~24 through the harness's own socket — 90% of a day's
reasoning existed only in two jsonl files, invisible to orient()/search()/fleet(). This
module is the recovery side: `extract_harness_sends` is the pure parser (soul_lines'
raw_line already carries everything, this only reads it); `recover_harness_exchanges` is
the dry-run-first repair verb (restore_attribution/unwire_informs_fanout's own shape) that
lands the parse into `harness_messages` (0051); `adoption_share` answers "how much of this
seat's real traffic did osiris actually see" by comparing osiris's own `fleet_messages`
against this table, per anchor_sid.

Reuses the soul store rather than re-parsing transcripts from disk — a session must
already be soul-stored (SoulStore.ingest_path) before it can be recovered; this module
never touches a jsonl file directly.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import asyncpg

_SEND_TOOL = "SendMessage"


def extract_harness_sends(raw_lines: list[str]) -> list[dict[str, Any]]:
    """Every SendMessage tool_use block across a session's raw JSONL lines, in order.
    Pure — no DB, no I/O — so the shape is testable against a hand-built fixture without
    touching Postgres or a real transcript. Returns
    `{turn_index, observed_at (ISO str | None), to, summary, message}` per hit; a line
    that fails to parse, or carries no SendMessage block, contributes nothing (never a
    partial/garbled record)."""
    out: list[dict[str, Any]] = []
    for idx, raw_line in enumerate(raw_lines):
        try:
            d = json.loads(raw_line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if not isinstance(d, dict) or d.get("type") != "assistant":
            continue
        content = (d.get("message") or {}).get("content")
        if not isinstance(content, list):
            continue
        ts = d.get("timestamp") if isinstance(d.get("timestamp"), str) else None
        for block in content:
            if not isinstance(block, dict) or block.get("name") != _SEND_TOOL:
                continue
            inp = block.get("input") or {}
            if not isinstance(inp, dict):
                continue
            to = inp.get("to")
            message = inp.get("message")
            if not isinstance(to, str) or not isinstance(message, str):
                continue  # a malformed call the harness itself would have refused
            out.append({
                "turn_index": idx, "observed_at": ts, "to": to,
                "summary": inp.get("summary") if isinstance(inp.get("summary"), str) else None,
                "message": message,
            })
    return out


async def _resolve_session_ref(pool: asyncpg.Pool, ref: str) -> str | None:
    """BEST EFFORT ONLY: the harness's own `to`/session identifiers are bare hex
    fragments (a job_dir key or a full session id), never a osiris canonical — this
    matches the SAME job_dir-basename convention `registry_census`/`SessionLocator`
    already rely on (`anchor_sid` == `Path(job_dir).name`'s leading 8 chars). A ref that
    matches nothing live (or historical) resolves to None — the raw value is never lost,
    only the resolved column stays empty."""
    frag = ref.strip().lower()[:8]
    if len(frag) < 6:  # too short to trust as a real fragment match
        return None
    result = await pool.fetchval(
        "SELECT agent_id FROM agent_mounts WHERE job_dir ~ ('/' || $1) "
        "ORDER BY last_seen DESC NULLS LAST LIMIT 1", frag)
    return str(result) if result is not None else None


async def recover_harness_exchanges(
    pool: asyncpg.Pool, anchor_sid: str, *, dry_run: bool = True, because: str | None = None,
) -> dict[str, Any]:
    """THE REPAIR VERB — dry-run-first, same shape as `restore_attribution`/
    `unwire_informs_fanout`: reads `anchor_sid`'s already-soul-stored lines
    (`SoulStore.raw_lines` — this session must be ingested first, this function never
    touches disk), extracts every SendMessage block, and reports (dry_run=True) or
    WRITES (dry_run=False) the ones not already recovered (idempotent per
    (anchor_sid, turn_index) — re-running costs nothing once complete). `dry_run=False`
    REQUIRES a non-blank `because` — landing a day's worth of cross-session traffic into
    the graph for the first time is a deliberate act on the record, never silent, same
    discipline every other repair verb here holds.

    `{"error": ...}` when nothing has been soul-stored for `anchor_sid` — recovery
    cannot invent what was never ingested. Otherwise: `{found, already_recovered,
    would_write / written, sample}` — `sample` is up to 3 {turn_index, to, summary}
    previews, never the full message bodies (kept short on purpose; the full row lands
    in the table, not the receipt)."""
    if not dry_run and not (because or "").strip():
        return {"error": "dry_run=False requires a non-blank `because` — recovery is a "
                         "deliberate act on the record, never silent"}
    from src.ingest.soul_store import SoulStore

    lines = await SoulStore(pool).raw_lines(anchor_sid)
    if lines is None:
        return {"error": f"no soul_lines ingested for {anchor_sid!r} — soul-store this "
                         "session first (SoulStore.ingest_path), recovery cannot invent "
                         "what was never ingested"}
    found = extract_harness_sends(lines)
    have = {r["turn_index"] for r in await pool.fetch(
        "SELECT turn_index FROM harness_messages WHERE anchor_sid=$1", anchor_sid)}
    todo = [r for r in found if r["turn_index"] not in have]
    sample = [{"turn_index": r["turn_index"], "to": r["to"], "summary": r["summary"]}
              for r in todo[:3]]
    if dry_run:
        return {"found": len(found), "already_recovered": len(found) - len(todo),
               "would_write": len(todo), "sample": sample}
    if not todo:
        return {"found": len(found), "already_recovered": len(found) - len(todo),
               "written": 0, "sample": sample}
    from_agent = await pool.fetchval(
        "SELECT agent_id FROM agent_mounts WHERE job_dir ~ ('/' || $1) "
        "ORDER BY last_seen DESC NULLS LAST LIMIT 1", anchor_sid[:8])
    rows = []
    for r in todo:
        to_resolved = await _resolve_session_ref(pool, r["to"])
        observed_at: datetime | None = None
        if r["observed_at"]:
            try:
                observed_at = datetime.fromisoformat(r["observed_at"].replace("Z", "+00:00"))
            except ValueError:
                observed_at = None
        rows.append((anchor_sid, r["turn_index"], from_agent, r["to"], to_resolved,
                     r["summary"], r["message"], observed_at))
    await pool.executemany(
        "INSERT INTO harness_messages "
        "  (anchor_sid, turn_index, from_agent, to_raw, to_resolved, summary, message, "
        "   observed_at) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8) "
        "ON CONFLICT (anchor_sid, turn_index) DO NOTHING",
        rows)
    return {"found": len(found), "already_recovered": len(found) - len(todo),
           "written": len(todo), "sample": sample}


async def adoption_share(
    pool: asyncpg.Pool, *, from_agent: str, anchor_sid: str | None, window_hours: int = 24,
) -> dict[str, Any]:
    """Per-agent traffic split: how many of its exchanges went through osiris
    (`fleet_messages`) versus the harness's own socket (`harness_messages`, RECOVERED
    only — never inferred). `anchor_sid=None` (no live/recent session to key off of, or
    nothing soul-stored) reports `harness_count: None` — "unknown", never a silent zero
    that would misread as perfect adoption. `share` is osiris / (osiris + harness),
    omitted whenever harness_count is unknown or both counts are zero (nothing to
    divide)."""
    osiris_count = await pool.fetchval(
        "SELECT count(*) FROM fleet_messages WHERE from_agent=$1 "
        "AND created_at > now() - make_interval(hours => $2)", from_agent, window_hours)
    osiris_count = int(osiris_count or 0)
    if anchor_sid is None:
        return {"osiris_count": osiris_count, "harness_count": None, "recovered": False}
    harness_count = await pool.fetchval(
        "SELECT count(*) FROM harness_messages WHERE anchor_sid=$1 "
        "AND (observed_at IS NULL OR observed_at > now() - make_interval(hours => $2))",
        anchor_sid, window_hours)
    harness_count = int(harness_count or 0)
    out: dict[str, Any] = {"osiris_count": osiris_count, "harness_count": harness_count,
                           "recovered": True}
    total = osiris_count + harness_count
    if total:
        out["share"] = round(osiris_count / total, 3)
    return out
