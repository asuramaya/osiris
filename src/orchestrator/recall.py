"""recall(ref) — the full, untruncated record for a Thread or Decision named by a short id,
UUID, or summary substring (thread d6ed2f17: the #60-truncation recovery path).

orient() caps open_threads/recent_decisions summaries to 160 chars in terse mode
(task #60) — `recall(ref)` is how an agent gets the WHOLE thing back without re-fetching
everything via verbose=True or guessing at it via search(). AUTO-DETECTS type: tries Thread
first, then Decision (Thoth's call, msg 1299) — an agent pasting a short id off orient()
shouldn't need to know which array it came from. `kind` disambiguates on the rare collision
(two objects sharing an 8-hex-char UUID prefix) or just skips the extra query.

Composes the SAME resolution ladder resolve_thread/record_decision already use privately
(`_find_thread`/`_find_decision` in capture.py — UUID -> short-id PREFIX -> summary
substring) rather than duplicating it or widening `resolve_ref` globally: Thoth's explicit
call was that prefix-matching everywhere risks ambiguity collisions in existing callers
never designed for it, so this stays its own small, focused verb."""
from __future__ import annotations

import uuid
from typing import Any

import asyncpg

_KINDS = ("thread", "decision")


async def _full_record(pool: asyncpg.Pool, oid: uuid.UUID, otype: str) -> dict[str, Any] | None:
    """Every CURRENT property on one object, winner-per-name, untruncated — None when no
    ACTIVE object of exactly this type exists at that id. `_find_thread`/`_find_decision`
    trust an explicit-UUID-shaped ref as intent WITHOUT checking existence (the grave rule,
    correct for the write verbs they were built for) — this is where recall() actually
    checks, so a syntactically-valid-but-nonexistent UUID refuses honestly instead of
    returning an empty shell of nulls."""
    canonical = await pool.fetchval(
        "SELECT canonical FROM objects WHERE id=$1 AND type=$2 AND status='active'",
        oid, otype)
    if canonical is None:
        return None
    rows = await pool.fetch(
        "SELECT DISTINCT ON (a.name) a.name, a.value #>> '{}' AS value "
        "FROM current_assertions a WHERE a.object_id=$1 "
        "ORDER BY a.name, a.confidence DESC, a.observed_at DESC", oid)
    return {"id": str(oid)[:8], "canonical": canonical, "type": otype,
            **{r["name"]: r["value"] for r in rows}}


async def recall(pool: asyncpg.Pool, ref: str, *, kind: str | None = None) -> dict[str, Any]:
    """The full, untruncated record for a Thread or Decision. `ref` = a UUID, the 8-char
    short-id prefix orient() already hands you, or a summary substring. `kind`
    ('thread'|'decision') skips auto-detection when you already know which; omitted, tries
    Thread then Decision. Refuses loudly (never guesses) when nothing matches either type,
    or when `kind` itself is not one of the two real values."""
    from src.orchestrator.capture import _find_decision, _find_thread

    if kind is not None and kind not in _KINDS:
        return {"error": f"kind must be 'thread' or 'decision', got {kind!r}"}
    tried: list[str] = []
    if kind in (None, "thread"):
        tid = await _find_thread(pool, ref)
        rec = await _full_record(pool, tid, "Thread") if tid is not None else None
        if rec is not None:
            return rec
        tried.append("thread")
    if kind in (None, "decision"):
        did = await _find_decision(pool, ref)
        rec = await _full_record(pool, did, "Decision") if did is not None else None
        if rec is not None:
            return rec
        tried.append("decision")
    return {"error": f"no {'/'.join(tried)} matches {ref!r} — recall never guesses; "
                     "try search(query=...) for a broader sweep"}
