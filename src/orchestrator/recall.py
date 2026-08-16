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
never designed for it, so this stays its own small, focused verb.

NOTES AND ADDENDA NOW SURFACE HERE (Thoth DM 3278, thread 1f4dcc03: amend_decision's own
docstring disclosed "recall() never reads it back" as a real, unfixed gap blocking the
AMEND family's Phase 4 merge). This is THE surface a Decision's or Thread's readers already
use to get the whole record back — the same reasoning `_fn_practices` already applied for
amend_practice's amendments (folded into `practices()`, "the ONE live surface every caller
already uses"), applied here to recall() instead, since that is Decision/Thread's own
equivalent surface, not a listing composition. annotate_thread's `note:%` rows carried the
IDENTICAL gap (verified: zero callers of `thread_notes` anywhere outside tests, same as
`decision_addenda` before this fix) — fixed in the same pass since both live in this same
function, not a second, separate defect worth leaving half-mended right next to the first.

BEARS_ON'S OWN READ-BACK (898840dc, Thoth msg 4828): a Thread's `bears_on_from` list is
every live `answers` edge FROM a Decision INTO it (mint_bears_on's own edge) - decisions
that speak to this row without having closed it, oldest first. Scoped to recall() only,
not orient()'s summary view: this is a per-object query, appropriate for the one-thread-
at-a-time surface, not for a per-session listing that would multiply it by every open
thread on the board."""
from __future__ import annotations

import re
import uuid
from typing import Any

import asyncpg

_KINDS = ("thread", "decision")

# The migrated-board address form (roadmap_migration.py's legacy_task_ref, msg 4429's
# acceptance test: "after apply, #150 must resolve to the object"). A bare decimal, with
# or without the leading '#' this reign's own mail always writes it with.
_LEGACY_TASK_ID_RE = re.compile(r"^#?(\d+)$")


async def _find_by_legacy_task_ref(
    pool: asyncpg.Pool, task_id: str,
) -> list[tuple[uuid.UUID, str, str | None]]:
    """Every ACTIVE object carrying legacy_task_ref.id == task_id, any store. A bare task
    number is only unique PER STORE (task_sync.py's own binding rule, proven the hard way
    this same reign — ruling 50c3ed90) — more than one hit here is a real ambiguity, never
    resolved by picking the first row."""
    rows = await pool.fetch(
        "SELECT a.object_id, o.type, a.value ->> 'store' AS store "
        "FROM current_assertions a JOIN objects o ON o.id = a.object_id "
        "WHERE a.name = 'legacy_task_ref' AND a.value ->> 'id' = $1 AND o.status = 'active'",
        task_id,
    )
    return [(r["object_id"], r["type"], r["store"]) for r in rows]


async def _full_record(pool: asyncpg.Pool, oid: uuid.UUID, otype: str) -> dict[str, Any] | None:
    """Every CURRENT property on one object, winner-per-name, untruncated — None when no
    ACTIVE object of exactly this type exists at that id. `_find_thread`/`_find_decision`
    trust an explicit-UUID-shaped ref as intent WITHOUT checking existence (the grave rule,
    correct for the write verbs they were built for) — this is where recall() actually
    checks, so a syntactically-valid-but-nonexistent UUID refuses honestly instead of
    returning an empty shell of nulls.

    `note:%`/`addendum:%` properties are EXCLUDED from the flat per-name dump above and
    folded back in as `notes`/`addenda` — an ordered, oldest-first list via `thread_notes`/
    `decision_addenda`, always present (empty list, never an absent key — this house's own
    law against a bucket collapsing to silence). Left in the flat dump, each one would
    appear as one more `note:a3f9c012`-shaped key: unordered (SQL sorts by the RANDOM
    `_append_property_name` suffix, not time), untimestamped (this query never selects
    `observed_at`), and undiscoverable unless a reader already knew the prefix to look for —
    functionally unreadable despite being technically present, which is exactly what made
    the original gap easy to miss. `observed_at` is stringified here (`.isoformat()`), not
    left as asyncpg's raw datetime — this dict crosses the MCP wire as this function's own
    caller, and every other datetime bound for that trip in this codebase is stringified at
    its own call site (mcp_server.py has no blanket encoder); `thread_notes`/`decision_
    addenda` keep returning real datetimes for their own direct callers (tests, `lap()`),
    unchanged."""
    canonical = await pool.fetchval(
        "SELECT canonical FROM objects WHERE id=$1 AND type=$2 AND status='active'",
        oid, otype)
    if canonical is None:
        return None
    rows = await pool.fetch(
        "SELECT DISTINCT ON (a.name) a.name, a.value #>> '{}' AS value "
        "FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name NOT LIKE 'note:%' AND a.name NOT LIKE 'addendum:%' "
        "ORDER BY a.name, a.confidence DESC, a.observed_at DESC", oid)
    record: dict[str, Any] = {"id": str(oid)[:8], "canonical": canonical, "type": otype,
                              **{r["name"]: r["value"] for r in rows}}
    if otype == "Thread":
        from src.orchestrator.capture import thread_notes
        notes = await thread_notes(pool, oid)
        record["notes"] = [{**n, "observed_at": n["observed_at"].isoformat()} for n in notes]
        # THE MEASURER'S MOMENT'S OWN READ-BACK (898840dc/e123b9fa): mint_bears_on() mints
        # an `answers` edge from a fresh Decision onto exactly the stale row it speaks to,
        # WITHOUT closing it — but nothing ever read that edge back onto the thread's own
        # surface, so a reader recalling this row had no way to see "N decisions already
        # speak to this" and could re-measure something already answered (the exact shape
        # four of six stale board rows turned out to be, Seshat's own sweep). Live edges
        # only (valid_until IS NULL) — an unmerge/retraction must not go on citing a row.
        bearers = await pool.fetch(
            "SELECT d.id, "
            " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=d.id "
            "  AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
            "  AS summary "
            "FROM links l JOIN objects d ON d.id=l.from_id AND d.type='Decision' "
            "WHERE l.to_id=$1 AND l.type='answers' AND l.valid_until IS NULL "
            "ORDER BY d.created_at", oid)
        record["bears_on_from"] = [
            {"id": str(r["id"])[:8], "summary": (r["summary"] or "")[:160]}
            for r in bearers]
    elif otype == "Decision":
        from src.orchestrator.capture import decision_addenda
        addenda = await decision_addenda(pool, oid)
        record["addenda"] = [{**a, "observed_at": a["observed_at"].isoformat()}
                              for a in addenda]
    return record


async def recall(pool: asyncpg.Pool, ref: str, *, kind: str | None = None) -> dict[str, Any]:
    """The full, untruncated record for a Thread or Decision. `ref` = a UUID, the 8-char
    short-id prefix orient() already hands you, or a summary substring. `kind`
    ('thread'|'decision') skips auto-detection when you already know which; omitted, tries
    Thread then Decision. Refuses loudly (never guesses) when nothing matches either type,
    or when `kind` itself is not one of the two real values."""
    from src.orchestrator.capture import _find_decision, _find_thread

    if kind is not None and kind not in _KINDS:
        return {"error": f"kind must be 'thread' or 'decision', got {kind!r}"}

    legacy_match = _LEGACY_TASK_ID_RE.match(ref.strip())
    if legacy_match:
        hits = await _find_by_legacy_task_ref(pool, legacy_match.group(1))
        if len(hits) > 1:
            stores = sorted({h[2] for h in hits if h[2] is not None})
            return {"error": f"{ref!r} matches legacy_task_ref id={legacy_match.group(1)!r} "
                             f"in {len(hits)} stores {stores} — a bare task number is only "
                             "unique per store; recall never guesses which one you mean"}
        if len(hits) == 1:
            oid, otype, _store = hits[0]
            rec = await _full_record(pool, oid, otype)
            if rec is not None:
                return rec
        # zero hits (or a dangling id with no active object): not migrated, or not this
        # id — fall through to the ordinary ladder unchanged, so "#150" typed before any
        # migration still resolves via a Thread/Decision that happens to quote it, exactly
        # as it always has.

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
