"""Shared guards + reconciliation for the mined-memory miners (decisions / threads).

Mining prose is rough by nature, and the 2026-07 live audit showed exactly how it rots
trust: mid-sentence fragments (`anchor), app start gated on…`), commits that DOCUMENT the
miner's own markers surfacing as threads, and stale rows from older miner versions living
forever because a re-mine only ever ADDED. Three shared defenses live here:

* `well_bounded` — a capture that starts or ends mid-sentence betrays itself with
  unbalanced delimiters (a `)` with no opener, an odd number of quotes). Cheap, and it
  rejects the fragments a lowercase-only guard can't (`Leon" vs "Daniel Leon") render…`
  starts uppercase).
* `unquoted` — a marker inside quotes/backticks is someone *talking about* the marker,
  not using it. Markers must match the text with quoted spans removed.
* `reconcile_mined` — re-mining must HEAL, not just add: mined objects the fresh mine no
  longer produces are archived via the event-sourced `Actions.set_status` (reversible),
  and objects the miner itself archived are resurrected if the text comes back. A human
  archive is never overridden (the last archive event's actor must be the miner).
"""

from __future__ import annotations

import re
import uuid
from typing import Any

from src.actions.core import ActionError, Actions

# spans whose content is MENTIONED, not asserted: "…", “…”, `…`. Single quotes are left
# alone (apostrophes in possessives would pair up and eat real text between them).
_QUOTED = re.compile(r'"[^"]*"|“[^”]*”|`[^`]*`')


def well_bounded(s: str) -> bool:
    """True when the fragment starts/ends at a plausible sentence boundary.

    A split artifact carries scars: a closing paren before any opener (``anchor), app
    start…``), unequal paren counts in either direction (``…phrase "THE WALL" (a real`` —
    cut off mid-parenthetical), or an odd number of double quotes (``Leon" vs "Daniel
    Leon")`` — the opener lives in the sentence the split ate. Deliberately strict: a
    bare list token ("2) the satellite") also reads as unbalanced and is rejected —
    tight stays trustworthy.
    """
    if not s or s[0] in ")]}":
        return False
    if s.count("(") != s.count(")") or s.count("[") != s.count("]"):
        return False
    if s.count('"') % 2 or s.count("`") % 2 or s.count("“") != s.count("”"):
        return False
    first_close = s.find(")")
    if first_close != -1 and s.find("(") > first_close:  # find('(') == -1 implies count mismatch
        return False
    return True


def unquoted(s: str) -> str:
    """The fragment with quoted/backticked spans removed — the text actually ASSERTED.

    A marker that only occurs inside quotes ("only the exact phrase \"THE WALL\"…") is
    documentation about the miner, and must not count as a marker hit.
    """
    return _QUOTED.sub(" ", s)


async def reconcile_mined(
    actions: Actions,
    *,
    object_type: str,
    prefix: str,
    produced: set[str],
    source_id: str,
    case_id: uuid.UUID | None = None,
) -> dict[str, int]:
    """Archive mined objects the fresh mine no longer produces; resurrect ones it does.

    Scope is strictly the miner's own output: the canonical prefix (``decision:`` /
    ``thread:``) AND at least one assertion from the miner's source. Archival is the
    event-sourced ``set_status`` (an `archive` object_event — reversible, auditable).
    Resurrection only fires when the LAST archive event was the miner's own actor, so a
    human's deliberate archive is never undone by a cron re-mine.
    """
    pool = actions.pool
    # Ownership = the miner authored the DEFINING assertion (`summary`), not merely touched
    # the object with some later assertion. Otherwise a stray miner write onto a session
    # object (e.g. a false auto-resolve) would make it look mined and get it archived as
    # "stale" — silently deleting a session's write-back. Scope stays inside the source.
    rows = await pool.fetch(
        "SELECT o.id, o.canonical, o.status FROM objects o "
        "WHERE o.type=$1 AND o.canonical LIKE $2 || '%' "
        "  AND o.status IN ('active','archived') "
        "  AND EXISTS (SELECT 1 FROM assertions a "
        "              WHERE a.object_id=o.id AND a.name='summary' AND a.source_id=$3)",
        object_type,
        prefix,
        source_id,
    )
    archived = resurrected = 0
    for r in rows:
        oid: Any = r["id"]
        if r["canonical"] in produced:
            if r["status"] != "archived":
                continue
            last_archiver = await pool.fetchval(
                "SELECT actor FROM object_events "
                "WHERE object_id=$1 AND event_type='archive' ORDER BY id DESC LIMIT 1",
                oid,
            )
            if last_archiver == source_id:
                await actions.set_status(
                    oid, "active", "re-mined: the source text produces this again",
                    source_id, case_id,
                )
                resurrected += 1
        elif r["status"] == "active":
            await actions.set_status(
                oid, "archived",
                "stale mined object — a re-mine of the commit record no longer produces it",
                source_id, case_id,
            )
            archived += 1
    return {"archived": archived, "resurrected": resurrected}


# Generic engineering vocabulary — present in half the commits, so a SHARED generic word is
# no evidence two memories are the same. Near-dup detection keys on shared DISTINCTIVE tokens
# (the project's own nouns: renderer, satellite, composer, briefing), never these.
_GENERIC = frozenset({
    "wire", "fold", "adds", "added", "build", "built", "ship", "make", "made", "feat",
    "fix", "test", "tests", "code", "into", "over", "with", "this", "that", "then",
    "when", "next", "wall", "remaining", "needs", "need", "live", "gated", "work",
    "thing", "things", "part", "pass", "runs", "real", "free", "file", "files", "from",
    "have", "still", "yet", "via", "all", "new", "key", "token", "first", "more", "one",
    "two", "the", "and", "for", "but", "now", "its",
})


def distinctive_terms(text: str) -> set[str]:
    """The project-distinctive tokens of a string (>=4 chars, minus generic vocabulary). The
    shared matcher for thread self-heal (resolve_threads) and near-duplicate consolidation."""
    return {t for t in re.findall(r"[a-z][a-z0-9_]{3,}", text.lower())} - _GENERIC


async def consolidate_memory(
    actions: Actions, *, object_type: str, prefix: str,
    min_shared: int = 5, min_containment: float = 0.6,
) -> dict[str, int]:
    """Collapse near-duplicate memory objects (Thread / Decision) the miners mint in
    differently-worded copies — the noise a session watches accrete when the session-miner
    re-senses work it already captured (distinct summaries hash to distinct objects, so the
    per-object grade read can't fold them; this is entity-level dedup).

    ONE anchored DIRECTION. The only auto-merge is a DERIVED echo folding into a DELIBERATE
    (SELF_DECLARED) capture — every merge is anchored by a human's deliberate memory, into
    which the echo folds (reversibly, event-sourced). The two ambiguous cases are surfaced
    for review, NEVER silently merged (the membrane, constitution #6): two deliberate captures
    (genuine divergence — a human's call to make), and two DERIVED echoes (near-dups with no
    deliberate anchor — a job for a judge that can tell "do X to M5" from "do Y to M5", which
    bag-of-tokens cannot). A dry run on the live graph is what drew this line: token overlap
    alone fused a blocker with the thing it blocks; the deliberate anchor is the guard.

    Match = >=`min_shared` shared distinctive tokens AND those cover >=`min_containment` of
    the smaller summary (containment, not Jaccard — tolerant of one summary rewording the
    other at a different length). Conservative by design: a missed merge is cheap noise; a
    wrong merge is bounded to a DERIVED loser and reversible."""
    pool = actions.pool
    rows = await pool.fetch(
        "SELECT o.id, "
        " (SELECT s.value #>> '{}' FROM current_assertions s WHERE s.object_id=o.id "
        "   AND s.name='summary' ORDER BY s.confidence DESC, s.observed_at DESC LIMIT 1) "
        "   AS summary,"
        " EXISTS (SELECT 1 FROM assertions a WHERE a.object_id=o.id AND a.name='summary' "
        "   AND a.evidence_class='self_declared') AS deliberate, "
        " (SELECT min(a.observed_at) FROM assertions a WHERE a.object_id=o.id "
        "   AND a.name='summary') AS born "
        "FROM objects o WHERE o.type=$1 AND o.status='active' AND o.canonical LIKE $2 || '%'",
        object_type, prefix,
    )
    items = [(r["id"], bool(r["deliberate"]), r["born"], distinctive_terms(r["summary"] or ""))
             for r in rows]
    items = [it for it in items if len(it[3]) >= min_shared]  # thin summaries can't match safely
    # winner precedence: deliberate first (never a loser), then oldest (the original capture)
    order = sorted(items, key=lambda it: (not it[1], it[2], str(it[0])))
    gone: set[uuid.UUID] = set()
    merged = review = 0
    for a in range(len(order)):
        w_id, w_delib, _wb, w_tok = order[a]
        if w_id in gone:
            continue
        for b in range(a + 1, len(order)):
            l_id, l_delib, _lb, l_tok = order[b]
            if l_id in gone:
                continue
            shared = len(w_tok & l_tok)
            if shared < min_shared or shared < min_containment * min(len(w_tok), len(l_tok)):
                continue
            if not (w_delib and not l_delib):
                # merge ONLY a DERIVED echo into a deliberate capture. Two deliberate (genuine
                # divergence) and two DERIVED (no anchor — a judge's job) are surfaced, not
                # merged: the loop closes, but never silently on an uncertain call.
                review += 1
                continue
            try:
                await actions.merge_objects(
                    w_id, l_id,
                    f"near-duplicate {object_type.lower()}: DERIVED echo of a deliberate capture",
                    "consolidate-memory")
                gone.add(l_id)
                merged += 1
            except ActionError:  # already merged in a prior step of a chain — skip
                pass
    return {f"{object_type.lower()}s_merged": merged, f"{object_type.lower()}s_for_review": review}
