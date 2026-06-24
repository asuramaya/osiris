"""Anchor-and-pivot frontier policy — what the cascade is allowed to expand.

The old cascade fanned out on everything, so a single weak seed ("hector") and
greedy snippet mining produced dozens of stranger accounts. The fix: only expand
a node that has a real reason to exist in the subject's graph. The signal is the
node's *inbound links* — "why is this connected to the subject?" — not its own
properties (an enumerated soundcloud:foo provably EXISTS, but the same-handle link
to the subject is only a guess, so we must not crawl it).

Policy (surgical — it only ever blocks, never widens):
  * seed / subject-anchored object            -> ANCHOR   (always expand)
  * object with no inbound links (a root)     -> ANCHOR   (can't judge; allow)
  * strongest inbound link is self-declared/
    authoritative                             -> ANCHOR
  * strongest inbound link is a real
    observation, or unclassified (NULL)       -> OBSERVED (expand; medium trust)
  * EVERY inbound link is a co-occurrence /
    derived guess                             -> SPECULATIVE (a leaf; do NOT expand)

A SPECULATIVE leaf still *receives* assertions from other sources — the gate is on
"does this node spawn new crawls", never on "can facts attach to it". So when a
second, non-speculative source links the same node (corroboration), its strongest
inbound class rises and it becomes expandable on the next fixpoint round of
`expand_case` — no re-queue machinery needed. NULL is treated as OBSERVED so the
not-yet-ported (threat-intel) parsers keep their prior expand-everything behaviour.
"""

from __future__ import annotations

import uuid
from enum import StrEnum

import asyncpg

from src.parsers.base import EvidenceClass
from src.parsers.evidence import is_anchor_grade, is_speculative, strength

# Identity-fragment types the subject report organizes (the footprint surface).
_ID_TYPES = ("Person", "Account", "Username", "Email", "Phone", "Domain", "URL")


class Tier(StrEnum):
    ANCHOR = "anchor"            # a self-declared/authoritative (or seed/subject) reason to exist
    OBSERVED = "observed"        # a real observation (or unclassified) — expandable, medium trust
    SPECULATIVE = "speculative"  # only a co-occurrence/derived guess connects it — a leaf


async def _is_seed(pool: asyncpg.Pool, case_id: uuid.UUID, object_id: uuid.UUID) -> bool:
    """The operator-chosen seed has no creating run (added_by_run IS NULL)."""
    row = await pool.fetchrow(
        "SELECT added_by_run FROM case_objects WHERE case_id=$1 AND object_id=$2",
        case_id,
        object_id,
    )
    return row is not None and row["added_by_run"] is None


async def _is_subject(pool: asyncpg.Pool, object_id: uuid.UUID) -> bool:
    """The '★ this is me' anchor writes a tag=subject assertion (and on the hub)."""
    return bool(
        await pool.fetchval(
            "SELECT 1 FROM current_assertions "
            "WHERE object_id=$1 AND name='tag' AND value->>'tag'='subject' LIMIT 1",
            object_id,
        )
    )


async def tier_of(pool: asyncpg.Pool, case_id: uuid.UUID, object_id: uuid.UUID) -> Tier:
    if await _is_seed(pool, case_id, object_id) or await _is_subject(pool, object_id):
        return Tier.ANCHOR

    rows = await pool.fetch(
        "SELECT evidence_class FROM links WHERE to_id=$1 AND (case_id=$2 OR case_id IS NULL)",
        object_id,
        case_id,
    )
    if not rows:
        return Tier.ANCHOR  # a root / manually added object — never gate it

    classes = [EvidenceClass(r["evidence_class"]) if r["evidence_class"] else None for r in rows]
    if any(c is not None and is_anchor_grade(c) for c in classes):
        return Tier.ANCHOR
    # any non-speculative inbound reason (a real observation, or NULL/unclassified)
    # is enough to keep crawling; only an all-speculative node is held back.
    if any(c is None or not is_speculative(c) for c in classes):
        return Tier.OBSERVED
    return Tier.SPECULATIVE


async def is_expandable(pool: asyncpg.Pool, case_id: uuid.UUID, object_id: uuid.UUID) -> bool:
    """Whether the cascade may fire collectors on this node (ANCHOR/OBSERVED) or
    must keep it as a leaf (SPECULATIVE)."""
    return await tier_of(pool, case_id, object_id) is not Tier.SPECULATIVE


async def subject_report(
    pool: asyncpg.Pool, case_id: uuid.UUID
) -> dict[str, list[dict[str, object]]]:
    """Answer 'who is this?' as a confidence ladder instead of a raw graph. Every
    identity fragment in the case is bucketed by HOW well it is established:
      * verified    — seed/subject, or a self-declared/authoritative reason to exist
      * corroborated — not anchor-grade, but >=2 independent sources point at it
      * speculative — a single weak (co-occurrence/derived) reason only
    Each fragment carries its strongest evidence_class, source count and confidence
    so the operator can see WHY it is believed."""
    subject_ids = {
        r["object_id"]
        for r in await pool.fetch(
            "SELECT object_id FROM current_assertions "
            "WHERE name='tag' AND value->>'tag'='subject'"
        )
    }
    rows = await pool.fetch(
        """
        SELECT o.id, o.type, o.canonical, co.added_by_run,
               array_agg(DISTINCT l.evidence_class)
                 FILTER (WHERE l.evidence_class IS NOT NULL) AS classes,
               count(DISTINCT l.source_id) FILTER (WHERE l.id IS NOT NULL) AS n_sources,
               max(l.confidence) AS confidence
        FROM case_objects co
        JOIN objects o ON o.id = co.object_id AND o.status = 'active'
        LEFT JOIN links l ON l.to_id = o.id AND (l.case_id = $1 OR l.case_id IS NULL)
        WHERE co.case_id = $1 AND o.type = ANY($2::text[])
        GROUP BY o.id, o.type, o.canonical, co.added_by_run
        """,
        case_id,
        list(_ID_TYPES),
    )

    buckets: dict[str, list[dict[str, object]]] = {
        "verified": [], "corroborated": [], "speculative": []
    }
    for r in rows:
        classes = [EvidenceClass(c) for c in (r["classes"] or [])]
        strongest = max(classes, key=strength) if classes else None
        is_subject = r["id"] in subject_ids
        is_seed = r["added_by_run"] is None
        if is_subject or is_seed or (strongest is not None and is_anchor_grade(strongest)):
            tier = "verified"
        elif int(r["n_sources"]) >= 2:
            tier = "corroborated"
        else:
            tier = "speculative"
        buckets[tier].append({
            "id": str(r["id"]),
            "type": r["type"],
            "canonical": r["canonical"],
            "evidence_class": strongest.value if strongest is not None else None,
            "sources": int(r["n_sources"]),
            "confidence": r["confidence"],
            "subject": is_subject,
            "speculative": strongest is not None and is_speculative(strongest),
        })
    for bucket in buckets.values():
        bucket.sort(key=lambda f: (f["confidence"] or 0, str(f["canonical"])), reverse=True)
    return buckets
