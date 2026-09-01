"""thread_closure_edges — derive a thread's closure from topology, not a property (Phase 2,
Thoth DM 2508, decision cb38d922).

THE PRECEDENT: 0001's own `current_assertions` view buries a superseded assertion by the
EXISTENCE of the row that supersedes it — no status column, nothing to forget, nothing that
can disagree with itself. Threads never got the equivalent even though the answering edges
(`resolved_by`, `answers`) already exist in the schema (0001/LinkType catalog) and are
already minted by resolve_thread()/record_decision(resolves=...) today.

WHY THIS MATTERS (cb38d922, measured 2026-08-01): a thread's `status` property is a
current_assertions read across POSSIBLY MULTIPLE SOURCES — Agent A opens, Agent B resolves,
both rows stay live (assert_property only supersedes within-source), so "how many threads
are open" gave three different answers depending on which query you wrote (549 / 527 / a
third by the vocabulary test) in the same night. An edge either exists or it does not; it
cannot produce that ambiguity. This view is the topology-derived alternative — read-only,
additive, nothing switches over to it yet (that is Phase 2b, and it needs Khnum's/Imhotep's
own edges to land first).

THE VIEW IS RAW AND DELIBERATELY UNOPINIONATED, same posture as current_assertions itself:
one row per (thread, closure edge), not a collapsed per-thread boolean — the boolean/
disagreement-flagging judgment lives in the read function
(src/orchestrator/thread_closure.py), never baked into SQL, so it can change without a
migration.

TWO EDGE TYPES COUNT TODAY, both STRONG (artifact- or ruling-backed):
  - `resolved_by` (Thread -> Commit|Decision): resolve_thread(artifact=...) mints this only
    when the artifact names a real graph object — most resolve_thread() calls have no
    artifact at all, which is the single largest source of the 408 untraceable closures
    cb38d922 measured. A thread closed this way but with no artifact= gets NO row here.
  - `answers` (Decision -> Thread): record_decision(resolves=...) mints this in the same
    transaction as the status='resolved' write — always present for that closure path.
    STALE AS OF 0055 (Thoth DM 6230/6234, decision 36cbec2f): this stopped being true the
    day `mint_bears_on` shipped a second, non-closing producer of the identical `answers`
    edge — a claim describing a world that no longer exists is exactly the shape that hid
    two doors in #48, so it is left here uncorrected on purpose and pointed at 0055, which
    carries the real story and the fix, rather than silently rewritten.

A THIRD, WEAKER edge type is coming out of Khnum's Phase 1a work (an agent-level edge for
the artifact-less resolve_thread case) and is NOT wired in here — its exact type name and
direction aren't landed yet, and guessing would mint a phantom reference into a kernel view.
Adding it later is exactly ONE more UNION ALL arm below, with strength='weak' — that
one-line-extension shape is the whole point of building the view this way instead of a
single opaque join.

THE VIEW READING FALSE IS NOT "CONFIRMED OPEN": every closure minted before this migration
landed has no edge at all (cb38d922's 408), so `NOT EXISTS (closure edge)` today mixes
genuinely-open threads with every pre-cutover closure. Only a row's PRESENCE is a trustworthy
signal; its absence just means "no closure edge found (yet)" — see
src/orchestrator/thread_closure.py's docstring for how the read function surfaces this
without ever collapsing it into a plain boolean a caller could mistake for ground truth.

Revision ID: 0043
Revises: 0042
Create Date: 2026-08-01
"""
from __future__ import annotations

from alembic import op

revision = "0043"
down_revision = "0042"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE VIEW thread_closure_edges AS
            SELECT o.id AS thread_id, l.id AS link_id, l.type AS edge_type,
                   'strong'::text AS strength, l.to_id AS closer_id,
                   l.source_id, l.created_at
            FROM objects o
            JOIN links l ON l.from_id = o.id AND l.type = 'resolved_by'
            WHERE o.type = 'Thread' AND l.valid_until IS NULL
        UNION ALL
            SELECT o.id AS thread_id, l.id AS link_id, l.type AS edge_type,
                   'strong'::text AS strength, l.from_id AS closer_id,
                   l.source_id, l.created_at
            FROM objects o
            JOIN links l ON l.to_id = o.id AND l.type = 'answers'
            WHERE o.type = 'Thread' AND l.valid_until IS NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS thread_closure_edges")
