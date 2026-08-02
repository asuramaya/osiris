"""thread_closure_edges' resolved_by arm reads its strength from SOURCE, not just edge type
(Thoth DM 2958/2975, thread 13725dbb — the closure-backfill design).

THE GAP: strength was hardcoded per edge TYPE — resolved_by/answers always 'strong',
closed_by always 'weak'. That conflated two independent questions: what KIND of link this
is, and how much a reader should TRUST who made it. A resolved_by edge minted by a mind
citing a commit or decision on purpose, and a resolved_by edge minted by a miner reading a
commit hash out of someone else's prose, are structurally the same edge type pointing at a
real object either way — but they do not deserve the same confidence, and until now nothing
here could tell them apart.

THE FIX: the resolved_by arm now computes strength with a CASE on `l.source_id` —
'closure-backfill' (the miner introduced alongside this migration, src/ingest/closure.py)
reads 'weak'; every other source (a mind's own resolve_thread(artifact=...) call, unchanged)
still reads 'strong'. The answers arm is untouched — record_decision(resolves=...) is always
a mind's own act, never minted by any miner, so it stays unconditionally strong. The
closed_by arm is untouched too (still unconditionally weak, 0044's own shape).

THIS IS THE SAME ONE-ARM-EXTENSION SHAPE 0044 USED, applied to an existing arm's CASE
expression rather than adding a new arm — src/orchestrator/thread_closure.py needs no code
change again: `thread_closure_status`'s strength ranking already treats 'weak'/'strong' as
opaque strings, so a resolved_by row reading 'weak' composes into strong/weak downstream
callers unchanged, including compositions.py's own closure_health strong/weak split
(commit af20ad9) — this migration is what makes that split show a project's prose-backfilled
share against its originally-strong share, rather than a name for a distinction the data
could not yet make.

Revision ID: 0045
Revises: 0044
Create Date: 2026-08-02
"""
from __future__ import annotations

from alembic import op

revision = "0045"
down_revision = "0044"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP VIEW thread_closure_edges")
    op.execute(
        """
        CREATE VIEW thread_closure_edges AS
            SELECT o.id AS thread_id, l.id AS link_id, l.type AS edge_type,
                   (CASE WHEN l.source_id = 'closure-backfill' THEN 'weak' ELSE 'strong' END)
                       ::text AS strength,
                   l.to_id AS closer_id,
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
        UNION ALL
            SELECT o.id AS thread_id, l.id AS link_id, l.type AS edge_type,
                   'weak'::text AS strength, l.to_id AS closer_id,
                   l.source_id, l.created_at
            FROM objects o
            JOIN links l ON l.from_id = o.id AND l.type = 'closed_by'
            WHERE o.type = 'Thread' AND l.valid_until IS NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW thread_closure_edges")
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
        UNION ALL
            SELECT o.id AS thread_id, l.id AS link_id, l.type AS edge_type,
                   'weak'::text AS strength, l.to_id AS closer_id,
                   l.source_id, l.created_at
            FROM objects o
            JOIN links l ON l.from_id = o.id AND l.type = 'closed_by'
            WHERE o.type = 'Thread' AND l.valid_until IS NULL
        """
    )
