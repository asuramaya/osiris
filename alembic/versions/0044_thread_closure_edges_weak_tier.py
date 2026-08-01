"""thread_closure_edges gains its weak tier — `closed_by` (Thread -> Agent), landed by
Khnum's Phase 1a (commit 23c5991, decision cb38d922): resolve_thread() now mints exactly
one closure edge per close — `resolved_by` when `artifact` resolves to a Commit/Decision
(unchanged, strong), else `closed_by` to the resolving agent (new, weak) — so a close with
no citable artifact, the majority of resolve_thread() calls and the single largest source
of cb38d922's 408 untraceable closures, now leaves a traversable edge too.

THIS IS EXACTLY THE ONE-LINE EXTENSION 0043's own docstring named as the whole point of
building the view as a UNION ALL of typed arms instead of an opaque join: one more arm,
`strength='weak'` literal, same shape as the other two. src/orchestrator/thread_closure.py
needs NO code change — `thread_closure_status`'s strength ranking already carries a 'weak'
entry, put there when the view could not yet produce one.

`closed_by` and `resolved_by` are mutually exclusive per closure (capture.py's
`resolve_thread` mints exactly one), so this arm never doubles up an existing strong row —
it only reaches threads that closed with resolved_by absent.

Revision ID: 0044
Revises: 0043
Create Date: 2026-08-01
"""
from __future__ import annotations

from alembic import op

revision = "0044"
down_revision = "0043"
branch_labels = None
depends_on = None


def upgrade() -> None:
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
        """
    )
