"""An index the lineage lookups can use for `canonical LIKE '<root>-%'`.

`lineage_works_in` (and its at-time sibling) find every generation of an agent lineage with
`a.type = 'Agent' AND (a.canonical = $1 OR a.canonical LIKE $1 || '-%')`. On a database whose
collation is not C (this box: en_US.utf8) a plain btree cannot answer a LIKE prefix, so the OR
forced a sequential scan of objects on every ingest: a profile of the idle worker sampled it in
197 of 1760 samples.

A `text_pattern_ops` index on (type, canonical) answers `LIKE 'prefix%'` as a range seek. The
same lookup on a synthetic 60,000-Agent objects table (150 projects, one works_in link each),
EXPLAIN ANALYZE: a sequential scan of objects, 15.1 ms, before; a BitmapOr of the existing
canonical index and this one, 0.20 ms, after.

CONCURRENTLY (own autocommit_block, as 0054 and 0077): a live table with live readers.

Revision ID: 0078
Revises: 0077
"""
from alembic import op

revision = "0078"
down_revision = "0077"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS objects_type_canonical_pattern_idx "
            "ON objects (type, canonical text_pattern_ops)"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS objects_type_canonical_pattern_idx")
