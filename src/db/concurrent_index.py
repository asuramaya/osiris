"""Build and drop indexes CONCURRENTLY from an Alembic migration, safely.

`CREATE INDEX CONCURRENTLY` cannot run in a transaction, so a migration runs it in an
autocommit block. If the build fails part way (a deadlock, a cancel, a crash) Postgres leaves the
index behind marked INVALID, and `CREATE INDEX CONCURRENTLY IF NOT EXISTS` then sees the name and
does nothing: a retry "succeeds" with an index the planner never uses. `create_index_concurrently`
looks for an invalid index of that name first and drops it, so a retry really builds.

Use it from every migration that builds a live index:

    from src.db.concurrent_index import create_index_concurrently, drop_index_concurrently

    def upgrade() -> None:
        create_index_concurrently(op, "agent_wakes_message_time", "agent_wakes",
                                  "(message_id, woke_at DESC)")
"""
from __future__ import annotations

from typing import Any

import sqlalchemy as sa

# present in pg_class but not valid in pg_index: what a failed concurrent build leaves behind
INVALID_SQL = (
    "SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
    "WHERE c.relname = :name AND NOT i.indisvalid"
)


def create_index_concurrently(
    op: Any, name: str, table: str, definition: str, *, unique: bool = False,
) -> None:
    """`CREATE [UNIQUE] INDEX CONCURRENTLY IF NOT EXISTS name ON table definition`, outside a
    transaction, after dropping an INVALID index of the same name if a failed build left one.
    `definition` is the part after the table name, e.g. "(a, b DESC) WHERE c IS NOT NULL"."""
    leftover = op.get_bind().execute(sa.text(INVALID_SQL), {"name": name}).scalar()
    with op.get_context().autocommit_block():
        if leftover:
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")
        op.execute(
            f"CREATE {'UNIQUE ' if unique else ''}INDEX CONCURRENTLY IF NOT EXISTS {name} "
            f"ON {table} {definition}")


def drop_index_concurrently(op: Any, name: str) -> None:
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")
