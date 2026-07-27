"""practice statement fts — widen the FTS index to a field it always excluded

Revision ID: 0037
Revises: 0036
Create Date: 2026-07-27

THE THAW (ruling 1e6d7367): the Practice type's ontology addition (schema.py) needs no
migration of its own — objects.type and assertions.name are unconstrained text, so a new
type is a pure catalog entry, same as Superstition needed none when it shipped (commit
ff29bb2). What DOES need one: migration 0020's partial GIN index and _fn_search's matching
SQL both hardcode `name IN ('name','summary','rationale')` — Superstition's own `statement`
field has never been indexed by search() since Superstition shipped, a latent gap that
predates this build and applies to every existing Superstition today, not just the new
Practice type. A partial index's predicate can't be ALTERed in place, so this drops and
recreates ix_assertions_fts with `statement` added to the same three fields — one file
heals the old blindness and makes the new type findable at once.
"""
from __future__ import annotations

from alembic import op

revision = "0037"
down_revision = "0036"
branch_labels = None
depends_on = None

_TSV = "to_tsvector('english', value #>> '{}')"
_OLD_FIELDS = "'name','summary','rationale'"
_NEW_FIELDS = "'name','summary','rationale','statement'"


def upgrade() -> None:
    op.execute("DROP INDEX ix_assertions_fts")
    op.execute(
        f"CREATE INDEX ix_assertions_fts ON assertions USING gin ({_TSV}) "
        f"WHERE name IN ({_NEW_FIELDS})"
    )


def downgrade() -> None:
    op.execute("DROP INDEX ix_assertions_fts")
    op.execute(
        f"CREATE INDEX ix_assertions_fts ON assertions USING gin ({_TSV}) "
        f"WHERE name IN ({_OLD_FIELDS})"
    )
