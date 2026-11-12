"""The `assertions` view's UPDATE trigger writes only the columns the statement changed.

`assertions` is a view over `assertions_hot` and `assertions_cold` (migration 0064), and its
INSTEAD OF UPDATE trigger used to copy EVERY column from the statement's own snapshot (OLD and
NEW) onto the base row. The snapshot is taken when the UPDATE statement starts, so a bump of
`observed_at` or a flip of `is_current` made through the view could write back stale values for
the columns it never meant to touch, undoing what a concurrent writer committed in between:

  - a supersede or retirement flipped the row's `is_current` to false, and the bump flipped it
    back (two current rows for one fact);
  - the fold or the abstention collapse repointed the row's `supersedes`, and the flip or bump
    put the old pointer back (a pointer to a row that no longer exists).

The trigger now sets each column to NEW only when NEW differs from OLD, and otherwise leaves the
row's own current value in place (the UPDATE re-reads the row after waiting on a lock). A caller
that changes a column still changes it; a column nobody asked to change is never rewritten.
Nothing else about the trigger moves: it still updates the hot row, then the cold row when the id
is not in hot.

Revision ID: 0080
Revises: 0079
"""
from collections.abc import Callable

from alembic import op

revision = "0080"
down_revision = "0079"
branch_labels = None
depends_on = None

_COLUMNS = (
    "object_id", "name", "value", "source_id", "case_id", "helper_run_id", "evidence_uri",
    "evidence_sha256", "observed_at", "confidence", "supersedes", "evidence_class",
    "is_current",
)


def _only_changed(table: str) -> str:
    sets = ",\n                ".join(
        f"{c}=CASE WHEN NEW.{c} IS DISTINCT FROM OLD.{c} THEN NEW.{c} ELSE {c} END"
        for c in _COLUMNS)
    return f"UPDATE {table} SET\n                {sets}\n            WHERE id=OLD.id;"


def _every_column(table: str) -> str:
    sets = ", ".join(f"{c}=NEW.{c}" for c in _COLUMNS)
    return f"UPDATE {table} SET {sets} WHERE id=OLD.id;"


def _function(body: Callable[[str], str]) -> str:
    return f"""
        CREATE OR REPLACE FUNCTION assertions_view_update() RETURNS trigger AS $$
        BEGIN
            {body('assertions_hot')}
            IF NOT FOUND THEN
                {body('assertions_cold')}
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """


def upgrade() -> None:
    op.execute(_function(_only_changed))


def downgrade() -> None:
    op.execute(_function(_every_column))
