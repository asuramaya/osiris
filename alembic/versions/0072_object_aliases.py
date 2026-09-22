"""object_aliases: a renamed project's OLD canonical stays resolvable forever
(operator ruling "A RENAME MIGRATES THE CANONICAL TOO", grounds 488ae750/b5663511,
Thoth DM 12786)

Revision ID: 0072
Revises: 0071

`rename_project` now migrates `objects.canonical` (repo:<old> -> repo:<new>) by a
compensating `canonical_changed` event. The object's uuid and every edge/assertion
stay (they key on the uuid); only the unique (type, canonical) string moves. The old
string is appended here so a read or write still naming it resolves to the SAME object
instead of minting a stub. Append-only by construction: rows are only ever INSERTed
(no UPDATE/DELETE path in code); (type, alias) is unique so one alias names one object.
"""
from __future__ import annotations

from alembic import op

revision = "0072"
down_revision = "0071"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # IDEMPOTENT ON A LIVE BOX (Thoth's own addition, DM 12798): a re-run of this
    # revision (a retried deploy, a reconnected alembic session) must never fail on an
    # object already created by an earlier attempt.
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS object_aliases (
            id          bigserial PRIMARY KEY,
            type        text NOT NULL,
            alias       text NOT NULL,
            object_id   uuid NOT NULL REFERENCES objects(id),
            because     text,
            actor       text,
            created_at  timestamptz NOT NULL DEFAULT now(),
            UNIQUE (type, alias)
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS object_aliases_object_idx ON object_aliases (object_id)")


def downgrade() -> None:
    op.execute("DROP TABLE object_aliases")
