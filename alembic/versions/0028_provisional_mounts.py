"""agent_mounts.last_seen may be NULL — a seat that has never acted has no pulse.

THE GHOST (Anubis XII, msg 424). Claude Code fires SessionStart for processes that are not
anybody: `claude bg-spare` pre-warms, pty hosts, claim-socket daemons. Each carries a real
session id and a real cwd, so the whisper seats it — and `last_seen NOT NULL DEFAULT now()`
meant the act of GREETING it handed it a HEARTBEAT. It was then LIVE by every test the fleet
owns: it inflated the roster, it made the co-agent collision warning cry wolf on an uncontended
tree, and it could take delivery of a DM into a process that will never read anything.

The column could not say "this seat has never done a thing", so it said the only thing it could:
"alive, just now". A schema with no way to express IGNORANCE will manufacture a CLAIM — which is
this codebase's named disease (an inference wearing the authority of a declaration) written into
DDL.

NULL now means NEVER ACTED, and it is a real answer. A heartbeat is earned by an act (an Osiris
call, or a transcript that moved under observe_liveness), never granted by a greeting.

Nothing existing changes: every current row has a real timestamp and keeps it.

Revision ID: 0028
Revises: 0027
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0028"
down_revision = "0027"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("agent_mounts", "last_seen", existing_type=sa.DateTime(timezone=True),
                    nullable=True, existing_server_default=sa.text("now()"))


def downgrade() -> None:
    # a row that never acted has no honest timestamp to fall back to; its mount time is the
    # closest true thing we know about it.
    op.execute("UPDATE agent_mounts SET last_seen = mounted_at WHERE last_seen IS NULL")
    op.alter_column("agent_mounts", "last_seen", existing_type=sa.DateTime(timezone=True),
                    nullable=False, existing_server_default=sa.text("now()"))
