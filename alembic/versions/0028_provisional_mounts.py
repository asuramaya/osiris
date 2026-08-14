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

EDITED IN PLACE, 2026-08-14 (Practice — a migration's locks are held for the duration of
the LONGEST step in its own transaction, 0047's own finding, decision 259e5c5b): this
migration was already applied everywhere live, so this edit changes nothing for any
already-migrated database — alembic tracks progress by revision id alone, never re-runs
or re-verifies an applied revision's content. It changes only what a FRESH database
build does from here on (a new dev box, a test container migrating from scratch) —
exactly where this fix needed to land, since a fresh build is the only place this file's
content still executes at all.

`upgrade()` was always safe as written and needed no change: dropping NOT NULL is
metadata-only in Postgres regardless of table size, no scan, no meaningful lock duration.

`downgrade()` was the hazard, structurally identical to 0047's (found while measuring
task #169's open half, decision be99505c): `UPDATE ... backfill` then `ALTER COLUMN ...
SET NOT NULL` in one ambient transaction. Unlike 0047's shape (a FAST lock held across a
separately SLOW step), here the exclusive-lock statement is itself the slow one — a bare
`SET NOT NULL` makes Postgres scan the WHOLE table under AccessExclusiveLock to prove no
NULL survives, blocking every reader of agent_mounts (touched by every mount()/orient()
call fleet-wide) for the scan's own duration. The fix is the standard PG12+ pattern: a
NOT VALID check costs nothing to add (no scan), VALIDATE CONSTRAINT does the scan under
ShareUpdateExclusiveLock only (blocks other DDL, never a reader or a writer), and by the
time SET NOT NULL runs it sees the already-valid constraint and skips its own scan
entirely. Verified under live concurrent read load with scripts/migration_lock_stress.py
(Seshat's general instrument, built to validate 0047) before this file was edited.

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

_NOT_NULL_CHECK = "agent_mounts_last_seen_not_null_check"


def upgrade() -> None:
    # SAFE AS-IS, unchanged: dropping NOT NULL is metadata-only, no table scan at any size.
    op.alter_column("agent_mounts", "last_seen", existing_type=sa.DateTime(timezone=True),
                    nullable=True, existing_server_default=sa.text("now()"))


def downgrade() -> None:
    # a row that never acted has no honest timestamp to fall back to; its mount time is the
    # closest true thing we know about it. RowExclusiveLock only — never blocks a reader,
    # safe to run for its full duration inside an ordinary transaction (same reasoning
    # 0047's own backfill UPDATE uses).
    with op.get_context().autocommit_block():
        op.execute("UPDATE agent_mounts SET last_seen = mounted_at WHERE last_seen IS NULL")
    # NOT VALID: adds the constraint without scanning existing rows — near-instant
    # regardless of table size, own block so a later step's lock never compounds with it.
    with op.get_context().autocommit_block():
        op.execute(f"ALTER TABLE agent_mounts ADD CONSTRAINT {_NOT_NULL_CHECK} "
                   "CHECK (last_seen IS NOT NULL) NOT VALID")
    # THE SCAN, moved here on purpose: VALIDATE CONSTRAINT takes ShareUpdateExclusiveLock
    # only — concurrent reads AND writes proceed unblocked while it walks the table.
    with op.get_context().autocommit_block():
        op.execute(f"ALTER TABLE agent_mounts VALIDATE CONSTRAINT {_NOT_NULL_CHECK}")
    # now instant: Postgres sees the already-VALID check and skips its own scan entirely —
    # the only step that ever needed AccessExclusiveLock now holds it for a heartbeat.
    with op.get_context().autocommit_block():
        op.alter_column("agent_mounts", "last_seen", existing_type=sa.DateTime(timezone=True),
                        nullable=False, existing_server_default=sa.text("now()"))
    with op.get_context().autocommit_block():
        op.execute(f"ALTER TABLE agent_mounts DROP CONSTRAINT {_NOT_NULL_CHECK}")
