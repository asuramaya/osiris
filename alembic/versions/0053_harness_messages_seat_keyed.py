"""harness_messages: key recovery on the SEAT, never job_dir fragments (decision d394c5f7,
Thoth's ruling DM 5383). 0051's `from_agent`/`to_resolved` both leaned on
`agent_mounts.job_dir` fragment-matching — confirmed live (task #181's Ptah/Ra recovery)
this is FALSE for any long-lived seat: `job_dir` persists across `claude --resume`/
compaction while a seat's own occupant (its `holds` link) can change mid-thread (Ptah's
own case). The durable key is the SEAT, resolved from `soul_sessions.source_path`'s own
office-directory slug (Claude Code dashes the session's cwd into its project-directory
name; once a seat has gone through `establish_office`, that cwd is literally
`~/.osiris/seats/<handle>/...`, so the slug survives resume/compaction/identity-swap in a
way `job_dir` cannot).

Two columns replace `from_agent`: `seat` (the durable Seat canonical — WHO this session's
office belongs to) and `held_seat` (the Agent canonical who actually held that seat at the
turn's own `observed_at`, per the `holds` link's temporal validity — `first_seen`/
`valid_until`, not "now"). Ruling: record BOTH, never merge them into one guess — a later
reader must be able to tell "the seat" from "who held it then".

Ruling (b): the harness's own `to` field (SendMessage's 17-hex handle) lives in a
DIFFERENT identifier space than osiris's own agent canonicals — do not try to map it in.
`to_resolved` (0051's best-effort job_dir-fragment guess) is dropped outright; `to_raw`
is renamed to `harness_to` to say plainly what it is: verbatim harness audit data, never
an osiris id.

Revision ID: 0053
Revises: 0052
"""
from alembic import op

revision = "0053"
down_revision = "0052"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE harness_messages RENAME COLUMN to_raw TO harness_to")
    op.execute("ALTER TABLE harness_messages DROP COLUMN to_resolved")
    op.execute("ALTER TABLE harness_messages DROP COLUMN from_agent")
    op.execute("ALTER TABLE harness_messages ADD COLUMN seat text")
    op.execute("ALTER TABLE harness_messages ADD COLUMN held_seat text")
    op.execute("CREATE INDEX harness_messages_seat ON harness_messages (seat)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS harness_messages_seat")
    op.execute("ALTER TABLE harness_messages DROP COLUMN held_seat")
    op.execute("ALTER TABLE harness_messages DROP COLUMN seat")
    op.execute("ALTER TABLE harness_messages ADD COLUMN from_agent text")
    op.execute("ALTER TABLE harness_messages ADD COLUMN to_resolved text")
    op.execute("ALTER TABLE harness_messages RENAME COLUMN harness_to TO to_raw")
    op.execute("CREATE INDEX harness_messages_from ON harness_messages (from_agent)")
