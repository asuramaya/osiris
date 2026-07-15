"""The identity core's hot half (ruling 5cef856b) — attach tokens + the seat binding.

Two pieces of PLAIN TABLE state beside the event-sourced kernel, deliberately:

* `seat_tokens` — one-time attach tokens. A token is a HOT SECRET: it must be revocable and
  must never be readable out of the graph's assertion history (an append-only kernel cannot
  un-say a secret). The spawner (the manager daemon) mints one per summoned session and
  exports it in the child's environment before the harness's first breath; the whisper
  presents it; the server binds and marks it used. A token re-presented by a DIFFERENT
  session is refused loudly (thread 2294e95d, ask #1 — silence was the whole bug).

* `agent_mounts.seat_id` — the durable binding of a session to its Seat object. The mount
  row is already the durable per-session anchor (keyed by job_dir); the seat binding rides
  it so a server bounce, a re-attach, or a lineage mint (which rewrites agent_id) never
  loses WHICH ROLE this session sits in. The Seat itself (seat:<uuid8>) lives in the kernel
  as a first-class object; only the session→seat pointer is hot state.

Revision ID: 0031
Revises: 0030
"""
from alembic import op

revision = "0031"
down_revision = "0030"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE seat_tokens (
            token      text PRIMARY KEY,
            seat_id    text NOT NULL,
            minted_by  text,
            minted_at  timestamptz NOT NULL DEFAULT now(),
            used_by    text,
            used_at    timestamptz
        )
    """)
    op.execute("CREATE INDEX seat_tokens_seat ON seat_tokens (seat_id)")
    op.execute("ALTER TABLE agent_mounts ADD COLUMN seat_id text")
    op.execute("CREATE INDEX agent_mounts_seat ON agent_mounts (seat_id) "
               "WHERE seat_id IS NOT NULL")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS agent_mounts_seat")
    op.execute("ALTER TABLE agent_mounts DROP COLUMN IF EXISTS seat_id")
    op.execute("DROP TABLE IF EXISTS seat_tokens")
