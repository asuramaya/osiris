"""resource_leases — task #103/#119, an exact-key resource lock (ruling 6a811488's fix:

worktrees kill MECHANICAL collision on the git index; this kills SEMANTIC collision on
everything else a fleet of agents can still step on at once — a file path, the docker
daemon, a merge-back into composer. open_thread(assignee=)'s `leased_to` looked like this
primitive already existed (Alfred's #4.3) but its conflict detection is fuzzy prose
similarity over a thread SUMMARY (find_near_duplicate_open_thread, threshold 0.60, never
empirically tuned) — no resource_id, no atomicity, repo-scoped only. Two agents about to
touch the same file with differently-worded summaries would fall under threshold and get no
lease at all: the tool meant to prevent silent collision would silently collide.

Mirrors helper_runs_active_claim (0001_initial_schema) exactly: a partial unique index over
the ACTIVE state only, so the same resource_id is re-claimable the moment it's released —
a claim, not a permanent lock. `thread_id` links each lease to a companion Thread object
(existing type, `objects(type,canonical)` already unique — canonical is a hash of
resource_id itself, never of prose, so the LOOKUP is exact by construction) for graph
visibility and mail integration; the SQL table's partial unique index is the actual
atomicity guarantee, since `current_assertions` is a view with no indexes of its own and
can't carry one.

Revision ID: 0042
Revises: 0041
Create Date: 2026-07-30
"""
from __future__ import annotations

from alembic import op

revision = "0042"
down_revision = "0041"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE resource_leases (
            id           bigserial PRIMARY KEY,
            resource_id  text NOT NULL,
            thread_id    uuid NOT NULL REFERENCES objects(id),
            holder       text NOT NULL,
            status       text NOT NULL DEFAULT 'held',  -- held|released|reaped
            acquired_at  timestamptz NOT NULL DEFAULT now(),
            released_at  timestamptz
        )
        """
    )
    op.execute(
        "CREATE INDEX resource_leases_resource_idx ON resource_leases "
        "(resource_id, acquired_at DESC)"
    )
    op.execute(
        "CREATE INDEX resource_leases_thread_idx ON resource_leases (thread_id)"
    )
    # the claim itself: one HELD row per resource_id at a time. A released/reaped resource_id
    # can be claimed again immediately — this is a lock, not a history eraser (old rows stay,
    # append-only, same doctrine as the rest of the kernel).
    op.execute(
        "CREATE UNIQUE INDEX resource_leases_active_claim ON resource_leases "
        "(resource_id) WHERE status = 'held'"
    )


def downgrade() -> None:
    op.execute("DROP TABLE resource_leases")
