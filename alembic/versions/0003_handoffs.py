"""handoffs — human-in-the-loop suspend/resume payload + tray (DESIGN §9)

Revision ID: 0003
Revises: 0002
Create Date: 2026-05-28

A gated helper (or an open helper that hits a challenge) suspends to a handoff:
the run parks in helper_runs.status='awaiting_human' (the authoritative state,
reconciled with the §9 state machine in migration 0001), and this table holds
the browser payload — URL to visit, detected challenge, partial state, cookies
snapshot — plus the tray's priority/TTL. State lives on helper_runs; handoffs
carries everything the analyst's browser needs to resume.
"""
from __future__ import annotations

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE handoffs (
            id               bigserial PRIMARY KEY,
            helper_run_id    uuid NOT NULL REFERENCES helper_runs(id),
            helper_id        text NOT NULL,
            object_id        uuid NOT NULL REFERENCES objects(id),
            case_id          uuid NOT NULL REFERENCES cases(id),
            origin           text,
            url              text,
            challenge_kind   text,            -- cloudflare|turnstile|hcaptcha|... ; null = gated
            partial_state    jsonb NOT NULL DEFAULT '{}',
            cookies_snapshot jsonb,           -- encrypted at rest in Phase 5
            priority         real NOT NULL DEFAULT 0,   -- tray ordering (heuristic)
            created_at       timestamptz NOT NULL DEFAULT now(),
            expires_at       timestamptz,
            resolved_at      timestamptz
        )
        """
    )
    op.execute("CREATE INDEX handoffs_run_idx ON handoffs (helper_run_id)")
    op.execute(
        "CREATE INDEX handoffs_open_idx ON handoffs (priority DESC, created_at) "
        "WHERE resolved_at IS NULL"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS handoffs CASCADE")
