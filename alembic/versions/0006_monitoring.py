"""monitoring — the watch (subscriptions, alerts, watermarks)

Revision ID: 0006
Revises: 0005
Create Date: 2026-06-26

The kernel was a *lens* (investigate on demand). This adds the *tripwire*: saved
criteria (`subscriptions`) matched against the durable outbox stream, firing rows
into a dumb sink (`alerts` table; optional webhook). The match is source-agnostic —
the outbox already carries object_created / property_added / link_created.

Two cursors, two jobs:
  * `outbox.evaluated_at` — the subscription evaluator's gap-free claim flag,
    INDEPENDENT of the cascade's `published_at` (two consumers, one outbox). A row
    is evaluated exactly once; new subscriptions are prospective by design.
  * `watermarks` — a generic "last cursor" store for SOURCE pulls (a scheduled tick
    pulls only the delta past its cursor). Keyed `source:<id>`; re-pulls already safe
    via find-or-create, the watermark just makes them cheap.
"""
from __future__ import annotations

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # the evaluator's own claim flag on the shared outbox (cascade owns published_at)
    op.execute("ALTER TABLE outbox ADD COLUMN evaluated_at timestamptz")
    op.execute(
        "CREATE INDEX outbox_unevaluated_idx ON outbox (id) WHERE evaluated_at IS NULL"
    )

    # generic per-source cursor store (source ticks; not the evaluator)
    op.execute(
        """
        CREATE TABLE watermarks (
            key        text PRIMARY KEY,
            cursor     text NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )

    # saved match criteria — "tell me when X happens"
    op.execute(
        """
        CREATE TABLE subscriptions (
            id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            name        text NOT NULL,
            criteria    jsonb NOT NULL,
            webhook_url text,
            active      boolean NOT NULL DEFAULT true,
            created_at  timestamptz NOT NULL DEFAULT now()
        )
        """
    )

    # the dumb sink — a durable record per fired match (NOT a CRM)
    op.execute(
        """
        CREATE TABLE alerts (
            id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            subscription_id uuid NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
            outbox_id       bigint NOT NULL,
            object_id       uuid REFERENCES objects(id),
            event_type      text NOT NULL,
            matched         jsonb NOT NULL DEFAULT '{}',
            created_at      timestamptz NOT NULL DEFAULT now(),
            delivered_at    timestamptz,
            UNIQUE (subscription_id, outbox_id)
        )
        """
    )
    op.execute("CREATE INDEX alerts_undelivered_idx ON alerts (id) WHERE delivered_at IS NULL")
    op.execute("CREATE INDEX alerts_subscription_idx ON alerts (subscription_id, created_at)")


def downgrade() -> None:
    op.execute("DROP TABLE alerts")
    op.execute("DROP TABLE subscriptions")
    op.execute("DROP TABLE watermarks")
    op.execute("DROP INDEX outbox_unevaluated_idx")
    op.execute("ALTER TABLE outbox DROP COLUMN evaluated_at")
