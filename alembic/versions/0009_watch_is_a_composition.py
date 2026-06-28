"""fold subscriptions into compositions — a watch IS a composition (P3)

Revision ID: 0009
Revises: 0008
Create Date: 2026-06-28

The watch and the lens become ONE primitive. A watch is a `kind='watch'` composition
whose spec is a `select` op-tree: the SAME spec you run() on demand (the lens — current
members) drives the evaluator (the tripwire — alert when a NEW object enters that set).
So the `subscriptions` table goes away; its rows migrate into `compositions`, and `alerts`
re-points at the composition that fired.

Compositions gain the watch's execution metadata (`webhook_url`, `active`); a lens just
ignores them. Existing subscription criteria `{object_type, where}` become a `select`
spec (the event-level generality — value_contains / property_name on arbitrary mutations —
is intentionally dropped: a watch now fires on set-entry, the real-beat semantic). Ids are
preserved so already-fired `alerts` keep pointing at the right watch.
"""
from __future__ import annotations

from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # a watch's execution metadata on the unified primitive (a lens leaves them default)
    op.execute("ALTER TABLE compositions ADD COLUMN webhook_url text")
    op.execute("ALTER TABLE compositions ADD COLUMN active boolean NOT NULL DEFAULT true")

    # migrate each subscription into a watch-composition, PRESERVING its id (so alerts that
    # already fired keep referencing the right watch after the FK re-point below). The
    # criteria's object_type + where become a `select` spec; other criteria keys are dropped.
    op.execute(
        """
        INSERT INTO compositions (id, name, kind, spec, webhook_url, active)
        SELECT s.id, s.name, 'watch',
               jsonb_build_object('op', 'select',
                                  'object_type', s.criteria->>'object_type',
                                  'where', coalesce(s.criteria->'where', '[]'::jsonb)),
               s.webhook_url, s.active
        FROM subscriptions s
        ON CONFLICT (name) DO NOTHING
        """
    )

    # re-point alerts: subscription_id -> composition_id (same uuids, now in compositions)
    op.execute("ALTER TABLE alerts DROP CONSTRAINT alerts_subscription_id_fkey")
    op.execute("ALTER TABLE alerts DROP CONSTRAINT alerts_subscription_id_outbox_id_key")
    op.execute("ALTER TABLE alerts RENAME COLUMN subscription_id TO composition_id")
    op.execute(
        "ALTER TABLE alerts ADD CONSTRAINT alerts_composition_id_fkey "
        "FOREIGN KEY (composition_id) REFERENCES compositions(id) ON DELETE CASCADE"
    )
    op.execute(
        "ALTER TABLE alerts ADD CONSTRAINT alerts_composition_id_outbox_id_key "
        "UNIQUE (composition_id, outbox_id)"
    )
    op.execute("DROP INDEX alerts_subscription_idx")
    op.execute("CREATE INDEX alerts_composition_idx ON alerts (composition_id, created_at)")

    op.execute("DROP TABLE subscriptions")


def downgrade() -> None:
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
    # reconstruct subscription rows from watch-compositions (criteria from the select spec)
    op.execute(
        """
        INSERT INTO subscriptions (id, name, criteria, webhook_url, active)
        SELECT id, name,
               jsonb_build_object('event_types', jsonb_build_array('object_created'),
                                  'object_type', spec->>'object_type',
                                  'where', coalesce(spec->'where', '[]'::jsonb)),
               webhook_url, active
        FROM compositions WHERE kind='watch'
        """
    )
    op.execute("DROP INDEX alerts_composition_idx")
    op.execute("ALTER TABLE alerts DROP CONSTRAINT alerts_composition_id_outbox_id_key")
    op.execute("ALTER TABLE alerts DROP CONSTRAINT alerts_composition_id_fkey")
    op.execute("ALTER TABLE alerts RENAME COLUMN composition_id TO subscription_id")
    op.execute(
        "ALTER TABLE alerts ADD CONSTRAINT alerts_subscription_id_fkey "
        "FOREIGN KEY (subscription_id) REFERENCES subscriptions(id) ON DELETE CASCADE"
    )
    op.execute(
        "ALTER TABLE alerts ADD CONSTRAINT alerts_subscription_id_outbox_id_key "
        "UNIQUE (subscription_id, outbox_id)"
    )
    op.execute("CREATE INDEX alerts_subscription_idx ON alerts (subscription_id, created_at)")
    op.execute("DELETE FROM compositions WHERE kind='watch'")
    op.execute("ALTER TABLE compositions DROP COLUMN active")
    op.execute("ALTER TABLE compositions DROP COLUMN webhook_url")
