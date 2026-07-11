"""the organized desk — bands, dim annotations (operator direction, 2026-07-11)

Revision ID: 0026
Revises: 0025
Create Date: 2026-07-11

The operator's desk drowned in shape, not volume: 5 of 12 briefs were one fleet-wide
condition reported five times, moot alarms lingered because only the human may dismiss,
and CRITICAL sat interleaved with FYI. Two columns fix the record side:

* desk_kind — the sender's own triage ('decision' | 'hands' | 'fyi'): which band of the
  desk this brief belongs to. Null = legacy/unclassified; the read side heuristic-bands it.
* moot_* — an agent's DIM annotation: "this was true when sent, is moot now, here's why".
  Dimming NEVER settles (the membrane: only the operator dismisses); it just saves the
  human the archaeology. Stamped with who and when — an annotation is testimony.

Same-story folding needs no schema: pg_trgm (0025) clusters near-duplicate briefs at read
time. The your-queue section derives from owner='operator' threads (graph, not mail).
"""
from __future__ import annotations

from alembic import op

revision = "0026"
down_revision = "0025"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE fleet_messages ADD COLUMN desk_kind text")
    op.execute("ALTER TABLE fleet_messages ADD COLUMN moot_note text")
    op.execute("ALTER TABLE fleet_messages ADD COLUMN moot_by text")
    op.execute("ALTER TABLE fleet_messages ADD COLUMN moot_at timestamptz")


def downgrade() -> None:
    op.execute("ALTER TABLE fleet_messages DROP COLUMN desk_kind")
    op.execute("ALTER TABLE fleet_messages DROP COLUMN moot_note")
    op.execute("ALTER TABLE fleet_messages DROP COLUMN moot_by")
    op.execute("ALTER TABLE fleet_messages DROP COLUMN moot_at")
