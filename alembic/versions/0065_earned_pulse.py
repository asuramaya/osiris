"""agent_mounts.earned_pulse_at — a durable, provenance-bearing pulse (THE EARNED-PULSE
COLUMN, decisions 04c1b4b4/687cba9d/813bcb14, thread 870d7391, operator ruling 2026-09-11).

THE GAP THIS CLOSES: 0028 (provisional mounts) made `last_seen` nullable so a whisper seat
could exist without a pulse — but the "earned vs granted" distinction it introduced stayed
pure BEHAVIOR, living only in save_mount's own CASE WHEN. Five OTHER writers (heartbeat.py's
statusline bump, three agents.py succession-repair sites, and the heir-mint path itself) all
touch `last_seen` without going through that CASE at all — measured live, 2026-09-11: 70% of
"live" whisper-provisional rows were forged (32/46, exhaustive transcript cross-check), and
one of the five ungated doors minted a whole new Agent generation (agent:f0d23039-ii) off
statusline-observed model drift alone, with zero conversation ever behind it.

`earned_pulse_at timestamptz`, nullable, no default: stamped ONLY by the two writers that
genuinely earn a pulse — save_mount's own `alive=True` path (a real mount()/automount() MCP
call) and liveness.py's `observe_liveness` transcript-mtime promotion (a transcript growing
IS an earned act, per that module's own law) — first-earn only, never overwritten by a later
touch. Every OTHER writer that bumps `last_seen` now reads this column first and refuses to
grant a pulse a row never earned.

LOCK SAFETY (Practice 513326d6, this table's own prior specimen — 0028's edit, #166's
`SET NOT NULL` deadlock): a bare `ADD COLUMN ... timestamptz` with no default (or a constant
NULL default) is METADATA-ONLY in Postgres at any table size — no table rewrite, no scan,
AccessExclusiveLock held for the DDL statement's own near-instant duration only. No backfill
follows it (the whole point — see below), so there is no slow step to autocommit-split away
from the fast one; unlike 0028/0047's own downgrades, this migration has nothing 513326d6
would flag.

THE 32 ALREADY-FORGED ROWS (and every other pre-migration row): deliberately NEVER flipped
to earned — no backfill UPDATE at all. Any attempt to retroactively classify the existing
population (even the "11 promotable" ones, verified only as far as "a transcript exists",
which is necessary but not sufficient) would launder tonight's fabrication into the "real"
population this column exists to distinguish. A genuinely-live row re-earns its pulse
naturally on its next real mount() call or transcript growth, same as a fresh row would.

THE RECONCILIATION RECEIPT: a read-only NOTICE, no lock implication beyond an ordinary
SELECT, naming how many existing rows the OLD law (`last_seen IS NOT NULL`) would have read
as "live" — precisely the population this migration deliberately does NOT carry over. Visible
in the migration's own log, once, at upgrade time; never written anywhere for a caller to
query later (a receipt for the operator's eye, not a new API).

Revision ID: 0065
Revises: 0064
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0065"
down_revision = "0064"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("agent_mounts",
                  sa.Column("earned_pulse_at", sa.DateTime(timezone=True), nullable=True))
    conn = op.get_bind()
    old_law_live = conn.execute(
        sa.text("SELECT count(*) FROM agent_mounts WHERE last_seen IS NOT NULL")).scalar()
    total = conn.execute(sa.text("SELECT count(*) FROM agent_mounts")).scalar()
    op.execute(sa.text(
        f"DO $$ BEGIN RAISE NOTICE "
        f"'earned_pulse_at added, no backfill: % of % existing agent_mounts rows read "
        f"live under the OLD last_seen-only law and are deliberately NOT carried over — "
        f"each re-earns its own pulse on its next real mount() call or transcript growth', "
        f"{int(old_law_live or 0)}, {int(total or 0)}; END $$;"))


def downgrade() -> None:
    # metadata-only, same as the upgrade — no data to preserve, this column is derived
    # provenance, never a source of truth anything downstream is the only copy of.
    op.drop_column("agent_mounts", "earned_pulse_at")
