"""stale-window backfill -- every open obligation without a stale_after gets one

practice 393be453's owner-stale ask (open_thread's own stale_after_days, default 14 --
capture.py's DEFAULT_STALE_AFTER_DAYS) only bites threads OPENED after that window
existed. Measured at dispatch time (decision 0d863363/thread b0c5ddff): 216 of 352 open
obligations fleet-wide are older than 14 days and only 7 carry a window today -- the
hygiene rule reaches none of the old rows.

Ships MECHANICALLY (ruling 0d863363: "the shape should carry over to other projects...
as if we were pushing out updates to a stranger's machine, osiris is open source") --
never a coordinator's hand pass. A stranger's install gets exactly the same backfill on
their next `osiris migrate` (which IS `alembic upgrade head`, cmd_migrate in cli.py).

Idempotent and safe to re-run: the NOT EXISTS guard means a second pass touches nothing,
and a thread that already carries a window (any source) is never re-stamped or
overwritten -- this only fills a true gap, it never corrects an existing value.

`stale_after` = the thread's own `created_at` (objects.created_at, the mint time -- the
best available proxy for "observed_at of the thread" on a row this old) + 14 days,
floored at `now() + 1 day` so a fleet-wide backfill does not page every stale owner in
the same instant it deploys -- the same DEFAULT_STALE_AFTER_DAYS window open_thread's own
live path already uses.

STALE_AFTER_BACKFILL_SQL is a module-level constant (not inlined in upgrade(), matching
no other migration in this tree, but the alternative -- a hand-copied second string in
this migration's own test -- is exactly the drift risk this house's own standing
practice on load-bearing comments warns about) so tests/test_stale_after_backfill.py can
import and run the identical SQL directly, since alembic migrations apply once at test-
session start (conftest) against an otherwise-empty graph and never see a synthetic old
obligation a test creates afterward.
"""
from __future__ import annotations

from alembic import op

revision = "0058"
down_revision = "0057"
branch_labels = None
depends_on = None

STALE_AFTER_BACKFILL_SOURCE = "migration:0058_stale_after_backfill"

STALE_AFTER_BACKFILL_SQL = f"""
    INSERT INTO assertions (object_id, name, value, source_id, observed_at, confidence,
                            evidence_class)
    SELECT o.id, 'stale_after',
           to_jsonb(GREATEST(o.created_at + interval '14 days',
                             now() + interval '1 day')::text),
           '{STALE_AFTER_BACKFILL_SOURCE}', now(), 0.9, 'self_declared'
    FROM objects o
    WHERE o.type = 'Thread' AND o.status = 'active'
      AND EXISTS (SELECT 1 FROM current_assertions ca WHERE ca.object_id = o.id
                  AND ca.name = 'kind' AND ca.value #>> '{{}}' = 'obligation')
      AND EXISTS (SELECT 1 FROM current_assertions ca WHERE ca.object_id = o.id
                  AND ca.name = 'status' AND ca.value #>> '{{}}' = 'open')
      AND NOT EXISTS (SELECT 1 FROM current_assertions ca WHERE ca.object_id = o.id
                      AND ca.name = 'stale_after')
"""


def upgrade() -> None:
    op.execute(STALE_AFTER_BACKFILL_SQL)


def downgrade() -> None:
    # Compensating-event law (constitution #3): a migration never DELETEs a fact it
    # wrote. The honest state to revert to is "we backfilled a window", not "we
    # un-observed one" -- nothing to reverse.
    pass
