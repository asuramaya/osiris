"""The owner-law residue re-runner (operator's word via Thoth msg 8606/8618, 2026-09-09)
-- a thin script wiring plan_owner_normalization/apply_owner_normalization (migration
0059's own tested resolver, fully covered by tests/test_owner_normalization.py) behind
the same dry-run-hard-default/--apply convention as
scripts/close_zero_recipient_dm_backlog.py. Only the genuinely new wiring -- the
refuse_silent_live_db guard -- is tested here, same scope as that sibling script's own
test file: a script's own DSN binds at import time (before the `pg_dsn` fixture sets
DATABASE_URL), so exercising its DB-writing path through run() itself belongs to a real
process invocation, never a pytest-internal call -- the resolve/apply logic underneath
is what's actually under test, and that already has full coverage."""
from __future__ import annotations

from scripts.run_owner_normalization import run


async def test_refuses_without_allow_live_or_database_url(monkeypatch) -> None:
    monkeypatch.delenv("OSIRIS_ALLOW_LIVE", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    try:
        await run(apply=False)
        raised = False
    except SystemExit:
        raised = True
    assert raised
