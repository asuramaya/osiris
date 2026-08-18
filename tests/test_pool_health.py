"""pg_activity_by_app (task #180 piece 2 (c)) — the pg_stat_activity-by-daemon surface
fleet() folds in as pool_health."""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.pool_health import pg_activity_by_app


async def test_pg_activity_by_app_names_this_connections_own_application(
    actions: Actions,
) -> None:
    row = await actions.pool.fetchval("SELECT current_setting('application_name')")
    key = row or "(unnamed)"  # an untagged connection's own application_name is '' — the
    # blank string is a legitimate Postgres default, not the absence of a key, so it gets
    # the same "(unnamed)" label a caller-supplied blank would.
    out = await pg_activity_by_app(actions.pool)
    assert key in out["by_application"]
    assert out["by_application"][key] >= 1
    assert out["backends"] >= 1


async def test_pg_activity_by_app_carries_cumulative_tx_totals(actions: Actions) -> None:
    out = await pg_activity_by_app(actions.pool)
    assert isinstance(out["tx_total"]["xact_commit"], int)
    assert isinstance(out["tx_total"]["xact_rollback"], int)
