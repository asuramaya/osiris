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


async def test_pg_activity_by_app_carries_the_envelope(actions: Actions) -> None:
    """msg 5340, THE ENVELOPE: each daemon's configured cap beside its current backend
    count and utilization, plus the whole-box fixed-budget arithmetic — all four known
    daemons present even when none of them currently hold a connection (cap is a
    CONFIGURED fact, not conditioned on current activity)."""
    from src.config.settings import get_settings

    out = await pg_activity_by_app(actions.pool)
    settings = get_settings()
    for app, expected_cap in (
        ("osiris-mcp", settings.osiris_mcp_pool_size),
        ("osiris-worker", settings.osiris_worker_pool_size),
        ("osiris-console", settings.osiris_api_pool_size),
        ("osiris-manager", settings.osiris_manager_pool_size),
    ):
        assert app in out["caps"]
        assert out["caps"][app]["cap"] == expected_cap
        assert out["caps"][app]["current"] >= 0
        assert out["caps"][app]["pct"] is not None

    expected_budget = (settings.osiris_mcp_pool_size + settings.osiris_worker_pool_size
                       + settings.osiris_api_pool_size + settings.osiris_manager_pool_size)
    assert out["fixed_budget"] == expected_budget
    assert isinstance(out["max_connections"], int)
    assert out["headroom"] == out["max_connections"] - expected_budget
