"""pg_autotune (ruling 45b251ed) — deriving Postgres GUCs from the live host instead of
a hand-run postgres_tuning.sql. `apply_tuning` is tested against a FAKE pool, never the
live `actions` fixture's real shared osiris-pg: `ALTER SYSTEM` is not transactional and
would persist a test's own tuning values onto the box every other test (and the fleet)
runs against. `plan_tuning`/`read_host_resources` are read-only (`SHOW`, /proc/meminfo)
and safe against the live pool."""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.pg_autotune import (
    _fmt_mem,
    _parse_mem,
    _significant_change,
    apply_tuning,
    compute_recommended,
    plan_tuning,
    read_host_resources,
)


class _FakePool:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple]] = []

    async def execute(self, query: str, *args: object) -> str:
        self.executed.append((query, args))
        return "ALTER SYSTEM"


def test_read_host_resources_reports_real_positive_values() -> None:
    host = read_host_resources()
    assert host["mem_total_bytes"] > 0
    assert host["mem_available_bytes"] > 0
    assert host["cpu_count"] >= 1


def test_compute_recommended_sizes_off_available_ram_not_total() -> None:
    small = compute_recommended(mem_available_bytes=1024**3, fixed_budget=56)
    large = compute_recommended(mem_available_bytes=32 * 1024**3, fixed_budget=56)
    assert small["shared_buffers"] < large["shared_buffers"]
    assert small["effective_cache_size"] < large["effective_cache_size"]


def test_compute_recommended_max_connections_tracks_fixed_budget() -> None:
    out = compute_recommended(mem_available_bytes=16 * 1024**3, fixed_budget=56,
                              headroom_target=40)
    assert out["max_connections"] == 96


def test_compute_recommended_shared_buffers_never_below_128mb_floor() -> None:
    out = compute_recommended(mem_available_bytes=1, fixed_budget=56)
    assert out["shared_buffers"] == 128 * 1024 * 1024


def test_fmt_mem_and_parse_mem_round_trip() -> None:
    for n in (128 * 1024 * 1024, 4 * 1024**3, 512 * 1024**2, 12 * 1024**3):
        assert _parse_mem(_fmt_mem(n)) == n


def test_significant_change_ignores_small_rounding_drift() -> None:
    # 4GB current vs a recommendation 5% higher — inside the 15% tolerance, not a change.
    assert not _significant_change("work_mem", "32MB", 33 * 1024 * 1024)


def test_significant_change_flags_a_real_swing() -> None:
    assert _significant_change("shared_buffers", "128MB", 4 * 1024**3)


def test_significant_change_max_connections_is_exact() -> None:
    assert _significant_change("max_connections", "100", 96)
    assert not _significant_change("max_connections", "96", 96)


async def test_plan_tuning_reads_every_owned_guc_off_the_live_server(actions: Actions) -> None:
    plan = await plan_tuning(actions.pool, fixed_budget=56)
    for name in ("work_mem", "maintenance_work_mem", "effective_cache_size",
                 "random_page_cost", "shared_buffers", "max_connections"):
        assert name in plan["current"]
    assert plan["host"]["mem_available_bytes"] > 0


async def test_apply_tuning_applies_reloadable_and_reloads_once() -> None:
    pool = _FakePool()
    plan = {"changes": [
        {"name": "work_mem", "before": "4MB", "after": "32MB", "restart_required": False},
    ]}
    result = await apply_tuning(pool, plan)
    assert result["applied"] == plan["changes"]
    assert result["deferred"] == []
    queries = [q for q, _ in pool.executed]
    assert any("ALTER SYSTEM SET work_mem" in q for q in queries)
    assert queries.count("SELECT pg_reload_conf()") == 1


async def test_apply_tuning_never_restarts_postgres_itself() -> None:
    """A restart-required change is persisted (ALTER SYSTEM, free — takes effect at
    whatever restart happens next) but always comes back deferred: this module has no
    restart mechanism at all, by design — see pg_autotune.py's own docstring."""
    pool = _FakePool()
    plan = {"changes": [
        {"name": "shared_buffers", "before": "128MB", "after": "4GB",
         "restart_required": True},
    ]}
    result = await apply_tuning(pool, plan)
    assert result["applied"] == []
    assert result["deferred"] == plan["changes"]
    queries = [q for q, _ in pool.executed]
    assert any("ALTER SYSTEM SET shared_buffers" in q for q in queries)
    # No reloadable change in this plan -> pg_reload_conf is never called either.
    assert "SELECT pg_reload_conf()" not in queries


async def test_apply_tuning_no_changes_is_a_no_op() -> None:
    pool = _FakePool()
    result = await apply_tuning(pool, {"changes": []})
    assert result == {"applied": [], "deferred": []}
    assert pool.executed == []
