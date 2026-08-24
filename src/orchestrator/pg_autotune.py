"""pg_autotune — derive Postgres GUCs from THIS host's actual RAM/CPU and the measured
daemon envelope (`pool_health`'s own `fixed_budget`), instead of a hand-run
postgres_tuning.sql the operator has to re-apply every time the fleet's envelope moves.
Ruling 45b251ed, the operator's own word on desk 5092: "this should be mechanical and
not depend on me, autotune dynamically?" Rulings 7d6815bb (mechanisms, never band-aids)
and df646654 (self-healing over manual) apply directly. `deploy/postgres_tuning.sql`'s
static values (applied by hand 2026-08-17, decision 1afeca3a) are what this REPLACES —
that file stays as the historical record of the values it computed on that one day, this
module recomputes them every time it runs.

RELOADABLE VS RESTART-REQUIRING (Postgres's own GUC context, `pg_settings.context`):
work_mem / maintenance_work_mem / effective_cache_size / random_page_cost are `sighup`
or `user` context — `ALTER SYSTEM` + `pg_reload_conf()` picks them up with no
interruption to a single live backend. `shared_buffers` and `max_connections` are
`postmaster` context — nothing short of a restart moves them, ever.

THIS MODULE NEVER RESTARTS POSTGRES ITSELF (house law: a worker never restarts
services, CLAUDE.md's own line — a restart of the ONE shared instance sixteen live
agents depend on is the operator's/Thoth's hand, not code running unattended on a
timer). It applies the reloadable half unconditionally (never a stop-the-world, msg
5397) and PERSISTS the restart-requiring half via `ALTER SYSTEM` regardless — that
costs nothing and takes effect at whatever restart happens next, from any cause — but a
restart-required change always comes back in `deferred`, confessed loudly (never
silently dropped), for a human to act on.

AVAILABLE, NOT TOTAL, RAM: this host is SHARED with the rest of the fleet's own
processes (`deploy/postgresql.conf`'s own prior reasoning, measured 2026-08-17) — sizing
off `MemAvailable` rather than `MemTotal` is the load-bearing difference between this
module and the usual "25% of RAM" dedicated-server rule of thumb."""
from __future__ import annotations

import os
from typing import Any

import asyncpg

# Postgres 16 pg_settings.context: 'sighup'/'user' (reloadable, no interruption) vs
# 'postmaster' (restart-only). Order matters only for readability in reports below.
_RELOADABLE = ("work_mem", "maintenance_work_mem", "effective_cache_size", "random_page_cost")
_RESTART_REQUIRED = ("shared_buffers", "max_connections")
_ALL_GUCS = (*_RELOADABLE, *_RESTART_REQUIRED)

_UNIT_BYTES = {"TB": 1024**4, "GB": 1024**3, "MB": 1024**2, "kB": 1024, "B": 1}


def read_host_resources() -> dict[str, int]:
    """Actual host RAM (total + available, bytes) and CPU count off /proc/meminfo and
    os.cpu_count() — never a hardcoded sizing-guide fraction, never guessed."""
    mem: dict[str, int] = {}
    with open("/proc/meminfo") as f:
        for line in f:
            key, _, rest = line.partition(":")
            if key in ("MemTotal", "MemAvailable"):
                mem[key] = int(rest.strip().split()[0]) * 1024  # kB -> bytes
    return {
        "mem_total_bytes": mem.get("MemTotal", 0),
        "mem_available_bytes": mem.get("MemAvailable", mem.get("MemTotal", 0)),
        "cpu_count": os.cpu_count() or 1,
    }


def compute_recommended(
    *, mem_available_bytes: int, fixed_budget: int, headroom_target: int = 40,
) -> dict[str, float | int]:
    """Mixed-workload sizing ratios (the same shape pgtune/postgresqlco.nf formulas use),
    applied against AVAILABLE RAM. `max_connections` is derived from the measured fixed
    daemon budget (`pool_health.pg_activity_by_app`'s own `fixed_budget`) plus a stated
    ad-hoc/CLI headroom target — docs/DEPLOY.md's own envelope section (the ~41-
    connection headroom, the 1,000-session refutation) is this function's justification,
    not a separate fact that has to be kept in sync by hand."""
    avail = mem_available_bytes
    max_connections = fixed_budget + headroom_target
    return {
        "shared_buffers": max(128 * 1024 * 1024, int(avail * 0.25)),
        "effective_cache_size": int(avail * 0.65),
        "maintenance_work_mem": min(1024**3, max(64 * 1024 * 1024, int(avail * 0.05))),
        # Worst-case exposure bounded the same way deploy/postgresql.conf reasoned about
        # 100 x 32MB by hand (a 3.2GB ceiling) — generalized to whatever max_connections
        # THIS run derives instead of a number fixed once.
        "work_mem": max(4 * 1024 * 1024, min(64 * 1024 * 1024,
                        int(avail * 0.2 / max(max_connections, 1)))),
        "max_connections": max_connections,
        # NVMe-confirmed for this box (deploy/postgresql.conf, `lsblk -d -o NAME,ROTA`);
        # no live rotational-disk detection worth adding for a single-host fleet.
        "random_page_cost": 1.1,
    }


def _fmt_mem(n_bytes: int) -> str:
    mb = n_bytes / (1024 * 1024)
    if mb >= 1024:
        gb = mb / 1024
        return f"{gb:.0f}GB" if gb == int(gb) else f"{gb:.1f}GB"
    return f"{max(round(mb), 1)}MB"


def _parse_mem(value: str) -> int:
    value = value.strip()
    for unit, mul in sorted(_UNIT_BYTES.items(), key=lambda kv: -len(kv[0])):
        if value.endswith(unit):
            return int(float(value[: -len(unit)]) * mul)
    return int(float(value))


def _fmt_value(name: str, recommended: float | int) -> str:
    if name == "max_connections":
        return str(int(recommended))
    if name == "random_page_cost":
        return f"{recommended:.1f}"
    return _fmt_mem(int(recommended))


def _significant_change(name: str, current: str, recommended: float | int) -> bool:
    """A >15% swing for memory GUCs, an exact mismatch for max_connections, a >0.05
    swing for random_page_cost — avoids perpetual no-op churn from rounding at a byte
    boundary (`_fmt_mem`'s own rounding) reading as a "change" every single run."""
    if name == "max_connections":
        return int(current) != int(recommended)
    if name == "random_page_cost":
        return abs(float(current) - float(recommended)) > 0.05
    current_bytes = _parse_mem(current)
    if current_bytes == 0:
        return recommended != 0
    return abs(recommended - current_bytes) / current_bytes > 0.15


async def plan_tuning(
    pool: asyncpg.Pool, *, fixed_budget: int, headroom_target: int = 40,
) -> dict[str, Any]:
    """Read the live host + live GUCs, compute what's recommended, and return every GUC
    this module owns that's a SIGNIFICANT distance from its recommended value — applying
    nothing itself (that's `apply_tuning`'s job, kept separate so a caller can inspect
    the plan, e.g. in a dry-run report, before anything touches the running server)."""
    host = read_host_resources()
    recommended = compute_recommended(
        mem_available_bytes=host["mem_available_bytes"],
        fixed_budget=fixed_budget, headroom_target=headroom_target)
    current: dict[str, str] = {}
    changes: list[dict[str, Any]] = []
    for name in _ALL_GUCS:
        cur_val = await pool.fetchval(f"SHOW {name}")
        current[name] = cur_val
        want = recommended[name]
        if _significant_change(name, cur_val, want):
            changes.append({
                "name": name, "before": cur_val, "after": _fmt_value(name, want),
                "restart_required": name in _RESTART_REQUIRED,
            })
    return {"host": host, "recommended": recommended, "current": current, "changes": changes}


async def apply_tuning(pool: asyncpg.Pool, plan: dict[str, Any]) -> dict[str, Any]:
    """Applies every reloadable change unconditionally (`ALTER SYSTEM` + a single
    `pg_reload_conf()`, never a stop-the-world — no live backend is interrupted). A
    restart-required change also gets `ALTER SYSTEM` (free, takes effect whenever
    Postgres next restarts, from any cause) but is ALWAYS returned in `deferred` — this
    module computes and persists, it never restarts the one shared instance sixteen live
    agents depend on; that stays the operator's/Thoth's own hand, confessed here loudly
    so it is never a silent gap."""
    applied: list[dict[str, Any]] = []
    deferred: list[dict[str, Any]] = []
    any_reloadable = False
    for change in plan["changes"]:
        name, after = change["name"], change["after"]
        await pool.execute(f"ALTER SYSTEM SET {name} = '{after}'")
        if change["restart_required"]:
            deferred.append(change)
        else:
            any_reloadable = True
            applied.append(change)
    if any_reloadable:
        await pool.execute("SELECT pg_reload_conf()")
    return {"applied": applied, "deferred": deferred}
