"""smoke — the deploy-time liveness check (ruling 2ee43411, task #63, threads bb763977 and
1849d800). Static gates (ruff/mypy/pytest) proved this session they cannot catch an
event-loop-lifecycle bug: `_boot_check` warming the wrong pool (fixed in 1da1bf2) shipped
past all of them and only broke at real server boot. This module is the honest, per-surface
answer to "is it actually up" — one real query over the pool a caller actually gets (the
exact class of bug 1da1bf2 fixed), and one GET per chrome route, never silently skipped.

Kept deliberately dumb: no retries, no thresholds, no alarms of its own — a probe reports
what it saw, the caller (the MCP tool wrapper, or the CLI) decides what to do about it."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import httpx

# every chrome route named in thread bb763977 — /health isn't here on purpose: it's already
# its own dedicated liveness endpoint (src/api/app.py), this walks the actual RENDERED pages.
# /membrane retired (task #71, ruling 0b3dd431) — "/" (THE INBOX) is :8011's new front door.
# /live-desk and /roadmap retired (ruling d42c543b) — pure duplicates of the "live-desk" and
# "roadmap" compositions already roomed in /ui, verified live before deletion.
# /canon retired 2026-07-30 (task #96): a pure pass-through to the "docs" composition, whose
# one distinct capability (fixed topic order) became the `sequence` op at 5987df5 and was
# verified live in /ui before deletion.
CHROME_ROUTES: tuple[str, ...] = (
    "/", "/desk", "/mail", "/fleet", "/overhead",
)


async def smoke_chrome(
    client: httpx.AsyncClient, routes: tuple[str, ...] = CHROME_ROUTES,
) -> dict[str, str]:
    """One GET per route — "ok" or exactly what went wrong, never silently dropped. `client`
    is caller-supplied so a real deploy points it at a live base_url and a test points it at
    an ASGI transport — the walk logic doesn't know or care which.

    A timed-out route is named explicitly (Thoth DM 2823, live measured: `/` at 7.13s past a
    5s client timeout) rather than falling into the generic error branch below — httpx's own
    timeout exceptions (ReadTimeout/ConnectTimeout) carry an EMPTY str() when raised with no
    message, which the generic branch would have rendered as the indistinguishable `"error: "`,
    identical in shape to a genuinely dead route. A refused connection keeps its own real
    message (e.g. "Connection refused") and needs no special case."""
    out: dict[str, str] = {}
    for route in routes:
        try:
            r = await client.get(route)
            out[route] = "ok" if r.status_code < 400 else f"http {r.status_code}"
        except httpx.TimeoutException:
            out[route] = f"timeout (no response within {client.timeout.read:.0f}s)"
        except Exception as e:  # noqa: BLE001 - report the surface as down, never crash the walk
            out[route] = f"error: {e}"
    return out


async def smoke_pool(pool: asyncpg.Pool) -> str:
    """One real query over the pool a caller actually gets — proves the event loop this pool
    is bound to is the one actually running. A pool that answers `describe`/`recall`/any
    other tool fine but fails here would be the exact silent-until-boot bug 1da1bf2 fixed."""
    try:
        await pool.fetchval("SELECT 1")
        return "ok"
    except Exception as e:  # noqa: BLE001 - report, never raise past this probe
        return f"error: {e}"


async def whisper_health(pool: asyncpg.Pool, *, window_hours: int = 24) -> dict[str, Any]:
    """READ-ONLY (kept dumb, same law as every other probe here): the whisper/session-end/
    precompact/stophook hooks each file a failure into the EXISTING blind-spot channel
    (task #34, `capture.record_hook_failure`) when they fail — this only reads that back,
    adding no new counter or table (task #179). `error_count` is the number of failure
    assertions across `capture.HOOK_ALARM_SURFACES` within `window_hours`; `last_error`
    names which surface, what it said, and when. `ok` is `error_count == 0` — a hook that
    never failed in the window leaves nothing here to find, same as a route that was never
    hit. This is NOT a positive probe (it cannot prove the whisper is UP, only that it
    hasn't confessed to being down) — `cmd_deploy`'s own synthetic /automount check is the
    closest thing to an active probe."""
    from src.orchestrator.capture import HOOK_ALARM_SURFACES

    since = datetime.now(UTC) - timedelta(hours=window_hours)
    error_count = 0
    last_error: dict[str, Any] | None = None
    try:
        for surface in HOOK_ALARM_SURFACES:
            obj_id = await pool.fetchval(
                "SELECT o.id FROM objects o WHERE o.type='BlindSpot' AND EXISTS "
                "(SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
                " AND a.name='surface' AND a.value #>> '{}' = $1)", surface)
            if obj_id is None:
                continue
            rows = await pool.fetch(
                "SELECT value #>> '{}' AS text, observed_at FROM assertions "
                "WHERE object_id=$1 AND name='cannot_see' AND observed_at > $2 "
                "ORDER BY observed_at DESC", obj_id, since)
            error_count += len(rows)
            if rows and (last_error is None or rows[0]["observed_at"] > last_error["when"]):
                last_error = {"surface": surface, "text": rows[0]["text"],
                             "when": rows[0]["observed_at"]}
    except Exception as e:  # noqa: BLE001 - report, never raise past this probe
        return {"window_hours": window_hours, "ok": False,
               "error": f"whisper_health probe itself failed: {e}"}
    out: dict[str, Any] = {"window_hours": window_hours, "error_count": error_count,
                           "ok": error_count == 0}
    if last_error is not None:
        out["last_error"] = {**last_error, "when": last_error["when"].isoformat()}
    return out


async def registry_rowless_warning(pool: asyncpg.Pool) -> str | None:
    """WARNING, never a failure (Thoth DM 5257, whisper-health follow-through): reads
    `mounts.registry_census`'s own `rowless` population back — verified-live harness
    bodies (real /proc-confirmed pids) with NO `agent_mounts` row at all, exactly the
    population #178's pieces (a)/(b) exist to close to zero. Folding "N listed bodies
    with no row" into smoke's own verdict means a stranger's machine sees it in the
    first minute, same law as `whisper_health` above — a read-back, no new counter.
    Never gates `ok`: a rowless body self-heals at its own next mount (registry_census's
    own docstring), a known transient gap, not a liveness failure. `None` on a clean
    census OR a blind one (the harness read itself failed — silence here, not a second
    alarm stacked on an already-reported gap). Same fail-open law as every other probe
    here: a broken POOL (registry_census's own `pool.fetch` call, not just its harness
    read) must degrade to `None`, never crash the whole `smoke()` composition — proven
    live by `test_smoke_is_not_ok_when_the_pool_fails`, which hands `smoke()` a pool with
    no `.fetch` at all."""
    from src.orchestrator.mounts import registry_census

    try:
        census = await registry_census(pool)
    except Exception:  # noqa: BLE001 - report nothing, never raise past this probe
        return None
    if census.get("blind"):
        return None
    n = len(census.get("rowless") or [])
    return f"{n} listed bodies with no row" if n else None


async def smoke(client: httpx.AsyncClient, pool: asyncpg.Pool) -> dict[str, Any]:
    """The whole picture: every chrome route + the pool + the whisper/hook alarm channel
    + the registry rowless count, composed. `ok` is a single boolean a deploy script can
    branch on without re-deriving the per-surface detail — `warnings` (a list, empty when
    clean) never feeds it: a warning names something worth a glance, never a reason to
    fail a deploy."""
    chrome = await smoke_chrome(client)
    db = await smoke_pool(pool)
    whisper = await whisper_health(pool)
    rowless_warning = await registry_rowless_warning(pool)
    ok = db == "ok" and all(v == "ok" for v in chrome.values()) and whisper["ok"]
    return {"chrome": chrome, "db": db, "whisper": whisper,
            "warnings": [rowless_warning] if rowless_warning else [], "ok": ok}


async def call_mcp_smoke(url: str) -> dict[str, Any] | str:
    """The CLIENT-side half (task #69's `osiris smoke`, and scripts/osiris_smoke.py before it):
    round-trip the real MCP protocol to call THIS module's own `smoke` tool as the fleet
    actually calls it — proving the pool a live agent gets, not a throwaway one. Returns the
    tool's own {chrome, db, ok} dict, or a plain error STRING if the round-trip itself failed
    (server down, refused, timed out) — that string IS the finding, never a silent gap."""
    from src.orchestrator.mcp_client import call_mcp_tool

    return await call_mcp_tool(url, "smoke")


def summarize_failures(chrome: dict[str, str], mcp_result: dict[str, Any] | str) -> list[str]:
    """The two probes (a direct chrome walk, the MCP round-trip's OWN view of both chrome and
    its pool) named as one flat list of failures — every surface that isn't 'ok' shows up by
    name, never collapsed into a single pass/fail bit."""
    fails = [f"chrome {route}: {status}" for route, status in chrome.items() if status != "ok"]
    if isinstance(mcp_result, str):
        fails.append(f"osiris-mcp round-trip: {mcp_result}")
    else:
        if mcp_result.get("db") != "ok":
            fails.append(f"osiris-mcp pool: {mcp_result.get('db')}")
        fails += [f"osiris-mcp's own chrome view {r}: {s}"
                  for r, s in (mcp_result.get("chrome") or {}).items() if s != "ok"]
        whisper = mcp_result.get("whisper")
        if isinstance(whisper, dict) and not whisper.get("ok"):
            last = whisper.get("last_error")
            detail = f" — last: {last['surface']}: {last['text']}" if last else ""
            fails.append(f"whisper/hook alarms: {whisper.get('error_count', '?')} in "
                        f"{whisper.get('window_hours', '?')}h{detail}")
    return fails


def summarize_warnings(mcp_result: dict[str, Any] | str) -> list[str]:
    """Non-blocking findings from osiris-mcp's own `smoke()` verdict — the `warnings` list
    (today: `registry_rowless_warning`) — kept OUT of `summarize_failures`'s list on
    purpose: a warning is worth a glance, never a deploy-gate reason. A bare error STRING
    (the round-trip itself failed) has nothing further to warn about beyond that already-
    fatal fact."""
    if isinstance(mcp_result, str):
        return []
    return list(mcp_result.get("warnings") or [])
