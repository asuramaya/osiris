"""smoke — the deploy-time liveness check (ruling 2ee43411, task #63, threads bb763977 and
1849d800). Static gates (ruff/mypy/pytest) proved this session they cannot catch an
event-loop-lifecycle bug: `_boot_check` warming the wrong pool (fixed in 1da1bf2) shipped
past all of them and only broke at real server boot. This module is the honest, per-surface
answer to "is it actually up" — one real query over the pool a caller actually gets (the
exact class of bug 1da1bf2 fixed), and one GET per chrome route, never silently skipped.

Kept deliberately dumb: no retries, no thresholds, no alarms of its own — a probe reports
what it saw, the caller (the MCP tool wrapper, or the CLI) decides what to do about it."""
from __future__ import annotations

from typing import Any

import asyncpg
import httpx

# every chrome route named in thread bb763977 — /health isn't here on purpose: it's already
# its own dedicated liveness endpoint (src/api/app.py), this walks the actual RENDERED pages.
# /membrane retired (task #71, ruling 0b3dd431) — "/" (THE INBOX) is :8011's new front door.
CHROME_ROUTES: tuple[str, ...] = (
    "/", "/desk", "/live-desk", "/mail", "/fleet", "/roadmap", "/canon", "/overhead",
)


async def smoke_chrome(
    client: httpx.AsyncClient, routes: tuple[str, ...] = CHROME_ROUTES,
) -> dict[str, str]:
    """One GET per route — "ok" or exactly what went wrong, never silently dropped. `client`
    is caller-supplied so a real deploy points it at a live base_url and a test points it at
    an ASGI transport — the walk logic doesn't know or care which."""
    out: dict[str, str] = {}
    for route in routes:
        try:
            r = await client.get(route)
            out[route] = "ok" if r.status_code < 400 else f"http {r.status_code}"
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


async def smoke(client: httpx.AsyncClient, pool: asyncpg.Pool) -> dict[str, Any]:
    """The whole picture: every chrome route + the pool, composed. `ok` is a single boolean a
    deploy script can branch on without re-deriving the per-surface detail."""
    chrome = await smoke_chrome(client)
    db = await smoke_pool(pool)
    ok = db == "ok" and all(v == "ok" for v in chrome.values())
    return {"chrome": chrome, "db": db, "ok": ok}


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
    return fails
