"""Smoke — the deploy-time liveness check that static gates cannot substitute for (ruling
2ee43411, task #63, threads bb763977 and 1849d800). ruff/mypy/1600 unit tests all passed the
day `_boot_check` warmed the wrong pool (fixed in 1da1bf2) — the bug only broke at a real
server boot, an event-loop-lifecycle class of failure no static check reaches. Run this right
after a deploy restart (`systemctl --user restart osiris-mcp osiris-console && .venv/bin/python
scripts/osiris_smoke.py`), not just once at boot.

Two independent probes, composed honestly (neither silences the other): the SAME logic
`src.orchestrator.smoke` runs walks every chrome route directly (this script's own httpx
client, no MCP in the loop) AND round-trips through osiris-mcp itself by calling its `smoke`
tool over the real MCP protocol — the ONE real DB-backed MCP call thread 1849d800 asked for,
proving the pool the FLEET actually gets (not a throwaway one) survived the restart. If MCP
itself is unreachable, that's reported as its own failure, not a silent gap in the report —
chrome's own status still lands.

    .venv/bin/python scripts/osiris_smoke.py

Silent-ish when green (one line); on any regression prints the failing surfaces AND puts a
brief on the operator's desk through the normal mailbox — the membrane, not a log nobody reads.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

# `from src...` (deferred, below) needs the repo root importable regardless of PYTHONPATH —
# osiris_fleet_glance.py's own precedent, not every script in scripts/ follows it (flagged
# separately: scripts/osiris_preflight.py's deferred `from src...` imports and
# scripts/backfill_thread_arc.py's top-level ones both fail on the exact bare invocation
# their own docstrings document, unless PYTHONPATH=. happens to already be set).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

DSN = "postgresql://osiris:osiris@127.0.0.1:5601/osiris"
MCP_URL = "http://127.0.0.1:8790/mcp"


async def call_mcp_smoke(url: str = MCP_URL) -> dict[str, Any] | str:
    """The real round-trip: connect over streamable-http, call the `smoke` tool. Returns the
    tool's own {chrome, db, ok} dict, or a plain error string if MCP itself never answered —
    that string IS the finding (osiris-mcp down), not a crash."""
    try:
        from mcp import ClientSession
        from mcp.client.streamable_http import streamablehttp_client

        async with streamablehttp_client(url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool("smoke", {})
        if result.isError:
            return f"error: {result.content}"
        data = result.structuredContent
        return data if isinstance(data, dict) else f"error: unexpected shape {data!r}"
    except Exception as e:  # noqa: BLE001 - report, never crash the walk
        return f"error: {e}"


async def local_chrome_walk() -> dict[str, str]:
    """Chrome walked DIRECTLY (no MCP in the loop) — so an MCP-only outage still leaves an
    honest chrome verdict, not a blank."""
    import httpx
    from src.config.settings import get_settings
    from src.orchestrator.smoke import smoke_chrome

    async with httpx.AsyncClient(
        base_url=get_settings().osiris_console_base_url, timeout=5.0,
    ) as client:
        return await smoke_chrome(client)


def _fails_from(chrome: dict[str, str], mcp_result: dict[str, Any] | str) -> list[str]:
    fails = [f"chrome {route}: {status}" for route, status in chrome.items() if status != "ok"]
    if isinstance(mcp_result, str):
        fails.append(f"osiris-mcp round-trip: {mcp_result}")
    else:
        if mcp_result.get("db") != "ok":
            fails.append(f"osiris-mcp pool: {mcp_result.get('db')}")
        fails += [f"osiris-mcp's own chrome view {r}: {s}"
                  for r, s in (mcp_result.get("chrome") or {}).items() if s != "ok"]
    return fails


async def brief_operator(fails: list[str]) -> None:
    """Regression → a brief on the desk through the normal mailbox (dedup makes re-runs safe),
    same pattern as scripts/osiris_preflight.py's own brief_operator."""
    import asyncpg
    from src.orchestrator.mailbox import send_message

    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=1)
    try:
        body = ("SMOKE REGRESSION — a surface failed a live post-deploy check:\n- "
                + "\n- ".join(fails)
                + "\nRun scripts/osiris_smoke.py after fixing; silence = green.")
        await send_message(pool, from_agent="system:smoke", from_project="osiris",
                           to_project="operator", body=body)
    finally:
        await pool.close()


async def _run_both() -> tuple[dict[str, str], dict[str, Any] | str]:
    chrome, mcp_result = await asyncio.gather(local_chrome_walk(), call_mcp_smoke())
    return chrome, mcp_result


def main() -> int:
    chrome, mcp_result = asyncio.run(_run_both())
    fails = _fails_from(chrome, mcp_result)
    if not fails:
        print("smoke: all green (8 chrome routes + the live mcp pool)")
        return 0
    print("SMOKE FAILURES:")
    for f in fails:
        print(" -", f)
    try:
        asyncio.run(brief_operator(fails))
        print("(brief placed on the operator's desk)")
    except Exception as e:  # noqa: BLE001 - the desk being down is itself printed
        print(f"(could not brief the desk: {e})")
    return 1


if __name__ == "__main__":
    sys.exit(main())
