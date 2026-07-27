"""A tiny MCP client helper (task #69) — the CLI's own way of reaching the ALREADY-RUNNING
osiris-mcp server (deploy/osiris-mcp.service) over streamable-http, the same wire protocol
scripts/osiris_smoke.py proved out first (task #63, src.orchestrator.smoke.call_mcp_smoke,
now a thin wrapper over this). Never a second implementation of what a tool computes: `osiris
fleet`/`osiris smoke` call the REAL deployed tool over the wire, so they see exactly what a
live Claude session sees, nothing re-derived and nothing to drift out of sync with it."""
from __future__ import annotations

from typing import Any


async def call_mcp_tool(
    url: str, name: str, arguments: dict[str, Any] | None = None,
) -> dict[str, Any] | str:
    """One tool call round-tripped over streamable-http. Returns the tool's own structured
    result, or a plain error STRING if the round-trip itself failed (server down, refused,
    timed out) — that string IS the finding, never a silent gap the caller has to detect."""
    try:
        from mcp import ClientSession
        from mcp.client.streamable_http import streamablehttp_client

        async with streamablehttp_client(url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(name, arguments or {})
        if result.isError:
            return f"error: {result.content}"
        data = result.structuredContent
        return data if isinstance(data, dict) else f"error: unexpected shape {data!r}"
    except Exception as e:  # noqa: BLE001 - report, never crash the caller
        return f"error: {e}"
