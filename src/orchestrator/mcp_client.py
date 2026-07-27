"""A tiny MCP client helper (task #69) — the CLI's own way of reaching the ALREADY-RUNNING
osiris-mcp server (deploy/osiris-mcp.service) over streamable-http, the same wire protocol
scripts/osiris_smoke.py proved out first (task #63, src.orchestrator.smoke.call_mcp_smoke,
now a thin wrapper over this). Never a second implementation of what a tool computes: `osiris
fleet`/`osiris smoke` call the REAL deployed tool over the wire, so they see exactly what a
live Claude session sees, nothing re-derived and nothing to drift out of sync with it."""
from __future__ import annotations

import hashlib
import json
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


def _tool_fingerprint(description: str | None, input_schema: Any) -> str:
    """A short, stable hash of a tool's own contract — its description + inputSchema. Two
    round-trips of the SAME tool land on the same fingerprint; a genuinely changed signature
    or docstring changes it, which is the whole point (task #69's `osiris deploy` tool-list
    diff, thread 6a78e64b leg 2 — naming '~smoke changed', not just '+'/'-' by name)."""
    blob = json.dumps({"description": description, "inputSchema": input_schema},
                      sort_keys=True, default=str)
    return hashlib.sha256(blob.encode()).hexdigest()[:12]


async def list_mcp_tools(url: str) -> dict[str, str] | str:
    """name -> fingerprint for every tool the server currently advertises, over the SAME
    streamable-http round-trip `call_mcp_tool` uses. A plain error STRING if the round-trip
    itself failed — never a silent empty dict a caller might mistake for 'no tools'."""
    try:
        from mcp import ClientSession
        from mcp.client.streamable_http import streamablehttp_client

        async with streamablehttp_client(url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.list_tools()
        return {t.name: _tool_fingerprint(t.description, t.inputSchema) for t in result.tools}
    except Exception as e:  # noqa: BLE001 - report, never crash the caller
        return f"error: {e}"
