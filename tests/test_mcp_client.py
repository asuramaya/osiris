"""src.orchestrator.mcp_client — the CLI's own way of reaching a live osiris-mcp server. The
only thing worth proving without a real running server is the honesty contract: a round-trip
that can't complete returns a plain error STRING, never a raised exception or a silent gap.
"""
from __future__ import annotations

from src.orchestrator.mcp_client import call_mcp_tool


async def test_unreachable_server_is_an_honest_error_string() -> None:
    # port 1 is a reserved, never-listened-on port — refuses instantly, no real network needed
    result = await call_mcp_tool("http://127.0.0.1:1/mcp", "smoke")
    assert isinstance(result, str)
    assert result.startswith("error: ")
