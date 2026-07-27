"""src.orchestrator.mcp_client — the CLI's own way of reaching a live osiris-mcp server. The
only thing worth proving without a real running server is the honesty contract: a round-trip
that can't complete returns a plain error STRING, never a raised exception or a silent gap.
"""
from __future__ import annotations

from src.orchestrator.mcp_client import _tool_fingerprint, call_mcp_tool, list_mcp_tools


async def test_unreachable_server_is_an_honest_error_string() -> None:
    # port 1 is a reserved, never-listened-on port — refuses instantly, no real network needed
    result = await call_mcp_tool("http://127.0.0.1:1/mcp", "smoke")
    assert isinstance(result, str)
    assert result.startswith("error: ")


async def test_list_mcp_tools_unreachable_server_is_an_honest_error_string() -> None:
    result = await list_mcp_tools("http://127.0.0.1:1/mcp")
    assert isinstance(result, str)
    assert result.startswith("error: ")


def test_tool_fingerprint_is_stable_for_identical_input() -> None:
    schema = {"type": "object", "properties": {"x": {"type": "string"}}}
    assert _tool_fingerprint("does a thing", schema) == _tool_fingerprint("does a thing", schema)


def test_tool_fingerprint_changes_with_description() -> None:
    schema = {"type": "object"}
    assert (_tool_fingerprint("v1", schema) != _tool_fingerprint("v2", schema))


def test_tool_fingerprint_changes_with_schema() -> None:
    assert (_tool_fingerprint("same", {"a": 1}) != _tool_fingerprint("same", {"a": 2}))
