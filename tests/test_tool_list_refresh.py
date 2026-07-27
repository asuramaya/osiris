"""TOOL-LIST REFRESH (thread 6a78e64b leg 1, operator-directed): osiris-mcp declares the
MCP protocol's own `tools.listChanged` capability and nudges each newly-connected client
session with `notifications/tools/list_changed`, once, on its first tool call — so a
long-lived agent session that RESUMES its connection across an osiris-mcp restart learns
new tools exist instead of staying silently stale until something else forces a refresh.
"""
from __future__ import annotations

from typing import Any

import src.mcp_server as srv
from mcp.server.lowlevel.server import NotificationOptions


def test_the_tools_capability_is_declared_by_default() -> None:
    """FastMCP's own create_initialization_options() call sites (stdio/sse/streamable-http,
    all inside the SDK) never pass a NotificationOptions, so tools_changed silently
    defaulted to False — the capability was never advertised even though the SDK fully
    supports sending the notification. The wrapped method must default it to True."""
    opts = srv.mcp._mcp_server.create_initialization_options()
    assert opts.capabilities.tools is not None
    assert opts.capabilities.tools.listChanged is True


def test_an_explicit_notification_options_is_never_overridden() -> None:
    """The wrapper only supplies a DEFAULT — a caller that passes its own
    NotificationOptions (none do today, but the SDK contract must still hold) keeps
    exactly what it asked for, tools_changed included."""
    opts = srv.mcp._mcp_server.create_initialization_options(
        notification_options=NotificationOptions(prompts_changed=True))
    assert opts.capabilities.tools.listChanged is False
    assert opts.capabilities.prompts.listChanged is True


class _FakeSession:
    def __init__(self) -> None:
        self.list_changed_calls = 0

    async def send_tool_list_changed(self) -> None:
        self.list_changed_calls += 1


class _FakeRequestContext:
    def __init__(self, session: _FakeSession) -> None:
        self.request: Any = None
        self.session = session


class _FakeCtx:
    def __init__(self) -> None:
        self.session = _FakeSession()
        self.request_context = _FakeRequestContext(self.session)


async def test_nudge_sends_the_notification_once_per_connection() -> None:
    """The dedup key is `_conn_key` — the SAME per-client-session key the identity cache
    uses — so one connection's many tool calls cost exactly one notification, not one per
    call."""
    srv._notified_list_changed.clear()
    ctx = _FakeCtx()
    try:
        await srv._nudge_tool_list_refresh(ctx)
        await srv._nudge_tool_list_refresh(ctx)
        await srv._nudge_tool_list_refresh(ctx)
        assert ctx.session.list_changed_calls == 1
    finally:
        srv._notified_list_changed.discard(srv._conn_key(ctx))


async def test_nudge_is_per_connection_not_global() -> None:
    """A SECOND, distinct connection still gets its own nudge — the dedup must not starve
    every connection after the first one ever seen."""
    srv._notified_list_changed.clear()
    ctx_a, ctx_b = _FakeCtx(), _FakeCtx()
    try:
        await srv._nudge_tool_list_refresh(ctx_a)
        await srv._nudge_tool_list_refresh(ctx_b)
        assert ctx_a.session.list_changed_calls == 1
        assert ctx_b.session.list_changed_calls == 1
    finally:
        srv._notified_list_changed.discard(srv._conn_key(ctx_a))
        srv._notified_list_changed.discard(srv._conn_key(ctx_b))


async def test_nudge_is_ambient_never_load_bearing() -> None:
    """A session whose `send_tool_list_changed` itself raises must not propagate the
    error — this rides every single tool call, and a transport hiccup here must never cost
    the caller its actual tool result."""

    class _BoomSession:
        async def send_tool_list_changed(self) -> None:
            raise RuntimeError("transport hiccup")

    class _BoomRequestContext:
        def __init__(self, session: Any) -> None:
            self.request: Any = None
            self.session = session

    class _BoomCtx:
        def __init__(self) -> None:
            self.session = _BoomSession()
            self.request_context = _BoomRequestContext(self.session)

    srv._notified_list_changed.clear()
    ctx = _BoomCtx()
    try:
        await srv._nudge_tool_list_refresh(ctx)  # must not raise
    finally:
        srv._notified_list_changed.discard(srv._conn_key(ctx))


async def test_nudge_no_op_on_an_unmounted_or_ctxless_call() -> None:
    """No ctx (a direct call outside the MCP protocol layer, as every other test in this
    suite makes) or no resolvable connection key must be a clean no-op, never an error."""
    await srv._nudge_tool_list_refresh(None)
