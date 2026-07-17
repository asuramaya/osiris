"""THE MANAGER'S CONTROL-OP CLIENT — one JSON line out, one JSON line back.

Deliberately stdlib-only: the trigger (worker process) and any CLI import THIS to reach the
daemon, without dragging the daemon's own world (asyncpg, redis, the broker) into theirs.
`attach.py` stays the interactive client for the framed PTY stream; this is the boring half —
`status`, `bodies`, `pty_list`, `pty_poke`, everything line-oriented.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
from pathlib import Path
from typing import Any


def default_socket_path() -> Path:
    """$XDG_RUNTIME_DIR/osiris-manager.sock, falling back to ~/.osiris/manager.sock — a unix
    domain socket only (ruling d6403d34: NOTHING listens on TCP). The one authority for the
    path; the daemon imports it from here."""
    runtime_dir = os.environ.get("XDG_RUNTIME_DIR")
    if runtime_dir:
        return Path(runtime_dir) / "osiris-manager.sock"
    return Path.home() / ".osiris" / "manager.sock"


async def manager_call(
    req: dict[str, Any], *, socket_path: str | Path | None = None,
    reply_timeout: float = 3.0,
) -> dict[str, Any]:
    """Send one control op, return its one-line JSON answer. Raises OSError when the daemon
    is down/absent (no socket) and TimeoutError when it hangs — CALLERS that must survive a
    dark manager (the trigger's poke lane) catch and fail open; a CLI lets it surface."""
    path = str(socket_path if socket_path is not None else default_socket_path())
    reader, writer = await asyncio.open_unix_connection(path)
    try:
        writer.write((json.dumps(req) + "\n").encode())
        await writer.drain()
        async with asyncio.timeout(reply_timeout):
            line = await reader.readline()
        if not line:
            return {"error": "the manager closed the connection without answering"}
        out: dict[str, Any] = json.loads(line)
        return out
    finally:
        writer.close()
        with contextlib.suppress(OSError):
            await writer.wait_closed()
