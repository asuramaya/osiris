"""The Claude harness daemon's control socket — the VISIBLE push lane (thread 4261a0d8).

The operator's front renders the DAEMON'S job stream, not transcript files: a turn a
foreign `claude -p --resume` appends is real in the record and invisible in the chrome
(the ghost problem, named and confirmed solved by the operator 2026-07-20). The daemon
holds every backgrounded session as a JOB and its `reply` op injects text as that job's
next turn, DAEMON-OWNED — the tile updates, the conversation shows the hop, the addressee
answers in its own window. Probe evidence and the recovered op vocabulary live in decision
5722a21a / thread 4261a0d8.

THESE ARE UNDOCUMENTED HARNESS INTERNALS, version-fragile BY NATURE. Everything here
fails OPEN (None/False, never raises) so the resume lane stays the permanent fallback:
an EPROTO after a harness update, a missing key, a dead socket — the dispatch simply
falls through to `claude -p --resume` exactly as before this module existed.

Protocol: newline-delimited JSON over the unix socket at
/tmp/cc-daemon-<uid>/<daemon-id>/control.sock; every request carries {proto: 1} and
privileged ops carry auth = the contents of ~/.claude/daemon/control.key (0600,
operator-owned; the worker runs as the same user).
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

_PROTO = 1
_TIMEOUT = 3.0


def _sock_paths() -> list[Path]:
    base = Path(tempfile.gettempdir()) / f"cc-daemon-{os.getuid()}"
    try:
        return sorted(base.glob("*/control.sock")) if base.exists() else []
    except OSError:
        return []


def _control_key() -> str | None:
    try:
        key = (Path.home() / ".claude" / "daemon" / "control.key").read_text().strip()
        return key or None
    except OSError:
        return None


async def _call(sock: Path, payload: dict[str, Any], wait_secs: float = _TIMEOUT
                ) -> dict[str, Any] | None:
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_unix_connection(str(sock)), wait_secs)
    except (OSError, TimeoutError):
        return None
    try:
        writer.write(json.dumps(payload).encode() + b"\n")
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), wait_secs)
        out = json.loads(line) if line else None
        return out if isinstance(out, dict) else None
    except (OSError, TimeoutError, ValueError):
        return None
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


async def list_jobs() -> list[dict[str, Any]]:
    """Every job every reachable daemon holds — the agents-view tiles, as data. Each row
    carries its origin socket under `_sock` so a later `reply` reaches the same daemon."""
    out: list[dict[str, Any]] = []
    for sock in _sock_paths():
        r = await _call(sock, {"proto": _PROTO, "op": "list"})
        if r and r.get("ok") and isinstance(r.get("jobs"), list):
            out.extend({**j, "_sock": str(sock)} for j in r["jobs"] if isinstance(j, dict))
    return out


async def job_for(ids: set[str]) -> dict[str, Any] | None:
    """The daemon job holding one of these identities — matched on the job SHORT (the
    CLAUDE_JOB_DIR basename) or the sessionId (full or 8-char prefix), because a session
    resumed across jobs wears a short that differs from its session id."""
    if not ids:
        return None
    for j in await list_jobs():
        sid = str(j.get("sessionId") or "")
        if (str(j.get("short") or "") in ids or sid in ids or (sid and sid[:8] in ids)):
            return j
    return None


async def reply(job: dict[str, Any], text: str) -> bool:
    """Inject `text` as the job's next turn — daemon-owned, chrome-visible. True only on
    the daemon's own {ok:true}; every failure is False and the caller falls through.

    SANCTIONED, NOT ILLEGAL (ruling 85fba696, operator 2026-07-29, superseding 482c3d0f).
    This rides the Claude-harness daemon's op='reply' control-socket channel. The house read
    it as an RCE and disclosed it; Anthropic reviewed and deemed the behavior INTENDED DESIGN,
    so the old prohibition ("do NOT build new machinery on it") is WITHDRAWN — build freely.
    TWO THINGS THAT DID NOT CHANGE, and a reader who skips them will misuse this: (1) an
    injected turn is stamped origin.kind='human' by the harness no matter who wrote it, so any
    caller must carry its OWN provenance marker rather than let that label stand (wake() does
    exactly this) — attribution honesty is ours to keep, not the harness's to give; (2) this
    is an UNDOCUMENTED INTERNAL of someone else's product, sanctioned to use but free to break
    without notice, so the injectable `nudge` parameter in trigger.trigger_mail_tick stays as
    the swap seam — operational insurance now rather than legal cover. (`job_for` above is a
    READ of daemon job state — the authoritative liveness Ra's reachability read consults.)"""
    key = _control_key()
    raw = str(job.get("_sock") or "")
    short = str(job.get("short") or "")
    if not key or not short or not raw:
        return False
    # no exists() pre-check: a vanished socket just fails the connect below, same fail-open
    sock = Path(raw)
    r = await _call(sock, {"proto": _PROTO, "op": "reply", "short": short,
                           "text": text, "auth": key}, wait_secs=8.0)
    return bool(r and r.get("ok"))
