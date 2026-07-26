"""deploy_guard — the code-ahead-of-schema alarm (thread e6f5556f). A BOOT-TIME check, not a
periodic one: on 2026-07-26 a service restart ran ahead of `alembic upgrade`, stranding the
schema two migrations behind the running code, and the gap was invisible until a resume
happened to look. `osiris-preflight.timer` (the existing weekly audit) is the wrong home for
this — no systemd ordering ties it to a service's own boot, so bolting on there would leave
the identical class of gap open for up to 7 days. This lives in each service's OWN startup.

LOUD ALARM, NEVER REFUSE-TO-SERVE (Thoth's ruling, DM 1339, after the operator drew the
identical lesson on the mount-guard the same day): `osiris-mcp` is a fleet-wide single point
of failure — one process, one shared pool, the whole fleet's only door in. A FALSE POSITIVE
from a buggy check refusing to serve would self-inflict a total outage on every boot, forever,
strictly worse than the silent drift this guard exists to catch. A loud alarm carries no such
asymmetry: a false positive costs one spurious alarm, a true positive achieves exactly the
goal (unmissable, never silent-until-someone-looks) without new blast radius. So: any error
IN the check itself degrades to UNKNOWN, never to a refusal — the same fail-open discipline
as every other net in this codebase (mark_swept, sweep_route, the miner's own health read)."""
from __future__ import annotations

import contextlib
import logging
from pathlib import Path

import asyncpg

_log = logging.getLogger("osiris.deploy_guard")

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent


def schema_drift(db_version: str | None, code_head: str | None) -> str | None:
    """Pure comparison, no IO — isolated-testable. Either side unknown (an empty
    alembic_version table, a script directory that failed to load) means "don't know", never
    drift: only a genuine, confident mismatch between two known values is reported."""
    if not db_version or not code_head:
        return None
    if db_version == code_head:
        return None
    return f"code expects migration head {code_head!r}, DB is at {db_version!r}"


async def check_schema_drift(pool: asyncpg.Pool) -> str | None:
    """The IO half. ANY failure here — DB unreachable, alembic_version missing, the script
    directory failing to load — degrades to None ("unknown"), never to a refusal."""
    try:
        from alembic.config import Config
        from alembic.script import ScriptDirectory

        cfg = Config(str(_REPO_ROOT / "alembic.ini"))
        code_head = ScriptDirectory.from_config(cfg).get_current_head()
        db_version = await pool.fetchval("SELECT version_num FROM alembic_version")
        return schema_drift(db_version, code_head)
    except Exception as exc:  # noqa: BLE001 — a check that can't complete is UNKNOWN, not a refusal
        _log.warning("schema_drift check failed, treating as unknown: %r", exc)
        return None


async def alarm_schema_drift(pool: asyncpg.Pool, drift: str, *, service: str) -> None:
    """LOUD, never a refusal, and never something that can itself block a boot (callers wrap
    this in their own broad guard too — belt and suspenders on the one rule this whole module
    exists to keep). A durable Thread, idempotent on the drift's own text so a persistent gap
    across many restarts never mints a duplicate, plus a CRITICAL log line, plus an
    operator-desk brief with a generous dedup window (24h, not send_message's own 600s
    default) — a schema drift can easily outlive ten minutes across restarts, and re-briefing
    the desk every single boot would be exactly the kind of noise that makes a real alarm
    easy to tune out."""
    from src.actions.core import Actions
    from src.orchestrator.capture import open_thread
    from src.orchestrator.mailbox import send_message

    _log.critical("%s booted against a drifted schema: %s", service, drift)
    actions = Actions(pool)
    await open_thread(
        actions,
        f"SCHEMA DRIFT at {service} boot: {drift}. Code is running ahead of (or behind) the "
        "database's own migrations — run `alembic upgrade head` against the real DB before "
        "trusting any feature the missing migration(s) touch.",
        kind="obligation", arc="Fleet-Hygiene", source=f"boot:{service}",
    )
    with contextlib.suppress(Exception):  # the desk being unreachable must not compound the alarm
        await send_message(
            pool, from_agent=f"system:{service}", from_project="osiris", to_project="operator",
            body=f"{service} booted with a drifted schema — {drift}. Run `alembic upgrade "
                 "head` against the real DB.",
            dedup_window_secs=86400,
        )
