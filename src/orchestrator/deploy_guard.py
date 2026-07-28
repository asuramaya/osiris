"""deploy_guard — the code-ahead-of-schema alarm (thread e6f5556f), plus the reboot-is-a-
deploy confession (thread 489a39d0). Both are BOOT-TIME checks, not periodic ones:
`osiris-preflight.timer` (the existing weekly audit) is the wrong home for either — no
systemd ordering ties it to a service's own boot, so bolting on there would leave the
identical class of gap open for up to 7 days (schema drift) or indefinitely (an unreviewed
reboot). This lives in each service's OWN startup.

LOUD ALARM, NEVER REFUSE-TO-SERVE (Thoth's ruling, DM 1339, after the operator drew the
identical lesson on the mount-guard the same day): `osiris-mcp` is a fleet-wide single point
of failure — one process, one shared pool, the whole fleet's only door in. A FALSE POSITIVE
from a buggy check refusing to serve would self-inflict a total outage on every boot, forever,
strictly worse than the silent drift this guard exists to catch. A loud alarm carries no such
asymmetry: a false positive costs one spurious alarm, a true positive achieves exactly the
goal (unmissable, never silent-until-someone-looks) without new blast radius. So: any error
IN the check itself degrades to UNKNOWN, never to a refusal — the same fail-open discipline
as every other net in this codebase (mark_swept, sweep_route, the miner's own health read).

THE REBOOT LEG (thread 489a39d0): on 2026-07-28 09:17 CDT the machine slept and woke, systemd
brought osiris-mcp/worker/console up on whatever HEAD happened to be checked out — three
commits HELD for executive review went live with no review, no gates, no smoke, no receipt.
`osiris deploy`'s own discipline (dirty-tree guard, migration gate, tool-delta narration)
only runs inside `osiris deploy`; a raw service restart or a reboot bypasses all of it. TWO
candidate fixes were named: pin services to a deployed ref (a checkout/worktree `osiris
deploy` advances), or a boot-time guard that confesses the gap. THIS LEG IS THE CONFESSION
ONLY — cheap, ships first, never blocks. The ref-pin is a deliberate, un-closed SEAM: nothing
here prevents an unreviewed boot from serving, it only makes sure nobody can miss that it
happened."""
from __future__ import annotations

import contextlib
import logging
import subprocess
from pathlib import Path

import asyncpg

_log = logging.getLogger("osiris.deploy_guard")

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# The watermark key `osiris deploy` writes on a successful restart (src/cli.py's own
# `_real_record_deploy`) and this module's reboot guard reads back — the generic cursor
# store (`watermarks`, get_cursor/set_cursor) pulse.py's own `devhead:<repo>` already uses,
# not a new table. Deliberately a DIFFERENT namespace than pulse's `devhead:` key: that one
# tracks the last HEAD the developer-persona heartbeat happened to observe (a read-only
# sensor), a wholly different fact than "the last HEAD that went through the deploy ritual".
_DEPLOY_CURSOR_KEY = "deployed:osiris"


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
    easy to tune out.

    THE THREAD SUMMARY DELIBERATELY OMITS `service` (thread 35c425f9, the boot-listener
    double-record bug): open_thread's idempotency is a hash of the summary TEXT
    (`_canon("thread", summary)`), so baking `{service}` into it made osiris-mcp and
    osiris-worker mint two separate Thread objects for the identical drift condition, one
    per listener, instead of converging on one. `service` still survives per-observation —
    the log line, the operator DM body, and `source=f"boot:{service}"` (a per-assertion
    witness, not part of the canonical identity) all still name it."""
    from src.actions.core import Actions
    from src.orchestrator.capture import open_thread
    from src.orchestrator.mailbox import send_message

    _log.critical("%s booted against a drifted schema: %s", service, drift)
    actions = Actions(pool)
    await open_thread(
        actions,
        f"SCHEMA DRIFT: {drift}. Code is running ahead of (or behind) the "
        "database's own migrations — run `alembic upgrade head` against the real DB before "
        "trusting any feature the missing migration(s) touch.",
        kind="obligation", arc="Fleet-Hygiene", severity="alarm", source=f"boot:{service}",
    )
    with contextlib.suppress(Exception):  # the desk being unreachable must not compound the alarm
        await send_message(
            pool, from_agent=f"system:{service}", from_project="osiris", to_project="operator",
            body=f"{service} booted with a drifted schema — {drift}. Run `alembic upgrade "
                 "head` against the real DB.",
            dedup_window_secs=86400,
        )


def _git_head(repo_root: Path) -> str | None:
    """The running code's own on-disk HEAD — same shape as pulse.py's own `_git_head`
    (that one takes a str path for a developer-persona sensor over arbitrary dev repos; this
    one is deploy_guard's own copy, scoped to a Path, so this module has no import-time
    dependency on `src.orchestrator.pulse`). None on any git failure, never raised —
    deploy_guard's fail-open law applies here too."""
    try:
        return subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def unreviewed_boot(running_head: str | None, last_deployed: str | None) -> str | None:
    """Pure comparison, no IO — same null-handling discipline as `schema_drift`: either side
    unknown means 'don't know', never a mismatch. A box that has never once run `osiris
    deploy` (no cursor yet) is not evidence of an unreviewed reboot — it's evidence this
    guard is new, or the box is fresh."""
    if not running_head or not last_deployed:
        return None
    if running_head == last_deployed:
        return None
    return (f"running HEAD {running_head!r} was never recorded by `osiris deploy` (last "
           f"recorded deploy: {last_deployed!r})")


async def check_unreviewed_boot(pool: asyncpg.Pool) -> str | None:
    """The IO half — same fail-open discipline as `check_schema_drift`: any failure (git
    missing, not a checkout, the watermark unreadable) degrades to None, never a refusal."""
    try:
        from src.orchestrator.monitor import get_cursor

        running = _git_head(_REPO_ROOT)
        last_deployed = await get_cursor(pool, _DEPLOY_CURSOR_KEY)
        return unreviewed_boot(running, last_deployed)
    except Exception as exc:  # noqa: BLE001 — a check that can't complete is UNKNOWN, not a refusal
        _log.warning("unreviewed_boot check failed, treating as unknown: %r", exc)
        return None


async def alarm_unreviewed_boot(pool: asyncpg.Pool, drift: str, *, service: str) -> None:
    """LOUD, never a refusal, never blocking — the reboot-is-a-deploy confession (thread
    489a39d0). Same shape as `alarm_schema_drift`, including the same lesson already applied
    there: `service` stays OUT of the Thread summary (the canonical-identity text) so two
    services confessing the same unreviewed HEAD converge on one Thread, not two — it still
    survives per-observation via the log line, the operator DM, and `source`."""
    from src.actions.core import Actions
    from src.orchestrator.capture import open_thread
    from src.orchestrator.mailbox import send_message

    _log.critical("%s booted on an unreviewed ref: %s", service, drift)
    actions = Actions(pool)
    await open_thread(
        actions,
        f"UNREVIEWED BOOT: {drift}. A service came up on code that never went through "
        "`osiris deploy` — most likely a raw service restart or a machine reboot picking up "
        "the working tree as-is, bypassing the dirty-tree guard and migration gate. Nothing "
        "was blocked; review what's actually running before trusting it, then run `osiris "
        "deploy` so the ledger and reality agree again.",
        kind="obligation", arc="Fleet-Hygiene", severity="alarm", source=f"boot:{service}",
    )
    with contextlib.suppress(Exception):  # the desk being unreachable must not compound the alarm
        await send_message(
            pool, from_agent=f"system:{service}", from_project="osiris", to_project="operator",
            body=f"{service} booted on an unreviewed ref — {drift}. Review before trusting "
                 "it, then `osiris deploy` to re-sync the ledger.",
            dedup_window_secs=86400,
        )
