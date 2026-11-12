"""Clean up after a pg_repack run that was killed instead of stopped.

pg_repack rewrites a table by creating a log table and a trigger on the target table, copying
the rows, replaying the log and swapping the files. When it is stopped with SIGINT it removes
all of that itself. When it is killed (SIGKILL, a dead container, a power cut) it cannot: the
trigger stays on the table, so every later write to that table also writes a row into the log
table, which grows without bound, and the half-built copy and its type stay in the `repack`
schema. The night compaction reports this as "pg_repack was killed" and names this step.

`repack_cleanup` finds what is left (a trigger whose function lives in the `repack` schema,
and any table or composite type in that schema that does not belong to the extension itself)
and, when told to apply, drops exactly those things, each in its own short transaction with a
short lock timeout. It refuses while a repack is still running (a backend whose query mentions
the repack objects, or a `pg_repack` process in the database container), because then the
trigger and the log table are doing their job.

DRY RUN IS THE DEFAULT: it lists the leftovers and the statements an apply would run.
`dry_run=False` REQUIRES a non-blank `because` and records what it dropped as a decision.
The fallback, if a leftover cannot be dropped, is the heavier reinstall named in the result:
`DROP EXTENSION pg_repack CASCADE; CREATE EXTENSION pg_repack;`."""
from __future__ import annotations

from typing import Any

import asyncpg

from src.orchestrator.db_compaction import ExecFn, _container, _docker_exec

LOCK_TIMEOUT = "3s"
REINSTALL = "DROP EXTENSION pg_repack CASCADE; CREATE EXTENSION pg_repack;"

# a trigger whose function is pg_repack's own, on any table
_TRIGGERS_SQL = """
SELECT t.tgname AS name, c.oid::regclass::text AS on_table,
       format('DROP TRIGGER %I ON %s', t.tgname, c.oid::regclass::text) AS statement
FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid
JOIN pg_proc p ON p.oid = t.tgfoid
JOIN pg_namespace pn ON pn.oid = p.pronamespace
WHERE NOT t.tgisinternal AND pn.nspname = 'repack' AND p.proname = 'repack_trigger'
ORDER BY 2, 1
"""

# anything in the repack schema that is not a member of the extension: the log tables, the
# half-built copies and the composite types a run creates and normally removes
_RELATIONS_SQL = """
SELECT c.relname AS name, c.relkind::text AS kind, pg_total_relation_size(c.oid) AS bytes,
       CASE c.relkind WHEN 'c' THEN format('DROP TYPE repack.%I', c.relname)
            ELSE format('DROP TABLE repack.%I', c.relname) END AS statement
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'repack' AND c.relkind IN ('r', 'p', 'c')
  AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass
                  AND d.objid = c.oid AND d.deptype = 'e')
ORDER BY 1
"""

# a backend that is working on a repack object right now
_ACTIVE_SQL = """
SELECT count(*) FROM pg_stat_activity
WHERE pid <> pg_backend_pid() AND datname = current_database() AND state <> 'idle'
  AND (query ~ 'repack\\.(log|table|pk|index)_[0-9]+' OR query ~ 'repack_trigger')
"""


async def _repack_process_running(exec_fn: ExecFn | None) -> bool | None:
    """True when a pg_repack process exists in the database container, False when there is
    none, None when the container could not be asked (no docker, no container)."""
    code, _out = await (exec_fn or _docker_exec)(
        ["exec", _container(), "pidof", "pg_repack"], 15)
    if code == 0:
        return True
    return False if code == 1 else None


async def find_leftovers(
    pool: asyncpg.Pool, exec_fn: ExecFn | None = None,
) -> dict[str, Any]:
    """What a killed repack left behind, and whether a repack is running right now."""
    extension = bool(await pool.fetchval(
        "SELECT 1 FROM pg_extension WHERE extname = 'pg_repack'"))
    triggers = [dict(r) for r in await pool.fetch(_TRIGGERS_SQL)]
    relations = [dict(r) for r in await pool.fetch(_RELATIONS_SQL)]
    in_database = int(await pool.fetchval(_ACTIVE_SQL)) > 0
    in_container = await _repack_process_running(exec_fn)
    return {
        "extension_installed": extension, "triggers": triggers, "relations": relations,
        "leftover": bool(triggers or relations),
        "running": in_database or bool(in_container),
        "container_checked": in_container is not None,
        "steps": [r["statement"] for r in triggers] + [r["statement"] for r in relations],
        "fallback": REINSTALL}


async def _drop(pool: asyncpg.Pool, statement: str) -> str | None:
    """One statement in its own transaction with a short lock timeout; None when it worked,
    otherwise why not."""
    try:
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
            await conn.execute(statement)
    except Exception as exc:  # noqa: BLE001 - reported per statement, never hides the rest
        return f"{type(exc).__name__}: {exc}"
    return None


async def repack_cleanup(
    pool: asyncpg.Pool, *, actor: str, dry_run: bool = True, because: str | None = None,
    exec_fn: ExecFn | None = None,
) -> dict[str, Any]:
    """List what a killed pg_repack left behind; with `dry_run=False` (and a `because`) drop
    it. Refuses to apply while a repack is running. Idempotent: a second run finds nothing."""
    if not dry_run and not (because or "").strip():
        return {"error": "cleaning up after pg_repack without a because is an un-audited "
                         "repair: cite who authorized it"}
    found = await find_leftovers(pool, exec_fn)
    summary: dict[str, Any] = {**found, "dry_run": dry_run,
                               "because": because if not dry_run else None}
    if dry_run or not found["leftover"]:
        return summary
    if found["running"]:
        return {**summary, "error": "a pg_repack run is in progress: its trigger and log table "
                                    "are doing their job. Nothing was dropped; run this again "
                                    "when it has finished"}
    dropped: list[str] = []
    failed: list[dict[str, str]] = []
    for statement in found["steps"]:
        reason = await _drop(pool, statement)
        if reason is None:
            dropped.append(statement)
        else:
            failed.append({"statement": statement, "error": reason})
    after = await find_leftovers(pool, exec_fn)
    summary.update({
        "dropped": dropped, "failed": failed, "actor": actor,
        "remaining": after["steps"], "finished": not after["leftover"]})
    if dropped:
        summary["audit"] = await _record_audit(pool, summary, actor, because or "")
    return summary


async def _record_audit(
    pool: asyncpg.Pool, summary: dict[str, Any], actor: str, because: str,
) -> dict[str, Any]:
    """Records what the cleanup dropped as a decision: the statements, the actor and the
    reason, so a bad night leaves a trace beyond the tool result."""
    from src.actions.core import Actions
    from src.orchestrator.capture import record_decision

    text = (f"pg_repack cleanup run by {actor}: dropped {len(summary['dropped'])} leftover "
            f"object(s) ({'; '.join(summary['dropped'])}), {len(summary['failed'])} failed, "
            f"{len(summary['remaining'])} remain. Reason given: {because}")
    try:
        decision = await record_decision(
            Actions(pool), text, kind="decision", repo="osiris", source=actor,
            rationale="A killed pg_repack leaves a trigger on the target table that logs every "
                      "write, and its half-built copy; this removes exactly those objects.")
    except Exception as exc:  # noqa: BLE001 - the objects are gone: report, never hide it
        return {"recorded": False, "error": f"{type(exc).__name__}: {exc}"}
    return {"recorded": True, "decision": str(decision)}
