"""TIME MACHINE, stage 4: what the console's /time-machine/* routes call.

One function per route, each returning a plain dict; an `error` key holds a sentence the
console shows as is. Operator-only by construction (the routes answer on loopback, and the
restore acts as the operator): there is deliberately no agent tool for any of this, and
`apply` is the only thing here that writes to the live graph.

The browse connection is ONE small pool to the open throwaway, made on first use and dropped
the moment the throwaway is gone, so no connection outlives what it reads."""
from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import asyncpg

from src.orchestrator import time_machine as tm
from src.orchestrator import time_machine_browse as browse
from src.orchestrator import time_machine_restore as restore

_NOT_OPEN = "No past moment is open. Pick one and start it first."
_NOT_READY = "That moment is still being opened. Try again in a moment."
_pool: asyncpg.Pool | None = None
_pool_for: str | None = None


async def _drop_pool() -> None:
    global _pool, _pool_for
    pool, _pool, _pool_for = _pool, None, None
    if pool is not None:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(pool.close(), timeout=10)


async def _browse_pool() -> tuple[asyncpg.Pool | None, dict[str, Any] | None]:
    """The pool onto the open, ready throwaway, or (None, an error dict)."""
    global _pool, _pool_for
    rec = await asyncio.to_thread(tm.read_record)
    if rec is None or rec.state == "failed":
        await _drop_pool()
        return None, {"error": _NOT_OPEN}
    if rec.state != "ready":
        return None, {"error": _NOT_READY}
    if _pool is None or _pool_for != rec.id:
        await _drop_pool()
        _pool = await asyncpg.create_pool(
            rec.reader_dsn(), min_size=1, max_size=3, timeout=15, command_timeout=60)
        _pool_for = rec.id
    return _pool, None


def _moment_dict(m: tm.Moment) -> dict[str, Any]:
    out: dict[str, Any] = {"id": m.id, "kind": m.kind, "label": m.label, "when": m.when,
                           "size_bytes": m.size_bytes}
    if m.at:
        out["at"] = m.at
    return out


async def moments() -> dict[str, Any]:
    from src.orchestrator import time_machine_pitr as pitr

    found = await asyncio.to_thread(tm.list_moments)
    from src.orchestrator import time_machine_restic as restic

    windows = await asyncio.to_thread(pitr.pitr_moments)
    nas = await asyncio.to_thread(restic.restic_moments)
    items = [{**_moment_dict(m), "available": True} for m in found] + windows + nas
    return {"moments": sorted(items, key=lambda i: str(i["when"]), reverse=True)}


async def status() -> dict[str, Any]:
    st = await asyncio.to_thread(tm.status)
    if st is None or st.get("state") in (None, "failed"):
        await _drop_pool()
    return {"throwaway": st}


async def start(moment_id: object, at: object = None) -> dict[str, Any]:
    """Begin opening a moment. Checks what can be checked now (the id, one at a time, disk),
    then hands the two-minute restore to a detached process; the console polls `status`."""
    if not isinstance(moment_id, str) or not moment_id.strip():
        return {"error": "Say which moment to open."}
    try:
        if at is not None and not isinstance(at, str):
            return {"error": "The time to open must be text such as 2026-11-17T09:30:00Z."}
        moment = await asyncio.to_thread(
            lambda: tm.find_moment(moment_id.strip(), at=at or None))
        if moment.kind == "restic":
            from src.orchestrator import time_machine_restic as restic

            item = next((i for i in await asyncio.to_thread(restic.restic_moments)
                         if i["id"] == moment.id), None)
            if item is not None and not item["available"]:
                return {"error": f"{item['reason']} That backup is only on the NAS, so it can "
                                 "only be opened at home."}
        rec = await asyncio.to_thread(tm.read_record)
        if rec is not None and rec.state != "failed":
            return {"error": f"A past moment is already {rec.state}. Tear it down first."}
        problem = tm.disk_problem(
            moment.size_bytes, await asyncio.to_thread(
                lambda: tm.free_bytes(tm.docker_root())),
            factor={"pitr": tm.PITR_DISK_FACTOR, "restic": 4.0}.get(moment.kind))
        if problem:
            return {"error": problem[0].upper() + problem[1:] + "."}
        pid = await asyncio.to_thread(lambda: tm.spawn_run(moment.id, at=moment.at))
    except tm.TimeMachineError as exc:
        return {"error": str(exc)}
    return {"started": True, "moment": _moment_dict(moment), "pid": pid}


async def extend() -> dict[str, Any]:
    try:
        rec = await asyncio.to_thread(tm.extend)
    except tm.TimeMachineError as exc:
        return {"error": str(exc)}
    return {"throwaway": rec.public()}


async def teardown() -> dict[str, Any]:
    await _drop_pool()
    removed = await asyncio.to_thread(tm.teardown)
    return {"removed": removed}


async def types() -> dict[str, Any]:
    pool, err = await _browse_pool()
    return err or {"types": await browse.type_counts(pool)}


def _int(raw: object, default: int) -> int:
    try:
        return int(str(raw))
    except (TypeError, ValueError):
        return default


async def objects(*, type_: str | None = None, q: str | None = None, limit: object = 50,
                  offset: object = 0) -> dict[str, Any]:
    pool, err = await _browse_pool()
    if err:
        return err
    lim, off = _int(limit, 50), _int(offset, 0)
    if q and q.strip():
        return await browse.search_objects(pool, q, type=type_ or None, limit=lim, offset=off)
    return await browse.list_objects(pool, type=type_ or None, limit=lim, offset=off)


async def detail(ref: str) -> dict[str, Any]:
    pool, err = await _browse_pool()
    if err:
        return err
    out = await browse.object_detail(pool, ref)
    return out or {"error": "Nothing in that moment matches that."}


async def sessions(*, q: str | None = None, limit: object = 50,
                   offset: object = 0) -> dict[str, Any]:
    pool, err = await _browse_pool()
    if err:
        return err
    return await browse.transcript_sessions(
        pool, query=q or "", limit=_int(limit, 50), offset=_int(offset, 0))


async def lines(anchor_sid: str, *, tail: object = 100,
                harness: str = "claude-code") -> dict[str, Any]:
    pool, err = await _browse_pool()
    if err:
        return err
    try:
        got = await browse.transcript_lines(pool, anchor_sid, tail=_int(tail, 100),
                                            harness=harness)
    except Exception as exc:  # noqa: BLE001 - a missing key or a bad row is a sentence
        return {"error": f"That transcript could not be read: {type(exc).__name__}."}
    if got is None:
        return {"error": "That session is not in this moment."}
    return {"anchor_sid": anchor_sid, "lines": got}


def _label(rec: dict[str, Any] | None) -> str:
    return str(((rec or {}).get("moment") or {}).get("label") or "")


async def plan(live_pool: asyncpg.Pool, ref: object) -> dict[str, Any]:
    from src.actions.core import Actions

    pool, err = await _browse_pool()
    if err:
        return err
    if not isinstance(ref, str) or not ref.strip():
        return {"error": "Say which item to restore."}
    st = await asyncio.to_thread(tm.status)
    try:
        return await restore.plan_restore(pool, Actions(live_pool), ref.strip(),
                                          moment_label=_label(st))
    except restore.RestoreError as exc:
        return {"error": str(exc)}


async def apply(live_pool: asyncpg.Pool, body: dict[str, Any]) -> dict[str, Any]:
    """The one write. Acts as the operator, needs the digest of the plan that was shown,
    and takes the operator's per-conflict choices and the links they want back."""
    from src.actions.core import Actions

    pool, err = await _browse_pool()
    if err:
        return err
    ref, digest = body.get("ref"), body.get("digest")
    if not isinstance(ref, str) or not ref.strip() or not isinstance(digest, str) or not digest:
        return {"error": "Restoring needs the item and the digest of the plan that was shown."}
    resolutions = body.get("resolutions")
    if resolutions is not None and not (
            isinstance(resolutions, dict)
            and all(isinstance(k, str) and isinstance(v, str) for k, v in resolutions.items())):
        return {"error": "Conflict choices must be a map of conflict to keep or restore."}
    link_keys = body.get("links")
    if link_keys is not None and not (
            isinstance(link_keys, list) and all(isinstance(k, str) for k in link_keys)):
        return {"error": "Links must be a list of the planned link keys to add."}
    note = body.get("note")
    st = await asyncio.to_thread(tm.status)
    try:
        return await restore.apply_restore(
            pool, Actions(live_pool), ref.strip(), expect_digest=digest,
            actor="analyst:operator", moment_label=_label(st), resolutions=resolutions,
            note=note if isinstance(note, str) else "", link_keys=link_keys)
    except restore.RestoreError as exc:
        return {"error": str(exc)}
