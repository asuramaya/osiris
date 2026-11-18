"""THE MONTHLY TIME MACHINE DRILL: proof that old history can still be opened and read.

The weekly drills prove the NEWEST local copies restore (the dump drill, the base backup plus
log drill) and that the NAS repository can be read. None of them proves that an OLD moment held
only on the NAS can still be opened end to end, which is exactly what thinning could silently
break, nor that the Time Machine itself (the throwaway, browsing, a restore plan) still works
after deploys. This does, once a month, alternating:

  dump   the oldest NAS dump older than 30 days: pulled over the home network, restored, browsed
         (objects exist, the newest one is not later than the moment), and ONE restore plan for
         an item is made against the live graph (it writes nothing);
  pitr   a time in the middle of the OLDEST window the NAS map knows: the base backup and the log
         cover are pulled, replayed to that time, and the newest assertion must not be later
         than it.

It only reads. It never applies a restore. It is skipped, with a plain note and no failure, when
the box is away from home, a backup to the NAS is running, a throwaway is already open, or the
disk is short; it tries again the next day. A failure leaves a receipt and one line for the
operator's desk; the at-home list shows an overdue or failed drill."""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.orchestrator import time_machine as tm
from src.orchestrator.instants import parse_time

RECEIPT_ENV = "OSIRIS_TM_DRILL_FILE"
_DEFAULT_RECEIPT = "~/.local/state/osiris/time_machine_drill.json"
INTERVAL_DAYS = 28
RETRY_AFTER_FAILURE_HOURS = 24
OVERDUE_DAYS = 45
MIN_DUMP_AGE_DAYS = 30
HISTORY_KEPT = 12
KINDS = ("dump", "pitr")
# A monthly check must stay cheap: the base backup plus the log pulled up to the chosen time may
# not exceed this. (The oldest window's base alone is 46 GB; an ordinary week's is a few GB.)
MAX_DRILL_BYTES = 12 * 2**30
AVG_SEGMENT_BYTES = 1_500_000
MAX_AHEAD = timedelta(days=2)
MIN_AHEAD = timedelta(hours=1)
# What the drill runs into that says nothing about the backups: not a failure.
_SKIP_MARKERS = ("home network", "backup to the NAS is running", "not enough disk",
                 "has not been built yet", "could not be checked")


def receipt_path() -> Path:
    return Path(os.environ.get(RECEIPT_ENV) or _DEFAULT_RECEIPT).expanduser()


def read_receipt() -> dict[str, Any]:
    try:
        body = json.loads(receipt_path().read_text())
    except (OSError, ValueError):
        return {}
    return body if isinstance(body, dict) else {}


def _write_receipt(body: dict[str, Any]) -> None:
    p = receipt_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(f"{p.name}.{os.getpid()}.tmp")
    try:
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as fh:
            json.dump(body, fh)
        os.replace(tmp, p)
    finally:
        tmp.unlink(missing_ok=True)


def due(receipt: dict[str, Any], *, now: datetime | None = None) -> bool:
    """Pure. Due when it never passed or last passed more than INTERVAL_DAYS ago, and not within
    RETRY_AFTER_FAILURE_HOURS of a failed attempt. A skipped run records no attempt, so it is
    simply due again."""
    now = now or datetime.now(UTC)
    attempt = parse_time(receipt.get("last_attempt_at"))
    passed = parse_time(receipt.get("last_passed_at"))
    if (receipt.get("last_error") and attempt
            and now - attempt < timedelta(hours=RETRY_AFTER_FAILURE_HOURS)):
        return False
    return passed is None or now - passed >= timedelta(days=INTERVAL_DAYS)


def next_kind(receipt: dict[str, Any]) -> str:
    """Alternate: the other kind from the one that last ran (a dump the first time)."""
    last = receipt.get("last_kind")
    return "pitr" if last == "dump" else "dump"


def status_line(receipt: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    """What the at-home list shows: {"state": never|passed|failed|overdue, "text": ...}."""
    now = now or datetime.now(UTC)
    passed = parse_time(receipt.get("last_passed_at"))
    if receipt.get("last_error"):
        return {"state": "failed", "text": "The monthly Time Machine check failed: "
                f"{receipt['last_error']}"}
    if passed is None:
        return {"state": "never", "text": "The monthly Time Machine check has not run yet; it "
                "runs by itself at home."}
    days = (now - passed).days
    kind = "an older dump" if receipt.get("last_kind") == "dump" else "a point in time"
    if days > OVERDUE_DAYS:
        return {"state": "overdue", "text": f"The monthly Time Machine check last passed {days} "
                "days ago; it only runs at home."}
    left_out = receipt.get("windows_not_drilled") or []
    note = ""
    if left_out:
        words = ", ".join(
            f"{parse_time(w.get('from')):%b %d}-{parse_time(w.get('to')):%b %d}"
            + (f" (base {w['base_bytes'] / 2**30:.0f} GB)" if w.get("base_bytes") else "")
            for w in left_out if parse_time(w.get("from")) and parse_time(w.get("to")))
        if words:
            note = f" Not checked monthly because they are too big: {words}."
    return {"state": "passed", "text": f"The monthly Time Machine check passed {days} day(s) "
            f"ago ({kind} opened from the NAS).{note}"}


# --- choosing what to open -------------------------------------------------------------------

def pick_dump(items: list[dict[str, Any]], *, now: datetime) -> dict[str, Any] | None:
    """The oldest NAS dump older than MIN_DUMP_AGE_DAYS, else the oldest there is."""
    if not items:
        return None
    ordered = sorted(items, key=lambda i: str(i["when"]))
    cutoff = now - timedelta(days=MIN_DUMP_AGE_DAYS)
    old = [i for i in ordered if (parse_time(i["when"]) or now) <= cutoff]
    return (old or ordered)[0]


def plan_window(window: dict[str, Any], base_bytes: int) -> tuple[str, int] | None:
    """(the time to open, estimated bytes pulled) for one window, or None when even the base
    backup plus a little log does not fit the budget. The time is EARLY in the window (a quarter
    in, at most MAX_AHEAD, and no further than the log budget allows) so little log is pulled."""
    a, b = parse_time(window.get("from")), parse_time(window.get("to"))
    if not (a and b and b > a):
        return None
    room = MAX_DRILL_BYTES - base_bytes
    span = (b - a).total_seconds()
    rate = float(window.get("segments") or 0) * AVG_SEGMENT_BYTES / span     # log bytes per second
    ahead = min(span / 4, MAX_AHEAD.total_seconds())
    if rate > 0:
        ahead = min(ahead, room / rate)
    if room <= 0 or ahead < MIN_AHEAD.total_seconds():
        return None
    return (a + timedelta(seconds=ahead)).isoformat(), int(base_bytes + ahead * rate)


def pick_pitr(windows: list[dict[str, Any]], size_of: Callable[[str, datetime], int | None],
              ) -> tuple[str, str, int, list[dict[str, Any]]] | None:
    """(moment id, time, estimated bytes, the windows skipped on the way) for the OLDEST window
    whose base backup plus the log up to a time near its start fits the budget: the stretch
    thinning is most likely to have hurt, without ever pulling a huge one. The skipped windows
    (too big, or not held on the NAS) are returned so the receipt can say what is NOT being
    drilled and why."""
    spans = []
    for w in windows:
        a, b = parse_time(w.get("from")), parse_time(w.get("to"))
        if a and b and b > a and w.get("base"):
            spans.append((a, w))
    skipped: list[dict[str, Any]] = []
    for a, w in sorted(spans, key=lambda x: x[0]):
        size = size_of(str(w["base"]), a)
        note = {"base": str(w["base"]), "from": w.get("from"), "to": w.get("to"),
                "base_bytes": size}
        if size is None:
            skipped.append({**note, "why": "its base backup is not on the NAS"})
            continue
        planned = plan_window(w, size)
        if planned:
            return ("pitr-" + str(w["base"]).removesuffix(".tar.gz"), planned[0], planned[1],
                    skipped)
        skipped.append({**note, "why": f"over the {MAX_DRILL_BYTES // 2**30} GiB budget "
                                       f"(base {size / 2**30:.1f} GiB)"})
    return None


# --- the run ---------------------------------------------------------------------------------

def _is_skip(message: str) -> bool:
    return any(m in message for m in _SKIP_MARKERS)


async def _verify_dump(mpool: Any, live_pool: Any, moment: tm.Moment) -> list[str]:
    """Problems found in an opened dump moment (empty when it is sound)."""
    from src.actions.core import Actions
    from src.orchestrator import time_machine_browse as browse
    from src.orchestrator import time_machine_restore as restore

    problems: list[str] = []
    total = await mpool.fetchval("SELECT count(*) FROM objects")
    if not total:
        return ["the opened moment holds no objects"]
    newest = await mpool.fetchval("SELECT max(created_at) FROM objects")
    when = parse_time(moment.when)
    if newest and when and newest > when + timedelta(days=1):
        problems.append(f"it holds an object from {newest:%Y-%m-%d}, after the moment itself")
    sample = await browse.list_objects(mpool, type="Decision", limit=1)
    if not sample["rows"]:
        return [*problems, "no decision could be listed in the opened moment"]
    plan = await restore.plan_restore(mpool, Actions(live_pool), sample["rows"][0]["id"],
                                      moment_label=moment.label)
    if not plan.get("digest"):
        problems.append("a restore plan could not be made for a listed item")
    return problems


async def _verify_pitr(mpool: Any, moment: tm.Moment) -> list[str]:
    problems: list[str] = []
    if not await mpool.fetchval("SELECT count(*) FROM objects"):
        return ["the opened moment holds no objects"]
    newest = await mpool.fetchval("SELECT max(created_at) FROM assertions")
    at, start = parse_time(moment.at), parse_time(moment.window_from)
    if newest is None:
        return ["the opened moment holds no assertions"]
    if at and newest > at + timedelta(minutes=1):
        problems.append(f"its newest assertion ({newest:%Y-%m-%d %H:%M}) is later than the time "
                        f"asked for ({at:%Y-%m-%d %H:%M})")
    if start and newest < start:
        problems.append("its newest assertion is older than the window it should be inside")
    return problems


async def run_drill(live_pool: Any, *, kind: str | None = None, force: bool = False,
                    now: datetime | None = None,
                    start: Callable[..., tm.Throwaway] | None = None,
                    base_size: Callable[[str, datetime], int | None] | None = None,
                    presence: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
                    say: Callable[[str], None] = print) -> dict[str, Any]:
    """One drill. Returns {"skipped": why} (nothing recorded but the note), or {"ok": bool,
    "kind", "moment", "seconds", ["error"]}. Never raises for a drill problem."""
    import asyncpg

    from src.orchestrator import restore_points
    from src.orchestrator import time_machine_pitr as pitr
    from src.orchestrator import time_machine_restic as restic

    now = now or datetime.now(UTC)
    receipt = read_receipt()
    if not force and not due(receipt, now=now):
        return {"skipped": "not due"}
    kind = kind or next_kind(receipt)
    est = 0
    over_budget: list[dict[str, Any]] = []
    if kind not in KINDS:
        return {"skipped": f"unknown kind {kind!r}"}

    def skip(why: str) -> dict[str, Any]:
        _write_receipt({**receipt, "last_skipped": {"at": now.isoformat(), "why": why}})
        say(f"time-machine drill: skipped: {why}")
        return {"skipped": why}

    reason = await asyncio.to_thread(restic.nas_reason, presence)
    if reason:
        return skip(reason)
    existing = await asyncio.to_thread(tm.read_record)
    if existing is not None and existing.state != "failed":
        return skip("a past moment is already open")
    cache = await asyncio.to_thread(restic.read_cache) or {}
    if kind == "dump":
        items = await asyncio.to_thread(restic.restic_moments, None, presence=presence)
        chosen = pick_dump([i for i in items if i["available"]], now=now)
        if chosen is None:
            return skip("no NAS dumps are listed yet; run `osiris time-machine refresh` at home")
        moment = await asyncio.to_thread(tm.find_moment, chosen["id"])
    else:
        windows = (await asyncio.to_thread(restore_points.read_cache) or {}).get("windows") or []
        try:
            pick = await asyncio.to_thread(
                lambda: pick_pitr(windows, base_size or restic.base_size))
        except tm.TimeMachineError as exc:
            return skip(str(exc) if _is_skip(str(exc)) else f"could not size the windows: {exc}")
        if pick is None:
            return skip("no point-in-time window is small enough for a monthly check "
                        f"(budget {MAX_DRILL_BYTES // 2**30} GiB) or none is known yet")
        est, over_budget = pick[2], pick[3]
        moment = await asyncio.to_thread(lambda: pitr.find_pitr_moment(pick[0], pick[1]))
    del cache
    say(f"time-machine drill: opening {moment.label} ({kind})")

    t0 = datetime.now(UTC)
    problems: list[str] = []
    error: str | None = None
    mpool: Any = None
    try:
        rec = await asyncio.to_thread(lambda: (start or tm.start)(moment))
        mpool = await asyncpg.create_pool(rec.reader_dsn(), min_size=1, max_size=2,
                                          command_timeout=300)
        problems = (await _verify_dump(mpool, live_pool, moment) if kind == "dump"
                    else await _verify_pitr(mpool, moment))
        if problems:
            error = "; ".join(problems)
    except tm.TimeMachineError as exc:
        if _is_skip(str(exc)):
            return skip(str(exc))
        error = str(exc)
    except Exception as exc:  # noqa: BLE001 - a drill that blows up is a failed drill
        error = f"{type(exc).__name__}: {exc}"
    finally:
        if mpool is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(mpool.close(), timeout=15)
        await asyncio.to_thread(tm.teardown)
    left = sorted(p.name for p in restic.scratch_root().glob("osiris-tm-*") if p.is_dir())
    if left and error is None:
        error = f"scratch directories were left behind: {', '.join(left)}"
    seconds = int((datetime.now(UTC) - t0).total_seconds())
    entry = {"at": now.isoformat(), "kind": kind, "moment": moment.id,
             "label": moment.label, "ok": error is None, "seconds": seconds}
    if est:
        entry["estimated_bytes"] = est
    if over_budget:
        entry["windows_not_drilled"] = over_budget
    if error:
        entry["error"] = error
    history = ([entry, *(receipt.get("history") or [])])[:HISTORY_KEPT]
    new = {**receipt, "last_attempt_at": now.isoformat(), "last_kind": kind,
           "last_moment": moment.id, "last_seconds": seconds, "history": history}
    new.pop("last_skipped", None)
    if kind == "pitr":
        new["windows_not_drilled"] = over_budget
    if error is None:
        new.update({"last_passed_at": now.isoformat(), "last_error": None})
    else:
        new["last_error"] = error
    _write_receipt(new)
    result = {"ok": error is None, "kind": kind, "moment": moment.id, "seconds": seconds}
    if error:
        result["error"] = error
        await _tell_desk(live_pool, kind, moment, error)
    say("time-machine drill: " + (f"passed in {seconds} s" if not error
                                  else f"FAILED: {error}"))
    return result


async def _tell_desk(pool: Any, kind: str, moment: tm.Moment, error: str) -> None:
    """One line for the operator's desk when a drill fails. Never fails the run."""
    import logging

    from src.orchestrator.mailbox import send_message

    try:
        await send_message(
            pool, from_agent="cron:time_machine_drill", from_project="osiris",
            to_project="operator", desk_kind="fyi", dedup_window_secs=86400,
            body=(f"The monthly Time Machine check failed ({moment.label}, {kind}): {error} "
                  "Run it again with `osiris time-machine drill --force`."))
    except Exception:  # noqa: BLE001
        logging.getLogger(__name__).warning("could not tell the desk about the drill",
                                            exc_info=True)
