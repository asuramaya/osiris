"""A nightly run that did not happen on time, said out loud.

The box sleeps. A laptop that is shut overnight misses every 02:15, 03:00 and 04:30 job, and
until now nothing said so: a systemd timer with `Persistent=true` quietly catches up when the
box wakes (the dump still happens, just at 10:54), and the worker's own daily jobs are simply
lost for that night. Both leave the same trace, the time each last ran, and this module reads it.

For each daily or weekly job with a fixed slot it answers one question, from the last run alone:
did the most recent slot get its run, did a run come late (and how late), or is a slot still
waiting? The sentence names the slot, the day and what happened. "The box was asleep" is an
inference and is worded as one ("probably asleep or off").

Pure over its inputs (slot, last run, now): the callers read the last run from systemd or from
the worker's own job record."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

# How long after its slot a job may start before it counts as late (a timer's own jitter, a busy
# box). A run later than this is reported; a slot still empty after this is reported as missed.
GRACE = timedelta(minutes=30)
# Only slots this recent are worth saying: an old miss that has since been made up is history.
LOOKBACK = timedelta(days=3)


@dataclass(frozen=True)
class Slot:
    """When a job is due: every day (`weekday` None) or one weekday (Monday is 0), at HH:MM."""
    hour: int
    minute: int
    weekday: int | None = None

    def latest_at_or_before(self, moment: datetime) -> datetime:
        """The most recent time the slot came due at or before `moment`, in its own zone."""
        candidate = moment.replace(hour=self.hour, minute=self.minute, second=0, microsecond=0)
        if candidate > moment:
            candidate -= timedelta(days=1)
        if self.weekday is not None:
            candidate -= timedelta(days=(candidate.weekday() - self.weekday) % 7)
        return candidate


def assess(label: str, slot: Slot, last_run: datetime | None, now: datetime) -> dict[str, Any]:
    """{label, status, sentence, due, ran}. status is one of:
    `on_time` (the latest slot got its run), `late` (a run came, but more than the grace after
    its slot), `waiting` (the slot has come and gone and no run yet), `never` (no run on
    record)."""
    due = slot.latest_at_or_before(now)
    base: dict[str, Any] = {"label": label, "due": due.isoformat(),
                            "ran": last_run.isoformat() if last_run else None}
    if last_run is None:
        return {**base, "status": "never", "sentence": f"{label} has no run on record"}
    when_due = f"{due:%H:%M} on {due:%m-%d}"
    if last_run >= due:
        if last_run - due <= GRACE:
            return {**base, "status": "on_time",
                    "sentence": f"{label} ran at {last_run:%H:%M} on {last_run:%m-%d}"}
        return {**base, "status": "late", "sentence": (
            f"{label} was due at {when_due} and ran at {last_run:%H:%M}, "
            f"{_hours(last_run - due)} late (the box was probably asleep or off)")}
    # the latest slot has no run after it
    if now - due <= GRACE:
        return {**base, "status": "on_time", "sentence": (
            f"{label} is due at {due:%H:%M} (last ran {last_run:%m-%d %H:%M})")}
    return {**base, "status": "waiting", "sentence": (
        f"{label} was due at {when_due} and has not run yet; it last ran at "
        f"{last_run:%H:%M} on {last_run:%m-%d} (the box was probably asleep or off)")}


def _hours(delta: timedelta) -> str:
    hours = delta.total_seconds() / 3600
    return f"{hours:.1f} h" if hours >= 1 else f"{round(delta.total_seconds() / 60)} min"


def recent_problems(
    jobs: list[tuple[str, Slot, datetime | None]], now: datetime,
) -> list[dict[str, Any]]:
    """The jobs whose most recent slot was late or is still waiting, within the lookback,
    worst first (waiting before late). On-time jobs are not listed: silence when well."""
    out = []
    for label, slot, last_run in jobs:
        item = assess(label, slot, last_run, now)
        if item["status"] in ("waiting", "late", "never"):
            due = datetime.fromisoformat(item["due"])
            if now - due <= LOOKBACK:
                out.append(item)
    rank = {"waiting": 0, "never": 1, "late": 2}
    out.sort(key=lambda i: (rank[i["status"]], i["due"]))
    return out


# The worker's own daily jobs and when its cron starts them (src/workers/arq_worker.py). A test
# keeps this table equal to the cron definitions, so a moved slot cannot leave it stale.
WORKER_DAILY_JOBS: tuple[tuple[str, str, Slot], ...] = (
    ("storage_housekeeping_heartbeat", "the nightly housekeeping", Slot(2, 15)),
    ("db_compaction_heartbeat", "the nightly database compaction", Slot(3, 0)),
    ("retention_heartbeat", "the nightly retention pass", Slot(3, 30)),
    ("soul_cold_tier_heartbeat", "the nightly cold-tier fold", Slot(3, 45)),
)

_DAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


def slot_from_calendar(spec: str) -> Slot | None:
    """A Slot from a simple systemd OnCalendar line ("*-*-* 04:30:00", "Sun 02:00:00"); None for
    anything else (a job that runs every few minutes has no slot to miss)."""
    import re

    m = re.fullmatch(
        r"(?:(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+)?(?:\*-\*-\*\s+)?(\d{1,2}):(\d{2})(?::\d{2})?",
        spec.strip())
    if not m:
        return None
    return Slot(int(m.group(2)), int(m.group(3)),
                _DAYS.index(m.group(1)) if m.group(1) else None)


def parse_systemd_stamp(text: object) -> datetime | None:
    """A systemd timestamp ("Thu 2026-11-12 10:54:47 CST", local time) as an aware datetime, or
    None for "n/a" or anything unreadable."""
    import re

    m = re.search(r"(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})", str(text or ""))
    if not m:
        return None
    return datetime.fromisoformat(f"{m.group(1)}T{m.group(2)}").astimezone()


async def section(pool: Any, timers: list[dict[str, Any]], *, now: datetime) -> dict[str, Any]:
    """The `missed_runs` block of backup-status: every daily or weekly timer in `timers` (each
    {unit, label, schedule, last_trigger}) and every worker daily job, the ones whose latest slot
    was late or is still waiting. A source that cannot be read leaves its jobs out and says so in
    `unread`; it never raises."""
    import json

    jobs: list[tuple[str, Slot, datetime | None]] = []
    unread: list[str] = []
    for t in timers:
        slot = slot_from_calendar(str(t.get("schedule") or ""))
        if slot is None:
            continue
        last = parse_systemd_stamp(t.get("last_trigger"))
        if last is None and not t.get("active_state"):
            unread.append(str(t.get("unit")))  # systemd said nothing about it
            continue
        jobs.append((str(t.get("label") or t["unit"]), slot, last))
    try:
        rows = await pool.fetch(
            "SELECT key, cursor FROM watermarks WHERE key = ANY($1::text[])",
            [f"job:{name}" for name, _, _ in WORKER_DAILY_JOBS])
        seen = {r["key"][4:]: json.loads(r["cursor"] or "{}") for r in rows}
        for name, label, slot in WORKER_DAILY_JOBS:
            raw = (seen.get(name) or {}).get("last_run")
            jobs.append((label, slot, datetime.fromisoformat(raw).astimezone(now.tzinfo)
                         if raw else None))
    except Exception:  # noqa: BLE001
        unread.append("the worker's job record")
    problems = recent_problems(jobs, now)
    return {"problems": [p["sentence"] for p in problems], "detail": problems,
            "checked": len(jobs), **({"unread": unread} if unread else {})}
