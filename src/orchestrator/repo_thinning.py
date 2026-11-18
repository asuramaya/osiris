"""Time Machine style thinning of the NAS backup repository, under the operator's rules.

THE RULES (operator decision, 2026-11-18): WAL and the base backups a rewind window needs are NEVER
thinned, so any moment of the recorded history can still be rewound to. Only database DUMPS thin:
one per day (the newest of each day) for 30 days, one per week after that, kept for ever unless
`backup.thin_weeks` caps it. Snapshots tagged `history-import` are kept for ever.

Every offload makes a snapshot of the whole vault, so a snapshot carries WAL, base backups and
dumps together, and restic can only forget whole snapshots. Two things follow:

  - DUMPS are thinned inside the snapshots by `restic rewrite --exclude-file`, naming exact dump
    files (each must look like osiris-YYYYMMDD-HHMMSS.dump or .sql and be a dump of the live
    vault; a pattern, WAL or a base backup is refused). Old mixed snapshots need no re-split.
  - SNAPSHOT COUNT is thinned by forgetting whole snapshots, but only behind a COVERAGE guard: a
    snapshot goes only if every WAL segment, base backup and kept dump still lives in at least one
    kept snapshot (read from each file's first and last snapshot). The plan keeps the tier snapshots
    (all for 24 hours, hourly for 24 hours after, daily for 30 days, weekly beyond) plus just enough
    more to cover everything, so a plain tier forget (which would lose WAL that only the forgotten
    snapshots hold) can never be applied.

The tier windows are calendar time back from the newest snapshot, not "the last N days that have a
backup", which would be the whole history for a laptop that is mostly away.

Then `prune` gives back what nothing refers to (this includes the packs left behind by interrupted
uploads, which are most of the unused space).

Nothing is removed by surprise:
  - `plan` only looks (restic's own dry runs) and reports what would go and about how much it frees.
  - `apply` does exactly one reviewed plan (by its id): forget, rewrite, prune.
  - the scheduled run does nothing until one plan has been applied by hand.
  - every run needs the home network (never Tailscale), a passed restore test, a recent good
    offload, and the offload lock, so it never meets a backup in the repository.
  - prune repacks at most `MAX_REPACK` per run; run it again to continue.

Never raises to the caller: every refusal is a plain reason."""
from __future__ import annotations

import bisect
import hashlib
import json
import os
import re
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

KEEP_TAGS = ("history-import",)
KEEP_LAST = 3
# CALENDAR windows, measured back from the newest snapshot
KEEP_WITHIN = "24h"
KEEP_HOURLY_WITHIN = "48h"      # one an hour for the 24 hours after the first 24
KEEP_DAILY_WITHIN = "30d"
KEEP_WEEKLY_WITHIN = "36500d"   # weekly snapshots are kept for ever
DUMP_DAILY_DAYS = 30
DUMP_SHARE_FREED = 0.86         # consecutive dumps share 13-15% of their chunks (measured)
MAX_REPACK = "20G"
MAX_UNUSED = "5%"
PLAN_MAX_AGE = timedelta(hours=24)
DRILL_MAX_AGE = timedelta(days=14)
OFFLOAD_MAX_AGE = timedelta(days=3)
MAX_REMOVE_SHARE = 0.9          # never remove more than this share of all snapshots in one plan
STATE_ENV = "OSIRIS_REPO_THINNING_FILE"
_DEFAULT_STATE = "~/.local/state/osiris/repo_thinning.json"
_DUMP_NAME = re.compile(r"^osiris-\d{8}-\d{6}\.(dump|sql)$")


@dataclass(frozen=True)
class Policy:
    keep_tags: tuple[str, ...] = KEEP_TAGS

    def args(self) -> list[str]:
        out = ["--keep-last", str(KEEP_LAST), "--keep-within", KEEP_WITHIN,
               "--keep-within-hourly", KEEP_HOURLY_WITHIN,
               "--keep-within-daily", KEEP_DAILY_WITHIN,
               "--keep-within-weekly", KEEP_WEEKLY_WITHIN]
        for tag in self.keep_tags:
            out += ["--keep-tag", tag]
        return out


@dataclass
class Plan:
    id: str
    at: str
    policy_args: list[str]
    keep: list[str]
    remove: list[str]
    total: int
    oldest_keep: str | None
    newest_removed: str | None
    drop_dumps: list[str] = field(default_factory=list)       # exact dump file names to drop
    drop_paths: list[str] = field(default_factory=list)       # their paths inside the snapshots
    rewrite: list[str] = field(default_factory=list)          # kept snapshots that hold them
    dump_bytes: int = 0                                       # logical size of the dropped dumps
    freed_bytes: int | None = None                            # estimate for the dump thinning
    prune_freed_bytes: int | None = None                      # estimate for the prune
    repo_bytes: int | None = None
    uncovered: int = 0
    problems: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def fingerprint(remove_ids: list[str], policy_args: list[str],
                drop: list[str] | None = None) -> str:
    raw = json.dumps([sorted(remove_ids), policy_args, sorted(drop or [])])
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def parse_forget(stdout: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """(kept, removed) snapshot objects from `restic forget --dry-run --json`: one group per
    host and paths."""
    groups = json.loads(stdout or "[]")
    kept: list[dict[str, Any]] = []
    removed: list[dict[str, Any]] = []
    for g in groups if isinstance(groups, list) else []:
        kept += [s for s in (g.get("keep") or []) if isinstance(s, dict)]
        removed += [s for s in (g.get("remove") or []) if isinstance(s, dict)]
    return kept, removed


# ---- what each file's life looks like, and the coverage guard ------------------------------------


@dataclass
class Life:
    kind: str
    size: int
    mtime: str
    first: str     # time of the oldest snapshot holding it
    last: str      # time of the newest
    path: str


def file_lifetimes(run: Callable[..., Any], snapshots: list[dict[str, Any]], *,
                   chunk: int = 20) -> dict[str, Life]:
    """For every restore-relevant file (WAL, base backup and its marker, live-vault dump): its size
    and which snapshots hold it (first and last), read a few snapshots at a time."""
    from src.orchestrator import restore_points

    when = {str(s["id"]): str(s.get("time")) for s in snapshots}
    lives: dict[str, Life] = {}
    ids = list(when)
    for i in range(0, len(ids), chunk):
        args = ["find", "--json", "--no-lock"]
        for sid in ids[i:i + chunk]:
            args += ["--snapshot", sid]
        proc = run([*args, "*"], timeout=1800)
        if proc.returncode != 0:
            raise RuntimeError("restic find failed: "
                               + ((proc.stderr or "").strip().splitlines()[-1:] or [""])[0][:300])
        for snap in json.loads(proc.stdout or "[]"):
            at = when.get(str(snap.get("snapshot")), "")
            for m in snap.get("matches") or []:
                if m.get("type") != "file":
                    continue
                got = restore_points.classify(str(m.get("path")))
                if not got:
                    continue
                kind, name = got
                cur = lives.get(name)
                if cur is None:
                    lives[name] = Life(kind, int(m.get("size") or 0), str(m.get("mtime")), at, at,
                                       str(m.get("path")))
                else:
                    cur.first, cur.last = min(cur.first, at), max(cur.last, at)
    return lives


def dump_drop_list(lives: dict[str, Life], *, now: datetime,
                   weeks: int = 0) -> list[str]:
    """The dump files the policy lets go: of each calendar day within 30 days all but the newest,
    and beyond that all but the newest of each week (and, when `weeks` is set, every dump older
    than that many weeks). Only real dump names ever appear here. Pure."""
    def when(life: Life) -> datetime:
        parsed = datetime.fromisoformat(life.mtime)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)

    dumps = sorted((when(v), n) for n, v in lives.items() if v.kind == "dump"
                   and _DUMP_NAME.match(n))
    keep: dict[Any, str] = {}
    for t, name in dumps:
        age = now - t
        if weeks and age > timedelta(weeks=weeks):
            continue
        key: Any = ("d", t.astimezone().date()) if age <= timedelta(days=DUMP_DAILY_DAYS) \
            else ("w", t.isocalendar()[:2])
        keep[key] = name       # sorted ascending, so the newest of the day or week wins
    kept = set(keep.values())
    return [n for _, n in dumps if n not in kept]


def cover_snapshots(snapshots: list[dict[str, Any]], kept_ids: set[str],
                    intervals: list[tuple[str, str]]) -> tuple[set[str], int]:
    """Add to `kept_ids` just enough snapshots that every (first, last) interval contains the time
    of a kept snapshot. Returns (the new kept ids, how many intervals could not be covered)."""
    by_time = sorted((str(s.get("time")), str(s["id"])) for s in snapshots)
    times = [t for t, _ in by_time]
    kept = set(kept_ids)
    kept_times = sorted(t for t, i in by_time if i in kept)
    lost = 0
    for first, last in sorted(intervals, key=lambda x: x[1]):
        j = bisect.bisect_left(kept_times, first)
        if j < len(kept_times) and kept_times[j] <= last:
            continue
        k = bisect.bisect_right(times, last) - 1
        if k < 0 or times[k] < first:
            lost += 1
            continue
        kept.add(by_time[k][1])
        bisect.insort(kept_times, times[k])
    return kept, lost


def safety_problems(snapshots: list[dict[str, Any]], kept_ids: set[str],
                    removed: list[dict[str, Any]], drop: list[str], uncovered: int) -> list[str]:
    """Reasons this plan must not run. Pure."""
    out: list[str] = []
    if not snapshots:
        return ["the repository has no snapshots"]
    newest = max(snapshots, key=lambda s: str(s.get("time")))
    if newest["id"] not in kept_ids:
        out.append("the newest snapshot would not be kept")
    if len(kept_ids) < KEEP_LAST and len(snapshots) >= KEEP_LAST:
        out.append(f"fewer than {KEEP_LAST} snapshots would be kept")
    if removed and len(removed) > MAX_REMOVE_SHARE * len(snapshots):
        out.append(f"{len(removed)} of {len(snapshots)} snapshots would go, which is too many "
                   "for one run")
    if [s for s in removed if set(s.get("tags") or []) & set(KEEP_TAGS)]:
        out.append("a snapshot tagged for keeping would be removed")
    if uncovered:
        out.append(f"{uncovered} WAL, base backup or dump file(s) would not be in any kept "
                   "snapshot")
    bad = [n for n in drop if not _DUMP_NAME.match(n)]
    if bad:
        out.append("the dump list holds something that is not a dump file name")
    return out


def build_plan(snapshots: list[dict[str, Any]], policy_keep: list[dict[str, Any]],
               lives: dict[str, Life], policy_args: list[str], *, now: datetime,
               weeks: int = 0) -> Plan:
    """The plan: tier snapshots plus the tagged ones plus the coverage cover are kept, the rest go;
    the dump files the policy lets go are dropped from the kept snapshots that hold them."""
    drop = dump_drop_list(lives, now=now, weeks=weeks)
    drop_set = set(drop)
    tagged = {str(s["id"]) for s in snapshots if set(s.get("tags") or []) & set(KEEP_TAGS)}
    intervals = [(v.first, v.last) for n, v in lives.items()
                 if v.kind in ("wal", "base", "verified")
                 or (v.kind == "dump" and n not in drop_set)]
    kept_ids, uncovered = cover_snapshots(
        snapshots, {str(s["id"]) for s in policy_keep} | tagged, intervals)
    removed = [s for s in snapshots if str(s["id"]) not in kept_ids]
    kept = [s for s in snapshots if str(s["id"]) in kept_ids]
    # kept, untagged snapshots that hold a dropped dump get rewritten without it
    spans = [(lives[n].first, lives[n].last) for n in drop]
    rewrite = [str(s["id"]) for s in kept if str(s["id"]) not in tagged
               and any(a <= str(s.get("time")) <= b for a, b in spans)]
    ids = [str(s["id"]) for s in removed]
    return Plan(
        id=fingerprint(ids + rewrite, policy_args, drop), at=now.isoformat(),
        policy_args=policy_args, keep=sorted(kept_ids), remove=ids, total=len(snapshots),
        oldest_keep=min((str(s.get("time")) for s in kept), default=None),
        newest_removed=max((str(s.get("time")) for s in removed), default=None),
        drop_dumps=drop, drop_paths=[lives[n].path for n in drop], rewrite=rewrite,
        dump_bytes=sum(lives[n].size for n in drop), uncovered=uncovered,
        problems=safety_problems(snapshots, kept_ids, removed, drop, uncovered))


# ---- the state file ---------------------------------------------------------------------------


def state_path() -> Path:
    return Path(os.environ.get(STATE_ENV) or _DEFAULT_STATE).expanduser()


def read_state() -> dict[str, Any]:
    try:
        body = json.loads(state_path().read_text())
    except (OSError, ValueError):
        return {}
    return body if isinstance(body, dict) else {}


def write_state(state: dict[str, Any]) -> None:
    path = state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_text(json.dumps(state, default=str))
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


# ---- the checks every run makes first ---------------------------------------------------------


def preconditions(*, presence: dict[str, Any], drill: dict[str, Any], offload: dict[str, Any],
                  now: datetime, parse: Callable[[Any], datetime | None]) -> str | None:
    """None when it is safe to go on, else the plain reason it is not. Pure."""
    if not presence.get("present"):
        return f"not on the home network ({presence.get('reason') or 'route unknown'})"
    if not presence.get("dev"):
        return "could not confirm which interface the route to the NAS leaves through"
    passed, error = parse(drill.get("last_passed_at")), drill.get("last_error")
    if not passed or error:
        return "the latest restore test has not passed" + (f" ({error})" if error else "")
    if now - passed > DRILL_MAX_AGE:
        return f"the last passed restore test is older than {DRILL_MAX_AGE.days} days"
    ok_at = parse(offload.get("last_successful_offload"))
    if not ok_at or now - ok_at > OFFLOAD_MAX_AGE:
        return f"there has been no good offload in the last {OFFLOAD_MAX_AGE.days} days"
    return None


# ---- the restic calls -------------------------------------------------------------------------


Runner = Callable[..., Any]   # (args, *, timeout, text=...) -> CompletedProcess


@dataclass(frozen=True)
class Deps:
    run: Runner
    presence: Callable[[dict[str, Any]], dict[str, Any]]
    hold_lock: Callable[[], Any]
    drill_receipt: Callable[[str], dict[str, Any]]
    offload_receipt: Callable[[str], dict[str, Any]]
    repo_bytes: Callable[[dict[str, Any]], int | None]
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))
    parse: Callable[[Any], datetime | None] = field(default=lambda v: None)


def _ok(proc: Any, what: str) -> str:
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout or "").strip().splitlines()[-1:] or [""]
        raise RuntimeError(f"{what} failed: {tail[0][:300]}")
    return str(proc.stdout or "")


_REMAINING = re.compile(r"^remaining:\s+\d+ blobs / ([\d.]+) (B|KiB|MiB|GiB|TiB)", re.M)
_UNIT = {"B": 1, "KiB": 1024, "MiB": 1024 ** 2, "GiB": 1024 ** 3, "TiB": 1024 ** 4}


def prune_estimate(deps: Deps, repo_bytes: int | None) -> int | None:
    """About how much a prune would give back right now (restic's own dry run): the repository's
    size on disk minus what would remain. None when it cannot be told."""
    if not repo_bytes:
        return None
    proc = deps.run(["prune", "--dry-run", "--no-lock"], timeout=3600)
    if proc.returncode != 0:
        return None
    m = _REMAINING.search(proc.stdout or "")
    if not m:
        return None
    return max(0, repo_bytes - int(float(m.group(1)) * _UNIT[m.group(2)]))


def make_plan(target: dict[str, Any], deps: Deps, *, weeks: int = 0, measure: bool = True) -> Plan:
    """Ask restic what the tiers would keep (a dry run: nothing is changed), read every file's life,
    and build the plan. Raises RuntimeError with a plain reason."""
    snaps = json.loads(_ok(deps.run(["snapshots", "--json", "--no-lock"], timeout=600),
                           "restic snapshots") or "[]")
    policy = Policy()
    out = _ok(deps.run(["forget", "--dry-run", "--json", "--no-lock", "--group-by",
                        "host,paths", *policy.args()], timeout=600), "restic forget (dry run)")
    kept, _removed = parse_forget(out)
    lives = file_lifetimes(deps.run, snaps)
    plan = build_plan(snaps, kept, lives, policy.args(), now=deps.now(), weeks=weeks)
    plan.repo_bytes = deps.repo_bytes(target)
    if measure and not plan.problems:
        plan.freed_bytes = int(plan.dump_bytes * DUMP_SHARE_FREED)
        plan.prune_freed_bytes = prune_estimate(deps, plan.repo_bytes)
    return plan


def apply_plan(target: dict[str, Any], deps: Deps, plan: Plan, *,
               say: Callable[[str], None] = print) -> str | None:
    """Do exactly this plan: forget its snapshots, drop its dump files from the kept snapshots
    that hold them, then prune (at most MAX_REPACK per run). A failure reason, or None. Everything
    is checked again first: a plan is only as good as the repository it was made against."""
    if plan.problems:
        return "; ".join(plan.problems)
    if plan.uncovered:
        return "the plan would leave files uncovered"
    if any(not _DUMP_NAME.match(n) for n in plan.drop_dumps):
        return "the dump list holds something that is not a dump file name"
    url = str(target.get("path_or_url") or "")
    reason = preconditions(
        presence=deps.presence(target), drill=deps.drill_receipt(url),
        offload=deps.offload_receipt(str(target.get("name"))), now=deps.now(), parse=deps.parse)
    if reason:
        return reason
    with deps.hold_lock() as held:
        if not held:
            return "an offload or restore test is running right now"
        snaps = json.loads(_ok(deps.run(["snapshots", "--json", "--no-lock"], timeout=600),
                               "restic snapshots") or "[]")
        present = {str(s["id"]) for s in snaps}
        if plan.remove and not any(i in present for i in plan.remove) and not plan.rewrite:
            return "none of the snapshots in this plan are in the repository any more"
        newest = max(snaps, key=lambda s: str(s.get("time")))["id"] if snaps else None
        if newest in plan.remove:
            return "the newest snapshot is in this plan; make a new plan"
        todo = [i for i in plan.remove if i in present]
        if todo:
            say(f"forgetting {len(todo)} snapshot(s)")
            _ok(deps.run(["forget", *todo], timeout=1800), "restic forget")
        rewrite = [i for i in plan.rewrite if i in present]
        if plan.drop_paths and rewrite:
            say(f"dropping {len(plan.drop_dumps)} thinned dump(s) from {len(rewrite)} snapshot(s)")
            with tempfile.NamedTemporaryFile("w", suffix=".exclude", delete=False) as handle:
                handle.write("\n".join(plan.drop_paths) + "\n")
            try:
                _ok(deps.run(["rewrite", "--forget", "--exclude-file", handle.name, *rewrite],
                             timeout=4 * 3600), "restic rewrite")
            finally:
                Path(handle.name).unlink(missing_ok=True)
        say(f"pruning (at most {MAX_REPACK} repacked this run)")
        _ok(deps.run(["prune", "--max-repack-size", MAX_REPACK, "--max-unused", MAX_UNUSED],
                     timeout=4 * 3600), "restic prune")
    return None


def describe(plan: Plan) -> list[str]:
    """Plain lines for the terminal."""
    lines = [f"{len(plan.remove)} of {plan.total} snapshot(s) would be forgotten, "
             f"{len(plan.keep)} kept (every WAL segment, base backup and kept dump stays in a kept "
             "snapshot, so every rewind window stays)."]
    if plan.oldest_keep:
        lines.append(f"The oldest snapshot kept is from {plan.oldest_keep[:10]}.")
    if plan.drop_dumps:
        lines.append(f"{len(plan.drop_dumps)} thinned dump(s) ({plan.dump_bytes / 1e9:.0f} GB) "
                     f"would be dropped from {len(plan.rewrite)} snapshot(s)"
                     + (f", freeing about {plan.freed_bytes / 1e9:.0f} GB"
                        if plan.freed_bytes is not None else "") + ".")
    if plan.prune_freed_bytes is not None:
        lines.append(f"Pruning what nothing refers to would give back about "
                     f"{plan.prune_freed_bytes / 1e9:.0f} GB"
                     + (f" of {plan.repo_bytes / 1e9:.0f} GB" if plan.repo_bytes else "") + ".")
    if plan.problems:
        lines.append("It will NOT run: " + "; ".join(plan.problems) + ".")
    elif plan.remove or plan.drop_dumps or plan.prune_freed_bytes:
        lines.append(f"To do it: osiris repo-thin --apply {plan.id}")
    else:
        lines.append("Nothing to do.")
    return lines


# ---- the real world ---------------------------------------------------------------------------


def _sftp_path(url: str) -> str | None:
    """`sftp:host:/path` -> `/path`."""
    parts = url.split(":", 2)
    return parts[2] if len(parts) == 3 and parts[0] == "sftp" and parts[2].startswith("/") else None


def ssh_repo_bytes(target: dict[str, Any]) -> int | None:
    """The repository's size on the NAS (`du -sb` over the same ssh the backup uses), or None."""
    import subprocess

    from src.orchestrator.network_presence import sftp_host

    url = str(target.get("path_or_url") or "")
    host, path = sftp_host(url), _sftp_path(url)
    if not host or not path:
        return None
    try:
        proc = subprocess.run(  # noqa: S603 - fixed argv, read-only
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", host, "du", "-sb", path],
            capture_output=True, text=True, timeout=180, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return None
    head = proc.stdout.split("\t")[0].strip() if proc.returncode == 0 else ""
    return int(head) if head.isdigit() else None


def real_deps(target: dict[str, Any], password: bytes) -> Deps:
    from src.orchestrator import restic_locks
    from src.orchestrator.instants import parse_time
    from src.orchestrator.network_presence import lan_presence
    from src.orchestrator.offload_runner import hold_tick_lock, offload_receipts
    from src.orchestrator.soul_key import restore_drill_receipts

    env = {**os.environ, "RESTIC_REPOSITORY": str(target.get("path_or_url") or ""),
           "RESTIC_PASSWORD": password.decode()}

    def run(args: list[str], *, timeout: float, text: bool = True) -> Any:
        return restic_locks.run_restic(args, env, timeout=timeout, text=text)

    return Deps(
        run=run, presence=lan_presence, hold_lock=hold_tick_lock,
        drill_receipt=lambda url: restore_drill_receipts().get(url) or {},
        offload_receipt=lambda name: offload_receipts().get(name) or {},
        repo_bytes=ssh_repo_bytes, parse=parse_time)


def refresh_points(target: dict[str, Any], deps: Deps) -> str | None:
    """Rebuild the saved restore map (read-only on the NAS). A failure reason, or None."""
    from src.orchestrator import restore_points

    try:
        restore_points.refresh(target, deps.run, repo_bytes=deps.repo_bytes, now=deps.now())
    except (RuntimeError, ValueError) as exc:
        return str(exc)
    return None


def refresh_time_machine(target: dict[str, Any], deps: Deps) -> str | None:
    """Keep the Time Machine's list of NAS-only dump moments fresh from this run's own restic
    runner (one NAS reader, not two). Read only; a failure is reported and never stops the run."""
    from src.orchestrator import time_machine_restic

    try:
        return time_machine_restic.refresh_map(target, deps.run, presence=deps.presence)
    except Exception as exc:  # noqa: BLE001 - never fail the thinning run for a listing
        return f"{type(exc).__name__}: {exc}"


async def command(*, pool: Any, apply_id: str | None = None, scheduled: bool = False,
                  refresh_only: bool = False, say: Callable[[str], None] = print,
                  deps: Deps | None = None) -> int:
    """The CLI body. Default: make and show a plan (changes nothing). `apply_id`: do that plan.
    `scheduled`: the timer's run, which does nothing until a plan has been applied by hand."""
    import asyncio

    from src.config.settings import get_settings
    from src.orchestrator.backup_settings import get_backup_settings
    from src.orchestrator.nas_first_offload import pick_target
    from src.orchestrator.restic_credential import ResticPasswordMissing, get_restic_password
    from src.orchestrator.settings_service import settings_with_overlay

    state = read_state()
    if scheduled and not state.get("approved"):
        # nothing is forgotten until a plan has been applied by hand; the weekly run still keeps
        # the restore map fresh
        say("repo-thin: no plan has been applied by hand yet (osiris repo-thin, then osiris "
            "repo-thin --apply <id>); only the restore map is rebuilt.")
        refresh_only = True
    settings = await get_backup_settings(pool)
    target, why = pick_target(settings.get("offload_targets") or [], None)
    if target is None:
        say(f"repo-thin: {why}.")
        return 1
    try:
        password = get_restic_password()
    except ResticPasswordMissing as exc:
        say(f"repo-thin: {exc}")
        return 1
    try:
        overlay = await settings_with_overlay(pool)
    except Exception:  # noqa: BLE001 - the settings are tuning, never a reason to stop
        overlay = get_settings()
    weeks = int(getattr(overlay, "osiris_thin_weeks", 0) or 0)   # 0 = weekly dumps for ever
    deps = deps or real_deps(target, password)
    now = deps.now().isoformat()

    seen = await asyncio.to_thread(deps.presence, target)
    if not seen.get("present") or not seen.get("dev"):
        say("repo-thin: not on the home network (" + str(seen.get("reason") or "route unknown")
            + "); nothing was done.")
        return 0 if scheduled else 1

    if refresh_only:
        failure = await asyncio.to_thread(refresh_points, target, deps)
        say(f"repo-thin: the restore map could not be built: {failure}." if failure
            else "repo-thin: the restore map is up to date (osiris backup-status shows it).")
        tm_note = await asyncio.to_thread(refresh_time_machine, target, deps)
        if tm_note:
            say(f"repo-thin: the Time Machine's NAS moment list could not be refreshed: "
                f"{tm_note}.")
        return 1 if failure else 0
    try:
        if apply_id is not None:
            saved = state.get("last_plan") or {}
            at = deps.parse(saved.get("at"))
            if saved.get("id") != apply_id or not at or deps.now() - at > PLAN_MAX_AGE:
                say("repo-thin: that plan is not the latest one or is more than a day old; "
                    "run `osiris repo-thin` for a fresh plan and apply that.")
                return 1
            plan = Plan(**saved)
        else:
            plan = await asyncio.to_thread(
                make_plan, target, deps, weeks=weeks, measure=not scheduled)
            if not scheduled:
                write_state({**state, "last_plan": plan.as_dict()})
                for line in describe(plan):
                    say(line)
                reason = preconditions(
                    presence=seen, drill=deps.drill_receipt(str(target.get("path_or_url"))),
                    offload=deps.offload_receipt(str(target.get("name"))), now=deps.now(),
                    parse=deps.parse)
                if reason:
                    say(f"It would not run now: {reason}.")
                return 0 if not plan.problems else 1
        failure = await asyncio.to_thread(apply_plan, target, deps, plan, say=say)
    except (RuntimeError, ValueError) as exc:
        failure = str(exc)
    run = {"at": now, "plan_id": plan.id, "forgot": len(plan.remove), "failure": failure}
    new = {**read_state(), "last_run": run}
    if failure is None:
        new["approved"] = {"at": now, "plan_id": plan.id}
    write_state(new)
    if failure:
        say(f"repo-thin: stopped: {failure}.")
        return 1
    say(f"repo-thin: done: {len(plan.remove)} snapshot(s) forgotten and the repository pruned.")
    note = await asyncio.to_thread(refresh_points, target, deps)
    if note:
        say(f"repo-thin: the restore map could not be rebuilt afterwards: {note}.")
    tm_note = await asyncio.to_thread(refresh_time_machine, target, deps)
    if tm_note:
        say(f"repo-thin: the Time Machine's NAS moment list could not be refreshed: {tm_note}.")
    return 0
