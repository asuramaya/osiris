"""Time Machine style thinning of the NAS backup repository.

Every offload makes one restic snapshot of the vault, about every 15 minutes while the box is
home, and nothing ever removed one: 358 snapshots and 959 GB. This keeps what a person wants from
a backup history and lets the rest go:

  - every snapshot for the last 24 hours
  - one per hour for the 24 hours after that
  - one per day for 30 days
  - one per week beyond that, for a number of weeks you set (`backup.thin_weeks`, default 26), and
    fewer weeks when the repository is over its size limit (`backup.thin_size_ceiling_gb`)
  (the windows are calendar time back from the newest snapshot, not "the last N days that have a
  backup", which would be the whole history for a laptop that is mostly away)
  - the 3 newest snapshots always, and every snapshot tagged `history-import` forever

Each snapshot holds the whole vault as it was, so a snapshot that is kept still holds the newest
verified base backup and the WAL after it: the newest snapshot is always kept, and the check
refuses a plan that would not keep it.

Nothing is removed by surprise:
  - `plan` only looks (restic's own dry run) and reports what would go and about how much space it
    frees, measured by restic from the blobs the kept snapshots still need.
  - `apply` removes exactly the snapshots of one reviewed plan (by its id), then runs prune.
  - the scheduled run does nothing until one plan has been applied by hand.
  - every run needs the home network (never Tailscale), a passed restore test, a recent good
    offload, and the offload lock, so it never meets a backup in the repository.
  - prune repacks at most `MAX_REPACK` per run; run it again to continue.

Never raises to the caller: every refusal is a plain reason."""
from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

KEEP_TAGS = ("history-import",)
KEEP_LAST = 3
# CALENDAR windows, measured back from the newest snapshot: `--keep-daily 30` would mean the last
# 30 days that HAVE a snapshot, which is the whole history for a laptop that is mostly away
KEEP_WITHIN = "24h"
KEEP_HOURLY_WITHIN = "48h"      # one an hour for the 24 hours after the first 24
KEEP_DAILY_WITHIN = "30d"
DEFAULT_WEEKS = 26
MIN_WEEKS = 4
MAX_REPACK = "20G"
MAX_UNUSED = "5%"
PLAN_MAX_AGE = timedelta(hours=24)
DRILL_MAX_AGE = timedelta(days=14)
OFFLOAD_MAX_AGE = timedelta(days=3)
MAX_REMOVE_SHARE = 0.9          # never remove more than this share of all snapshots in one plan
STATE_ENV = "OSIRIS_REPO_THINNING_FILE"
_DEFAULT_STATE = "~/.local/state/osiris/repo_thinning.json"


@dataclass(frozen=True)
class Policy:
    weeks: int = DEFAULT_WEEKS
    keep_tags: tuple[str, ...] = KEEP_TAGS

    def args(self) -> list[str]:
        out = ["--keep-last", str(KEEP_LAST), "--keep-within", KEEP_WITHIN,
               "--keep-within-hourly", KEEP_HOURLY_WITHIN,
               "--keep-within-daily", KEEP_DAILY_WITHIN,
               "--keep-within-weekly", f"{self.weeks * 7}d"]
        for tag in self.keep_tags:
            out += ["--keep-tag", tag]
        return out


def weeks_for(weeks: int, repo_bytes: int | None, ceiling_gb: int) -> tuple[int, str | None]:
    """The weekly count to use: the setting, or half of it (never under `MIN_WEEKS`) when the
    repository is over its size limit. Returns (weeks, why it was cut or None)."""
    weeks = max(MIN_WEEKS, int(weeks))
    if ceiling_gb and repo_bytes and repo_bytes > ceiling_gb * 10**9:
        cut = max(MIN_WEEKS, weeks // 2)
        if cut < weeks:
            return cut, (f"the repository is {repo_bytes / 1e9:.0f} GB, over its {ceiling_gb} GB "
                         f"limit, so weekly snapshots are kept for {cut} weeks instead of {weeks}")
    return weeks, None


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
    freed_bytes: int | None = None
    repo_bytes: int | None = None
    weeks_note: str | None = None
    problems: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def fingerprint(remove_ids: list[str], policy_args: list[str]) -> str:
    raw = json.dumps([sorted(remove_ids), policy_args])
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


def safety_problems(snapshots: list[dict[str, Any]], kept: list[dict[str, Any]],
                    removed: list[dict[str, Any]]) -> list[str]:
    """Reasons this plan must not run. Pure."""
    out: list[str] = []
    if not snapshots:
        return ["the repository has no snapshots"]
    newest = max(snapshots, key=lambda s: str(s.get("time")))
    if newest["id"] not in {s["id"] for s in kept}:
        out.append("the newest snapshot would not be kept")
    if len(kept) < KEEP_LAST and len(snapshots) >= KEEP_LAST:
        out.append(f"fewer than {KEEP_LAST} snapshots would be kept")
    if removed and len(removed) > MAX_REMOVE_SHARE * len(snapshots):
        out.append(f"{len(removed)} of {len(snapshots)} snapshots would go, which is too many "
                   "for one run")
    tagged = [s for s in removed if set(s.get("tags") or []) & set(KEEP_TAGS)]
    if tagged:
        out.append("a snapshot tagged for keeping would be removed")
    return out


def build_plan(snapshots: list[dict[str, Any]], kept: list[dict[str, Any]],
               removed: list[dict[str, Any]], policy_args: list[str], *,
               now: datetime | None = None) -> Plan:
    ids = [str(s["id"]) for s in removed]
    return Plan(
        id=fingerprint(ids, policy_args), at=(now or datetime.now(UTC)).isoformat(),
        policy_args=policy_args, keep=[str(s["id"]) for s in kept], remove=ids,
        total=len(snapshots),
        oldest_keep=min((str(s.get("time")) for s in kept), default=None),
        newest_removed=max((str(s.get("time")) for s in removed), default=None),
        problems=safety_problems(snapshots, kept, removed))


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


# ---- the real restic calls --------------------------------------------------------------------


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


def _stats_bytes(deps: Deps, ids: list[str] | None) -> int | None:
    args = ["stats", "--mode", "raw-data", "--json", "--no-lock"] + (ids or [])
    try:
        body = json.loads(_ok(deps.run(args, timeout=3600), "restic stats"))
    except (RuntimeError, ValueError):
        return None
    size = body.get("total_size") if isinstance(body, dict) else None
    return int(size) if isinstance(size, int | float) else None


def make_plan(target: dict[str, Any], deps: Deps, *, weeks: int, ceiling_gb: int = 0,
              measure: bool = True) -> Plan:
    """Ask restic what the policy would forget (a dry run: nothing is changed) and, when
    `measure`, how much space that frees. Raises RuntimeError with a plain reason."""
    snaps = json.loads(_ok(deps.run(["snapshots", "--json", "--no-lock"], timeout=600),
                           "restic snapshots") or "[]")
    repo_bytes = deps.repo_bytes(target)
    use_weeks, why = weeks_for(weeks, repo_bytes, ceiling_gb)
    policy = Policy(weeks=use_weeks)
    out = _ok(deps.run(["forget", "--dry-run", "--json", "--no-lock", "--group-by",
                        "host,paths", *policy.args()], timeout=600), "restic forget (dry run)")
    kept, removed = parse_forget(out)
    plan = build_plan(snaps, kept, removed, policy.args(), now=deps.now())
    plan.repo_bytes, plan.weeks_note = repo_bytes, why
    if measure and plan.remove and not plan.problems:
        kept_bytes = _stats_bytes(deps, plan.keep)
        total_bytes = _stats_bytes(deps, None)
        if kept_bytes is not None and total_bytes is not None:
            plan.freed_bytes = max(0, total_bytes - kept_bytes)
    return plan


def apply_plan(target: dict[str, Any], deps: Deps, plan: Plan, *,
               say: Callable[[str], None] = print) -> str | None:
    """Remove exactly `plan.remove`, then prune (at most MAX_REPACK per run). Returns a failure
    reason, or None. Checks everything again first: a plan is only as good as the repository it
    was made against."""
    if plan.problems:
        return "; ".join(plan.problems)
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
        missing = [i for i in plan.remove if i not in present]
        if len(missing) == len(plan.remove) and plan.remove:
            return "none of the snapshots in this plan are in the repository any more"
        newest = max(snaps, key=lambda s: str(s.get("time")))["id"] if snaps else None
        if newest in plan.remove:
            return "the newest snapshot is in this plan; make a new plan"
        todo = [i for i in plan.remove if i in present]
        if todo:
            say(f"forgetting {len(todo)} snapshot(s)")
            _ok(deps.run(["forget", *todo], timeout=1800), "restic forget")
        say(f"pruning (at most {MAX_REPACK} repacked this run)")
        _ok(deps.run(["prune", "--max-repack-size", MAX_REPACK, "--max-unused", MAX_UNUSED],
                     timeout=4 * 3600), "restic prune")
    return None


def describe(plan: Plan) -> list[str]:
    """Plain lines for the terminal."""
    lines = [f"{len(plan.remove)} of {plan.total} snapshot(s) would be forgotten, "
             f"{len(plan.keep)} kept."]
    if plan.oldest_keep:
        lines.append(f"The oldest snapshot kept is from {plan.oldest_keep[:10]}.")
    if plan.freed_bytes is not None:
        lines.append(f"That frees about {plan.freed_bytes / 1e9:.0f} GB"
                     + (f" of {plan.repo_bytes / 1e9:.0f} GB" if plan.repo_bytes else "")
                     + " (measured from what the kept snapshots still need).")
    if plan.weeks_note:
        lines.append(plan.weeks_note + ".")
    if plan.problems:
        lines.append("It will NOT run: " + "; ".join(plan.problems) + ".")
    elif plan.remove:
        lines.append(f"To do it: osiris repo-thin --apply {plan.id}")
    else:
        lines.append("Nothing to forget.")
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
    weeks = int(getattr(overlay, "osiris_thin_weeks", DEFAULT_WEEKS) or DEFAULT_WEEKS)
    ceiling = int(getattr(overlay, "osiris_thin_size_ceiling_gb", 0) or 0)
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
                make_plan, target, deps, weeks=weeks, ceiling_gb=ceiling, measure=not scheduled)
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
    return 0
