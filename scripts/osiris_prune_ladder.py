#!/usr/bin/env python3
"""THE RETENTION LADDER (the vault lane, operator ruling 39384a87/c53a5fc0, item 2): a
classic GFS (grandfather-father-son) thinning schedule over the DB dump population in
BOTH `backups/` and the vault, AND over `<vault>/basebackups/` (item 3's own weekly
pg_basebackup, osiris_base_backup.sh -- a base backup is, like a DB dump, complete and
independently restorable on its own, so the identical ladder applies unmodified) --
every survivor inside the last 48h stays whole (nothing thinned there at all); 48h-30d
thins to one-per-CALENDAR-DAY; 30d-1y thins to one-per-CALENDAR-WEEK; beyond 1y thins to
one-per-CALENDAR-MONTH, forever. Within any bucket the NEWEST survives (the operator's
own reasoning: "the graph is append-only so a newer full contains every older one" --
every survivor here IS a full snapshot by construction, no special-casing needed).

SCOPED TO DB DUMPS ONLY for `plan_prune`/`--apply`'s main pass (osiris-*.dump / the
pre-.dump-switch osiris-*.sql still on disk during the transition) -- an individual
transcript tarball is never a candidate here, since pruning one out of the middle of its
own incremental chain breaks every later day's restorability. `plan_prune_transcript_chains`
is the SEPARATE, chain-aware sibling (Thoth msg 8211, off the same ruling): osiris_backup.sh
now keys the snapshot file and each day's tarball by ISO WEEK, so restorability only ever
needs to span one week's own chain -- this prunes whole weekly chains at a time (every
tarball plus the snapshot file sharing one week-key, together, never a partial chain),
keeping the last 4 weekly chains plus one chain per calendar month beyond that.

DRY-RUN IS THE ONLY WIRED MODE (operator's own word, relayed by Thoth: "do not delete
anything until I relay the operator's word on that list"). `--apply` exists so a human
can actually execute the ladder ONCE that word has been given -- this script itself never
calls it, and nothing here is wired into a timer/cron; running with `--apply` is a
deliberate, always-manual act.

Pure logic (`plan_prune`) is tested directly; the CLI is a thin scan-report-optionally-
delete shell around it."""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

_NAME_RE = re.compile(r"^osiris-(\d{8})-(\d{6})\.(?:dump|sql)$")
_BASEBACKUP_RE = re.compile(r"^osiris-basebackup-(\d{8})-(\d{6})\.tar\.gz$")


@dataclass(frozen=True)
class DumpFile:
    path: str
    when: datetime
    size_bytes: int = 0


def _bucket_key(when: datetime, tier: str) -> str:
    if tier == "daily":
        return when.date().isoformat()
    if tier == "weekly":
        iso = when.isocalendar()
        return f"{iso[0]}-W{iso[1]:02d}"
    if tier == "monthly":
        return when.strftime("%Y-%m")
    raise ValueError(f"unknown tier: {tier!r}")


def plan_prune(
    files: list[DumpFile], *, now: datetime,
    hot_window: timedelta = timedelta(hours=48),
    daily_window: timedelta = timedelta(days=30),
    weekly_window: timedelta = timedelta(days=365),
) -> dict[str, list[DumpFile]]:
    """The ladder itself, pure and total-order-free of any filesystem call: given every
    dump's own (path, timestamp), returns {"keep": [...], "remove": [...]}, both sorted
    oldest-first. `remove` is what a caller would delete; this function never deletes
    anything itself."""
    files_sorted = sorted(files, key=lambda f: f.when)
    keep: set[str] = set()
    tiers: dict[str, list[DumpFile]] = {"hot": [], "daily": [], "weekly": [], "monthly": []}
    for f in files_sorted:
        age = now - f.when
        if age <= hot_window:
            tiers["hot"].append(f)
        elif age <= daily_window:
            tiers["daily"].append(f)
        elif age <= weekly_window:
            tiers["weekly"].append(f)
        else:
            tiers["monthly"].append(f)
    keep.update(f.path for f in tiers["hot"])  # the hot window is never thinned
    for tier_name in ("daily", "weekly", "monthly"):
        buckets: dict[str, DumpFile] = {}
        for f in tiers[tier_name]:
            key = _bucket_key(f.when, tier_name)
            if key not in buckets or f.when > buckets[key].when:
                buckets[key] = f
        keep.update(f.path for f in buckets.values())
    return {
        "keep": [f for f in files_sorted if f.path in keep],
        "remove": [f for f in files_sorted if f.path not in keep],
    }


def _scan(directory: Path, *, glob: str = "osiris-*") -> list[DumpFile]:
    """Every file matching `glob` in `directory` (non-recursive — `backups/`, the vault,
    and `<vault>/basebackups/` are each flat). Timestamp parsed from the filename itself
    (the identity osiris_backup.sh/osiris_base_backup.sh already stamp into it — either
    `_NAME_RE` for a DB dump or `_BASEBACKUP_RE` for a base backup, tried in that order),
    falling back to the file's own mtime only for a name neither pattern recognizes —
    never silently skipped, so a stray file still gets a real answer instead of vanishing
    from the ladder's view. A base backup is, like a DB dump, a complete and
    independently-restorable unit on its own (`pg_basebackup`'s whole point) — the SAME
    `plan_prune` ladder applies to both, unlike the transcript tarballs' own chain-scoped
    sibling."""
    out: list[DumpFile] = []
    if not directory.is_dir():
        return out
    for p in sorted(directory.glob(glob)):
        if not p.is_file():
            continue
        m = _NAME_RE.match(p.name) or _BASEBACKUP_RE.match(p.name)
        if m:
            when = datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S").replace(tzinfo=UTC)
        else:
            when = datetime.fromtimestamp(p.stat().st_mtime, tz=UTC)
        out.append(DumpFile(str(p), when, p.stat().st_size))
    return out


_WEEK_RE = re.compile(r"^claude-transcripts-(\d{4})-W(\d{2})-\d{8}\.tar\.gz$")


@dataclass(frozen=True)
class TranscriptChain:
    week_key: str  # "2026-W37"
    week_start: datetime  # the ISO week's own Monday, for month-bucketing
    files: list[str]  # every tarball + snapshot file belonging to this chain
    size_bytes: int = 0


def plan_prune_transcript_chains(
    chains: list[TranscriptChain], *, keep_recent: int = 4,
) -> dict[str, list[TranscriptChain]]:
    """Chain-aware, unlike `plan_prune`: the unit thinned is a WHOLE weekly chain (every
    tarball sharing one week-key, plus that week's snapshot file), never a single tarball
    out of the middle of one — removing a chain removes exactly the week it belongs to,
    always in full, so a survivor's own restorability never depends on a chain this
    function didn't keep. The most recent `keep_recent` chains (by week, not by age
    against `now` — "the last four weekly chains", not a fixed cutoff) survive whole;
    older chains thin to one per calendar month, newest-in-month wins."""
    chains_sorted = sorted(chains, key=lambda c: c.week_start, reverse=True)
    keep_keys = {c.week_key for c in chains_sorted[:keep_recent]}
    buckets: dict[str, TranscriptChain] = {}
    for c in chains_sorted[keep_recent:]:
        mkey = c.week_start.strftime("%Y-%m")
        if mkey not in buckets or c.week_start > buckets[mkey].week_start:
            buckets[mkey] = c
    keep_keys.update(c.week_key for c in buckets.values())
    return {
        "keep": [c for c in chains_sorted if c.week_key in keep_keys],
        "remove": [c for c in chains_sorted if c.week_key not in keep_keys],
    }


def _scan_transcript_chains(directory: Path) -> list[TranscriptChain]:
    """Every `claude-transcripts-<week>-<day>.tar.gz` in `directory`, grouped by its own
    week-key into a `TranscriptChain` — the corresponding `.transcript-archive-<week>.snar`
    snapshot file, if still present, rides along as part of the SAME chain (it's only ever
    needed to produce that week's own next incremental, never to extract an existing one,
    but it belongs to the chain's identity all the same)."""
    if not directory.is_dir():
        return []
    groups: dict[str, list[Path]] = {}
    for p in sorted(directory.glob("claude-transcripts-*.tar.gz")):
        m = _WEEK_RE.match(p.name)
        if m:
            groups.setdefault(f"{m.group(1)}-W{m.group(2)}", []).append(p)
    chains = []
    for key, paths in groups.items():
        year, week = int(key[:4]), int(key[6:8])
        week_start = datetime.fromisocalendar(year, week, 1).replace(tzinfo=UTC)
        snar = directory / f".transcript-archive-{key}.snar"
        files = [str(p) for p in paths]
        if snar.is_file():
            files.append(str(snar))
        size = sum(Path(f).stat().st_size for f in files)
        chains.append(TranscriptChain(key, week_start, files, size))
    return chains


def _report_chains_text(label: str, plan: dict[str, list[TranscriptChain]]) -> str:
    removed_bytes = sum(c.size_bytes for c in plan["remove"])
    lines = [f"\n{label} transcript chains: {len(plan['keep'])} kept, "
             f"{len(plan['remove'])} whole chains would be removed "
             f"({removed_bytes / (1024**3):.2f} GB)"]
    for c in plan["remove"]:
        lines.append(f"  REMOVE CHAIN {c.week_key}  ({len(c.files)} files, "
                     f"{c.size_bytes / (1024**2):.1f} MB)")
    return "\n".join(lines)


def _report_text(label: str, plan: dict[str, list[DumpFile]]) -> str:
    removed_bytes = sum(f.size_bytes for f in plan["remove"])
    lines = [f"\n{label}: {len(plan['keep'])} kept, {len(plan['remove'])} would be removed "
             f"({removed_bytes / (1024**3):.2f} GB)"]
    for f in plan["remove"]:
        lines.append(f"  REMOVE  {f.path}  ({f.when.isoformat()}, "
                     f"{f.size_bytes / (1024**2):.1f} MB)")
    return "\n".join(lines)


def _report_chains(label: str, plan: dict[str, list[TranscriptChain]]) -> None:
    print(_report_chains_text(label, plan))


def _report(label: str, plan: dict[str, list[DumpFile]]) -> None:
    print(_report_text(label, plan))


def build_manifest_body(
    plans: dict[str, dict[str, list[DumpFile]]],
    chain_plan: dict[str, list[TranscriptChain]],
) -> str:
    """Pure: the exact text mailed to the operator's desk (thread 9fac4e0d part 1) —
    the SAME wording the dry-run CLI prints, so a human reading the mail sees exactly
    what a human running the command by hand would have seen."""
    total_remove = sum(len(p["remove"]) for p in plans.values())
    total_chains_remove = len(chain_plan["remove"])
    parts = [f"PRUNE LADDER MANIFEST — {total_remove} dump file(s) and "
             f"{total_chains_remove} transcript chain(s) are planned for removal "
             "tomorrow unless this brief is dimmed before then (thread 9fac4e0d)."]
    parts.extend(_report_text(label, plan) for label, plan in plans.items())
    parts.append(_report_chains_text("vault", chain_plan))
    parts.append("\nRun scripts/osiris_prune_ladder.py (no flags) yourself for the "
                 "identical dry-run at any time. To stop tomorrow's apply, dim this "
                 "brief.")
    return "\n".join(parts)


def _compute_plans(
    backups: Path, vault: Path,
) -> tuple[dict[str, dict[str, list[DumpFile]]], dict[str, list[TranscriptChain]]]:
    """The scan-and-plan step, shared by the dry-run CLI, --apply, --manifest, and
    --apply-if-clear — one computation, never four copies to drift apart."""
    now = datetime.now(UTC)
    plans = {
        "backups/": plan_prune(_scan(backups), now=now),
        "vault": plan_prune(_scan(vault), now=now),
        "vault/basebackups": plan_prune(_scan(vault / "basebackups"), now=now),
    }
    chain_plan = plan_prune_transcript_chains(_scan_transcript_chains(vault))
    return plans, chain_plan


DSN = "postgresql://osiris:osiris@127.0.0.1:5601/osiris"
_MANIFEST_FROM_AGENT = "system:prune-ladder"
MANIFEST_MIN_AGE = timedelta(hours=20)


async def mail_manifest(
    plans: dict[str, dict[str, list[DumpFile]]],
    chain_plan: dict[str, list[TranscriptChain]],
) -> int:
    """Send the dry-run plan to the operator's desk as a decision-band brief (thread
    9fac4e0d part 1) — uses src.db.pool.create_pool, NOT bare asyncpg.create_pool
    (thread 8542ee89's own lesson: the jsonb codec it registers is what makes
    send_message's own graph-edge write actually land). Returns the sent message id,
    the SAME id --apply-if-clear looks up the next day."""
    from src.db.pool import create_pool
    from src.orchestrator.mailbox import send_message

    body = build_manifest_body(plans, chain_plan)
    pool = await create_pool(
        DSN, min_size=1, max_size=1,
        application_name="osiris-script:prune-ladder-manifest")
    try:
        result = await send_message(
            pool, from_agent=_MANIFEST_FROM_AGENT, from_project="osiris",
            to_project="operator", desk_kind="decision", body=body)
        return int(result["id"])
    finally:
        await pool.close()


async def find_clear_manifest(
    *, min_age: timedelta = MANIFEST_MIN_AGE,
) -> tuple[int | None, str]:
    """The apply-side gate (thread 9fac4e0d part 1: "apply next day unless dimmed").
    Looks up the NEWEST manifest this script itself sent to the operator's desk and
    returns (message_id, reason) — message_id is None whenever applying would be
    wrong: no manifest was ever sent, the newest one is younger than `min_age` (today
    is not genuinely "the day after" yet), or it was DIMMED (`fleet_messages.moot_at`
    set — `dim_brief`'s own mechanism, mailbox.py: an agent annotating a brief moot,
    NEVER the operator's own settle, which stays a human act; a dim here means
    something judged the plan no longer current). A found, non-dimmed, old-enough
    manifest returns its id and a `reason` describing why applying is clear."""
    import asyncpg

    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=1)
    try:
        row = await pool.fetchrow(
            "SELECT id, created_at, moot_at FROM fleet_messages "
            "WHERE from_agent=$1 AND to_project='operator' "
            "ORDER BY created_at DESC LIMIT 1", _MANIFEST_FROM_AGENT)
    finally:
        await pool.close()
    if row is None:
        return None, "no manifest has ever been sent — run --manifest first"
    if row["moot_at"] is not None:
        return None, (f"manifest {row['id']} was dimmed at "
                      f"{row['moot_at'].isoformat()} — not applying")
    age = datetime.now(UTC) - row["created_at"]
    if age < min_age:
        return None, (f"manifest {row['id']} is only {age} old (need "
                      f"{min_age}) — today is not yet 'the day after'")
    return int(row["id"]), f"manifest {row['id']}, sent {age} ago, not dimmed — clear"


def _apply(
    plans: dict[str, dict[str, list[DumpFile]]],
    chain_plan: dict[str, list[TranscriptChain]],
) -> None:
    for plan in plans.values():
        for f in plan["remove"]:
            Path(f.path).unlink(missing_ok=True)
    for c in chain_plan["remove"]:
        for path in c.files:
            Path(path).unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    import asyncio

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backups", type=Path, default=Path("backups"))
    parser.add_argument("--vault", type=Path,
                        default=Path.home() / "osiris-vault")
    parser.add_argument("--apply", action="store_true",
                        help="ACTUALLY DELETE the files the dry-run lists — never run "
                             "this without the operator's own word on the printed list "
                             "first (ruling 39384a87/c53a5fc0).")
    parser.add_argument("--manifest", action="store_true",
                        help="Mail the dry-run plan to the operator's desk as a "
                             "decision brief (thread 9fac4e0d part 1) instead of "
                             "printing it. Deletes nothing.")
    parser.add_argument("--apply-if-clear", action="store_true",
                        help="Apply ONLY if the newest --manifest brief this script "
                             "sent is at least 20h old and has not been dimmed — "
                             "refuses with a named reason otherwise. Deletes nothing "
                             "when refusing.")
    args = parser.parse_args(argv)

    if args.manifest:
        plans, chain_plan = _compute_plans(args.backups, args.vault)
        mid = asyncio.run(mail_manifest(plans, chain_plan))
        print(f"manifest mailed to the operator's desk — message {mid}")
        return 0

    if args.apply_if_clear:
        mid, reason = asyncio.run(find_clear_manifest())
        if mid is None:
            print(f"REFUSING — {reason}", file=sys.stderr)
            return 1
        plans, chain_plan = _compute_plans(args.backups, args.vault)
        total_remove = sum(len(p["remove"]) for p in plans.values())
        total_chains_remove = len(chain_plan["remove"])
        print(f"{reason} — applying {total_remove} dump file(s) and "
              f"{total_chains_remove} transcript chain(s) now.")
        _apply(plans, chain_plan)
        return 0

    plans, chain_plan = _compute_plans(args.backups, args.vault)
    for label, plan in plans.items():
        _report(label, plan)
    # transcript CHAINS live only in the vault (osiris_backup.sh never writes them to
    # backups/) — a distinct population, reported and pruned as whole chains
    _report_chains("vault", chain_plan)

    total_remove = sum(len(p["remove"]) for p in plans.values())
    total_chains_remove = len(chain_plan["remove"])
    if not args.apply:
        print(f"\nDRY RUN ONLY — {total_remove} dump file(s) and {total_chains_remove} "
              "transcript chain(s) would be removed, none deleted. Re-run with --apply "
              "once the operator has ruled on this list.")
        return 0
    print(f"\n--apply given — deleting {total_remove} dump file(s) and "
          f"{total_chains_remove} transcript chain(s) now.")
    _apply(plans, chain_plan)
    return 0


if __name__ == "__main__":
    sys.exit(main())
