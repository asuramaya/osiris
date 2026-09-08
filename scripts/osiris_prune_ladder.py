#!/usr/bin/env python3
"""THE RETENTION LADDER (the vault lane, operator ruling 39384a87/c53a5fc0, item 2): a
classic GFS (grandfather-father-son) thinning schedule over the DB dump population in
BOTH `backups/` and the vault -- every dump inside the last 48h survives whole (nothing
thinned there at all); 48h-30d thins to one-per-CALENDAR-DAY; 30d-1y thins to one-per-
CALENDAR-WEEK; beyond 1y thins to one-per-CALENDAR-MONTH, forever. Within any bucket the
NEWEST survives (the operator's own reasoning: "the graph is append-only so a newer full
contains every older one" -- every survivor here IS a full pg_dump by construction, no
special-casing needed).

SCOPED TO DB DUMPS ONLY (osiris-*.dump / the pre-.dump-switch osiris-*.sql still on disk
during the transition), never the transcript tarballs osiris_backup.sh's own incremental
--listed-incremental chain produces -- pruning an incremental archive by "newest N" (or
by this ladder) breaks every later day's restorability the moment it removes a link out
of the middle of the chain. That population needs its own, chain-aware retention design,
not this one; deliberately out of scope here (flagged on commit 27058bd already).

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


def _scan(directory: Path) -> list[DumpFile]:
    """Every `osiris-<timestamp>.dump`/`.sql` in `directory` (non-recursive — both
    `backups/` and the vault are flat). Timestamp parsed from the filename itself (the
    identity osiris_backup.sh already stamps into it), falling back to the file's own
    mtime only for a name this pattern doesn't recognize — never silently skipped, so a
    stray file still gets a real answer instead of vanishing from the ladder's view."""
    out: list[DumpFile] = []
    if not directory.is_dir():
        return out
    for p in sorted(directory.glob("osiris-*")):
        if not p.is_file():
            continue
        m = _NAME_RE.match(p.name)
        if m:
            when = datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S").replace(tzinfo=UTC)
        else:
            when = datetime.fromtimestamp(p.stat().st_mtime, tz=UTC)
        out.append(DumpFile(str(p), when, p.stat().st_size))
    return out


def _report(label: str, plan: dict[str, list[DumpFile]]) -> None:
    removed_bytes = sum(f.size_bytes for f in plan["remove"])
    print(f"\n{label}: {len(plan['keep'])} kept, {len(plan['remove'])} would be removed "
          f"({removed_bytes / (1024**3):.2f} GB)")
    for f in plan["remove"]:
        print(f"  REMOVE  {f.path}  ({f.when.isoformat()}, "
              f"{f.size_bytes / (1024**2):.1f} MB)")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backups", type=Path, default=Path("backups"))
    parser.add_argument("--vault", type=Path,
                        default=Path.home() / "osiris-vault")
    parser.add_argument("--apply", action="store_true",
                        help="ACTUALLY DELETE the files the dry-run lists — never run "
                             "this without the operator's own word on the printed list "
                             "first (ruling 39384a87/c53a5fc0).")
    args = parser.parse_args(argv)

    now = datetime.now(UTC)
    plans = {
        "backups/": plan_prune(_scan(args.backups), now=now),
        "vault": plan_prune(_scan(args.vault), now=now),
    }
    for label, plan in plans.items():
        _report(label, plan)
    total_remove = sum(len(p["remove"]) for p in plans.values())
    if not args.apply:
        print(f"\nDRY RUN ONLY — {total_remove} file(s) would be removed, none deleted. "
              "Re-run with --apply once the operator has ruled on this list.")
        return 0
    print(f"\n--apply given — deleting {total_remove} file(s) now.")
    for plan in plans.values():
        for f in plan["remove"]:
            Path(f.path).unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
