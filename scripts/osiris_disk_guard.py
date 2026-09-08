"""THE DISK GUARD (the vault lane, operator ruling 39384a87/c53a5fc0, item 5): the disk-space
emergency that opened this whole lane (92% full, ~3 days runway at 44GB/day) was caught by a
human noticing, not by any check in this house. This closes that gap on the write side —
before osiris_backup.sh writes a new FULL pg_dump, it asks `has_room` whether the filesystem
holding it has at least the last dump's own size plus a margin free. NO is a refusal to write,
never a prune — pruning is `osiris_prune_ladder.py`'s job, and only ever under `--apply` after
the operator's own word on the printed list. A refusal alarms the desk via osiris_alarm.py so
a human sees it the same day, not at the next weekly preflight pass.

Pure logic (`has_room`) is tested directly; the CLI is a thin scan-and-decide shell around it,
exit 0 = room, exit 1 = refuse.
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def has_room(free_bytes: int, last_size_bytes: int, margin_frac: float = 0.20) -> bool:
    """True iff `free_bytes` covers another dump the size of the last one, plus a
    `margin_frac` cushion on top (default 20%) — the margin exists because a live dump
    can be larger than the last one (the graph is append-only, only ever grows), so
    "exactly enough room for a same-size copy" is already too tight a bar."""
    return free_bytes >= last_size_bytes * (1 + margin_frac)


def main(argv: list[str] | None = None) -> int:
    from scripts.osiris_prune_ladder import _scan

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dir", type=Path, help="directory the next full dump would land in")
    parser.add_argument("--margin", type=float, default=0.20,
                        help="fraction of the last dump's size to require as headroom "
                             "on top of its own size (default 0.20)")
    args = parser.parse_args(argv)

    dumps = _scan(args.dir)
    if not dumps:
        # nothing to compare against yet — never refuse a FIRST dump on an empty directory
        print(f"osiris_disk_guard: no prior dump in {args.dir}, nothing to compare — OK")
        return 0
    last = max(dumps, key=lambda f: f.when)
    free = shutil.disk_usage(args.dir).free
    if has_room(free, last.size_bytes, margin_frac=args.margin):
        print(f"osiris_disk_guard: {free / (1024**3):.2f} GB free, last dump "
              f"{last.size_bytes / (1024**3):.2f} GB — OK")
        return 0
    needed = last.size_bytes * (1 + args.margin)
    print(f"osiris_disk_guard: REFUSING — {free / (1024**3):.2f} GB free, need "
          f"{needed / (1024**3):.2f} GB ({last.size_bytes / (1024**3):.2f} GB last dump "
          f"+ {args.margin:.0%} margin)", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
