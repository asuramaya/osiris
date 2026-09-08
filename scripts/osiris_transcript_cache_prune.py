"""THE CACHE PRUNE (item 4 of the soul-store lane, thread 78efd46d): "transcript files
of dead sessions past a window are pruned from ~/.claude/projects after (2) covers them,
and rematerialized on demand by resume or a read." The on-demand half already exists and
is battle-tested — trigger.py's own resume-materialization inversion (ruling d161a156/
d63b2ca6) already emits a session's file back via SoulStore.rematerialize_to_disk on
every resume hop, and both the MCP `rematerialize` tool and `osiris rematerialize` CLI
command already expose it directly. This is the other half: actually removing a dead
session's file from disk once the store has safely captured it.

A session is PRUNABLE only when BOTH hold: (1) DEAD — its file's own mtime is older than
`dead_after` (default 30 days, no recent activity at all); (2) FULLY CAPTURED — the
file's mtime is NOT newer than the store's own last_ingested_at, the SAME guard
`rematerialize_to_disk` already uses to refuse overwriting a live transcript, applied
here in the opposite direction: never delete a file whose latest bytes the store hasn't
seen yet. A session failing either test is left alone, never pruned on a guess.

DRY-RUN IS THE ONLY WIRED MODE, same discipline as osiris_prune_ladder.py: `--apply`
exists so a human can execute the plan once ready, this script itself never calls it,
and nothing here is wired into a timer — activation is the operator's own word, same
gate as every other deletion in this lane.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

DSN = "postgresql://osiris:osiris@127.0.0.1:5601/osiris"


@dataclass(frozen=True)
class SessionRow:
    anchor_sid: str
    source_path: str
    file_mtime: datetime | None  # None = the file is already gone
    last_ingested_at: datetime


def find_prunable_sessions(
    sessions: list[SessionRow], *, now: datetime,
    dead_after: timedelta = timedelta(days=30),
) -> list[SessionRow]:
    """Pure: which sessions' FILES are safe to delete right now. Never touches
    soul_lines/soul_sessions — those are infinite-retention by design (soul_store.py's
    own module docstring); only the on-disk CACHE copy is ever a candidate here."""
    out = []
    for s in sessions:
        if s.file_mtime is None:
            continue  # already gone — nothing to prune
        if s.file_mtime > s.last_ingested_at:
            continue  # the store hasn't seen this file's latest bytes yet — never delete
        if now - s.file_mtime < dead_after:
            continue  # not dead yet
        out.append(s)
    return out


async def _collect_sessions(harness: str = "claude-code") -> list[SessionRow]:
    import asyncpg

    pool = await asyncpg.create_pool(
        DSN, min_size=1, max_size=1,
        server_settings={"application_name": "osiris-script:transcript-cache-prune"})
    try:
        rows = await pool.fetch(
            "SELECT anchor_sid, source_path, last_ingested_at FROM soul_sessions "
            "WHERE harness=$1", harness)
    finally:
        await pool.close()
    out = []
    for r in rows:
        mtime = _file_mtime(Path(r["source_path"]))
        out.append(SessionRow(r["anchor_sid"], r["source_path"], mtime,
                              r["last_ingested_at"]))
    return out


def _file_mtime(path: Path) -> datetime | None:
    """Kept in a sync helper for the blocking-call lint (ASYNC240), same convention
    soul_store.py's own `_path_is_file`/`_read_source` already use."""
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC) if path.is_file() else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dead-after-days", type=int, default=30,
                        help="a session's file must be untouched at least this many "
                             "days to be prune-eligible (default 30)")
    parser.add_argument("--apply", action="store_true",
                        help="ACTUALLY DELETE the files the dry-run lists — never run "
                             "this without the operator's own word on the printed list "
                             "first.")
    args = parser.parse_args(argv)

    sessions = asyncio.run(_collect_sessions())
    now = datetime.now(UTC)
    plan = find_prunable_sessions(
        sessions, now=now, dead_after=timedelta(days=args.dead_after_days))
    print(f"{len(plan)} of {len(sessions)} session file(s) are prune-eligible "
          f"(dead >= {args.dead_after_days}d, fully captured by the store):")
    for s in plan:
        print(f"  PRUNE  {s.source_path}  (anchor {s.anchor_sid}, last file activity "
              f"{s.file_mtime.isoformat() if s.file_mtime else '?'})")
    if not args.apply:
        print("\nDRY RUN ONLY — nothing deleted. Re-run with --apply once the operator "
              "has ruled on this list. A pruned file rematerializes on demand via "
              "`osiris rematerialize <anchor_sid>` or automatically on the next resume.")
        return 0
    print(f"\n--apply given — deleting {len(plan)} file(s) now.")
    for s in plan:
        Path(s.source_path).unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
