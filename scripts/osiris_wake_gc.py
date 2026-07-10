#!/usr/bin/env python3
"""Wake GC — the zombie-card culler (wake hygiene, thread fc2071f8, Anubis VI's handoff).

FleetView accumulates corpses: wake-minted sessions that finished (or found nothing) and sit
forever at 'send a prompt to start', helper stubs that never got a first prompt, sessions
killed by transient API errors. At wakes 10/h the mint rate outruns manual cleanup. This
lists (default) and culls (--apply) the residue:

  * session transcripts with ZERO main-loop assistant turns, older than --age hours — a card
    that never became a mind;
  * synthesized wake job dirs (/tmp/osiris-wakes/jobs/wake-*) older than --age hours — always
    ephemeral, no cleanup was ever owed;
  * the extractor's OWN transcripts (project slug ending -osiris-extract) older than --age
    hours — every miner tick's `claude -p` call leaves one (~145/day); the miner already
    refuses to mine them (the loop-pathology exclusion in _list_transcripts), so they are
    disk litter with no reader.

CULLING IS SAFE under at-least-once mail: a killed card holds no state the graph doesn't —
any mail it leased un-leases at expiry and redelivers on the next wake; anything it settled
is settled forever. (Operator note: API-error corpses can be killed freely for the same
reason.) The graph is NEVER touched — this trims harness litter, not memory.

STDLIB ONLY. Usage: python scripts/osiris_wake_gc.py [--age 24] [--apply]
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import time
from pathlib import Path

PROJECTS = Path.home() / ".claude" / "projects"
WAKE_JOBS = Path(tempfile.gettempdir()) / "osiris-wakes" / "jobs"
EXTRACT_SUFFIX = "-osiris-extract"  # providers.ClaudeCliClient's dedicated cwd slug


def _has_assistant_turn(path: Path) -> bool:
    """True if the transcript contains at least one main-loop assistant message — a mind
    actually spoke here. Reads incrementally; bails at the first hit."""
    try:
        with path.open() as fh:
            for line in fh:
                if '"assistant"' not in line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if e.get("type") == "assistant" and not e.get("isSidechain"):
                    return True
    except OSError:
        return True  # unreadable → assume alive; never cull blind
    return False


def find_victims(projects: Path, cutoff: float) -> tuple[list[Path], list[Path]]:
    """(zero-turn sessions, extractor transcripts) older than cutoff — pure, testable."""
    zero_turn: list[Path] = []
    extract: list[Path] = []
    for t in sorted(projects.glob("*/*.jsonl")):
        try:
            if t.stat().st_mtime > cutoff:
                continue
        except OSError:
            continue
        if "subagents" in t.parts:
            continue
        if t.parent.name.endswith(EXTRACT_SUFFIX):
            extract.append(t)  # the extractor spoke, but nothing will ever read it
            continue
        if not _has_assistant_turn(t):
            zero_turn.append(t)
    return zero_turn, extract


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--age", type=float, default=24.0, help="minimum age in hours (default 24)")
    ap.add_argument("--apply", action="store_true", help="delete; default is a dry-run list")
    args = ap.parse_args()
    cutoff = time.time() - args.age * 3600
    victims, extracts = find_victims(PROJECTS, cutoff)
    dirs: list[Path] = []
    if WAKE_JOBS.is_dir():
        for d in sorted(WAKE_JOBS.iterdir()):
            try:
                if d.is_dir() and d.stat().st_mtime <= cutoff:
                    dirs.append(d)
            except OSError:
                continue
    for v in victims:
        print(f"{'CULL' if args.apply else 'would cull'} zero-turn session: {v}")
        if args.apply:
            v.unlink(missing_ok=True)
    for e in extracts:
        print(f"{'CULL' if args.apply else 'would cull'} extractor transcript: {e}")
        if args.apply:
            e.unlink(missing_ok=True)
    for d in dirs:
        print(f"{'CULL' if args.apply else 'would cull'} stale wake job dir: {d}")
        if args.apply:
            shutil.rmtree(d, ignore_errors=True)
    n = len(victims) + len(extracts) + len(dirs)
    print(f"{'culled' if args.apply else 'dry run — would cull'} {n} "
          f"({len(victims)} zero-turn sessions, {len(extracts)} extractor transcripts, "
          f"{len(dirs)} wake dirs)"
          + ("" if args.apply else " — pass --apply to delete"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
