"""One fast walk over a transcripts root, for everything that needs "every session file".

`~/.claude/projects` holds ~13,700 project directories, each with a `memory` directory (Claude
Code's notes, never a transcript), for ~575 transcripts. `root.rglob("*.jsonl")` builds a Path and
a stat for each of those ~27,000 directories: 0.3 s warm and 0.75 s cold, per call, and five
callers made it on every tick. `jsonl_files` finds the same files with `os.scandir` and does not
enter `memory` directories: ~0.08 s.
"""
from __future__ import annotations

import os
from pathlib import Path

# directories that never hold a session transcript
PRUNE_DIRS = frozenset({"memory"})


def jsonl_files(root: Path) -> list[Path]:
    """Every `*.jsonl` under `root`, at any depth: what `root.expanduser().rglob("*.jsonl")`
    returned, except inside a `memory` directory. Like rglob it does not follow symlinked
    directories; unlike rglob an unreadable directory (or a missing root) is skipped, never
    fatal, and the result is a list, not a generator, so a vanishing directory cannot raise
    halfway through a caller's loop."""
    found: list[Path] = []
    stack = [str(root.expanduser())]
    while stack:
        here = stack.pop()
        try:
            entries = os.scandir(here)
        except OSError:
            continue
        with entries:
            for entry in entries:
                try:
                    is_dir = entry.is_dir(follow_symlinks=False)
                except OSError:
                    continue
                if is_dir:
                    if entry.name not in PRUNE_DIRS:
                        stack.append(entry.path)
                elif entry.name.endswith(".jsonl"):
                    found.append(Path(entry.path))
    return found
