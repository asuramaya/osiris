"""Wake GC — the victim classes are pure functions over a projects tree."""
from __future__ import annotations

import json
import time
from pathlib import Path

from scripts.osiris_wake_gc import find_victims

_OLD = time.time() - 48 * 3600


def _transcript(path: Path, *, spoke: bool, mtime: float = _OLD) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps({"type": "user", "message": "hi"})]
    if spoke:
        lines.append(json.dumps({"type": "assistant", "message": {"content": []}}))
    path.write_text("\n".join(lines) + "\n")
    import os

    os.utime(path, (mtime, mtime))
    return path


def test_extract_transcripts_are_victims_even_though_the_extractor_spoke(
        tmp_path: Path) -> None:
    """Every miner tick's `claude -p` call leaves a transcript nothing will ever read
    (the miner refuses to mine its own instrument) — old ones are litter, ~145/day."""
    litter = _transcript(tmp_path / "-home-x-osiris-extract" / "a.jsonl", spoke=True)
    fresh = _transcript(tmp_path / "-home-x-osiris-extract" / "b.jsonl",
                        spoke=True, mtime=time.time())
    alive = _transcript(tmp_path / "-home-x-code-osiris" / "c.jsonl", spoke=True)
    zombie = _transcript(tmp_path / "-home-x-code-osiris" / "d.jsonl", spoke=False)

    zero_turn, extract = find_victims(tmp_path, time.time() - 24 * 3600)
    assert extract == [litter]  # fresh extractor output survives (a tick may be mid-call)
    assert zero_turn == [zombie]
    assert alive not in zero_turn and fresh not in extract


def test_subagent_transcripts_are_never_victims(tmp_path: Path) -> None:
    """Sidechain trees live under <session>/subagents/ — they belong to their parent."""
    _transcript(tmp_path / "-home-x-code-osiris" / "subagents" / "agent-1.jsonl",
                spoke=False)
    zero_turn, extract = find_victims(tmp_path, time.time())
    assert zero_turn == [] and extract == []
