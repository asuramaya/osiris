"""The transcript walk behind sense_liveness: the same files rglob found, without entering the
per-project `memory` directories (half of the ~27,000 directories under the real root)."""
from __future__ import annotations

import os
import time
from pathlib import Path

from src.ingest.transcript_walk import jsonl_files
from src.orchestrator import liveness


def _tree(root: Path) -> None:
    (root / "-home-u-proj" / "memory").mkdir(parents=True)
    (root / "-home-u-proj" / "memory" / "notes.jsonl").write_text("x")      # never a transcript
    (root / "-home-u-proj" / "aaaaaaaa-1111.jsonl").write_text("{}")
    (root / "-home-u-proj" / "aaaaaaaa-1111" / "subagents").mkdir(parents=True)
    (root / "-home-u-proj" / "aaaaaaaa-1111" / "subagents" / "agent-x.jsonl").write_text("{}")
    (root / "-home-u-other-osiris-extract").mkdir()
    (root / "-home-u-other-osiris-extract" / "scratch.jsonl").write_text("{}")
    (root / "top.jsonl").write_text("{}")
    (root / "-home-u-proj" / "readme.md").write_text("no")


def test_it_finds_what_rglob_finds_except_inside_memory_directories(tmp_path: Path) -> None:
    _tree(tmp_path)

    mine = {p.relative_to(tmp_path).as_posix() for p in jsonl_files(tmp_path)}
    theirs = {p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*.jsonl")}

    assert theirs - mine == {"-home-u-proj/memory/notes.jsonl"}
    assert mine <= theirs and len(mine) == 4


def test_it_does_not_follow_a_symlinked_directory_and_skips_an_unreadable_one(
    tmp_path: Path,
) -> None:
    _tree(tmp_path)
    outside = tmp_path.parent / (tmp_path.name + "-outside")
    outside.mkdir()
    (outside / "elsewhere.jsonl").write_text("{}")
    (tmp_path / "link").symlink_to(outside, target_is_directory=True)
    locked = tmp_path / "locked"
    locked.mkdir()
    (locked / "hidden.jsonl").write_text("{}")
    locked.chmod(0)
    try:
        names = {p.name for p in jsonl_files(tmp_path)}
    finally:
        locked.chmod(0o700)

    assert "elsewhere.jsonl" not in names and "hidden.jsonl" not in names
    assert "top.jsonl" in names


def test_sessions_skip_the_extractors_scratch_dir_and_report_each_mtime(tmp_path: Path) -> None:
    _tree(tmp_path)
    stamp = time.time() - 3600
    os.utime(tmp_path / "-home-u-proj" / "aaaaaaaa-1111.jsonl", (stamp, stamp))

    seen = dict(liveness._sessions(tmp_path))

    assert "scratch" not in seen and "notes" not in seen
    assert set(seen) == {"aaaaaaaa-1111", "agent-x", "top"}
    assert abs(seen["aaaaaaaa-1111"].timestamp() - stamp) < 1
    assert liveness._sessions(tmp_path / "missing") == []
