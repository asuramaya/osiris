"""locate_current_transcript finds one session's file without stat()ing every transcript, and
its cached directory listings never hide a file that was added or removed."""
from __future__ import annotations

import os
import time
from pathlib import Path

from src.ingest import sessions
from src.ingest.sessions import locate_current_transcript


def _touch(path: Path, age_secs: float = 0.0) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}\n")
    t = time.time() - age_secs
    os.utime(path, (t, t))
    return path


def test_the_freshest_anchored_copy_wins_across_project_directories(tmp_path: Path) -> None:
    old = _touch(tmp_path / "-a-office" / "abcd1234-0000-4000-8000-000000000000.jsonl", 600)
    new = _touch(tmp_path / "-b-tree" / "abcd1234-0000-4000-8000-000000000000.jsonl", 5)
    _touch(tmp_path / "-c-other" / "ffff9999-0000-4000-8000-000000000000.jsonl", 1)
    assert locate_current_transcript(tmp_path, "jobs/abcd1234", anchored_only=True) == new
    assert old.exists()


def test_a_file_added_after_the_first_lookup_is_found_on_the_next(tmp_path: Path) -> None:
    first = _touch(tmp_path / "-a" / "abcd1234-0000-4000-8000-000000000000.jsonl", 600)
    assert locate_current_transcript(tmp_path, "jobs/abcd1234", anchored_only=True) == first
    second = _touch(tmp_path / "-a" / "abcd1234-1111-4000-8000-000000000000.jsonl", 1)
    assert locate_current_transcript(tmp_path, "jobs/abcd1234", anchored_only=True) == second


def test_a_removed_file_or_directory_stops_being_found(tmp_path: Path) -> None:
    keep = _touch(tmp_path / "-a" / "abcd1234-0000-4000-8000-000000000000.jsonl", 600)
    gone = _touch(tmp_path / "-b" / "abcd1234-0000-4000-8000-000000000000.jsonl", 1)
    assert locate_current_transcript(tmp_path, "jobs/abcd1234", anchored_only=True) == gone
    gone.unlink()
    assert locate_current_transcript(tmp_path, "jobs/abcd1234", anchored_only=True) == keep
    keep.unlink()
    keep.parent.rmdir()
    assert locate_current_transcript(tmp_path, "jobs/abcd1234", anchored_only=True) is None


def test_no_anchor_means_none_when_anchored_only_and_the_newest_otherwise(
    tmp_path: Path,
) -> None:
    newest = _touch(tmp_path / "-a" / "11112222-0000-4000-8000-000000000000.jsonl", 1)
    _touch(tmp_path / "-a" / "33334444-0000-4000-8000-000000000000.jsonl", 900)
    assert locate_current_transcript(tmp_path, "jobs/deadbeef", anchored_only=True) is None
    assert locate_current_transcript(tmp_path, "jobs/deadbeef") == newest
    assert locate_current_transcript(tmp_path / "missing", "jobs/deadbeef") is None


def test_only_stat_the_few_matches_not_every_transcript(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    for i in range(30):
        _touch(tmp_path / f"-d{i}" / f"{i:08x}-0000-4000-8000-000000000000.jsonl")
    target = _touch(tmp_path / "-d3" / "abcd1234-0000-4000-8000-000000000000.jsonl")
    sessions._dir_listing_cache.clear()
    stats: list[str] = []
    real = Path.is_file

    def _spy(self: Path) -> bool:
        stats.append(self.name)
        return real(self)

    monkeypatch.setattr(Path, "is_file", _spy)
    assert locate_current_transcript(tmp_path, "jobs/abcd1234", anchored_only=True) == target
    assert stats == [target.name]
