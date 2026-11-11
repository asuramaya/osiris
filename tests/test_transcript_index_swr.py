"""The transcript index serves stale and rebuilds in the background: once an index exists, no
caller waits for the walk of the projects directory."""
from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest
from src.orchestrator import mounts


@pytest.fixture(autouse=True)
def _clean() -> None:
    mounts._transcript_index_cache.clear()
    mounts._transcript_index_refreshing.clear()


def _age(root: Path, seconds: float) -> None:
    stamp, index = mounts._transcript_index_cache[str(root)]
    mounts._transcript_index_cache[str(root)] = (stamp - seconds, index)


def _wait_for_refresh() -> None:
    deadline = time.monotonic() + 10
    while mounts._transcript_index_refreshing and time.monotonic() < deadline:
        time.sleep(0.01)
    assert not mounts._transcript_index_refreshing


def test_the_first_call_walks_the_tree_and_a_fresh_one_does_not(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "p").mkdir()
    (tmp_path / "p" / "aaaa.jsonl").write_text("{}\n")
    walks: list[Path] = []
    real = mounts._walk_transcripts
    monkeypatch.setattr(mounts, "_walk_transcripts", lambda r: walks.append(r) or real(r))
    first = mounts._transcript_index(tmp_path)
    assert set(first) == {"aaaa"} and len(walks) == 1
    assert mounts._transcript_index(tmp_path) is first and len(walks) == 1


def test_a_stale_index_is_served_at_once_while_one_thread_rebuilds_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "old.jsonl").write_text("{}\n")
    stale = mounts._transcript_index(tmp_path)
    (tmp_path / "new.jsonl").write_text("{}\n")
    _age(tmp_path, mounts._TRANSCRIPT_INDEX_TTL_SECS + 5)
    gate = threading.Event()
    started: list[str] = []
    real = mounts._walk_transcripts

    def slow(root: Path) -> dict[str, Path]:
        started.append("walk")
        gate.wait(10)
        return real(root)

    monkeypatch.setattr(mounts, "_walk_transcripts", slow)
    began = time.monotonic()
    served = [mounts._transcript_index(tmp_path) for _ in range(5)]
    assert time.monotonic() - began < 1.0               # nobody waited for the walk
    assert all(s is stale for s in served) and "new" not in stale
    for _ in range(100):                                # let the one rebuild begin
        if started:
            break
        time.sleep(0.01)
    assert started == ["walk"]                          # five callers, one rebuild
    gate.set()
    _wait_for_refresh()
    assert set(mounts._transcript_index(tmp_path)) == {"old", "new"}


def test_a_failed_thread_start_keeps_serving_the_stale_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "a.jsonl").write_text("{}\n")
    stale = mounts._transcript_index(tmp_path)
    _age(tmp_path, mounts._TRANSCRIPT_INDEX_TTL_SECS + 5)

    def refuse(*_a: object, **_k: object) -> threading.Thread:
        raise RuntimeError("can't start new thread")

    monkeypatch.setattr(mounts.threading, "Thread", refuse)
    assert mounts._transcript_index(tmp_path) is stale
    assert not mounts._transcript_index_refreshing      # a later call may try again


def test_a_missing_root_is_an_empty_index_not_an_error(tmp_path: Path) -> None:
    assert mounts._transcript_index(tmp_path / "nowhere") == {}
