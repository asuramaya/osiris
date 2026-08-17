"""Memory profiling seam (thread e6fd3772 piece 2) — inert unless explicitly armed, so the
two real production daemons never pay for it by accident.
"""
from __future__ import annotations

import json
import os
import signal
import tracemalloc
from pathlib import Path

from src import memprofile


def test_maybe_start_is_a_noop_without_the_env_var(monkeypatch) -> None:
    monkeypatch.delenv("OSIRIS_PROFILE_MEMORY", raising=False)
    was_tracing = tracemalloc.is_tracing()
    if was_tracing:
        tracemalloc.stop()
    try:
        memprofile.maybe_start()
        assert not tracemalloc.is_tracing()
    finally:
        if was_tracing:
            tracemalloc.start()


def test_maybe_start_arms_tracemalloc_when_enabled(monkeypatch) -> None:
    monkeypatch.setenv("OSIRIS_PROFILE_MEMORY", "1")
    was_tracing = tracemalloc.is_tracing()
    try:
        memprofile.maybe_start()
        assert tracemalloc.is_tracing()
    finally:
        if not was_tracing:
            tracemalloc.stop()


def test_dump_writes_top_n_json_on_sigusr1(monkeypatch, tmp_path: Path) -> None:
    dump_path = tmp_path / "memtop.json"
    monkeypatch.setenv("OSIRIS_PROFILE_MEMORY", "1")
    monkeypatch.setenv("OSIRIS_PROFILE_DUMP_PATH", str(dump_path))
    was_tracing = tracemalloc.is_tracing()
    try:
        memprofile.maybe_start()
        # allocate something durable so the snapshot has at least one real entry
        _hold = [object() for _ in range(1000)]  # noqa: F841
        os.kill(os.getpid(), signal.SIGUSR1)
        payload = json.loads(dump_path.read_text())
        assert payload["pid"] == os.getpid()
        assert isinstance(payload["top"], list) and len(payload["top"]) > 0
        top0 = payload["top"][0]
        assert set(top0) == {"file", "line", "size_kb", "count"}
    finally:
        signal.signal(signal.SIGUSR1, signal.SIG_DFL)
        if not was_tracing:
            tracemalloc.stop()
