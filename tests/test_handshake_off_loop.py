"""The automount path looks a session's transcript up by walking the box's project
directories (13k of them here), which the loop-lag sampler caught holding the event loop
for about 190 ms. Each of the three handshake lookups now runs on a worker thread."""
from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator import handshake


@pytest.mark.parametrize("fn", ["fork_seat", "resumed_job_seat", "compact_seat"])
async def test_the_transcript_lookup_runs_off_the_event_loop(
    fn: str, actions: Actions, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    main = threading.get_ident()
    seen: list[int] = []

    def spy(root: Path, job_dir: str, **kw: Any) -> None:
        seen.append(threading.get_ident())
        return None

    monkeypatch.setattr(handshake, "locate_current_transcript", spy)
    await getattr(handshake, fn)(actions, job_dir="jobs/abcd1234", root=tmp_path)
    assert seen and all(t != main for t in seen)
