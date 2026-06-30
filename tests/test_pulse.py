"""The heartbeat — the autonomic loop that makes the developer persona come alive.

A pile of lenses only moves when queried; the pulse runs WITHOUT you: it senses a repo whose
HEAD moved, re-ingests it, re-runs the lenses, and records the delta vs the last pulse as
findings — so you return to a "what changed while I was away" digest. It NEVER mutates the repo.
"""
from __future__ import annotations

import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.compositions import run_spec
from src.orchestrator.pulse import pulse

T0 = datetime(2026, 6, 30, 9, 0, tzinfo=UTC)
T1 = datetime(2026, 6, 30, 10, 0, tzinfo=UTC)
T2 = datetime(2026, 6, 30, 11, 0, tzinfo=UTC)


def _git(repo: Path, *args: str) -> None:
    env = {**os.environ, "GIT_AUTHOR_NAME": "Ada", "GIT_AUTHOR_EMAIL": "ada@x.io",
           "GIT_COMMITTER_NAME": "Ada", "GIT_COMMITTER_EMAIL": "ada@x.io"}
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, env=env)


def _status(repo: Path) -> str:
    return subprocess.run(["git", "-C", str(repo), "status", "--porcelain"],
                          check=True, capture_output=True, text=True).stdout


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / "util"
    repo.mkdir()
    _git(repo, "init", "-q")
    (repo / "README.md").write_text("# util")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "feat: genesis")
    return repo


async def test_pulse_senses_baseline_quiet_then_change(actions: Actions, tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    repos = [("util", str(repo))]

    # 1) first pulse — senses the repo, establishes a baseline
    r1 = await pulse(actions, repos, now=T0)
    assert r1["synced"] == ["util"]
    assert any("baseline" in f for f in r1["findings"])

    # 2) nothing changed — the pulse is QUIET (no re-sync, no findings); off-the-clock noise = 0
    r2 = await pulse(actions, repos, now=T1)
    assert r2["synced"] == [] and r2["findings"] == []

    # 3) a new commit lands — the next pulse SENSES it and reports it (what changed while away)
    (repo / "f.py").write_text("x = 1\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "feat(core): add x")
    r3 = await pulse(actions, repos, now=T2)
    assert r3["synced"] == ["util"]
    assert any("1 new commit in util" in f for f in r3["findings"])

    # the digest surface — the morning prosthesis, newest pulse first
    res = await run_spec(actions.pool, {"op": "function", "name": "pulse",
                                        "args": {"last": 3}}, None)
    feed = next(iter(res["items"].values()))
    assert any("1 new commit in util" in row["finding"] for row in feed)
    assert any("baseline" in row["finding"] for row in feed)


async def test_pulse_never_writes_to_the_repo(actions: Actions, tmp_path: Path) -> None:
    """The hard line: the heartbeat reads + tells, never mutates your repo (no dirty tree)."""
    repo = _repo(tmp_path)
    await pulse(actions, [("util", str(repo))], now=T0)
    assert _status(repo) == ""                   # working tree untouched by the pulse
