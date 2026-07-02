"""Atomic project ingest — one call brings a repo in WHOLE (history + files + decisions).

The dogfood finding this guards: gitlog / files / decisions were separate steps and a repo
could land half-ingested (osiris had commits but 0 files). `ingest_project` composes all
three; this proves a single call yields the Commit, the File, and the mined Decision.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

from src.actions.core import Actions
from src.ingest.project import ingest_project


def _git(repo: Path, *args: str) -> None:
    env = {**os.environ, "GIT_AUTHOR_NAME": "Ada", "GIT_AUTHOR_EMAIL": "ada@x.io",
           "GIT_COMMITTER_NAME": "Ada", "GIT_COMMITTER_EMAIL": "ada@x.io"}
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, env=env)


async def test_ingest_project_brings_a_repo_in_whole(actions: Actions, tmp_path: Path) -> None:
    repo = tmp_path / "proj"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Ada")
    _git(repo, "config", "user.email", "ada@x.io")
    (repo / "LICENSE").write_text("AGPL-3.0")
    (repo / "main.py").write_text("print('hi')")
    _git(repo, "add", ".")
    # a decision sentence in the body → a mined Decision
    _git(repo, "commit", "-q", "-m", "feat(core): the kernel\n\nWe chose to event-source merges.")

    res = await ingest_project(actions, str(repo))
    assert res["repo"] == "proj"
    assert res["history"]["commits"] == 1
    assert res["files"]["files"] >= 2          # LICENSE + main.py
    assert res["decisions"]["decisions"] >= 1  # the "we chose" sentence

    p = actions.pool
    # all three object kinds landed from ONE call — the atomic guarantee
    assert await p.fetchval("SELECT count(*) FROM objects WHERE type='Commit'") == 1
    assert await p.fetchval("SELECT count(*) FROM objects WHERE type='File'") >= 2
    assert await p.fetchval("SELECT count(*) FROM objects WHERE type='Decision'") >= 1
    assert await p.fetchval("SELECT count(*) FROM objects WHERE type='SoftwareProject'") == 1


async def test_ingest_project_is_idempotent(actions: Actions, tmp_path: Path) -> None:
    repo = tmp_path / "p"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Ada")
    _git(repo, "config", "user.email", "ada@x.io")
    (repo / "a.txt").write_text("1")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "genesis")

    await ingest_project(actions, str(repo))
    await ingest_project(actions, str(repo))  # re-run must not inflate
    p = actions.pool
    assert await p.fetchval("SELECT count(*) FROM objects WHERE type='Commit'") == 1
    assert await p.fetchval("SELECT count(*) FROM links WHERE type='in_repo'") == \
        await p.fetchval("SELECT count(DISTINCT (from_id, to_id)) FROM links WHERE type='in_repo'")
