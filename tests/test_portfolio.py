"""The portfolio lens — the developer's projects, DERIVED.

For each ingested repo it surfaces the terms DISTINCTIVE to it (what it's about) and its
stack — the substrate half of cross-repo cognition. The test that matters: a word every repo
shares is NOT distinctive (it's noise and must fall out with no stoplist), while a word unique
to one repo names it. That's the whole 'never the flat corpus, only the distinctive set' trick.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

from src.actions.core import Actions
from src.ingest.project import ingest_project
from src.orchestrator.compositions import run_spec


def _git(repo: Path, *args: str) -> None:
    env = {**os.environ, "GIT_AUTHOR_NAME": "Dev", "GIT_AUTHOR_EMAIL": "dev@x.io",
           "GIT_COMMITTER_NAME": "Dev", "GIT_COMMITTER_EMAIL": "dev@x.io"}
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, env=env)


def _repo(tmp: Path, name: str, unique: str, ext: str) -> Path:
    """A repo whose commits repeat a UNIQUE word (its identity) and a word SHARED by every
    repo ('refactor', the noise). Two commits each so the unique term clears tf>=2."""
    repo = tmp / name
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Dev")
    _git(repo, "config", "user.email", "dev@x.io")
    (repo / f"main.{ext}").write_text("x")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", f"feat: the {unique} core, a refactor")
    (repo / f"more.{ext}").write_text("y")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", f"fix: {unique} edge case, another refactor")
    return repo


async def test_portfolio_surfaces_distinctive_terms_not_shared_noise(
    actions: Actions, tmp_path: Path
) -> None:
    await ingest_project(actions, str(_repo(tmp_path, "throttle", "frobnicator", "rs")))
    await ingest_project(actions, str(_repo(tmp_path, "caster", "sprocket", "py")))
    await ingest_project(actions, str(_repo(tmp_path, "pointer", "widgetron", "py")))

    res = await run_spec(actions.pool, {"op": "function", "name": "portfolio"}, None)
    rows = {r["repo"]: r for r in next(iter(res["items"].values()))}
    assert set(rows) == {"throttle", "caster", "pointer"}

    # each repo is NAMED by its unique term …
    assert "frobnicator" in rows["throttle"]["about"]
    assert "sprocket" in rows["caster"]["about"]
    assert "widgetron" in rows["pointer"]["about"]
    # … and NONE is characterised by 'refactor' — it's in all 3 repos, so it's noise that the
    # low-document-frequency filter drops for free (no stoplist did this)
    assert not any("refactor" in r["about"] for r in rows.values())
    # the stack is read from file extensions — throttle is the Rust one
    assert "rs" in rows["throttle"]["stack"]
    assert rows["throttle"]["commits"] == 2


async def test_portfolio_scopes_to_requested_repos(actions: Actions, tmp_path: Path) -> None:
    await ingest_project(actions, str(_repo(tmp_path, "keep", "keepsake", "py")))
    await ingest_project(actions, str(_repo(tmp_path, "drop", "dropling", "py")))
    res = await run_spec(
        actions.pool, {"op": "function", "name": "portfolio", "args": {"repos": ["keep"]}}, None)
    rows = next(iter(res["items"].values()))
    assert [r["repo"] for r in rows] == ["keep"]
