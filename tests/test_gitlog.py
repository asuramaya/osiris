"""Git-history ingest — Osiris modelling a repository (and, live, itself).

Proves the collector pattern generalizes to a non-OSINT domain: a git history becomes
SoftwareProject / Commit / Person(dev) objects + authored_by / in_repo / follows links,
graded AUTHORITATIVE_API, all through the same Actions waist. Hermetic: a throwaway repo.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

from src.actions.core import Actions
from src.ingest.gitlog import ingest_repo, parse_git_log


def test_parse_git_log_is_tolerant() -> None:
    raw = ("h1\x1fAda\x1fada@x.io\x1f2026-01-01T00:00:00+00:00\x1f\x1fgenesis\x1e"
           "h2\x1fAda\x1fada@x.io\x1f2026-01-02T00:00:00+00:00\x1fh1\x1fsecond\x1e")
    commits = parse_git_log(raw)
    assert [c.subject for c in commits] == ["genesis", "second"]
    assert commits[0].parents == [] and commits[1].parents == ["h1"]


def _git(repo: Path, *args: str) -> None:
    env = {**os.environ, "GIT_AUTHOR_NAME": "Ada", "GIT_AUTHOR_EMAIL": "ada@x.io",
           "GIT_COMMITTER_NAME": "Ada", "GIT_COMMITTER_EMAIL": "ada@x.io"}
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, env=env)


async def test_ingests_a_repo_history(actions: Actions, tmp_path: Path) -> None:
    repo = tmp_path / "proj"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Ada")
    _git(repo, "config", "user.email", "ada@x.io")
    (repo / "a.txt").write_text("1")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "genesis")
    (repo / "a.txt").write_text("2")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "second commit")

    res = await ingest_repo(actions, str(repo))
    assert res == {"repo": "proj", "commits": 2, "developers": 1}

    p = actions.pool
    # the SoftwareProject + 2 Commits + 1 dev Person exist
    assert await p.fetchval("SELECT count(*) FROM objects WHERE type='Commit'") == 2
    assert await p.fetchval(
        "SELECT count(*) FROM objects WHERE type='Person' AND canonical='dev:ada@x.io'"
    ) == 1
    # the genesis commit (no parent) is flagged
    assert await p.fetchval("SELECT count(*) FROM current_assertions WHERE name='genesis'") == 1
    # the DAG + authorship are edges, graded authoritative
    assert await p.fetchval("SELECT count(*) FROM links WHERE type='authored_by'") == 2
    assert await p.fetchval("SELECT count(*) FROM links WHERE type='follows'") == 1
    ec = await p.fetchval("SELECT evidence_class FROM links WHERE type='in_repo' LIMIT 1")
    assert ec == "authoritative_api"


async def test_repo_name_survives_conventional_commits(actions: Actions, tmp_path: Path) -> None:
    """Regression: the property loop used to reuse `name`, shadowing the repo name, so a repo
    of Conventional Commits returned repo='summary' (the last property key). The node was fine
    but the return was wrong — and multi-repo ingest leans on that return."""
    repo = tmp_path / "proj2"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Ada")
    _git(repo, "config", "user.email", "ada@x.io")
    (repo / "f").write_text("x")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "feat(core): the thing")   # conventional → triggers the loop
    res = await ingest_repo(actions, str(repo))
    assert res["repo"] == "proj2"                                # not "summary"


async def test_ingest_is_idempotent(actions: Actions, tmp_path: Path) -> None:
    repo = tmp_path / "p"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Ada")
    _git(repo, "config", "user.email", "ada@x.io")
    (repo / "f").write_text("x")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "only")
    (repo / "g").write_text("y")          # a second commit, so there's a follows edge to dedup
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "feat(x): two")
    for _ in range(3):  # re-ingesting the same history must not fork the graph OR its edges
        await ingest_repo(actions, str(repo))
    p = actions.pool
    assert await p.fetchval("SELECT count(*) FROM objects WHERE type='Commit'") == 2
    # the regression: links are append-only, so a naive re-ingest used to triple every edge
    assert await p.fetchval("SELECT count(*) FROM links WHERE type='authored_by'") == 2
    assert await p.fetchval("SELECT count(*) FROM links WHERE type='in_repo'") == 2
    assert await p.fetchval("SELECT count(*) FROM links WHERE type='follows'") == 1


def test_parse_subject_extracts_conventional_commit() -> None:
    """The structure that makes the log queryable memory: type + scope + summary."""
    from src.ingest.gitlog import parse_subject
    assert parse_subject("feat(composer): W5 — author a Room") == {
        "change_type": "feat", "scope": "composer", "summary": "W5 — author a Room"}
    assert parse_subject("docs: reflect the vision") == {
        "change_type": "docs", "summary": "reflect the vision"}
    assert parse_subject("Phase 0: schema") == {}  # not conventional → no false structure


async def test_commit_carries_type_scope_and_rationale(actions: Actions, tmp_path: Path) -> None:
    """A commit becomes a lightweight DECISION record: its type/scope are groupable and its
    body is the rationale (why, not just what) — project memory, queryable."""
    repo = tmp_path / "p2"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Ada")
    _git(repo, "config", "user.email", "ada@x.io")
    (repo / "f").write_text("1")
    _git(repo, "add", ".")
    msg = "feat(engine): close the op set\n\nwe chose a closed set + a Function hatch"
    _git(repo, "commit", "-q", "-m", msg)

    await ingest_repo(actions, str(repo))
    p = actions.pool

    async def prop(name: str) -> str | None:
        return await p.fetchval(
            "SELECT value #>> '{}' FROM current_assertions WHERE name=$1", name)

    assert await prop("change_type") == "feat"
    assert await prop("scope") == "engine"
    assert "Function hatch" in (await prop("rationale") or "")
