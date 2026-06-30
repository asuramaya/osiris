"""File substrate — a repo's tree as File nodes (metadata only) with a normalized role.

The role is the blocking key the cross-repo family audit compares on (every repo's `license`,
every repo's `ci`), so analogous files line up without an all-files-all-pairs scan.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

from src.actions.core import Actions
from src.ingest.files import file_role, ingest_files


def test_file_role_maps_known_roles() -> None:
    assert file_role("LICENSE") == "license"
    assert file_role("README.md") == "readme"
    assert file_role(".github/workflows/ci.yml") == "ci"
    assert file_role("Cargo.toml") == "manifest"
    assert file_role("Makefile") == "makefile"
    assert file_role("src/main.rs") == ""              # code files carry no audit role


def _git(repo: Path, *args: str) -> None:
    env = {**os.environ, "GIT_AUTHOR_NAME": "Ada", "GIT_AUTHOR_EMAIL": "ada@x.io",
           "GIT_COMMITTER_NAME": "Ada", "GIT_COMMITTER_EMAIL": "ada@x.io"}
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, env=env)


async def test_ingest_files(actions: Actions, tmp_path: Path) -> None:
    repo = tmp_path / "util"
    repo.mkdir()
    _git(repo, "init", "-q")
    (repo / "README.md").write_text("# util")
    (repo / "LICENSE").write_text("MIT")
    (repo / "main.py").write_text("print('hi')")
    (repo / ".github" / "workflows").mkdir(parents=True)
    (repo / ".github" / "workflows" / "ci.yml").write_text("on: push")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "init")

    res = await ingest_files(actions, str(repo))
    assert res["repo"] == "util" and res["files"] == 4
    p = actions.pool
    assert await p.fetchval("SELECT count(*) FROM objects WHERE type='File'") == 4
    roles = {r["v"] for r in await p.fetch(
        "SELECT value #>> '{}' AS v FROM current_assertions WHERE name='role'")}
    assert roles == {"license", "readme", "ci"}        # main.py has no role
    assert await p.fetchval("SELECT count(*) FROM links WHERE type='in_repo'") == 4
    ec = await p.fetchval("SELECT evidence_class FROM links WHERE type='in_repo' LIMIT 1")
    assert ec == "authoritative_api"                   # the tracked tree is ground truth
    # idempotent — re-ingest adds no File or edge (the gitlog re-ingest lesson)
    again = await ingest_files(actions, str(repo))
    assert again["files"] == 4
    assert await p.fetchval("SELECT count(*) FROM objects WHERE type='File'") == 4
    assert await p.fetchval("SELECT count(*) FROM links WHERE type='in_repo'") == 4


def test_classify_license() -> None:
    from src.ingest.files import classify_license
    assert classify_license("Permission is hereby granted, free of charge, to anyone") == "MIT"
    assert classify_license("Apache License\nVersion 2.0, January 2004") == "Apache-2.0"
    assert classify_license("GNU GENERAL PUBLIC LICENSE\nVersion 3, 29 June 2007") == "GPL-3.0"
    assert classify_license("some random text") == "unknown"


async def test_ingest_records_content_facts(actions: Actions, tmp_path: Path) -> None:
    """Role-bearing files carry a content_hash (identity drift) and, for a license, its TYPE —
    the facts the drift audit compares (the body itself is never stored)."""
    repo = tmp_path / "u"
    repo.mkdir()
    _git(repo, "init", "-q")
    (repo / "LICENSE").write_text("Permission is hereby granted, free of charge, to any person")
    (repo / ".gitignore").write_text("*.pyc\n__pycache__/\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "init")

    await ingest_files(actions, str(repo))
    p = actions.pool
    lt = await p.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE name='license_type'")
    assert lt == "MIT"
    n = await p.fetchval("SELECT count(*) FROM current_assertions WHERE name='content_hash'")
    assert n == 2                                       # license + gitignore (the role files)
