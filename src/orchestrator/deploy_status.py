"""deploy_status — THE SETTINGS PANE's own BOX section (Thoth mail 13350, piece 5):
"deploy snapshot sha vs deployed.sha" had no read door at all before this. Two facts,
both read live off the filesystem, never fabricated:

  running_sha        the git sha THIS repo checkout (wherever the API process's own
                      source lives) currently has checked out — `git rev-parse HEAD`
                      in the directory this module's own file lives under.
  deploy_snapshot_sha the git sha scripts/update_deploy_snapshot.sh's own pinned
                      worktree is checked out to — same `OSIRIS_DEPLOY_SNAPSHOT_DIR`
                      env override / `~/.local/share/osiris/deployed` default that
                      script itself uses, so a test points both at the same tmp_path
                      sandbox a real box would use production paths for.

Both are UNAVAILABLE, never fabricated, the moment `git` fails or the path doesn't
exist (no snapshot ever pinned, a dev worktree, CI) — same "unavailable, not silent"
law every other degrading section in this house holds
(`compositions._fn_fleet_live_agents`, `_fn_backup_status`'s own per-section try/except).

Read-only, no writes — the snapshot's own write path stays `osiris deploy`'s alone
(Thoth ruling msg 6949: deploy is the one sanctioned hand that writes machine files)."""
from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

_SNAPSHOT_DIR_ENV = "OSIRIS_DEPLOY_SNAPSHOT_DIR"
_DEFAULT_SNAPSHOT_DIR = "~/.local/share/osiris/deployed"


def _snapshot_dir() -> Path:
    override = os.environ.get(_SNAPSHOT_DIR_ENV)
    return Path(override).expanduser() if override else Path(_DEFAULT_SNAPSHOT_DIR).expanduser()


def _running_repo_root() -> Path:
    # this file's own location: src/orchestrator/deploy_status.py -> repo root two up.
    return Path(__file__).resolve().parents[2]


async def _git_head_sha(repo_dir: Path) -> str | None:
    if not repo_dir.is_dir():  # noqa: ASYNC240 — a stat() on a machine-managed path, negligible
        return None
    try:
        proc = await asyncio.create_subprocess_exec(
            "git", "-C", str(repo_dir), "rev-parse", "HEAD",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=5.0)
    except (OSError, TimeoutError):
        return None
    if proc.returncode != 0:
        return None
    sha = out.decode().strip()
    return sha or None


async def get_deploy_status() -> dict[str, Any]:
    """Both facts, independently degrading — a missing snapshot worktree (never yet
    pinned) never blanks `running_sha`, and vice versa."""
    running_sha, snapshot_sha = await asyncio.gather(
        _git_head_sha(_running_repo_root()), _git_head_sha(_snapshot_dir()))
    return {
        "running_sha": running_sha,
        "deploy_snapshot_sha": snapshot_sha,
        "in_sync": (running_sha is not None and running_sha == snapshot_sha),
    }
