"""Read-only readers for the at-home list's word-only items: the stopped rollback container and
the scratch folders. They only look (`docker inspect`, `du`); nothing here removes anything."""
from __future__ import annotations

from pathlib import Path
from typing import Any

ROLLBACK_CONTAINER = "osiris-pg-pre-repack"
SCRATCH_PATHS = ("~/code/zzscratch-imh", "~/.claude/projects/-home-asuramaya-code-zzscratch-imh")


def read_rollback_container() -> dict[str, Any] | None:
    """The stopped rollback container as docker reports it, or None when it is absent. Raises when
    docker itself cannot be asked, so that shows as unreadable rather than as done."""
    import subprocess

    proc = subprocess.run(  # noqa: S603 - fixed argv, read-only
        ["docker", "inspect", "-s", "--format",
         "{{.State.Status}}|{{.Config.Image}}|{{.SizeRw}}", ROLLBACK_CONTAINER],
        capture_output=True, text=True, timeout=15, check=False)
    if proc.returncode != 0:
        if "no such" in proc.stderr.lower():
            return None
        raise RuntimeError(proc.stderr.strip() or "docker inspect failed")
    status, image, size = (proc.stdout.strip().split("|") + ["", "", ""])[:3]
    return {"status": status, "image": image, "size": int(size) if size.isdigit() else None}


def read_scratch(paths: tuple[str, ...] = SCRATCH_PATHS) -> list[dict[str, Any]]:
    """Each scratch path: whether it exists and its size on disk (du, so a link is not followed)."""
    import subprocess

    out = []
    for raw in paths:
        path = Path(raw).expanduser()
        if not path.exists() and not path.is_symlink():
            out.append({"path": raw, "exists": False, "size": None})
            continue
        proc = subprocess.run(["du", "-sb", str(path)],  # noqa: S603,S607 - read-only
                              capture_output=True, text=True, timeout=30, check=False)
        head = proc.stdout.split("\t")[0]
        out.append({"path": raw, "exists": True, "size": int(head) if head.isdigit() else None})
    return out
