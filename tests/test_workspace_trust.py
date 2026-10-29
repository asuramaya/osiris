"""Seat workspaces the harness does not trust: read-only visibility, never a write.

`claude --bg` refuses a directory the user has not trusted and the seat never starts. The
rule (measured on the installed harness): trusted when the directory or any ancestor has
`hasTrustDialogAccepted: true` in ~/.claude.json, whatever its own entry says."""
from __future__ import annotations

import json
from pathlib import Path

from src.orchestrator.readiness import STEP_ORDER, compute_readiness_steps
from src.orchestrator.workspace_trust import is_trusted, untrusted_dirs


def write_config(home: Path, trusted: list[str], untrusted: list[str] = ()) -> None:  # type: ignore[assignment]
    projects = {p: {"hasTrustDialogAccepted": True} for p in trusted}
    projects.update({p: {"hasTrustDialogAccepted": False} for p in untrusted})
    (home / ".claude.json").write_text(json.dumps({"projects": projects}))


def test_a_directory_is_trusted_through_itself_or_any_ancestor(tmp_path: Path) -> None:
    projects = {"/code": {"hasTrustDialogAccepted": True},
                "/code/own": {"hasTrustDialogAccepted": False},
                "/solo": {"hasTrustDialogAccepted": False}}
    assert is_trusted("/code", projects)
    assert is_trusted("/code/new/deeper", projects)      # no entry of its own: inherited
    assert is_trusted("/code/own", projects)             # its own false: parent still wins
    assert not is_trusted("/solo", projects)
    assert not is_trusted("/elsewhere/x", projects)


def test_only_existing_untrusted_directories_are_reported(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    trusted = tmp_path / "trusted"
    (trusted / "child").mkdir(parents=True)
    loose = tmp_path / "loose"
    loose.mkdir()
    write_config(home, [str(trusted)])
    got = untrusted_dirs([str(trusted / "child"), str(loose), str(loose),
                          str(tmp_path / "ghost"), None], home)
    assert got == [str(loose)]


def test_an_unreadable_config_accuses_nobody(tmp_path: Path) -> None:
    loose = tmp_path / "loose"
    loose.mkdir()
    assert untrusted_dirs([str(loose)], tmp_path / "no-home") == []
    (tmp_path / ".claude.json").write_text("not json")
    assert untrusted_dirs([str(loose)], tmp_path) == []


def test_readiness_names_the_exact_command_for_each_untrusted_workspace() -> None:
    steps = compute_readiness_steps(
        soul_key={}, restic_key={}, offload_targets=[], restore_drill_receipts={},
        services_restarted=None, untrusted_workspaces=["/home/a/code/loop"])
    assert [s["key"] for s in steps if s["key"] == "workspace_trust"] == ["workspace_trust"]
    step = next(s for s in steps if s["key"] == "workspace_trust")
    assert step["status"] == "needs_attention"
    assert "workspace not trusted: run claude once in /home/a/code/loop" in step["reason"]
    assert "workspace_trust" in STEP_ORDER
    assert next(s for s in compute_readiness_steps(
        soul_key={}, restic_key={}, offload_targets=[], restore_drill_receipts={},
        services_restarted=None, untrusted_workspaces=[])
        if s["key"] == "workspace_trust")["status"] == "done"
