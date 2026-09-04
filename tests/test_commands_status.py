"""commands_status — the install-verification twin of gate_hook.hook_status/push_guard.
hook_status (#204, Thoth ruling msg 6918, decision 012b36fb). CLAUDE_COMMANDS_DIR overrides
the machine target so these never touch the real ~/.claude/commands."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

from scripts.commands_status import REPO_ROOT, commands_status


def test_commands_status_reports_source_missing_off_a_repo_with_no_commands_dir(
    tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.setenv("CLAUDE_COMMANDS_DIR", str(tmp_path / "target"))
    assert "SOURCE MISSING" in commands_status(tmp_path / "no-such-repo")


def test_commands_status_reports_empty_source(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("CLAUDE_COMMANDS_DIR", str(tmp_path / "target"))
    (tmp_path / "commands").mkdir()
    assert "no *.md files" in commands_status(tmp_path)


def test_commands_status_reports_not_installed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("CLAUDE_COMMANDS_DIR", str(tmp_path / "target"))
    (tmp_path / "commands").mkdir()
    (tmp_path / "commands" / "seat.md").write_text("seat doc\n")
    assert "NOT INSTALLED" in commands_status(tmp_path)


def test_commands_status_reports_missing_and_stale_individually(
    tmp_path: Path, monkeypatch,
) -> None:
    target = tmp_path / "target"
    monkeypatch.setenv("CLAUDE_COMMANDS_DIR", str(target))
    source = tmp_path / "commands"
    source.mkdir()
    (source / "seat.md").write_text("seat doc\n")
    (source / "fleet.md").write_text("fleet doc\n")
    target.mkdir()
    (target / "seat.md").write_text("seat doc OLD\n")  # stale, not missing
    status = commands_status(tmp_path)
    assert "OUT OF SYNC" in status
    assert "missing: fleet.md" in status
    assert "stale: seat.md" in status


def test_commands_status_reports_installed_and_current(tmp_path: Path, monkeypatch) -> None:
    target = tmp_path / "target"
    monkeypatch.setenv("CLAUDE_COMMANDS_DIR", str(target))
    source = tmp_path / "commands"
    source.mkdir()
    (source / "seat.md").write_text("seat doc\n")
    target.mkdir()
    (target / "seat.md").write_text("seat doc\n")
    assert commands_status(tmp_path) == "slash commands: 1 installed and current — " + str(target)


def test_commands_status_is_accurate_on_the_real_osiris_checkout() -> None:
    """The real, live installed state — not a synthetic repo — proving the actual house
    convention (tracked commands/*.md + scripts/install_commands.sh) round-trips, the same
    discipline test_gate_hook_install.py's own final test uses."""
    status = commands_status(REPO_ROOT)
    assert status.startswith("slash commands:")
    assert "SOURCE MISSING" not in status


def test_install_commands_script_is_idempotent_and_copies_real_changes(
    tmp_path: Path,
) -> None:
    """Real subprocess run of scripts/install_commands.sh, same discipline
    test_gate_hook_install.py's own tests use for install_gate_hook.sh — a synthetic repo
    with its own commands/ so this never touches the real machine ~/.claude/commands."""
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True, capture_output=True)
    (repo / "commands").mkdir()
    (repo / "commands" / "seat.md").write_text("seat doc v1\n")
    target = tmp_path / "target"
    script = REPO_ROOT / "scripts" / "install_commands.sh"

    env_first = {**os.environ, "CLAUDE_COMMANDS_DIR": str(target)}
    out1 = subprocess.run(["sh", str(script)], cwd=repo, check=True, capture_output=True,
                          text=True, env=env_first)
    assert "1 installed/updated, 0 already current" in out1.stdout
    assert (target / "seat.md").read_text() == "seat doc v1\n"

    out2 = subprocess.run(["sh", str(script)], cwd=repo, check=True, capture_output=True,
                          text=True, env=env_first)
    assert "0 installed/updated, 1 already current" in out2.stdout

    (repo / "commands" / "seat.md").write_text("seat doc v2\n")
    out3 = subprocess.run(["sh", str(script)], cwd=repo, check=True, capture_output=True,
                          text=True, env=env_first)
    assert "1 installed/updated, 0 already current" in out3.stdout
    assert (target / "seat.md").read_text() == "seat doc v2\n"
