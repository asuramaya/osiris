"""osiris_boot_heal.py: reboot survival, the units half. Never a second installer:
proves this script calls the SAME two sanctioned, already-tested install paths
(_real_install_user_units, _run_install_script) rather than reimplementing
their diff-and-copy logic, and that a failure here never raises past main()."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from scripts.osiris_boot_heal import _heal, main


async def test_heal_calls_both_sanctioned_installers(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    calls: list[str] = []

    async def _fake_install_user_units(repo_root: Path) -> list[str]:
        calls.append(f"install_user_units({repo_root})")
        return ["unit: installed (new) osiris-mcp.service"]

    def _fake_run_install_script(script_rel: str, root: Path) -> str:
        calls.append(f"run_install_script({script_rel}, {root})")
        return f"{script_rel}: ok"

    monkeypatch.setattr("scripts.osiris_boot_heal._real_install_user_units",
                        _fake_install_user_units)
    monkeypatch.setattr("scripts.osiris_boot_heal._run_install_script",
                        _fake_run_install_script)

    notes = await _heal(tmp_path)
    assert calls == [
        f"install_user_units({tmp_path})",
        f"run_install_script(scripts/install_prune_timers.sh, {tmp_path})",
    ]
    assert notes == ["unit: installed (new) osiris-mcp.service",
                     "scripts/install_prune_timers.sh: ok"]


def test_main_returns_0_on_a_clean_heal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    async def _fake_heal(repo_root: Path) -> list[str]:
        return ["all clean"]

    monkeypatch.setattr("scripts.osiris_boot_heal._heal", _fake_heal)
    monkeypatch.setattr("scripts.osiris_boot_heal._find_repo_root", lambda start: tmp_path)

    assert main() == 0
    assert "osiris-boot-heal: all clean" in capsys.readouterr().out


def test_main_never_raises_past_itself_on_a_heal_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    """This script's own module docstring: 'a bug in this SCRIPT can never be the reason
    the fleet fails to boot': a raised exception must degrade to exit 1, never propagate."""
    async def _boom(repo_root: Path) -> list[str]:
        raise RuntimeError("systemctl not found")

    monkeypatch.setattr("scripts.osiris_boot_heal._heal", _boom)
    monkeypatch.setattr("scripts.osiris_boot_heal._find_repo_root", lambda start: tmp_path)

    assert main() == 1
    assert "FAILED" in capsys.readouterr().err


def test_main_falls_back_to_home_code_osiris_when_repo_root_unresolvable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A boot-time invocation's own WorkingDirectory=%h/code/osiris should always resolve
    via git, but a genuinely detached checkout must still try the conventional path rather
    than crash before ever attempting a heal."""
    seen: list[Path] = []

    async def _record(repo_root: Path) -> list[Any]:
        seen.append(repo_root)
        return []

    monkeypatch.setattr("scripts.osiris_boot_heal._find_repo_root", lambda start: None)
    monkeypatch.setattr("scripts.osiris_boot_heal._heal", _record)

    assert main() == 0
    assert seen == [Path.home() / "code" / "osiris"]
