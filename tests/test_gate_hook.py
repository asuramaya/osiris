"""gate_hook's touched-file -> test-file resolution — the piece that keeps a per-commit gate
cheap enough to survive contact (Sekhmet's #112 finding: full-suite is 209s under real
concurrency). Pure, IO-only via tmp_path, no git and no subprocess."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import scripts.gate_hook as gate_hook
from scripts.gate_hook import cmd_precommit, resolve_test_files, run_gates


def _write(root: Path, rel: str, content: str = "") -> None:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content)


def test_resolves_by_exact_basename_match(tmp_path: Path) -> None:
    _write(tmp_path, "src/orchestrator/smoke.py")
    _write(tmp_path, "tests/test_smoke.py", "from src.orchestrator.smoke import smoke_chrome")
    out = resolve_test_files(["src/orchestrator/smoke.py"], tmp_path)
    assert out == {"tests/test_smoke.py"}


def test_a_touched_test_file_runs_itself(tmp_path: Path) -> None:
    _write(tmp_path, "tests/test_cli.py")
    out = resolve_test_files(["tests/test_cli.py"], tmp_path)
    assert out == {"tests/test_cli.py"}


def test_finds_a_cross_referencing_test_with_no_matching_basename(tmp_path: Path) -> None:
    """The grep tier's whole reason to exist: a module with no `test_<name>.py` twin, but
    imported by name into an unrelated test file, must still be found."""
    _write(tmp_path, "src/orchestrator/widget.py")
    _write(tmp_path, "tests/test_something_else.py",
           "from src.orchestrator.widget import build_widget\n")
    out = resolve_test_files(["src/orchestrator/widget.py"], tmp_path)
    assert out == {"tests/test_something_else.py"}


def test_a_file_with_no_resolvable_test_returns_empty(tmp_path: Path) -> None:
    _write(tmp_path, "src/orchestrator/untested.py")
    _write(tmp_path, "tests/test_unrelated.py", "import os\n")
    out = resolve_test_files(["src/orchestrator/untested.py"], tmp_path)
    assert out == set()


def test_non_src_non_test_files_are_ignored(tmp_path: Path) -> None:
    _write(tmp_path, "tests/test_widget.py", "")
    out = resolve_test_files(["deploy/osiris-mcp.service", "docs/DEPLOY.md"], tmp_path)
    assert out == set()


def test_multiple_touched_files_union_their_resolved_tests(tmp_path: Path) -> None:
    _write(tmp_path, "tests/test_a.py")
    _write(tmp_path, "tests/test_b.py")
    out = resolve_test_files(["tests/test_a.py", "tests/test_b.py"], tmp_path)
    assert out == {"tests/test_a.py", "tests/test_b.py"}


def test_a_module_import_without_from_is_also_found(tmp_path: Path) -> None:
    _write(tmp_path, "src/orchestrator/widget.py")
    _write(tmp_path, "tests/test_other.py", "import src.orchestrator.widget as w\n")
    out = resolve_test_files(["src/orchestrator/widget.py"], tmp_path)
    assert out == {"tests/test_other.py"}


def test_missing_tests_dir_returns_empty_not_a_crash(tmp_path: Path) -> None:
    _write(tmp_path, "src/orchestrator/widget.py")
    out = resolve_test_files(["src/orchestrator/widget.py"], tmp_path)
    assert out == set()


# --- run_gates: the fanout cap, a hub module measured live (mounts.py: 37 files) -------------

def test_run_gates_skips_pytest_past_the_fanout_cap(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    monkeypatch.setattr(gate_hook, "_PYTEST_FANOUT_CAP", 2)
    calls: list[list[str]] = []

    def _fake_run(cmd: list[str], cwd: Path) -> tuple[bool, str]:
        calls.append(cmd)
        return True, ""

    monkeypatch.setattr(gate_hook, "_run", _fake_run)
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    for name in ("a", "b", "c"):
        (tests_dir / f"test_{name}.py").write_text("")
    changed = [f"tests/test_{n}.py" for n in ("a", "b", "c")]
    results = run_gates(tmp_path, changed)
    ok, msg = results["pytest"]
    assert ok is True
    assert "SKIPPED" in msg and "3 test files resolved" in msg
    assert not any("pytest" in c[0] for c in calls if c)  # never actually invoked


def test_run_gates_reports_no_resolvable_tests_distinctly(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, ""))
    results = run_gates(tmp_path, ["docs/DEPLOY.md"])
    assert results["pytest"] == (True, "no resolvable test files touched")


def test_run_gates_pytest_invocation_passes_the_xdist_cap(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    """msg 2919: this gate can fire from multiple concurrent agents' commits at once, so it
    must never inherit an uncapped `-n auto` — it passes its own fixed, small `-n` always."""
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, ""))
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_a.py").write_text("")
    captured: dict[str, Any] = {}

    class _FakeProc:
        returncode = 0
        stdout = ""
        stderr = ""

    def _fake_subprocess_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
        captured["cmd"] = cmd
        return _FakeProc()

    monkeypatch.setattr(gate_hook.subprocess, "run", _fake_subprocess_run)
    results = run_gates(tmp_path, ["tests/test_a.py"])
    assert results["pytest"][0] is True
    cmd = captured["cmd"]
    assert "-n" in cmd
    assert cmd[cmd.index("-n") + 1] == str(gate_hook._PYTEST_XDIST_CAP)


# --- cmd_precommit: the enforce/dry-run branches ----------------------------------------------

def test_precommit_passes_clean_regardless_of_enforce(monkeypatch: Any) -> None:
    monkeypatch.setattr(gate_hook, "changed_files_staged", lambda root=None: ["a.py"])
    monkeypatch.setattr(
        gate_hook, "run_gates",
        lambda root, changed: {"ruff": (True, ""), "mypy": (True, ""), "pytest": (True, "")})
    assert cmd_precommit(enforce=False) == 0
    assert cmd_precommit(enforce=True) == 0


def test_precommit_lets_a_failure_through_when_not_enforced(monkeypatch: Any) -> None:
    monkeypatch.setattr(gate_hook, "changed_files_staged", lambda root=None: ["a.py"])
    monkeypatch.setattr(
        gate_hook, "run_gates",
        lambda root, changed: {"ruff": (False, "boom"), "mypy": (True, ""), "pytest": (True, "")})
    assert cmd_precommit(enforce=False) == 0


def test_precommit_refuses_a_failure_when_enforced(monkeypatch: Any) -> None:
    monkeypatch.setattr(gate_hook, "changed_files_staged", lambda root=None: ["a.py"])
    monkeypatch.setattr(
        gate_hook, "run_gates",
        lambda root, changed: {"ruff": (False, "boom"), "mypy": (True, ""), "pytest": (True, "")})
    assert cmd_precommit(enforce=True) == 1


def test_precommit_nothing_staged_is_a_clean_noop(monkeypatch: Any) -> None:
    monkeypatch.setattr(gate_hook, "changed_files_staged", lambda root=None: [])

    def _unreachable(root: Path, changed: list[str]) -> dict[str, tuple[bool, str]]:
        raise AssertionError("must never run gates with nothing staged")

    monkeypatch.setattr(gate_hook, "run_gates", _unreachable)
    assert cmd_precommit(enforce=True) == 0
