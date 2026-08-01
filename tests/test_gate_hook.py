"""gate_hook's touched-file -> test-file resolution — the piece that keeps a per-commit gate
cheap enough to survive contact (Sekhmet's #112 finding: full-suite is 209s under real
concurrency). Pure, IO-only via tmp_path, no git and no subprocess."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import scripts.gate_hook as gate_hook
from scripts.gate_hook import (
    _module_imports,
    _status_word,
    classify_test_files,
    cmd_precommit,
    resolve_test_files,
    run_gates,
)


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
    """The AST tier's whole reason to exist: a module with no `test_<name>.py` twin, but
    imported by name into an unrelated test file, must still be found."""
    _write(tmp_path, "src/orchestrator/widget.py")
    _write(tmp_path, "tests/test_something_else.py",
           "from src.orchestrator.widget import build_widget\n")
    out = resolve_test_files(["src/orchestrator/widget.py"], tmp_path)
    assert out == {"tests/test_something_else.py"}


def test_finds_a_package_level_import_of_the_leaf_name(tmp_path: Path) -> None:
    _write(tmp_path, "src/orchestrator/widget.py")
    _write(tmp_path, "tests/test_pkg_import.py",
           "from src.orchestrator import widget\n")
    out = resolve_test_files(["src/orchestrator/widget.py"], tmp_path)
    assert out == {"tests/test_pkg_import.py"}


def test_a_bare_word_in_a_string_literal_is_not_an_import(tmp_path: Path) -> None:
    """Thoth DM 2948's live catch: test_mailbox.py imported an UNRELATED name from the same
    package (mounts, not smoke) and separately contained the bare word "smoke" only inside a
    prose string — the old substring-plus-word-boundary heuristic conflated the two into a
    false match. Same shape reproduced here, must NOT resolve."""
    _write(tmp_path, "src/orchestrator/smoke.py")
    _write(tmp_path, "src/orchestrator/mounts.py")
    _write(tmp_path, "tests/test_mailbox.py",
           'from src.orchestrator import mounts\n\n'
           'def test_x():\n'
           '    open_thread(summary="deploy smoke races the service")\n')
    out = resolve_test_files(["src/orchestrator/smoke.py"], tmp_path)
    assert out == set()


def test_a_comment_mentioning_the_module_name_is_not_an_import(tmp_path: Path) -> None:
    _write(tmp_path, "src/orchestrator/smoke.py")
    _write(tmp_path, "tests/test_unrelated.py",
           "# TODO: no smoke without fire, fix this later\nimport os\n")
    out = resolve_test_files(["src/orchestrator/smoke.py"], tmp_path)
    assert out == set()


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


# --- _module_imports: what a test file actually imports from one specific module -------------

def test_module_imports_finds_names_inside_function_bodies(tmp_path: Path) -> None:
    """Real example, msg 2941: `from src import mcp_server as srv` lives inside a test
    function body, not at module level — the ORIGINAL name is what's collected, not `srv`."""
    f = tmp_path / "t.py"
    f.write_text("def test_x():\n    from src.orchestrator.mounts import save_mount as sm\n"
                  "    sm()\n")
    assert _module_imports(f, "src.orchestrator.mounts") == {"save_mount"}


def test_module_imports_ignores_other_modules(tmp_path: Path) -> None:
    f = tmp_path / "t.py"
    f.write_text("from src.other import thing\n")
    assert _module_imports(f, "src.orchestrator.mounts") == set()


def test_module_imports_unparseable_file_returns_empty_not_a_crash(tmp_path: Path) -> None:
    f = tmp_path / "t.py"
    f.write_text("def broken(:\n")
    assert _module_imports(f, "src.orchestrator.mounts") == set()


# --- classify_test_files: direct vs fixture-only, Khnum's mounts.py refinement (msg 2941) -----

def test_classify_reproduces_khnums_mounts_split(tmp_path: Path) -> None:
    """3 files import ONLY `save_mount` (fixture-only, per his own empirical finding); 1 file
    also imports `rebind_seat` (a real function under test) and must be DIRECT."""
    _write(tmp_path, "src/orchestrator/mounts.py")
    for name in ("x1", "x2", "x3"):
        _write(tmp_path, f"tests/test_{name}.py",
               "from src.orchestrator.mounts import save_mount\n")
    _write(tmp_path, "tests/test_mounts.py",
           "from src.orchestrator.mounts import save_mount, rebind_seat\n")
    direct, fixture_only = classify_test_files(["src/orchestrator/mounts.py"], tmp_path)
    assert direct == {"tests/test_mounts.py"}
    assert fixture_only == {"tests/test_x1.py", "tests/test_x2.py", "tests/test_x3.py"}


def test_classify_a_touched_test_file_is_always_direct(tmp_path: Path) -> None:
    out_direct, out_fixture = classify_test_files(["tests/test_cli.py"], tmp_path)
    assert out_direct == {"tests/test_cli.py"}
    assert out_fixture == set()


def test_classify_with_no_split_needed_everything_is_direct(tmp_path: Path) -> None:
    """No hub-module effect at all (every candidate imports something unique) — nothing
    should land in fixture_only just because a split MECHANISM exists."""
    _write(tmp_path, "src/orchestrator/widget.py")
    _write(tmp_path, "tests/test_widget.py",
           "from src.orchestrator.widget import build_widget\n")
    direct, fixture_only = classify_test_files(["src/orchestrator/widget.py"], tmp_path)
    assert direct == {"tests/test_widget.py"}
    assert fixture_only == set()


# --- run_gates: the fanout cap applies ONLY to the fixture-only tier (msg 2941) ----------------

def test_run_gates_caps_only_the_fixture_only_tier(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(gate_hook, "_PYTEST_FANOUT_CAP", 2)
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, ""))
    _write(tmp_path, "src/pkg/hub.py")
    for name in ("f1", "f2", "f3"):
        _write(tmp_path, f"tests/test_{name}.py", "from src.pkg.hub import common\n")
    _write(tmp_path, "tests/test_direct.py",
           "from src.pkg.hub import common, real_logic\n")
    captured: dict[str, Any] = {}

    class _FakeProc:
        returncode = 0
        stdout = ""
        stderr = ""

    def _fake_subprocess_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
        captured["cmd"] = cmd
        return _FakeProc()

    monkeypatch.setattr(gate_hook.subprocess, "run", _fake_subprocess_run)
    results = run_gates(tmp_path, ["src/pkg/hub.py"])
    ok, msg = results["pytest"]
    assert ok is True
    assert "SKIPPED 3 fixture-only files" in msg
    cmd = captured["cmd"]
    assert "tests/test_direct.py" in cmd
    assert "tests/test_f1.py" not in cmd
    assert "tests/test_f2.py" not in cmd
    assert "tests/test_f3.py" not in cmd


def test_run_gates_all_fixture_only_and_over_cap_skips_pytest_entirely(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    monkeypatch.setattr(gate_hook, "_PYTEST_FANOUT_CAP", 1)
    calls: list[list[str]] = []

    def _fake_run(cmd: list[str], cwd: Path) -> tuple[bool, str]:
        calls.append(cmd)
        return True, ""

    monkeypatch.setattr(gate_hook, "_run", _fake_run)
    _write(tmp_path, "src/pkg/hub.py")
    for name in ("f1", "f2"):
        _write(tmp_path, f"tests/test_{name}.py", "from src.pkg.hub import common\n")
    results = run_gates(tmp_path, ["src/pkg/hub.py"])
    ok, msg = results["pytest"]
    assert ok is True
    assert "no resolvable test files touched" in msg
    assert not any("pytest" in c[0] for c in calls if c)


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


# --- _status_word / timeout-vs-failure (Thoth DM 2948) ----------------------------------------

def test_status_word_distinguishes_timeout_from_failure() -> None:
    assert _status_word(True, "") == "ok"
    assert _status_word(False, "TIMED OUT after 180s under real ambient fleet load") == "TIMEOUT"
    assert _status_word(False, "1 failed, 3 passed") == "FAILED"


def test_run_gates_pytest_timeout_is_reported_distinctly_from_a_failure(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, ""))
    _write(tmp_path, "tests/test_a.py")

    def _raise_timeout(cmd: list[str], **kwargs: Any) -> None:
        raise gate_hook.subprocess.TimeoutExpired(cmd, kwargs.get("timeout", 0))

    monkeypatch.setattr(gate_hook.subprocess, "run", _raise_timeout)
    results = run_gates(tmp_path, ["tests/test_a.py"])
    ok, msg = results["pytest"]
    assert ok is False
    assert msg.startswith("TIMED OUT")
    assert "not a proven code failure" in msg
    assert _status_word(ok, msg) == "TIMEOUT"
