"""gate_hook's touched-file -> test-file resolution — the piece that keeps a per-commit gate
cheap enough to survive contact (Sekhmet's #112 finding: full-suite is 209s under real
concurrency). Pure, IO-only via tmp_path, no git and no subprocess."""
from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import scripts.gate_hook as gate_hook
from scripts.gate_hook import (
    DERIVATION_TRACE_QUESTION,
    _is_receipt_shaped,
    _module_imports,
    _status_word,
    classify_test_files,
    cmd_precommit,
    receipt_shaped_touches,
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
    assert msg.startswith("SKIPPED")  # never a plain "ok" — Thoth DM 2957
    assert "ran 1 clean" in msg
    assert "omitted 3 fixture-only files" in msg
    assert _status_word(ok, msg) == "SKIPPED"
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
    assert msg.startswith("SKIPPED")  # never "no resolvable test files touched" —
    assert "nothing ran" in msg       # that phrase means there was genuinely nothing to run,
    assert "omitted 2 fixture-only files" in msg  # this is an omission, a different thing
    assert _status_word(ok, msg) == "SKIPPED"
    assert not any("pytest" in c[0] for c in calls if c)


def test_run_gates_genuinely_nothing_to_test_is_a_plain_ok(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    """The one case that's honestly a plain pass: nothing was omitted because nothing was
    ever relevant in the first place — must not be confused with a capped skip."""
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, ""))
    results = run_gates(tmp_path, ["docs/DEPLOY.md"])
    ok, msg = results["pytest"]
    assert ok is True
    assert msg == "no resolvable test files touched"
    assert _status_word(ok, msg) == "ok"


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


def test_status_word_never_folds_a_skip_into_a_plain_ok() -> None:
    """Thoth DM 2957: the first two retroactive audits reported "17/18 pass" as real when a
    flat cap silently skipped pytest for hub-module commits and the skip path returned
    ok=True — indistinguishable from a genuine pass. "SKIPPED" must be its own word."""
    assert _status_word(True, "SKIPPED — nothing ran; omitted 12 fixture-only files") \
        == "SKIPPED"
    assert _status_word(True, "SKIPPED (partial) — ran 3 clean, omitted 9 files") == "SKIPPED"


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


# --- _report: a skip is UNVERIFIED, never folded into plain PASS (Thoth DM 2957) --------------

def test_report_marks_a_skip_as_unverified_not_pass(capsys: Any) -> None:
    results = {
        "ruff": (True, ""), "mypy": (True, ""),
        "pytest": (True, "SKIPPED — nothing ran; omitted 5 fixture-only files"),
    }
    all_ok = gate_hook._report("test", results)
    out = capsys.readouterr().out
    assert all_ok is True  # enforcement semantics unchanged -- a skip never refuses
    assert "PASS (UNVERIFIED" in out.splitlines()[0]
    assert "pytest: SKIPPED" in out
    assert "omitted 5 fixture-only files" in out  # the detail is printed, not hidden


def test_report_a_genuine_clean_run_says_plain_pass(capsys: Any) -> None:
    results = {
        "ruff": (True, ""), "mypy": (True, ""),
        "pytest": (True, "[tests/test_a.py]\n1 passed"),
    }
    all_ok = gate_hook._report("test", results)
    out = capsys.readouterr().out
    assert all_ok is True
    assert out.splitlines()[0] == "gate_hook[test]: PASS"


# --- #117 piece (b): the derivation-trace nudge (decision d0ab1b0b, routed msg 2984) ----------

def test_added_line_ranges_parses_a_single_hunk(tmp_path: Path, monkeypatch: Any) -> None:
    diff = "@@ -0,0 +1,3 @@\n+def f():\n+    return {}\n+\n"
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, diff))
    assert gate_hook._added_line_ranges(tmp_path, "x.py") == [(1, 3)]


def test_added_line_ranges_omitted_count_defaults_to_one(tmp_path: Path, monkeypatch: Any) -> None:
    diff = "@@ -5,0 +6 @@\n+a\n"
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, diff))
    assert gate_hook._added_line_ranges(tmp_path, "x.py") == [(6, 6)]


def test_added_line_ranges_a_pure_deletion_contributes_nothing(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    """`+c,0` -- nothing landed on the new-file side at this point, only a deletion."""
    diff = "@@ -5,2 +5,0 @@\n-a\n-b\n"
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, diff))
    assert gate_hook._added_line_ranges(tmp_path, "x.py") == []


def test_added_line_ranges_git_failure_returns_empty(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (False, "fatal: not a repo"))
    assert gate_hook._added_line_ranges(tmp_path, "x.py") == []


def _first_func(src: str) -> ast.FunctionDef | ast.AsyncFunctionDef:
    node = ast.parse(src).body[0]
    assert isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    return node


def test_is_receipt_shaped_true_for_mcp_tool_decorator() -> None:
    assert _is_receipt_shaped(_first_func("@mcp.tool()\ndef f():\n    return {}\n")) is True


def test_is_receipt_shaped_true_for_dict_str_any_return() -> None:
    assert _is_receipt_shaped(_first_func("def f() -> dict[str, Any]:\n    return {}\n")) is True


def test_is_receipt_shaped_false_for_a_plain_function() -> None:
    assert _is_receipt_shaped(_first_func("def f():\n    return 1\n")) is False


def test_is_receipt_shaped_false_for_a_different_dict_shape() -> None:
    """The trigger is the EXACT `dict[str, Any]` shape, not any dict return -- a narrower,
    more precise return type is not the receipt-collapse pattern #117 named."""
    assert _is_receipt_shaped(_first_func("def f() -> dict[str, int]:\n    return {}\n")) is False


def test_is_receipt_shaped_false_for_an_unrelated_decorator() -> None:
    assert _is_receipt_shaped(_first_func("@app.route('/x')\ndef f():\n    return {}\n")) is False


def test_receipt_shaped_touches_flags_a_newly_added_tool_function(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    content = (
        "@mcp.tool()\n"
        "def real_tool() -> dict[str, Any]:\n"
        "    return {}\n"
    )
    _write(tmp_path, "src/pkg/x.py", content)
    diff = "@@ -0,0 +1,3 @@\n+@mcp.tool()\n+def real_tool() -> dict[str, Any]:\n+    return {}\n"
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, diff))
    out = receipt_shaped_touches(tmp_path, ["src/pkg/x.py"])
    assert out == {"src/pkg/x.py": ["real_tool"]}


def test_receipt_shaped_touches_ignores_an_untouched_function_in_the_same_file(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    """A receipt-shaped function that ALREADY EXISTED and was not touched by this diff must
    not be flagged just because the file is in `changed_files` -- only lines the diff itself
    added or modified count."""
    content = (
        "@mcp.tool()\n"
        "def untouched_tool() -> dict[str, Any]:\n"
        "    return {}\n"
        "\n"
        "\n"
        "def helper():\n"
        "    return 1\n"
    )
    _write(tmp_path, "src/pkg/x.py", content)
    # only the helper's body line changed (line 7)
    diff = "@@ -7 +7 @@\n-    return 0\n+    return 1\n"
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, diff))
    out = receipt_shaped_touches(tmp_path, ["src/pkg/x.py"])
    assert out == {}


def test_receipt_shaped_touches_catches_a_decorator_only_addition(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    """Adding `@mcp.tool()` above an ALREADY-EXISTING function touches only the decorator
    line, not `def`'s own line (ast's own lineno behavior) -- must still be caught."""
    content = (
        "@mcp.tool()\n"
        "def now_a_tool() -> dict[str, Any]:\n"
        "    return {}\n"
    )
    _write(tmp_path, "src/pkg/x.py", content)
    diff = "@@ -0,0 +1 @@\n+@mcp.tool()\n"
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, diff))
    out = receipt_shaped_touches(tmp_path, ["src/pkg/x.py"])
    assert out == {"src/pkg/x.py": ["now_a_tool"]}


def test_receipt_shaped_touches_ignores_non_python_files(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, "@@ -0,0 +1 @@\n+x\n"))
    out = receipt_shaped_touches(tmp_path, ["docs/DEPLOY.md"])
    assert out == {}


def test_receipt_shaped_touches_skips_a_deleted_file(tmp_path: Path, monkeypatch: Any) -> None:
    """`changed_files` can name a file this diff DELETES -- nothing left on disk to trace."""
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, "@@ -1 +0,0 @@\n-x\n"))
    out = receipt_shaped_touches(tmp_path, ["src/pkg/gone.py"])
    assert out == {}


def test_precommit_prints_the_derivation_trace_question_when_a_tool_is_touched(
    monkeypatch: Any, capsys: Any,
) -> None:
    monkeypatch.setattr(gate_hook, "changed_files_staged", lambda root=None: ["src/pkg/x.py"])
    monkeypatch.setattr(
        gate_hook, "run_gates",
        lambda root, changed: {"ruff": (True, ""), "mypy": (True, ""), "pytest": (True, "")})
    monkeypatch.setattr(
        gate_hook, "receipt_shaped_touches",
        lambda root, changed: {"src/pkg/x.py": ["real_tool"]})
    assert cmd_precommit(enforce=True) == 0
    out = capsys.readouterr().out
    assert DERIVATION_TRACE_QUESTION in out
    assert "src/pkg/x.py: real_tool" in out


def test_precommit_is_silent_about_derivation_trace_when_nothing_receipt_shaped_touched(
    monkeypatch: Any, capsys: Any,
) -> None:
    monkeypatch.setattr(gate_hook, "changed_files_staged", lambda root=None: ["a.py"])
    monkeypatch.setattr(
        gate_hook, "run_gates",
        lambda root, changed: {"ruff": (True, ""), "mypy": (True, ""), "pytest": (True, "")})
    monkeypatch.setattr(gate_hook, "receipt_shaped_touches", lambda root, changed: {})
    assert cmd_precommit(enforce=True) == 0
    out = capsys.readouterr().out
    assert DERIVATION_TRACE_QUESTION not in out


def test_derivation_trace_question_never_changes_the_verdict_on_a_failing_gate(
    monkeypatch: Any, capsys: Any,
) -> None:
    """The nudge is print-only -- a REAL gate failure still refuses when enforced, with or
    without a receipt-shaped touch alongside it."""
    monkeypatch.setattr(gate_hook, "changed_files_staged", lambda root=None: ["src/pkg/x.py"])
    monkeypatch.setattr(
        gate_hook, "run_gates",
        lambda root, changed: {"ruff": (False, "boom"), "mypy": (True, ""), "pytest": (True, "")})
    monkeypatch.setattr(
        gate_hook, "receipt_shaped_touches",
        lambda root, changed: {"src/pkg/x.py": ["real_tool"]})
    assert cmd_precommit(enforce=True) == 1
    out = capsys.readouterr().out
    assert DERIVATION_TRACE_QUESTION in out
    assert "gate_hook: REFUSED" in out
