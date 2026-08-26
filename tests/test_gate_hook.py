"""gate_hook's touched-file -> test-file resolution — the piece that keeps a per-commit gate
cheap enough to survive contact (Sekhmet's #112 finding: full-suite is 209s under real
concurrency). Pure, IO-only via tmp_path, no git and no subprocess."""
from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import scripts.gate_hook as gate_hook
from scripts.gate_hook import (
    _RATCHET_TEST_NODEID,
    DERIVATION_TRACE_QUESTION,
    _is_merge_context,
    _is_receipt_shaped,
    _module_imports,
    _module_level_import_names,
    _own_scope_local_imports,
    _pytest_sole_failure_is_ratchet_ceiling,
    _reads_name_before,
    _status_word,
    classify_test_files,
    cmd_precommit,
    receipt_shaped_touches,
    resolve_test_files,
    run_gates,
    shadow_before_use_violations,
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
    monkeypatch.setattr(gate_hook, "_staged_diff_digest", lambda root=None: "same")
    assert cmd_precommit(enforce=False) == 0
    assert cmd_precommit(enforce=True) == 0


def test_precommit_lets_a_failure_through_when_not_enforced(monkeypatch: Any) -> None:
    monkeypatch.setattr(gate_hook, "changed_files_staged", lambda root=None: ["a.py"])
    monkeypatch.setattr(
        gate_hook, "run_gates",
        lambda root, changed: {"ruff": (False, "boom"), "mypy": (True, ""), "pytest": (True, "")})
    monkeypatch.setattr(gate_hook, "_staged_diff_digest", lambda root=None: "same")
    assert cmd_precommit(enforce=False) == 0


def test_precommit_refuses_a_failure_when_enforced(monkeypatch: Any) -> None:
    monkeypatch.setattr(gate_hook, "changed_files_staged", lambda root=None: ["a.py"])
    monkeypatch.setattr(
        gate_hook, "run_gates",
        lambda root, changed: {"ruff": (False, "boom"), "mypy": (True, ""), "pytest": (True, "")})
    monkeypatch.setattr(gate_hook, "_staged_diff_digest", lambda root=None: "same")
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


# --- run_gates: the f1f8ad62 tolerance remedy — retry-once-on-timeout, never blindness --------

def test_run_gates_retries_once_on_timeout_and_reports_it_distinctly(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    """A single timeout is not proof of a real problem (f1f8ad62, ruling f61cad1b) — but a
    pass that only happened on retry must NEVER read the same as a clean first-try "ok"."""
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, ""))
    _write(tmp_path, "tests/test_a.py")
    calls: list[str] = []

    class _FakeProc:
        returncode = 0
        stdout = "1 passed"
        stderr = ""

    def _first_times_out_then_passes(cmd: list[str], **kwargs: Any) -> _FakeProc:
        calls.append("call")
        if len(calls) == 1:
            raise gate_hook.subprocess.TimeoutExpired(cmd, kwargs.get("timeout", 0))
        return _FakeProc()

    monkeypatch.setattr(gate_hook.subprocess, "run", _first_times_out_then_passes)
    results = run_gates(tmp_path, ["tests/test_a.py"])
    ok, msg = results["pytest"]
    assert len(calls) == 2  # the retry actually ran a second attempt, not a no-op
    assert ok is True
    assert msg.startswith("PASSED ON RETRY")  # never folded into a plain "ok"
    assert "timed out" in msg
    assert "second attempt" in msg
    assert _status_word(ok, msg) == "PASSED-ON-RETRY"


def test_run_gates_timing_out_twice_still_refuses_unconditionally(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    """The retry is a SINGLE extra chance, not infinite tolerance — two timeouts in a row is
    a stronger signal than one and must still refuse (never becomes blindness)."""
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, ""))
    _write(tmp_path, "tests/test_a.py")
    calls: list[str] = []

    def _always_times_out(cmd: list[str], **kwargs: Any) -> None:
        calls.append("call")
        raise gate_hook.subprocess.TimeoutExpired(cmd, kwargs.get("timeout", 0))

    monkeypatch.setattr(gate_hook.subprocess, "run", _always_times_out)
    results = run_gates(tmp_path, ["tests/test_a.py"])
    ok, msg = results["pytest"]
    assert len(calls) == 2  # exactly one retry, never a loop
    assert ok is False
    assert msg.startswith("TIMED OUT TWICE")
    assert "tolerance exhausted" in msg
    assert _status_word(ok, msg) == "TIMEOUT"


def test_run_gates_a_retry_that_reveals_a_real_failure_is_not_swallowed(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    """The retry exists ONLY for a timeout — if the second attempt runs to completion but
    fails for real, that is a genuine FAILED, never quietly treated as tolerated."""
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, ""))
    _write(tmp_path, "tests/test_a.py")
    calls: list[str] = []

    class _FakeFailedProc:
        returncode = 1
        stdout = "1 failed, 2 passed"
        stderr = ""

    def _first_times_out_then_fails(cmd: list[str], **kwargs: Any) -> _FakeFailedProc:
        calls.append("call")
        if len(calls) == 1:
            raise gate_hook.subprocess.TimeoutExpired(cmd, kwargs.get("timeout", 0))
        return _FakeFailedProc()

    monkeypatch.setattr(gate_hook.subprocess, "run", _first_times_out_then_fails)
    results = run_gates(tmp_path, ["tests/test_a.py"])
    ok, msg = results["pytest"]
    assert len(calls) == 2
    assert ok is False
    assert not msg.startswith("PASSED ON RETRY")
    assert "unchanged after an immediate retry" in msg
    assert _status_word(ok, msg) == "FAILED"


def test_status_word_never_folds_a_retried_pass_into_a_plain_ok() -> None:
    assert _status_word(
        True, "PASSED ON RETRY — timed out after 180s on the first attempt, then passed "
        "clean on an immediate second attempt") == "PASSED-ON-RETRY"


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


def test_report_marks_a_retried_pass_distinctly_not_a_plain_pass(capsys: Any) -> None:
    results = {
        "ruff": (True, ""), "mypy": (True, ""),
        "pytest": (True, "PASSED ON RETRY — timed out after 180s on the first attempt, "
                          "then passed clean on an immediate second attempt\n1 passed"),
    }
    all_ok = gate_hook._report("test", results)
    out = capsys.readouterr().out
    assert all_ok is True  # enforcement semantics unchanged -- this never refuses
    assert "PASSED ON RETRY" in out.splitlines()[0]
    assert out.splitlines()[0] != "gate_hook[test]: PASS"
    assert "pytest: PASSED-ON-RETRY" in out


# --- obligation a3c71bf5: the omitted-file list must survive the printed tail -----------------
# Filed while verifying b9045c3 (Thoth msg 3281): the omitted-files summary is the FIRST line
# of a SKIPPED detail, but a real pytest run underneath it can be long, and `[-15:]` takes the
# LAST 15 lines of the WHOLE blob -- the summary line silently scrolls out. The headline still
# reads "SKIPPED (partial)" (honest), but a reader has no way to learn WHICH files were
# skipped (not actionable) -- #117's own shape one layer down from where it was last caught.

def test_report_skip_summary_survives_a_long_pytest_tail(capsys: Any) -> None:
    body = "\n".join(f"noise line {i}" for i in range(40))
    detail = (
        "SKIPPED (partial) — ran 3 clean, omitted 5 fixture-only files (hub-module fan-out, "
        f"over cap 12): [a_test.py b_test.py c_test.py d_test.py e_test.py]\n{body}"
    )
    results = {"ruff": (True, ""), "mypy": (True, ""), "pytest": (True, detail)}
    gate_hook._report("test", results)
    out = capsys.readouterr().out
    assert "omitted 5 fixture-only files" in out  # would fail pre-fix: pushed out by the tail
    assert "a_test.py" in out and "e_test.py" in out


def test_report_skip_with_no_body_still_shows_the_summary(capsys: Any) -> None:
    results = {
        "ruff": (True, ""), "mypy": (True, ""),
        "pytest": (True, "SKIPPED — nothing ran; omitted 3 fixture-only files: [a.py b.py c.py]"),
    }
    gate_hook._report("test", results)
    out = capsys.readouterr().out
    assert "omitted 3 fixture-only files" in out
    assert "a.py" in out and "c.py" in out


def test_report_says_so_explicitly_when_the_omitted_list_is_too_long_to_show_in_full(
    capsys: Any,
) -> None:
    many_files = " ".join(f"file_{i}.py" for i in range(400))
    detail = (
        f"SKIPPED — nothing ran; omitted 400 fixture-only files (hub-module fan-out, over "
        f"cap 12): [{many_files}]"
    )
    results = {"ruff": (True, ""), "mypy": (True, ""), "pytest": (True, detail)}
    gate_hook._report("test", results)
    out = capsys.readouterr().out
    # "could not show" must never render as "nothing to show" (Khnum's census_blind rule) --
    # a truncation must say so explicitly, not just silently cut the line short.
    assert "file_0.py" in out  # still shows what it can, from the front
    assert "TRUNCATED" in out or "elided" in out
    assert "400 fixture-only files" in out  # the count survives even when the names don't


def test_report_a_real_failure_keeps_its_existing_tail_only_behavior(capsys: Any) -> None:
    body = "\n".join(f"line {i}" for i in range(20)) + "\nAssertionError: boom"
    results = {"ruff": (True, ""), "mypy": (True, ""), "pytest": (False, body)}
    gate_hook._report("test", results)
    out = capsys.readouterr().out
    assert "AssertionError: boom" in out
    assert "line 0" not in out  # unchanged: a real failure still only gets the last 15 lines


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
    monkeypatch.setattr(gate_hook, "_staged_diff_digest", lambda root=None: "same")
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
    monkeypatch.setattr(gate_hook, "_staged_diff_digest", lambda root=None: "same")
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
    monkeypatch.setattr(gate_hook, "_staged_diff_digest", lambda root=None: "same")
    monkeypatch.setattr(
        gate_hook, "receipt_shaped_touches",
        lambda root, changed: {"src/pkg/x.py": ["real_tool"]})
    assert cmd_precommit(enforce=True) == 1
    out = capsys.readouterr().out
    assert DERIVATION_TRACE_QUESTION in out
    assert "gate_hook: REFUSED" in out


# --- the stage-race TOCTOU guard (Thoth DM 3005/3012, thread 3005) -----------------------------
# NOT a #117 shape-3 cure (decision 96463307 found no general mechanical path for that) --
# a narrower, separately-motivated mechanism for Practice 81cab2f4/decision b1863e56's own
# documented hazard: a concurrent `git add` landing between a staged-diff check and the commit
# that follows it. Reproduced live in a throwaway repo before this was built: git does not hold
# the index lock across a pre-commit hook's own execution, so the hazard extends across the
# ENTIRE gate run, not just an agent's own instant diff-then-commit gap.

def test_staged_diff_digest_changes_with_content_not_just_file_list(
    tmp_path: Any, monkeypatch: Any,
) -> None:
    """Practice 81cab2f4's own point: `--stat`/file names alone would miss a same-file
    content race. The digest must be sensitive to the diff BODY."""
    calls = iter(["diff --cached v1", "diff --cached v1", "diff --cached v2"])
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, next(calls)))
    d1 = gate_hook._staged_diff_digest(tmp_path)
    d1_again = gate_hook._staged_diff_digest(tmp_path)
    d2 = gate_hook._staged_diff_digest(tmp_path)
    assert d1 == d1_again
    assert d1 != d2


def test_precommit_detects_a_stage_race_and_refuses_when_enforced(
    monkeypatch: Any, capsys: Any,
) -> None:
    monkeypatch.setattr(gate_hook, "changed_files_staged", lambda root=None: ["a.py"])
    monkeypatch.setattr(
        gate_hook, "run_gates",
        lambda root, changed: {"ruff": (True, ""), "mypy": (True, ""), "pytest": (True, "")})
    digests = iter(["before", "after"])
    monkeypatch.setattr(gate_hook, "_staged_diff_digest", lambda root=None: next(digests))
    assert cmd_precommit(enforce=True) == 1
    out = capsys.readouterr().out
    assert "gate_hook[staged]: RACE" in out
    assert "gate_hook: REFUSED — staged content raced with the gate run" in out


def test_precommit_stage_race_is_advisory_when_not_enforced(
    monkeypatch: Any, capsys: Any,
) -> None:
    monkeypatch.setattr(gate_hook, "changed_files_staged", lambda root=None: ["a.py"])
    monkeypatch.setattr(
        gate_hook, "run_gates",
        lambda root, changed: {"ruff": (True, ""), "mypy": (True, ""), "pytest": (True, "")})
    digests = iter(["before", "after"])
    monkeypatch.setattr(gate_hook, "_staged_diff_digest", lambda root=None: next(digests))
    assert cmd_precommit(enforce=False) == 0
    out = capsys.readouterr().out
    assert "gate_hook[staged]: RACE" in out
    assert "NOT ENFORCED" in out
    assert "stage race" in out


def test_precommit_stage_race_names_the_files_that_appeared(
    monkeypatch: Any, capsys: Any,
) -> None:
    file_calls = iter([["a.py"], ["a.py", "b.py"]])
    monkeypatch.setattr(gate_hook, "changed_files_staged", lambda root=None: next(file_calls))
    monkeypatch.setattr(
        gate_hook, "run_gates",
        lambda root, changed: {"ruff": (True, ""), "mypy": (True, ""), "pytest": (True, "")})
    digests = iter(["before", "after"])
    monkeypatch.setattr(gate_hook, "_staged_diff_digest", lambda root=None: next(digests))
    assert cmd_precommit(enforce=True) == 1
    out = capsys.readouterr().out
    assert "b.py" in out


def test_precommit_a_stage_race_takes_priority_even_when_gates_all_passed(
    monkeypatch: Any, capsys: Any,
) -> None:
    """A race means the gate results above are not evidence about the tree that would
    actually commit -- a coincidental all-clean gate run must not paper over that."""
    monkeypatch.setattr(gate_hook, "changed_files_staged", lambda root=None: ["a.py"])
    monkeypatch.setattr(
        gate_hook, "run_gates",
        lambda root, changed: {"ruff": (True, ""), "mypy": (True, ""), "pytest": (True, "")})
    digests = iter(["before", "after"])
    monkeypatch.setattr(gate_hook, "_staged_diff_digest", lambda root=None: next(digests))
    assert cmd_precommit(enforce=True) == 1
    out = capsys.readouterr().out
    assert "gate_hook[staged]: PASS" in out  # the (misleading, stale) gate verdict still prints
    assert "RACE" in out                     # but the race verdict is what actually decides it


def test_precommit_race_refusal_reads_differently_from_a_gate_failure_refusal(
    monkeypatch: Any, capsys: Any,
) -> None:
    """THE VOCABULARY-DISTINCTNESS REQUIREMENT (Thoth DM 3012: 'must distinguish "the index
    moved" from "the commit failed" in its own words'). Two REFUSED commits for two different
    reasons must never render the identical sentence -- the same disjointness discipline as
    tests/test_receipt_vocabulary.py, applied to this mechanism's own two refusal reasons."""
    monkeypatch.setattr(gate_hook, "changed_files_staged", lambda root=None: ["a.py"])

    monkeypatch.setattr(
        gate_hook, "run_gates",
        lambda root, changed: {"ruff": (True, ""), "mypy": (True, ""), "pytest": (True, "")})
    race_digests = iter(["before", "after"])
    monkeypatch.setattr(gate_hook, "_staged_diff_digest", lambda root=None: next(race_digests))
    assert cmd_precommit(enforce=True) == 1
    race_out = capsys.readouterr().out
    race_line = next(
        line for line in race_out.splitlines() if line.startswith("gate_hook: REFUSED"))

    monkeypatch.setattr(
        gate_hook, "run_gates",
        lambda root, changed: {"ruff": (False, "boom"), "mypy": (True, ""), "pytest": (True, "")})
    monkeypatch.setattr(gate_hook, "_staged_diff_digest", lambda root=None: "same")
    assert cmd_precommit(enforce=True) == 1
    fail_out = capsys.readouterr().out
    fail_line = next(
        line for line in fail_out.splitlines() if line.startswith("gate_hook: REFUSED"))

    assert race_line != fail_line
    assert "raced" in race_line and "raced" not in fail_line
    assert "a gate failed" in fail_line and "a gate failed" not in race_line


def test_precommit_no_race_is_unaffected_when_digest_is_stable(
    monkeypatch: Any, capsys: Any,
) -> None:
    """Regression guard: the new bracket must be a no-op when nothing actually raced."""
    monkeypatch.setattr(gate_hook, "changed_files_staged", lambda root=None: ["a.py"])
    monkeypatch.setattr(
        gate_hook, "run_gates",
        lambda root, changed: {"ruff": (True, ""), "mypy": (True, ""), "pytest": (True, "")})
    monkeypatch.setattr(gate_hook, "_staged_diff_digest", lambda root=None: "stable")
    assert cmd_precommit(enforce=True) == 0
    out = capsys.readouterr().out
    assert "RACE" not in out


def test_venv_bin_tracks_the_running_interpreter_not_repo_root(tmp_path: Path) -> None:
    """The exact worktree bug (thread 64c6b197): a worktree has no materialized `.venv` --
    `.gitignore` excludes it, so `git worktree add` never copies one. If VENV_BIN were still
    `REPO_ROOT / ".venv" / "bin"` (derived from `__file__`, i.e. wherever this module's own
    copy happens to live), running gate_hook.py from a tree with no .venv at all would
    resolve to a nonexistent path. It must instead track `sys.executable` -- the venv that
    actually launched the process -- regardless of where the module file itself sits."""
    import shutil
    import subprocess
    import sys as real_sys

    fake_repo = tmp_path / "no-venv-here"
    fake_scripts = fake_repo / "scripts"
    fake_scripts.mkdir(parents=True)
    shutil.copy(Path(gate_hook.__file__), fake_scripts / "gate_hook.py")
    assert not (fake_repo / ".venv").exists()  # proves REPO_ROOT/.venv/bin would be bogus here

    proc = subprocess.run(
        [real_sys.executable, "-c",
         f"import sys; sys.path.insert(0, {str(fake_scripts)!r}); "
         "import gate_hook; print(gate_hook.VENV_BIN)"],
        capture_output=True, text=True, check=True,
    )
    assert proc.stdout.strip() == str(Path(real_sys.executable).parent)


def test_venv_bin_does_not_resolve_past_a_symlinked_interpreter(tmp_path: Path) -> None:
    """Caught live by the worktree acceptance test (thread 64c6b197): this repo's `.venv` is
    uv-managed, and `.venv/bin/python` is a symlink STRAIGHT to the shared uv toolchain
    (`~/.local/share/uv/python/.../bin/python3.12`), not a copy. `Path(sys.executable)
    .resolve()` follows that symlink past the venv boundary entirely, landing in a directory
    with no ruff/mypy/pytest. VENV_BIN must use the unresolved, as-invoked path instead."""
    import subprocess

    fake_venv_bin = tmp_path / "fake-venv" / "bin"
    fake_venv_bin.mkdir(parents=True)
    real_target = tmp_path / "elsewhere-real-interpreter"
    real_target.write_text("")  # just needs to exist for the symlink to resolve somewhere
    fake_python = fake_venv_bin / "python3"
    fake_python.symlink_to(real_target)

    proc = subprocess.run(
        ["python3", "-c",
         f"import sys; sys.executable = {str(fake_python)!r}; "
         f"sys.path.insert(0, {str(Path(gate_hook.__file__).parent)!r}); "
         "import gate_hook; print(gate_hook.VENV_BIN)"],
        capture_output=True, text=True, check=True,
    )
    assert proc.stdout.strip() == str(fake_venv_bin)


# --- thread 1a0f91bb: the ratchet-lag standing-law fork, DECIDED (the gate tolerates the
# split rather than demanding the ratchet move in the same commit as a merge) and ENCODED,
# not documented around. Dispatch 5399 LEG 1. ------------------------------------------------

def _git(repo: Path, *args: str) -> None:
    import subprocess

    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def _init_repo(repo: Path) -> None:
    repo.mkdir(parents=True, exist_ok=True)
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "gate-hook-test@example.com")
    _git(repo, "config", "user.name", "gate hook test")


def _commit(repo: Path, msg: str) -> None:
    _git(repo, "commit", "-q", "--allow-empty", "-m", msg)


def test_is_merge_context_false_for_a_plain_single_parent_commit(tmp_path: Path) -> None:
    repo = tmp_path / "r"
    _init_repo(repo)
    _commit(repo, "first")
    _commit(repo, "second")
    assert _is_merge_context(repo) is False


def test_is_merge_context_true_for_a_landed_merge_commit(tmp_path: Path) -> None:
    repo = tmp_path / "r"
    _init_repo(repo)
    _commit(repo, "base")
    _git(repo, "checkout", "-q", "-b", "side")
    _commit(repo, "side work")
    _git(repo, "checkout", "-q", "-")
    _git(repo, "merge", "-q", "--no-ff", "-m", "merge side", "side")
    assert _is_merge_context(repo) is True


def test_is_merge_context_true_mid_merge_before_the_merge_commit_lands(
    tmp_path: Path,
) -> None:
    """MERGE_HEAD exists from the moment a conflict-free `git merge` starts until the merge
    commit itself is made — this is the LIVE pre-commit path's own signal (the commit hasn't
    happened yet, so there is no parent count to read from HEAD)."""
    repo = tmp_path / "r"
    _init_repo(repo)
    _commit(repo, "base")
    _git(repo, "checkout", "-q", "-b", "side")
    _commit(repo, "side work")
    _git(repo, "checkout", "-q", "-")
    _git(repo, "merge", "-q", "--no-ff", "--no-commit", "side")
    assert _is_merge_context(repo) is True


def test_pytest_sole_failure_is_ratchet_ceiling_true_for_an_exact_solo_match() -> None:
    out = (
        f"FAILED {_RATCHET_TEST_NODEID} - AssertionError: tool contract grew to 200000 "
        "chars\n1 failed, 40 passed in 3.2s"
    )
    assert _pytest_sole_failure_is_ratchet_ceiling(out) is True


def test_pytest_sole_failure_is_ratchet_ceiling_false_when_anything_else_also_fails() -> None:
    """#133's own real specimens (a74ce7a/ff72377): a merge that ALSO broke something else
    must still refuse — never a blanket pass-through for merge commits generally."""
    out = (
        f"FAILED {_RATCHET_TEST_NODEID} - AssertionError: ...\n"
        "FAILED tests/test_mounts.py::test_something_unrelated - AssertionError: ...\n"
        "2 failed, 39 passed in 3.2s"
    )
    assert _pytest_sole_failure_is_ratchet_ceiling(out) is False


def test_pytest_sole_failure_is_ratchet_ceiling_false_for_a_different_solo_failure() -> None:
    out = "FAILED tests/test_mounts.py::test_unrelated - AssertionError: ...\n1 failed, 40 passed"
    assert _pytest_sole_failure_is_ratchet_ceiling(out) is False


def test_status_word_names_ratchet_debt_distinctly_never_a_plain_ok() -> None:
    assert _status_word(True, "RATCHET-DEBT — merge commit exceeds the ceiling") \
        == "RATCHET-DEBT"


def test_run_gates_ratchet_ceiling_solo_failure_on_a_merge_is_debt_not_failed(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, ""))
    monkeypatch.setattr(gate_hook, "_is_merge_context", lambda repo_root: True)
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_a.py").write_text("")

    class _FakeProc:
        returncode = 1
        stdout = (
            f"FAILED {_RATCHET_TEST_NODEID} - AssertionError: over ceiling\n"
            "1 failed, 5 passed"
        )
        stderr = ""

    monkeypatch.setattr(gate_hook.subprocess, "run", lambda cmd, **kw: _FakeProc())
    results = run_gates(tmp_path, ["tests/test_a.py"])
    ok, msg = results["pytest"]
    assert ok is True  # never refused
    assert msg.startswith("RATCHET-DEBT")
    assert _status_word(ok, msg) == "RATCHET-DEBT"


def test_run_gates_ratchet_ceiling_solo_failure_on_a_non_merge_commit_still_fails(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    """The same exact pytest output, but NOT a merge commit — a real, ordinary regression
    (the growth the commit itself caused) and must still refuse."""
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, ""))
    monkeypatch.setattr(gate_hook, "_is_merge_context", lambda repo_root: False)
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_a.py").write_text("")

    class _FakeProc:
        returncode = 1
        stdout = (
            f"FAILED {_RATCHET_TEST_NODEID} - AssertionError: over ceiling\n"
            "1 failed, 5 passed"
        )
        stderr = ""

    monkeypatch.setattr(gate_hook.subprocess, "run", lambda cmd, **kw: _FakeProc())
    results = run_gates(tmp_path, ["tests/test_a.py"])
    ok, msg = results["pytest"]
    assert ok is False
    assert _status_word(ok, msg) == "FAILED"


def test_report_marks_ratchet_debt_as_pass_but_names_it_distinctly(capsys: Any) -> None:
    results = {
        "ruff": (True, ""), "mypy": (True, ""),
        "pytest": (True, "RATCHET-DEBT — merge commit exceeds the ceiling\n1 failed, 5 passed"),
    }
    all_ok = gate_hook._report("test", results)
    out = capsys.readouterr().out
    assert all_ok is True  # never refuses
    assert "RATCHET DEBT" in out.splitlines()[0]
    assert "pytest: RATCHET-DEBT" in out


# --- main(): the escape hatch (dispatch 5399) — an internal bug in this diagnostic's own
# code fails OPEN, never blocks a commit --------------------------------------------------

def test_main_fails_open_on_an_internal_error_in_precommit(
    monkeypatch: Any, capsys: Any,
) -> None:
    def _boom(*, enforce: bool | None = None) -> int:
        raise RuntimeError("a bug in the diagnostic itself")

    monkeypatch.setattr(gate_hook, "cmd_precommit", _boom)
    assert gate_hook.main([]) == 0
    err = capsys.readouterr().err
    assert "INTERNAL ERROR" in err
    assert "failing OPEN" in err


def test_main_fails_open_on_an_internal_error_in_audit(monkeypatch: Any, capsys: Any) -> None:
    def _boom(rev_range: str) -> int:
        raise RuntimeError("a bug in the diagnostic itself")

    monkeypatch.setattr(gate_hook, "cmd_audit", _boom)
    assert gate_hook.main(["--audit", "a..b"]) == 0
    err = capsys.readouterr().err
    assert "INTERNAL ERROR" in err


def test_main_a_real_gate_failure_still_refuses_normally(monkeypatch: Any) -> None:
    """The escape hatch must never swallow a GENUINE gate failure — only an unhandled
    exception inside the diagnostic's own code reaches the fail-open branch."""
    monkeypatch.setattr(gate_hook, "changed_files_staged", lambda root=None: ["a.py"])
    monkeypatch.setattr(
        gate_hook, "run_gates",
        lambda root, changed: {"ruff": (False, "boom"), "mypy": (True, ""), "pytest": (True, "")})
    monkeypatch.setattr(gate_hook, "_staged_diff_digest", lambda root=None: "same")
    from src.config.settings import get_settings

    monkeypatch.setattr(get_settings(), "osiris_gate_hook_enforce", True)
    assert gate_hook.main([]) == 1


# ═══════════ THE SHADOWED-IMPORT LINT (dispatch 5441 LEG 4, ruling 2f7e1588) ══════════
# The discriminator is EXECUTION ORDER, never mere shadow presence — a fleet-wide audit
# found 19 candidate shadows and exactly ONE real bug (mailbox.py's send_message: a nested
# `_stamp_threads` closure read `datetime`/`UTC` before a redundant later local import of
# the same names ran, an UnboundLocalError/NameError on every real call). This lint is
# built on that discriminator, not on the audit's own over-broad first heuristic.

def test_module_level_import_names_collects_both_import_forms(tmp_path: Path) -> None:
    f = tmp_path / "m.py"
    f.write_text("import os\nfrom datetime import UTC, datetime as dt\n")
    tree = ast.parse(f.read_text())
    assert _module_level_import_names(tree) == {"os", "UTC", "dt"}


def test_module_level_import_names_ignores_a_nested_import(tmp_path: Path) -> None:
    f = tmp_path / "m.py"
    f.write_text("def f():\n    import os\n    return os\n")
    tree = ast.parse(f.read_text())
    assert _module_level_import_names(tree) == set()


def test_own_scope_local_imports_finds_ones_inside_try_and_if(tmp_path: Path) -> None:
    src = (
        "def f():\n"
        "    if True:\n"
        "        try:\n"
        "            from datetime import datetime\n"
        "        except ImportError:\n"
        "            pass\n"
    )
    tree = ast.parse(src)
    func = tree.body[0]
    assert _own_scope_local_imports(func) == [("datetime", 4)]


def test_own_scope_local_imports_ignores_a_nested_functions_own_import(tmp_path: Path) -> None:
    """A nested function's local import belongs to ITS OWN scope, not the outer one."""
    src = (
        "def outer():\n"
        "    def inner():\n"
        "        import os\n"
        "        return os\n"
        "    return inner\n"
    )
    tree = ast.parse(src)
    outer = tree.body[0]
    assert _own_scope_local_imports(outer) == []


def test_reads_name_before_true_for_a_read_on_an_earlier_line() -> None:
    src = "def f():\n    x = datetime\n    from datetime import datetime\n"
    tree = ast.parse(src)
    func = tree.body[0]
    assert _reads_name_before(func, "datetime", 3) is True


def test_reads_name_before_false_when_the_only_read_is_later() -> None:
    src = "def f():\n    from datetime import datetime\n    return datetime\n"
    tree = ast.parse(src)
    func = tree.body[0]
    assert _reads_name_before(func, "datetime", 2) is False


def test_reads_name_before_sees_a_nested_closures_read_the_bug_shape(tmp_path: Path) -> None:
    """The EXACT mailbox.py shape: a nested function reads the name, is defined (and would
    be CALLED) before the outer function's own later local import runs."""
    src = (
        "def outer():\n"
        "    def inner():\n"
        "        return datetime.now()\n"
        "    inner()\n"
        "    from datetime import datetime\n"
    )
    tree = ast.parse(src)
    outer = tree.body[0]
    local_imports = _own_scope_local_imports(outer)
    assert local_imports == [("datetime", 5)]
    assert _reads_name_before(outer, "datetime", 5) is True


def test_shadow_before_use_violations_catches_the_mailbox_shape(tmp_path: Path) -> None:
    src = (
        "from datetime import UTC, datetime\n\n"
        "async def send_message():\n"
        "    async def _stamp_threads():\n"
        "        return datetime.now(UTC)\n"
        "    await _stamp_threads()\n"
        "    try:\n"
        "        from datetime import UTC, datetime\n"
        "        return datetime.now(UTC)\n"
        "    except Exception:\n"
        "        pass\n"
    )
    (tmp_path / "mailbox_like.py").write_text(src)
    out = shadow_before_use_violations(tmp_path, ["mailbox_like.py"])
    assert "mailbox_like.py" in out
    assert any("send_message" in msg and "'datetime'" in msg for msg in out["mailbox_like.py"])


def test_shadow_before_use_violations_silent_on_a_harmless_shadow(tmp_path: Path) -> None:
    """A local import that is NEVER read before its own line — the common, harmless shape
    (18 of the audit's own 19 candidates) — must not be flagged, even with a nested
    function present, even when the SAME name is shadowed."""
    src = (
        "import os\n\n"
        "def f():\n"
        "    def inner():\n"
        "        return 1  # never touches os\n"
        "    inner()\n"
        "    import os\n"
        "    return os.getcwd()\n"
    )
    (tmp_path / "harmless.py").write_text(src)
    out = shadow_before_use_violations(tmp_path, ["harmless.py"])
    assert out == {}


def test_shadow_before_use_violations_ignores_a_name_never_imported_at_module_level(
    tmp_path: Path,
) -> None:
    """A purely-local import with no module-level twin is not a SHADOW at all — never in
    scope for this lint, however it's used."""
    src = "def f():\n    import json\n    return json.dumps({})\n"
    (tmp_path / "nomodulelevel.py").write_text(src)
    out = shadow_before_use_violations(tmp_path, ["nomodulelevel.py"])
    assert out == {}


def test_shadow_before_use_violations_skips_unparseable_files(tmp_path: Path) -> None:
    (tmp_path / "broken.py").write_text("def broken(:\n")
    assert shadow_before_use_violations(tmp_path, ["broken.py"]) == {}


def test_shadow_before_use_violations_skips_missing_and_non_python_files(
    tmp_path: Path,
) -> None:
    out = shadow_before_use_violations(
        tmp_path, ["does_not_exist.py", "README.md", "scripts/gate_hook.py"])
    assert out == {}


def test_run_gates_refuses_on_a_shadow_before_use_violation(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, ""))
    src = (
        "from datetime import UTC, datetime\n\n"
        "async def send_message():\n"
        "    async def _stamp_threads():\n"
        "        return datetime.now(UTC)\n"
        "    await _stamp_threads()\n"
        "    from datetime import UTC, datetime\n"
    )
    _write(tmp_path, "src/orchestrator/mailbox_like.py", src)
    results = run_gates(tmp_path, ["src/orchestrator/mailbox_like.py"])
    ok, detail = results["shadow_lint"]
    assert ok is False
    assert "send_message" in detail
    assert _status_word(ok, detail) == "FAILED"


def test_run_gates_shadow_lint_is_a_plain_ok_with_nothing_to_flag(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    monkeypatch.setattr(gate_hook, "_run", lambda cmd, cwd: (True, ""))
    _write(tmp_path, "src/orchestrator/clean.py", "def f():\n    return 1\n")
    results = run_gates(tmp_path, ["src/orchestrator/clean.py"])
    assert results["shadow_lint"] == (True, "")
