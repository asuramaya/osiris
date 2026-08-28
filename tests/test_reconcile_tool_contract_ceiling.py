"""scripts/reconcile_tool_contract_ceiling.py — the merge driver for TOOL_CONTRACT_CEILING_
CHARS collisions (dispatch 26686b77). No test file existed for this script before thread
197164ae's first real collision surfaced a live bug in it — the exact "a check nobody runs
is a check that isn't there" shape this driver's own sibling ratchet exists to prevent,
now proven against a script instead of a docstring.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import scripts.reconcile_tool_contract_ceiling as rtc

_NAME = "TOOL_CONTRACT_CEILING_CHARS"


def test_const_value_reads_the_number_not_the_first_underscore_in_the_name() -> None:
    """THE BUG ITSELF (thread 197164ae): every real constant name this driver has ever
    been pointed at contains an underscore (TOOL_CONTRACT_CEILING_CHARS). The old
    implementation re-searched `[\\d_]+` over the WHOLE matched line and `re.search`
    grabbed the FIRST such run — the bare `_` inside "TOOL_CONTRACT..." itself, never
    the number — so this call raised ValueError on every single invocation, not
    intermittently."""
    assert rtc._const_value(f"{_NAME} = 197_348", _NAME) == 197348


def test_const_value_is_none_when_the_constant_is_absent() -> None:
    assert rtc._const_value("SOME_OTHER_LINE = 5", _NAME) is None


def _write(p: Path, text: str) -> Path:
    p.write_text(text)
    return p


def test_main_resolves_a_real_conflict_by_arithmetic_never_the_larger_value(
    tmp_path: Path,
) -> None:
    """The three inputs are each a plain, clean file — exactly the %O/%A/%B shape git
    hands a merge driver — never pre-baked conflict markers; `main()` calls `git
    merge-file` itself to produce the collision on the same line, in place."""
    ancestor = _write(tmp_path / "base.py", f"# comment\n{_NAME} = 135_077  # base\n")
    ours = _write(tmp_path / "ours.py",
                 f"# ours raised it for its own reason\n{_NAME} = 135_292  # base + 215\n")
    theirs = _write(tmp_path / "theirs.py",
                    f"# theirs raised it for its own reason\n{_NAME} = 135_189  # base + 112\n")
    path_arg = "tests/test_tool_contract_diet.py"

    rc = rtc.main([str(ancestor), str(ours), str(theirs), path_arg])

    assert rc == 0
    text = ours.read_text()
    assert "<<<<<<<" not in text
    assert f"{_NAME} = 135404" in text  # 135_077 + 215 + 112, never the larger of the two


def test_main_resolves_the_exact_specimen_shape_from_thread_197164ae(
    tmp_path: Path,
) -> None:
    """THE REAL SPECIMEN, reconstructed from history (2620af9 vs d26703f, merged by hand
    at 1893116): `ours`' own line carried a trailing "# MEASURED..." changelog comment,
    `theirs`' own line was bare. Against the OLD regex, `ours` silently read back as
    "missing" (a comment-tolerant match never existed) while `theirs` (bare, so it DID
    match) crashed inside `_const_value`'s own digit-search bug — exactly the traceback
    Thoth pasted, "computing theirs_v". Both defects are fixed; this is the shape that
    must resolve clean, not just avoid crashing."""
    ancestor = _write(tmp_path / "base.py", f"{_NAME} = 196_473  # MEASURED: 141 tools.\n")
    ours = _write(tmp_path / "ours.py",
                 f"{_NAME} = 196_635  # MEASURED against the merged tree: 141 tools.\n")
    theirs = _write(tmp_path / "theirs.py", f"{_NAME} = 197_348\n")

    rc = rtc.main([str(ancestor), str(ours), str(theirs), "tests/test_tool_contract_diet.py"])

    assert rc == 0
    text = ours.read_text()
    assert "<<<<<<<" not in text
    assert f"{_NAME} = 197510" in text  # 196_473 + (196_635-196_473) + (197_348-196_473)


def test_main_declines_rather_than_raising_when_a_side_fails_to_parse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """THE ACTUAL POINT (thread 197164ae, Thoth's own framing): a driver that cannot
    parse a side must DECLINE with a named reason, never raise past the merge machinery
    into an opaque, indistinguishable-from-a-human-conflict traceback. Forces the defect
    class directly (monkeypatching `_const_value` to raise) rather than relying on the
    one instance that has already been fixed at its root — this is the belt, not the
    buckle."""
    ancestor = _write(tmp_path / "base.py", f"{_NAME} = 135_077\n")
    ours = _write(tmp_path / "ours.py", f"{_NAME} = 135_292\n")
    theirs = _write(tmp_path / "theirs.py", f"{_NAME} = 135_189\n")

    def _boom(text: str, name: str) -> int | None:
        raise ValueError("invalid literal for int() with base 10: ''")

    monkeypatch.setattr(rtc, "_const_value", _boom)

    rc = rtc.main([str(ancestor), str(ours), str(theirs), "some/path.py"])

    assert rc != 0
    err = capsys.readouterr().err
    assert "DECLINED" in err
    assert "reconcile_tool_contract_ceiling" in err


def test_main_leaves_a_real_conflict_untouched_when_the_constant_is_missing(
    tmp_path: Path,
) -> None:
    ancestor = _write(tmp_path / "base.py", "SOME_OTHER_CONSTANT = 1\n")
    ours = _write(tmp_path / "ours.py", "SOME_OTHER_CONSTANT = 2\n")
    theirs = _write(tmp_path / "theirs.py", "SOME_OTHER_CONSTANT = 3\n")

    rc = rtc.main([str(ancestor), str(ours), str(theirs), "unrelated/file.py"])

    assert rc != 0


# ═══ two constants, one file, one driver invocation (thread 5999): the actual defect
# that lived through four real merges — the char ceiling always conflicted (both
# branches touch adjacent prose) and always got reconciled; the tool count sat lines
# away, touched by only ONE branch at a time, so it never even reached a conflict marker
# and this driver never saw it. ═══

_COUNT_NAME = "TOOL_CONTRACT_EXPECTED_COUNT"


_UNCHANGED_GAP = "\n".join(f"# unrelated context line {i}" for i in range(10)) + "\n"
# git's own diff3 groups ADJACENT changed lines into one conflict block — the real
# specimen had the two constants nine lines apart (thread 5999's own count), so a
# fixture with them touching would misrepresent the actual bug shape (one combined
# block, not one real conflict beside one silent, markerless auto-merge).


def test_main_defaults_to_reconciling_both_named_constants(tmp_path: Path) -> None:
    """The char ceiling collides (both sides touch it — a real conflict block, separated
    by unchanged context from the count so git treats them as independent hunks); the
    count line does NOT (only `theirs` touched it — no conflict marker at all, git took
    theirs' side silently). Both must still resolve correctly in ONE invocation, with no
    explicit --constant-name needed — this is what `_DEFAULT_CONSTANTS` is for."""
    ancestor = _write(tmp_path / "base.py", (
        f"{_NAME} = 196_473  # MEASURED: 141 tools.\n" + _UNCHANGED_GAP
        + f"{_COUNT_NAME} = 141\n"
    ))
    ours = _write(tmp_path / "ours.py", (
        f"{_NAME} = 196_635  # MEASURED against the merged tree: 141 tools.\n"
        + _UNCHANGED_GAP + f"{_COUNT_NAME} = 141\n"  # ours never touched the count
    ))
    theirs = _write(tmp_path / "theirs.py", (
        f"{_NAME} = 197_348\n" + _UNCHANGED_GAP
        + f"{_COUNT_NAME} = 142\n"  # theirs added one tool
    ))

    rc = rtc.main([str(ancestor), str(ours), str(theirs), "tests/test_tool_contract_diet.py"])

    assert rc == 0
    text = ours.read_text()
    assert "<<<<<<<" not in text
    assert f"{_NAME} = 197510" in text  # 196_473 + 162 + 875
    assert f"{_COUNT_NAME} = 142" in text  # 141 + 0 + 1 — theirs' own real addition


def test_main_reconciles_the_count_even_with_zero_conflict_markers(tmp_path: Path) -> None:
    """THE EXACT SHAPE THAT SLIPPED THROUGH (thread 5999): the char ceiling is untouched
    by either side (no conflict there at all, elsewhere in the same merge), and the count
    line is ALSO untouched by git's own merge (only one side changed it, so `git
    merge-file` just keeps that side with zero markers) — yet the resolved value must
    still be the true combined total, not whatever one branch happened to leave behind."""
    ancestor = _write(tmp_path / "base.py", (
        f"{_NAME} = 196_473  # MEASURED: 141 tools.\n"
        f"{_COUNT_NAME} = 141\n"
    ))
    ours = _write(tmp_path / "ours.py", (
        f"{_NAME} = 196_473  # MEASURED: 141 tools.\n"
        f"{_COUNT_NAME} = 141\n"  # ours untouched
    ))
    theirs = _write(tmp_path / "theirs.py", (
        f"{_NAME} = 196_473  # MEASURED: 141 tools.\n"
        f"{_COUNT_NAME} = 142\n"  # theirs added one tool, git will take this silently
    ))

    rc = rtc.main([str(ancestor), str(ours), str(theirs), "tests/test_tool_contract_diet.py"])

    assert rc == 0
    text = ours.read_text()
    assert "<<<<<<<" not in text
    assert f"{_COUNT_NAME} = 142" in text
