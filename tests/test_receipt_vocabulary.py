"""THE RECEIPT-VOCABULARY TEST (task #117, Thoth DM 2961/2967/2974): the mechanical-rule
half of #117's finding (decision d0ab1b0b) — SHAPE C, VOCABULARY COLLAPSE (a status
representation with no distinct word for "didn't happen," so an omission renders
identically to a genuine pass) — applied to its own sharpest specimen: gate_hook.py's
`_status_word`/`_report`, buggy until commit 0044671 shipped THE SAME NIGHT the pattern had
already been named and fixed twice elsewhere (`ec01c42`'s deploy-smoke timer, `7718f56`'s
gate_hook TIMED OUT/FAILED split). An informed, primed author who had personally fixed one
of the earlier instances hours before still wrote a fourth one — decision d0ab1b0b's own
strongest evidence that this is a structural defect, not a discipline one, and that
operator ruling 4ef68cfe (mechanical enforcement over documentation) is the right response.

WHY THIS IS BEHAVIORAL, NOT A PURE "WHICH STRING LITERALS APPEAR" AST WALK, EVEN THOUGH IT
FOLLOWS test_tool_contract_diet.py'S OWN SHAPE: that file's own docstring says plainly —
"THE MEASUREMENT IS THE LIVE, IN-PROCESS TOOL REGISTRATION, NOT A DOCSTRING GREP" — static
text analysis under-counts. The 0044671 bug was NOT a missing string literal in the source:
the pre-fix `_status_word` already only ever returned "ok"/"TIMEOUT"/"FAILED", a valid-
looking subset of its own four documented words — a check that merely asked "does every
literal belong to the vocabulary" would have PASSED the buggy version, because nothing was
OUTSIDE the vocabulary. The actual defect was that the `ok=True` branch never inspected
`detail` at all, so a genuinely-verified run and an entirely-skipped one produced the exact
same literal. That is an INPUT-SENSITIVITY property — provable only by calling the real
function with real representative inputs and checking the outputs differ — the same reason
the tool-contract ratchet calls the real `mcp.list_tools()` instead of grepping docstrings.

THE RULE THIS TEST ENFORCES, reusable beyond this one specimen (Thoth's own scoping,
msg 2974: "do not generalize it beyond functions that already declare a finite outcome
vocabulary"): for a status-word function whose job is collapsing an operation's real
outcome into one of N words, build a CASE TABLE naming every real scenario the surrounding
code can produce in plain English, tag each with the word it OUGHT to render, call the
function once per case, and assert (1) every case renders its own expected word, and (2)
no case meaning "not verified" ever renders the same word as one meaning "verified" — the
exact defect class 0044671 shipped. Applying this pattern to a NEW status-word function
elsewhere in the codebase means copying this file's shape with that function's own case
table, not writing a new AST framework.
"""
from __future__ import annotations

from typing import Any

from scripts.gate_hook import _report, _status_word

# Every real scenario `run_gates` can hand to `_status_word` today, named in plain English
# and tagged with the word gate_hook.py's OWN docstring says it should render (see
# `run_gates`'s "A GATE THAT CANNOT DISTINGUISH..." paragraph). Two scenarios sharing an
# `ok`/`SKIPPED` tag are INTENTIONALLY equivalent (e.g. "nothing to run at all" and "ran
# clean" are both honestly "ok" — gate_hook.py's own module docstring calls the former "the
# one case that is honestly a plain, unqualified ok"); this table encodes that judgment
# explicitly rather than asserting blanket pairwise distinctness, which would be a FALSE
# requirement for those two.
#
# A REAL, NAMED LIMIT (this file's own "not broad" scope): if `run_gates` grows a NEW real
# scenario without a new row here, this test cannot see it — it only enforces vocabulary
# discipline over scenarios it has been told about, same as the tool-contract ratchet only
# polices the surface it measures.
_STATUS_WORD_SCENARIOS: dict[str, tuple[tuple[bool, str], str]] = {
    "ran clean, nothing omitted": (
        (True, "[tests/test_x.py]\n1 passed"), "ok"),
    "no resolvable test files touched at all": (
        (True, "no resolvable test files touched"), "ok"),
    "ran clean but some fixture-only files were capped out (partial skip)": (
        (True, "SKIPPED (partial) — ran 3 clean, omitted 14 fixture-only files "
               "(hub-module fan-out, over cap 12): [...]\n1 passed"), "SKIPPED"),
    "nothing ran, every candidate was capped out": (
        (True, "SKIPPED — nothing ran; omitted 14 fixture-only files "
               "(hub-module fan-out, over cap 12): [...]"), "SKIPPED"),
    "a real assertion failed": (
        (False, "[tests/test_x.py]\n1 failed"), "FAILED"),
    "timed out under real ambient contention, not a proven code failure": (
        (False, "TIMED OUT after 180s under real ambient fleet load..."), "TIMEOUT"),
}


def test_status_word_matches_each_scenarios_documented_meaning() -> None:
    mismatches = [
        (name, expected, _status_word(ok, detail))
        for name, ((ok, detail), expected) in _STATUS_WORD_SCENARIOS.items()
        if _status_word(ok, detail) != expected
    ]
    assert not mismatches, (
        f"scenario -> (expected word, actual word) mismatches: {mismatches}")


def test_status_word_never_lets_an_unverified_scenario_share_a_verified_words() -> None:
    """THE SPECIFIC 0044671 DEFECT, checked directly rather than only via the lookup
    table above: a scenario meaning "this was NOT verified" (SKIPPED, TIMEOUT) must
    never render the same word as one meaning "this WAS verified" (ok) — the property
    a caller actually depends on to not misread a receipt."""
    verified_meanings = {"ok"}
    unverified_meanings = {"SKIPPED", "TIMEOUT"}
    verified_words = {
        _status_word(ok, detail)
        for (ok, detail), tag in _STATUS_WORD_SCENARIOS.values() if tag in verified_meanings
    }
    unverified_words = {
        _status_word(ok, detail)
        for (ok, detail), tag in _STATUS_WORD_SCENARIOS.values() if tag in unverified_meanings
    }
    assert verified_words.isdisjoint(unverified_words), (
        f"an unverified scenario rendered the same word as a verified one: "
        f"verified={verified_words} unverified={unverified_words}")


def _fossil_status_word(ok: bool, detail: str) -> str:
    """THE PRE-0044671 SHAPE, reproduced verbatim from `git show 0044671 -- \
scripts/gate_hook.py`'s own diff (the `-` lines) — kept ONLY as a negative control
    proving the two tests above are not vacuously passing. Never imported by real code;
    do not resurrect this shape."""
    if ok:
        return "ok"
    return "TIMEOUT" if detail.startswith("TIMED OUT") else "FAILED"


def test_the_pre_0044671_fossil_fails_the_disjointness_test() -> None:
    """THE ACCEPTANCE TEST NAMED IN THOTH'S DISPATCH (msg 2974), stated directly: this
    module's own check must fail against the shape that actually shipped and reached
    Thoth as "17/18 pass" before he caught it (DM 2957). Run the SAME scenario table
    through the fossil and confirm the collision that was live in production reproduces
    here — the negative control that proves the positive tests above have teeth."""
    verified_words = {
        _fossil_status_word(ok, detail)
        for (ok, detail), tag in _STATUS_WORD_SCENARIOS.values() if tag == "ok"
    }
    unverified_words = {
        _fossil_status_word(ok, detail)
        for (ok, detail), tag in _STATUS_WORD_SCENARIOS.values() if tag == "SKIPPED"
    }
    assert not verified_words.isdisjoint(unverified_words), (
        "the fossil was expected to collide 'ok' and 'SKIPPED' scenarios into the same "
        "word (that is the exact bug 0044671 fixed) but it didn't — this negative "
        "control no longer proves anything and needs re-deriving from the real diff")
    assert verified_words == unverified_words == {"ok"}


# ── _report: the user-facing verdict line (the "17/18 pass" specimen itself) ───────────

_REPORT_SCENARIOS: dict[str, tuple[dict[str, tuple[bool, str]], str]] = {
    "everything genuinely ran and passed": (
        {"ruff": (True, "ok"), "mypy": (True, "ok"),
         "pytest": (True, "[tests/test_x.py]\n1 passed")},
        "PASS"),
    "pytest was skipped over cap, ruff/mypy clean": (
        {"ruff": (True, "ok"), "mypy": (True, "ok"),
         "pytest": (True, "SKIPPED — nothing ran; omitted 14 fixture-only files...")},
        "PASS (UNVERIFIED"),
    "a real failure": (
        {"ruff": (True, "ok"), "mypy": (True, "ok"),
         "pytest": (False, "[tests/test_x.py]\n1 failed")},
        "FAIL"),
}


def _verdict_line(capsys: Any, results: dict[str, tuple[bool, str]]) -> str:
    _report("test", results)
    out = capsys.readouterr().out
    return next(line for line in out.splitlines() if line.startswith("gate_hook["))


def test_report_verdict_names_each_scenario_correctly(capsys: Any) -> None:
    for name, (results, expected_prefix) in _REPORT_SCENARIOS.items():
        line = _verdict_line(capsys, results)
        assert expected_prefix in line, f"{name}: expected {expected_prefix!r} in {line!r}"


def test_report_verdict_never_reads_identical_for_clean_vs_skipped(capsys: Any) -> None:
    """THE EXACT "17/18 PASS" BUG (Thoth's msg 2961, the sharpest of the four specimens
    named that dispatch), stated as a direct comparison rather than trusting the prefix
    check above alone: a human or a downstream reader comparing these two lines must
    never see them collapse to the identical word."""
    clean = _verdict_line(capsys, _REPORT_SCENARIOS["everything genuinely ran and passed"][0])
    skipped = _verdict_line(
        capsys, _REPORT_SCENARIOS["pytest was skipped over cap, ruff/mypy clean"][0])
    assert clean != skipped


def _fossil_report_verdict(results: dict[str, tuple[bool, str]]) -> str:
    """THE PRE-0044671 SHAPE, reproduced from `git show e42426f:scripts/gate_hook.py`
    (this repo's OWN original commit for this file, before either fix landed) — the
    verdict line's entire logic was `"PASS" if all_ok else "FAIL"`, with no third state
    at all. Never imported by real code."""
    all_ok = all(ok for ok, _ in results.values())
    return "PASS" if all_ok else "FAIL"


def test_the_original_report_shape_collapses_clean_and_skipped_into_one_word() -> None:
    """The second negative control — proves the "17/18 pass" bug was real and reproduces
    it directly from this file's own git history, not from a paraphrase of it."""
    clean = _fossil_report_verdict(_REPORT_SCENARIOS["everything genuinely ran and passed"][0])
    skipped = _fossil_report_verdict(
        _REPORT_SCENARIOS["pytest was skipped over cap, ruff/mypy clean"][0])
    assert clean == skipped == "PASS", (
        "expected the original shape to render an identical 'PASS' for both a genuine "
        "run and a fully-skipped one — if it no longer does, this negative control "
        "needs re-deriving from the real diff")
