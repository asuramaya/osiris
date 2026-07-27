"""The response budget — the waist that no tool escapes.

The bug class this closes (b228ab49): a tool answers correctly and enormously, and the answer
kills the context it was meant to inform. The rule under test is not 'results are small' — it
is that a result which does NOT fit comes back TRUNCATED AND SAYING SO. A silent cap is the
only outcome that would be worse than the crash, because the caller would believe it.
"""

from __future__ import annotations

import json

from src.orchestrator.budget import BUDGET_CHARS, MIN_KEEP, fit


def _big(n: int) -> list[dict[str, str]]:
    return [{"agent": f"agent:{i:08d}", "project": "osiris", "note": "x" * 60} for i in range(n)]


def test_small_result_passes_through_untouched() -> None:
    # the overwhelming case: the bound must be invisible when it is not needed
    result = {"count": 3, "rows": _big(3)}
    assert fit(result, tool="t") == result
    assert "_bounded" not in fit(result, tool="t")


def test_oversized_result_is_trimmed_to_fit() -> None:
    result = {"count": 5000, "rows": _big(5000)}
    out = fit(result, tool="fleet")
    assert len(json.dumps(out, default=str)) <= BUDGET_CHARS
    assert len(out["rows"]) < 5000


def test_a_trim_is_never_silent() -> None:
    out = fit({"rows": _big(5000)}, tool="fleet")
    bounded = out["_bounded"]
    assert bounded["tool"] == "fleet"
    assert bounded["dropped"]["rows"]["of"] == 5000          # the WHOLE is still reported
    assert bounded["dropped"]["rows"]["shown"] == len(out["rows"])
    assert "not the whole" in bounded["note"].lower()


def test_counts_survive_the_trim() -> None:
    """The lens under-SHOWS; it must never under-COUNT — a scalar is cheap and load-bearing."""
    out = fit({"count": 5000, "live": 11, "rows": _big(5000)}, tool="fleet")
    assert out["count"] == 5000
    assert out["live"] == 11


def test_the_biggest_firehose_is_cut_first() -> None:
    """One runaway stream loses its tail; the small streams stay WHOLE.

    The alternative — trimming everything by an equal fraction — would corrupt every stream in
    the result to save the one that misbehaved.
    """
    out = fit({"danger": _big(4), "roster": _big(5000)}, tool="fleet_digest")
    assert out["danger"] == _big(4)                          # untouched
    assert "danger" not in out["_bounded"]["dropped"]
    assert out["_bounded"]["dropped"]["roster"]["of"] == 5000


def test_nested_lists_are_reachable() -> None:
    out = fit({"summary": {"agents": 5000}, "streams": {"roster": _big(5000)}}, tool="t")
    assert out["summary"]["agents"] == 5000
    assert out["_bounded"]["dropped"]["streams.roster"]["of"] == 5000


def test_a_trimmed_list_still_shows_its_shape() -> None:
    """Cut to nothing, a list teaches nothing — the reader cannot even see what a row IS."""
    out = fit({"rows": [{"blob": "x" * 100_000} for _ in range(10)]}, tool="t")
    assert len(out["rows"]) >= MIN_KEEP


def test_a_monstrous_string_is_bounded_too() -> None:
    """Lists are the usual firehose, not the only one — a giant render is one as well."""
    out = fit({"tree": "line\n" * 200_000}, tool="fleet")
    assert len(json.dumps(out, default=str)) <= BUDGET_CHARS
    assert out["tree"].endswith("… [truncated]")
    assert out["_bounded"]["dropped"]["tree"]["of"] > BUDGET_CHARS


def test_the_tool_result_is_not_mutated_in_place() -> None:
    """A lens reports on the record; it does not edit what the tool actually computed."""
    result = {"rows": _big(5000)}
    fit(result, tool="t")
    assert len(result["rows"]) == 5000


def test_non_dict_results_pass_through() -> None:
    assert fit("just a string", tool="t") == "just a string"
    assert fit(None, tool="t") is None


# --- task #64's own live measurement (ruling ad19a779): MANY SMALL-BUT-VERBOSE rows defeat
# both phases above — every list already ≤MIN_KEEP, every string already ≤MAX_STR, yet the
# SUM stays over budget. The old code returned silently over budget in this shape (empty
# `dropped` never sets `_bounded` at all) — exactly the "hides what it dropped" lie the
# module's own docstring forbids, just at one remove (it hid that it didn't cap). ------------

def _many_small_verbose_groups(n: int, per_group: int = MIN_KEEP) -> dict[str, list[dict]]:
    """`n` groups, each already AT the keep-floor (never a list-halving candidate), each row
    individually well under MAX_STR — only the SUM is a firehose."""
    return {f"group{g}": [{"summary": "x" * 2000} for _ in range(per_group)]
            for g in range(n)}


def test_many_small_verbose_lists_are_named_honestly_when_fit_cannot_reduce_further() -> None:
    result = {"items": _many_small_verbose_groups(30)}
    assert len(json.dumps(result, default=str)) > BUDGET_CHARS  # confirms the fixture is real
    out = fit(result, tool="t")
    assert len(json.dumps(out, default=str)) > BUDGET_CHARS  # genuinely could not be reduced
    assert out["_bounded"]["still_over_budget"] is True
    assert "cannot reduce further" in out["_bounded"]["note"]
    # nothing was silently cut either — every group is still fully present, untouched
    assert all(len(g) == MIN_KEEP for g in out["items"].values())


def test_a_normal_successful_trim_never_claims_still_over_budget() -> None:
    """The new honesty flag must never leak into the ORDINARY successful-trim case."""
    out = fit({"rows": _big(5000)}, tool="fleet")
    assert len(json.dumps(out, default=str)) <= BUDGET_CHARS
    assert "still_over_budget" not in out["_bounded"]
