"""Fix (e), Metron's mechanism report (mail 8890/8921/8922): the fleet-wide audit the
report itself asked for — every active Thread whose newest note post-dates its own last
summary correction, sized in one query, and surfaced as graph_lint's own
'contested-summary' check.
"""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.capture import annotate_thread, correct_thread_summary, open_thread
from src.orchestrator.compositions import _fn_lint, contested_summary_audit


async def test_audit_counts_a_contested_thread(actions: Actions) -> None:
    t = await open_thread(actions, "nothing renders the gain reduction",
                          source="agent:audit1")
    await annotate_thread(actions, str(t), "checked: MasterBand.tsx DOES render it")

    out = await contested_summary_audit(actions.pool)

    assert out["total"] == 1
    assert out["rows"][0]["id"] == t


async def test_audit_never_counts_an_uncontested_thread(actions: Actions) -> None:
    await open_thread(actions, "a perfectly settled thread", source="agent:audit2")

    out = await contested_summary_audit(actions.pool)

    assert out["total"] == 0


async def test_audit_stops_counting_once_the_summary_is_corrected(actions: Actions) -> None:
    t = await open_thread(actions, "nothing renders the gain reduction",
                          source="agent:audit3")
    await annotate_thread(actions, str(t), "checked: MasterBand.tsx DOES render it")
    await correct_thread_summary(
        actions, str(t), "MasterBand.tsx renders the gain reduction via .mband-gr")

    out = await contested_summary_audit(actions.pool)

    assert out["total"] == 0


async def test_graph_lint_surfaces_the_contested_summary_check(actions: Actions) -> None:
    t = await open_thread(actions, "nothing renders the gain reduction",
                          source="agent:audit4")
    await annotate_thread(actions, str(t), "checked: MasterBand.tsx DOES render it")

    result = await _fn_lint(actions.pool, None, {})

    assert result["counts"].get("contested-summary", 0) >= 1
    finding = next(f for f in result["findings"] if f["check"] == "contested-summary")
    assert finding["severity"] == "warn"
