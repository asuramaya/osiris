"""adoption_meter(pool) — the #189 acceptance instrument (Thoth msg 5825/5866). The old
headline metric (population-wide `median_links` snapshot, obligation 8d510875) was retired
as structurally unreachable (Khnum's decision b71e1e0dcadf — a Thread/Reference cannot
declare a forward link at birth, so their connectivity is entirely later-accrued and a
snapshot of an always-young population never moves). Replaced with cohort-aged
connectivity: does a birth-week cohort's own median link count climb between birth and
day 30 — a FIXED historical fact once a cohort has genuinely aged 30 days, needing no
persisted baseline (see the module's own docstring)."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from src.actions.core import Actions
from src.orchestrator.adoption_meter import (
    HEADLINE_TYPE,
    adoption_meter,
    render_adoption_line,
)

# LIVE, NOT FROZEN (corrected 2026-08-31, thread 1ef3a6e1). This was
# `datetime(2026, 8, 27, tzinfo=UTC)` — a fixed instant — while every eligibility gate in
# adoption_meter.py compares against the DATABASE's own now(). So the fixture aged
# backwards from a frozen August 27 while the code measured against real time, and every
# `born_days_ago` in this file silently meant something older with each day that passed.
# It went RED on 2026-08-31 with no code change at all: a decision declared 2 days old was
# 6 days old by the gate's reckoning, and the UTC week rollover tipped it over the line.
# THE TEST WAS DRIFTING, NOT THE CODE. A fixture clock that does not advance with the
# clock the subject reads is a detector with a duty cycle — it fired one day in seven and
# would have gone quiet again by Tuesday on its own.
NOW = datetime.now(UTC)


async def _backdate_object(actions: Actions, obj_id: object, created_at: datetime) -> None:
    await actions.pool.execute(
        "UPDATE objects SET created_at=$1 WHERE id=$2", created_at, obj_id)


async def _backdate_link(actions: Actions, link_id: int, created_at: datetime) -> None:
    await actions.pool.execute(
        "UPDATE links SET created_at=$1 WHERE id=$2", created_at, link_id)


async def _aged_decision(
    actions: Actions, canonical: str, *, born_days_ago: int, birth_links: int = 1,
    later_link_at_days: int | None = None,
) -> None:
    """A Decision born `born_days_ago` days ago, linked to a fresh Thread at birth
    (`birth_links` copies — only 0 or 1 meaningful here, since one link is enough to prove
    the birth checkpoint counts it), and optionally a SECOND link added
    `later_link_at_days` after birth (still within the object's own life, whatever that
    means for the test) — the growth this metric exists to detect."""
    born = NOW - timedelta(days=born_days_ago)
    dec = await actions.create_or_find_object("Decision", canonical, "test")
    await _backdate_object(actions, dec, born)
    if birth_links:
        thr = await actions.create_or_find_object("Thread", f"{canonical}-t0", "test")
        lid = await actions.create_link(dec, thr, "answers", "test", NOW, 0.9)
        await _backdate_link(actions, lid, born)
    if later_link_at_days is not None:
        thr2 = await actions.create_or_find_object("Thread", f"{canonical}-t1", "test")
        lid2 = await actions.create_link(dec, thr2, "answers", "test", NOW, 0.9)
        await _backdate_link(actions, lid2, born + timedelta(days=later_link_at_days))


async def test_cohort_reports_no_growth_when_only_the_birth_link_ever_lands(
    actions: Actions,
) -> None:
    await _aged_decision(actions, "decision:flat1", born_days_ago=45)

    meter = await adoption_meter(actions.pool)
    dec = meter["cohorts"]["Decision"]
    assert dec["n"] >= 1
    assert dec["median_at_birth"] == dec["median_at_30d"] == 1.0


async def test_cohort_reports_growth_when_a_link_lands_within_the_30_day_window(
    actions: Actions,
) -> None:
    await _aged_decision(
        actions, "decision:grows1", born_days_ago=45, later_link_at_days=20)

    meter = await adoption_meter(actions.pool)
    dec = meter["cohorts"]["Decision"]
    assert dec["median_at_birth"] == 1.0
    assert dec["median_at_30d"] == 2.0  # the day-20 link counts; it landed inside 30 days


async def test_cohort_excludes_a_link_that_lands_after_the_30_day_window(
    actions: Actions,
) -> None:
    await _aged_decision(
        actions, "decision:toolate1", born_days_ago=45, later_link_at_days=40)

    meter = await adoption_meter(actions.pool)
    dec = meter["cohorts"]["Decision"]
    assert dec["median_at_30d"] == 1.0  # the day-40 link is outside the checkpoint


async def test_a_cohort_not_yet_30_days_old_is_absent_not_zero(actions: Actions) -> None:
    """A recently-born object must never render as a bad (flat/zero) 30-day figure — it
    simply has not aged into eligibility yet, and this must read as absence, not failure.
    The only Decision in this test's own isolated DB is 10 days old, so no cohort has
    reached the 30-day checkpoint at all."""
    await _aged_decision(actions, "decision:toosoon1", born_days_ago=10)

    meter = await adoption_meter(actions.pool)
    assert "Decision" not in meter["cohorts"]


async def test_trend_compares_the_two_newest_eligible_cohorts(actions: Actions) -> None:
    # two distinct ISO weeks, both >=30 days old, so both are 30-day-eligible and
    # distinguishable as separate cohort_week buckets.
    await _aged_decision(actions, "decision:cohortA", born_days_ago=90)
    await _aged_decision(
        actions, "decision:cohortB", born_days_ago=45, later_link_at_days=5)

    meter = await adoption_meter(actions.pool)
    dec = meter["cohorts"]["Decision"]
    assert "prev_week" in dec
    assert "trend_30d_delta" in dec
    # the NEWER cohort (45 days ago) is the headline; the OLDER (90 days ago) is prev
    assert dec["week"] != dec["prev_week"]


async def test_orphan_birth_rate_reports_zero_when_every_member_links_within_7_days(
    actions: Actions,
) -> None:
    await _aged_decision(actions, "decision:obr-ok1", born_days_ago=10)

    meter = await adoption_meter(actions.pool)
    dec = meter["orphan_birth_rate"]["Decision"]
    assert dec["n"] >= 1
    assert dec["n_orphan"] == 0
    assert dec["rate"] == 0.0


async def test_orphan_birth_rate_counts_a_member_with_no_link_within_7_days(
    actions: Actions,
) -> None:
    await _aged_decision(actions, "decision:obr-late1", born_days_ago=10,
                          birth_links=0, later_link_at_days=8)

    meter = await adoption_meter(actions.pool)
    dec = meter["orphan_birth_rate"]["Decision"]
    assert dec["n_orphan"] == 1
    assert dec["rate"] == 1.0


async def test_orphan_birth_rate_is_unmoved_by_a_link_landing_after_the_7_day_window(
    actions: Actions,
) -> None:
    """The whole point of the bound: a LATER backfill (any lane) minting a link on an
    already-eligible cohort must never revise that cohort's own reported rate — a fixed
    historical fact, same discipline as `_cohort_connectivity`'s own 30-day checkpoint."""
    await _aged_decision(actions, "decision:obr-backfilled1", born_days_ago=45,
                          birth_links=0, later_link_at_days=30)

    meter = await adoption_meter(actions.pool)
    dec = meter["orphan_birth_rate"]["Decision"]
    assert dec["n_orphan"] == 1  # still counted orphan — the day-30 link is outside 7d
    assert dec["rate"] == 1.0


async def test_orphan_birth_rate_absent_when_no_cohort_has_reached_7_days(
    actions: Actions,
) -> None:
    await _aged_decision(actions, "decision:obr-toosoon1", born_days_ago=2)

    meter = await adoption_meter(actions.pool)
    assert "Decision" not in meter["orphan_birth_rate"]


async def test_orphan_birth_rate_trend_compares_the_two_newest_eligible_weeks(
    actions: Actions,
) -> None:
    await _aged_decision(actions, "decision:obr-trendA", born_days_ago=90, birth_links=0)
    await _aged_decision(actions, "decision:obr-trendB", born_days_ago=45)

    meter = await adoption_meter(actions.pool)
    dec = meter["orphan_birth_rate"]["Decision"]
    assert "prev_week" in dec
    assert "trend_delta" in dec
    assert dec["week"] != dec["prev_week"]


async def test_render_adoption_line_names_the_orphan_birth_headline(
    actions: Actions,
) -> None:
    await _aged_decision(actions, "decision:obr-render1", born_days_ago=10)

    meter = await adoption_meter(actions.pool)
    line = render_adoption_line(meter)
    assert "\n" not in line
    assert "orphan_birth: Decision" in line
    assert "%" in line


def test_render_reports_orphan_birth_absence_honestly_when_no_cohort_is_eligible() -> None:
    line = render_adoption_line({
        "cohorts": {},
        "orphan_birth_rate": {},
        "hatch": {"total": 0, "by_reason_raw": {}, "split": None, "note": ""},
    })
    assert "\n" not in line
    assert "no 7d-aged cohort yet" in line


def test_render_orphan_birth_degrades_honestly_when_the_key_is_entirely_absent() -> None:
    """A meter dict built before this instrument existed (or a caller that only wants the
    old two fields) must not crash the renderer — same "absence, not failure" contract
    as the cohort half already has."""
    line = render_adoption_line({
        "cohorts": {},
        "hatch": {"total": 0, "by_reason_raw": {}, "split": None, "note": ""},
    })
    assert "\n" not in line
    assert "no 7d-aged cohort yet" in line


async def test_hatch_reads_a_real_zero_when_nothing_has_hatched(actions: Actions) -> None:
    """No `unlinked_because` assertions exist anywhere in a fresh test DB — the real
    fail-honest path, never a fabricated schema-missing flag."""
    meter = await adoption_meter(actions.pool)
    assert meter["hatch"]["total"] == 0
    assert meter["hatch"]["by_reason_raw"] == {}
    assert "note" in meter["hatch"]


async def test_hatch_counts_a_real_assertion_and_splits_against_the_live_constant(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`unlinked_because` is an ordinary property assertion (Imhotep msg 5828) — this
    writes one for real via `assert_property`, then monkeypatches
    `src.mcp_server._EXTENSION_LINK_PENDING_REASON` (imported live, per this module's own
    docstring) to prove the split actually separates the two populations."""
    import src.mcp_server as srv

    obj = await actions.create_or_find_object("Decision", "decision:hatch1", "test")
    await actions.assert_property(
        obj, "unlinked_because", "extension-link-pending (fake)", "test", NOW, 0.9,
        evidence_class="self_declared")
    obj2 = await actions.create_or_find_object("Decision", "decision:hatch2", "test")
    await actions.assert_property(
        obj2, "unlinked_because", "a genuinely standalone reason", "test", NOW, 0.9,
        evidence_class="self_declared")
    monkeypatch.setattr(srv, "_EXTENSION_LINK_PENDING_REASON",
                        "extension-link-pending (fake)", raising=False)

    meter = await adoption_meter(actions.pool)
    assert meter["hatch"]["total"] == 2
    assert meter["hatch"]["split"] == {"extension_link_pending": 1, "standalone_other": 1}


async def test_render_adoption_line_names_the_headline_cohort(actions: Actions) -> None:
    assert HEADLINE_TYPE == "Decision"
    await _aged_decision(
        actions, "decision:renderme1", born_days_ago=45, later_link_at_days=10)

    meter = await adoption_meter(actions.pool)
    line = render_adoption_line(meter)
    assert "\n" not in line
    assert line.startswith("adoption189: Decision cohort")
    assert "birth=" in line and "30d=" in line


def test_render_reports_absence_honestly_when_no_cohort_is_eligible_yet() -> None:
    line = render_adoption_line({
        "cohorts": {},
        "hatch": {"total": 0, "by_reason_raw": {}, "split": None, "note": ""},
    })
    assert "\n" not in line
    assert "no 30-day-aged cohort yet" in line


def test_render_degrades_to_unsplit_when_the_reason_constant_is_gone() -> None:
    """The `split is None` branch is NOT dead code — it is what this instrument does on a
    build where the constant was renamed or removed. Rendered from a synthetic meter
    rather than by breaking the real import, so it stays a test of the RENDERER."""
    line = render_adoption_line({
        "cohorts": {},
        "hatch": {"total": 7, "by_reason_raw": {}, "split": None, "note": ""},
    })
    assert "\n" not in line
    assert "7 total (unsplit" in line
