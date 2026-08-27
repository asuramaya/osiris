"""adoption_meter(pool) — the #189 acceptance instrument (Thoth msg 5825, obligation
8d510875): read-only, filtered off the same triage(mode='census') the obligation's own
baseline was measured with, never a second query reinventing that SQL."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from src.actions.core import Actions
from src.orchestrator.adoption_meter import (
    _FIXED_BASELINE,
    ADOPTION_BASELINE_KEY,
    adoption_meter,
    render_adoption_line,
)
from src.orchestrator.monitor import get_cursor, set_cursor

NOW = datetime(2026, 8, 27, tzinfo=UTC)


async def test_current_census_scopes_to_decision_thread_reference_active_only(
    actions: Actions,
) -> None:
    orphan_d = await actions.create_or_find_object("Decision", "decision:orphan1", "test")
    linked_d = await actions.create_or_find_object("Decision", "decision:linked1", "test")
    thr = await actions.create_or_find_object("Thread", "thread:unrelated", "test")
    await actions.create_link(linked_d, thr, "answers", "test", NOW, 0.9)
    await actions.create_or_find_object("Reference", "ref:present", "test")
    del orphan_d

    meter = await adoption_meter(actions.pool)
    dec = meter["current"]["Decision"]
    assert dec["n"] >= 2
    assert dec["orphans"] >= 1
    # File/Type/Person never leak into the scoped output
    assert set(meter["current"].keys()) == {"Decision", "Thread", "Reference"}


async def test_baseline_seeds_once_and_is_a_durable_row(actions: Actions) -> None:
    assert await get_cursor(actions.pool, ADOPTION_BASELINE_KEY) is None

    first = await adoption_meter(actions.pool)
    assert first["baseline"]["types"]["Decision"]["median_links"] == 1.0
    raw = await get_cursor(actions.pool, ADOPTION_BASELINE_KEY)
    assert raw is not None  # a row in watermarks now, not just an in-memory constant

    # a second call never re-seeds — the SAME baseline row survives a fresh call, exactly
    # the "a row, not a sentence" property: nothing re-derives or overwrites it silently.
    second = await adoption_meter(actions.pool)
    assert second["baseline"] == first["baseline"]
    assert await get_cursor(actions.pool, ADOPTION_BASELINE_KEY) == raw


async def test_delta_is_computed_against_the_seeded_baseline(actions: Actions) -> None:
    await set_cursor(actions.pool, ADOPTION_BASELINE_KEY, __import__("json").dumps({
        "taken_at": "2026-08-27T15:03:00+00:00", "source": "test-seed",
        "types": {"Decision": {"n": 0, "orphans": 0, "thin": 0, "median_links": 1.0},
                  "Thread": {"n": 0, "orphans": 0, "thin": 0, "median_links": 1.0},
                  "Reference": {"n": 0, "orphans": 0, "thin": 0, "median_links": None}},
    }))
    await actions.create_or_find_object("Decision", "decision:fresh1", "test")
    await actions.create_or_find_object("Decision", "decision:fresh2", "test")
    await actions.create_or_find_object("Reference", "ref:fresh1", "test")

    meter = await adoption_meter(actions.pool)
    assert meter["delta"]["Decision"]["orphans_delta"] == 2
    assert meter["delta"]["Reference"]["median_links_delta"] is None  # baseline never had one


async def test_hatch_reads_a_real_zero_when_nothing_has_hatched(actions: Actions) -> None:
    """No `unlinked_because` assertions exist anywhere in a fresh test DB — this is the
    real fail-honest path (Imhotep's gate, imhotep-189-declare-or-refuse, is unmerged as of
    this test), never a fabricated schema-missing flag."""
    meter = await adoption_meter(actions.pool)
    assert meter["hatch"]["total"] == 0
    assert meter["hatch"]["by_reason_raw"] == {}
    assert "note" in meter["hatch"]  # the caveat travels even on a real zero


async def test_hatch_counts_a_real_assertion_and_splits_against_the_live_constant(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`unlinked_because` is an ordinary property assertion (Imhotep msg 5828) — this
    writes one for real via `assert_property`, then monkeypatches
    `src.mcp_server._EXTENSION_LINK_PENDING_REASON` (imported live, per this module's own
    docstring) to prove the split actually separates the two populations rather than
    guessing at a hardcoded copy of a string Imhotep named as still liable to move."""
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


async def test_render_adoption_line_is_one_line_and_names_the_split_hatch(
    actions: Actions,
) -> None:
    """Post-merge (gate landed at 45b42cd) the reason constant IS importable, so the live
    render NAMES BOTH BUCKETS. This assertion inverted the day the gate merged, and that is
    the mechanism working: an extension-link-only write is now distinguishable from a
    genuinely standalone one, which is the whole reason Thoth required the split."""
    meter = await adoption_meter(actions.pool)
    line = render_adoption_line(meter)
    assert "\n" not in line
    assert line.startswith("adoption189:")
    assert "extension=" in line and "standalone=" in line
    assert "unsplit" not in line


def test_render_degrades_to_unsplit_when_the_reason_constant_is_gone() -> None:
    """The `split is None` branch is NOT dead code — it is what this instrument does on a
    build where the constant was renamed or removed. Rendered from a synthetic meter rather
    than by breaking the real import, so it stays a test of the RENDERER, not of import
    machinery. Without this the branch would go uncovered the moment the gate landed."""
    line = render_adoption_line({
        "current": {}, "baseline": None,
        "hatch": {"total": 7, "by_reason_raw": {}, "split": None, "note": ""},
    })
    assert "\n" not in line
    assert "7 total (unsplit" in line


async def test_fixed_baseline_matches_the_obligation_8d510875_numbers() -> None:
    """A transcription guard: if this constant ever drifts from what the obligation
    actually published, this is the one place that would catch it."""
    dec = _FIXED_BASELINE["types"]["Decision"]
    assert (dec["n"], dec["orphans"], dec["thin"], dec["median_links"]) == (5478, 274, 4463, 1.0)
    thr = _FIXED_BASELINE["types"]["Thread"]
    assert (thr["n"], thr["orphans"], thr["thin"], thr["median_links"]) == (3465, 232, 3019, 1.0)
    ref = _FIXED_BASELINE["types"]["Reference"]
    assert (ref["n"], ref["orphans"], ref["thin"]) == (300, 112, 102)
