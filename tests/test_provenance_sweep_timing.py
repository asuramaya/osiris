"""The provenance sweep says which lane is slow, even when the 60 s budget cancels it."""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest
from src.orchestrator import capture


async def _quick(*a: Any, **k: Any) -> dict[str, Any]:
    return {"to_mint": 1, "to_abstain": 2}


@pytest.fixture
def lanes(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("resolve_agent_orphans", "backfill_lineage_repo_links",
                 "backfill_lineage_repo_links_at_write_time", "resolve_reference_orphans",
                 "resolve_practice_orphans", "resolve_superstition_orphans",
                 "resolve_seat_orphans", "resolve_project_orphans"):
        monkeypatch.setattr(capture, name, _quick)


async def test_the_sweep_logs_every_lanes_time_and_keeps_its_results(
    lanes: None, caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="osiris.capture")

    out = await capture.apply_provenance_sweep_heartbeat(object())  # type: ignore[arg-type]

    assert out["total_minted"] == 8 and out["total_abstained"] == 16
    line = next(r.getMessage() for r in caplog.records if "lane timings" in r.getMessage())
    for name in ("agent", "decision_thread", "reference", "practice", "superstition", "seat",
                 "project"):
        assert f"'{name}'" in line


async def test_a_slow_lane_gets_its_own_line(
    lanes: None, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(capture, "_SLOW_LANE_SECS", 0.0)
    caplog.set_level(logging.INFO, logger="osiris.capture")

    await capture.apply_provenance_sweep_heartbeat(object())  # type: ignore[arg-type]

    assert any("lane agent took" in r.getMessage() for r in caplog.records)


async def test_a_lane_that_is_cancelled_by_the_budget_says_where_it_stopped(
    lanes: None, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    async def stuck(*a: Any, **k: Any) -> dict[str, Any]:
        await asyncio.sleep(30)
        return {}

    monkeypatch.setattr(capture, "resolve_reference_orphans", stuck)
    caplog.set_level(logging.INFO, logger="osiris.capture")

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(
            capture.apply_provenance_sweep_heartbeat(object()), timeout=0.3)  # type: ignore[arg-type]

    msg = next(r.getMessage() for r in caplog.records if "was cancelled" in r.getMessage())
    assert "lane reference was cancelled" in msg and "agent" in msg  # the lanes done before it


async def test_one_lanes_failure_is_recorded_and_the_rest_still_run(
    lanes: None, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def broken(*a: Any, **k: Any) -> dict[str, Any]:
        raise RuntimeError("db hiccup")

    monkeypatch.setattr(capture, "resolve_practice_orphans", broken)

    out = await capture.apply_provenance_sweep_heartbeat(object())  # type: ignore[arg-type]

    assert "db hiccup" in out["lanes"]["practice_error"] and "seat" in out["lanes"]


# ---- the at-write-time lane works in resumable, budgeted slices -------------------------------


async def _ambiguous(actions: Any, tag: str) -> Any:
    from datetime import UTC, datetime

    from src.orchestrator.capture import open_thread

    now = datetime.now(UTC)
    g1 = await actions.create_or_find_object("Agent", f"agent:{tag}", "test")
    g2 = await actions.create_or_find_object("Agent", f"agent:{tag}-ii", "test")
    for gen, name in ((g1, "a"), (g2, "b")):
        proj = await actions.create_or_find_object("SoftwareProject", f"repo:{tag}{name}", "test")
        await actions.create_link(gen, proj, "works_in", "test", now, 0.9,
                                  evidence_class="self_declared")
    return await open_thread(actions, f"lineage that disagrees with itself {tag}",
                             source=f"agent:{tag}-iii")


async def _cursor(actions: Any) -> str | None:
    from src.orchestrator.monitor import get_cursor

    return await get_cursor(actions.pool, capture._AT_WRITE_TIME_CURSOR_KEY)


async def test_the_lane_works_through_the_leftovers_in_slices_and_then_starts_over(
    actions: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(capture, "_AT_WRITE_TIME_SLICE_ROWS", 2)
    threads = {str(await _ambiguous(actions, f"slice{i}")) for i in range(5)}
    seen: set[str] = set()
    calls = 0

    while True:
        out = await capture.backfill_lineage_repo_links_at_write_time(
            actions, actor="test", dry_run=False, because="test", resume=True)
        calls += 1
        seen |= {e["id"] for e in out["plan"]}
        assert out["scanned"] <= 2
        if not out["more_remain"]:
            break
        assert await _cursor(actions)  # the position is kept between slices
        assert calls < 10

    assert threads <= seen and calls >= 3  # every thread was reached, over several calls
    assert not await _cursor(actions)  # a finished pass clears the position
    again = await capture.backfill_lineage_repo_links_at_write_time(
        actions, actor="test", dry_run=False, because="test", resume=True)
    # the next pass starts over from the first leftover, but the threads it abstained on are
    # judged: it does not walk them again
    assert not ({e["id"] for e in again["plan"]} & threads)


async def test_a_budget_that_runs_out_stops_the_lane_and_keeps_the_position(
    actions: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.orchestrator.monitor import set_cursor

    for i in range(3):
        await _ambiguous(actions, f"budget{i}")
    first = await capture.backfill_lineage_repo_links_at_write_time(
        actions, actor="test", dry_run=False, because="test", resume=True, budget_secs=0.0)

    assert first["scanned"] == 0 and first["more_remain"] is True

    await set_cursor(actions.pool, capture._AT_WRITE_TIME_CURSOR_KEY,
                     "00000000-0000-0000-0000-000000000001")
    kept = await capture.backfill_lineage_repo_links_at_write_time(
        actions, actor="test", dry_run=False, because="test", resume=True, budget_secs=0.0)
    assert kept["more_remain"] is True
    assert await _cursor(actions) == "00000000-0000-0000-0000-000000000001"


async def test_without_resume_the_lane_scans_everything_as_before(actions: Any) -> None:
    for i in range(3):
        await _ambiguous(actions, f"plain{i}")

    out = await capture.backfill_lineage_repo_links_at_write_time(
        actions, actor="test", dry_run=True)

    assert out["scanned"] >= 3 and "more_remain" not in out
    assert not await _cursor(actions)  # a dry run touches no position


async def _second_summary(actions: Any, thread_id: Any, tag: str) -> None:
    """A second summary assertion on the thread from another source and time: its own row in
    the lane's result."""
    from datetime import UTC, datetime, timedelta

    await actions.assert_property(
        thread_id, "summary", f"amended {tag}", f"agent:{tag}-iii",
        datetime.now(UTC) + timedelta(seconds=5), 0.9)


async def test_a_slice_never_cuts_between_the_rows_of_one_object(
    actions: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One object with several summary rows used to be split by the row limit: the cursor was
    its id, so the rest of its rows were stepped past. A slice is a set of objects now, and each
    object arrives whole."""
    monkeypatch.setattr(capture, "_AT_WRITE_TIME_SLICE_ROWS", 3)  # odd: cuts mid-object by rows
    tags = [f"whole{i}" for i in range(4)]
    objs = {}
    for tag in tags:
        objs[tag] = await _ambiguous(actions, tag)
        await _second_summary(actions, objs[tag], tag)  # two rows for every object
    want = {str(v) for v in objs.values()}
    per_object: dict[str, int] = {}
    while True:
        out = await capture.backfill_lineage_repo_links_at_write_time(
            actions, actor="test", dry_run=True, resume=True)
        for e in out["plan"]:
            if e["id"] in want:
                per_object[e["id"]] = per_object.get(e["id"], 0) + 1
        if not out["more_remain"]:
            break
    assert set(per_object) == want
    assert all(n == 2 for n in per_object.values()), per_object  # no object split across slices


async def test_the_heartbeat_does_not_walk_objects_it_already_abstained_on(
    actions: Any,
) -> None:
    thread = str(await _ambiguous(actions, "judged"))
    first = await capture.backfill_lineage_repo_links_at_write_time(
        actions, actor="test", dry_run=False, because="test", resume=True)
    assert thread in {e["id"] for e in first["plan"] if e["verdict"] == "abstain"}
    second = await capture.backfill_lineage_repo_links_at_write_time(
        actions, actor="test", dry_run=False, because="test", resume=True)
    assert thread not in {e["id"] for e in second["plan"]}
    # a manual scan (no resume) still sees everything, as before
    manual = await capture.backfill_lineage_repo_links_at_write_time(
        actions, actor="test", dry_run=True)
    assert thread in {e["id"] for e in manual["plan"]}


def test_the_heartbeat_logs_counts_and_not_the_plans() -> None:
    big = {"lanes": {"agent": {"scanned": 98, "to_mint": 1, "to_abstain": 97,
                               "plan": [{"id": "x" * 40}] * 98},
                     "decision_thread_at_write_time": {"scanned": 3, "to_mint": 0,
                                                       "to_abstain": 3, "more_remain": True,
                                                       "plan": [{"id": "y"}] * 3},
                     "reference_error": "RuntimeError('db hiccup')"},
           "total_minted": 1, "total_abstained": 100}
    out = capture.sweep_summary(big)
    assert "plan" not in str(out) and len(str(out)) < 400
    assert out["lanes"]["agent"] == {"scanned": 98, "to_mint": 1, "to_abstain": 97}
    assert out["lanes"]["decision_thread_at_write_time"]["more_remain"] is True
    assert out["lanes"]["reference_error"] == "RuntimeError('db hiccup')"
    assert out["total_minted"] == 1
