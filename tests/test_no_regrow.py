"""THE NO-REGROW RULE (operator's word via Thoth DM 8606/8618, 2026-09-09): an open
obligation Thread with no annotate/owner-change/resolution for N_GRACE_DAYS past its own
`stale_after` reclassifies to kind='task', status untouched, with a receipt attempted on
the owner's mail. A SEPARATE clock from obligation_hygiene.py's own no-regrow leg (tested
in test_obligation_hygiene.py) -- every test here proves one boundary of THIS clock: the
grace window, the touched-since-stale reset, or the write/no-write split.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from src.actions.core import Actions
from src.config.settings import Settings
from src.orchestrator.no_regrow import (
    N_GRACE_DAYS,
    apply_no_regrow,
    no_regrow_scheduled_tick,
    plan_no_regrow,
)

NOW = datetime.now(UTC)
_SRC = "test-source"


async def _mk_obligation(
    actions: Actions, canonical: str, *, owner: str | None = None,
    stale_after: datetime | None = None, touched_at: datetime | None = None,
) -> Any:
    """A Thread whose `kind`/`status`/`summary`/`owner` are written at `touched_at`
    (defaulting to NOW, i.e. "never touched since going stale" when `stale_after` is
    older) and whose `stale_after` is written separately so its own observed_at never
    counts as a touch on the thread's OTHER fields (matching how open_thread's real
    stale_after write already works — a distinct assertion, not bundled with summary/
    status)."""
    touched_at = touched_at or NOW
    t = await actions.create_or_find_object("Thread", canonical, _SRC)
    await actions.assert_property(t, "summary", canonical, _SRC, touched_at, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(t, "status", "open", _SRC, touched_at, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(t, "kind", "obligation", _SRC, touched_at, 0.9,
                                  evidence_class="self_declared")
    if owner:
        await actions.assert_property(t, "owner", owner, _SRC, touched_at, 0.9,
                                      evidence_class="self_declared")
    if stale_after is not None:
        await actions.assert_property(t, "stale_after", stale_after.isoformat(), _SRC,
                                      touched_at, 0.9, evidence_class="self_declared")
    return t


def _find(rows: list[dict[str, Any]], tid: Any) -> dict[str, Any] | None:
    return next((r for r in rows if r["thread_id"] == str(tid)), None)


# ═══ candidacy: only threads carrying a stale_after are ever considered ═════════════════

async def test_an_obligation_with_no_stale_after_at_all_is_never_a_candidate(
    actions: Actions,
) -> None:
    t = await _mk_obligation(actions, "noregrow-no-window", owner="agent:nr1")
    out = await plan_no_regrow(actions.pool, now=NOW)
    assert _find(out["would_reclassify"], t) is None
    assert _find(out["no_action"], t) is None


# ═══ the grace window itself ═════════════════════════════════════════════════════════

async def test_past_stale_after_but_inside_the_grace_window_takes_no_action(
    actions: Actions,
) -> None:
    stale_after = NOW - timedelta(days=N_GRACE_DAYS - 1)
    old_touch = stale_after - timedelta(days=1)
    t = await _mk_obligation(actions, "noregrow-in-grace", owner="agent:nr2",
                             stale_after=stale_after, touched_at=old_touch)
    out = await plan_no_regrow(actions.pool, now=NOW)
    row = _find(out["no_action"], t)
    assert row is not None
    assert "grace window" in row["reason"]


async def test_past_stale_after_and_past_the_grace_window_with_no_touch_would_reclassify(
    actions: Actions,
) -> None:
    stale_after = NOW - timedelta(days=N_GRACE_DAYS + 1)
    old_touch = stale_after - timedelta(days=1)
    t = await _mk_obligation(actions, "noregrow-past-grace", owner="agent:nr3",
                             stale_after=stale_after, touched_at=old_touch)
    out = await plan_no_regrow(actions.pool, now=NOW)
    assert _find(out["would_reclassify"], t) is not None
    assert _find(out["no_action"], t) is None


# ═══ a genuine touch after stale_after resets the window ════════════════════════════════

async def test_a_touch_after_stale_after_resets_the_window_even_when_old(
    actions: Actions,
) -> None:
    """The thread went stale long ago, but a mind annotated/reclassified/re-owned it
    AFTER that — a real touch, not the sweep's own write. Never reclassified, no matter
    how far stale_after itself now sits in the past."""
    stale_after = NOW - timedelta(days=N_GRACE_DAYS + 30)
    recent_touch = NOW - timedelta(days=1)
    t = await _mk_obligation(actions, "noregrow-touched-since", owner="agent:nr4",
                             stale_after=stale_after, touched_at=recent_touch)
    out = await plan_no_regrow(actions.pool, now=NOW)
    row = _find(out["no_action"], t)
    assert row is not None
    assert "touched since going stale" in row["reason"]
    assert _find(out["would_reclassify"], t) is None


# ═══ dry run vs. execute ═════════════════════════════════════════════════════════════

async def test_dry_run_never_writes(actions: Actions) -> None:
    stale_after = NOW - timedelta(days=N_GRACE_DAYS + 1)
    old_touch = stale_after - timedelta(days=1)
    t = await _mk_obligation(actions, "noregrow-dry-run", owner="agent:nr5",
                             stale_after=stale_after, touched_at=old_touch)
    out = await apply_no_regrow(actions, execute=False, now=NOW)
    assert out["execute"] is False
    assert _find(out["would_reclassify"], t) is not None

    row = await actions.pool.fetchrow(
        "SELECT a.value #>> '{}' AS kind FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id WHERE o.id=$1 AND a.name='kind' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", t)
    assert row["kind"] == "obligation"


async def test_execute_reclassifies_to_task_and_leaves_status_open(
    actions: Actions,
) -> None:
    stale_after = NOW - timedelta(days=N_GRACE_DAYS + 1)
    old_touch = stale_after - timedelta(days=1)
    t = await _mk_obligation(actions, "noregrow-execute", owner="agent:nr6",
                             stale_after=stale_after, touched_at=old_touch)
    out = await apply_no_regrow(actions, execute=True, now=NOW)
    assert len(out["reclassified"]) == 1
    assert out["reclassified"][0]["thread"] == str(t)

    row = await actions.pool.fetchrow(
        "SELECT "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='kind' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS kind, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS status "
        "FROM objects o WHERE o.id=$1", t)
    assert row["kind"] == "task"
    assert row["status"] == "open"


async def test_execute_never_reclassifies_a_row_still_inside_the_grace_window(
    actions: Actions,
) -> None:
    stale_after = NOW - timedelta(days=N_GRACE_DAYS - 1)
    old_touch = stale_after - timedelta(days=1)
    t = await _mk_obligation(actions, "noregrow-execute-in-grace", owner="agent:nr7",
                             stale_after=stale_after, touched_at=old_touch)
    out = await apply_no_regrow(actions, execute=True, now=NOW)
    assert not out["reclassified"]

    row = await actions.pool.fetchrow(
        "SELECT a.value #>> '{}' AS kind FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id WHERE o.id=$1 AND a.name='kind' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", t)
    assert row["kind"] == "obligation"


# ═══ the scheduled leg's own kill switch ═════════════════════════════════════════════

async def test_scheduled_tick_is_on_by_default_and_can_be_darkened(actions: Actions) -> None:
    """Operator 2026-09-09: "make it 7 days, flip it on" — the default is ON; the kill
    switch still darkens it."""
    stale_after = NOW - timedelta(days=N_GRACE_DAYS + 1)
    old_touch = stale_after - timedelta(days=1)
    await _mk_obligation(actions, "noregrow-dark-default", owner="agent:nr8",
                         stale_after=stale_after, touched_at=old_touch)
    assert Settings().osiris_no_regrow_enabled is True
    settings = Settings(osiris_no_regrow_enabled=False)
    out = await no_regrow_scheduled_tick(actions, settings=settings, now=NOW)
    assert out["enabled"] is False
    assert out["reclassified"] == []


async def test_scheduled_tick_acts_when_enabled(actions: Actions) -> None:
    stale_after = NOW - timedelta(days=N_GRACE_DAYS + 1)
    old_touch = stale_after - timedelta(days=1)
    t = await _mk_obligation(actions, "noregrow-enabled", owner="agent:nr9",
                             stale_after=stale_after, touched_at=old_touch)
    settings = Settings(osiris_no_regrow_enabled=True)
    out = await no_regrow_scheduled_tick(actions, settings=settings, now=NOW)
    assert out["enabled"] is True
    assert _find(out["reclassified"], t) is not None
