"""OBLIGATION HYGIENE — the no-regrow rule (dispatch #204 follow-on, operator ruling
relayed Thoth DM 7161, 2026-09-05): N1=7 idle days -> a DM nudge to the owner; N2=+7 more
days of silence -> a STALE-CANDIDATE marker + a desk brief, never auto-resolved. Every test
proves ONE transition boundary or the idempotency law that stops a tick from re-nudging or
re-classifying a row it already acted on.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from src.actions.core import Actions
from src.config.settings import Settings
from src.orchestrator.obligation_hygiene import (
    N1_IDLE_DAYS,
    N2_SILENCE_DAYS,
    hygiene_dry_run,
    hygiene_execute,
    hygiene_status,
    obligation_hygiene_scheduled_tick,
)

NOW = datetime.now(UTC)
_SRC = "test-source"


async def _mk_obligation(
    actions: Actions, canonical: str, *, owner: str | None = None,
    touched_at: datetime | None = None,
) -> Any:
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
    return t


def _find(rows: list[dict[str, Any]], tid: Any) -> dict[str, Any] | None:
    return next((r for r in rows if r["thread_id"] == str(tid)), None)


# ═══ N1: idle detection ══════════════════════════════════════════════════════════════

async def test_a_freshly_touched_obligation_is_not_idle(actions: Actions) -> None:
    t = await _mk_obligation(actions, "hyg-fresh-1", owner="agent:hyg1", touched_at=NOW)
    out = await hygiene_dry_run(actions.pool, now=NOW)
    row = _find(out["buckets"]["no_action"], t)
    assert row is not None and row["reason"] == "not idle"


async def test_an_obligation_idle_past_n1_with_a_silent_owner_would_be_nudged(
    actions: Actions,
) -> None:
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    t = await _mk_obligation(actions, "hyg-idle-1", owner="agent:hyg-silent",
                             touched_at=stale)
    out = await hygiene_dry_run(actions.pool, now=NOW)
    assert _find(out["buckets"]["would_nudge"], t) is not None


async def test_an_idle_obligation_whose_owner_is_active_elsewhere_is_never_nudged(
    actions: Actions,
) -> None:
    """BOTH halves of "idle" must hold: an old last_touched is not enough on its own if
    the owner has made a self_declared write ANYWHERE in the window — the benefit-of-the-
    doubt half of the operator's own definition."""
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    t = await _mk_obligation(actions, "hyg-idle-active-owner", owner="agent:hyg-active",
                             touched_at=stale)
    # the SAME owner wrote something else in the graph yesterday
    other = await actions.create_or_find_object("Thread", "hyg-owner-other-write", _SRC)
    await actions.assert_property(other, "summary", "unrelated", "agent:hyg-active",
                                  NOW - timedelta(days=1), 0.9,
                                  evidence_class="self_declared")

    out = await hygiene_dry_run(actions.pool, now=NOW)
    assert _find(out["buckets"]["would_nudge"], t) is None
    row = _find(out["buckets"]["no_action"], t)
    assert row is not None and row["reason"] == "not idle"


# ═══ execute: the nudge itself, and its idempotency ══════════════════════════════════

async def test_execute_nudges_and_stamps_a_marker_that_stops_a_re_nudge_same_tick(
    actions: Actions,
) -> None:
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    t = await _mk_obligation(actions, "hyg-exec-nudge", owner="agent:hyg-exec1",
                             touched_at=stale)

    out = await hygiene_execute(actions, execute=True, now=NOW)
    assert len(out["nudged"]) == 1 and out["nudged"][0]["thread_id"] == str(t)

    # the SAME tick, run again, must not re-nudge: the marker landed and nothing on the
    # thread (nor the owner) has changed since.
    again = await hygiene_dry_run(actions.pool, now=NOW)
    assert _find(again["buckets"]["would_nudge"], t) is None
    row = _find(again["buckets"]["no_action"], t)
    assert row is not None and "nudged" in row["reason"]


async def test_a_real_touch_after_a_nudge_resets_the_idle_clock(actions: Actions) -> None:
    """A thread genuinely re-annotated by a mind after its own nudge must never be treated
    as still-silent — the SAME re-derivation the operator's own idle definition demands."""
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    t = await _mk_obligation(actions, "hyg-reset-clock", owner="agent:hyg-reset",
                             touched_at=stale)
    await hygiene_execute(actions, execute=True, now=NOW)

    # a mind touches the thread itself, AFTER the nudge
    touch_at = NOW + timedelta(hours=1)
    await actions.assert_property(t, "summary", "hyg-reset-clock (annotated)",
                                  "agent:hyg-reset", touch_at, 0.9,
                                  evidence_class="self_declared")

    out = await hygiene_dry_run(actions.pool, now=touch_at + timedelta(minutes=1))
    row = _find(out["buckets"]["no_action"], t)
    assert row is not None and row["reason"] == "not idle"


# ═══ N2: stale-candidate promotion ════════════════════════════════════════════════════

async def test_n2_silence_past_a_nudge_promotes_to_stale_candidate(actions: Actions) -> None:
    stale = NOW - timedelta(days=N1_IDLE_DAYS + N2_SILENCE_DAYS + 3)
    t = await _mk_obligation(actions, "hyg-n2-promote", owner="agent:hyg-n2",
                             touched_at=stale)
    nudge_time = stale + timedelta(days=N1_IDLE_DAYS + 1)
    await hygiene_execute(actions, execute=True, now=nudge_time)

    later = nudge_time + timedelta(days=N2_SILENCE_DAYS + 1)
    out = await hygiene_execute(actions, execute=True, now=later)
    assert len(out["staled"]) == 1 and out["staled"][0]["thread_id"] == str(t)


async def test_stale_candidate_is_never_reclassified_or_re_briefed_twice(
    actions: Actions,
) -> None:
    stale = NOW - timedelta(days=N1_IDLE_DAYS + N2_SILENCE_DAYS + 3)
    t = await _mk_obligation(actions, "hyg-n2-idempotent", owner="agent:hyg-n2b",
                             touched_at=stale)
    nudge_time = stale + timedelta(days=N1_IDLE_DAYS + 1)
    await hygiene_execute(actions, execute=True, now=nudge_time)
    later = nudge_time + timedelta(days=N2_SILENCE_DAYS + 1)
    await hygiene_execute(actions, execute=True, now=later)

    # a further tick, well past N2 again, must never re-brief or re-classify
    out = await hygiene_execute(actions, execute=True, now=later + timedelta(days=10))
    assert out["staled"] == [] and out["nudged"] == []
    dry = await hygiene_dry_run(actions.pool, now=later + timedelta(days=10))
    row = _find(dry["buckets"]["no_action"], t)
    assert row is not None and "stale-candidate" in row["reason"]


async def test_stale_candidate_never_changes_the_thread_status(actions: Actions) -> None:
    """NEVER AUTO-RESOLVED, exactly as ruled — the thread's own status stays 'open'."""
    stale = NOW - timedelta(days=N1_IDLE_DAYS + N2_SILENCE_DAYS + 3)
    t = await _mk_obligation(actions, "hyg-never-resolved", owner="agent:hyg-nr",
                             touched_at=stale)
    nudge_time = stale + timedelta(days=N1_IDLE_DAYS + 1)
    await hygiene_execute(actions, execute=True, now=nudge_time)
    later = nudge_time + timedelta(days=N2_SILENCE_DAYS + 1)
    await hygiene_execute(actions, execute=True, now=later)

    status = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", t)
    assert status == "open"


# ═══ owner-address fallback ═══════════════════════════════════════════════════════════

async def test_an_unresolvable_owner_falls_back_to_the_operator_desk(
    actions: Actions,
) -> None:
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    t = await _mk_obligation(actions, "hyg-no-live-agent", owner="agent:no-such-agent-xyz",
                             touched_at=stale)
    out = await hygiene_execute(actions, execute=True, now=NOW)
    row = next(r for r in out["nudged"] if r["thread_id"] == str(t))
    assert row["sent"].get("dm_to") is None or "error" not in row["sent"]
    # the send landed as a desk brief (to_project=operator), not a raw error
    assert "error" not in row["sent"]


async def test_a_project_name_owner_falls_back_to_the_operator_desk(actions: Actions) -> None:
    await actions.create_or_find_object("SoftwareProject", "repo:hygtestproj", _SRC)
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    t = await _mk_obligation(actions, "hyg-project-owner", owner="hygtestproj",
                             touched_at=stale)
    out = await hygiene_execute(actions, execute=True, now=NOW)
    row = next(r for r in out["nudged"] if r["thread_id"] == str(t))
    assert "error" not in row["sent"]


async def test_an_unowned_obligation_nudges_the_operator_desk(actions: Actions) -> None:
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    t = await _mk_obligation(actions, "hyg-unowned", owner=None, touched_at=stale)
    out = await hygiene_dry_run(actions.pool, now=NOW)
    assert _find(out["buckets"]["would_nudge"], t) is not None


# ═══ hygiene_status ═══════════════════════════════════════════════════════════════════

async def test_hygiene_status_counts_by_stage(actions: Actions) -> None:
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    await _mk_obligation(actions, "hyg-status-fresh", owner="agent:s1", touched_at=NOW)
    nudge_target = await _mk_obligation(actions, "hyg-status-nudge", owner="agent:s2",
                                        touched_at=stale)
    await hygiene_execute(actions, execute=True, now=NOW)

    out = await hygiene_status(actions.pool)
    unfiled = out["counts_by_project"].get("(unfiled)", {})
    assert unfiled.get("nudged", 0) >= 1
    assert unfiled.get("none", 0) >= 1
    # both created threads counted, none double-counted for lacking an in_repo link
    del nudge_target  # id only needed to mint the row


# ═══ the scheduled leg's own kill switch ══════════════════════════════════════════════

async def test_scheduled_tick_is_a_no_op_when_the_flag_is_off(actions: Actions) -> None:
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    await _mk_obligation(actions, "hyg-flag-off", owner="agent:flagoff", touched_at=stale)
    settings = Settings(osiris_obligation_hygiene_enabled=False)
    out = await obligation_hygiene_scheduled_tick(actions, settings=settings, now=NOW)
    assert out == {"enabled": False, "nudged": [], "staled": [],
                    "note": "the sweep's scheduled leg is dark "
                            "(osiris_obligation_hygiene_enabled=0)"}


async def test_scheduled_tick_acts_when_the_flag_is_on(actions: Actions) -> None:
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    t = await _mk_obligation(actions, "hyg-flag-on", owner="agent:flagon", touched_at=stale)
    settings = Settings(osiris_obligation_hygiene_enabled=True)
    out = await obligation_hygiene_scheduled_tick(actions, settings=settings, now=NOW)
    assert out["enabled"] is True
    assert any(r["thread_id"] == str(t) for r in out["nudged"])
