"""THE SURFACE — one segment authority above vitals (thread 109b6c48, ruling e9ef7373).

These tests pin the RULES (thresholds, scope, dark-until-matters) that the old per-surface
copies drifted on, so a future drift fails here instead of quietly disagreeing across
fleet_pulse / the statusline / the membrane header again."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from src.actions.core import Actions
from src.orchestrator import surface
from src.orchestrator.mailbox import send_message
from src.orchestrator.mounts import save_mount


async def test_empty_desk_is_all_zero_and_quiet(actions: Actions) -> None:
    """A fresh graph: every ambient-always segment shows a real zero; every dark-until-it-
    matters segment stays hidden (spend unmetered by default in tests)."""
    seg = await surface.fetch(actions.pool)
    assert seg.live.show and seg.live.data == {"souls": 0, "visitors": 0}
    assert seg.owed.show and seg.owed.data == {"owed": 0} and seg.owed.severity == "ok"
    assert seg.owed_here.show and seg.owed_here.data == {"owed_here": 0}
    assert seg.briefs_total.show and seg.briefs_total.data == {"briefs": 0}
    assert seg.briefs_mine.show is False  # dark at zero
    assert seg.wakes.show and seg.wakes.data == {"wakes": 0}
    assert seg.mail.show and seg.mail.data == {"mail": 0, "flight": 0, "dm": 0}
    assert seg.mail.severity == "ok"
    assert seg.sensing.show is False
    assert seg.spend.show is False and seg.spend.data == {"metered": False}


async def test_owed_is_fleet_wide_but_owed_here_is_scoped_to_the_project(
    actions: Actions,
) -> None:
    """Two rulings, still in force: 'owe' on the pulse is the whole desk; on the statusline
    it is THIS project's slice only (2026-07-16: a number you can't act on is wallpaper)."""
    oid = await actions.create_or_find_object("Thread", "thread:test-owed", "test")
    await actions.assert_property(oid, "owner", "operator", "test", datetime.now(UTC), 0.9,
                                  evidence_class="direct_observation")
    await actions.assert_property(oid, "status", "open", "test", datetime.now(UTC), 0.9,
                                  evidence_class="direct_observation")
    await actions.assert_property(oid, "summary", "an open debt", "test", datetime.now(UTC), 0.9,
                                  evidence_class="direct_observation")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:elsewhere", "test")
    await actions.create_link(oid, proj, "in_repo", "test", datetime.now(UTC), 0.9)

    seg_here = await surface.fetch(actions.pool, project="osiris")
    seg_elsewhere = await surface.fetch(actions.pool, project="elsewhere")
    assert seg_here.owed.data == {"owed": 1}          # fleet-wide: sees it regardless
    assert seg_here.owed_here.data == {"owed_here": 0}  # scoped: not this project's debt
    assert seg_elsewhere.owed_here.data == {"owed_here": 1}
    assert seg_here.owed.severity == "red"
    assert seg_here.owed_here.severity == "ok"


async def test_briefs_total_is_the_desk_briefs_mine_is_this_agents_own(
    actions: Actions,
) -> None:
    await send_message(actions.pool, from_agent="agent:aaa11111", from_project="osiris",
                       to_project="operator", body="a brief from aaa")
    seg_mine = await surface.fetch(actions.pool, agent="agent:aaa11111")
    seg_other = await surface.fetch(actions.pool, agent="agent:bbb22222")
    assert seg_mine.briefs_total.data == {"briefs": 1}
    assert seg_other.briefs_total.data == {"briefs": 1}   # the whole desk, same for both
    assert seg_mine.briefs_mine.data == {"briefs": 1} and seg_mine.briefs_mine.show
    assert seg_other.briefs_mine.data == {"briefs": 0} and seg_other.briefs_mine.show is False


async def test_dm_lights_the_mail_segment_by_itself(actions: Actions) -> None:
    """The Alfred chain, 2026-07-19: seven DMs waiting, mail 0, flight 0 — a render
    condition that forgot `dm` alone rendered a dim 'mail 0' over live traffic."""
    await save_mount(actions.pool, job_dir="/j/a", agent_id="agent:aaa11111",
                     project="osiris", cwd="/w", model=None, session_key="sid:a")
    await send_message(actions.pool, from_agent="agent:sender1", from_project="osiris",
                       to_agent="agent:aaa11111", to_project="osiris", body="dm for you")
    seg = await surface.fetch(actions.pool, project="osiris", agent="agent:aaa11111")
    assert seg.mail.data["dm"] == 1 and seg.mail.data["mail"] == 0
    assert seg.mail.show is True
    assert seg.mail.severity == "alarm"


async def test_sensing_is_dark_until_a_job_actually_goes_sick(actions: Actions) -> None:
    p = actions.pool
    now = datetime.now(UTC)
    # a healthy job: recent last_ok well inside its own cadence
    await p.execute(
        "INSERT INTO watermarks (key, cursor) VALUES ('job:healthy', $1)",
        f'{{"last_ok": "{now.isoformat()}", "every": 600}}')
    healthy = await surface.fetch(p)
    assert healthy.sensing.show is False and healthy.sensing.data == {"sick": []}

    # a job that has NEVER confessed an ok
    await p.execute(
        "INSERT INTO watermarks (key, cursor) VALUES ('job:neverok', $1)", '{"every": 600}')
    never_ok = await surface.fetch(p)
    assert never_ok.sensing.show and "neverok" in never_ok.sensing.data["sick"]
    assert never_ok.sensing.severity == "alarm"

    # a job three cadences stale
    stale = now - timedelta(seconds=3 * 600 + 1)
    await p.execute(
        "UPDATE watermarks SET cursor=$1 WHERE key='job:neverok'",
        f'{{"last_ok": "{stale.isoformat()}", "every": 600}}')
    stale_seg = await surface.fetch(p)
    assert "neverok" in stale_seg.sensing.data["sick"]


async def test_a_fast_cadence_job_survives_a_deploy_restart_under_the_floor(
    actions: Actions,
) -> None:
    """drain_cascade/evaluate_watch run every=5s — 3x that is 15s, which a routine deploy
    restart's cancel-and-resume cost (measured up to ~43s, 2026-09-01) blows past on its own.
    `_SICK_FLOOR_SECS` exists so THIS job survives that cost without reading sick, while a job
    that is actually dead for longer than the floor still does."""
    p = actions.pool
    now = datetime.now(UTC)

    inside_floor = now - timedelta(seconds=surface._SICK_FLOOR_SECS - 5)
    await p.execute(
        "INSERT INTO watermarks (key, cursor) VALUES ('job:drain_cascade', $1)",
        f'{{"last_ok": "{inside_floor.isoformat()}", "every": 5}}')
    seg = await surface.fetch(p)
    assert "drain_cascade" not in seg.sensing.data["sick"]

    past_floor = now - timedelta(seconds=surface._SICK_FLOOR_SECS + 5)
    await p.execute(
        "UPDATE watermarks SET cursor=$1 WHERE key='job:drain_cascade'",
        f'{{"last_ok": "{past_floor.isoformat()}", "every": 5}}')
    seg = await surface.fetch(p)
    assert "drain_cascade" in seg.sensing.data["sick"]


async def test_spend_is_dark_below_the_60_percent_gate(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.ingest.providers.spend_is_metered", lambda s=None: True)
    await actions.pool.execute(
        "INSERT INTO llm_usage (purpose, model, cost_usd, ran_at) "
        "VALUES ('test', 'x', 1.20, now())")
    seg = await surface.fetch(actions.pool)
    assert seg.spend.show is False
    assert seg.spend.data["spent"] == 1.20 and seg.spend.severity == "ok"


async def test_spend_lights_amber_at_60_and_red_at_85_percent(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.ingest.providers.spend_is_metered", lambda s=None: True)
    p = actions.pool
    await p.execute("INSERT INTO llm_usage (purpose, model, cost_usd, ran_at) "
                    "VALUES ('test', 'x', 6.50, now())")
    amber = await surface.fetch(p)
    assert amber.spend.show and amber.spend.severity == "amber"

    await p.execute("DELETE FROM llm_usage")
    await p.execute("INSERT INTO llm_usage (purpose, model, cost_usd, ran_at) "
                    "VALUES ('test', 'x', 8.50, now())")
    red = await surface.fetch(p)
    assert red.spend.show and red.spend.severity == "red"


async def test_an_unpriced_call_is_loud_regardless_of_the_percent_gate(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A PRODUCER THAT CANNOT PRICE ITSELF MAY NOT SPEND — blind is never scored as zero,
    and it outranks the ordinary amber/red thresholds even at a trivial spend level."""
    monkeypatch.setattr("src.ingest.providers.spend_is_metered", lambda s=None: True)
    await actions.pool.execute(
        "INSERT INTO llm_usage (purpose, model, cost_usd, ran_at) "
        "VALUES ('test', 'x', NULL, now())")
    seg = await surface.fetch(actions.pool)
    assert seg.spend.show and seg.spend.severity == "alarm"
    assert seg.spend.data["blind"] == 1


async def test_spend_on_a_subscription_is_never_computed_at_all(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Thoth LIII, 2026-07-21: the CLI's cost is notional on a subscription — the segment
    must not exist as a phantom '$X/$10', not even a hidden one carrying real numbers."""
    monkeypatch.setattr("src.ingest.providers.spend_is_metered", lambda s=None: False)
    await actions.pool.execute(
        "INSERT INTO llm_usage (purpose, model, cost_usd, ran_at) "
        "VALUES ('test', 'x', 9.99, now())")
    seg = await surface.fetch(actions.pool)
    assert seg.spend.show is False
    assert seg.spend.data == {"metered": False}
