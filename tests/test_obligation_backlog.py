"""THE BACKLOG BAND, piece 1 (thread 8608, operator nudge via Thoth): the
`obligation_backlog` composition -- the per-seat axis over the same open-obligation
population digest.py's own `_obligation_pressure` already scores per project, resolving
each row's free-text `owner` against the live roster's own seat ids/handles."""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.capture import open_thread
from src.orchestrator.compositions import _fn_obligation_backlog
from src.orchestrator.seats import ensure_seat


async def test_obligation_backlog_groups_by_resolved_seat(actions: Actions) -> None:
    seat = await ensure_seat(actions, house="obhouse", handle="Obelisk", source="test")
    await open_thread(actions, "duty one for obelisk", kind="obligation",
                      owner="Obelisk", repo="obproj", source="agent:seed-ob1")
    await open_thread(actions, "duty two for obelisk, by seat id", kind="obligation",
                      owner=seat["seat_id"], repo="obproj", source="agent:seed-ob2")

    out = await _fn_obligation_backlog(actions.pool, None, {})

    rows = [r for r in out["by_seat"] if r["seat"] == "Obelisk"]
    assert len(rows) == 1
    assert rows[0]["open"] == 2


async def test_obligation_backlog_owner_match_is_case_insensitive(actions: Actions) -> None:
    await ensure_seat(actions, house="obhouse", handle="Casewell", source="test")
    await open_thread(actions, "lowercased owner", kind="obligation",
                      owner="casewell", repo="obproj2", source="agent:seed-ob3")

    out = await _fn_obligation_backlog(actions.pool, None, {})
    rows = [r for r in out["by_seat"] if r["seat"] == "Casewell"]
    assert len(rows) == 1
    assert rows[0]["open"] == 1


async def test_obligation_backlog_unowned_and_literal_owner_are_distinct_buckets(
    actions: Actions,
) -> None:
    await open_thread(actions, "no owner at all", kind="obligation", owner=None,
                      repo="obproj3", source="session")
    await open_thread(actions, "a made-up owner name", kind="obligation",
                      owner="not-a-real-seat-xyz", repo="obproj3", source="agent:seed-ob5")

    out = await _fn_obligation_backlog(actions.pool, None, {})
    assert out["unowned"] >= 1
    assert out["literal_owner"] >= 1


async def test_obligation_backlog_past_window_and_oldest_ride_along_per_seat(
    actions: Actions,
) -> None:
    await ensure_seat(actions, house="obhouse", handle="Staleford", source="test")
    await open_thread(actions, "a stale duty", kind="obligation", owner="Staleford",
                      repo="obproj4", source="agent:seed-ob6", stale_after_days=-1)

    out = await _fn_obligation_backlog(actions.pool, None, {})
    row = next(r for r in out["by_seat"] if r["seat"] == "Staleford")
    assert row["past_window"] == 1
    assert row["oldest"][0]["summary"] == "a stale duty"


async def test_obligation_backlog_fleet_total_accounts_for_every_bucket(
    actions: Actions,
) -> None:
    await ensure_seat(actions, house="obhouse", handle="Tallyman", source="test")
    await open_thread(actions, "seated duty", kind="obligation", owner="Tallyman",
                      repo="obproj5", source="agent:seed-ob7")
    await open_thread(actions, "unowned duty", kind="obligation", owner=None,
                      repo="obproj5", source="session")
    await open_thread(actions, "literal owner duty", kind="obligation",
                      owner="ghost-owner", repo="obproj5", source="agent:seed-ob8")

    out = await _fn_obligation_backlog(actions.pool, None, {})
    assert out["fleet_total"] == (
        sum(r["open"] for r in out["by_seat"]) + out["unowned"] + out["literal_owner"])


async def test_obligation_backlog_by_project_matches_obligation_pressure_verbatim(
    actions: Actions,
) -> None:
    from src.orchestrator.digest import _obligation_pressure

    await open_thread(actions, "a project-scoped duty", kind="obligation", owner="Whoever",
                      repo="obproj6", source="agent:seed-ob9")

    out = await _fn_obligation_backlog(actions.pool, None, {})
    expected = await _obligation_pressure(Actions(actions.pool))
    assert out["by_project"] == expected
