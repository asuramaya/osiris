"""The operator's desk is not a log: a scheduled job or system process posts one current,
graded notice, not one more unread message per run; the obligation-hygiene job sends one
summary per run; and the dry-run cleanup counts the old automated backlog by sender.
Real Postgres through the `actions` fixture."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.actions.core import Actions
from src.orchestrator.mailbox import send_message, settle_automated_desk_backlog
from src.orchestrator.obligation_hygiene import N1_IDLE_DAYS, hygiene_execute

NOW = datetime.now(UTC)


async def _unread(actions: Actions, sender: str) -> list[dict[str, object]]:
    return [dict(r) for r in await actions.pool.fetch(
        "SELECT id, body, grade, desk_kind, read_at, moot_note FROM fleet_messages "
        "WHERE to_project='operator' AND from_agent=$1 ORDER BY id", sender)]


async def test_a_job_receipt_is_graded_and_replaces_its_own_older_unread_notice(
    actions: Actions,
) -> None:
    first = await send_message(actions.pool, from_agent="cron:retention_heartbeat",
                               from_project="osiris", to_project="operator",
                               body="retention: audit_log 4 rows", desk_kind="fyi")
    second = await send_message(actions.pool, from_agent="cron:retention_heartbeat",
                                from_project="osiris", to_project="operator",
                                body="retention: audit_log 9 rows", desk_kind="fyi")
    third = await send_message(actions.pool, from_agent="cron:retention_heartbeat",
                               from_project="osiris", to_project="operator",
                               body="retention: audit_log 2 rows", desk_kind="fyi")

    rows = await _unread(actions, "cron:retention_heartbeat")
    unread = [r for r in rows if r["read_at"] is None]
    assert len(rows) == 3 and [r["id"] for r in unread] == [third["id"]]
    assert unread[0]["grade"] == "fyi"
    assert "(replaces 1 earlier unread notice(s) from this job)" in str(unread[0]["body"])
    assert second["replaced"] == [first["id"]] and third["replaced"] == [second["id"]]
    old = next(r for r in rows if r["id"] == first["id"])
    assert "replaced by a newer notice" in str(old["moot_note"])     # kept, with the reason


async def test_a_different_notice_from_the_same_job_stands_beside_it(actions: Actions) -> None:
    await send_message(actions.pool, from_agent="system:preflight", from_project="osiris",
                       to_project="operator", body="MCP LIVENESS: fine", desk_kind="fyi")
    await send_message(actions.pool, from_agent="system:preflight", from_project="osiris",
                       to_project="operator", body="BACKUP AGE: 3 days", desk_kind="fyi")
    unread = [r for r in await _unread(actions, "system:preflight") if r["read_at"] is None]
    assert len(unread) == 2


async def test_another_job_is_never_touched(actions: Actions) -> None:
    keep = await send_message(actions.pool, from_agent="cron:no_regrow_heartbeat",
                              from_project="osiris", to_project="operator",
                              body="retention: not mine", desk_kind="fyi")
    await send_message(actions.pool, from_agent="cron:retention_heartbeat",
                       from_project="osiris", to_project="operator",
                       body="retention: mine", desk_kind="fyi")
    kept = [r for r in await _unread(actions, "cron:no_regrow_heartbeat")]
    assert kept[0]["id"] == keep["id"] and kept[0]["read_at"] is None


async def test_a_decision_or_hands_notice_from_a_job_is_an_ask(actions: Actions) -> None:
    await send_message(actions.pool, from_agent="system:prune-ladder", from_project="osiris",
                       to_project="operator", body="PRUNE LADDER MANIFEST: 3 files",
                       desk_kind="decision")
    rows = await _unread(actions, "system:prune-ladder")
    assert rows[0]["grade"] == "ask"


async def test_an_agent_or_a_person_is_never_rolled(actions: Actions) -> None:
    for n in range(2):
        await send_message(actions.pool, from_agent="agent:sekhmet-test", from_project="osiris",
                           to_project="operator", body=f"report: item {n}", desk_kind="fyi")
    rows = await _unread(actions, "agent:sekhmet-test")
    assert len(rows) == 2 and all(r["read_at"] is None for r in rows)


async def test_the_hygiene_job_sends_one_summary_per_run_not_one_per_thread(
    actions: Actions,
) -> None:
    stale = NOW - timedelta(days=N1_IDLE_DAYS + 1)
    for i in range(3):
        t = await actions.create_or_find_object("Thread", f"hyg-desk-{i}", "test-source")
        for name, value in (("summary", f"idle thread {i}"), ("status", "open"),
                            ("kind", "obligation"), ("owner", f"nobody-{i}")):
            await actions.assert_property(t, name, value, "test-source", stale, 0.9,
                                          evidence_class="self_declared")
    out = await hygiene_execute(actions, execute=True, now=NOW)
    assert len(out["nudged"]) == 3
    rows = await _unread(actions, "cron:obligation_hygiene_heartbeat")
    assert len(rows) == 1 and rows[0]["grade"] == "fyi"
    assert str(rows[0]["body"]).startswith("OBLIGATION HYGIENE NUDGE (owner not reachable): 3")
    assert all(f"idle thread {i}" in str(rows[0]["body"]) for i in range(3))


async def _old_message(actions: Actions, sender: str, **kw: object) -> None:
    mid = await actions.pool.fetchval(
        "INSERT INTO fleet_messages (from_agent, from_project, to_project, body, desk_kind, "
        "grade, created_at) VALUES ($1,'osiris','operator','old notice',$2,$3, "
        "now() - interval '3 days') RETURNING id", sender, kw.get("desk_kind"), kw.get("grade"))
    assert mid


async def test_the_cleanup_dry_run_counts_by_sender_and_keeps_asks_and_agents(
    actions: Actions,
) -> None:
    for _ in range(3):
        await _old_message(actions, "cron:retention_heartbeat", desk_kind="fyi")
    await _old_message(actions, "system:osiris-worker")
    await _old_message(actions, "system:prune-ladder", desk_kind="decision")
    await _old_message(actions, "cron:thing", grade="ask")
    await _old_message(actions, "agent:someone-xi", desk_kind="fyi")
    await send_message(actions.pool, from_agent="cron:fresh_heartbeat", from_project="osiris",
                       to_project="operator", body="fresh: today", desk_kind="fyi")

    dry = await settle_automated_desk_backlog(actions.pool, actor="test")
    assert dry["dry_run"] is True and dry["settled"] == 0
    assert dry["by_sender"] == {"cron:retention_heartbeat": 3, "system:osiris-worker": 1}
    assert dry["to_settle"] == 4
    assert dry["kept"]["written_by_an_agent_or_person"] == 1
    assert dry["kept"]["asks_and_decisions"] == 2
    assert dry["kept"]["newer_than_the_cutoff"] == 1
    assert await actions.pool.fetchval(           # nothing was written
        "SELECT count(*) FROM fleet_messages WHERE to_project='operator' AND read_at IS NULL"
    ) == 8

    refused = await settle_automated_desk_backlog(actions.pool, actor="test", dry_run=False)
    assert "error" in refused
    done = await settle_automated_desk_backlog(
        actions.pool, actor="test", dry_run=False, because="operator approved")
    assert done["settled"] == 4
    again = await settle_automated_desk_backlog(actions.pool, actor="test")
    assert again["to_settle"] == 0                                    # idempotent
    assert await actions.pool.fetchval(           # asks and agent mail are still unread
        "SELECT count(*) FROM fleet_messages WHERE to_project='operator' AND read_at IS NULL"
    ) == 4
