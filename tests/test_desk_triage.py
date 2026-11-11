"""`desk_triage`: four named rules settle old agent-written operator mail; a row no rule fits
is kept. Real Postgres through the `actions` fixture."""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.backfill import run_backfill
from src.orchestrator.mailbox import triage_agent_desk_backlog


async def _msg(actions: Actions, sender: str, body: str, *, days: int, kind: str | None = None,
               grade: str | None = None, reply_to: int | None = None) -> int:
    mid = await actions.pool.fetchval(
        "INSERT INTO fleet_messages (from_agent, from_project, to_project, body, desk_kind, "
        "grade, reply_to, created_at) VALUES ($1,'osiris','operator',$2,$3,$4,$5, "
        "now() - make_interval(days => $6)) RETURNING id",
        sender, body, kind, grade, reply_to, days)
    return int(mid)


async def _seed(actions: Actions) -> dict[str, int]:
    ids: dict[str, int] = {}
    ids["n1_fold"] = await _msg(actions, "agent:aaaa1111-g1", "GREAT FOLD seat x: folded 0",
                                days=60, kind="fyi")
    ids["n1_model"] = await _msg(actions, "agent:bbbb2222", "Model divergence at mount: Haiku",
                                 days=3)
    ids["n2_status"] = await _msg(actions, "agent:cccc3333-v", "Session closed, all green",
                                  days=40, kind="fyi")
    ids["s1_first"] = await _msg(actions, "agent:dddd4444-x", "Two PRs need your hands",
                                 days=60, kind="hands")
    ids["s1_fix"] = await _msg(actions, "agent:dddd4444-x",
                               f"Correction to msg #{ids['s1_first']}: only one PR",
                               days=60, kind="fyi")
    ids["s2_ask"] = await _msg(actions, "agent:eeee5555", "Decision needed: which cadence?",
                               days=50, kind="decision")
    # kept
    ids["k_recent_ask"] = await _msg(actions, "agent:ffff6666", "Which one?", days=5,
                                     kind="decision", grade="ask")
    ids["k_recent_fyi"] = await _msg(actions, "agent:ffff6666", "Shipped", days=5, kind="fyi")
    ids["k_job"] = await _msg(actions, "cron:retention_heartbeat", "retention: 3 rows", days=1,
                              kind="fyi", grade="fyi")
    ids["k_manifest"] = await _msg(actions, "system:prune-ladder", "PRUNE LADDER MANIFEST: 2",
                                   days=4, kind="decision")
    return ids


async def _unread(actions: Actions) -> set[int]:
    return {r["id"] for r in await actions.pool.fetch(
        "SELECT id FROM fleet_messages WHERE to_project='operator' AND read_at IS NULL")}


async def test_the_dry_run_counts_by_rule_and_sender_and_writes_nothing(
    actions: Actions,
) -> None:
    ids = await _seed(actions)
    out = await triage_agent_desk_backlog(actions.pool, actor="test")
    assert out["dry_run"] is True and out["settled"] == 0
    # the correction itself is an old fyi brief, so N2 takes it, not S1
    assert out["by_rule"] == {"N1": 2, "N2": 2, "S1": 1, "S2": 1}
    assert out["to_settle"] == 6 and out["kept"] == 4
    assert "S3" not in out["rules"]
    assert out["by_sender"]["S1"] == {"dddd4444": 1}
    assert await _unread(actions) == set(ids.values())


async def test_apply_settles_each_row_with_its_rule_and_keeps_the_rest(
    actions: Actions,
) -> None:
    ids = await _seed(actions)
    refused = await triage_agent_desk_backlog(actions.pool, actor="test", dry_run=False)
    assert "error" in refused and await _unread(actions) == set(ids.values())

    out = await triage_agent_desk_backlog(
        actions.pool, actor="test", dry_run=False, because="the operator approved")
    assert out["settled"] == out["to_settle"] == 6
    still = await _unread(actions)
    assert still == {ids[k] for k in ("k_recent_ask", "k_recent_fyi", "k_job",
                                      "k_manifest")}
    note = await actions.pool.fetchval(
        "SELECT moot_note FROM fleet_messages WHERE id=$1", ids["s2_ask"])
    assert "rule S2" in note and "the operator approved" in note
    assert await actions.pool.fetchval(          # rows are kept, marked read and moot
        "SELECT count(*) FROM fleet_messages WHERE id = ANY($1::bigint[]) AND read_at IS NOT NULL "
        "AND moot_at IS NOT NULL", list(ids.values())) == 6
    again = await triage_agent_desk_backlog(actions.pool, actor="test")
    assert again["to_settle"] == 0                                  # idempotent


async def test_a_reply_settles_the_first_message_not_the_reply(actions: Actions) -> None:
    first = await _msg(actions, "agent:1234abcd", "Needs your hands: merge", days=60,
                       kind="hands")
    await _msg(actions, "agent:1234abcd", "all done", days=60, kind="fyi", reply_to=first)
    out = await triage_agent_desk_backlog(actions.pool, actor="test")
    assert out["by_rule"].get("S1") == 1                  # the first; the reply is informational


async def test_a_later_message_that_only_uses_a_keyword_settles_nothing(
    actions: Actions,
) -> None:
    ask = await _msg(actions, "agent:5678abcd", "Needs your hands: plug the dock", days=60,
                     kind="hands")
    await _msg(actions, "agent:5678abcd", "The deploy concern is resolved, a correction follows",
               days=60, kind="decision", grade="ask")
    other = await _msg(actions, "agent:5678abcd", "Number 12 was fixed", days=60,
                       kind="decision", grade="ask")
    out = await triage_agent_desk_backlog(actions.pool, actor="test")
    assert out["by_rule"] == {"S2": 3}          # all by the ask window, none by S1
    assert ask and other


async def test_a_message_that_names_the_number_answers_it(actions: Actions) -> None:
    first = await _msg(actions, "agent:9abc1234", "Which cadence?", days=60, kind="decision")
    await _msg(actions, "agent:9abc1234", f"Withdrawing the ask in mail {first}", days=60,
               kind="decision", grade="ask")
    out = await triage_agent_desk_backlog(actions.pool, actor="test")
    assert out["by_rule"].get("S1") == 1


async def test_noise_words_never_settle_an_ask_decision_or_hands_brief(
    actions: Actions,
) -> None:
    await _msg(actions, "agent:aa00bb11", "Model downgrade policy: needs your call", days=1,
               kind="decision")
    await _msg(actions, "agent:aa00bb11", "Identity check failed: needs your hands", days=1,
               grade="ask")
    out = await triage_agent_desk_backlog(actions.pool, actor="test")
    assert out["to_settle"] == 0 and out["kept"] == 2


async def test_an_unclassified_brief_waits_for_the_ask_window_not_the_fyi_window(
    actions: Actions,
) -> None:
    young = await _msg(actions, "agent:cc00dd11", "A long status", days=40)
    old = await _msg(actions, "agent:cc00dd11", "An older status", days=50)
    out = await triage_agent_desk_backlog(actions.pool, actor="test")
    assert out["by_rule"] == {"S2": 1} and out["kept"] == 1
    assert young and old
    await triage_agent_desk_backlog(actions.pool, actor="test", dry_run=False,
                                    because="test authorization")
    assert await _unread(actions) == {young}


async def test_a_prune_manifest_is_never_dimmed_by_the_triage(actions: Actions) -> None:
    manifest = await _msg(actions, "system:prune-ladder", "PRUNE LADDER MANIFEST: 2",
                          days=9, kind="decision", grade="ask")
    out = await triage_agent_desk_backlog(actions.pool, actor="test")
    assert out["to_settle"] == 0 and out["kept"] == 1
    assert await _unread(actions) == {manifest}


async def test_a_recent_ask_is_never_touched_and_the_verb_dispatches_the_target(
    actions: Actions,
) -> None:
    await _msg(actions, "agent:77778888", "Which cadence?", days=10, kind="decision")
    out = await run_backfill(actions.pool, "desk_triage", actor="test")
    assert out["to_settle"] == 0 and out["kept"] == 1 and out["dry_run"] is True
