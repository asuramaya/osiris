"""The membrane — fleet_digest, the operator's window into the autonomous fleet.

Constructs a small fleet with all four upward streams — a clean and an unresolved identity, a
model-swapped agent, agent-authored activity inside and outside the window (and miner backfill
that must be excluded), and a relay/origin co-assertion — and asserts the digest surfaces each.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from src.actions.core import Actions
from src.orchestrator.digest import fleet_digest
from src.orchestrator.mailbox import OPERATOR_ADDR, send_message, unread_count
from src.parsers.base import EvidenceClass

NOW = datetime(2026, 7, 7, tzinfo=UTC)
DO = EvidenceClass.DIRECT_OBSERVATION.value
SD = EvidenceClass.SELF_DECLARED.value


async def _prop(actions, oid, name, value, source, ec, conf=0.9, when=NOW):
    await actions.assert_property(oid, name, value, source, when, conf, evidence_class=ec)


async def test_fleet_digest_surfaces_the_four_streams(actions: Actions) -> None:
    since = NOW - timedelta(hours=24)
    # --- roster + health: a clean, swapped agent and an unresolved one ---
    a = await actions.create_or_find_object("Agent", "agent:aaa", "fleet-observer")
    await _prop(actions, a, "project", "osiris", "fleet-observer", DO)
    await _prop(actions, a, "identity_resolved", True, "fleet-observer", DO)
    await _prop(actions, a, "model_swapped", "claude-fable-5 → claude-opus-4-8",
                "fleet-observer", DO)
    # a swap is witnessed by READING a transcript, so the miner stamps the sighting in the same
    # breath (see sessions._stamp_alive) — an agent cannot be swapped and never-seen at once.
    await _prop(actions, a, "last_active", (NOW - timedelta(minutes=5)).isoformat(),
                "fleet-observer", DO)
    u = await actions.create_or_find_object("Agent", "agent:unknown-sibling-one", "fleet-observer")
    await _prop(actions, u, "identity_resolved", False, "fleet-observer", DO)
    # --- activity: a fresh agent-authored decision, an OLD one, and miner backfill (excluded) ---
    d = await actions.create_or_find_object("Decision", "decision:recent", "agent:aaa")
    await _prop(actions, d, "summary", "shipped credence", "agent:aaa", SD)
    old = await actions.create_or_find_object("Decision", "decision:old", "agent:aaa")
    await _prop(actions, old, "summary", "ancient", "agent:aaa", SD, when=NOW - timedelta(days=3))
    m = await actions.create_or_find_object("Decision", "decision:mined", "session-miner")
    await _prop(actions, m, "summary", "mined noise", "session-miner", "derived", conf=0.4)
    # --- laundering: agent:bbb (child) observed; agent:aaa (parent) RELAYED the same claim
    #     (reworded) at an inflated grade → clamp + flag ---
    b = await actions.create_or_find_object("Agent", "agent:bbb", "fleet-observer")
    await actions.create_link(b, a, "spawned_by", "fleet-observer", NOW, 0.6, evidence_class=DO)
    await _prop(actions, a, "backed_by_observation", False, "fleet-observer", DO, conf=0.6)
    o = await actions.create_or_find_object("SoftwareProject", "repo:x", "test")
    await _prop(actions, o, "status", "The deploy FAILED.", "agent:aaa", SD)
    await _prop(actions, o, "status", "deploy failed", "agent:bbb", DO, conf=0.6)
    # --- disputes: on a DIFFERENT fact the parent MATERIALLY DISAGREES with the child → Tier-2
    #     reads it as a dispute, not a relay: surfaced (both poles), never clamped or flagged ---
    o2 = await actions.create_or_find_object("SoftwareProject", "repo:x2", "test")
    await _prop(actions, o2, "verdict", "rollback is required", "agent:aaa", SD)
    await _prop(actions, o2, "verdict", "no rollback needed", "agent:bbb", DO, conf=0.6)

    dg = await fleet_digest(actions, since=since)

    # roster: 3 agents, 1 unresolved, 1 swapped
    assert dg["summary"]["agents"] == 3
    assert dg["summary"]["unresolved"] == 1
    assert dg["summary"]["swapped"] == 1
    assert any(not r["resolved"] and r["agent"] == "agent:unknown-sibling-one"
               for r in dg["roster"])
    # danger map names the swapped agent + its transition
    assert dg["danger"][0]["agent"] == "agent:aaa"
    assert "claude-fable-5 → claude-opus-4-8" in dg["danger"][0]["swapped"]
    # activity: the recent agent decision is in; the old one and the mined one are NOT
    summaries = [x["summary"] for x in dg["activity"]]
    assert "shipped credence" in summaries
    assert "ancient" not in summaries and "mined noise" not in summaries
    # laundering: the parent relayed the child's observation inflated → flagged, origin wins
    assert dg["summary"]["laundering"] == 1
    assert "agent:aaa" in dg["laundering"][0]["laundering_sources"]
    assert dg["laundering"][0]["value"] == "deploy failed"  # origin won, relay clamped
    # disputes: the genuine disagreement is surfaced with both poles, never buried as laundering
    assert dg["summary"]["disputes"] == 1
    disp = dg["disputes"][0]
    assert disp["name"] == "verdict"
    assert {p["source"] for p in disp["positions"]} == {"agent:aaa", "agent:bbb"}
    assert {p["value"] for p in disp["positions"]} == {"rollback is required", "no rollback needed"}


async def test_digest_surfaces_conversations_and_the_operator_desk(actions: Actions) -> None:
    """The upward lane's compliance-free half: lateral threads and the operator's inbox are
    read straight off fleet_messages — an agent that shirks its report-up duty is still seen."""
    p = actions.pool
    since = NOW - timedelta(hours=24)
    # a lateral request→reply thread between two projects
    ask = await send_message(p, from_agent="agent:aaa", from_project="sibling-two",
                             to_project="sibling-one", body="run the ablation?")
    await send_message(p, from_agent="agent:bbb", from_project="sibling-one",
                       body="done — organ delta reproduces", reply_to=ask["id"])
    # a report-up brief on the operator's desk
    await send_message(p, from_agent="agent:bbb", from_project="sibling-one",
                       to_project=OPERATOR_ADDR, body="FINDING: organ delta is real; "
                       "two witnesses agree. Details in decision log.")

    dg = await fleet_digest(actions, since=since)

    assert dg["summary"]["conversations"] >= 1
    lateral = next(c for c in dg["conversations"] if c["thread"] == ask["id"])
    assert set(lateral["between"]) >= {"sibling-two", "sibling-one"}
    assert lateral["msgs"] == 2
    assert lateral["last"]["body"].startswith("done — organ delta")
    # the desk: counted and previewed, newest first, NOT leased by the digest (a peek)
    assert dg["summary"]["operator_unread"] == 1
    assert dg["operator_inbox"]["latest"][0]["from_project"] == "sibling-one"
    dg2 = await fleet_digest(actions, since=since)
    assert dg2["summary"]["operator_unread"] == 1  # reading the digest didn't consume it


async def test_activity_excludes_agent_sourced_mined_rows(actions: Actions) -> None:
    """Origin-attribution regression: the session-miner now SOURCES its DERIVED backfill to the
    ORIGINATING agent (agent:%), so `source_id LIKE 'agent:%'` alone would LEAK a mined echo into
    the deliberate-activity stream. _activity must exclude it by GRADE — only SELF_DECLARED agent
    work is 'what the fleet deliberately did in your name'."""
    since = NOW - timedelta(hours=24)
    deliberate = await actions.create_or_find_object("Decision", "decision:deliberate", "agent:h")
    await _prop(actions, deliberate, "summary", "deliberately decided X", "agent:h", SD)
    # the miner's echo of this SAME session's words: agent-SOURCED now (post origin attribution),
    # but DERIVED — the object is the miner's (actor session-miner), the words are the agent's.
    mined = await actions.create_or_find_object("Thread", "thread:mined-echo", "session-miner")
    await _prop(actions, mined, "summary", "a mined echo of the discussion", "agent:h",
                "derived", conf=0.4)

    dg = await fleet_digest(actions, since=since)

    summaries = [x["summary"] for x in dg["activity"]]
    assert "deliberately decided X" in summaries               # deliberate agent work surfaces
    assert "a mined echo of the discussion" not in summaries   # the agent-sourced mining does NOT
    assert dg["summary"]["activity"] == 1


async def test_activity_dedups_a_co_asserted_summary_to_one_row(actions: Actions) -> None:
    """A Decision/Thread co-asserted by several agents carries one summary row PER source (the
    multi-source set), so _activity would list the SAME activity twice. Dedup by object, keeping
    the highest-grade (then most-recent) row — one line per decision, not one per asserter."""
    since = NOW - timedelta(hours=24)
    # two agents deliberately assert the SAME summary → same canonical → ONE object, two sources
    d = await actions.create_or_find_object("Decision", "decision:coassert", "agent:a")
    await _prop(actions, d, "summary", "we shipped the credence clamp", "agent:a", SD,
                when=NOW - timedelta(hours=2))
    await _prop(actions, d, "summary", "we shipped the credence clamp", "agent:b", SD, when=NOW)
    # the object really does carry two current summary assertions (the multi-source set)
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM current_assertions WHERE object_id=$1 AND name='summary'", d) == 2

    dg = await fleet_digest(actions, since=since)

    matches = [x for x in dg["activity"] if x["summary"] == "we shipped the credence clamp"]
    assert len(matches) == 1                    # deduped to a single activity line
    assert dg["summary"]["activity"] == 1
    assert matches[0]["at"] == NOW.isoformat()  # kept the most-recent asserter's row
    assert matches[0]["agent"] == "agent:b"


async def test_watermark_mode_advances_only_on_mark_seen(actions: Actions) -> None:
    """The desk norm applied to the digest: glancing is a peek — only the DELIBERATE act
    (mark_seen) advances the stored operator watermark; an explicit `since` never touches it."""
    # no watermark yet → 24h fallback, reported honestly
    dg = await fleet_digest(actions)
    assert dg["watermark"]["mode"] == "watermark" and dg["watermark"]["value"] is None
    assert dg["watermark"]["marked"] is False
    # a plain read did NOT set one (peek changed nothing)
    dg2 = await fleet_digest(actions)
    assert dg2["watermark"]["value"] is None
    # the deliberate act advances it…
    dg3 = await fleet_digest(actions, mark_seen=True)
    assert dg3["watermark"]["marked"] is True and dg3["watermark"]["advanced_to"]
    # …and the next glance opens exactly there
    dg4 = await fleet_digest(actions)
    assert dg4["watermark"]["mode"] == "watermark"
    assert dg4["watermark"]["value"] == dg3["watermark"]["advanced_to"]
    assert dg4["since"] == dg4["watermark"]["value"]
    # an explicit window is explicit — and leaves the watermark alone
    dg5 = await fleet_digest(actions, since=NOW - timedelta(hours=1))
    assert dg5["watermark"]["mode"] == "explicit"
    dg6 = await fleet_digest(actions)
    assert dg6["watermark"]["value"] == dg3["watermark"]["advanced_to"]


async def test_costs_stream_meters_the_window_honestly(
    actions: Actions, monkeypatch: pytest.MonkeyPatch) -> None:
    """The operator's 'where are the tokens burnt', metered — grouped by burner, biggest
    first, and NEVER without its coverage note (a spend figure that overclaims is worse
    than none: wakes and interactive tabs are unmetered today). This is the BILLED case
    (spend_is_metered True): only there is the dollar figure a real debit; the subscription
    case — where it is notional and omitted — is the test below."""
    monkeypatch.setattr("src.ingest.providers.spend_is_metered", lambda s=None: True)
    p = actions.pool
    await p.execute(
        "INSERT INTO llm_usage (purpose, model, input_tokens, output_tokens, cost_usd) VALUES "
        "('session-extract','claude-haiku-4-5-20251001', 30000, 2000, 0.05),"
        "('session-extract','claude-haiku-4-5-20251001', 20000, 1000, 0.03),"
        "('doc-extract','claude-haiku-4-5-20251001', 5000, 500, 0.01)")
    dg = await fleet_digest(actions, since=NOW - timedelta(hours=24))
    assert dg["summary"]["spend_tokens"] == 58_500
    assert dg["summary"]["spend_usd"] == 0.09
    top = dg["costs"]["by"][0]
    assert top["purpose"] == "session-extract" and top["calls"] == 2
    assert top["tokens"] == 53_000
    assert "unmetered" in dg["costs"]["coverage"]  # the honesty clause is part of the stream


async def test_costs_stream_OMITS_notional_dollars_on_a_subscription(
    actions: Actions, monkeypatch: pytest.MonkeyPatch) -> None:
    """On a subscription the CLI's cost_usd is notional, so the console shows the REAL token
    counts and drops the phantom $ — the same reason the daily ceiling no longer gates on it
    (Thoth LIII 2026-07-21). Tokens stay; every usd goes None so the membrane's guards omit it."""
    monkeypatch.setattr("src.ingest.providers.spend_is_metered", lambda s=None: False)
    p = actions.pool
    await p.execute(
        "INSERT INTO llm_usage (purpose, model, input_tokens, output_tokens, cost_usd) VALUES "
        "('session-extract','claude-haiku-4-5-20251001', 30000, 2000, 0.05)")
    dg = await fleet_digest(actions, since=NOW - timedelta(hours=24))
    assert dg["summary"]["spend_tokens"] == 32_000   # tokens are real, kept
    assert dg["summary"]["spend_usd"] is None         # the notional $ is dropped, not zeroed
    assert dg["costs"]["by"][0]["usd"] is None        # ...per-row too, so the table cell is blank
    assert "DOLLARS OMITTED" in dg["costs"]["coverage"]


async def test_desk_folds_superseded_briefs_under_the_newest_head(actions: Actions) -> None:
    """Task #50: an agent updating its prior brief (reply_to its OWN message — the threading
    duty) stacks the thread; the desk shows ONE head per thread with the older briefs counted
    under it. The system folds; only the human settles — the superseded rows stay unread."""
    p = actions.pool
    first = await send_message(p, from_agent="agent:h", from_project="sibling-one",
                               to_project=OPERATOR_ADDR, body="BRIEF: run 1 started")
    # the self-reply routes ONWARD to the desk (not back to the sender) and joins the thread
    second = await send_message(p, from_agent="agent:h", from_project="sibling-one",
                                body="BRIEF v2: run 1 done — organ delta reproduces",
                                reply_to=first["id"])
    assert second["to"] == OPERATOR_ADDR and second["thread_id"] == first["id"]
    # an unrelated brief from another project is its own head
    await send_message(p, from_agent="agent:c", from_project="sibling-five",
                       to_project=OPERATOR_ADDR, body="BRIEF: memory audit queued")

    dg = await fleet_digest(actions, since=NOW - timedelta(hours=24))
    desk = dg["operator_inbox"]

    assert desk["unread"] == 2       # active HEADS — the number that nags
    assert desk["unread_raw"] == 3   # the honest backlog underneath
    heads = {m["body"]: m for m in desk["latest"]}
    assert "BRIEF v2: run 1 done — organ delta reproduces" in heads   # newest head shown
    assert "BRIEF: run 1 started" not in heads                        # superseded — folded
    assert heads["BRIEF v2: run 1 done — organ delta reproduces"]["supersedes"] == 1
    assert heads["BRIEF: memory audit queued"]["supersedes"] == 0
    # NOTHING was settled by the fold: the superseded brief is still the operator's to clear
    assert await unread_count(p, OPERATOR_ADDR, reader_agent=OPERATOR_ADDR, lease_secs=0) == 3


async def test_replying_to_your_own_lateral_message_routes_onward(actions: Actions) -> None:
    """The route fix behind the fold: reply_to your OWN message goes to its RECIPIENT (thread
    continuation), never back to yourself."""
    p = actions.pool
    ask = await send_message(p, from_agent="agent:a", from_project="sibling-two",
                             to_project="sibling-one", body="first volley")
    follow = await send_message(p, from_agent="agent:a", from_project="sibling-two",
                                body="second volley, same thread", reply_to=ask["id"])
    assert follow["to"] == "sibling-one" and follow["thread_id"] == ask["id"]
    assert await unread_count(p, "sibling-one", reader_agent="agent:h", lease_secs=0) == 2


async def test_live_swap_surfaces_before_a_re_mount(actions: Actions) -> None:
    """The sibling-eight audit's #2: a mid-session classifier swap lands in agent_mounts via
    the heartbeat BEFORE the Agent object is re-stamped. The danger map must show it live —
    a mount-once agent can't be left running opus behind a stale-green roster."""
    a = await actions.create_or_find_object("Agent", "agent:ra", "fleet-observer")
    await _prop(actions, a, "project", "sibling-eight", "fleet-observer", DO)
    await _prop(actions, a, "source_model", "claude-fable-5", "fleet-observer", DO)
    # the heartbeat wrote the LIVE model (opus) to the mount row; the Agent object still says fable
    await actions.pool.execute(
        "INSERT INTO agent_mounts (job_dir, agent_id, project, cwd, model, last_seen) "
        "VALUES ('/j/ra','agent:ra','sibling-eight','/w/ra','claude-opus-4-8', now())")

    dg = await fleet_digest(actions, since=NOW - timedelta(hours=24))
    r = next(x for x in dg["roster"] if x["agent"] == "agent:ra")
    assert r["live_model"] == "claude-opus-4-8"
    assert "claude-fable-5 → claude-opus-4-8 (unstamped)" == r["live_swap"]
    # and it counts in the danger map + swapped tally, without any re-mount
    assert any(d["agent"] == "agent:ra" for d in dg["danger"])
    assert dg["summary"]["swapped"] >= 1


async def test_matching_live_model_is_not_a_swap(actions: Actions) -> None:
    """The heartbeat model AGREEING with the stamp is the healthy case — no false alarm."""
    a = await actions.create_or_find_object("Agent", "agent:ok", "fleet-observer")
    await _prop(actions, a, "source_model", "claude-fable-5", "fleet-observer", DO)
    await actions.pool.execute(
        "INSERT INTO agent_mounts (job_dir, agent_id, project, cwd, model, last_seen) "
        "VALUES ('/j/ok','agent:ok','osiris','/w/ok','claude-fable-5', now())")
    dg = await fleet_digest(actions, since=NOW - timedelta(hours=24))
    r = next(x for x in dg["roster"] if x["agent"] == "agent:ok")
    assert r["live_swap"] is None and r["live_model"] is None
    assert not any(d["agent"] == "agent:ok" for d in dg["danger"])


# --- miner tick telemetry: the onboarding-day instrument (decision 3191e0df) ------------

async def test_miner_telemetry_records_and_digest_surfaces_it(actions: Actions) -> None:
    """A tick's whole life lands in miner:ticks — start, outcome, saturation, error — and
    the digest reads it back as vital signs. The heartbeat says the worker breathes; THIS
    says the sensing tick actually finishes (the outage ran a day behind a green beat)."""
    from src.orchestrator.monitor import miner_health, miner_tick_ended, miner_tick_started

    pool = actions.pool
    # two clean saturated ticks, one error tick, one start that never confessed (timeout
    # cancel that outran the shield)
    for _ in range(2):
        await miner_tick_started(pool)
        await miner_tick_ended(pool, secs=190.0, budget=3,
                               report={"chunks": 3, "decisions": 2, "threads": 1})
    await miner_tick_started(pool)
    await miner_tick_ended(pool, secs=301.2, budget=3, error="timeout")
    await miner_tick_started(pool)  # dies unconfessed

    blob = await miner_health(pool)
    assert blob["starts"] == 4 and blob["completions"] == 3
    assert [t.get("error") for t in blob["ticks"]] == [None, None, "timeout"]
    assert blob["ticks"][0]["yield"] == 3  # decisions+threads folded into one number

    dg = await fleet_digest(actions, since=NOW - timedelta(hours=24))
    m = dg["miner"]
    assert m["ticks"] == 3 and m["errors"] == 1 and m["saturated"] == 2
    assert m["unaccounted"] == 1
    assert m["max_secs"] == 301.2
    assert m["last_ok"] is not None
    assert dg["summary"]["miner_errors"] == 1


async def test_miner_telemetry_is_bounded_and_absent_is_quiet(actions: Actions) -> None:
    """The blob keeps a bounded tail (~8h of ticks), and a fleet with no telemetry yet
    digests to zeros instead of an error — a young instrument is not a failure."""
    from src.orchestrator.monitor import _MINER_KEEP, miner_health, miner_tick_ended

    dg = await fleet_digest(actions, since=NOW - timedelta(hours=24))
    assert dg["miner"]["ticks"] == 0 and dg["miner"]["unaccounted"] == 0

    for i in range(_MINER_KEEP + 5):
        await miner_tick_ended(actions.pool, secs=float(i), budget=3,
                               report={"chunks": 1})
    blob = await miner_health(actions.pool)
    assert len(blob["ticks"]) == _MINER_KEEP
    assert blob["ticks"][-1]["secs"] == float(_MINER_KEEP + 4)  # newest kept, oldest dropped


async def test_the_window_bounds_the_roster_without_ever_deleting_a_soul(
    actions: Actions,
) -> None:
    """The window applies to the ROSTER, and what it excludes it still COUNTS.

    fleet_digest(hours=24) used to ship every agent that ever lived — 1026 rows, 173k chars, a
    firehose wearing a window — because `_roster` was the one stream that ignored `since`. But
    the naive fix (drop anything not seen in the window) would have silently deleted 91 real model
    swaps from the danger map, on the grounds that the graph could not say whether those minds
    were alive. Absence of evidence is not evidence of absence. So: bounded ROWS, whole COUNTS.
    """
    since = NOW - timedelta(hours=24)
    fresh = await actions.create_or_find_object("Agent", "agent:fresh", "fleet-observer")
    await _prop(actions, fresh, "identity_resolved", True, "fleet-observer", DO)
    await _prop(actions, fresh, "last_active", (NOW - timedelta(hours=1)).isoformat(),
                "fleet-observer", DO)
    stale = await actions.create_or_find_object("Agent", "agent:stale", "fleet-observer")
    await _prop(actions, stale, "identity_resolved", True, "fleet-observer", DO)
    await _prop(actions, stale, "last_active", (NOW - timedelta(days=30)).isoformat(),
                "fleet-observer", DO)
    # the ghost: no sighting of any kind, ever — and it carries a swap
    ghost = await actions.create_or_find_object("Agent", "agent:ghost", "fleet-observer")
    await _prop(actions, ghost, "identity_resolved", True, "fleet-observer", DO)
    await _prop(actions, ghost, "model_swapped", "claude-opus-4-8 → claude-haiku-4-5",
                "fleet-observer", DO)

    dg = await fleet_digest(actions, since=since)

    shown = {r["agent"] for r in dg["roster"]}
    assert shown == {"agent:fresh"}                  # the window bounds the ROWS
    assert dg["summary"]["agents"] == 3              # ...and never the COUNTS
    assert dg["summary"]["unseen"] == 1              # the ghost is named, not vanished
    assert dg["summary"]["swapped_unseen"] == 1      # its swap is on the books
    assert "3" in dg["roster_scope"]                 # and the lens says what it did
