"""The membrane — fleet_digest, the operator's window into the autonomous fleet.

Constructs a small fleet with all four upward streams — a clean and an unresolved identity, a
model-swapped agent, agent-authored activity inside and outside the window (and miner backfill
that must be excluded), and a relay/origin co-assertion — and asserts the digest surfaces each.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.actions.core import Actions
from src.orchestrator.digest import fleet_digest
from src.orchestrator.mailbox import OPERATOR_ADDR, send_message
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
    u = await actions.create_or_find_object("Agent", "agent:unknown-heinrich", "fleet-observer")
    await _prop(actions, u, "identity_resolved", False, "fleet-observer", DO)
    # --- activity: a fresh agent-authored decision, an OLD one, and miner backfill (excluded) ---
    d = await actions.create_or_find_object("Decision", "decision:recent", "agent:aaa")
    await _prop(actions, d, "summary", "shipped credence", "agent:aaa", SD)
    old = await actions.create_or_find_object("Decision", "decision:old", "agent:aaa")
    await _prop(actions, old, "summary", "ancient", "agent:aaa", SD, when=NOW - timedelta(days=3))
    m = await actions.create_or_find_object("Decision", "decision:mined", "session-miner")
    await _prop(actions, m, "summary", "mined noise", "session-miner", "derived", conf=0.4)
    # --- laundering: agent:bbb (child) observed; agent:aaa (parent) relayed it inflated ---
    b = await actions.create_or_find_object("Agent", "agent:bbb", "fleet-observer")
    await actions.create_link(b, a, "spawned_by", "fleet-observer", NOW, 0.6, evidence_class=DO)
    await _prop(actions, a, "backed_by_observation", False, "fleet-observer", DO, conf=0.6)
    o = await actions.create_or_find_object("SoftwareProject", "repo:x", "test")
    await _prop(actions, o, "status", "relayed", "agent:aaa", SD)
    await _prop(actions, o, "status", "observed", "agent:bbb", DO, conf=0.6)

    dg = await fleet_digest(actions, since=since)

    # roster: 3 agents, 1 unresolved, 1 swapped
    assert dg["summary"]["agents"] == 3
    assert dg["summary"]["unresolved"] == 1
    assert dg["summary"]["swapped"] == 1
    assert any(not r["resolved"] and r["agent"] == "agent:unknown-heinrich" for r in dg["roster"])
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
    assert dg["laundering"][0]["value"] == "observed"  # origin won, relay clamped


async def test_digest_surfaces_conversations_and_the_operator_desk(actions: Actions) -> None:
    """The upward lane's compliance-free half: lateral threads and the operator's inbox are
    read straight off fleet_messages — an agent that shirks its report-up duty is still seen."""
    p = actions.pool
    since = NOW - timedelta(hours=24)
    # a lateral request→reply thread between two projects
    ask = await send_message(p, from_agent="agent:aaa", from_project="decepticons",
                             to_project="heinrich", body="run the ablation?")
    await send_message(p, from_agent="agent:bbb", from_project="heinrich",
                       body="done — organ delta reproduces", reply_to=ask["id"])
    # a report-up brief on the operator's desk
    await send_message(p, from_agent="agent:bbb", from_project="heinrich",
                       to_project=OPERATOR_ADDR, body="FINDING: organ delta is real; "
                       "two witnesses agree. Details in decision log.")

    dg = await fleet_digest(actions, since=since)

    assert dg["summary"]["conversations"] >= 1
    lateral = next(c for c in dg["conversations"] if c["thread"] == ask["id"])
    assert set(lateral["between"]) >= {"decepticons", "heinrich"}
    assert lateral["msgs"] == 2
    assert lateral["last"]["body"].startswith("done — organ delta")
    # the desk: counted and previewed, newest first, NOT leased by the digest (a peek)
    assert dg["summary"]["operator_unread"] == 1
    assert dg["operator_inbox"]["latest"][0]["from_project"] == "heinrich"
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
