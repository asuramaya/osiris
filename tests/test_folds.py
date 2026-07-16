"""AGENT FOLDS — the reconciliation primitive (thread b975851b, operator directive
2026-07-16: append-only merging so the fleet census deflates without one DELETE).

The kernel merge (Actions.merge_objects) is already covered by the entity tests; these
witness the AGENT half: the estate follows the fold, provenance survives, the census
deflates, and every refusal is loud.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.folds import canonical_agent, fold_agent, living_head
from src.orchestrator.mailbox import send_message, unread_count
from src.orchestrator.mounts import save_mount


async def _mk_agent(actions: Actions, label: str, project: str = "foldhouse") -> None:
    a = await actions.create_or_find_object("Agent", label, label)
    await actions.assert_property(a, "project", project, label, datetime.now(UTC), 0.9,
                                  evidence_class="self_declared")


async def test_fold_moves_the_estate_and_deflates_the_census(actions: Actions) -> None:
    p = actions.pool
    await _mk_agent(actions, "agent:f01dbeef")            # the clone (a tab-alias mint)
    await _mk_agent(actions, "agent:0c11ec7a")            # the real lineage, base...
    await _mk_agent(actions, "agent:0c11ec7a-ii")         # ...whose HEAD is generation ii
    await save_mount(p, job_dir="/jobs/0c11ec7a", agent_id="agent:0c11ec7a-ii",
                     project="foldhouse", cwd="/w/foldhouse", model=None, session_key=None)
    await save_mount(p, job_dir="/jobs/f01dbeef", agent_id="agent:f01dbeef",
                     project="foldhouse", cwd="/w/foldhouse", model=None, session_key=None)
    await send_message(p, from_agent="agent:sender", from_project="osiris",
                       to_agent="agent:f01dbeef", body="answers you never got")
    tid = await actions.create_or_find_object("Thread", "thread:f01d0001", "agent:f01dbeef")
    now = datetime.now(UTC)
    await actions.assert_property(tid, "owner", "agent:f01dbeef", "agent:f01dbeef",
                                  now, 0.9, evidence_class="self_declared")
    before = await p.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' AND status='active'")

    out = await fold_agent(actions, dupe="agent:f01dbeef", into="agent:0c11ec7a",
                           evidence="census: tmp-only jobs dir, no transcript, co-timed "
                                    "with the living session at the same cwd",
                           actor="agent:test")

    assert out["living_head"] == "agent:0c11ec7a-ii"       # estate lands on the HEAD
    assert out["mail_readdressed"] == 1
    assert out["mount_rows_repointed"] == 1
    assert out["threads_reowned"] == 1
    # the projection: folded label leaves the census; canonical resolves at read
    after = await p.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' AND status='active'")
    assert after == before - 1
    assert await canonical_agent(p, "agent:f01dbeef") == "agent:0c11ec7a"
    # the mail reached the living head, readable by IT
    assert await unread_count(p, "foldhouse", reader_agent="agent:0c11ec7a-ii") >= 1
    # authorship SURVIVES: the thread's creation is still the dupe's act on the record
    src = await p.fetchval(
        "SELECT a.source_id FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='thread:f01d0001' AND a.name='owner' "
        "ORDER BY a.observed_at LIMIT 1")
    assert src == "agent:f01dbeef"


async def test_send_forwards_a_folded_address_and_confesses(actions: Actions) -> None:
    p = actions.pool
    await _mk_agent(actions, "agent:dead1111")
    await _mk_agent(actions, "agent:beef2222")
    await save_mount(p, job_dir="/jobs/beef2222", agent_id="agent:beef2222",
                     project="foldhouse", cwd="/w/f2", model=None, session_key=None)
    await fold_agent(actions, dupe="agent:dead1111", into="agent:beef2222",
                     evidence="test evidence", actor="agent:test")

    out = await send_message(p, from_agent="agent:sender", from_project="osiris",
                             to_agent="agent:dead1111", body="stale address book")

    assert out["to_agent"] == "agent:beef2222"      # the forwarding order executed
    assert out["folded_from"] == "agent:dead1111"   # ...and confessed in the receipt


async def test_fold_refusals_are_loud_and_write_nothing(actions: Actions) -> None:
    await _mk_agent(actions, "agent:aaaa5555")
    await _mk_agent(actions, "agent:bbbb6666")

    out = await fold_agent(actions, dupe="agent:aaaa5555", into="agent:bbbb6666",
                           evidence="   ", actor="agent:test")
    assert "auto-merge" in out["error"]              # evidence is mandatory
    out = await fold_agent(actions, dupe="agent:aaaa5555", into="agent:aaaa5555-ii",
                           evidence="x", actor="agent:test")
    assert "succession" in out["error"]              # generations never fold
    out = await fold_agent(actions, dupe="agent:nobody99", into="agent:bbbb6666",
                           evidence="x", actor="agent:test")
    assert "unknown" in out["error"]
    # nothing was written by any refusal
    st = await actions.pool.fetchval(
        "SELECT status FROM objects WHERE canonical='agent:aaaa5555'")
    assert st == "active"


async def test_fold_refuses_a_seated_dupe(actions: Actions) -> None:
    from src.orchestrator.seats import attach_session, ensure_seat, mint_attach_token

    p = actions.pool
    await _mk_agent(actions, "agent:cccc7777", project="seathouse")
    await _mk_agent(actions, "agent:dddd8888", project="seathouse")
    await save_mount(p, job_dir="/jobs/cccc7777", agent_id="agent:cccc7777",
                     project="seathouse", cwd="/w/s1", model=None, session_key=None)
    seat = await ensure_seat(actions, house="seathouse", handle="Folded",
                             anchor_cwd="/w/s1", source="test")
    token = await mint_attach_token(p, seat_id=seat["seat_id"])
    await attach_session(actions, seat_id=seat["seat_id"], token=token,
                         job_dir="/jobs/cccc7777", agent_id="agent:cccc7777")

    out = await fold_agent(actions, dupe="agent:cccc7777", into="agent:dddd8888",
                           evidence="x", actor="agent:test")

    assert "holds" in out["error"] and "seat" in out["error"].lower()


async def test_fold_of_an_already_folded_label_points_home(actions: Actions) -> None:
    await _mk_agent(actions, "agent:eeee9999")
    await _mk_agent(actions, "agent:ffff0000")
    await _mk_agent(actions, "agent:abab1212")
    await fold_agent(actions, dupe="agent:eeee9999", into="agent:ffff0000",
                     evidence="x", actor="agent:test")

    out = await fold_agent(actions, dupe="agent:eeee9999", into="agent:abab1212",
                           evidence="x", actor="agent:test")
    assert "already folded" in out["error"] and "agent:ffff0000" in out["error"]
    out = await fold_agent(actions, dupe="agent:abab1212", into="agent:eeee9999",
                           evidence="x", actor="agent:test")
    assert "living label" in out["error"]            # fold into the canonical, not a ghost


async def test_living_head_reads_the_registry(actions: Actions) -> None:
    p = actions.pool
    await save_mount(p, job_dir="/jobs/12ab34cd", agent_id="agent:12ab34cd-iii",
                     project="headhouse", cwd="/w/h", model=None, session_key=None)
    assert await living_head(p, "agent:12ab34cd") == "agent:12ab34cd-iii"
    assert await living_head(p, "agent:12ab34cd-ii") == "agent:12ab34cd-iii"
    assert await living_head(p, "agent:never5een") == "agent:never5een"
