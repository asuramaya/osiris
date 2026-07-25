"""THE ONBOARDING CLUSTERFUCK — the operator brought up ten agents in one window (2026-07-14)
and the identity layer burned THIRTEEN GENERATIONS across seven houses (ruling f7a715a1):
TJMAX V→X in six minutes, Soundwave VI and VII in the same second, Thoth XXX alive for eleven
minutes and zero acts. Root cause: TWO SEAM OBSERVERS THAT DON'T SHARE A CLOCK — the chrome
heartbeat compares against the mount row, the mount/whisper path compares against the
transcript TAIL, and the tail lags a /model until the next assistant turn. Each stamped its
reading; the other read the stamp as a fresh seam; one deliberate swap cascaded.

    A SEAM MUST BE DATED BY THE EVIDENCE THAT WITNESSED IT. A stale observation compared
    against a fresher stamp is an old newspaper arguing with today's — not a death.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator.agents import (
    AgentIdentity,
    claim_name,
    live_succession,
    mint_heir,
    register_agent,
    seat_holders,
)
from src.orchestrator.heal import heal_husks
from src.orchestrator.lineage import register_spawn
from src.orchestrator.liveness import observe_liveness
from src.orchestrator.mounts import save_mount

T0 = datetime(2026, 7, 14, 1, 0, tzinfo=UTC)
FABLE, OPUS = "claude-fable-5", "claude-opus-4-8"


def _ident(sid: str, model: str, at: datetime | None, project: str = "osiris") -> AgentIdentity:
    """An ANCHORED identity (model_method='job_dir') — the only grade the seam gate trusts."""
    return AgentIdentity(agent_id=f"agent:{sid}", session=sid, project=project, model=model,
                         cwd=None, model_method="job_dir", model_observed_at=at)


async def _register(actions: Actions, sid: str, model: str, at: datetime | None,
                    mint_reason: str | None = None) -> str:
    ident = _ident(sid, model, at)
    await register_agent(actions, ident, actor="test", mint_reason=mint_reason)
    return ident.agent_id


async def test_a_STALE_TAIL_is_not_a_seam(actions: Actions) -> None:
    """THE DATING GATE. The register path observed the transcript tail; the tail predated the
    stamp it disagreed with (a /model had landed, no assistant turn yet). TJMAX VIII and IX —
    opposite seams, four seconds apart — were both this. An observation loses to a fresher
    stamp; only fresher evidence may testify to a death."""
    a = await _register(actions, "aaaa0001", OPUS, T0 + timedelta(minutes=5))
    assert a == "agent:aaaa0001"
    # a STALE read (older than the opus stamp) claiming fable — an old newspaper, not a seam
    b = await _register(actions, "aaaa0001", FABLE, T0 + timedelta(minutes=3))
    assert b == "agent:aaaa0001", "a stale tail minted a generation"
    # the SAME claim, but witnessed FRESHER than the stamp — a real seam, one mint
    c = await _register(actions, "aaaa0001", FABLE, T0 + timedelta(minutes=9))
    assert c == "agent:aaaa0001-ii", "a fresh anchored disagreement is a death (a882b334)"


async def test_the_debounce_heals_in_the_REGISTER_path_too(actions: Actions) -> None:
    """The debounce lived only in the heartbeat, so a round-trip whose return leg arrived via
    a MOUNT minted a phantom instead of healing (Soundwave VI). Now: a model seam alone, whose
    head is an actless model-mint younger than the window, heals whichever observer sees it."""
    await _register(actions, "aaaa0002", OPUS, T0)
    heir = await _register(actions, "aaaa0002", FABLE, datetime.now(UTC))
    assert heir == "agent:aaaa0002-ii"
    back = await _register(actions, "aaaa0002", OPUS, datetime.now(UTC))
    assert back == "agent:aaaa0002", "the return leg reached a mount and did not heal"
    fm = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='agent:aaaa0002-ii' AND a.name='false_mint' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1")
    assert fm == "true", "the phantom's record must say what it was"


async def test_a_COMPACTION_mint_never_debounces(actions: Actions) -> None:
    """A context death is a death: the weights survive, the memory does not (a882b334). Only
    MODEL flapping heals — a compact whose model happens to round-trip still minted a mind."""
    await _register(actions, "aaaa0003", OPUS, T0)
    heir = await _register(actions, "aaaa0003", OPUS, datetime.now(UTC),
                           mint_reason="compaction")
    assert heir == "agent:aaaa0003-ii"
    again = await _register(actions, "aaaa0003", OPUS, datetime.now(UTC),
                            mint_reason="compaction")
    assert again == "agent:aaaa0003-iii", "a compaction is never settings churn"


# ═══ NOTIFY-AT-SEAM (thread aeae9977) — a compacting worker DMs its own manager, with the
# daemon's own reachability() evidence inline. Only the silent class (compaction/context-
# clear); a manager of record must exist; both gates proven with a negative case. ═══════════


async def _bind_managed_worker(
    actions: Actions, agent_id: str, *, handle: str, manager_seat: str,
) -> str:
    """claim a seat for `agent_id` and put it under `manager_seat`'s management — the exact
    managed_by shape notify-at-seam reads. Returns the worker's own seat canonical."""
    a = await actions.create_or_find_object("Agent", agent_id, agent_id)
    await actions.assert_property(a, "project", "osiris", "test", T0, 0.9,
                                  evidence_class="self_declared")
    claimed = await claim_name(actions, agent_id, handle, source="test")
    worker = await actions.create_or_find_object("Seat", claimed["seat_id"], "test")
    manager = await actions.create_or_find_object("Seat", manager_seat, "test")
    await actions.create_link(worker, manager, "managed_by", "test", datetime.now(UTC), 0.9,
                              evidence_class="self_declared")
    return str(claimed["seat_id"])


async def test_a_compaction_mint_notifies_the_managed_by_manager(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ra's clean repro (aeae9977): a compacting worker's manager learned from the HUMAN,
    not the fleet. The heir now DMs its own manager, and the daemon's own reachability()
    confirmation rides along inline — not just our say-so."""
    from src.ingest.harness import claude_daemon

    ancestor = await _register(actions, "cccc0001", OPUS, T0)
    seat_id = await _bind_managed_worker(actions, ancestor, handle="Ptah",
                                         manager_seat="seat:cccc9999")
    await save_mount(actions.pool, job_dir="/home/t/.claude/jobs/cccc0001",
                     agent_id=ancestor, project="osiris", cwd="/t", model=OPUS,
                     session_key=None)
    job = {"short": "cccc0001", "sessionId": "cccc0001-full"}

    async def _fake(ids: set[str]) -> dict[str, Any] | None:
        return job if "cccc0001" in ids else None

    monkeypatch.setattr(claude_daemon, "job_for", _fake)

    before = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    heir = await _register(actions, "cccc0001", OPUS, datetime.now(UTC),
                           mint_reason="compaction")
    after = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")

    assert heir == "agent:cccc0001-ii"
    assert after == before + 1, "the heir must DM its manager exactly once"
    row = await actions.pool.fetchrow(
        "SELECT from_agent, to_agent, body, grade FROM fleet_messages ORDER BY id DESC LIMIT 1")
    assert row["from_agent"] == heir
    assert row["to_agent"] == "seat:cccc9999"
    assert row["grade"] == "fyi"
    assert "Ptah" in row["body"] and "compaction" in row["body"]
    assert "cccc0001 is live right now" in row["body"], (
        "the daemon's OWN confirmation must ride inline, not a bare claim")
    assert seat_id != "seat:cccc9999"  # sanity: worker and manager are distinct seats


async def test_a_compaction_mint_is_silent_with_no_manager_of_record(
    actions: Actions,
) -> None:
    """The same 'nobody to confess to' shape Stage A's stop-hook already uses: a claimed,
    bound seat with NO managed_by edge sends nothing — there is no one to notify."""
    ancestor = await _register(actions, "cccc0002", OPUS, T0)
    a = await actions.create_or_find_object("Agent", ancestor, ancestor)
    await actions.assert_property(a, "project", "osiris", "test", T0, 0.9,
                                  evidence_class="self_declared")
    await claim_name(actions, ancestor, "Anubis", source="test")

    before = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    heir = await _register(actions, "cccc0002", OPUS, datetime.now(UTC),
                           mint_reason="compaction")
    after = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")

    assert heir == "agent:cccc0002-ii"
    assert after == before, "an unmanaged worker has nobody to notify"


async def test_a_model_succession_mint_does_NOT_notify(actions: Actions) -> None:
    """The whitelist is precise on purpose: model-succession and live-swap already surface
    on the membrane's DANGER map, so a plain anchored model swap — no mint_reason at all —
    must never fire this, even with a manager of record sitting right there."""
    ancestor = await _register(actions, "cccc0003", OPUS, T0)
    await _bind_managed_worker(actions, ancestor, handle="Sobek",
                               manager_seat="seat:cccc8888")

    before = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")
    heir = await _register(actions, "cccc0003", FABLE, datetime.now(UTC))
    after = await actions.pool.fetchval("SELECT count(*) FROM fleet_messages")

    assert heir == "agent:cccc0003-ii", "the model seam itself must still mint"
    assert after == before, "model-succession is not in the notify whitelist"


async def test_TWO_concurrent_heartbeats_mint_ONE_generation(actions: Actions) -> None:
    """Soundwave VI and VII: identical seam strings, the same second — two heartbeats raced
    the read-compare-mint and STACKED. The mint lock serializes per lineage; the loser
    re-reads inside the lock, sees the winner's write, and concludes no-op."""
    sid = "beef0001"
    await actions.create_or_find_object("Agent", f"agent:{sid}", "test")
    await save_mount(actions.pool, job_dir=f"/home/t/.claude/jobs/{sid}",
                     agent_id=f"agent:{sid}", project="osiris", cwd="/t",
                     model=OPUS, session_key="sid:test")
    results = await asyncio.gather(
        live_succession(actions, session_id=sid + "aaaa-bbbb", observed_model=FABLE),
        live_succession(actions, session_id=sid + "aaaa-bbbb", observed_model=FABLE))
    minted = [r for r in results if "minted" in r]
    assert len(minted) == 1, f"the race minted {len(minted)} generations: {results}"
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' AND canonical LIKE $1",
        f"agent:{sid}%")
    assert n == 2, "exactly the ancestor and one heir"


# ═══ /succession IDEMPOTENCY (thread 8dc9940c) ═══════════════════════════════════════════
# Thoth's own live repro: one real fable→opus swap, minted correctly once — then THREE
# numerals for it (agent:ad1a1cb0-g40-xx → xxi → xxii) because agent_mounts.model kept
# drifting back to fable between mounts (a separate, still-open root) and every later
# heartbeat re-detected the SAME already-completed transition as if it were new. NOT the
# debounce's case: each intermediate generation genuinely ACTED (sent real mail) before
# being succeeded, so heal_husks/round-trip debounce correctly refuse to touch them —
# idempotency on the /succession call site is the only fix that doesn't erase a mind that
# spoke.


async def test_idempotent_swap_repeats_without_reminting(actions: Actions) -> None:
    """A real swap mints once; the stored model then drifts back and the next heartbeat
    re-detects the identical transition. Caught even though the heir ACTED in between
    (msg-1078-shaped) — this is NOT the unwitnessed-round-trip case the debounce already
    covers; the idempotency check works independent of whether anyone spoke."""
    sid = "1de40001"
    await actions.create_or_find_object("Agent", f"agent:{sid}", "test")
    await save_mount(actions.pool, job_dir=f"/home/t/.claude/jobs/{sid}",
                     agent_id=f"agent:{sid}", project="osiris", cwd="/t",
                     model=FABLE, session_key="sid:test")
    first = await live_succession(actions, session_id=sid + "aaaa-bbbb", observed_model=OPUS)
    heir = first["minted"]
    assert heir == f"agent:{sid}-ii"
    await _acts(actions, heir)  # a REAL act — the old debounce would refuse to heal this
    # the drift: agent_mounts.model resets to fable (the reset's own root cause is a
    # separate, still-open question — this reproduces its OBSERVABLE effect)
    await actions.pool.execute(
        "UPDATE agent_mounts SET model=$1 WHERE job_dir=$2",
        FABLE, f"/home/t/.claude/jobs/{sid}")

    again = await live_succession(actions, session_id=sid + "aaaa-bbbb", observed_model=OPUS)

    assert "minted" not in again
    assert again["unchanged"] is True
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' AND canonical LIKE $1",
        f"agent:{sid}%")
    assert n == 2, "the duplicate re-detection minted a THIRD generation"
    row_model = await actions.pool.fetchval(
        "SELECT model FROM agent_mounts WHERE job_dir=$1", f"/home/t/.claude/jobs/{sid}")
    assert row_model == OPUS, "the drifted row must be repaired, not left stale"


async def test_a_genuinely_new_target_still_mints_after_an_idempotent_repair(
    actions: Actions,
) -> None:
    """The idempotency guard only absorbs a REPEAT of a transition this lineage already
    recorded — a target it has never reached before is a real seam and mints exactly as
    before, even against the same stale-row drift pattern."""
    sid = "1de40002"
    await actions.create_or_find_object("Agent", f"agent:{sid}", "test")
    await save_mount(actions.pool, job_dir=f"/home/t/.claude/jobs/{sid}",
                     agent_id=f"agent:{sid}", project="osiris", cwd="/t",
                     model=FABLE, session_key="sid:test")
    first = await live_succession(actions, session_id=sid + "aaaa-bbbb", observed_model=OPUS)
    await _acts(actions, first["minted"])
    await actions.pool.execute(
        "UPDATE agent_mounts SET model=$1 WHERE job_dir=$2",
        FABLE, f"/home/t/.claude/jobs/{sid}")

    sonnet = "claude-sonnet-5"
    again = await live_succession(actions, session_id=sid + "aaaa-bbbb", observed_model=sonnet)

    assert again.get("minted") == f"agent:{sid}-iii", "a genuinely new target must still mint"


async def test_idempotency_never_absorbs_a_COMPACTION_head(actions: Actions) -> None:
    """A compaction mint stamps no model_succession at all — _already_reached has nothing
    to compare against and must never mistake silence for a match."""
    a = await actions.create_or_find_object("Agent", "agent:1de40003", "test")
    await mint_heir(actions, "agent:1de40003", a, because="compaction", succession=None)
    await save_mount(actions.pool, job_dir="/home/t/.claude/jobs/1de40003",
                     agent_id="agent:1de40003-ii", project="osiris", cwd="/t",
                     model=FABLE, session_key="sid:test")

    out = await live_succession(actions, session_id="1de40003aaaa-bbbb", observed_model=OPUS)

    assert out.get("minted") == "agent:1de40003-iii"


async def test_a_promoted_mount_row_FOLLOWS_the_lineage_head(
    actions: Actions, tmp_path: Path,
) -> None:
    """Ferryman IV read LIVE beside Ferryman V; Anubis XII beside XIII — promotion is by
    transcript mtime, and mtime knows nothing of succession. Every mind onboarded that night
    was co-agent-warned about its own ancestor. A promoted row re-points at the head."""
    sid = "cafe0001"
    a = await actions.create_or_find_object("Agent", f"agent:{sid}", "test")
    await mint_heir(actions, f"agent:{sid}", a, because="compaction", succession=None)
    # a PROVISIONAL seat naming the ANCESTOR (alive=False → no pulse until the disk speaks)
    await save_mount(actions.pool, job_dir=f"/home/t/.claude/jobs/{sid}",
                     agent_id=f"agent:{sid}", project="osiris", cwd="/t",
                     model=OPUS, session_key="whisper:test", alive=False)
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / f"{sid}-1111-2222.jsonl").write_text('{"type":"user"}\n')
    assert await observe_liveness(actions.pool, tmp_path) == 1
    row = await actions.pool.fetchrow(
        "SELECT agent_id, last_seen FROM agent_mounts WHERE job_dir LIKE '%' || $1", sid)
    assert row["last_seen"] is not None, "the transcript moved — the seat earned its pulse"
    assert row["agent_id"] == f"agent:{sid}-ii", \
        "the row still names a superseded generation — its own descendant reads as a co-agent"


async def test_a_ghost_spawn_earns_NO_heartbeat(actions: Actions) -> None:
    """42 of the 44 spawns registered that night were spawn_witnessed=false — announced by
    the harness, transcript never materialized — and the stop-stamp handed each a last_active
    pulse anyway. A HEARTBEAT MUST BE EARNED BY AN ACT, NEVER GRANTED BY A GREETING."""
    ghost = await register_spawn(actions, "a" + "0" * 16, agent_type="ghost", done=True)
    assert ghost is not None
    la = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='last_active'", ghost)
    assert la is None, "an unwitnessed stop-announcement stamped a pulse"
    real = await register_spawn(actions, "b" + "1" * 16, agent_type="worker", done=True,
                                witnessed=True)
    la = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='last_active'", real)
    assert la is not None, "a witnessed child's death must be dated"


async def test_seat_holders_do_not_count_HEALED_PHANTOMS(actions: Actions) -> None:
    """TJMAX read X when ~six minds ever acted: seat_holders counted false mints, so a healed
    phantom still inflated every later numeral, forever. RETIRED real holders still count —
    they held the seat; filtering them would renumber history."""
    now = datetime.now(UTC)
    for i, (canon, phantom) in enumerate(
            [("agent:d00d0001", False), ("agent:d00d0002", True), ("agent:d00d0003", False)]):
        o = await actions.create_or_find_object("Agent", canon, "test")
        await actions.assert_property(o, "handle", "TestSeat", "test", now, 0.9,
                                      evidence_class="self_declared")
        await actions.assert_property(o, "project", "osiris", "test", now, 0.9,
                                      evidence_class="self_declared")
        if phantom:
            await actions.assert_property(o, "false_mint", "true", "test", now, 0.9,
                                          evidence_class="self_declared")
        if i == 2:  # a real holder who RETIRED — held the seat, still counts
            await actions.assert_property(o, "retired", "true", "test", now, 0.9,
                                          evidence_class="self_declared")
    holders = await seat_holders(actions.pool, "osiris", "TestSeat")
    assert holders == ["agent:d00d0001", "agent:d00d0003"]


async def test_mint_heir_passes_the_HOUSE_with_the_blood(actions: Actions) -> None:
    """Heartbeat-minted heirs carried a project assertion but no works_in EDGE — invisible to
    every lens that walks the edge. The heir now inherits both at the mint."""
    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", "agent:f00d0001", "test")
    p = await actions.create_or_find_object("SoftwareProject", "repo:osiris", "test")
    await actions.assert_property(a, "project", "osiris", "test", now, 0.9,
                                  evidence_class="self_declared")
    await actions.create_link(a, p, "works_in", "test", now, 0.9,
                              evidence_class="self_declared")
    heir, heir_oid = await mint_heir(actions, "agent:f00d0001", a, because="live-swap",
                                     succession=f"{OPUS} → {FABLE}")
    linked = await actions.pool.fetchval(
        "SELECT 1 FROM links l JOIN objects t ON t.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='works_in' AND t.canonical='repo:osiris'", heir_oid)
    assert linked, "the heir has a project but no house — every works_in lens misses it"


async def _acts(actions: Actions, canonical: str) -> None:
    """Give an agent a REAL act (an assertion on a foreign domain object) — the thing a husk,
    by definition, never did."""
    t = await actions.create_or_find_object("Thread", f"thread:{uuid.uuid4().hex[:12]}",
                                            canonical)
    await actions.assert_property(t, "status", "open", canonical, datetime.now(UTC), 0.9,
                                  evidence_class="self_declared")


async def test_the_heal_retires_a_MID_CHAIN_husk_and_moves_its_estate(
    actions: Actions,
) -> None:
    """base(real) → ii(husk) → iii(real head): ii heals as false_mint, its unread mail and
    mount rows follow iii, and the walk still lands on iii through the healed link."""
    a = await actions.create_or_find_object("Agent", "agent:ea570001", "test")
    _, ii_oid = await mint_heir(actions, "agent:ea570001", a, because="compaction",
                                succession=None)
    _, _ = await mint_heir(actions, "agent:ea570001-ii", ii_oid, because="compaction",
                           succession=None)
    await _acts(actions, "agent:ea570001-iii")
    await save_mount(actions.pool, job_dir="/home/t/.claude/jobs/ea570001",
                     agent_id="agent:ea570001-ii", project="osiris", cwd="/t",
                     model=OPUS, session_key="sid:test")

    dry = await heal_husks(actions, ["agent:ea570001-ii"])
    assert dry["applied"] is False and dry["verified"] == ["agent:ea570001-ii"]
    assert dry["plan"][0]["estate_to"] == "agent:ea570001-iii"

    out = await heal_husks(actions, ["agent:ea570001-ii"], apply=True)
    assert out["applied"] is True
    fm = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 "
        "AND name='false_mint' ORDER BY confidence DESC, observed_at DESC LIMIT 1", ii_oid)
    assert fm == "true"
    seat = await actions.pool.fetchval(
        "SELECT agent_id FROM agent_mounts WHERE job_dir='/home/t/.claude/jobs/ea570001'")
    assert seat == "agent:ea570001-iii", "the husk's mount row must follow the living head"


async def test_the_heal_UNWINDS_a_tail_of_husks(actions: Actions) -> None:
    """7118bf41: the last two generations were both husks, so the lineage HEAD was a corpse —
    and lineage_head reads succeeded_by, not false_mint. The heal restores the last real mind
    as head by clearing its forward pointer (a compensating stamp, never a delete)."""
    a = await actions.create_or_find_object("Agent", "agent:ea570002", "test")
    await _acts(actions, "agent:ea570002")
    _, ii_oid = await mint_heir(actions, "agent:ea570002", a, because="compaction",
                                succession=None)
    await mint_heir(actions, "agent:ea570002-ii", ii_oid, because="live-swap",
                    succession=f"{OPUS} → {FABLE}")
    out = await heal_husks(actions, ["agent:ea570002-ii", "agent:ea570002-iii"], apply=True)
    assert out["unwinds"] == [{"lineage": "agent:ea570002",
                               "head_restored": "agent:ea570002"}]
    from src.orchestrator.agents import lineage_head
    assert await lineage_head(actions.pool, "agent:ea570002") == "agent:ea570002", \
        "the head-walk still lands on a corpse"


async def test_the_heal_REFUSES_a_mind_that_acted(actions: Actions) -> None:
    """The verification is re-derived at heal time, never trusted from the ticket: one real
    act and the agent is a mind — not ours to erase, whatever the diagnosis said."""
    a = await actions.create_or_find_object("Agent", "agent:ea570003", "test")
    await mint_heir(actions, "agent:ea570003", a, because="compaction", succession=None)
    await _acts(actions, "agent:ea570003-ii")
    out = await heal_husks(actions, ["agent:ea570003-ii"], apply=True)
    assert out["verified"] == [] and "ACTED" in out["refused"]["agent:ea570003-ii"]
    fm = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='agent:ea570003-ii' AND a.name='false_mint'")
    assert fm is None
