"""RECEIPT DIET (operator's context-bloat priority, 2026-09-04, msg 6870/6885): a receipt
returns what the caller needs to ACT, and nothing it did not ask for. prior_art/listener/
co_agents/held_work become opt-in params (send/inbox/mount) rather than always-attached
payload — measured live against Thoth's own transcript a93f82b4 (decision 205f7071):
inbox's own messages carry 131.6 bytes/message of prior_art on average (6.4% of the
messages payload), send's prior_art+listener together are ~38% of its own receipt bytes.
orient()'s own blind_spots list (msg 6885's next assignment) measured as the single
largest static field in a 37-call sample of the same transcript: 154,059 of 751,203 total
orient() bytes (~4.2K bytes/call) — a project-level fact, not a per-call work item.

This file is the RATCHET for the read-verb receipt slice (send, inbox, mount, orient)
AND the write-verb slice (record_decision/open_thread/send nags + prior_art, msg 6871)
— a byte-per-call CEILING per representative call shape, raised
only as a deliberate, reasoned act (same convention as tests/test_tool_contract_diet.py's
own history), never a reflex.
"""
from __future__ import annotations

import json
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.capture import open_thread, record_blind_spot, record_decision


class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


async def _seed(pool, project: str) -> None:
    from src.orchestrator import mounts
    await mounts.save_mount(pool, job_dir=f"/test/seed/{project}", agent_id=f"agent:seed-{project}",
                            project=project, cwd="/test", model=None, session_key=None)


def _receipt_bytes(payload: object) -> int:
    return len(json.dumps(payload).encode("utf-8"))


async def test_inbox_seven_message_peek_prior_art_is_opt_in_and_measurably_smaller(
    actions: Actions, tmp_path: Path,  # noqa: ARG001 -- tmp_path unused, kept for parity
) -> None:
    """THE SEVEN-MESSAGE PEEK (Thoth's own acceptance shape, msg 6870): a realistic mixed
    inbox — some messages carry real prior_art (a standing decision each message's own
    body echoes), some don't — measured BEFORE (want_prior_art=True, functionally
    identical to every pre-diet inbox() call) and AFTER (the new default) for the exact
    same seven messages. AFTER must be smaller; the ceiling below is the measured AFTER
    value, ratcheted."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    proj = "receiptdietbox"
    await _seed(actions.pool, proj)
    standing = await record_decision(
        actions,
        "RULING — the quorumlatch counter must reset on every leader re-election, per the "
        "operator's direct 2026-08-02 instruction.",
        kind="ruling", repo="osiris", source="agent:standing-law-rd")

    send_ctx = _Ctx()
    saved = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(send_ctx)] = AgentIdentity(
        agent_id="agent:rd-sender", session="rdsender", project=proj, model=None, cwd=None)
    try:
        for i in range(7):
            # odd-numbered messages echo the standing decision's own wording closely
            # enough to surface it as prior art (send()'s own hop, grade='ask' triggers
            # it); even-numbered ones are plain chatter
            body = (f"re: the quorumlatch counter reset on leader re-election, item {i}"
                    if i % 2 else f"status update #{i}, nothing load-bearing")
            await srv.send(body, to=proj, grade="ask" if i % 2 else None, ctx=send_ctx)
    finally:
        srv._pool = saved
        srv._agents.pop(srv._conn_key(send_ctx), None)

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        before = await srv.inbox(project=proj, peek=True, want_prior_art=True, ctx=ctx)
        after = await srv.inbox(project=proj, peek=True, ctx=ctx)
    finally:
        srv._pool = saved_pool

    assert len(before["messages"]) == 7
    assert len(after["messages"]) == 7
    before_bytes = _receipt_bytes(before)
    after_bytes = _receipt_bytes(after)
    assert after_bytes < before_bytes, (before_bytes, after_bytes)
    # every message that carried prior_art before still SAYS so after, just as a count
    before_with_pa = sum(1 for m in before["messages"] if m.get("prior_art"))
    after_with_count = sum(1 for m in after["messages"] if m.get("prior_art_count"))
    assert before_with_pa == after_with_count and before_with_pa > 0
    assert not any("prior_art" in m for m in after["messages"])
    # RATCHET: measured exact AFTER value for this fixture (1523 bytes; BEFORE was 2234 —
    # a 31.8% cut from dropping the full prior_art array down to a per-message count).
    # Raise only with a reason, never a reflex.
    assert after_bytes <= 1650, (
        f"seven-message peek receipt grew to {after_bytes} bytes, over the ratchet of "
        f"1650 (before-diet equivalent was {before_bytes}) — if the growth is genuinely "
        "load-bearing, raise the ceiling as a deliberate act with a reason, not a reflex")
    _ = (standing, tmp_path)


async def test_send_dm_receipt_omits_prior_art_and_listener_by_default(
    actions: Actions,
) -> None:
    """send()'s own receipt no longer echoes prior_art/listener unless asked — the
    reader's copy (via inbox) and the write-side persistence are both untouched; only the
    SENDER'S OWN receipt sheds the duplicate payload by default."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    await record_decision(
        actions,
        "RULING — the parallax queue drains oldest-first, verbatim, per operator word.",
        kind="ruling", repo="osiris", source="agent:standing-law-rd2")

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:rd-sender2", session="rdsender2", project="osiris", model=None,
        cwd=None)
    try:
        out = await srv.send("the parallax queue drains oldest-first, please confirm",
                             to_agent="agent:rd-target2", grade="ask", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    assert "prior_art" not in out
    assert "listener" not in out
    assert "prior_art_flag" in out  # the cheap nudge survives — only the full echo is gated
    assert "want_prior_art=True" in out["prior_art_flag"]
    receipt_bytes = _receipt_bytes(out)
    # RATCHET: measured exact value, 388 bytes. Raise only with a reason.
    assert receipt_bytes <= 430, (
        f"send() DM receipt (no prior_art/listener requested) grew to {receipt_bytes} "
        "bytes, over the ratchet of 430")


async def test_mount_receipt_omits_co_agents_and_held_work_by_default(
    actions: Actions, tmp_path: Path,
) -> None:
    """mount()'s own receipt reports COUNTS, not full lists, unless asked — the safety
    signal (something is there) survives at near-zero cost; the full payload is opt-in."""
    from src import mcp_server as srv
    from src.orchestrator import mounts

    proj = "receiptdietmount"
    office = tmp_path / "o"
    office.mkdir()
    (office / ".osiris").write_text(f'project = "{proj}"\n')
    await mounts.save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "rdsib1"),
                            agent_id="agent:rdsib1", project=proj, cwd=str(tmp_path / "sib"),
                            model=None, session_key=None)
    await open_thread(actions, "held: batch the receipt-diet fixture read", repo=proj,
                      kind="obligation", branch="rd-branch",
                      files_touched=["src/orchestrator/receipt_diet.py"])
    job_dir = str(tmp_path / "jobs" / "rdmount01")

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(office), job_dir=job_dir)
    finally:
        srv._pool = saved_pool

    assert "co_agents" not in out and (out.get("co_agents_count") or 0) >= 1
    assert "held_work" not in out and out.get("held_work_count") == 1
    receipt_bytes = _receipt_bytes(out)
    # RATCHET: measured exact value, 502 bytes. Raise only with a reason.
    assert receipt_bytes <= 560, (
        f"mount() receipt (no co_agents/held_work requested) grew to {receipt_bytes} "
        "bytes, over the ratchet of 560")


async def test_orient_omits_blind_spots_list_by_default(actions: Actions) -> None:
    """orient()'s own scoped briefing reports a blind_spots COUNT, not the full list,
    unless asked — the measured largest static (non-work-item) field in a 37-call sample
    of Thoth's own transcript a93f82b4 (~4.2K bytes/call). The safety fact ('something
    here is unverifiable') survives at near-zero cost; the full list is opt-in."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity
    from src.orchestrator.compositions import seed_default_compositions

    proj = "receiptdietorient"
    await _seed(actions.pool, proj)
    await seed_default_compositions(actions.pool)
    for surface in ("ios-touch", "webkit-rendering", "bluetooth-pairing"):
        await record_blind_spot(
            actions, surface, f"{surface} cannot be verified headless",
            verify_with="hand the device to the operator", repo=proj)

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:rd-orienter", session="rdorienter", project=proj, model=None,
        cwd=None)
    try:
        before = await srv.orient(project=proj, want_blind_spots=True, ctx=ctx)
        after = await srv.orient(project=proj, ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    assert len(before["blind_spots"]) == 3
    assert "blind_spots" not in after
    assert after["blind_spots_count"] == 3
    before_bytes = _receipt_bytes(before)
    after_bytes = _receipt_bytes(after)
    assert after_bytes < before_bytes, (before_bytes, after_bytes)
    # RATCHET: measured exact AFTER value for this fixture (908 bytes; BEFORE was 1324 —
    # a 31.4% cut from dropping the full blind_spots array down to a count). Headroom for
    # serial-id digit growth under a full-suite run, same convention as the inbox/send/
    # mount ratchets above. Raise only with a reason, never a reflex.
    assert after_bytes <= 1000, (
        f"orient() receipt (no want_blind_spots requested) grew to {after_bytes} bytes, "
        f"over the ratchet of 1000 (before-diet equivalent was {before_bytes}) — if the "
        "growth is genuinely load-bearing, raise the ceiling as a deliberate act with a "
        "reason, not a reflex")



# --- WRITE-VERB SLICE (msg 6871, operator's context-bloat priority): record_decision/
# open_thread/send — echoed summary dropped, protocol_nag/assertion_nag collapsed to a
# short `nags` code (full text moved to describe('nags:<code>')), prior_art slimmed to
# {id,type,summary} (grade/via dropped). resolve_thread/settle needed no change — already
# lean, same finding Sekhmet's own lane made for get_status()/settle().

async def test_record_decision_receipt_omits_echoed_summary(actions: Actions) -> None:
    """The caller supplied `summary` this same turn — echoing it back is pure duplication.
    `resolved_thread`'s OWN summary (the closed THREAD's words, a different string) still
    echoes: that's the one place a valid id naming the wrong target is only catchable by
    reading it, a mis-citation risk, not a duplication."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:rd-nosummary", session="rdnosummary", project="osiris", model=None,
        cwd=None)
    try:
        out = await srv.record_decision(
            "RULING — the beryl queue drains in arrival order, per operator word.",
            kind="ruling", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    assert "summary" not in out
    assert out["kind"] == "ruling"


async def test_record_decision_protocol_nag_collapses_to_a_code(actions: Actions) -> None:
    """A measurement-smelling decision with no `protocol` used to pay ~300 bytes of nag
    prose on the receipt every time; now it's one short code, full text one lookup away
    (describe('nags:protocol'))."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:rd-nag", session="rdnag", project="osiris", model=None, cwd=None)
    try:
        out = await srv.record_decision(
            "verified 4/5 nodes converge within the threshold window after the index change",
            kind="finding", ctx=ctx)
        described = await srv.describe("nags:protocol")
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    assert "protocol_nag" not in out
    assert out["nags"] == ["protocol"]
    assert described["code"] == "protocol" and "MEASUREMENT" in described["text"]
    receipt_bytes = _receipt_bytes(out)
    # RATCHET: measured exact value, 251 bytes (the old protocol_nag prose alone ran
    # ~330 bytes on its own, on top of everything else in the receipt). Raise only with
    # a reason, never a reflex.
    assert receipt_bytes <= 280, (
        f"record_decision() receipt with a protocol nag grew to {receipt_bytes} bytes, "
        "over the ratchet of 280")


async def test_record_decision_prior_art_is_slimmed_not_the_full_search_shape(
    actions: Actions, monkeypatch,
) -> None:
    """prior_art on record_decision/open_thread keeps {id,type,summary} — enough to
    recognize the hit and go read it — and drops the ranking metadata (`grade`/`via`)
    the underlying search() result carries but this turn's caller doesn't act on. Same
    monkeypatch-the-search-hit fixture shape as test_ack_prior_art_distinguishes_weak_
    hits_from_no_hits above — live search indexing timing is not this test's concern."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    unslimmed_hit = [{"id": "feedf00d", "type": "Decision", "summary": "a related ruling",
                      "grade": "self_declared", "via": "both"}]
    monkeypatch.setattr(
        "src.orchestrator.capture.prior_art_from_hits", lambda *a, **k: unslimmed_hit)

    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:rd-priorart", session="rdpriorart", project="osiris", model=None,
        cwd=None)
    try:
        out = await srv.record_decision(
            "the onyx cache invalidates on every leader re-election, confirmed live",
            kind="finding", repo="osiris", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)

    assert out["prior_art"] == [{"id": "feedf00d", "type": "Decision",
                                 "summary": "a related ruling"}]
