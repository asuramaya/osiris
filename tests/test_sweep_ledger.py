"""sweep_ledger — Finding A (thread 5177057a, Thoth's design approval DM 1326): PreCompact ->
sweep_route only ever ENQUEUED an async arq job with no confirmation it ran. B7 (the orphan
reaper) catches a transcript that never got ANY successful sweep, ever, but mark_swept's
watermark is a one-time-ever boolean per file — so it goes permanently blind to a dropped
enqueue on a lineage's 2nd/3rd/Nth compaction once the 1st already succeeded. This ledger
tracks one row per ENQUEUE ATTEMPT so a lightweight watchdog can retry (or, past a ceiling,
escalate loudly rather than loop forever) exactly that gap, without reviving the crawl.
"""
from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from src.actions.core import Actions
from src.workers.arq_worker import (
    SWEEP_RETRY_CEILING,
    SWEEP_RETRY_SLA,
    _mark_ledger_done,
    reap_stuck_sweeps,
    sweep_session,
)


class _FakeCascade:
    def __init__(self, actions: Actions) -> None:
        self.actions = actions


def _ctx(actions: Actions) -> dict[str, Any]:
    return {"cascade": _FakeCascade(actions)}


async def _enqueue_row(actions: Actions, path: str, session_id: str, *, age_secs: float) -> int:
    """Insert a ledger row as if sweep_route had just fired, backdated by `age_secs`."""
    row = await actions.pool.fetchrow(
        "INSERT INTO sweep_ledger (transcript_path, session_id, enqueued_at) "
        "VALUES ($1, $2, now() - make_interval(secs => $3)) RETURNING id",
        path, session_id, float(age_secs))
    return row["id"]


async def test_mark_ledger_done_completes_only_still_incomplete_rows(actions: Actions) -> None:
    a = await _enqueue_row(actions, "/t/one.jsonl", "sess1", age_secs=10)
    b = await _enqueue_row(actions, "/t/one.jsonl", "sess1", age_secs=5)  # a second attempt
    await _mark_ledger_done(actions.pool, "/t/one.jsonl")
    rows = await actions.pool.fetch(
        "SELECT id, completed_at FROM sweep_ledger WHERE id = ANY($1)", [a, b])
    assert all(r["completed_at"] is not None for r in rows)


async def test_mark_ledger_done_never_reopens_an_already_completed_row(actions: Actions) -> None:
    rid = await _enqueue_row(actions, "/t/two.jsonl", "sess2", age_secs=10)
    await _mark_ledger_done(actions.pool, "/t/two.jsonl")
    first = await actions.pool.fetchval(
        "SELECT completed_at FROM sweep_ledger WHERE id = $1", rid)
    await _mark_ledger_done(actions.pool, "/t/two.jsonl")  # a second, later call
    second = await actions.pool.fetchval(
        "SELECT completed_at FROM sweep_ledger WHERE id = $1", rid)
    assert first == second, "WHERE completed_at IS NULL must not touch a row twice"


async def test_sweep_route_writes_one_ledger_row_alongside_the_enqueue(
    actions: Actions,
) -> None:
    from src import mcp_server as srv

    class _FakeArq:
        def __init__(self) -> None:
            self.enqueued: list[tuple[str, tuple[Any, ...]]] = []

        async def enqueue_job(self, name: str, *args: Any) -> None:
            self.enqueued.append((name, args))

    class _FakeRequest:
        async def json(self) -> dict[str, str]:
            return {"transcript_path": "/t/three.jsonl", "session_id": "sess3",
                    "trigger": "compact"}

    saved_pool, saved_arq = srv._pool, srv._arq
    srv._pool = actions.pool
    srv._arq = _FakeArq()
    try:
        out = await srv.sweep_route(_FakeRequest())
    finally:
        srv._pool, srv._arq = saved_pool, saved_arq
    assert out.status_code == 200 if hasattr(out, "status_code") else True
    row = await actions.pool.fetchrow(
        "SELECT transcript_path, session_id, completed_at FROM sweep_ledger "
        "WHERE session_id = 'sess3'")
    assert row is not None
    assert row["transcript_path"] == "/t/three.jsonl"
    assert row["completed_at"] is None, "just enqueued — not yet swept"


async def test_sweep_session_marks_the_ledger_done_even_when_the_subsystem_is_dark(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The FIRST early return (osiris_sense_sessions unset — mining is off entirely) must not
    leave a ledger row stuck forever: nothing will ever retry it into existing, so the
    watchdog must not nag (and eventually poison-pill escalate) something that was never
    going to run by design."""
    import src.workers.arq_worker as worker

    monkeypatch.setattr(worker, "get_settings",
                        lambda: SimpleNamespace(osiris_sense_sessions=""))
    rid = await _enqueue_row(actions, "/t/dark.jsonl", "sess4", age_secs=1)
    out = await sweep_session(_ctx(actions), "/t/dark.jsonl")
    assert out == 0
    assert await actions.pool.fetchval(
        "SELECT completed_at FROM sweep_ledger WHERE id = $1", rid) is not None


async def test_sweep_session_marks_the_ledger_done_when_out_of_scope(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, tmp_path: Any,
) -> None:
    """The SECOND early return (out-of-scope defer, task #37) is a deliberate, stable
    decision — same reasoning as the dark-subsystem case: the ledger row must not sit
    incomplete waiting for a retry that scope will never allow."""
    import src.workers.arq_worker as worker

    d = tmp_path / "-repo-not-armed"
    d.mkdir()
    p = d / "sess5.jsonl"
    p.write_text('{"type":"user","message":{"content":"hi"}}\n')
    monkeypatch.setattr(worker, "get_settings", lambda: SimpleNamespace(
        osiris_sense_sessions=str(tmp_path), osiris_sense_projects="only-this-other-one"))
    rid = await _enqueue_row(actions, str(p), "sess5", age_secs=1)
    out = await sweep_session(_ctx(actions), str(p))
    assert out == 0
    assert await actions.pool.fetchval(
        "SELECT completed_at FROM sweep_ledger WHERE id = $1", rid) is not None


async def test_reap_stuck_sweeps_retries_a_row_inside_the_ceiling(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.workers.arq_worker as worker

    retried: list[str] = []

    async def _fake_sweep(ctx: dict[str, Any], transcript: str) -> int:
        retried.append(transcript)
        await _mark_ledger_done(ctx["cascade"].actions.pool, transcript)
        return 0

    monkeypatch.setattr(worker, "sweep_session", _fake_sweep)
    await _enqueue_row(actions, "/t/stuck.jsonl", "sess6", age_secs=SWEEP_RETRY_SLA + 30)
    acted = await reap_stuck_sweeps(_ctx(actions))
    assert acted == 1
    assert retried == ["/t/stuck.jsonl"]


async def test_reap_stuck_sweeps_ignores_a_row_still_inside_its_sla(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Below SWEEP_RETRY_SLA a healthy attempt is probably just still running — nagging it
    this early would be the watchdog racing a normal in-flight sweep."""
    import src.workers.arq_worker as worker

    calls: list[str] = []
    monkeypatch.setattr(worker, "sweep_session",
                        lambda ctx, t: calls.append(t))  # type: ignore[arg-type]
    await _enqueue_row(actions, "/t/fresh.jsonl", "sess7", age_secs=5)
    acted = await reap_stuck_sweeps(_ctx(actions))
    assert acted == 0
    assert calls == []


async def test_reap_stuck_sweeps_escalates_past_the_ceiling_instead_of_retrying_forever(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A genuinely poisoned transcript must not re-enqueue forever (Thoth's explicit ask,
    DM 1326 refinement #4) — past SWEEP_RETRY_CEILING, stop and confess loudly instead."""
    import src.workers.arq_worker as worker

    retried: list[str] = []

    async def _fake_sweep(ctx: dict[str, Any], transcript: str) -> int:
        retried.append(transcript)  # must NEVER be called for a poisoned row
        return 0

    monkeypatch.setattr(worker, "sweep_session", _fake_sweep)
    rid = await _enqueue_row(
        actions, "/t/poison.jsonl", "sess8", age_secs=SWEEP_RETRY_CEILING + 60)
    acted = await reap_stuck_sweeps(_ctx(actions))
    assert acted == 1
    assert retried == [], "a poisoned row must not be retried, only escalated"
    assert await actions.pool.fetchval(
        "SELECT completed_at FROM sweep_ledger WHERE id = $1", rid) is not None
    thread = await actions.pool.fetchrow(
        "SELECT a.value #>> '{}' AS summary FROM current_assertions a "
        "JOIN objects o ON o.id = a.object_id "
        "WHERE o.type = 'Thread' AND a.name = 'summary' "
        "AND a.value #>> '{}' ILIKE '%POISON SWEEP%'")
    assert thread is not None and "poison.jsonl" in thread["summary"]


async def test_reap_stuck_sweeps_is_scheduled() -> None:
    from src.workers.arq_worker import WorkerSettings

    crons = {c.coroutine.__name__ for c in WorkerSettings.cron_jobs}
    assert "reap_stuck_sweeps" in crons


async def test_orient_confesses_an_unconfirmed_sweep_past_sla(
    actions: Actions, tmp_path: Any,
) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    proj = await actions.create_or_find_object("SoftwareProject", "repo:sweeptest", "test")
    await actions.assert_property(proj, "name", "sweeptest", "test",
                                  datetime(2026, 7, 26, tzinfo=UTC), 0.9,
                                  evidence_class="self_declared")
    await _enqueue_row(actions, "/t/mine.jsonl", "sessA", age_secs=SWEEP_RETRY_SLA + 30)
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:sweeptest-i", session="sessA", project="sweeptest",
        model=None, cwd=None)
    try:
        out = await srv.orient(ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert "sweep_unconfirmed" in out
    assert "mining sweep" in out["sweep_unconfirmed"]


async def test_orient_stays_silent_once_the_sweep_completes(
    actions: Actions,
) -> None:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    proj = await actions.create_or_find_object("SoftwareProject", "repo:sweeptest2", "test")
    await actions.assert_property(proj, "name", "sweeptest2", "test",
                                  datetime(2026, 7, 26, tzinfo=UTC), 0.9,
                                  evidence_class="self_declared")
    await _enqueue_row(actions, "/t/mine2.jsonl", "sessB", age_secs=SWEEP_RETRY_SLA + 30)
    await _mark_ledger_done(actions.pool, "/t/mine2.jsonl")
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:sweeptest2-i", session="sessB", project="sweeptest2",
        model=None, cwd=None)
    try:
        out = await srv.orient(ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert "sweep_unconfirmed" not in out
