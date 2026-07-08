"""Durable mounts — identity that survives a server bounce (decision 56f6a0d6).

The in-memory registry dies with the process and the process dies routinely (deploys, an
OOM-kill); every bounce used to wipe the WHOLE fleet's identities at once. These tests drive
the durable half (agent_mounts) and the re-attach path: a call that misses the hot dict
recovers its identity from the table by the client's job_dir header — transparently, with a
FRESH transcript read (never a stale copy of the stored model).
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from src.actions.core import Actions
from src.orchestrator import mounts
from src.orchestrator.agents import resolve_identity


async def test_save_find_upsert(actions: Actions) -> None:
    p = actions.pool
    await mounts.save_mount(p, job_dir="/x/jobs/aaaa1111", agent_id="agent:aaaa1111",
                            project="osiris", cwd="/repo/osiris", model="claude-fable-5",
                            session_key="sid:one")
    rec = await mounts.find_mount(p, job_dir="/x/jobs/aaaa1111")
    assert rec is not None
    assert rec.agent_id == "agent:aaaa1111" and rec.project == "osiris"
    assert await mounts.find_mount(p, job_dir="/never/mounted") is None
    # upsert: a re-mount moves the row, it never duplicates it
    await mounts.save_mount(p, job_dir="/x/jobs/aaaa1111", agent_id="agent:aaaa1111",
                            project="osiris", cwd="/repo/osiris", model="claude-opus-4-8",
                            session_key="sid:two")
    rec2 = await mounts.find_mount(p, job_dir="/x/jobs/aaaa1111")
    assert rec2 is not None and rec2.model == "claude-opus-4-8"
    assert await p.fetchval("SELECT count(*) FROM agent_mounts") == 1


async def test_project_last_seen_feeds_the_listener_probe(actions: Actions) -> None:
    p = actions.pool
    assert await mounts.project_last_seen(p, "ghost-town") is None
    await mounts.save_mount(p, job_dir="/x/jobs/bbbb2222", agent_id="agent:bbbb2222",
                            project="lively", cwd="/repo/lively", model=None, session_key=None)
    seen = await mounts.project_last_seen(p, "lively")
    assert seen is not None  # ISO stamp — send() turns this into listener.live


async def test_reattach_recovers_identity_after_a_bounce(
    actions: Actions, tmp_path: Path
) -> None:
    """The whole point: hot dict empty (the bounce), header present → the identity comes back
    without the agent doing anything, and the session key is re-bound."""
    from src import mcp_server as srv

    job_dir = str(tmp_path / "jobs" / "tst00001")  # …/jobs/<id> — the anchor _job_id parses
    expected = resolve_identity(cwd=str(tmp_path / "demo"), job_dir=job_dir)
    await mounts.save_mount(actions.pool, job_dir=job_dir, agent_id=expected.agent_id,
                            project=expected.project, cwd=str(tmp_path / "demo"),
                            model=None, session_key="sid:old")
    srv._agents.pop("sid:fresh", None)  # the bounce: nothing hot
    ident = await srv._reattach(actions.pool, "sid:fresh", job_dir)
    assert ident is not None
    assert ident.agent_id == expected.agent_id == "agent:tst00001"  # same actor, not a fork
    assert srv._agents.get("sid:fresh") is ident                    # re-cached hot
    rec = await mounts.find_mount(actions.pool, job_dir=job_dir)
    assert rec is not None and rec.agent_id == expected.agent_id
    srv._agents.pop("sid:fresh", None)  # leave no global residue for other tests


async def test_reattach_without_a_hint_stays_none(actions: Actions) -> None:
    from src import mcp_server as srv

    assert await srv._reattach(actions.pool, "sid:x", None) is None
    assert await srv._reattach(actions.pool, "sid:x", "/never/mounted/jobs/zzzz9999") is None


def test_conn_key_and_job_hint_read_the_headers() -> None:
    from src import mcp_server as srv

    def _ctx(headers: dict[str, str]) -> SimpleNamespace:
        req = SimpleNamespace(headers=headers)
        return SimpleNamespace(request_context=SimpleNamespace(request=req, session=object()))

    ctx = _ctx({"mcp-session-id": "abc123", "x-osiris-job": "/h/.claude/jobs/ad1a1cb0"})
    assert srv._conn_key(ctx) == "sid:abc123"          # the protocol session id wins
    assert srv._job_hint(ctx) == "/h/.claude/jobs/ad1a1cb0"
    # no session header → the object-id fallback, PREFIXED (keyspaces can't collide)
    key = srv._conn_key(_ctx({}))
    assert key is not None and key.startswith("obj:")
    # an unexpanded client variable is no hint at all
    assert srv._job_hint(_ctx({"x-osiris-job": "${CLAUDE_JOB_DIR}"})) is None
    assert srv._job_hint(_ctx({"x-osiris-job": ""})) is None
    assert srv._conn_key(None) is None and srv._job_hint(None) is None


def test_prune_agents_drops_the_least_recently_used() -> None:
    from src import mcp_server as srv

    saved, saved_touch = dict(srv._agents), dict(srv._agents_touched)
    try:
        srv._agents.clear()
        srv._agents_touched.clear()
        for i in range(10):
            srv._agents[f"sid:{i}"] = SimpleNamespace(agent_id=f"agent:{i}")  # type: ignore
            srv._agents_touched[f"sid:{i}"] = float(i)
        srv._prune_agents(cap=8)  # over cap → drop down to cap//2, oldest first
        assert len(srv._agents) == 4
        assert set(srv._agents) == {"sid:6", "sid:7", "sid:8", "sid:9"}
        srv._prune_agents(cap=8)  # under cap → untouched
        assert len(srv._agents) == 4
    finally:
        srv._agents.clear()
        srv._agents.update(saved)
        srv._agents_touched.clear()
        srv._agents_touched.update(saved_touch)


async def test_save_mount_returns_the_previous_last_seen(actions: Actions) -> None:
    """The while-you-were-away anchor: first mount has no past (None); a re-mount returns the
    lineage's prior sign of life."""
    p = actions.pool
    prev = await mounts.save_mount(p, job_dir="/x/jobs/cccc3333", agent_id="agent:cccc3333",
                                   project="demo", cwd="/repo/demo", model=None,
                                   session_key=None)
    assert prev is None                       # first mount — no past
    prev2 = await mounts.save_mount(p, job_dir="/x/jobs/cccc3333", agent_id="agent:cccc3333",
                                    project="demo", cwd="/repo/demo", model=None,
                                    session_key=None)
    assert prev2 is not None                  # the re-entry sees the prior last_seen


async def test_while_away_names_the_face_wearers(actions: Actions) -> None:
    """A returning agent is told WHO acted in its project's name and how its threads moved —
    'mail 0' must never silently mean 'a stranger settled your conversations'."""
    from datetime import timedelta

    from src.orchestrator.mailbox import read_inbox, send_message

    p = actions.pool
    anchor = datetime.now(UTC) - timedelta(hours=8)
    # while the owner slept: a twin was woken (resume lane), SENT mail wearing the project's
    # face, and the counterparty's ask got leased+settled
    await p.execute("INSERT INTO agent_wakes (to_project, from_agent, message_id, mode) "
                    "VALUES ('heinrich','agent:deceptor',NULL,'resume')")
    ask = await send_message(p, from_agent="agent:deceptor", from_project="decepticons",
                             to_project="heinrich", body="image the 50k pair?")
    await read_inbox(p, "heinrich")  # the twin leased it…
    await send_message(p, from_agent="agent:twin-99", from_project="heinrich",
                       body="done — imaged, verdict recorded", reply_to=ask["id"])  # …and settled

    away = await mounts.while_away(p, "heinrich", "agent:a8c15486", anchor)

    assert away is not None
    assert away["acted_in_your_name"] == ["agent:twin-99"]      # the face-wearer, named
    assert away["wakes"] == {"resume": 1}
    threads = {t["thread"]: t for t in away["threads"]}
    # the thread's LAST WORD is the twin's reply, sent wearing your face; the counterparty
    # hasn't read it yet — settled=False is the honest state ("answered for you, their side
    # pending"), and last_from names the hand that did it
    assert threads[ask["id"]]["last_from"] == "agent:twin-99"
    assert threads[ask["id"]]["between"] == "heinrich → decepticons"
    assert threads[ask["id"]]["settled"] is False
    # the owner itself is never listed as its own face-wearer
    away2 = await mounts.while_away(p, "heinrich", "agent:twin-99", anchor)
    assert away2 is not None and away2["acted_in_your_name"] == []


async def test_while_away_is_quiet_when_nothing_happened(actions: Actions) -> None:
    away = await mounts.while_away(actions.pool, "ghost-town", "agent:x",
                                   datetime.now(UTC))
    assert away is None
    assert await mounts.while_away(actions.pool, "ghost-town", "agent:x", None) is None
