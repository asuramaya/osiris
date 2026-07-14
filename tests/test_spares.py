"""THE SPARE — a process that is not anybody, wearing a real session id.

Claude Code fires SessionStart for things no human ever addresses: `claude bg-spare` pre-warms,
pty hosts, claim-socket daemons. Each has a genuine session_id and a genuine cwd, so the whisper
seats it — and `last_seen=now()` handed it a HEARTBEAT, which made it LIVE by every test the fleet
owns. It inflated the roster, it made the co-agent collision warning cry wolf on an uncontended
tree (Anubis XII obeyed it and declined to stage; the tree was empty), and it could take delivery
of a DM into a process that will never read anything.

    A HEARTBEAT MUST BE EARNED BY AN ACT, NEVER GRANTED BY A GREETING.

And NOT by `model IS NULL`, which is the fix the field report asked for and which would have been
wrong: of the two seats reported as ghosts, one carried a RESOLVED MODEL and a LIVE HEARTBEAT and
had NO TRANSCRIPT ON DISK AT ALL. `model IS NULL` does not mean "not anybody" — it means WE HAVE
NOT LOOKED YET. This project has been bitten by that exact shape three times.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.liveness import observe_liveness
from src.orchestrator.mounts import save_mount


async def _live(actions: Actions, agent: str) -> bool:
    return bool(await actions.pool.fetchval(
        "SELECT 1 FROM agent_mounts WHERE agent_id=$1 "
        "AND last_seen > now() - interval '15 minutes'", agent))


async def _mount(actions: Actions, sid: str, agent: str, *, alive: bool) -> None:
    await save_mount(actions.pool, job_dir=f"/home/x/.claude/jobs/{sid}", agent_id=agent,
                     project="repo", cwd="/home/x/repo", model="claude-opus-4-8",
                     session_key=f"whisper:{sid}", alive=alive)


async def test_a_GREETING_does_not_make_you_alive(actions: Actions) -> None:
    """The whisper seats you. It does not certify you as a mind. A spare stops here forever."""
    await _mount(actions, "aaaaaaaa", "agent:aaaaaaaa", alive=False)
    assert not await _live(actions, "agent:aaaaaaaa")


async def test_an_ACT_does(actions: Actions) -> None:
    """A real Osiris call — the thing a spare never makes."""
    await _mount(actions, "bbbbbbbb", "agent:bbbbbbbb", alive=False)
    assert not await _live(actions, "agent:bbbbbbbb")
    await _mount(actions, "bbbbbbbb", "agent:bbbbbbbb", alive=True)
    assert await _live(actions, "agent:bbbbbbbb")


async def test_a_PROVISIONAL_seat_is_PROMOTED_when_its_transcript_moves(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE CLAUSE THE WHOLE DESIGN HANGS ON.

    observe_liveness used `v.moved > m.last_seen`, and `x > NULL` is NULL — which is not TRUE. So
    a provisionally-seated agent could work all day, writing to its transcript the whole time, and
    the fleet would read it as DEAD FOREVER. Taking the heartbeat away is only safe if the way
    back is real.
    """
    await _mount(actions, "cccccccc", "agent:cccccccc", alive=False)
    assert not await _live(actions, "agent:cccccccc")

    d = tmp_path / "-repo"
    d.mkdir(parents=True)
    (d / "cccccccc-0000.jsonl").write_text('{"type":"user","message":{"content":"hi"}}\n')

    assert await observe_liveness(actions.pool, tmp_path) == 1
    assert await _live(actions, "agent:cccccccc"), "a working mind was left for dead"


async def test_a_SPARE_never_writes_and_so_is_never_promoted(
    actions: Actions, tmp_path: Path,
) -> None:
    """The other half, and the point of the whole thing: the sweep is FREE and it is EXACT.
    We do not guess which process is a person — we let the process show us, and a spare has
    nothing to show."""
    await _mount(actions, "dddddddd", "agent:dddddddd", alive=False)
    (tmp_path / "-repo").mkdir(parents=True)          # a project dir, but no transcript for it

    await observe_liveness(actions.pool, tmp_path)
    assert not await _live(actions, "agent:dddddddd")


async def test_a_greeting_never_REVOKES_a_pulse_it_did_not_grant(actions: Actions) -> None:
    """A living agent that gets re-whispered (a reconnect, a re-fire of SessionStart) must keep
    the life it EARNED. A fix for over-counting that starts under-counting is not a fix — it is
    the liveness bug (456960e5) coming back through the other door."""
    await _mount(actions, "eeeeeeee", "agent:eeeeeeee", alive=True)
    assert await _live(actions, "agent:eeeeeeee")
    await _mount(actions, "eeeeeeee", "agent:eeeeeeee", alive=False)   # SessionStart re-fires
    assert await _live(actions, "agent:eeeeeeee"), "the greeting revoked a heartbeat it never gave"


async def test_liveness_only_ever_ADDS_life(actions: Actions, tmp_path: Path) -> None:
    """A stale transcript must never drag a freshly-active agent backwards."""
    await _mount(actions, "ffffffff", "agent:ffffffff", alive=True)
    d = tmp_path / "-repo"
    d.mkdir(parents=True)
    p = d / "ffffffff-0000.jsonl"
    p.write_text("{}\n")
    import os
    old = (datetime.now(UTC) - timedelta(days=2)).timestamp()
    os.utime(p, (old, old))

    await observe_liveness(actions.pool, tmp_path)
    assert await _live(actions, "agent:ffffffff"), "an old file buried a live agent"
