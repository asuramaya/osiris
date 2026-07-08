"""The whisper's server half — automount: every session wakes up already mounted.

Drives the same tested mount path the tool uses, so these tests focus on what the whisper
ADDS: the derived job_dir anchor (durable + resolved, visible to the liveness probe), the
payload the hook prints (mail/desk/away), and idempotence on hook re-fire.
"""
from __future__ import annotations

import json
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.handshake import automount
from src.orchestrator.mailbox import send_message

SID = "39fb22a2-0000-4000-8000-000000000000"


def _transcript(root: Path, cwd: str, model: str = "claude-fable-5") -> None:
    proj = root / cwd.replace("/", "-")
    proj.mkdir(parents=True, exist_ok=True)
    (proj / f"{SID}.jsonl").write_text(json.dumps(
        {"type": "assistant", "cwd": cwd,
         "message": {"model": model, "content": [{"type": "text", "text": "hi"}]}}) + "\n")


async def test_automount_is_a_durable_anchored_mount(actions: Actions, tmp_path: Path) -> None:
    root = tmp_path / "projects"
    _transcript(root, "/w/rotten-apple")
    await send_message(actions.pool, from_agent="agent:x", from_project="osiris",
                       to_project="rotten-apple", body="mail waiting at birth")

    out = await automount(actions, session_id=SID, cwd="/w/rotten-apple",
                          actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs")

    assert out["agent"] == "agent:39fb22a2"       # anchored on the derived job id
    assert out["resolved"] is True                # never the cwd-guess
    assert out["model"] == "claude-fable-5"       # read off its OWN transcript
    assert out["mail"] == 1                       # the whisper says so at birth
    # the DURABLE half: registered in agent_mounts → the liveness probe sees the tab,
    # mail takes the deliver lane, a bounce re-attaches
    row = await actions.pool.fetchrow(
        "SELECT agent_id, project FROM agent_mounts WHERE agent_id='agent:39fb22a2'")
    assert row is not None and row["project"] == "rotten-apple"
    # hook re-fire (session resume) is idempotent — same identity, no dup Agent
    again = await automount(actions, session_id=SID, cwd="/w/rotten-apple",
                            actor="analyst:operator", root=root, jobs_home=tmp_path / "jobs")
    assert again["agent"] == out["agent"]
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' AND canonical='agent:39fb22a2'") == 1


async def test_automount_survives_a_sessionless_stranger(actions: Actions,
                                                         tmp_path: Path) -> None:
    # no transcript, junk session id → still a valid (unresolved) mount, never a crash:
    # the whisper is fail-open end to end
    out = await automount(actions, session_id="x", cwd="/w/mystery",
                          actor="analyst:operator", root=tmp_path / "empty",
                          jobs_home=tmp_path / "jobs")
    assert out["agent"].startswith("agent:") and out["resolved"] is False
    assert out["mail"] == 0 and "desk" in out
