"""A refused launch names the command that fixes workspace trust."""
from __future__ import annotations

from pathlib import Path

from src.actions.core import Actions


async def test_a_refused_launch_names_the_command_that_fixes_trust(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator import trigger as trigger_module

    from tests.test_trigger import _fake_agents_json, _fake_spawn, _managed_pair, _office

    seat, _m = await _managed_pair(
        actions, worker_agent="agent:hw7r05", manager_agent="agent:hm7r05",
        worker_handle="Trustfix", house="osiris")
    await _office(actions, seat, "/tmp/trustfix-test")
    d = await trigger_module.launch_seat(
        actions, caller="agent:hm7r05", target=seat,
        spawn=_fake_spawn([], raises=OSError(
            "claude --bg exited 1 in /x: Workspace not trusted. Run `claude` in /x once "
            "and accept the trust prompt, then retry.")),
        agents_json=_fake_agents_json([[]]))
    assert d["status"] == "refused-spawn"
    assert "Workspace not trusted" in d["detail"]
    assert "run `claude` once in " in d["detail"] and "accept the trust prompt" in d["detail"]
