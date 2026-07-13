"""THE ANCHOR ON EVERY CALL — the most-reported bug in the fleet, and it was never broken.

FOUR INDEPENDENT SIGHTINGS IN ONE NIGHT (2026-07-13): Khepri III of tony (msg 420), the code seat
(msg 417), the xxit seat, and Thoth XXVIII four times — once WHILE READING THE MAIL REPORTING IT.
Every one of us called it "transient", because the bounce gave us no way to know otherwise.

    "mount() bounced TWICE across this lineage's life with zero detail — just 'mount first'.
     When it happens between two calls in the same turn it looks identical to a real state loss."
                                                                              — Khepri III

THE RE-ATTACH MACHINERY ALWAYS EXISTED. It was STARVED, not broken: it keys off the X-Osiris-Job
header, which .mcp.json fills from ${CLAUDE_JOB_DIR} — AND THAT IS EMPTY IN EVERY INTERACTIVE
SESSION. So after an MCP reconnect the server had nothing to re-attach BY, and the call bounced —
or, far worse, wrote ANONYMOUSLY into the `session` bucket. For a graph whose entire value is
provenance, an anonymous write is the worst failure it has.

And the fix was sitting in the hook the whole time: the PreToolUse hook holds the harness's own
session_id on EVERY osiris call, derives the durable job_dir from it — and then handed it over
only for mount().
"""

from __future__ import annotations

import inspect
import json
import subprocess
import sys
from pathlib import Path

_HOOK = Path(__file__).resolve().parent.parent / "scripts" / "osiris_mount_anchor.py"


def _run_hook(payload: dict[str, object]) -> dict[str, object]:
    out = subprocess.run([sys.executable, str(_HOOK)], input=json.dumps(payload),
                         capture_output=True, text=True, check=False)
    return dict(json.loads(out.stdout)) if out.stdout.strip() else {}


def test_the_hook_and_the_SERVER_stay_in_LOCKSTEP() -> None:
    """THE TRIPWIRE. ANCHOR_AWARE names the tools whose signature accepts `session_anchor`. A name
    in that set whose tool does NOT take the param makes every call fail schema validation — a
    LOUDER, WORSE bug than the one it fixes. So the set is checked against the real signatures.

    (This is the same class as SPAWN_AWARE, which has always carried a hand-maintained list and a
    comment begging the next reader to keep it in sync. A comment is not a guard.)
    """
    import src.mcp_server as srv
    from scripts.osiris_mount_anchor import ANCHOR_AWARE

    for name in sorted(ANCHOR_AWARE):
        fn = getattr(srv, name.removeprefix("mcp__osiris__"), None)
        assert fn is not None, f"{name} is stamped by the hook but does not exist on the server"
        params = inspect.signature(fn).parameters
        assert "session_anchor" in params, (
            f"{name} is in ANCHOR_AWARE but its signature does not accept `session_anchor` — "
            "every call to it would fail schema validation")


def test_the_anchor_rides_EVERY_call_not_only_mount() -> None:
    """The bug, in one assertion. inbox() is where I was bounced tonight; send() is where Khepri
    was; orient() is where the code seat was. All three now carry the re-attach hint."""
    for tool in ("inbox", "send", "orient", "record_decision", "open_thread", "resolve_thread"):
        out = _run_hook({"tool_name": f"mcp__osiris__{tool}",
                         "session_id": "513aa520-6f1e-4807-948d-2e0820af1574",
                         "tool_input": {}})
        anchor = out["hookSpecificOutput"]["updatedInput"]["session_anchor"]  # type: ignore[index]
        assert str(anchor).endswith("/jobs/513aa520"), f"{tool} rode without its anchor"


def test_a_SPAWN_never_gets_a_seat_anchor() -> None:
    """A child shares its parent's connection and job dir. If the anchor rode on a spawn's calls,
    the child would re-attach AS THE SEAT — full authority, which is exactly the masquerade the
    spawn stamp exists to prevent (live repro, 2026-07-10)."""
    out = _run_hook({"tool_name": "mcp__osiris__open_thread",
                     "session_id": "513aa520-6f1e-4807-948d-2e0820af1574",
                     "agent_id": "agent-abc", "agent_type": "Explore",
                     "tool_input": {}})
    ti = out["hookSpecificOutput"]["updatedInput"]  # type: ignore[index]
    assert "session_anchor" not in ti, "a spawn re-attached as its parent's seat"
    assert ti["subagent_id"] == "agent-abc"  # type: ignore[index]


def test_an_agent_s_OWN_anchor_is_never_overwritten() -> None:
    """A mind deliberately wearing a seat (a claim, a resume) supplied its anchor on purpose. The
    hook only ever FILLS a missing one — it does not overrule a mind that knows who it is."""
    out = _run_hook({"tool_name": "mcp__osiris__orient",
                     "session_id": "513aa520-6f1e-4807-948d-2e0820af1574",
                     "tool_input": {"session_anchor": "/home/x/.claude/jobs/deadbeef"}})
    if out:  # unchanged input → the hook may emit nothing at all
        ti = out["hookSpecificOutput"]["updatedInput"]  # type: ignore[index]
        assert ti["session_anchor"] == "/home/x/.claude/jobs/deadbeef"  # type: ignore[index]


def test_a_non_osiris_tool_is_never_touched() -> None:
    """The hook fires on every PreToolUse. It must be invisible to everything that is not ours."""
    assert _run_hook({"tool_name": "Bash", "session_id": "513aa520-aaaa",
                      "tool_input": {"command": "ls"}}) == {}
