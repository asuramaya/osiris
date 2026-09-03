"""THE --json PROMISE, ENFORCED BY RUNNING THE CLI, NOT BY READING IT (Thoth dispatch
6746, operator's own words: "not a documentation problem... about size and scope").

test_cli_mcp_parity.py's own gate is a pair of STATIC metadata comparisons — CLI argparse
structure vs MCP inputSchema, names and param sets only. It never invokes either surface:
no exit code, no --help text, no --json output is ever parsed by anything in that suite.
That is the whole, precise reason two live specimens (found by running `osiris --help`
and every subcommand, not by reading the code) reached the operator through a 4,331-test
green suite:

  SPECIMEN A: the top-level help's own promise — "Every read verb takes --json" — is
  FALSE for two of the four verbs in its own displayed "see the fleet" category
  (boot-status, smoke). Not a regression: this was never true, so no diff anyone could
  have reviewed would ever have caught it.

  SPECIMEN B: `osiris unmerge --json` silently dropped to a bare stderr print on its own
  refusal path (fixed alongside this file, cli.py's `cmd_unmerge`) — the ONE command of
  the seven that both register `--json` AND used to short-circuit before ever reaching
  `render.emit`.

Two gates here, matching that shape exactly:

  (1) ARGPARSE-ONLY, ZERO EXECUTION (same live-walk discipline test_cli_mcp_parity.py
  already trusts): every verb this codebase's own top-level help calls "read the record"
  or "see the fleet" must register `--json` on its own subparser. Would have caught
  specimen A immediately, costs nothing to run.

  (2) THE FIRST TEST IN THIS SUITE THAT ACTUALLY EXECUTES A CLI COMMAND AND READS STDOUT:
  every command that registers `--json` gets run once, through its own edge (a refusal
  for a write-shaped verb, a mocked minimal payload for a pure-read one — the SAME
  edge/mock shapes tests/test_cli.py already uses for these functions, never a live
  daemon, never a real write), and its stdout must round-trip through `json.loads()`.
  Would have caught specimen B immediately.
"""
from __future__ import annotations

import argparse
import json
from typing import Any

from src.actions.core import Actions
from src.cli import (
    _build_parser,
    cmd_boot_status,
    cmd_desk,
    cmd_fleet,
    cmd_retention,
    cmd_roster,
    cmd_show,
    cmd_smoke,
    cmd_stop,
    cmd_unmerge,
)

# The CLI's own top-level help groups these six under "read the record" / "see the
# fleet" and claims, in the same breath: "Every read verb takes --json." Declared
# explicitly here (never inferred by parsing the prose, which is fragile) so a rename or
# a removal fails a staleness check below rather than silently going untested.
READ_VERBS = frozenset({"fleet", "roster", "boot-status", "smoke", "desk", "show"})

# Every subcommand that actually registers `--json` today (cross-checked live below,
# never hand-trusted) — the population gate (2) exercises one edge invocation each for.
JSON_COMMANDS = frozenset({
    "stop", "fleet", "roster", "desk", "show", "unmerge", "retention",
    "boot-status", "smoke",
})


def _subparsers() -> dict[str, argparse.ArgumentParser]:
    parser = _build_parser()
    sub_action = next(
        a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
    return dict(sub_action.choices)


def _registers_json(subparser: argparse.ArgumentParser) -> bool:
    return "as_json" in {act.dest for act in subparser._actions}


# --- GATE 1: argparse-only, zero execution ------------------------------------------------

def test_read_verbs_still_name_real_commands() -> None:
    """Same discipline as test_cli_mcp_parity's own allowlist-staleness checks: a renamed
    or removed read verb must fail here, not sit unread in a frozenset nobody re-checks."""
    subparsers = _subparsers()
    for name in READ_VERBS:
        assert name in subparsers, (
            f"{name!r} is declared a read verb in READ_VERBS but no such CLI command "
            "exists any more — update this set")


def test_every_declared_read_verb_registers_json() -> None:
    """THE GATE THAT WOULD HAVE CAUGHT SPECIMEN A: the CLI's own help says every read
    verb takes --json. Prove it, live, off the same argparse structure the parity gate
    already trusts — never by reading the help text."""
    subparsers = _subparsers()
    missing = sorted(
        name for name in READ_VERBS if not _registers_json(subparsers[name]))
    assert missing == [], (
        "the CLI's own top-level help promises \"Every read verb takes --json\" but "
        f"these declared read verbs register no such flag: {missing} — either add "
        "--json to each, or narrow the promise in _TOP_LEVEL_HELP to match reality "
        "(Thoth dispatch 6746, specimen A: a promise that was never true, not a "
        "regression)")


def test_the_gate_itself_catches_a_read_verb_with_no_json(monkeypatch: Any) -> None:
    """PROVE THE MECHANISM before trusting it against the real parser — the same
    discipline test_cli_mcp_parity.py's own proof tests hold to."""
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command")
    has_json = sub.add_parser("has-json")
    has_json.add_argument("--json", action="store_true", dest="as_json")
    no_json = sub.add_parser("no-json")
    no_json.add_argument("--full", action="store_true")

    assert _registers_json(has_json) is True
    assert _registers_json(no_json) is False


# --- GATE 2: execution, one edge per --json command -----------------------------------

def test_json_commands_set_matches_the_parser_live() -> None:
    """JSON_COMMANDS is a declared population, not a hand-trusted guess — this proves it
    against the live parser exactly the way test_cli_mcp_parity's own allowlist checks
    prove theirs: every subcommand that registers --json is in the set, and nothing in
    the set has stopped registering it."""
    subparsers = _subparsers()
    live = frozenset(name for name, sp in subparsers.items() if _registers_json(sp))
    assert live == JSON_COMMANDS, (
        f"JSON_COMMANDS has drifted from the live parser — live has {sorted(live)}, "
        f"declared has {sorted(JSON_COMMANDS)}. Update JSON_COMMANDS and add/remove the "
        "matching edge test below.")


async def test_cmd_stop_refusal_emits_json(actions: Actions) -> None:
    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_stop("nonexistent-seat-xyz", as_json=True, pool=actions.pool)
    assert out == 1
    printed = json.loads(buf.getvalue())
    assert printed["status"] == "refused-no-seat"


async def test_cmd_unmerge_refusal_emits_json(actions: Actions) -> None:
    """Specimen B itself, re-proven here as part of the population gate — the dedicated
    regression test lives in test_cli.py (test_cmd_unmerge_refusal_still_emits_json),
    watched fail before cli.py's fix and pass after; this is the enumeration's own copy,
    proving the population as a whole rather than one command in isolation."""
    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_unmerge("nonexistent-repo-xyz", "reconsidered", actor="operator",
                                pool=actions.pool, as_json=True)
    assert out == 1
    printed = json.loads(buf.getvalue())
    assert "error" in printed


async def test_cmd_retention_dry_run_emits_json(actions: Actions) -> None:
    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_retention("outbox", days=30, execute=False, pool=actions.pool,
                                  as_json=True)
    assert out == 0
    printed = json.loads(buf.getvalue())
    assert printed["table"] == "outbox"


async def test_cmd_show_refusal_emits_json(monkeypatch: Any) -> None:
    import io
    from contextlib import redirect_stdout

    async def _no_match(url: str, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        return {"error": "no thread/decision matches 'nope' — recall never guesses"}

    monkeypatch.setattr("src.orchestrator.mcp_client.call_mcp_tool", _no_match)
    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_show("nope", as_json=True)
    assert out == 1
    printed = json.loads(buf.getvalue())
    assert "error" in printed


async def test_cmd_fleet_emits_json(monkeypatch: Any) -> None:
    import io
    from contextlib import redirect_stdout

    async def _fake_call(url: str, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        return {"nodes": {}, "tree": "fleet: empty"}

    monkeypatch.setattr("src.orchestrator.mcp_client.call_mcp_tool", _fake_call)
    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_fleet(full=False, as_json=True)
    assert out == 0
    printed = json.loads(buf.getvalue())
    assert "nodes" in printed


async def test_cmd_roster_emits_json(monkeypatch: Any) -> None:
    import io
    from contextlib import redirect_stdout

    async def _fake_call(url: str, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        return {"seats": []}

    monkeypatch.setattr("src.orchestrator.mcp_client.call_mcp_tool", _fake_call)
    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_roster(repo=None, as_json=True)
    assert out == 0
    printed = json.loads(buf.getvalue())
    assert "seats" in printed


async def test_cmd_boot_status_emits_json(actions: Actions) -> None:
    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_boot_status(pool=actions.pool, as_json=True)
    assert out == 0  # a fresh test DB carries no active seats at all — no gaps
    printed = json.loads(buf.getvalue())
    assert printed == {"gaps": []}


async def test_cmd_smoke_emits_json(monkeypatch: Any) -> None:
    import io
    from contextlib import redirect_stdout

    async def _fake_probes() -> tuple[list[str], list[str]]:
        return [], ["a non-blocking warning"]

    monkeypatch.setattr("src.cli._run_smoke_probes_full", _fake_probes)
    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_smoke(as_json=True)
    assert out == 0
    printed = json.loads(buf.getvalue())
    assert printed == {"fails": [], "warnings": ["a non-blocking warning"]}


async def test_cmd_desk_emits_json(monkeypatch: Any) -> None:
    import io
    from contextlib import redirect_stdout

    async def _fake_call(url: str, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        return {"owed": 0, "letters": 0, "needs_decision": [], "needs_hands": [], "fyi": []}

    monkeypatch.setattr("src.orchestrator.mcp_client.call_mcp_tool", _fake_call)
    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_desk(as_json=True)
    assert out == 0
    printed = json.loads(buf.getvalue())
    assert printed["owed"] == 0
