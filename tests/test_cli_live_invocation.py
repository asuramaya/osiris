"""THE LIVE-INVOCATION CHECK (Thoth mail 9382 item 2, f0a64374): "a test that runs
--help and one no-write invocation of every CLI command... so a broken door is caught
by the gate not the operator." Same live-walk discipline test_cli_mcp_parity.py and
test_cli_json_promise.py already hold — never by reading the code, by actually calling
it.

GATE 1 (--help, complete, all 63 subcommands): mechanical and fully safe — argparse
only, never touches a pool or a real process. Catches a broken subparser wiring
(a bad `add_argument`, a dest typo) that a purely-static check would miss.

GATE 2 (one real no-write invocation per command): NOT complete. Every entry in
NO_WRITE_INVOCATIONS below is either (a) an EXISTING refusal-shaped call already
proven safe by a passing test elsewhere in this suite (test_cli.py or test_cli_json_
promise.py — this file never reinvents those, it reuses the exact same args), or
(b) safe BY CONSTRUCTION via an explicit dry-run/apply/execute=False default in the
command's own signature, true regardless of what ref is passed. Roughly half the CLI
surface — mostly seat/project lifecycle commands with a single string ref + actor and
NO declared dry-run flag — has no such proof in this codebase today; calling any of
them for real, even with a deliberately-nonexistent ref, is UNVERIFIED (this house's
own convention is "resolve first, refuse honestly" everywhere it's been checked, but
"everywhere it's been checked" is not "everywhere," and three of them — mint-seat, new,
bootstrap — mint or spawn something for real with no confirmed refusal path at all).
NEEDS_SAFE_INVOCATION below names each one with a real reason, and the completeness
gate requires every live subcommand to be in ONE of the two dicts — so a name can
never silently fall through either as untested or as wrongly presumed safe.
"""
from __future__ import annotations

import argparse
import io
from contextlib import redirect_stderr, redirect_stdout
from typing import Any

import pytest
from src.actions.core import Actions
from src.cli import (
    _build_parser,
    cmd_amend_decision,
    cmd_amend_practice,
    cmd_annotate_thread,
    cmd_attach,
    cmd_backlog,
    cmd_boot_status,
    cmd_desk,
    cmd_fleet,
    cmd_fleet_prune,
    cmd_fleet_reconcile,
    cmd_heal_seat_anchor,
    cmd_heal_seat_transcript,
    cmd_inbox,
    cmd_launch,
    cmd_rebind_seat,
    cmd_rematerialize,
    cmd_rename_project,
    cmd_resume,
    cmd_retention,
    cmd_roster,
    cmd_search,
    cmd_send,
    cmd_show,
    cmd_smoke_chaos,
    cmd_status,
    cmd_stop,
    cmd_sweep_seat_disk,
    cmd_team,
    cmd_thread,
    cmd_threads,
    cmd_transition_seat_project,
    cmd_unmerge,
)


def _subparsers() -> dict[str, argparse.ArgumentParser]:
    parser = _build_parser()
    sub_action = next(
        a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
    return dict(sub_action.choices)


# --- GATE 1: --help for every subcommand, zero execution --------------------------------------

def test_every_subcommand_help_exits_clean_with_a_usage_line() -> None:
    """Runs `osiris <command> --help` for real, through the actual top-level parser —
    a broken subparser (a typo'd dest, a bad add_argument call) raises here even
    though it would never surface from a static structure check."""
    parser = _build_parser()
    broken: list[str] = []
    for name in sorted(_subparsers()):
        buf = io.StringIO()
        try:
            with redirect_stdout(buf), pytest.raises(SystemExit) as exc_info:
                parser.parse_args([name, "--help"])
        except Exception as e:  # noqa: BLE001 — a broken door, not an expected SystemExit
            broken.append(f"{name}: raised {e!r} instead of exiting cleanly")
            continue
        if exc_info.value.code != 0:
            broken.append(f"{name}: --help exited {exc_info.value.code}, not 0")
        elif "usage" not in buf.getvalue().lower():
            broken.append(f"{name}: --help produced no usage line")
    assert broken == [], "\n".join(broken)


# --- GATE 2: one real no-write invocation, where proven or structurally safe ------------------

async def _fake_manager_no_match(req: dict[str, Any]) -> dict[str, Any]:
    return {"sessions": [{"name": "[OS] thoth", "alive": True}]}


async def _unreachable(*a: Any, **k: Any) -> Any:
    raise AssertionError("should never be called — the ref lookup must refuse first")


async def _empty_agents_json(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
    return []


async def _fake_chaos_gate(pool: Any) -> dict[str, Any]:
    return {"ok": True, "findings": [], "storm_fired": 0, "recovery_elapsed_secs": 0.0,
            "automount_probes_total": 0}


# name -> async thunk(actions) -> int exit code. Every call here is EITHER an existing
# proven-safe refusal (copied verbatim from test_cli.py/test_cli_json_promise.py) or
# safe by an explicit dry-run/apply/execute=False default — see the module docstring.
NO_WRITE_INVOCATIONS: dict[str, Any] = {
    "attach": lambda a: cmd_attach("nobody-here", manager=_fake_manager_no_match),
    "status": lambda a: cmd_status(as_json=False),
    "fleet": lambda a: cmd_fleet(full=False, as_json=True),
    "roster": lambda a: cmd_roster(repo=None, as_json=False),
    "backlog": lambda a: cmd_backlog(all_projects=False, as_json=False),
    "threads": lambda a: cmd_threads(project="osiris", as_json=False),
    "team": lambda a: cmd_team(seat="No-Such-Manager-Anywhere", pool=a.pool),
    "inbox": lambda a: cmd_inbox(project="osiris", as_json=False),
    "desk": lambda a: cmd_desk(),
    "show": lambda a: cmd_show("no-such-ref-anywhere"),
    "boot-status": lambda a: cmd_boot_status(pool=a.pool),
    "search": lambda a: cmd_search("no-such-query-anywhere-xyz", limit=1),
    "launch": lambda a: cmd_launch(
        "no-such-handle-at-all", model=None, pool=a.pool, manager=_unreachable, debug=True),
    "resume": lambda a: cmd_resume(
        "no-such-handle-at-all", model=None, pool=a.pool,
        resume_spawn=_unreachable, agents_json=_empty_agents_json),
    "stop": lambda a: cmd_stop("nonexistent-seat-xyz", pool=a.pool),
    "unmerge": lambda a: cmd_unmerge(
        "nonexistent-repo-xyz", "reconsidered", actor="operator", pool=a.pool),
    "amend-practice": lambda a: cmd_amend_practice(
        "no such practice anywhere", "an amendment", actor="agent:liveinvoke1", pool=a.pool),
    "annotate-thread": lambda a: cmd_annotate_thread(
        "no such thread anywhere", "a note", actor="agent:liveinvoke2", pool=a.pool),
    "amend-decision": lambda a: cmd_amend_decision(
        "no such decision anywhere", "an addendum", actor="agent:liveinvoke3", pool=a.pool),
    "send": lambda a: cmd_send(
        "hello", to="no-such-project-ever", actor="agent:liveinvoke4", pool=a.pool),
    "thread": lambda a: cmd_thread(
        ["no such thread anywhere"], actor="agent:liveinvoke5", pool=a.pool),
    "rebind-seat": lambda a, tmp: cmd_rebind_seat(
        "no-such-seat-or-agent-anywhere", str(tmp), actor="operator", pool=a.pool),
    "rematerialize": lambda a, tmp: cmd_rematerialize(
        "neverseen0", dest=str(tmp / "x.jsonl"), pool=a.pool),
    # --- structurally safe: an explicit dry-run/apply/execute default, true regardless
    # --- of whether the ref resolves at all
    "sweep-seat-disk": lambda a: cmd_sweep_seat_disk(
        "no-such-handle", dry_run=True, pool=a.pool),
    "transition-seat-project": lambda a: cmd_transition_seat_project(
        "no-such-handle", apply=False, pool=a.pool),
    "heal-seat-anchor": lambda a: cmd_heal_seat_anchor(
        "no-such-handle", because="test", apply=False, actor="operator", pool=a.pool),
    "heal-seat-transcript": lambda a: cmd_heal_seat_transcript(
        "no-such-handle", ["/tmp/does-not-exist.jsonl"], apply=False, pool=a.pool),
    "fleet-reconcile": lambda a: cmd_fleet_reconcile(
        execute=False, actor="operator", pool=a.pool),
    "fleet-prune": lambda a: cmd_fleet_prune(execute=False, actor="operator", pool=a.pool),
    "rename-project": lambda a: cmd_rename_project(
        "no-such-project", "still-no-such-project", "test", dry_run=True,
        actor="operator", pool=a.pool),
    "retention": lambda a: cmd_retention(
        "not-a-real-table", days=30, execute=False, pool=a.pool),
}

# CLI commands the population gate below knows are NOT in NO_WRITE_INVOCATIONS, each
# with the real reason — never a silent gap. Most are "no declared dry-run flag, no
# existing test proves a bare nonexistent-ref call is genuinely write-free" — this
# house's own resolve-first-then-refuse convention is confirmed for MANY of these
# siblings above, but confirming it for each of these specifically needs reading that
# command's own body (or its wrapped orchestrator function's), not assumed from shape
# alone. mint-seat/new/bootstrap are flagged separately: they mint or spawn for real,
# with no confirmed refusal-only path found anywhere in this suite.
NEEDS_SAFE_INVOCATION: dict[str, str] = {
    "smoke": "needs a live osiris-mcp pool + real chrome routes per its own docstring "
             "(\"8 chrome routes + the live mcp pool\") — a different, heavier infra "
             "assumption than the plain MCP search/fleet calls above",
    "deploy": "11 injectable callables (git_status/restart/chaos gate/etc); every "
              "existing test replaces ALL of them with fakes — assembling that set here "
              "risks missing one and running a real restart/kill",
    "migrate": "check=True is the declared dry-run mode, but needs a real repo_root + "
               "alembic env; no existing test exercises check=True at all (every test "
               "injects a fake `state`/`run_migrations` instead) — same operator-only "
               "class as deploy",
    "seed": "genuinely writes (compositions/canon) even with compositions_only=True; "
            "no dry-run flag — already declared an operator devops bootstrap act "
            "elsewhere (NO_MCP_EQUIVALENT)",
    "merge": "no dry-run flag; no existing test calls it with a nonexistent dupe/into "
             "pair — resolve-first-then-refuse is this house's convention but unproven "
             "here specifically",
    "fold-project": "same underlying write as merge, same gap",
    "charter-for": "no existing nonexistent-seat-id test; own body not yet read to "
                   "confirm an early refusal",
    "correct-pin-value": "no existing nonexistent-handle test; writes a real .osiris "
                         "pin file on the filesystem if it doesn't refuse first",
    "correct-agent-house": "no existing nonexistent-handle test",
    "reconcile-merge": "no existing nonexistent-ref test",
    "retire-agent": "no existing nonexistent-ref test — retirement is hard to undo",
    "attach-seat": "no existing nonexistent-worker/manager test",
    "detach-seat": "no existing nonexistent-seat test",
    "promote": "no existing nonexistent-target test — mints a manager relationship",
    "vacate-seat": "no existing nonexistent-seat test",
    "retire-seat": "no existing nonexistent-seat test — retirement is hard to undo",
    "bind-seat-tree": "no existing nonexistent-seat test",
    "rename-seat": "no existing nonexistent-seat test",
    "set-seat-attended": "no existing nonexistent-seat test",
    "reissue-office": "no existing nonexistent-seat test — writes real office files",
    "establish-office": "no existing nonexistent-seat test — writes real office files",
    "resync-seat-house": "no existing nonexistent-seat test",
    "reconcile-seat-identity": "no existing nonexistent-seat test",
    "create-project": "no existing test — mints a real SoftwareProject",
    "set-project-tag": "no existing nonexistent-project test",
    "retire-project": "no existing nonexistent-project test — retirement is hard to undo",
    "fork-project": "no existing test — mints a real forked SoftwareProject",
    "decide": "grounds=[\"not-a-uuid\"] in the one existing near-miss test may trigger a "
              "citation-resolution refusal OR a partial write with a warning — not "
              "confirmed either way without reading that test's own asserts",
    "proposal": "no existing test at all; a malformed candidate JSON is a plausible "
                "safe invocation (hits the parse-error return before any write) but "
                "unconfirmed",
    "mint-seat": "genuinely mints a seat; no confirmed refusal-only path — every "
                 "existing test mints for real",
    "new": "genuinely spawns/mints a seat for real; no confirmed refusal-only path",
    "bootstrap": "genuinely writes for real in every existing test; no confirmed "
                 "refusal-only path",
}


def test_every_subcommand_is_covered_or_declares_why_not() -> None:
    """The population gate: nothing falls through either dict silently — a new
    subcommand must be added to one of them, and a renamed/removed one must be
    dropped from whichever it's in."""
    live = set(_subparsers())
    declared = set(NO_WRITE_INVOCATIONS) | set(NEEDS_SAFE_INVOCATION)
    missing = sorted(live - declared)
    stale = sorted(declared - live)
    assert missing == [], f"these subcommands have no coverage and no declared reason: {missing}"
    assert stale == [], f"these declared entries no longer name a real subcommand: {stale}"
    overlap = sorted(set(NO_WRITE_INVOCATIONS) & set(NEEDS_SAFE_INVOCATION))
    assert overlap == [], f"declared in BOTH dicts, pick one: {overlap}"


@pytest.mark.parametrize("name", sorted(
    n for n in NO_WRITE_INVOCATIONS if n not in ("rebind-seat", "rematerialize")))
async def test_no_write_invocation_returns_a_real_exit_code(
    name: str, actions: Actions,
) -> None:
    thunk = NO_WRITE_INVOCATIONS[name]
    buf_out, buf_err = io.StringIO(), io.StringIO()
    with redirect_stdout(buf_out), redirect_stderr(buf_err):
        out = await thunk(actions)
    assert isinstance(out, int), f"{name}: cmd_* returned {out!r}, not an int exit code"


async def test_no_write_invocation_rebind_seat(actions: Actions, tmp_path: Any) -> None:
    out = await NO_WRITE_INVOCATIONS["rebind-seat"](actions, tmp_path)
    assert isinstance(out, int)


async def test_no_write_invocation_rematerialize(actions: Actions, tmp_path: Any) -> None:
    out = await NO_WRITE_INVOCATIONS["rematerialize"](actions, tmp_path)
    assert isinstance(out, int)
    assert not (tmp_path / "x.jsonl").exists()


async def test_no_write_invocation_cmd_smoke_chaos(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`cmd_smoke_chaos` — reached via `smoke --chaos`, not its own subcommand, so it
    has no population-gate key of its own (`smoke` itself is in NEEDS_SAFE_INVOCATION).
    A bonus check anyway: its REAL default (`_real_chaos_gate`) kills/restarts real
    units — the same fake every existing test_cli.py test for it already substitutes."""
    import src.cli as cli_mod

    monkeypatch.setattr(cli_mod, "_real_chaos_gate", _fake_chaos_gate)
    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_smoke_chaos(pool=actions.pool)
    assert isinstance(out, int)
