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
command's own signature, true regardless of what ref is passed, or (c) — the class
this file's own second wave (mail 9382 item 6) added — a resolve-first-then-refuse
path CONFIRMED by reading the wrapped orchestrator function's own body: an unknown/
nonexistent ref hits a named refusal (an "unknown seat"/"unknown project"/etc. return)
strictly BEFORE any write, verified line-by-line rather than assumed from this house's
general convention. NEEDS_SAFE_INVOCATION below names every command where that same
close reading found the OPPOSITE — either the body genuinely writes unconditionally
(mint-seat/new/bootstrap/create-project mint for real every time; decide always
records a fresh Decision; smoke/deploy/seed touch real infra with no dry-run switch).
The completeness gate requires every live subcommand to be in ONE of the two dicts — so
a name can never silently fall through either as untested or as wrongly presumed safe.

resync-seat-house's own third-party verb WAS the one specimen this reading actually
caught, not merely presumed: it used to find-or-CREATE the Seat object it was told to
correct with no existence check at all, so a "safe" nonexistent-ref call would silently
mint a stray Seat rather than refuse. Fixed (WAVE 21 item 4, mail 9869 ad48598f) —
resync_seat_house_third_party now refuses an unknown seat_id by name, the same
`SELECT ... WHERE canonical=$1 AND type='Seat' AND status='active'` shape its own
precedent-named sibling reconcile_seat_identity_third_party already used; moved to
NO_WRITE_INVOCATIONS below alongside it.
"""
from __future__ import annotations

import argparse
import io
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.cli import (
    _build_parser,
    cmd_amend_decision,
    cmd_amend_practice,
    cmd_annotate_thread,
    cmd_attach,
    cmd_attach_seat,
    cmd_audit,
    cmd_backfill,
    cmd_backlog,
    cmd_backup_settings,
    cmd_backup_status,
    cmd_bind_seat_tree,
    cmd_boot_status,
    cmd_candidates,
    cmd_charter_for,
    cmd_citation,
    cmd_cite,
    cmd_composition,
    cmd_correct_agent_house,
    cmd_correct_agent_project,
    cmd_correct_pin_value,
    cmd_declare_machine_identity,
    cmd_desk,
    cmd_detach_seat,
    cmd_digest,
    cmd_dossier,
    cmd_establish_office,
    cmd_establish_seat_dir,
    cmd_fleet,
    cmd_fleet_prune,
    cmd_fleet_reconcile,
    cmd_fold_project,
    cmd_fork_project,
    cmd_graph_export,
    cmd_graph_migrate,
    cmd_heal_seat_anchor,
    cmd_heal_seat_transcript,
    cmd_inbox,
    cmd_inspect,
    cmd_launch,
    cmd_lint,
    cmd_merge,
    cmd_migrate,
    cmd_object_events,
    cmd_practices,
    cmd_promote,
    cmd_proposal,
    cmd_rebind_seat,
    cmd_reconcile_merge,
    cmd_reconcile_seat_identity,
    cmd_reissue_office,
    cmd_reissue_seat_dir,
    cmd_rematerialize,
    cmd_rename_project,
    cmd_rename_seat,
    cmd_resume,
    cmd_resync_seat_project,
    cmd_retention,
    cmd_retire_agent,
    cmd_retire_assertion,
    cmd_retire_link,
    cmd_retire_object,
    cmd_retire_project,
    cmd_retire_seat,
    cmd_roster,
    cmd_search,
    cmd_send,
    cmd_set_project_tag,
    cmd_set_seat_attended,
    cmd_settings,
    cmd_settle,
    cmd_show,
    cmd_smoke_chaos,
    cmd_status,
    cmd_stop,
    cmd_succession_chain,
    cmd_sweep_seat_disk,
    cmd_sweep_seat_trees,
    cmd_team,
    cmd_thread,
    cmd_threads,
    cmd_transition_seat_project,
    cmd_unmerge,
    cmd_vacate_seat,
)

_REPO_ROOT = Path(__file__).resolve().parent.parent


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
    "lint": lambda a: cmd_lint(pool=a.pool),
    "graph-export": lambda a: cmd_graph_export(as_json=True, pool=a.pool),
    "audit": lambda a: cmd_audit("the-wall", pool=a.pool),
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
    "sweep-seat-trees": lambda a: cmd_sweep_seat_trees(
        apply=False, actor="operator", pool=a.pool),
    "transition-seat-project": lambda a: cmd_transition_seat_project(
        "no-such-handle", apply=False, pool=a.pool),
    "heal-seat-anchor": lambda a: cmd_heal_seat_anchor(
        "no-such-handle", because="test", apply=False, actor="operator", pool=a.pool),
    "heal-seat-transcript": lambda a: cmd_heal_seat_transcript(
        "no-such-handle", ["/tmp/does-not-exist.jsonl"], apply=False, pool=a.pool),
    "backfill": lambda a: cmd_backfill(
        "bootstrap_orphan_references", apply=False, actor="operator", pool=a.pool),
    "graph-migrate": lambda a: cmd_graph_migrate(
        "repo_seats_fix", apply=False, actor="operator", pool=a.pool),
    "fleet-reconcile": lambda a: cmd_fleet_reconcile(
        execute=False, actor="operator", pool=a.pool),
    "fleet-prune": lambda a: cmd_fleet_prune(execute=False, actor="operator", pool=a.pool),
    "rename-project": lambda a: cmd_rename_project(
        "no-such-project", "still-no-such-project", "test", dry_run=True,
        actor="operator", pool=a.pool),
    "retention": lambda a: cmd_retention(
        "not-a-real-table", days=30, execute=False, pool=a.pool),
    "migrate": lambda a: cmd_migrate(
        check=True, repo_root=_REPO_ROOT, pool=a.pool),
    # --- wave 2 (mail 9382 item 6): each of these was proven refusal-only by reading
    # --- the wrapped orchestrator function's own body — an unknown ref hits a named
    # --- refusal strictly before any write, not merely assumed from house convention.
    "merge": lambda a: cmd_merge(
        "no-such-dupe-anywhere", "no-such-into-anywhere", "test evidence",
        actor="operator", pool=a.pool),
    "fold-project": lambda a: cmd_fold_project(
        "no-such-dupe-anywhere", "no-such-into-anywhere", "test evidence",
        actor="operator", pool=a.pool),
    "reconcile-merge": lambda a: cmd_reconcile_merge(
        "no-such-dupe-anywhere", "no-such-into-anywhere", actor="operator", pool=a.pool),
    "retire-agent": lambda a: cmd_retire_agent(
        "no-such-agent-anywhere", "test", actor="operator", pool=a.pool),
    "attach-seat": lambda a: cmd_attach_seat(
        "no-such-worker-anywhere", "no-such-manager-anywhere", "test evidence",
        actor="operator", pool=a.pool),
    "detach-seat": lambda a: cmd_detach_seat(
        "no-such-seat-anywhere", "test", actor="operator", pool=a.pool),
    "promote": lambda a: cmd_promote(
        "no-such-target-anywhere", ["no-such-worker-anywhere"], "test",
        actor="operator", pool=a.pool),
    "vacate-seat": lambda a: cmd_vacate_seat(
        "no-such-seat-anywhere", "test", actor="operator", pool=a.pool),
    "retire-seat": lambda a: cmd_retire_seat(
        "no-such-seat-anywhere", "test", actor="operator", pool=a.pool),
    "bind-seat-tree": lambda a: cmd_bind_seat_tree(
        "no-such-seat-anywhere", "/tmp/does-not-matter", "test",
        actor="operator", pool=a.pool),
    "rename-seat": lambda a: cmd_rename_seat(
        "no-such-seat-anywhere", "still-no-such-handle", "test",
        actor="operator", pool=a.pool),
    "set-seat-attended": lambda a: cmd_set_seat_attended(
        "no-such-seat-anywhere", "worker", "test", actor="operator", pool=a.pool),
    "reissue-office": lambda a: cmd_reissue_office(
        "no-such-seat-anywhere", "test", actor="operator", pool=a.pool),
    "establish-office": lambda a: cmd_establish_office(
        "no-such-seat-or-agent-anywhere", actor="operator", pool=a.pool),
    "reissue-seat-dir": lambda a: cmd_reissue_seat_dir(
        "no-such-seat-anywhere", "test", actor="operator", pool=a.pool),
    "establish-seat-dir": lambda a: cmd_establish_seat_dir(
        "no-such-seat-or-agent-anywhere", actor="operator", pool=a.pool),
    "reconcile-seat-identity": lambda a: cmd_reconcile_seat_identity(
        "no-such-seat-anywhere", "test", actor="operator", pool=a.pool),
    "resync-seat-project": lambda a: cmd_resync_seat_project(
        "no-such-seat-anywhere", "test", actor="operator", pool=a.pool),
    "retire-project": lambda a: cmd_retire_project(
        "no-such-project-anywhere", "test", actor="operator", pool=a.pool),
    "retire-object": lambda a: cmd_retire_object(
        "no-such-object-anywhere", "test", actor="operator", pool=a.pool),
    "fork-project": lambda a: cmd_fork_project(
        "no-such-project-anywhere", "still-no-such-project-anywhere", "test",
        actor="operator", pool=a.pool),
    "set-project-tag": lambda a: cmd_set_project_tag(
        "no-such-project-anywhere", "ZZ", "test", actor="operator", pool=a.pool),
    "charter-for": lambda a: cmd_charter_for(
        "no-such-seat-anywhere", [], "test", actor="operator", pool=a.pool),
    "correct-pin-value": lambda a: cmd_correct_pin_value(
        "no-such-handle-anywhere", "some_key", "some_value", "test", pool=a.pool),
    "correct-agent-project": lambda a: cmd_correct_agent_project(
        "no-such-handle-anywhere", actor="operator", pool=a.pool),
    "correct-agent-house": lambda a: cmd_correct_agent_house(
        "no-such-handle-anywhere", actor="operator", pool=a.pool),
    "declare-machine-identity": lambda a: cmd_declare_machine_identity(
        "nobody@example.com", "no-such-project-anywhere", because="test",
        actor="operator", pool=a.pool),
    "proposal": lambda a: cmd_proposal(
        "propose", candidate="not valid json{", pool=a.pool),
    # settings get: get_setting refuses BEFORE any write on an unregistered key
    # (settings_service.get_setting's own "unknown setting key" refusal) — a pure
    # read regardless, never anything write-shaped.
    "settings": lambda a: cmd_settings("get", key="no-such-key-anywhere", pool=a.pool),
    # backup-settings get: a pure read (get_backup_settings), never write-shaped —
    # PARITY GAPS, WAVE 27 item 3 (thread 45aff160).
    "backup-settings": lambda a: cmd_backup_settings("get", pool=a.pool),
    # backup-status: a pure read (_fn_backup_status), never write-shaped — THE BACKUP
    # CLI DOOR (Thoth mail 12809).
    "backup-status": lambda a: cmd_backup_status(pool=a.pool),
    # CLI PARITY, THE NEXT CENSUS GAPS (Thoth mail 10441, thread 163c6832): dossier/
    # object-events/succession-chain/candidates/composition are all pure reads called
    # over the wire (same shape as show/search above); retire-assertion/retire-link/
    # cite resolve-first-then-refuse on a nonexistent ref strictly before any write
    # (retirement.py's own body, read directly — see the CLI door's own docstring);
    # citation is a pure read, never writes.
    # #92, THE ZERO-TOKEN READ HOOK (Thoth mail 11780 item B): fleet_digest is a pure
    # read (watermark mode, mark_seen defaults False so this never advances it).
    "digest": lambda a: cmd_digest(),
    # #93, THE MECHANICAL SETTLE (Thoth mail 11789): no args is settle()'s own read-only
    # completeness-boxes surface, per its own docstring — never a write.
    "settle": lambda a: cmd_settle(),
    "dossier": lambda a: cmd_dossier("no-such-ref-anywhere"),
    "object-events": lambda a: cmd_object_events("no-such-ref-anywhere"),
    "succession-chain": lambda a: cmd_succession_chain("no-such-ref-anywhere"),
    "candidates": lambda a: cmd_candidates(project="no-such-project-anywhere", limit=1),
    "composition": lambda a: cmd_composition("list"),
    "inspect": lambda a: cmd_inspect("no-such-ref-anywhere"),
    "practices": lambda a: cmd_practices(pool=a.pool),
    "retire-assertion": lambda a: cmd_retire_assertion(
        "no-such-ref-anywhere", "no_such_property", 999999999, "x", "test",
        actor="operator", pool=a.pool),
    "retire-link": lambda a: cmd_retire_link(
        "no-such-ref-anywhere", "no-such-other-ref-anywhere", "no_such_link_type", "test",
        actor="operator", pool=a.pool),
    "cite": lambda a: cmd_cite(
        "no-such-ref-anywhere", "no-such-agent-anywhere", 0, "test",
        actor="operator", pool=a.pool),
    "citation": lambda a: cmd_citation(
        "no-such-ref-anywhere", "no-such-agent-anywhere", pool=a.pool),
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
    "layout": "genuinely writes unconditionally (own body read) — graph_layout."
              "run_layout_migrate loops layout_batch, which asserts real graph_x/"
              "graph_y/graph_layout_v on every unplaced object it finds; no dry-run "
              "switch, and a hermetic test DB is never guaranteed to already be fully "
              "placed under the current version, so a 'safe' invocation would still "
              "write for real",
    "smoke": "needs a live osiris-mcp pool + real chrome routes per its own docstring "
             "(\"8 chrome routes + the live mcp pool\") — a different, heavier infra "
             "assumption than the plain MCP search/fleet calls above",
    "deploy": "11 injectable callables (git_status/restart/chaos gate/etc), every one "
              "defaulting to the REAL side-effecting callable (own body read, wave 2): "
              "assembling a full fake set here risks missing one and running a real "
              "restart/kill",
    "seed": "genuinely writes (compositions/canon) even with compositions_only=True; "
            "no dry-run flag — already declared an operator devops bootstrap act "
            "elsewhere (NO_MCP_EQUIVALENT)",
    "create-project": "own body read, wave 2: create_project is a genuine find-OR-CREATE "
                      "(never a mint-a-twin door, but a never-before-seen name mints "
                      "fresh every time) — no refusal-only shape exists for any name "
                      "guaranteed not to collide",
    "decide": "own body read, wave 2: record_decision unconditionally mints a fresh "
              "Decision on any valid summary/kind — there is no ref to fail to resolve. "
              "The one near-miss (grounds=['not-a-uuid']) does avoid the write, but only "
              "by raising SystemExit from this CLI door's own pre-call UUID validation "
              "(_uuids()), not by returning a clean int — the wrong shape for this file's "
              "own thunk contract, so still no usable no-write invocation",
    "soul-key-init": "genuinely writes a real key file to disk for any non-root caller "
                     "(soul_key_init's own refusal gate fires ONLY when running as root "
                     "with no --owner given — this test process is never root); no "
                     "dry-run flag, same operator-devops-bootstrap class as seed/bootstrap",
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
