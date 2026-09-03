"""ONE ACT, ONE NAME, ONE VOCABULARY (decision 0b29f1cbcc5a, dispatch 3683, the operator's
own words: "I need consistency and unity across the surfaces for humans and machines so we
can communicate properly") — the drift detector that law demanded. Every act that exists on
BOTH the CLI and the MCP surface must carry the SAME name and the SAME parameter names; any
asymmetry must be DECLARED here, with a reason, rather than left for an operator to notice a
month later. THREE MEASURED SPECIMENS proved this recurs and nothing was catching it:
fold-project/merge (renamed alongside this test, dispatch 3683), mint-seat's --adopt/--force
(msg 3681, now stated explicitly in mint_seat's own MCP docstring), and launch's
handle/target (found BUILDING this detector, not by a human — named honestly below rather
than hidden, since fixing it is a separate, riskier decision this dispatch didn't ask for).

WALKS BOTH SURFACES LIVE, never a hand-maintained mirror of either: CLI names/params come
from `_build_parser()`'s own argparse structure, MCP names/params from `mcp.list_tools()`'s
own inputSchema — the same measurement `_measure_tool_contract()` already trusts. A rename
on EITHER side that isn't reflected here fails this test, not a human noticing a month
later — that is the whole point: discipline cannot hold this because the two surfaces are
edited by different people at different times for different reasons.
"""
from __future__ import annotations

import argparse

from src.cli import _build_parser

# MCP session-identity plumbing present on SOME tools' own signatures (never all — a raw
# terminal invocation has no "subagent" or "session anchor" concept at all to reach parity
# with, so these are exempt globally rather than repeated per command).
_MCP_ONLY_PLUMBING = frozenset({
    "session_anchor", "subagent_id", "subagent_type", "subagent_transcript",
})

# Every sanctioned-second-door command takes --actor; no MCP tool does (an agent caller is
# identified by its own mounted session, msg 3681 — never a caller-supplied string that
# could simply be forged). One reason, every command, rather than the same sentence six
# times over.
_CLI_ONLY_GLOBAL = frozenset({"actor"})

# CLI commands with NO MCP tool at all (decision 0b29f1cbcc5a's own census). Five are
# operator devops acts with no agent equivalent BY DESIGN — the fix there is documentation,
# never harmonising for symmetry's own sake. The sixth is `fold-project`, the deprecated
# alias for `merge` kept working for muscle memory and deliberately never advertised —
# not a live act left to reconcile, so it belongs here too, not in a rename.
NO_MCP_EQUIVALENT = {
    "attach": "a human's own terminal act (attach to a live PTY session) — no agent "
             "equivalent",
    "boot-status": "operator devops read across the fleet's own compiled bodies",
    "deploy": "operator devops act — an agent should not restart shared services",
    "migrate": "operator devops act — an agent should not run schema migrations",
    "retention": "operator devops act (thread e6fd3772 piece 1, Khnum 288675e) — a retention "
                 "DELETE on outbox/audit_log has no unmerge; execute=False is the only "
                 "default and the act is deliberately kept off the agent-callable surface, "
                 "same class as deploy/migrate",
    "seed": "operator devops bootstrap act, rare and deliberate, not an ordinary agent verb",
    "fold-project": "deprecated alias for merge (dispatch 3683) — kept working for muscle "
                    "memory, never advertised; not a live act to reconcile",
    "resume": "operator ruling 60c78788 (2026-09-01) split the CLI's own `osiris launch` "
             "into launch/resume so the two front doors are each exactly one predictable "
             "thing (fresh --bg mint vs one-shot -p --resume). The MCP `launch` tool "
             "(backed by trigger.launch_seat) still has its OWN automatic resume-or-fresh "
             "branch, unsplit — whether it should also split into two MCP tools is a "
             "real, separate, operator-owned question, named to Thoth rather than decided "
             "unilaterally in this CLI-only change (thread bc11a2d3's family).",
    "new": "an OPERATOR founding a self-managed seat for a mind that does not exist yet "
          "(dispatch 3685/3688) is a DIFFERENT act from walk_in's self-naming (a mind "
          "that already exists arriving and naming ITSELF) — different actor, different "
          "precondition, different moment. The unity law binds one act to one name on "
          "both surfaces; it does not force two genuinely different acts to share one "
          "just because they're adjacent in purpose. Reasoned explicitly, not assumed.",
    "desk": "a deliberately NARROWED, terminal-native door onto inbox(project='operator', "
           "peek=True) — thread 00913be9, Thoth's CLI-surface audit: the operator's own "
           "desk was readable only via the web console or an agent peeking on his behalf. "
           "Not a rename (the MCP tool answers a broader question — any project's mailbox, "
           "leasing, ack) — 'desk' fixes both params to the one question a human at his "
           "own terminal actually asks, so it earns its own name rather than exposing "
           "inbox's full parameter surface as CLI flags.",
    "show": "a deliberately NARROWED, terminal-native door onto recall(ref=...) — same "
           "audit, same reasoning as 'desk' above: recall() is already the exact verb, "
           "'show' is simply the word a human reaches for at a prompt.",
}

# (cli_command, param) -> reason: a CLI-only param beyond the blanket "actor" exemption.
CLI_ONLY_PARAMS = {
    ("launch", "debug"): "the PTY-broker fallback lane is an operational/incident choice; "
        "the MCP tool only ever spawns the harness-native default substrate",
    ("mint-seat", "manager"): "MCP infers the manager from the caller's own held seat by "
        "design (\"there is no override param\" — mint_seat's own MCP docstring); a raw "
        "terminal holds no seat of its own to infer it from",
    ("mint-seat", "adopt"): "deliberate console-only escape hatch, stated explicitly in "
        "mint_seat's own MCP docstring (dispatch 3678 addendum, msg 3681)",
    ("mint-seat", "force"): "deliberate console-only escape hatch, same docstring",
    ("bootstrap", "project"): "bootstrap_project itself takes this override; the MCP "
        "tool's own wrapper never exposes it, always inferring from cwd's basename — a "
        "gap on that side, not an inconsistency to hide here",
    ("smoke", "chaos"): "the crash replay SIGKILLs osiris-mcp/osiris-worker and fires a "
        "session-end storm — an operator/manager-hand operational act (every worker "
        "charter says 'you NEVER restart services'); deliberately unreachable from any "
        "MCP tool a seat could call mid-turn (Sekhmet bfa2bd2, #178 residual). Still true "
        "for the REAL production daemons after #186 (dispatch 5690): this entry is about "
        "CLI-vs-MCP surface parity, unchanged. What #186 added is a SEPARATE, test-only "
        "pathway — tests/test_chaos.py's own isolated osiris-mcp/osiris-worker pair (own "
        "port, own testcontainer DB/Redis) — so the crash-replay mechanism itself is now "
        "exercised for real in the suite; neither a CLI param nor an MCP tool, so this "
        "table's own claim is untouched by it.",
    ("fleet", "as_json"): "a PRESENTATION flag, not an act: --json picks the compact "
        "one-line machine render over the human one at the terminal boundary "
        "(src/cli_render.emit). An MCP tool ALREADY returns structured data to its "
        "caller, so it has no second format to choose between — the counterpart would be "
        "meaningless, not missing. Identical content either way; this changes bytes on a "
        "tty, never what the verb does or returns (Thoth LXXXVII, 2026-08-28)",
    ("roster", "as_json"): "a PRESENTATION flag, not an act: --json picks the compact "
        "one-line machine render over the human one at the terminal boundary "
        "(src/cli_render.emit). An MCP tool ALREADY returns structured data to its "
        "caller, so it has no second format to choose between — the counterpart would be "
        "meaningless, not missing. Identical content either way; this changes bytes on a "
        "tty, never what the verb does or returns (Thoth LXXXVII, 2026-08-28)",
    ("unmerge", "as_json"): "a PRESENTATION flag, not an act: --json picks the compact "
        "one-line machine render over the human one at the terminal boundary "
        "(src/cli_render.emit). An MCP tool ALREADY returns structured data to its "
        "caller, so it has no second format to choose between — the counterpart would be "
        "meaningless, not missing. Identical content either way; this changes bytes on a "
        "tty, never what the verb does or returns (Thoth LXXXVII, 2026-08-28)",
    ("retention", "as_json"): "a PRESENTATION flag, not an act: --json picks the compact "
        "one-line machine render over the human one at the terminal boundary "
        "(src/cli_render.emit). An MCP tool ALREADY returns structured data to its "
        "caller, so it has no second format to choose between — the counterpart would be "
        "meaningless, not missing. Identical content either way; this changes bytes on a "
        "tty, never what the verb does or returns (Thoth LXXXVII, 2026-08-28)",
    ("stop", "as_json"): "a PRESENTATION flag, not an act — same reason as the four read "
        "verbs above; the stop receipt renders for a human by default and as one compact "
        "JSON line under --json, for the teardown scripts this verb exists to serve "
        "(Thoth LXXXVII, 2026-08-28)",
    ("smoke", "as_json"): "a PRESENTATION flag, not an act — same reason as the read "
        "verbs above (Thoth dispatch 6746, specimen A: the CLI's own top-level help "
        "promises every read verb takes --json, which was false for this one until "
        "this fix). Never --chaos's own path, which keeps its separate text-only receipt.",
    ("correct-pin-value", "seat"): "the MCP tool correct_pin_value is SELF-scoped by "
        "design (resolved off the mounted caller's own held_seat, msg 4761/obligation "
        "114f7ac9 — it can only ever correct ITS OWN seat's pin). A terminal has no "
        "mounted identity to be self about, so the console door (thread 6437) takes an "
        "EXPLICIT target instead, resolved the same way rebind-seat's own console door "
        "resolves its target (resolve_handle, falling back to a raw agent id that "
        "genuinely exists) before calling the identical offices.correct_own_pin_value.",
    ("heal-seat-anchor", "apply"): "the SAME dry-run/apply concept as MCP's `dry_run` "
        "below, INVERTED and renamed to match every other repair script's own --apply "
        "convention in this house (scripts/backfill_*.py) rather than the MCP tools' "
        "dry_run=True-default convention — apply=False means dry_run=True. Declared "
        "here, not reconciled: the two conventions serve different callers (a human "
        "typing a flag vs. an agent passing a keyword) and forcing one shape onto both "
        "would fight one of them.",
}

# (mcp_tool, param) -> reason: an MCP-only param with no CLI counterpart.
MCP_ONLY_PARAMS = {
    ("launch", "message"): "the CLI has no way to deliver an opening brief in one act "
        "today — a real gap, named rather than hidden, not yet built",
    ("heal_seat_anchor", "dry_run"): "the CLI's own `apply` (see CLI_ONLY_PARAMS above) "
        "is the same concept, inverted and renamed to match this house's --apply repair "
        "convention rather than the MCP tools' own dry_run=True default.",
}

# (cli_command, cli_param, mcp_tool, mcp_param) -> reason: the SAME concept under TWO
# DIFFERENT NAMES — the sharpest shape this law names. Neither renamed here: fixing either
# risks CLI muscle memory (launch is the front door itself) or an MCP tool-contract cost
# nobody has weighed yet — naming the drift honestly is this dispatch's job, not silently
# picking a side.
RENAMED_PARAMS = {
    ("launch", "handle", "launch", "target"):
        "the same seat reference, two names — found building this detector, not by a "
        "human; not yet reconciled",
    ("stop", "handle", "stop", "target"):
        "the same seat reference, two names — MATCHES launch's own handle/target split "
        "exactly (the entry above), and deliberately so: stop is launch's inverse and the "
        "pair must read identically at the terminal. Reconciling this means reconciling "
        "BOTH together, never one of them (Thoth LXXXVII, 2026-08-28)",
    ("charter-for", "seat", "charter_for", "seat_id"):
        "the same seat reference, two names — found building this detector; not yet "
        "reconciled",
    ("heal-seat-anchor", "seat", "heal_seat_anchor", "seat_id"):
        "the same seat reference, two names — same shape as charter-for/seat above. "
        "heal_seat_anchor was consolidated (task #199 lane 2, thread 6778): it now takes "
        "an optional `seat_id` covering what heal_seat_anchor_third_party's own `seat_id` "
        "used to (that name is now a hidden, deprecated alias forwarding here, dropped "
        "from list_tools() but still callable — see mcp_server.py's BoundedMCP.list_tools "
        "override). The CLI door (`osiris heal-seat-anchor <seat> --because`) still "
        "calls identity_heal.heal_seat_anchor_third_party directly, unchanged and "
        "unaffected by the MCP-layer consolidation; its own `seat` param maps to this "
        "same `seat_id`, not yet reconciled by name.",
}

# LANE 3 (Thoth dispatch, msg 6438): the drift detector above walks ONE direction only —
# every CLI command needs an MCP tool or a declared reason (NO_MCP_EQUIVALENT). It has no
# reverse loop at all, so an MCP tool with zero CLI counterpart is structurally invisible to
# it — not exempted, never checked. That is the actual, code-level reason it never caught the
# seat/office/project binding-verb population being wildly CLI-poor (measured: 4 of ~20 had a
# CLI door before Khnum's khnum-cli-parity lane added two more).
#
# The fix is NOT "walk all 147 MCP tools looking for a missing CLI door" — most of them
# (search, recall, record_decision, dossier, ingest_*, the whole mail/thread/lease surface)
# are agent-cognition primitives with no terminal analogue BY NATURE, and forcing every one
# to carry a NO_CLI_EQUIVALENT reason would be the exact hollow-by-declaration failure Thoth
# warned about: a table so large nobody reads it, satisfied by rote rather than judgment.
# Ruling (decision this dispatch records): SCOPE THE GATE to the population the dispatch is
# actually about — verbs that MOVE A BINDING (a seat's identity, tree, office, or a
# project's own existence) — named explicitly here rather than inferred from a naming
# convention, so adding a new binding-moving MCP tool later means adding it to this set on
# purpose, not drifting into or out of scope silently.
BINDING_VERBS = frozenset({
    "mint_seat", "launch", "stop", "walk_in", "attach_seat", "detach_seat", "pause_seat",
    "vacate_seat", "retire_seat", "rebind_seat", "bind_seat_tree", "sweep_seat_disk",
    "rename_seat", "set_seat_attended", "reissue_office", "establish_office", "charter",
    "charter_for", "invalidate_works_in", "reconcile_seat_identity",
    "reconcile_seat_identity_third_party", "correct_pin_value", "create_project",
    "rename_project", "retire_project", "fork_project", "unfork_project",
    "heal_seat_anchor",
})

# mcp_tool -> reason: a BINDING_VERBS member with no CLI door at all (mirrors
# NO_MCP_EQUIVALENT's shape, reversed). "not yet built" is still a DECLARED gap, not a
# hidden one — distinct from a "by design" entry, and worth re-reading before assuming any
# one of these should stay this way forever.
NO_CLI_EQUIVALENT = {
    "walk_in": "agent-only by design (same shape as found_seat/launch, Thoth dispatch "
        "6438): 'a mind with nothing but this server' names itself — there is no human "
        "at a terminal on the other end of this call, ever.",
    "charter": "self-charter has no CLI door by design: it resolves the target seat from "
        "the CALLING agent's own mounted identity (set_charter), and a raw terminal holds "
        "no such identity to be self about. charter_for (the operator-on-another's-behalf "
        "form) already has one, --repos and all.",
    "attach_seat": "not yet built — a real gap named by Khnum's lane-2 scoping (msg 6463): "
        "not on the jesus/chad reconciliation path his dispatch scoped him to.",
    "detach_seat": "not yet built — same scoping note as attach_seat.",
    "pause_seat": "not yet built — not on the jesus/chad path; not ruled out.",
    "vacate_seat": "not yet built — not on the jesus/chad path; not ruled out.",
    "retire_seat": "not yet built at the RAW CLI layer — already reachable indirectly via "
        "the /seat retire slash command (step 1 of its two-step retire), but that is a "
        "prose composition outside this repo, not a `_build_parser()` subcommand this "
        "gate can see; a direct CLI door is still a real, separate gap.",
    "bind_seat_tree": "not yet built at the raw CLI layer — same shape as retire_seat: "
        "already reachable via /seat bind-tree's slash composition, not via argparse.",
    "sweep_seat_disk": "not yet built at the raw CLI layer — same shape: reachable via "
        "/seat retire's step 2, not via argparse.",
    "rename_seat": "not yet built — not on the jesus/chad path; not ruled out.",
    "set_seat_attended": "not yet built — not on the jesus/chad path; not ruled out.",
    "reissue_office": "not yet built — not on the jesus/chad path; not ruled out.",
    "establish_office": "not yet built — not on the jesus/chad path; not ruled out.",
    "invalidate_works_in": "not yet built — not on the jesus/chad path; not ruled out.",
    "reconcile_seat_identity": "not yet built — not on the jesus/chad path; not ruled out.",
    "reconcile_seat_identity_third_party": "not yet built — same as reconcile_seat_identity.",
    "create_project": "not yet built — project lifecycle verbs weren't in lane 2's scope "
        "(seat reconciliation only); a real gap, not ruled out.",
    "rename_project": "not yet built — same scoping note as create_project.",
    "retire_project": "not yet built — same scoping note as create_project.",
    "fork_project": "not yet built — same scoping note as create_project.",
    "unfork_project": "not yet built — same scoping note as create_project.",
}


def _find_missing_cli_doors(
    cli: dict[str, set[str]], mcp: dict[str, set[str]], *,
    binding_verbs: frozenset[str] = BINDING_VERBS,
    no_cli_equivalent: dict[str, str] = NO_CLI_EQUIVALENT,
) -> list[str]:
    """The REVERSE of _find_problems's own direction, deliberately scoped to
    BINDING_VERBS rather than all of `mcp` — see the ruling above the table this reads.
    Returns one string per binding verb that exists on MCP, has no CLI command of the same
    name (dashes for underscores), and carries no reason in NO_CLI_EQUIVALENT."""
    cli_as_mcp_names = {name.replace("-", "_") for name in cli}
    problems: list[str] = []
    for name in sorted(binding_verbs):
        if name not in mcp:
            problems.append(
                f"{name!r} is in BINDING_VERBS but no such MCP tool exists — stale entry")
            continue
        if name in cli_as_mcp_names:
            continue
        if name not in no_cli_equivalent:
            problems.append(
                f"MCP {name!r} is a binding-moving verb with no CLI command and no "
                f"reason in NO_CLI_EQUIVALENT")
    return problems


async def test_every_binding_verb_has_a_cli_door_or_declares_why_not() -> None:
    problems = _find_missing_cli_doors(_cli_commands(), await _mcp_tools())
    assert problems == [], (
        "a seat/office/project binding-moving verb drifted out of CLI reach, undeclared "
        "(Thoth dispatch 6438 — BINDING_VERBS is the scoped population this lane gates; "
        "add a NO_CLI_EQUIVALENT reason, or build the door):\n" + "\n".join(problems))


def test_no_cli_equivalent_entries_still_name_real_binding_verbs() -> None:
    """Same discipline as test_allowlist_entries_still_name_real_things, mirrored: an
    exemption for a verb that got renamed, retired, or actually gained a CLI door should
    fail here rather than sit stale and unread."""
    for name in NO_CLI_EQUIVALENT:
        assert name in BINDING_VERBS, (
            f"{name!r} is in NO_CLI_EQUIVALENT but not in BINDING_VERBS — either scope it "
            f"in or drop the now-pointless entry")


def test_the_reverse_detector_itself_catches_an_undeclared_missing_cli_door() -> None:
    """PROVE THE MECHANISM — same discipline as the forward detector's own proof tests."""
    cli: dict[str, set[str]] = {}
    mcp: dict[str, set[str]] = {"some_binding_verb": set()}
    problems = _find_missing_cli_doors(
        cli, mcp, binding_verbs=frozenset({"some_binding_verb"}), no_cli_equivalent={})
    assert len(problems) == 1 and "some_binding_verb" in problems[0]

    declared = _find_missing_cli_doors(
        cli, mcp, binding_verbs=frozenset({"some_binding_verb"}),
        no_cli_equivalent={"some_binding_verb": "test fixture, deliberate"})
    assert declared == []

    built = _find_missing_cli_doors(
        {"some-binding-verb": set()}, mcp,
        binding_verbs=frozenset({"some_binding_verb"}), no_cli_equivalent={})
    assert built == []


def _cli_commands() -> dict[str, set[str]]:
    """Every registered subcommand's own param set (positional dest + optional dest,
    `-h`/`--help` excluded) — read live off `_build_parser()`, never hand-copied."""
    parser = _build_parser()
    sub_action = next(
        a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
    return {
        name: {act.dest for act in subparser._actions if act.dest != "help"}
        for name, subparser in sub_action.choices.items()
    }


async def _mcp_tools() -> dict[str, set[str]]:
    """Every registered MCP tool's own param set, plumbing stripped — read live off
    `mcp.list_tools()`'s own inputSchema, the same source `_measure_tool_contract()`
    trusts for the ratchet."""
    from src import mcp_server as srv

    tools = await srv.mcp.list_tools()
    return {
        t.name: set(t.inputSchema.get("properties", {}).keys()) - _MCP_ONLY_PLUMBING
        for t in tools
    }


def _find_problems(
    cli: dict[str, set[str]], mcp: dict[str, set[str]], *,
    no_mcp_equivalent: dict[str, str] = NO_MCP_EQUIVALENT,
    cli_only_params: dict[tuple[str, str], str] = CLI_ONLY_PARAMS,
    mcp_only_params: dict[tuple[str, str], str] = MCP_ONLY_PARAMS,
    renamed_params: dict[tuple[str, str, str, str], str] = RENAMED_PARAMS,
) -> list[str]:
    """The reconciler's own core check, extracted so it can be proven against synthetic
    fixtures (see test_the_detector_itself below) rather than only exercised live — the
    same "prove the mechanism, don't just assert it" discipline the merge-driver reconciler
    used. Returns one string per UNDECLARED mismatch; empty means everything either
    matches or is named on an allowlist."""
    problems: list[str] = []
    for cli_name, cli_params in cli.items():
        mcp_name = cli_name.replace("-", "_")
        if mcp_name not in mcp:
            if cli_name not in no_mcp_equivalent:
                problems.append(
                    f"{cli_name!r} has no MCP tool {mcp_name!r} and no reason in "
                    f"NO_MCP_EQUIVALENT")
            continue
        mcp_params = mcp[mcp_name]

        cli_extra = cli_params - mcp_params - _CLI_ONLY_GLOBAL
        cli_extra -= {p for p in cli_extra if (cli_name, p) in cli_only_params}
        cli_extra -= {p for p in cli_extra
                     if any(k[0] == cli_name and k[1] == p for k in renamed_params)}
        if cli_extra:
            problems.append(
                f"{cli_name!r} has CLI-only param(s) {sorted(cli_extra)} with no MCP "
                f"{mcp_name!r} counterpart and no reason in CLI_ONLY_PARAMS")

        mcp_extra = mcp_params - cli_params
        mcp_extra -= {p for p in mcp_extra if (mcp_name, p) in mcp_only_params}
        mcp_extra -= {p for p in mcp_extra
                     if any(k[2] == mcp_name and k[3] == p for k in renamed_params)}
        if mcp_extra:
            problems.append(
                f"MCP {mcp_name!r} has param(s) {sorted(mcp_extra)} with no CLI "
                f"{cli_name!r} counterpart and no reason in MCP_ONLY_PARAMS")
    return problems


async def test_every_cli_command_and_param_matches_its_mcp_tool_or_declares_why_not() -> None:
    problems = _find_problems(_cli_commands(), await _mcp_tools())
    assert problems == [], (
        "CLI/MCP surfaces drifted apart, undeclared (decision 0b29f1cbcc5a — an act on "
        "both surfaces needs the same name and params, or a stated reason):\n"
        + "\n".join(problems))


def test_allowlist_entries_still_name_real_things() -> None:
    """An allowlist that outlives what it excuses stops meaning anything a reader can
    trust — every entry's own command/param must still exist on the side it claims to,
    so a later rename or removal is caught here rather than leaving a dangling exemption
    nobody notices is now pointless."""
    cli = _cli_commands()
    for name in NO_MCP_EQUIVALENT:
        assert name in cli, f"{name!r} is allowlisted as CLI-only but no longer exists"
    for cli_name, param in CLI_ONLY_PARAMS:
        assert cli_name in cli, f"{cli_name!r} no longer exists (stale allowlist entry)"
        assert param in cli[cli_name], (
            f"{cli_name!r}'s {param!r} no longer exists (stale allowlist entry)")
    for cli_name, cli_param, _mcp_name, _mcp_param in RENAMED_PARAMS:
        assert cli_name in cli, f"{cli_name!r} no longer exists (stale RENAMED_PARAMS entry)"
        assert cli_param in cli[cli_name], (
            f"{cli_name!r}'s {cli_param!r} no longer exists (stale RENAMED_PARAMS entry)")


async def test_allowlist_entries_still_name_real_mcp_things() -> None:
    mcp = await _mcp_tools()
    for mcp_name, param in MCP_ONLY_PARAMS:
        assert mcp_name in mcp, f"MCP {mcp_name!r} no longer exists (stale allowlist entry)"
        assert param in mcp[mcp_name], (
            f"MCP {mcp_name!r}'s {param!r} no longer exists (stale allowlist entry)")
    for _cli_name, _cli_param, mcp_name, mcp_param in RENAMED_PARAMS:
        assert mcp_name in mcp, f"MCP {mcp_name!r} no longer exists (stale RENAMED_PARAMS)"
        assert mcp_param in mcp[mcp_name], (
            f"MCP {mcp_name!r}'s {mcp_param!r} no longer exists (stale RENAMED_PARAMS)")


def test_the_detector_itself_catches_an_undeclared_drift() -> None:
    """PROVE THE MECHANISM, DON'T JUST ASSERT IT — a synthetic pair with a real,
    undeclared mismatch must fail; the same pair, once the mismatch is allowlisted, must
    pass. Never trust a checker that has only ever seen a clean tree."""
    cli = {"widget": {"name", "actor"}}
    mcp = {"widget": {"name"}}
    assert _find_problems(cli, mcp) == []  # "actor" is globally exempt — a true non-issue

    cli_drifted = {"widget": {"name", "surprise_param"}}
    problems = _find_problems(cli_drifted, mcp)
    assert len(problems) == 1
    assert "surprise_param" in problems[0]

    # the SAME drift, now declared, passes clean
    problems_allowlisted = _find_problems(
        cli_drifted, mcp,
        cli_only_params={("widget", "surprise_param"): "test fixture, deliberate"})
    assert problems_allowlisted == []


def test_the_detector_itself_catches_a_missing_no_mcp_equivalent_reason() -> None:
    cli = {"console-only-verb": {"x"}}
    mcp: dict[str, set[str]] = {}
    problems = _find_problems(cli, mcp)
    assert len(problems) == 1 and "console-only-verb" in problems[0]
    assert _find_problems(
        cli, mcp, no_mcp_equivalent={"console-only-verb": "test fixture"}) == []


def test_the_detector_itself_catches_a_rename_without_a_declared_pair() -> None:
    cli = {"widget": {"handle"}}
    mcp = {"widget": {"target"}}
    problems = _find_problems(cli, mcp)
    assert len(problems) == 2  # handle unexplained on the CLI side, target on the MCP side
    clean = _find_problems(
        cli, mcp,
        renamed_params={("widget", "handle", "widget", "target"): "test fixture"})
    assert clean == []
