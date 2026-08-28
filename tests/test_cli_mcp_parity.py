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
    "new": "an OPERATOR founding a self-managed seat for a mind that does not exist yet "
          "(dispatch 3685/3688) is a DIFFERENT act from walk_in's self-naming (a mind "
          "that already exists arriving and naming ITSELF) — different actor, different "
          "precondition, different moment. The unity law binds one act to one name on "
          "both surfaces; it does not force two genuinely different acts to share one "
          "just because they're adjacent in purpose. Reasoned explicitly, not assumed.",
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
}

# (mcp_tool, param) -> reason: an MCP-only param with no CLI counterpart.
MCP_ONLY_PARAMS = {
    ("launch", "message"): "the CLI has no way to deliver an opening brief in one act "
        "today — a real gap, named rather than hidden, not yet built",
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
}


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
