"""THE TOOL-CONTRACT RATCHET (task #129, Thoth's ruling 1c414054): osiris-mcp's own
advertised tool surface — every `@mcp.tool()` NAME + DESCRIPTION + inputSchema — is paid by
every client eagerly, at connect time, before its first act. Measured before any cut: 97
tools, names 1,109 + descriptions ~85,988 + inputSchemas ~32,130 chars ~= 119,227 chars total
(~30k tokens). Same mechanism as #125's frozen tool-list index: too big to ship eagerly ->
deferred -> stale cache. Every char cut here is pressure off the thing that freezes.

THE MEASUREMENT IS THE LIVE, IN-PROCESS TOOL REGISTRATION, NOT A DOCSTRING GREP: `t.name` /
`t.description` / `t.inputSchema` come from `mcp.list_tools()` on the actual FastMCP server
object this module builds — the exact three fields a connecting client receives (json.dumps
on inputSchema because that is how it travels the wire, a dict). A raw `ast.get_docstring`
scan under-counts: FastMCP's own description rendering and the JSON schema (which is NOT a
docstring at all — it comes from the function's type hints and Field() defaults) both add
weight a source-only scan would miss entirely. No live deploy or network round-trip needed
(list_tools() runs against the local server object) — this stays a plain, offline pytest.

THIS NUMBER MOVES DOWNWARD BY HAND, NEVER RECOMPUTED (same law as
tests/test_render_hygiene.py's `_ALLOWLIST`, Thoth msg 1921): a ratchet that derives its own
ceiling from the tree is not a ratchet, it is a thermometer. If you are here because this
test failed after you ADDED prose to a tool docstring (or a new tool), the fix is trimming
it back under the category rule (task #129: keep what the verb does, what the args mean,
WHAT IT REFUSES AND WHY, what it returns, the trap that makes callers get it wrong; cut
provenance, war stories, dated citations, restatement of the inputSchema) or, if the
addition is genuinely load-bearing, RAISE the ceiling as a deliberate, justified act — never
bump it to make a failure go away without reading why it fired.

A CEILING, NOT EXACT EQUALITY (unlike chrome.py's per-file counts): inputSchema's JSON
serialization can shift by a few characters for reasons wholly unrelated to content (a
FastMCP/pydantic version bump reordering fields, a default's repr changing) — an aggregate
across 97 tools is the wrong place to chase byte-exact reproducibility. The margin above is
small and round on purpose; it catches real regrowth, not serialization noise.

ALL 23 OF THOTH'S NAMED HEAVY VERBS NOW CUT (task #129, two tranches): tranche 1 landed
record_decision 5,371 -> 3,671 (-32%) and settle 4,824 -> 3,687 (-24%). Tranche 2 (this
commit) landed the remaining 21 — launch, send, open_thread, wake, rebind_seat, triage,
dispose, acquire_lease, save_composition, graph_lint, handoff_briefing, fleet,
record_practice, retire, vacate_seat, lift, mount, pause_seat, charter_for, fold_project,
orient. Whole-surface total: 119,227 -> 114,216 chars (-5,011, -4.2%) — a SMALLER cut than
tranche 1's own ~28% per-verb rate, honestly reported rather than smoothed over: the first
two verbs carried unusually heavy citation/war-story prose; most of these 21 are already
lean, argument-dense reference material (spec grammars, refusal-status enums) with less fat
to trim without crossing the hard reject line (task #129: never cut a refusal). Confirmed
with Thoth (msg 2570) BEFORE this tranche that the headline token number was never going to
match the "transformation" a 30k-token boot cost implies — the ratchet existing at all, and
holding, is the point; the char count is a side effect of doing that properly.

RAISED DELIBERATELY, task #103's tree_cwd build (Thoth DM 2794 sign-off): +1 tool
(bind_seat_tree, 3 string params -- seat_id/tree_cwd/because), 114,216 -> 115,146 chars
exactly. Trimmed the new tool's own docstring under the category rule first (provenance/
ticket citations moved to the seats.py implementation, which this ratchet does not measure,
never duplicated into the MCP-facing description); the remaining growth is the new
capability itself, a real verb rather than prose creep on an existing one, so the ceiling
moves rather than the tool disappearing. 97 -> 98 tools.
"""
from __future__ import annotations

import json

# RAISED, task 187323d9's graph_lint gate (2026-08-02): found this test ALREADY failing at
# 118,901 chars before touching anything — a genuinely pre-existing overage, not fresh
# growth from this commit. Root cause: amend_practice (task #113, commit 9cbd8d6) landed a
# whole new MCP tool (name+description+inputSchema, ~1,830 chars) after the 117,067 ceiling
# was set for walk_in and nobody re-ran this specific test to catch it — the exact "a check
# nobody runs is a check that isn't there" failure this ratchet exists to prevent, now
# proven against itself. This session's own two docstring additions (launch()'s
# dormant_history field, graph_lint's severity/counts_by_severity fields) were trimmed to
# the category rule's lean end BEFORE raising — net effect vs the pre-existing 118,901 is
# +2 chars, not the source of the overage. Raised to the exact measured total, not a round
# number. Flagged to Thoth: this ratchet needs a standing habit (or a hook) that runs it on
# every MCP-tool-touching commit, not memory.
#
# RAISED, the authority-census fix (2026-08-02, operator's word via Thoth msg 3273):
# fold_agent/resolve_fold/fleet_reconcile/vacate_seat's docstrings now say what their code
# actually enforces (previously false or silent on who may fold an identity) — trimmed to
# the category rule's lean end first (all dated citations and provenance cut), landing at
# +744 chars over the prior ceiling. Load-bearing: a caller reading these four contracts
# needs to know the gate is real, not prose creep. Raised to the exact measured total.
# RAISED AT THE MERGE, 2026-08-02 (Thoth LXX, integrating four seat branches into composer
# at the operator's "merge and deploy"): 119,647 -> 119,953, +306. THE RATCHET CAUGHT A
# CROSS-BRANCH INTERACTION NO AUTHOR COULD HAVE SEEN. Khnum raised it to 119,647 on
# seat/khnum for the authority-gate docstrings (2fe5aeb..a2f1333); Sekhmet independently
# edited recall's own MCP tool doc on seat/sekhmet (c33d2a7). Neither branch could observe
# the other — THE MERGE IS THE FIRST MOMENT THE COMBINED SURFACE EXISTS, and this test is
# the only thing that looked. First real evidence that per-seat worktrees (bound tonight,
# 0 of 31 seats had ever used one) move the collision from the working tree to the merge,
# which is exactly where you want it and exactly where a ratchet must be watching.
# Both authors trimmed to the category rule's lean end before their own raises, so the
# residue is capability — four enforced authority gates plus a read path for addenda that
# were previously unordered and untimestamped — not prose creep. Exact measured total.
# STILL TRUE, STILL UNFIXED, NOW THREE FIRINGS IN ONE EVENING (two legitimate): nothing
# forces this test to run on an MCP-tool-touching commit; each catch happened only because
# someone ran the full suite by hand. It belongs in the gate hook, armed and worktree-aware
# as of 0146287. And note decision 94b85709: the EAGER-BOOT PREMISE THIS RATCHET DEFENDS IS
# FALSE FOR THIS FLEET — every worker defers and pays via ToolSearch instead — so the reason
# to keep a ceiling at all is a non-Claude-Code client, not us.
#
# RAISED, bind_seat_tree's own authority gap (2026-08-02, found scoping the seat-metadata
# merge, Thoth msg 3307): the verb carried NO authority language at all, claimed or enforced
# — worse than its rename_seat/set_seat_attended siblings, which at least overclaimed.
# `tree_cwd` is what `launch_seat` trusts as the code a relaunched seat executes, so this is
# judged gate-worthy on the same blast-radius reasoning already applied twice tonight, mirrors
# charter_for's actor-vs-manager_of_seat check exactly. Trimmed the MCP-facing docstring to
# the category rule's lean end first (dropped the dated citation and the "not a metadata gap"
# aside, kept the WHY implicit in "trusts as the code it executes"); 119,953 -> 120,081, +128.
# Exact measured total, not a round number.
#
# RAISED, consolidate's missing refusal (2026-08-03, Thoth LXXI's Phase 0 Tier 2 dispatch,
# msg 3354, his own priority pick of the three): the tool took NO parameters at all before
# this fix — not even `ctx` — an ungated whole-graph automatic merge sweep any mounted
# caller could trigger. Added `ctx` + an `_OPERATOR_ACTORS` check, the same convention
# mint_seat's cross-house guard already uses for a no-target global operation. Trimmed the
# MCP-facing docstring to the category rule's lean end first (dropped the dated citation and
# the mechanism-name aside, kept only what it refuses and why); 120,081 -> 120,268, +187.
# Exact measured total, not a round number.
#
# RAISED, resolve_thread's misleading error text (2026-08-03, same Tier 2 dispatch): the
# tool's own error text ("no open thread matches") always implied a status check that
# never ran — `_find_thread` matches by identity only. FIRST DRAFT of this fix built a
# gate (refuse/no-op a repeat close) reasoning it must be a mistake to guard against;
# MEASURED against test_two_strong_edges_still_report_strong (Phase 1a's own multi-witness
# design: a SECOND resolve_thread(artifact=...) call legitimately attaches a real closure
# witness after record_decision's `resolves=` closed a thread with only an `answers`
# edge) and found the gate broke it — a real premise refutation, not a hypothetical one.
# Corrected to cure B (honesty): re-resolving is allowed, error text dropped the
# misleading "open", and the receipt names plainly when a call landed on an already-
# resolved thread instead of looking identical to a fresh close. Trimmed the MCP-facing
# docstring to the category rule's lean end twice (once per draft); 120,532 -> 120,693,
# +161 net for the corrected, final design. Exact measured total, not a round number.
# RAISED, the silent-authority census's Tier 1 fixes (2026-08-03, Thoth's dispatch off
# decision 497a066a): bootstrap() and reap_stale_leases() both had a REAL blast radius --
# bootstrap() stamped every write with a hardcoded literal regardless of caller (untraceable
# even after the fact); reap_stale_leases(older_than_secs=0) force-released every held lease
# fleet-wide in one call, no gate needed, the parameter itself was the weapon. Trimmed both
# docstrings to the category rule's lean end across two passes (cut the redundant "unmounted
# callers still bootstrap, never gated" aside and the restated "rather than silently reaping
# less than asked") before raising; the residue is WHAT EACH NOW REFUSES AND WHY, the one
# thing this rule never cuts. 120,081 -> 120,282, +201. Exact measured total, not a round
# number.
TOOL_CONTRACT_CEILING_CHARS = 120_282


async def _measure_tool_contract() -> tuple[int, dict[str, int]]:
    """Returns (total_chars, {tool_name: its own name+description+inputSchema chars})."""
    from src import mcp_server as srv

    tools = await srv.mcp.list_tools()
    per_tool: dict[str, int] = {}
    for t in tools:
        per_tool[t.name] = (
            len(t.name) + len(t.description or "") + len(json.dumps(t.inputSchema))
        )
    return sum(per_tool.values()), per_tool


async def test_tool_contract_stays_under_the_ceiling() -> None:
    total, per_tool = await _measure_tool_contract()
    if total <= TOOL_CONTRACT_CEILING_CHARS:
        return
    heaviest = sorted(per_tool.items(), key=lambda kv: kv[1], reverse=True)[:10]
    named = ", ".join(f"{name}={chars}" for name, chars in heaviest)
    raise AssertionError(
        f"tool contract grew to {total} chars, over the ceiling of "
        f"{TOOL_CONTRACT_CEILING_CHARS} (task #129's ratchet, this file's own docstring) — "
        f"heaviest 10 tools right now: {named}. If you added prose, trim it under the "
        f"category rule; if the growth is genuinely load-bearing, raise the ceiling as a "
        f"deliberate act with a reason, not a reflex.")


async def test_tool_contract_has_the_expected_tool_count() -> None:
    """A cheap companion signal: if this number moves, a tool was added or removed — not
    what this ratchet polices, but worth knowing at a glance when the char total also
    moves, to tell 'one tool's prose grew' from 'the surface itself changed shape'. 99 -> 100
    (2026-08-02): amend_practice (task #113, commit 9cbd8d6) was never reflected here either
    — the same pre-existing drift the char ceiling above was just found and raised for."""
    _, per_tool = await _measure_tool_contract()
    assert len(per_tool) == 100
