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

# Measured after #112's walk_in build (this commit): 117,067 chars exactly (was 115,146,
# task #103's tree_cwd). Raised deliberately, not a reflex — walk_in's docstring was
# already trimmed to the category rule's lean end (903 chars, the deep rationale moved to
# src/orchestrator/walkin.py's own module docstring, unmeasured by this ratchet) before
# raising; the remaining growth is the tool's real 8-parameter inputSchema, irreducible
# without dropping a parameter the tool actually needs. Flagged to Thoth for review.
TOOL_CONTRACT_CEILING_CHARS = 117_067


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
    moves, to tell 'one tool's prose grew' from 'the surface itself changed shape'."""
    _, per_tool = await _measure_tool_contract()
    assert len(per_tool) == 99
