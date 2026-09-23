"""TOOL CONTRACT DIET (this test suite polices osiris-mcp's own advertised tool
surface): every `@mcp.tool()` name, description, inputSchema and outputSchema is
downloaded by every connecting client before its first call. Every character trimmed
here is weight a client does not have to carry before it can act.

The measurement is the live, in-process tool registration, not a docstring grep:
`t.name` / `t.description` / `t.inputSchema` / `t.outputSchema` come from
`mcp.list_tools()` on the actual FastMCP server object this module builds. A raw
source-text scan under-counts, because FastMCP's own description rendering and the
generated JSON schemas add weight that does not live in any docstring. No live deploy
or network round trip is needed, so this stays a plain, offline pytest.

The ceiling is a maximum, not an exact match: a JSON schema's serialization can shift
by a few characters for reasons unrelated to content (a library version bump reordering
fields, for example), so a small round margin absorbs serialization noise without
hiding real regrowth.

This number moves down only by hand, never by recomputing it from the tree, matching
the same rule tests/test_render_hygiene.py's own allowlist follows: a ratchet that
derives its own ceiling from the tree is not a ratchet, it is a thermometer. If this
test fails because you added prose to a tool docstring or a new tool, first trim under
the category rule (keep what the verb does, what its arguments mean, what it refuses
and why, what it returns, and the trap that makes callers get it wrong; cut
restated schema, background and citations); if the remaining growth is genuinely
load-bearing, raise the ceiling as a deliberate, justified act, never as a reflex to
make a failing test go away.

Both constants below have moved many times, as tools were added, retired, folded into
shared dispatchers with hidden deprecated aliases, or had a docstring trimmed and then
regrown for a real new capability. When two branches each raise the same constant
independently from the same base, neither branch's own number is correct for the
merged tree, and summing the two deltas is also wrong: the real combined surface does
not exist until both changes are present together, so the only correct resolution is a
fresh measurement against the merged tree. A repo-registered merge driver
(scripts/reconcile_tool_contract_ceiling.py) automates exactly that reconciliation for
both constants when it recognizes the collision shape; when it cannot (for example
when only one branch touched this file at all, so there is no textual conflict to
resolve), whoever runs the full test suite against the merged tree is the one who
catches it, by design, since no single branch's own gate law can see a surface that
does not exist until the merge.
"""
from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

# This ceiling has been raised many times as genuinely new tools and parameters
# shipped (repair and backfill verbs, object-type dispatchers, read and write doors for
# new object types), each time only after the touched docstring was trimmed under the
# category rule above, and raised to the exact measured total, never a round number. It
# has also been lowered a few times, when tools were retired or several tools were
# folded into one dispatcher with hidden deprecated aliases, since that is a real
# shrink in the surface rather than prose trimming. Every entry in that history was an
# honest, deliberate measurement; none of it is repeated here play by play, since the
# rule that governs the next raise is the one stated in the module docstring above, not
# any one past entry.

def _tool_chars(t: Any) -> int:
    """One tool's own wire cost: name + description + inputSchema + outputSchema, all
    four fields a connecting client actually receives. `outputSchema`
    is None for a tool FastMCP couldn't derive one for; never counted when absent."""
    total = len(t.name) + len(t.description or "") + len(json.dumps(t.inputSchema))
    if t.outputSchema is not None:
        total += len(json.dumps(t.outputSchema))
    return total
# See the note above this file's own module docstring: this constant is measured
# against the live tool registration and only ever moves by hand, to the exact new
# total, after the triggering docstring has first been trimmed under the category
# rule.
TOOL_CONTRACT_EXPECTED_COUNT = 88
# Every past raise or lowering of the two constants below is real history, tracked
# in the commit log, not repeated here as an in-file changelog. See the module
# docstring above for the rule that governs the next change.
TOOL_CONTRACT_CEILING_CHARS = 140340

def test_ceiling_has_exactly_one_executable_assignment() -> None:
    """THE RATCHET'S OWN GUARD. This file used to carry every historical
    `TOOL_CONTRACT_CEILING_CHARS = N` as an EXECUTABLE line, so a three-way merge that kept an
    ancestor's line as the last assignment could silently revert the ceiling, since git had
    no way to tell that later assignment was meant to replace the earlier one. History moved
    to comments; a second executable assignment fails here before it can win a merge."""
    import re
    from pathlib import Path
    src = Path(__file__).read_text().split("\n")
    hits = [i + 1 for i, line in enumerate(src)
            if re.match(r"^TOOL_CONTRACT_CEILING_CHARS\s*=", line)]
    assert hits == [hits[0]] and len(hits) == 1, f"executable ceiling assignments at {hits}"


async def _measure_tool_contract() -> tuple[int, dict[str, int]]:
    """Returns (total_chars, {tool_name: its own wire chars}); see `_tool_chars`."""
    from src import mcp_server as srv

    tools = await srv.mcp.list_tools()
    per_tool = {t.name: _tool_chars(t) for t in tools}
    return sum(per_tool.values()), per_tool


def test_tool_chars_counts_outputschema_not_just_the_original_three_fields() -> None:
    """NEGATIVE CONTROL: before this fix, the per-tool sum
    (then inlined in `_measure_tool_contract`) counted only name+description+inputSchema,
    so two tools differing only in outputSchema measured identically, a real fleet-wide
    undercount found by comparing the live deployed server against this ratchet's own
    in-process measurement several independent ways. Before the fix, `_tool_chars`
    did not exist at all, confirmed failing via a clean checkout (AttributeError, not a
    semantic pass)."""
    base = {"name": "t", "description": "d", "inputSchema": {"type": "object"}}
    without_output = SimpleNamespace(outputSchema=None, **base)
    with_output = SimpleNamespace(
        outputSchema={"type": "object", "title": "TOutput"}, **base)
    assert _tool_chars(with_output) > _tool_chars(without_output)
    assert _tool_chars(with_output) - _tool_chars(without_output) == len(
        json.dumps(with_output.outputSchema))


async def test_tool_contract_stays_under_the_ceiling() -> None:
    total, per_tool = await _measure_tool_contract()
    if total <= TOOL_CONTRACT_CEILING_CHARS:
        return
    heaviest = sorted(per_tool.items(), key=lambda kv: kv[1], reverse=True)[:10]
    named = ", ".join(f"{name}={chars}" for name, chars in heaviest)
    raise AssertionError(
        f"tool contract grew to {total} chars, over the ceiling of "
        f"{TOOL_CONTRACT_CEILING_CHARS} (this file's own ratchet, see its module docstring). "
        f"heaviest 10 tools right now: {named}. If you added prose, trim it under the "
        f"category rule; if the growth is genuinely load-bearing, raise the ceiling as a "
        f"deliberate act with a reason, not a reflex.")


async def test_tool_contract_has_the_expected_tool_count() -> None:
    """A cheap companion signal: if this number moves, a tool was added or removed, which
    is not what this ratchet polices but is worth knowing at a glance alongside the char
    total, to tell "one tool's prose grew" from "the surface itself changed shape." This
    count has moved many times: new tools added for genuinely new capabilities (repair and
    backfill verbs, object-type dispatchers, read and write doors for object types that had
    none before), and lowered a few times when several tools were folded into one shared
    dispatcher with hidden, still-callable, deprecated aliases. Several raises went
    unrecorded in an earlier version of this docstring for a stretch of commits; rather
    than reconstruct that history from the commit log after the fact, it is simply not
    backfilled here. When two branches each raise this constant from the same base, neither
    branch's own number is correct for the merged tree, and the two are never picked
    between or averaged; the only correct resolution is a fresh measurement against the
    merged tree, matching the rule the character ceiling above follows."""
    _, per_tool = await _measure_tool_contract()
    # This value is hoisted into a named constant (TOOL_CONTRACT_EXPECTED_COUNT) rather
    # than an inline literal so a merge driver can reconcile it automatically when two
    # branches each raise it independently, the same mechanism the character ceiling above
    # uses. Recorded history of individual raises lives in the module docstring's general
    # rule rather than as a per-tool changelog here.
    assert len(per_tool) == TOOL_CONTRACT_EXPECTED_COUNT
