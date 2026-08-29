"""THE TOOL-CONTRACT RATCHET (task #129, Thoth's ruling 1c414054): osiris-mcp's own
advertised tool surface — every `@mcp.tool()` NAME + DESCRIPTION + inputSchema — is paid by
every client eagerly, at connect time, before its first act. Measured before any cut: 97
tools, names 1,109 + descriptions ~85,988 + inputSchemas ~32,130 chars ~= 119,227 chars total
(~30k tokens). Same mechanism as #125's frozen tool-list index: too big to ship eagerly ->
deferred -> stale cache. Every char cut here is pressure off the thing that freezes.

QUALIFIED FOR THIS FLEET (decision 94b85709, task #148's harness-boundary survey): the
eager-boot premise above is no longer true FOR US — every worker in this fleet defers tool
resolution and pays via ToolSearch instead, so the connect-time cost this ratchet was built
to police doesn't land on our own clients. The ceiling stays armed for a non-Claude-Code
client that might still boot eagerly, not for this fleet's own connections.

THE MEASUREMENT IS THE LIVE, IN-PROCESS TOOL REGISTRATION, NOT A DOCSTRING GREP: `t.name` /
`t.description` / `t.inputSchema` / `t.outputSchema` come from `mcp.list_tools()` on the
actual FastMCP server object this module builds (json.dumps on the two schema dicts because
that is how each travels the wire). A raw `ast.get_docstring` scan under-counts: FastMCP's
own description rendering and the JSON schemas (NOT docstrings at all — they come from the
function's type hints and Field() defaults) both add weight a source-only scan would miss
entirely. No live deploy or network round-trip needed (list_tools() runs against the local
server object) — this stays a plain, offline pytest.

FOUR FIELDS, NOT THREE (2026-08-03, decision 553b5173) — this docstring itself used to claim
"the exact three fields a connecting client receives", and that claim was FALSE: verified
against the live deployed server three independent ways (real SDK round-trip, raw HTTP
JSON-RPC bytes, a bracket-matched substring off the raw wire text, all three agreeing),
every tool ships a FOURTH field, `outputSchema` (FastMCP auto-generates one per tool from
its Python return-type annotation — the `structured_output` param on `@mcp.tool()`, never
set anywhere in this codebase, defaults to auto-detect). That field was invisible to this
ratchet by construction, not drift, the same shape as `consolidate()`'s own blind spot in
the silent-authority census (decision 497a066a): a check that reads green while an entire
category sits outside its field of view. In-process vs wire is NOT the gap for the three
original fields — those two are identical, verified the same three ways (119,810 chars,
both sides) — the gap was fields-measured vs fields-shipped.

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
from types import SimpleNamespace
from typing import Any

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
# RAISED, phase 6 of the naming sweep (4dd526fe/aed9d4c1eb43, Thoth DM 3436): three verbs
# renamed for the intent-search axis after a control-validated live ToolSearch measurement
# showed their old names failing it — lap->provenance, doors->whois, dim->moot_brief. The
# growth is the names themselves (longer, literal words a caller's own phrasing actually
# contains, chosen FOR ranking, not decoration) plus the sibling references that had to
# follow (lift()'s own docstring names whois() three times). Trimmed to the category rule's
# lean end first (dropped provenance()'s and moot_brief()'s self-referential restatement of
# their own new name in running prose, e.g. "this shows" not "provenance shows"); residual
# growth is the two 7-char-longer names themselves, irreducible without cutting the rename's
# own point. 120,081 -> 120,099, +18. Exact measured total, not a round number.
# LOWERED, the merge/unmerge collapse (2026-08-03, operator's ruling 31c02dca, branched
# additively from composer at 88e09cf per Thoth's own merge plan — this file's prior
# entries above reflect composer's state, not this seat's own Phase 0 Tier 2 branch,
# which lands separately and is not yet folded in here): four tools (fold_agent,
# unfold_agent, fold_seat, fold_project) retired in favor of two (merge, unmerge) — a
# real reduction in the advertised surface, not prose trimming, so the ceiling moves DOWN
# to track it rather than leaving slack a future regrowth could hide in. 120,081 ->
# 118,940, -1,141. 100 -> 98 tools. Measured, reported — per the operator's own
# instruction on this exact build: NEVER offered as the reason to collapse the three
# folds (findability and parity were; this number is only the honest side effect of two
# fewer doors). Exact measured total, not a round number.
#
# RESOLVED AT A FOUR-WAY MERGE, 2026-08-03 (Thoth LXXI, integrating the whole night's work
# at the operator's "deploy all now"). FOUR branches each raised this ceiling independently
# and NOT ONE COULD SEE THE OTHER THREE: khnum-phase0t2 -> 120,693 (Phase 0 Tier 2's three
# verbs), seat/imhotep -> 120,282 (the silent-authority census's Tier 1), seat/seshat ->
# 120,099 (lap/doors/dim renamed to provenance/whois/moot_brief), khnum-mergeunmerge ->
# 118,940 (the fold collapse, forked from composer BEFORE any of the other three landed).
# Every one of those numbers was honestly measured and every one is WRONG for the combined
# tree. THE ONLY MOMENT THE REAL SURFACE EXISTS IS THE MERGE, and this test is again the
# only thing that looked. Fourth firing in two evenings; second time it caught a cross-branch
# interaction no author could have seen; first time across four branches at once.
#
# TRUE COMBINED, measured on the merged tree, not derived from any branch: 98 tools,
# 119,810 chars. LOWER than three of the four branch numbers, because merge/unmerge retires
# four verbs (fold_agent/fold_seat/fold_project/unfold_agent) and adds two, and that -2 net
# more than absorbs the three fix branches' additions. The session opened at 120,081 and
# closes at 119,810 — DOWN 271 despite shipping four authority fixes, three renames and a
# collapse. REPORTED, NEVER ARGUED FROM: operator's ruling 31c02dca is explicit that a
# collapse is justified by findability and symmetry and NEVER by size. This number is an
# observation. It is not why any of this was built and must never be cited as though it were.
#
# RAISED, a FOURTH FIELD the ratchet never measured (2026-08-03, task #129's re-scope,
# Thoth DM 3505/3513, decision 553b5173): every tool ships an `outputSchema` (FastMCP
# auto-generates one per tool from its Python return-type annotation) that this ratchet's
# three-field sum — name+description+inputSchema — never counted, BY CONSTRUCTION, not
# drift; the same blind-spot shape as `consolidate()`'s in the silent-authority census
# (decision 497a066a), a check reading green while a whole category sat outside its view.
# Verified against the live deployed server three independent ways (real SDK round-trip,
# raw HTTP JSON-RPC bytes, a bracket-matched substring off the raw wire text) before
# touching this file — for the ORIGINAL three fields, in-process and wire are identical
# (119,810 both sides); the gap was never source-vs-wire, it was fields-measured-vs-
# fields-shipped. This is a CORRECTNESS fix, defensible on its own — the ratchet was
# silently guarding 93% of the real surface — not a size argument (31c02dca): 119,810 ->
# 128,672, +8,862, all of it outputSchema, none of it prose. Exact measured total, not a
# round number.


def _tool_chars(t: Any) -> int:
    """One tool's own wire cost: name + description + inputSchema + outputSchema — all
    FOUR fields a connecting client actually receives (decision 553b5173). `outputSchema`
    is None for a tool FastMCP couldn't derive one for; never counted when absent."""
    total = len(t.name) + len(t.description or "") + len(json.dumps(t.inputSchema))
    if t.outputSchema is not None:
        total += len(json.dumps(t.outputSchema))
    return total
# RAISED, reconcile_merge (2026-08-03, task #127, Thoth msg 3504): a genuinely NEW
# capability, not prose creep — the repair door task #127 asked for existed for NO type
# before this (reconcile_project_fold shipped #127's own P0 half but was never given an
# MCP tool of its own, a doorless verb the same class as b8654e4c; Agent and Seat had no
# repair at all). Trimmed the MCP-facing docstring to the category rule's lean end FIRST
# (cut the "first ever exposure"/provenance framing and a restatement of merge's own
# idempotent-by-refusal contrast, kept only what it refuses, why, and the unmerge-then-
# remerge trap) — 1,535 -> 1,128 chars for the tool itself before raising anything.
# 119,810 -> 120,938, +1,128. 98 -> 99 tools. Exact measured total, not a round number.
# RAISED AGAIN, the is_handoff read-receipt redesign (2026-08-03, operator ruling): a new
# tool, `ack_handoff` (2,181 chars — the only thing that retires a live handoff now, replacing
# Thoth DM 3355's write-triggered version), plus a rewritten `settle()` section explaining the
# supersession. Trimmed both docstrings to the category rule's lean end FIRST (ack_handoff cut
# from 3,098 to 2,181 chars, settle's new section cut by more than half) before raising this —
# the remaining growth is a genuinely new capability, not unpruned prose. 120,081 -> 122,936,
# +2,855. Exact measured total, not a round number.
#
# RESOLVED AT A FOUR-WAY MERGE, 2026-08-03 (Thoth LXXI, second such merge in one session).
# FOUR branches, FOUR ceilings, every one honestly measured and every one wrong for the
# combined tree: seat/imhotep 128,672 (the outputSchema fix, 98 tools), khnum-reconcile127
# 120,938 (reconcile_merge, +1 tool, measured with the OLD three-field formula because it
# forked before the fix), seat/sekhmet 122,936 (ack_handoff, also pre-fix formula),
# sekhmet-task122 119,810 (untouched). TWO OF THOSE NUMBERS WERE COMPUTED BY A FORMULA THAT
# WAS ITSELF WRONG — the branches forked before outputSchema was known to exist.
#
# TRUE COMBINED, measured by THIS FILE'S OWN _tool_chars on the merged tree: 100 tools,
# 132,836 chars. That is +4,164 over Imhotep's 98-tool four-field baseline, for exactly two
# new verbs (reconcile_merge, ack_handoff) — proportionate, and NOT prose creep.
#
# NOTE FOR ANYONE RE-DERIVING THIS: an ad-hoc script using json.dumps with compact separators
# read 129,245 on the same tree. THE DIFFERENCE IS SERIALIZATION, NOT SURFACE. Only this
# file's own measurement is authoritative for this ceiling, because only it is what the
# ratchet actually compares against. Do not set this constant from any other instrument.
#
# REPORTED, NEVER ARGUED FROM (operator ruling 31c02dca): this number exists to catch
# regrowth. It is not a justification for any build and must never be cited as one.
#
# RAISED, task #117's urgent record_decision fixes (2026-08-03, Thoth msg 3524): no new
# tool, same 100 — pure docstring growth, so scrutinized harder than a new-verb raise.
# Trimmed three dated ("task #117") citations from the live docstring first (the WHY
# survives without the ticket number; category rule: cut provenance, keep refusals).
# The residue is load-bearing: callers now need to know (a) supersedes/implements/
# refutes/confirms no longer accept a free-text/prose match — a real behavior change,
# not prose — and (b) ANY error on this call, including a dropped connection with no
# response at all, is safe to retry (task #117 point 2: the same failure string
# covered "written, you didn't hear back" and "never written," and a caller could not
# tell them apart without this guarantee). 132,836 -> 133,621, +785. Exact measured
# total via this file's own `_measure_tool_contract`, not a round number.
#
# RAISED, Thoth's naming-pattern falsification test (2026-08-03, DM 3541, reply to Seshat's
# control-validated phase-6 sweep, decision e9ad0cc8): provenance/whois/moot_brief all failed
# intent-search on their current bare-noun names (0/3, 3/3-miss, 2/3-miss respectively).
# Re-proposed as verb+self-typing-target per design law 3d4a792e — NOT yet final, Thoth's own
# words ("candidates to beat, not to adopt") — built and gated so the pattern can be tested
# empirically post-deploy, the same protocol the original renames used.
# provenance->trace_evidence (888->900, +12), whois->identify_agent (757->784, +27, also
# fixing lift()'s own three cross-references to the old name), moot_brief->dismiss_brief
# (729->738, +9). SAME DISPATCH also trimmed merge's docstring (2025->1998, -27): it named
# `unmerge` and said "reversible via unmerge" in its own prose — Seshat's measurement showed
# this was CANNIBALIZING unmerge's own findability ("undo a merge that turned out wrong"
# ranked merge #1 and unmerge not at all, even at max_results=20). Net: 133,621 -> 133,673,
# +52, no tool added or removed (100 -> 100, a rename ratchet). Exact measured total via this
# file's own `_measure_tool_contract`, not a round number.
#
# RAISED, invalidate_works_in (2026-08-03, task #128 piece 4, thread 8640a625/decision
# fce39baa): a genuinely NEW capability — unpeer heals peer_of, detach_seat heals
# managed_by, nothing healed works_in before this, so a live agent's own duplicate edge
# (John XVII's own specimen) had no repair path except raw SQL. Trimmed the MCP-facing
# docstring to the category rule's lean end FIRST (cut the thread-id citation and the
# fork-story provenance, kept only what it does, the self-scoping, and the refusals) —
# graph_lint's own docstring addition (the new duplicate-works-in check) trimmed the same
# way. 133,673 -> 135,077, +1,404. 100 -> 101 tools. Exact measured total via this file's
# own `_measure_tool_contract`, not a round number.
#
# RAISED TWICE IN ONE MERGE, and this comment block is the resolution of a real conflict —
# two workers raised this constant from the SAME base (135,077) on parallel branches, to
# 135,292 and 135,189 respectively. NEITHER number is correct for the merged tree: they are
# disjoint docstring growth, so the combined total is larger than either. The conflict was
# resolved by RE-MEASURING via this file's own `_measure_tool_contract` on the merged tree,
# never by taking the larger of the two. Both causes below; both are true.
#
# (a) succession_chain gains `session` (ruling 7fa4b599's own named additive step, task
# #135/#136 point 4): the operator asked how to reliably tell the latest transcript; the
# record already existed (mount() asserts it on every generation's own Agent object) and
# the walker just never read it — one subquery, no new tool, no new table. +215 alone.
#
# (b) the cache-coherence follow-on (thread 8640a625 / decision 4001f6d1):
# invalidate_works_in and correct_house now patch every live cached identity in the
# caller's own lineage after a successful write, closing the gap that made John's own fix
# appear to take effect three steps late (rebind_seat's own docstring named the trap first;
# neither of these two followed it). One sentence added to correct_house's own MCP-facing
# docstring; invalidate_works_in's own docstring untouched, only its code grew. +112 alone.
#
# 135,077 -> 135,404. 101 -> 101 tools, no tool added or removed.
#
# THIS COLLISION IS NOW STRUCTURAL, NOT JUST DOCUMENTED (dispatch 26686b77, Thoth msg
# 3658): the merge driver registered for this file (.gitattributes + self-installed by
# tests/conftest.py's pytest_configure, see scripts/reconcile_tool_contract_ceiling.py's
# own docstring for the full reasoning) re-measures this constant on the merged tree
# whenever two branches raise it from the same base — the exact fix above, automated at
# the collision point, never by comparing the two conflicting numbers. If you land here
# because a raw conflict survived anyway (the driver couldn't recognize the shape, or
# isn't registered — run pytest once, which installs it), resolve it BY HAND the same
# way: `uv run python scripts/measure_tool_contract.py`, never by picking the larger of
# the two values.
#
# RAISED, closing an MCP/CLI parameter-asymmetry gap Thoth's own measurement found
# (2026-08-04, dispatch 3678 addendum, msg 3681): `osiris mint-seat`'s --adopt/--force are
# parameters of the shared mintseat.mint_seat function that the MCP tool's own signature
# cannot reach at all — not stated anywhere as deliberate, so a reader had no way to tell
# "omitted on purpose" from "a real gap." Measured which it is rather than guessing: an
# agent's ordinary mint (extending itself with a fresh specialist, per this tool's own
# docstring) has no evident need for either — --adopt's whole effect is refusing instead
# of the SAME auto-adopt-on-exact-match this tool already does unconditionally, and
# --force exists to override the near-miss-twin safety guard, a deliberate human
# judgment call, not a coordinator's routine act. One sentence added stating this
# explicitly, so nobody re-opens it as an oversight; no new parameter, no new tool.
# 135,404 -> 135,689, +285. Exact measured total via this file's own
# `_measure_tool_contract`, not a round number.
#
# RAISED AT AN INTEGRATION MERGE, and this one is worth reading because THE DRIVER DID NOT
# FIRE AND WAS NOT SUPPOSED TO. The collision documented above is two branches raising this
# constant from the same base; the driver handles that shape. THIS collision is different
# and the driver is structurally blind to it: NEITHER branch touched this file at all.
# Khnum's open_thread(resolves=...) (dace6b8) grew open_thread's own MCP docstring, Sekhmet's
# climb-stop fix (28a2fa3) landed in the same merge, and the ceiling was breached by growth
# in a file with no conflict to resolve. A merge driver keyed to this file's conflicts can
# never see that.
#
# WHY NEITHER WORKER COULD HAVE CAUGHT IT, which is the more useful half: the gate law is
# `pytest <touched files>`, and this ratchet is a GLOBAL invariant that no touched-file set
# ever includes. Both workers gated correctly, both were green, and the breach was real.
# A per-lane gate cannot distinguish "my change is safe" from "I did not run the check that
# would have caught it" — ruling 60bc15db's exact shape, found in our own gate law.
# THE RATCHET IS THEREFORE THE INTEGRATOR'S LINE, not the worker's: only whoever runs the
# FULL suite on the merged tree can see it, and that is Thoth at merge time.
#
# Resolved the only correct way: RE-MEASURED on the merged tree via
# `_measure_tool_contract`, exact total, never rounded and never inferred from the two
# branches' own numbers (neither of which existed — see above).
# 135,689 -> 136,661, +972. 101 -> 101 tools, no tool added or removed.
#
# RAISED FOR A GENUINELY NEW TOOL — `roster` (task #140, Seshat, 25bf570): the graph-level
# "who owns this repo" a coordinator never had. Alfred XIII misrouted work to two different
# seats in one session because mount()'s LIVE co-agents was the only roster-shaped thing on
# offer and he read cold as vacant; the authoritative seat->repo map lived only in
# ~/.osiris/seats/*/.osiris ON DISK, which contradicted our own ad19a779 (the graph must be
# navigable by cheap MCP calls). roster costs 2,096 chars — the single largest deliberate
# addition since the ratchet was armed, and worth it.
#
# BOTH HALVES OF THE GUARD FIRED THIS TIME, which is the point of having two: the ceiling
# AND the tool-count assertion below (101 -> 102). A ceiling raise alone would have hidden a
# tool appearing; the count alone would have hidden docstring growth. Neither is redundant.
# 136,661 -> 138,757, +2,096. 101 -> 102 tools, exactly one added.
#
# RAISED AGAIN, SAME DAY, SECOND NEW TOOL — `wake_preflight` (task #156.4, Imhotep,
# b325a26): answers WHICH GATES WOULD REFUSE A WAKE *before* the attempt, read-only,
# reusing dispatch_dm's own gate functions verbatim rather than restating them. Built
# because the operator's `osiris launch metron` discovered eight refusals only by reading
# a wall of them afterwards; a pre-flight answer is the difference between a gate and an
# ambush. Costs 1,028 chars — the cheapest tool added since the ratchet was armed.
# 138,757 -> 139,785, +1,028. 102 -> 103 tools, exactly one added.
#
# RAISED A THIRD TIME, NO NEW TOOL — two EXISTING tools grew a genuinely new capability
# each (task #149, Thoth DM 3847): record_decision's `content_landed` (a post-write
# read-back so a caller can tell "your rationale landed" from "it silently lost a
# tie-break," the specimen behind thread 20145def and Thoth's own "I had to go READ the
# object to find out" account) and run_composition's `offset` (real pagination — `take`
# alone could only ever show a list's first N, forever; the actual workaround was ~20
# hand-built narrower compositions, confirmed live against the 603-item open-threads
# composition). Trimmed the added prose twice before raising this (141,048 -> 140,373);
# the remainder is the `offset` parameter's own inputSchema field plus the minimal text
# either new field needs to be discoverable at all — load-bearing, not reflex.
# 139,785 -> 140,373, +588. 103 -> 103 tools, none added — two tools' own contracts grew.
# RAISED AT A FOUR-BRANCH MERGE, AND THIS IS THE THIRD AND CLEANEST PROOF THAT THIS LINE
# BELONGS TO THE INTEGRATOR, NOT THE AUTHOR (45e72476). Khnum raised it to 140,373 on
# khnum-receipt-honesty — correctly, for his branch: he trimmed his own prose twice first
# (141,048 -> 140,373) and raised only for the load-bearing remainder, exactly the discipline
# this file asks for. HIS NUMBER WAS STILL WRONG FOR main, by +671, because three other
# branches (sekhmet-resolver, seshat-broadcast, imhotep-bootdelivery) grew docstrings he
# could not see. No author can measure a surface that does not exist until the merge.
# 140,373 -> 141,044. 103 -> 103 tools, no tool added or removed.
# RAISED AT AN EIGHT-BRANCH MERGE, AND THIS IS THE FOURTH PROOF OF 45e72476 IN A SINGLE DAY.
# TWO NEW TOOLS, both load-bearing, both the direct repayment of debts this house measured
# the same evening:
#   task_sync_reconcile (~1,692) — the DOOR onto task_sync.py, 503 loc of implementation and
#     471 loc of tests that had run live exactly once (2026-08-01, 41 obligation threads
#     minted) and then had no caller at all. Khnum trimmed its docstring under the category
#     rule BEFORE handing me the number (143,577 -> 142,736): provenance and dated citations
#     cut, behaviour/refusals/returns kept. That trim order is the standing one.
#   tree_ledger (~4,144) — the audit instrument that, with zero prior knowledge encoded,
#     independently flagged repo:seats and repo:code as phantom-suspect (the exact two found
#     by eye that night) AND found two nobody had reached. Its contract is heavy because it
#     declares its own verdict vocabulary and its own blind spots; that text is the honesty,
#     not the fat.
# The remainder (~730) is Seshat's triage docstring correction — the `contradicted` bucket was
# implemented and documented in compositions.py and ABSENT from the tool contract an agent
# actually reads, which is why this house hand-rolled SQL for a question triage was already
# answering — plus receipt text from the mount-identity and charter work.
# AND THE PROOF ITSELF: Khnum measured 142,736/104 on khnum-task-sync-verb and handed me the
# number instead of raising the line. Correct for his branch. WRONG FOR main BY +4,874,
# because seshat-measure added a whole tool and two more branches grew receipts he could not
# see. Third author in a row to decline this line correctly; third time the author's honest
# number could not have been right. No author can measure a surface that does not exist
# until the merge.
# 155,414 -> 156,685. 110 -> 111 tools: create_project (#139) is the new one;
# the rest is tool_traffic gaining its per-caller cut (#170).
# 163,475 -> 163,650 (2026-08-15, msg 4679). TOOL COUNT UNCHANGED at 115 — this is +173
# chars of prose on ONE existing tool, and it is the integrator's line so the integrator
# raises it (45e72476). Imhotep flagged the overrun and refused to raise it himself, which
# is the rule working. THE GROWTH IS ack_handoff's `resolved` FIELD: acking a handoff left
# the Thread's own status 'open' forever, so a receipt saying "acknowledged: true" could not
# distinguish "acked" from "acked AND retired." That is 42176e16 at a receipt boundary — the
# defect family this house spent the week closing — and the field is worthless if the
# docstring does not say the caller may read it. Prose that makes a new honesty field
# LEGIBLE is load-bearing; prose that re-explains an unchanged one is the diet's target.
# 163,650 -> 164,250 (2026-08-16, msg 4712). TOOL COUNT UNCHANGED at 115 — +580 chars on
# record_decision, already the heaviest tool, for ONE new parameter: `bears_on`. Raised on
# the rule written into the entry above, applied to a harder case: this is a NEW DOOR, and
# a door needs its LAW stated or callers invent one. Two things in that prose are load-
# bearing and I told the author to write them before he wrote them: the ADDRESSING LAW
# (uuid/canonical/8-char only, never a prose match — the same law resolves/supersedes
# follow, because an addressing act must name its target exactly or refuse), and the
# RECEIPT-HONESTY contract (the echoed thread summary that makes a mis-citation visible in
# the same turn, and new_link=false on an already-linked pair). Cut either and the field
# still works while becoming unreadable — which is the failure this whole file exists to
# prevent, one layer up from tool count. THE AUTHOR TRIMMED TWICE, 1,270 over -> 580, and
# named exactly what further cutting would cost; he did not gut it to sneak under, and he
# did not raise it himself. That is 45e72476 working in both directions.
# 164,250 -> 165,600. 115 -> 116 tools (2026-08-16, msg 4766). THE FIRST RAISE OF THIS
# REIGN FOR A GENUINELY NEW TOOL: `correct_pin_value`, the door that did not exist. Raised
# on the rule the entry above established, applied to its intended case — a NEW DOOR NEEDS
# ITS LAW STATED OR CALLERS INVENT ONE.
# THE AUTHOR'S OWN NUMBER WAS WRONG AND COULD NOT HAVE BEEN RIGHT — he measured +1,462 off
# a base of 163,475, having branched from cb7c85b, an ANCESTOR of main rather than its tip.
# The real delta against main is +1,269. Fourth proof of the comment above: NO AUTHOR CAN
# MEASURE A SURFACE THAT DOES NOT EXIST UNTIL THE MERGE. He flagged and refused to raise,
# which is the rule working; the wrongness of his figure is WHY the rule exists, not a
# failure of his.
# WHAT THE 1,269 BUYS, every clause a law a caller must know or will violate:
# (1) THE NAMED EXCEPTION — write_pin_additions never overwrites an existing key BY DESIGN;
# this one does. A caller ignorant of that reaches for the wrong verb or fears the right
# one. (2) SELF-SCOPED — always the caller's OWN office via held_seat, and it takes NO path
# parameter at all, so the authority boundary is enforced by the signature AND stated in the
# prose. (3) `reason` REQUIRED — the correction is auditable or it does not happen.
# (4) THE FOUR REFUSALS enumerated, so a caller designs against them instead of discovering
# them. (5) revert_pin_write NAMED as the undo. Cut any one and the verb still works while
# becoming a thing callers use wrongly on a file that carries identity.
# WHY IT WAS BUILT AT ALL, the part worth inheriting: TWO LIVE SEATS HIT THE MISSING DOOR IN
# ONE EVENING — henry (shellbiz) was told this verb was the supported fix, found nothing in
# his toolbelt, and hand-edited a file; Till (ramstein) had a correct, operator-authorized
# fix and could not perform it. A VERB WITH NO SURFACE IS NOT A VERB.
#
# 165,600 -> 166,900. 116 -> 117 tools (2026-08-16, Thoth LXXVI, one merge wave of four
# branches). TWO CONTRIBUTIONS, ONE MEASUREMENT: `list_assertions` (Sekhmet, dcfca4c) — the
# read door retire_assertion's `superseded_id` always needed and nothing ever exposed; the
# assertion row id was already IN current_assertions (SELECT a.*) and no reader had ever
# selected it, a pure SURFACE gap. Fifth-disease specimen (382067d9: authorized but
# unexecutable) closed by the smallest honest verb. Plus send()'s new paragraph (Seshat,
# e111e3b) stating that a DM or ask-graded broadcast now carries `prior_art` — the read-side
# hop the operator asked for by name ("why is read-the-graph a mail instruction and not
# architecture"), and a caller who does not know the receipt carries it will not look.
# THE AUTHORS' NUMBERS WERE WRONG AGAIN, AND SUMMED WRONG: +860 and +345 = +1,205; the real
# delta at the merged tip is +1,286. FIFTH PROOF of the comment above — this time neither
# author branched stale (both off 166f205, main's tip at the time); the surface still could
# not be measured until BOTH landed on one tree. Each flagged and refused to raise. The rule
# held; the integrator measured.
# SIXTH PROOF, same night: this author (Seshat) branched off cb7c85b — origin/main's tip,
# NOT local main's real tip (b6e50a9, three merge waves ahead) — and measured 163,475 ->
# 165,662, 115 -> 117 tools. Both numbers were wrong: the true prior ceiling was already
# 166,900/117 (the entry above), so his own delta double-counted list_assertions, which
# main already carried. Caught before merge (Thoth, msg 5000: "run git rev-parse main in
# your worktree AND the repo, report both" — the two agreed; the real gap was origin/main
# vs local main, not worktree staleness — only local main advances between milestone
# pushes, per house convention). Rebased onto b6e50a9, the list_assertions cherry-pick
# dropped as a duplicate, re-measured clean against the real tip: +1 tool only
# (reconcile_seat_identity, fe8ec7ff mechanism 3b, ruling df646654 — the self-service
# seat-identity heal replacing #157's four operator-authorized retire_assertion calls with
# one self-scoped call per seat). Raised to the exact measured total below.
# SEVENTH PROOF, same night: unwitnessed_spawns (obligation cabfb4b2, Ptah VII's rotten-
# apple report) — the self-audit "what is executing under my identity that I did not
# spawn," every live-registered spawned_by fact with no on-disk transcript ever confirming
# it, per ruling 7d6815bb's standard. Measured against the real tip alongside the sixth
# proof's own fix; +540 chars is the docstring's own honest caveat (Thoth's isSidechain
# hypothesis, msg 5008, tested against Ptah's real transcript and found not to apply — zero
# isSidechain:true lines anywhere in it — stated so a hit here is read as a lead, not a
# verdict, the same lesson Ptah's own retraction (msg 4993) already taught once).
# 169,775 -> 171,100. 119 -> 120 tools (2026-08-17, Thoth LXXVI heir viii, merge wave of
# four branches at 2212101). ONE NEW TOOL: backfill_agent_project_links (Sekhmet 2ac2344,
# thread 20af2c95) — the one-time backfill for works_in/governs edges stranded on off-head
# generations before the 08-04 write-side fix; existed and was unit-tested but had NO
# reachable door (fifth-ledger-disease: authorized-but-unexecutable). dry_run=True default,
# list-only; the bulk act stays the operator's, #150's shape. Sekhmet flagged "+~1.2k" and
# did not raise (practice 45e72476); measured at the merged tip: +1,242 chars, exact. The
# rest of the wave (Khnum's pytest live-DSN guard + remote_url duplicate auto-merge,
# Imhotep's heal guardrails, Seshat's receipt invariant) added no tools and no chars —
# Khnum's remote_url_duplicate_candidates is a deploy-time step, deliberately not a verb.
# 171,100 -> 171,450. 120 tools unchanged (2026-08-17, Thoth LXXVI heir viii, merge of
# Imhotep's 7612b18, thread 7304bfd8). NO NEW TOOL: +306 chars is mount()'s docstring
# explaining the new named state — `model` present when resolved, `model_unresolved` when
# not, never the string "unknown" — and that transcript_path is now consulted (it was
# captured by mount()/automount() but never threaded into identity_reading, the actual
# resolver; Ptah VII's bridge-fork specimen). Load-bearing under 7d6815bb: a caller has to
# be told a third state exists to branch on it. Imhotep flagged, did not raise (45e72476).
# 171,450 -> 173,000. 120 -> 121 tools (2026-08-17, Thoth LXXVI heir viii, merge of Seshat's
# 335bbfc, #157). ONE NEW TOOL: reconcile_seat_identity_third_party — mechanism 3 (fe8ec7ff)
# had only a self-service half; #157's four rows are OTHER seats' identities and the self-
# scoped verb refuses by construction (Seshat f78b41c8). Mirrors resync_seat_house_third_party
# exactly; `because` mandatory; contract test proves identical writes to the self-service
# path. Measured +1,821 chars at the merged tip; Seshat flagged, did not raise (45e72476).
# 173,000 -> 174,600 (measured 174,500 exact, +1,500 for rematerialize). 121 -> 122 tools
# (2026-08-17, Thoth LXXVI heir viii, merge of Imhotep's
# 9b5073c, #51 piece 2). ONE NEW TOOL: rematerialize — byte-for-byte transcript reconstruction from
# soul_lines with the hash chain verified while collecting (a break is a NAMED receipt, writes
# nothing); default dest = the session's recorded source_path so `claude --resume` on ANY host
# finds it; refuses to overwrite a live transcript (mtime newer than last ingest) unless force.
# CLI + MCP parity by construction. Imhotep flagged, did not raise (45e72476). Exact number
# measured at the merged tip and written below.
# ONE NEW TOOL: stale_current_flags (thread 09bde57e) — the read door for the
# is_current/supersedes kernel-integrity gap khepri's own live specimen surfaced:
# every row where is_current=true yet a real supersedes FK already excludes it.
# Seshat flagged, did not raise (matching the entry directly above). Measured
# 174,600 -> 175,309, 122 -> 123.
# 174,600 -> 175,400 (measured 175,309 exact). 122 -> 123 tools (2026-08-17, Thoth LXXVI
# heir viii, merge of Seshat's f7ed043, thread 09bde57e). ONE NEW TOOL: stale_current_flags —
# the read door onto "is_current=true rows a real supersedes FK already excludes"; its FIRST
# LIVE MEASUREMENT: 123,914 of 267,305 (46.4%) stale, 99.97% pre-0047 git/git-tree ingest —
# a migration-0047 backfill-completeness gap. Read-only, count + sample. Seshat flagged, did
# not raise (45e72476).
# 175,400 -> 176,400 (measured 176,307 exact). 123 -> 124 tools (2026-08-17, Thoth LXXVI
# heir viii, thread 09bde57e piece (c)+(d)). ONE NEW TOOL: repair_stale_current_flags — the
# backfill for stale_current_flags' own population: dry_run=True default (list-only, safe
# unmounted), dry_run=False is the operator's own mounted call, batched + idempotent UPDATE
# on assertions.is_current, same #150 "list-only default, execute is the operator's" shape.
# Load-bearing (piece (d) is the whole point of measuring the population in the first
# place) but matching the immediately preceding precedent's own judgment call: Seshat
# flagged, did not raise (45e72476).
# 175,400 -> 180,200 (measured 180,016 exact). 123 -> 128 tools (2026-08-17 ~23:30, Thoth
# LXXVI heir viii, five-branch wave at 408e8d2+): FIVE NEW TOOLS — restore_attribution
# (Imhotep 9e3db73, 3f7969a3 repair verb; live dry-run: 183 damaged ramstein edges, not 74),
# uningested_trees + ingest_project + ingest_project_third_party (Sekhmet abd0750, #41 as a
# self-healing mechanism: 48 of 60 projects have zero commits ingested), repair_stale_current_flags
# (Seshat e428dc1, 09bde57e (d); dry-run 123,914). Each flagged by its author, raised once here
# at the merged tip (45e72476).
# 180,200 -> 181,600 (measured 181,402 exact). 128 -> 129 tools (2026-08-18 ~00:55, Thoth
# LXXVII heir ix, #177 merge wave): ONE NEW TOOL — unwire_informs_fanout (Imhotep 0a05d2c,
# thread 5156: _wire_informs' cross-join repair verb, dry_run=True default; live dry-run 1020
# edges across 60 projects, execution operator-gated). Flagged by its author, raised once
# here at the merged tip (45e72476).
# 181,600 -> 182,000 (measured 181,843 exact). Tool count unchanged at 129 (2026-08-18 ~01:55,
# Thoth LXXVII heir ix, #172/#173a/#179 wave): NO NEW TOOL — fleet()'s docstring grew by the
# whisper_health field (Imhotep d72281c, #179: hook failures alarmed and read back). Flagged by
# its author ("~250 chars, not raised"), raised once here at the merged tip (45e72476).
# 182,000 -> 182,400 (measured 182,241 exact). 129 tools unchanged (2026-08-18 ~02:30, Thoth
# LXXVII heir ix, #174/#175/#180-piece-1 wave): NO NEW TOOL — fleet()'s ghost_gap went
# per-identity (false_live/false_dead, Khnum 18a5d81, #174) and the docstring grew with it.
# Flagged by its author, raised once here at the merged tip (45e72476).
# 182,400 -> 183,200 (measured 183,130 exact at the merged tip d65d396+84ab414; Seshat's
# own branch measured 182,732). 129 -> 130 tools (2026-08-18, Seshat XXXV,
# #178 piece c): ONE NEW TOOL — registry_census. The harness's own live-body list (`claude
# agents --json`), each row verified against /proc, reconciled against agent_mounts (the
# cache, never a second source of truth); `rowless` names the exact population #178's
# pieces (a)/(b) exist to close to zero. Load-bearing (this IS the piece Thoth dispatched),
# trimmed to the category rule's lean end first — flagged by its author, not raised beyond
# the exact measured total, matching the immediately preceding precedent (45e72476).
# 183,200 -> 183,600 (measured 183,571 exact). Tool count unchanged at 130 (2026-08-18
# ~04:40, Seshat XXXVI, thread 5256's next-lane dispatch): NO NEW TOOL — fleet() gained
# `harness_registry`, folding registry_census's own harness-vs-mount view in (occupancy AND
# identity, one call) with a #174-style `ghost_status` per body. Trimmed to the lean end
# first (two passes), then flagged by its author, not raised beyond the exact measured
# total, matching the immediately preceding precedent.
# 183,600 -> 184,400 (measured 184,220 exact). 130 tools unchanged (2026-08-18 ~05:20, Thoth
# LXXVII heir ix, #180 piece 2 merge): NO NEW TOOL — fleet()'s docstring grew with pool_health
# (pg_stat_activity by application_name + tx_total) and merged_into grouping (Khnum d061a5a).
# Flagged by its author, raised once here at the merged tip (45e72476).
# 184,400 -> 185,800 (measured 185,628 exact). 130 -> 131 tools (2026-08-18 ~04:35, Thoth
# LXXVII heir ix, #181 merge): ONE NEW TOOL — recover_harness_exchanges (Imhotep 9e09d30):
# dry-run-first recovery of the harness's own cross-session SendMessage traffic out of the
# soul-stored transcripts into harness_messages (migration 0051), so reasoning behind
# rulings never lives only in two jsonl files (Ptah 5260); plus fleet()'s per-seat
# osiris-vs-harness adoption share. Flagged by its author, raised once here at the merged
# tip (45e72476).
# 185,800 -> 187,200 (measured 186,895 exact). Tool count unchanged at 131 (2026-08-18 ~09:00,
# Thoth LXXVII, seshat-roster-review merge): roster()'s contract grew — repo= now distinguishes
# governed-vs-conflict and names near-misses, no-office is an honest null instead of a claim
# (Seshat XXIX 08-12 batch, accepted into a merge batch that never landed it — found by the
# merge-base sweep). Raised once here at the merged tip (45e72476).
# 187,200 -> 187,800 (measured 187,501 exact). 131 tools unchanged (2026-08-18 ~17:20, Thoth
# LXXVII, khnum-scale-envelope merge): fleet()'s pool_health contract grew a `caps` envelope
# (per-daemon pool cap / backends / utilization / max_connections headroom — the 1,000-worker
# arithmetic, decision 6a745efa). Raised once here at the merged tip (45e72476).
# 187,800 -> 190,400 (measured 190,072 exact). 131 -> 136 tools (2026-08-23, Thoth LXXXI,
# reconciling main after the operator's DeepSeek-Harness build): the five GRANULAR GETTERS
# (get_status 309, get_mail 301, get_thread_list 753, get_decision_list 485, graph_search 921
# = 2,769 chars) landed in 6a1dd99 WITHOUT this ceiling being moved — main shipped over the
# ratchet and only the merge gate would ever have caught it, which is exactly the failure
# mode thread 1a0f91bb is about. Raised HERE, at the merged tip, with the measurement, per
# the standing rule; the raise is not retroactive absolution for shipping through it.
# WHY THE GROWTH IS LOAD-BEARING, not prose: these decompose the orient() monolith (~59K
# chars of RESPONSE) into bounded, paginated reads a weaker/cheaper model can actually
# carry. 2,769 contract chars buying a cheap alternative to a 59K response is the trade the
# diet exists to make. Contract WITHOUT them measures 187,303 — under the old ceiling, so
# the getters account for the entire breach and nothing else drifted.
# NOTE for the next raise: sekhmet-wake-lifecycle already raised this to 189,400 on its own
# branch for stop(). That branch is unmerged; when it lands, re-measure at THAT merged tip
# rather than taking either number on faith.
# 187,800 -> 189,400 (measured 188,986 exact). 131 -> 132 tools (2026-08-19, Sekhmet,
# dispatch 5398 leg 2, ruling 94c2e7e8/b3ccd3f6): a genuinely NEW verb, `stop` — the
# process-lifecycle inverse of launch(), #156's own held half, finally ships. Not prose
# bloat on an existing tool; a new capability necessarily adds a new contract entry.
# Both docstrings touched this same dispatch (wake_preflight's fresh-heir-available
# status, stop() itself) were trimmed under the category rule first — this is the
# remainder after that, not a reflex.
# 190,400 -> 191,800 (measured 191,530 exact). 136 -> 137 tools (2026-08-24, Thoth LXXXI,
# THE MERGE TRAIN). CONFLICT RESOLVED BY MEASURING, NOT BY PICKING A SIDE: sekhmet-wake-
# lifecycle carried 189,400/132 for stop(); main carried 190,400/136 for the five granular
# getters. Both changelog entries above are KEPT — each explains real growth — but NEITHER
# number was correct at the merged tip, because neither branch could see the other's tools.
# Taking the larger would have been a guess that happened to be too small. Re-measure at
# the merged tip is the rule precisely because two honest raises still don't compose.
# 191,800 -> 192,981 (measured exact). 137 -> 138 tools (2026-08-27, Khnum, decision
# 49231693/operator ruling on the Reference-orphan trace): backfill_bootstrap_orphan_
# references — a genuinely NEW repair verb (the bootstrap_project door-gap's historical-
# damage half), same shape as unwire_informs_fanout's own repair door. Docstring trimmed
# under the category rule first (1,840 -> 1,451 chars for the tool itself); the remaining
# growth is the verb existing at all, not prose.
# 192,981 -> 193,507 (measured exact). Tool count unchanged (task #189, decision
# 7ea187b9): record_decision and open_thread each gained one new PARAMETER,
# `unlinked_because` — the declare-or-refuse gate's mandatory countable hatch, not
# decoration. Both docstrings were trimmed to one line each under the category rule
# first; the remaining growth is the two params' own wire schema entries plus that
# one line apiece, not prose left untrimmed.
# THE CEILING WAS RAISED THREE TIMES INDEPENDENTLY AND IS RECONCILED BY MEASUREMENT,
# NEVER BY PICKING THE LARGEST (2026-08-28, Thoth, merging Wave 2). Khnum's Lane 1,
# Imhotep's f1f14cb3 and Seshat's Lane A each added exactly ONE tool and each bumped
# this constant off ITS OWN BASE, so all three pre-merge values (194_532 / 194_353 /
# 195_627) are wrong for the merged tree and the true figure is none of them. A ratchet
# resolved by preference rather than by re-measurement is a ratchet that has stopped
# ratcheting — so this is re-measured against the merged tree, which is the same
# discipline the constant exists to enforce, applied to the constant itself.
# 2026-08-28, THREE-WAY MERGE RECONCILED BY MEASUREMENT (Thoth LXXXVII). Both sides were
# correct per-branch and BOTH ARE WRONG FOR THE MERGED TREE, which carries both changes:
#   Khnum  (Wave 4, thread 5ec2b82d): 196,635 — chars only, still 141 tools. get_thread_list
#          and get_decision_list each gained a one-line "charter-aware" note, both docstrings
#          trimmed twice under the category rule first.
#   Imhotep (Wave 4, thread e67fc338): 197,348 — 141 -> 142 tools. A genuinely new verb,
#          `retryable_abstentions`: the structurally-safe zero-candidate subset of
#          abstained_derivations, filtered in SQL so a 2+-candidate abstention can never
#          appear regardless of caller behaviour.
# The merged tree has BOTH the new tool AND both docstring notes, so its real cost is
# neither number. RE-MEASURED BELOW against the merged tree — never picked, never averaged.
# (A ratchet settled by preference has stopped ratcheting.)
# 196_473 -> 197_670 (measured exact). 141 -> 142 tools (2026-08-28, Sekhmet, thread
# e2326ab7, Wave 4 leg (b)): repair_stale_pile_summons — a genuinely new repair verb for
# the 2026-07-13 bulk-minted "DISPOSE OF YOUR MINER PILE" threads, whose frozen counts
# drift the moment their project's own pile is drained or judged. Docstring trimmed under
# the category rule first (1,673 -> 1,197 chars for the tool itself); the remaining
# growth is the verb existing at all, not prose.
# 197,510 -> 197,965 (measured exact). Tool count unchanged (2026-08-28, Imhotep, thread
# e05e439d, Soundwave XV's specimen 0c4dc7ce, operator ruling "same problem as 1" — build
# it): record_decision gained one new PARAMETER, `narrows` — a Decision->Decision edge
# that bounds an earlier ruling's scope without refuting or superseding it, non-burying
# by construction (mirrors `rediscovers`: no property write on either side). Docstring
# line trimmed to match the sibling params' own one-line shape first; the remaining
# growth is the new param's own wire schema entry plus that one line.
# 197,510 -> 198,577 (measured exact). 142 -> 143 tools (2026-08-28, Seshat, thread
# 72cd8e3c, decision c1073f00, Wave 4's historical backfill): backfill_lineage_repo_
# links — a genuinely NEW repair verb linking every zero-live-link Decision/Thread
# authored by a real Agent lineage to its project, via the SAME rung-3 lookup a live
# write already gets (resolve_repo_default). Docstring trimmed under the category rule
# first (the full rationale — why this is a missing verb and not a live-safeguard
# defect, the cross-source supersede trap — lives in capture.py's own module comment,
# not duplicated here); the remaining growth is the new capability itself, not prose.
# 200,229 -> 200,736 (measured exact). Tool count unchanged (2026-08-28, Imhotep, msg
# 6000, live specimen decision 7706efb4): record_decision gained one new PARAMETER,
# `cites` — the declared form of the prose-citation miner's own `cites` edge
# (origin="declared"), for a Decision that adds a new facet to an earlier one without
# bounding (`narrows`), refuting, or independently re-deriving it. `bears_on` and
# `narrows` both correctly refused this specimen; no new LinkType needed, `cites`
# already legal Decision->Decision. Docstring line trimmed to the sibling params' own
# one-line shape first; the remaining growth is the new param's own wire schema entry
# plus that one line.


# HOISTED FROM A BARE INLINE ASSERT (2026-08-28, thread 5999): the count used to live only
# as `assert len(per_tool) == N` inside the test function below, with no name a merge
# driver could find. A wave of four merges left the char ceiling above correctly
# reconciled at every step (only one branch at a time touched IT too, so it always
# conflicted and this driver always fired) while this count sat NINE LINES BELOW, touched
# by only one branch — no conflict at all, so git silently kept that branch's own number
# and main read 143 while the merged tree actually carried 144. Same shape, same name
# convention, same driver (scripts/reconcile_tool_contract_ceiling.py's own
# `_DEFAULT_CONSTANTS` now reconciles both in one pass) — a ratchet with two numbers needs
# both of them findable by name, not one.
TOOL_CONTRACT_EXPECTED_COUNT = 144
# 200,229 -> 202,302 (measured exact). 144 -> 146 tools (2026-08-28, Seshat, thread 6001,
# Wave 5's ambiguous-abstention retry door): retryable_ambiguous_abstentions (READ-ONLY,
# the sibling retryable_abstentions never covers — 2+-candidate abstentions reduced by
# elimination to exactly one live survivor) and retry_ambiguous_abstentions (the write
# half, lane-agnostic since it only rechecks stored candidate ids' own status, never
# re-derives). Two genuinely new capabilities, not one padded — docstrings trimmed under
# the category rule first (full rationale in capture.py's own module comment).
TOOL_CONTRACT_CEILING_CHARS = 202809

async def _measure_tool_contract() -> tuple[int, dict[str, int]]:
    """Returns (total_chars, {tool_name: its own wire chars}) — see `_tool_chars`."""
    from src import mcp_server as srv

    tools = await srv.mcp.list_tools()
    per_tool = {t.name: _tool_chars(t) for t in tools}
    return sum(per_tool.values()), per_tool


def test_tool_chars_counts_outputschema_not_just_the_original_three_fields() -> None:
    """NEGATIVE CONTROL (2026-08-03, decision 553b5173): before this fix, the per-tool sum
    (then inlined in `_measure_tool_contract`) counted only name+description+inputSchema —
    two tools differing ONLY in outputSchema measured IDENTICALLY, a real ~8,862-char
    fleet-wide undercount (98 tools) found by comparing the live deployed server against
    this ratchet's own in-process measurement three independent ways. Pre-fix, `_tool_chars`
    didn't exist at all — confirmed failing via git stash (AttributeError, not a semantic
    pass)."""
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
        f"{TOOL_CONTRACT_CEILING_CHARS} (task #129's ratchet, this file's own docstring) — "
        f"heaviest 10 tools right now: {named}. If you added prose, trim it under the "
        f"category rule; if the growth is genuinely load-bearing, raise the ceiling as a "
        f"deliberate act with a reason, not a reflex.")


async def test_tool_contract_has_the_expected_tool_count() -> None:
    """A cheap companion signal: if this number moves, a tool was added or removed — not
    what this ratchet polices, but worth knowing at a glance when the char total also
    moves, to tell 'one tool's prose grew' from 'the surface itself changed shape'. 99 -> 100
    (2026-08-02): amend_practice (task #113, commit 9cbd8d6) was never reflected here either
    — the same pre-existing drift the char ceiling above was just found and raised for.
    100 -> 98 (2026-08-03, ruling 31c02dca): fold_agent/unfold_agent/fold_seat/fold_project
    retired in favor of merge/unmerge — a real shrink in the surface, not drift.
    98 -> 100 (2026-08-03, ONE MERGE, TWO INDEPENDENT ADDITIONS): reconcile_merge (task #127,
    the fold family's first-ever repair door for any type) and ack_handoff (the is_handoff
    read-receipt redesign). Neither author could see the other's branch; this file is again
    the only place the combined surface exists.
    100 -> 101 (2026-08-03, task #128 piece 4): invalidate_works_in — a live agent's own
    repair door for a duplicate works_in edge, thread 8640a625's own toolkit hole.
    101 -> 102 (2026-08-08, task #140): roster — the graph-level "who owns this repo".
    A coordinator had no such call, so mount()'s LIVE co-agent list was read as the roster
    and cold seats read as vacant; two real misroutes followed (Alfred XIII, 2813da48).
    102 -> 103 (2026-08-08, task #156.4): wake_preflight — which gates would refuse a wake,
    asked BEFORE the attempt instead of discovered as a wall of refusals after.
    103 -> 113: TEN RAISES WENT UNRECORDED HERE. The assert moved, this log did not — so
    the file whose whole purpose is to be the one place the combined surface is visible
    stopped being that, silently, for ten tools. Named rather than back-filled: prose
    reconstructed from git and typed once is the same defect one layer up (ruling
    e6277013). Read the commit that raised the assert for a given tool's provenance.
    113 -> 114 (2026-08-14, task #76 item 3): peer_ledger — the history of the peer
    relationship between two seats, the last item of the #76 punch list.
    114 -> 115 (2026-08-14, ruling e6277013/5273e0f3): correct_thread_summary — a Thread's
    summary could be TWINNED but never CORRECTED (open_thread is idempotent on the summary
    hash; annotate_thread's own docstring refuses the job). The one genuinely missing verb
    of the three ledger diseases; the other two already had uncalled cures.
    117 -> 118 (2026-08-17, fe8ec7ff mechanism 3b, ruling df646654 — self-healing over
    manual cleanup): reconcile_seat_identity — heals a cross-source contradiction on a
    seat's own house/project, self-scoped, no operator sign-off — what #157's four staged
    retire_assertion calls become, one self-service call per seat.
    118 -> 119 (2026-08-17, obligation cabfb4b2): unwitnessed_spawns — Ptah VII's rotten-
    apple report (subagents spawned_by his own identity that he never spawned, invisible to
    the operator). Every LIVE spawned_by fact with no transcript ever materializing on disk
    to confirm it — "what is executing under my identity that I did not spawn," self-scoped
    default, any identity nameable (a pure read, never gated).
    119 -> 120 (2026-08-17, thread 20af2c95): backfill_agent_project_links — the write-side
    edge-leak fix (mint_heir/fold_agent invalidating a predecessor's works_in/governs onto
    its heir) has shipped and been tested since 2026-08-04, but the one-time repair for
    edges already stranded on off-head generations before that fix landed had no reachable
    door at all — importable only, a fifth-ledger-disease specimen. dry_run=True default,
    same "list only, the bulk act is the operator's" shape as #150's own repairs.
    123 -> 126 (2026-08-17, thread 5126, operator ruling df646654/fe8ec7ff — chronohorn's
    own wave): uningested_trees (the census door onto discover_trees, existing/tested/
    zero-callers until now), ingest_project + ingest_project_third_party (self-service and
    coordinator forms, same authority shape as reconcile_seat_identity's own pair).
    131 -> 132 (2026-08-19, Sekhmet, dispatch 5398 leg 2, ruling 94c2e7e8/b3ccd3f6): stop —
    launch()'s process-lifecycle inverse, #156's own held half, finally ships.
    137 -> 138 (2026-08-27, Khnum, decision 49231693/operator ruling): backfill_bootstrap_
    orphan_references — the repair-verb half of the bootstrap_project door-gap fix (see
    the ceiling's own changelog above for the full account).
    138 -> 139 (2026-08-28, Khnum, decision 18464c67/thread 33962e00): backfill_boot_
    alarm_commit_links — Lane 1's repair verb (see the ceiling's own changelog above).
    139 -> 140 (2026-08-28, Seshat, decision a55b1014/thread 5f47e23d): backfill_
    task_sync_citation_links — Wave 2 Lane A's repair verb (see the ceiling's own
    changelog above).
    140 -> 144 (2026-08-28, THE WAVE 4/5 MERGE, reconciled by Thoth LXXXVII): four
    branches landed together and EACH SIDE'S CHANGELOG WAS CORRECT FOR ITS OWN BRANCH
    AND WRONG FOR THE MERGED TREE — one said 141 -> 142, the other 140 -> 142 -> 143,
    and the tree that carries all of them is neither. The raises, all four measured
    against the merged tree rather than inherited from any branch:
      · repair_stale_pile_summons (Sekhmet, thread e2326ab7, Wave 4 leg (b)) — a frozen
        count in a bulk-minted summons goes false the moment its pile is drained.
      · retryable_abstentions (Imhotep, thread e67fc338) — the structurally-safe
        zero-candidate subset of abstained_derivations, filtered in SQL so a 2+-candidate
        abstention can never appear regardless of caller behaviour.
      · backfill_lineage_repo_links (Seshat, decision c1073f00/thread 72cd8e3c) — Wave 4's
        historical repair for the repo= lineage ladder.
      · desk + show (Sekhmet, Wave 5) — the CLI's read half; see the ceiling's changelog.
    Khnum's charter-aware get_thread_list/get_decision_list changed no count, docstrings
    only. The 140 -> 141 raise predating this wave still went unrecorded here, same class
    as the ten-raise gap named earlier in this log; not backfilled for the same reason.
    A RATCHET SETTLED BY PREFERENCE HAS STOPPED RATCHETING — so no side was picked and
    no two numbers were averaged; the tree was re-measured."""
    _, per_tool = await _measure_tool_contract()
    # Khnum's named-constant form wins over the inline literal, and the two sides were
    # each right for their own branch: his hoisted this into TOOL_CONTRACT_EXPECTED_COUNT
    # so the merge driver could reconcile it (the fix for Wave 4/5's silent 143-vs-144),
    # hers bumped the literal to 146 for Wave 5's two new abstention-retry verbs. The
    # merged tree is neither number — re-measured below, never picked.
    assert len(per_tool) == TOOL_CONTRACT_EXPECTED_COUNT
