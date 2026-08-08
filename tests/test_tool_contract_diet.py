"""THE TOOL-CONTRACT RATCHET (task #129, Thoth's ruling 1c414054): osiris-mcp's own
advertised tool surface — every `@mcp.tool()` NAME + DESCRIPTION + inputSchema — is paid by
every client eagerly, at connect time, before its first act. Measured before any cut: 97
tools, names 1,109 + descriptions ~85,988 + inputSchemas ~32,130 chars ~= 119,227 chars total
(~30k tokens). Same mechanism as #125's frozen tool-list index: too big to ship eagerly ->
deferred -> stale cache. Every char cut here is pressure off the thing that freezes.

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
TOOL_CONTRACT_CEILING_CHARS = 139_785


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
    asked BEFORE the attempt instead of discovered as a wall of refusals after."""
    _, per_tool = await _measure_tool_contract()
    assert len(per_tool) == 103
