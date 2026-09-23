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
# 116 -> 117 (2026-09-04, Seshat, #203/Thoth dispatch 6966, decision a49d2730/38755abe):
# list_unfiled_threads — the H-bucket instrument gap: a Thread filter on absence of an
# in_repo edge (plus source=/kind= equality), paginated, that no existing door provided
# (the-wall's top_of_wall hard-caps at 25, triage has no project-edge/source filter,
# graph_lint's stale check never catches a perpetually-re-touched cluster). Genuinely
# new capability, not padding; docstring trimmed once under the category rule.
# 116 -> 117 (2026-09-04, Sekhmet, decision 68fba2e4/thread 19d6bdcb7fa9): resync_seat_house
# — the MCP door onto seats.resync_seat_house_third_party (existed, unreached until the
# house/project ruling's six-seat repair task needed it, msg 6967).
# 115 -> 116 (2026-09-04, Imhotep, task #199's core-verbs lane, Thoth dispatch 6901):
# transition_seat_project — the self-service TRANSITION verb (invalidate_works_in +
# correct_pin_value + set_charter, one composed act, MCP + CLI same commit) — see the
# char ceiling's own changelog below for the full mechanism and why rebind_seat is
# deliberately excluded.
# 116 -> 115 (2026-09-04, Imhotep, task #199 lane 2, retirement wave 2, Thoth dispatch
# 6872/6876, operator-accepted null result): uningested_trees hidden — zero MCP traffic,
# no CLI/daemon/slash bypass, and its automated companion uningested_trees_alarm_tick
# already covers the same ground proactively. The other 25 candidates from the wave-2
# proposal (decision 36433427) stand as a KEEP finding, not touched — see that decision
# for the full per-tool reasoning; Thoth's own verdict: record it as the standing "the
# tail is load-bearing" finding so nobody dispatches a third retirement pool against it.
# 146 -> 147 (2026-09-01/02, Imhotep, thread 6272): sweep_seat_disk, the disk-half wrapper
# over sweep_retired_office/sweep_seat_workspace — see the ceiling's own changelog above.
# 147 -> 148 (2026-09-02, Imhotep, ruling b30e2b38): revert_own_pin_write, the self-scoped
# door onto offices.revert_pin_write — existed, tested, unreached until a seat that
# followed the rules into a bad pin state had no sanctioned way back out.
# 148 -> 150 (2026-09-02, Sekhmet, ruling 23771416): heal_seat_anchor +
# heal_seat_anchor_third_party — see the ceiling's own changelog above.
# 144 -> 146: Seshat's retryable_ambiguous_abstentions + retry_ambiguous_abstentions
# (thread 6001, Wave 5's ambiguous-abstention retry door).
# THE DRIVER CORRECTLY DECLINED THIS ONE AND SAID SO: "TOOL_CONTRACT_EXPECTED_COUNT
# missing from one of ancestor/ours/theirs — not this collision's shape for this
# constant." True and right — the constant did not EXIST in the merge base, because
# the commit that created it is in this same wave. A driver cannot reconcile a
# three-way delta on a value with only two sides. It declined by name instead of
# guessing, which is the whole point of the try/except; the count was then measured
# by hand, the same way the chars are. From the NEXT merge on it has an ancestor and
# reconciles like its sibling.
# 200,229 -> 202,302 (measured exact). 144 -> 146 tools (2026-08-28, Seshat, thread 6001,
# Wave 5's ambiguous-abstention retry door): retryable_ambiguous_abstentions (READ-ONLY,
# the sibling retryable_abstentions never covers — 2+-candidate abstentions reduced by
# elimination to exactly one live survivor) and retry_ambiguous_abstentions (the write
# half, lane-agnostic since it only rechecks stored candidate ids' own status, never
# re-derives). Two genuinely new capabilities, not one padded — docstrings trimmed under
# the category rule first (full rationale in capture.py's own module comment).
# 200,229 -> 200,308 (measured exact). Tool count unchanged (2026-08-29, Sekhmet, thread
# 6002, Wave 6 half 2): `stop`'s own docstring corrected — it claimed "SIGTERMs a LIVE
# body's OS process", no longer true after the live-reproduced fix (a raw SIGTERM to a
# --bg-substrate body gets silently respawned by the harness's own daemon; `stop_seat`
# now prefers the harness's own `claude stop <id>` and falls back to SIGTERM only when
# no harness-tracked id exists). The growth is a corrected fact, not added prose.
# was 202871: RATCHETED DOWN 202,888 -> 202,871 on measurement.  (history, not executable)
# Seshat trimmed the duplicate-works-in lint text under the category rule while fixing its
# overclaim, so the merged tree came in 17 chars UNDER the standing ceiling. Left alone that
# is 17 chars of headroom nobody measured and the next raise would silently spend it — which
# is how a ratchet stops ratcheting. Lowered to the real number instead.
# 202,871 -> 203,332 (measured exact). Tool count unchanged (2026-09-01, Imhotep, obligation
# 8f59b64f/95a0feb3, Thoth XC's dispatch): open_thread's docstring gained two new receipt
# fields a caller genuinely needs to know exist — `dedup_scope` (names what the twin check
# actually covered, so `deduped: "false"` stops reading as "nothing similar exists anywhere"
# when it only ever meant "no twin among this project's own open Threads") and `prior_art`/
# `prior_art_flag` (open_thread was the one write verb of three — record_decision, send,
# open_thread — with no semantic prior-art check at all; #86's own borrowing went one way,
# never back). Trimmed once under the category rule before raising (removed the two
# obligation-id citations, which a caller has no use for at call time).
# 203,332 -> 204,880 (measured exact). Tool count 146 -> 147 (2026-09-01/02, Imhotep,
# thread 6272, Thoth's dispatch "wire, don't rebuild"): new tool sweep_seat_disk wires
# the fully-built, fully-guarded sweep_retired_office (offices.py, pre-existing) plus its
# new workspace-half sibling sweep_seat_workspace to MCP for the first time — a genuinely
# new capability, not padding. Trimmed once under the category rule before raising
# (removed the "unreached vs rebuild" narrative aside, a caller has no use for it at call
# time; 1,850 -> 1,548 chars).
# was 204880  (history, not executable)
# 204,880 -> 205,497 (measured exact). Tool count unchanged at 147 (2026-09-02, Imhotep,
# decision 7fe20cc5, obligation 53424b07, operator-authorized): `merge`/`rebind_seat`
# each gained `force`/`because` params and a short docstring note for the new
# third-party-on-a-live-target liveness guard — no new tool, existing tools' own
# contracts grew because the guard's behavior is something a caller now genuinely needs
# to know exists. Trimmed once under the category rule before raising (both new
# docstring notes cut to one sentence each).
# was 205497  (history, not executable)
# 205,497 -> 206,209 (measured exact). Tool count 147 -> 148 (2026-09-02, Imhotep, ruling
# b30e2b38): new tool revert_own_pin_write (see the count changelog above) plus a short
# correct_pin_value docstring addendum for its own anchor-copy extension — a genuinely
# new capability, not padding. Trimmed both new docstrings once under the category rule
# before raising.
# was 206209  (history, not executable)
# 206,209 -> 208,607 (measured exact). Tool count 148 -> 149 (2026-09-02, Khnum, thread
# 6483/6559/6567/6576): new tool heal_seat_transcript (see the count changelog above) —
# a genuinely new capability, not padding.
# 206,209 -> 208,053 (measured exact). Tool count 148 -> 150 (2026-09-02, Sekhmet, ruling
# 23771416): two new tools, heal_seat_anchor + heal_seat_anchor_third_party — self-service
# and third-party repair for THE ANCHOR INVARIANT (anchor_cwd is identity, always
# <office_root>/<handle>, corrupted live on Chad/Jesus/henry/Marquee by rebind_seat's own
# now-closed write-path gap). A genuinely new capability, not padding. Trimmed both new
# docstrings once under the category rule before raising.
# was 202871  (history, not executable)
# 202,871 -> 210,451 (measured exact). Tool count unchanged at 151 (2026-09-03, Khnum,
# the khnum-splice-seek/main reconciliation, ruling d161a156's own coordination cost):
# the merge driver correctly declined to add the two branches' own ceiling deltas
# together (msg 6620's own gate_hook log: "never the larger of the two") and left the
# ceiling at the shared ancestor's value, 202871, for a human/agent to re-measure
# against the REAL merged tree rather than guess a sum — both heal_seat_transcript
# (this branch) and heal_seat_anchor/heal_seat_anchor_third_party (Sekhmet's,
# already in main) are genuinely present in the tool count (151, already correctly
# 3-way-reconciled) but their combined char cost was never actually measured together
# until now. Measured fresh, not summed.
# was 210451  (history, not executable)
# 210,451 -> 212,749 (measured exact). Tool count 151 -> 152 (2026-09-03, Khnum, task #199
# lane 3C, ruling 41a41437): ONE new tool, `resume` — launch's former automatic resume-or-
# fresh branch split into its own agent-facing verb, mirroring the CLI's own already-ruled
# launch/resume split (60c78788) exactly. A genuinely new capability (a manager can now
# resume its own worker's dormant session directly, not only via the CLI door), not
# padding — launch's own docstring shrank in the same commit (its resume-era paragraphs
# moved to resume's docstring rather than being duplicated), so this is the net cost of
# ONE new tool, not two tools' worth of prose.
# 210,451 -> 209,867 (measured exact). Tool count 151 -> 150 (2026-09-03, Imhotep, task
# #199 lane 2, thread 6778 — the six-pair consolidation's proof): heal_seat_anchor_third_
# party retired as a hidden alias (BoundedMCP.list_tools now filters any tool registered
# with meta={"deprecated": True}, still fully callable via call_tool — see mcp_server.py's
# own docstring on the override), its body sharing heal_seat_anchor's own new
# `seat_id=None` (self) / `seat_id=<seat>` (third-party, `because` required) parameter
# instead of a second implementation. FIRST SHRINK either ratchet has recorded: one fewer
# tool AND fewer total chars in the same change, not one traded for the other — the
# mechanism this lane exists to prove.

# 209,867 -> 208,349 (measured exact). Tool count 150 -> 147 (2026-09-03, Imhotep, task
# #199 lane 2, thread 6778/6788): three more hidden-alias retirements in the same wave
# as the count changelog above — see there for the full list. Second shrink in a row,
# same mechanism, not a fluke.

# 208,349 -> 186,392 (measured exact). Tool count 147 -> 121 (2026-09-03, Imhotep, task #199
# lane 2, thread 6822, retirement wave 1 — see the count changelog above for the full list,
# the held-back exceptions, and the 7 caught by test_cli_mcp_parity.py's own BINDING_VERBS
# gate and reverted before this measurement). Largest single-wave drop either ratchet has
# recorded: 26 tools hidden from list_tools() in one pass, all genuinely unused rather than
# merged into a surviving sibling.

# 186,392 -> 181,493 (measured exact). Tool count 121 -> 115 (2026-09-03, Imhotep, task
# #199 lane 2, thread 6854, families wave — see the count changelog above for the full
# breakdown and the three declined families).
# STALE RECONCILIATION, RE-MEASURED (2026-09-03/04, Imhotep, task #199 docstring-diet wave,
# Thoth dispatch 6872, thread 6854): tonight's multi-branch merge (Khnum's launch/resume
# split + resume's own new tool, Sekhmet's rebind_seat holder=, the BINDING_VERBS raw-
# registry fix) left this ceiling reconciled back to the shared ancestor's stale 202871,
# same class as Khnum's own earlier note above — a driver correctly declines to sum two
# branches' deltas, so a human/agent re-measures against the REAL merged tree instead of
# trusting the arithmetic. Tool count 115 -> 116 (one new tool, `resume`, landed on main
# independently of this branch).

# 202,871 -> 133,436 (measured exact). Tool count unchanged at 116 (2026-09-04, Imhotep,
# operator's own priority: "make context bloating a priority, one turn in you're at 25%"
# — Thoth dispatch 6872, task #129's ~32K-token schema-load measurement). THE
# DOCSTRING-DIET WAVE: rewrote ~40 of the highest-cost tool docstrings (record_decision,
# open_thread, settle, send, mount, fleet, tree_ledger, roster, launch/resume, triage,
# graph_lint, wake, rebind_seat, ack_handoff, dispose, acquire_lease, merge, and more) to
# a lean contract — what it does, the params that matter, one line on refusal — moving
# incident history, ruling ids, and multi-paragraph rationale out of the live docstring
# and into the graph (consult_canon pointers added where the cut material still matters).
# No parameter, refusal condition, or receipt field was dropped — only the WHY-it-exists
# prose, which a caller does not need at call time to use a tool correctly. Full suite
# verified green after (not a docstring-only change nobody re-ran). NOT the operator's
# stated target (under 60K aggregate) — the top ~40 tools were the highest-leverage cut;
# the remaining ~76 model-facing tools average a few hundred chars each and would need a
# second wave to close the rest of the gap. Reported honestly, not padded toward a number.

# 133,436 -> 132,810 (measured exact). Tool count 116 -> 115 (2026-09-04, Imhotep, task
# #199 lane 2, retirement wave 2 — see the count changelog above): uningested_trees
# hidden, same commit as the docstring diet per Thoth's own instruction.

# 132,810 -> 133,746 (merge of sekhmet-receipt-diet onto imhotep-docstring-diet, 2026-09-04):
# four opt-in params (want_prior_art/want_listener on send, want_prior_art on inbox,
# want_co_agents/want_held_work on mount) add inputSchema + a one-line note each — the
# params exist to SHRINK receipts; +936 schema chars buys ~32% off every send/inbox call.

# 133,746 -> 129,145 (2026-09-04, Imhotep, task #199 lane 2, docstring diet wave 2,
# Thoth dispatch 6886): trimmed record_decision/send/mount/open_thread/settle (per-
# parameter prose -> compact tables, per Thoth's own instruction) plus ~25 more of the
# remaining ~75 tools where WHY-prose (ruling ids, incident narrative) still had room to
# cut. NOT the operator's 60K target, and short of even this wave's own 5K ask on the
# top-5 alone — most of the remaining tools were already lean from wave 1 or are
# genuinely dense contract (refusal-condition lists, per-parameter disambiguation) that
# Thoth's own msg 6886 named as correctly kept whole. Diminishing returns confirmed a
# second time; reported honestly rather than padded toward the number.
# 133,746 -> 133,906 (sekhmet-orient-diet, 2026-09-04): one opt-in param
# (want_blind_spots on orient) adds inputSchema + a one-line docstring note — measured
# to buy a 31.4% cut (1324 -> 908 bytes) on the seven-blind-spot fixture in
# tests/test_receipt_diet.py, same trade as the prior raise above.
# 210,451 -> 210,891 (measured exact). Tool count unchanged at 151 (2026-09-04, Seshat, #203
# mechanism ruling 880ffe79/decision 46ee0083, Thoth dispatch 6865): resolve_thread's own
# `ref` widened to accept a LIST (batch-close under one shared because/artifact, refuse-
# whole-run on any bad ref) plus a new `dry_run` param — a genuinely new capability (the
# operator's own "closing 1000+ obligations" mechanism), not padding. Docstring trimmed
# once under the category rule before raising.
# 129,145 -> 131,200 (2026-09-04, Imhotep, task #199's core-verbs lane, Thoth dispatch
# 6901): +2,055 chars for transition_seat_project, the new self-service TRANSITION
# verb — a genuine new tool, not a diet regression. A real cost for closing the Jesus/
# Chad specimen's own gap (no single verb folded works_in + pin + charter, and the
# hand-run sequence is exactly what broke both seats' anchors and mail attribution
# once already).

# 131,200 -> 114,770 (2026-09-04, Imhotep, task #199's context-bloat priority, Thoth
# dispatch 6886/6908, decision on the schema lever): BoundedMCP.list_tools now strips
# every auto-generated, always-redundant JSON-Schema `title` key (497 occurrences
# fleet-wide) from inputSchema/outputSchema before it reaches a model — the same seam
# the deprecated-tool filter already lives in, mechanical, zero semantic risk (no
# client reads `title`; the property KEY it duplicates is untouched). The anyOf/null
# branches pydantic emits for Optional params are deliberately left alone — not the
# same zero-risk shape, a strict client's validation could depend on them. -12.5% off
# the whole tool-contract surface in one mechanical change, no docstring rewritten.
# 114,770 -> 115,367 (sekhmet-orient-diet, 2026-09-04, rebased onto the title-strip
# above): correct_pin_value's `value` widened str -> str | None so a caller can UNSET a
# fabricated pin key (delete the line) instead of only ever rewriting it to another
# string -- #199's mint-layer prerequisite for a transition verb walking a seat off an
# old fabricated project value, decision 24e0b761/commit cf201a9's own law extended from
# mint-time to correction-time. A real capability, not bloat: the anyOf-null schema
# shape (deliberately NOT stripped by the title-strip above, per its own note) is the
# honest cost of representing a legal "no value" the tool previously could not express.
# was 115367  (history, not executable)
# 115,367 -> 115,446 (merge of sekhmet-orient-diet, 2026-09-04): correct_pin_value's `value`
# widened to str | None so a pin key can be UNSET (operator ruling 004cc8d8: homeless is legal);
# +79 post-title-strip schema chars for the anyOf, no prose.
# 115,367 -> 116,137 (measured exact). Tool count 116 -> 117 (2026-09-04, Seshat, #203/
# Thoth dispatch 6966): new tool list_unfiled_threads — see the tool-count changelog
# above for the full rationale. Trimmed once under the category rule before raising
# (dropped the per-clause mechanism narrative, a caller has no use for it at call time).
# 115,446 -> 116,642 (2026-09-04, Sekhmet, decision 68fba2e4/thread 19d6bdcb7fa9): a
# genuinely new tool, resync_seat_house -- the MCP door onto seats.resync_seat_house_
# third_party (existed, unreached until the six-seat house repair needed it). A real
# capability, not bloat.
# STALE POST-MERGE VALUE CAUGHT AND FIXED (2026-09-04, Imhotep): this constant had
# regressed to 202,871 -- a merge driver declined to sum two branches' own deltas and
# left the ceiling at a shared ancestor's stale value, the exact recurring class Khnum
# and this seat have both hit before (see the changelog entries above this one for the
# same pattern). Re-measured against the real merged tree rather than trusted: 116,137,
# matching this comment's own claim exactly.

# 116,137 -> 113,757 (2026-09-04, Imhotep, #202 wave 3, Thoth dispatch 6987, operator's
# relaxed dispatch-shape rule): resolve_thread/annotate_thread/correct_thread_summary/
# reclassify_thread folded into thread_action(action=...) -- one door, four actions,
# all four kept as hidden deprecated aliases forwarding to the shared _thread_action_
# impl body (no logic duplicated, no behavior changed -- resolve_thread's own batch
# mode and dry_run=True default carried over unchanged). Tool count 117 -> 114 (-3).

# 113,757 -> 112,621 (2026-09-04, Imhotep, #202 wave 3, Thoth dispatch 6987): acquire_
# lease/release_lease/check_lease/reap_stale_leases folded into lease(action=...) —
# same shape as the thread_action fold above, four hidden deprecated aliases.

# 112,621 -> 112,241 (2026-09-04, Imhotep, #202 wave 3, Thoth dispatch 6987): attach_
# seat/detach_seat folded into seat_edge(action=...), both kept as hidden deprecated
# aliases. No CLI door for either name before or after (NO_CLI_EQUIVALENT retargeted).

# 112,241 -> 111,966 (2026-09-04, Imhotep, #202 wave 3, Thoth dispatch 6987): retire_
# seat/retire_project/retire_agent folded into retire_object(kind=...), all three kept
# as hidden deprecated aliases. Self-scoped retire() and retire_assertion deliberately
# excluded (decision 1ddf8e1c) — different auth shape / unrelated param shape.
# 111,966 -> 113,162 (2026-09-04, Thoth, merge of sekhmet-orient-diet + imhotep-wave3-
# family-folds): the same stale-ancestor merge artifact Imhotep named above landed AGAIN
# on this merge (both branches' deltas, ancestor value kept); re-measured against the
# merged tree: 113,162 = wave 3's 111,966 + resync_seat_house (Sekhmet, ruling 68fba2e4's
# repair door) + list_unfiled_threads' carry. Pinned to the measurement, not summed. THIS
# IS THE THIRD OCCURRENCE of the stale-post-merge-ceiling class (see the 202,871 catch
# above and the 116,137 catch further down this same file) — a merge driver repeatedly
# declining to reconcile two branches' own deltas and leaving the ceiling at a shared
# ancestor's stale value; not a new bug, the same recurring one, worth naming again.
# 113,162 -> 113,259 (2026-09-04, Imhotep, #202 wave 4, Thoth dispatch 7034, decision
# 6fe4305c): +97 net despite -3 tools — three folds' merged docstrings (each one now
# carries both branches' own prose, e.g. current_flags documents both inspect and repair
# in one place) cost slightly more schema than the six standalone tools they replaced.
# Measured exact (113,259), not estimated. A genuine tradeoff of the dispatch-shape rule,
# not padding: fewer tools at a small per-tool prose cost.
# 113,259 -> 99,443 (2026-09-04, Imhotep, #202 SEAT DISPATCHER, operator ruling
# f9182ad7, Thoth dispatch 7039, migration plan decision 620bdb32): -13,816 net despite
# seat's own hand-built oneOf schema costing 10,828 chars alone (the single most
# expensive tool in the whole contract, ahead of record_decision's 3,753) — removing 24
# standalone tools' own full schemas from the live listing outweighs it by a wide
# margin. Measured exact (99,443), not estimated. The largest single-commit reduction
# this ratchet has recorded, and the first real evidence for the operator's own
# thesis ("the shrink seems like the correct direction... vs 100 individual tools").
# 99,443 -> 101,729 (2026-09-04, Imhotep, same commit, price-minimizer follow-through):
# +2,286 for two real-client-validation gates SEAT_INPUT_SCHEMA was still missing —
# `additionalProperties: False` on every one of the 30 branches (a real client's typo
# or cross-action param now gets rejected client-side, not silently accepted) and the
# subagent-attribution trio (subagent_id/subagent_type/session_anchor) added to the six
# branches (stop/walk_in/pause/launch/resume/wake) whose own standalone predecessors
# genuinely accepted them — without this, a real client validating against the schema
# would have wrongly rejected a legitimate attributed call. seat now costs 13,114 chars,
# a real cost of correctness, not padding. Measured exact (101,729).
# 101,729 -> 102,243 (2026-09-05, Sekhmet, thread 4de94895 / decision fff496fe22b0's own
# named gap): new `resync_pin` action on the seat dispatcher, the third-party sibling of
# `correct_pin` — mirrors `resync_house`'s own third-party shape exactly (dry_run
# default, `reason` enforced only at write time). +514 for one new oneOf branch plus its
# ACTION TABLE docstring line — a real cost of a genuinely missing door, not padding.
# Measured exact (102,243).
# 102,243 -> 102,722 (2026-09-05, Seshat, thread 6a1dfc52's own named gap — Thoth dispatch
# 7098 item 2): `min_age_days`/`max_age_days` added to get_object_list's thread branch and
# list_unfiled_threads — THE AGE-BIN INSTRUMENT: no primitive exposed a Thread's own
# creation timestamp in bulk or let a caller filter by it, so binning ~938 open obligations
# by age required a per-object pull, not a cheap call. Trimmed both new docstring additions
# under the category rule first (814 chars, before this bump) before raising the ceiling for
# the remaining +479 — a real cost of a genuinely missing filter, not padding. Measured
# exact (102,722).
# 102,243 -> 100,470 (2026-09-05, Imhotep, #202 COMPOSITION + PROJECT DISPATCHERS, Thoth
# dispatch 7095): removing 9 standalone tools' own full schemas from the live listing
# (3 composition, 6 project — see the tool-count ratchet's own changelog above for the
# exact roster) outweighs the two new small hand-built oneOf schemas added (composition:
# 3 actions; project: 8 actions, both far smaller than seat's own 30-branch schema).
# Measured exact (100,470), not estimated.
# STALE POST-MERGE VALUE CAUGHT AND FIXED AGAIN (2026-09-05, Imhotep, branched off
# main@bf3123a for the #202 THREAD DISPATCHER build): this constant had regressed to
# 202,871 on main@bf3123a itself (merge sekhmet-thread-list-fanout) — the FOURTH
# occurrence of the exact same recurring class this changelog already names three times
# above (the 202,871 catch near the top of this file, the 116,137 catch, and the 113,162
# catch): a merge driver declining to reconcile two branches' own deltas and leaving the
# ceiling at a shared ancestor's stale value instead of the real merged tree's own total.
# Re-measured against a clean bf3123a checkout rather than trusted: 101,001 (this
# session's own 100,470 and Seshat's own 102,722 both landed in that merge; the real
# combined tree measures neither number, same lesson each of the three prior catches
# already drew — pinned to the measurement, not summed or averaged).
# 101,001 -> 102,670 (2026-09-05, Imhotep, #202 THREAD DISPATCHER, Thoth dispatch 7162):
# thread_action ITSELF (already a wave-3 action-dispatcher) re-platformed into the
# object-type-dispatcher naming convention as `thread(action=...)`, its own flat
# auto-generated schema replaced by a hand-built oneOf (price-minimizer #1) — a genuine
# re-platforming, not a second fold of the same four names again (thread_action's own
# four hidden aliases — resolve_thread/annotate_thread/correct_thread_summary/
# reclassify_thread — are untouched, still forwarding to the same _thread_action_impl).
# +1,669 net: the hand-built oneOf's own additionalProperties=False and subagent-
# attribution trio on every one of the four branches cost more per-branch overhead than
# the flat schema thread_action carried before, the same real correctness cost seat's
# own price-minimizer follow-through named (99,443 -> 101,729). Tool count unchanged
# (76 -> 76): one hidden, one added. Measured exact (102,670), not estimated.
# 102,670 -> 103,032 (2026-09-05, Imhotep, #202 AGENT DISPATCHER, Thoth dispatch 7162,
# proposal decision 65a6eb73 approved as scoped): 6 actions fold in (claim_name,
# correct_agent_house, retire_agent, fleet_reconcile, file_subagent, file_subagents) —
# three of the six (correct_agent_house, retire_agent, file_subagent) were ALREADY
# hidden before this fold (zero-traffic retirement or an earlier retire_object repoint),
# so only three tools' own full schemas actually leave the live listing (claim_name,
# fleet_reconcile, file_subagents) against one new 2,833-char hand-built oneOf schema
# added. Net +362, a small real cost — the six-branch schema's own additionalProperties/
# action-const overhead outweighs three small flat schemas removed by less than either
# prior dispatcher's own price-minimizer follow-through did. Measured exact (103,032),
# not estimated.
# 103,032 -> 103,371 (2026-09-05, Imhotep, #202 PRACTICE DISPATCHER, Thoth dispatch
# 7162, proposal decision 07395004 approved as scoped): the SIXTH AND FINAL object-type
# dispatcher of #202's own fold arc. 2 actions fold in (record_practice, amend_practice),
# both genuinely live before this commit — their two flat schemas leave the live
# listing against one new 2,705-char hand-built oneOf schema added. +339 net, the
# smallest of any dispatcher's own price-minimizer cost so far (a 2-action schema is
# cheap). Measured exact (103,371), not estimated.
# 103,032 -> 103,112 (2026-09-05, Khnum, Thoth dispatch 7391, "one more round" — Marquee's
# blind spot): resync_pin gains `tree_cwd`, a genuinely new capability (an explicit
# override, else the seat's own bind_tree-declared tree, for the third-copy correction's
# workspace guess — previously unreachable for any seat whose real tree isn't named after
# its own handle). No new tool; one new optional schema param on an existing action.
# Measured exact (103,112), not estimated.
# -> 103451 (2026-09-05, Thoth, merge of practice-dispatcher + hygiene-owner-ladder): FIFTH stale-
# ancestor regression to 202,871 by the merge driver; re-measured on the merged tree, pinned.
# Whoever merges re-measures (standing order, thoth charter 2026-09-04).
# 103,451 -> 103,857 (2026-09-05, Khnum, decision fb85dd4f/381c9132, Thoth dispatch): seat
# gains a new action, `rehold` — the third-party re-hold door the werner/Thoth live
# specimen found missing (no sanctioned MCP verb could put a mis-bound seat's holds link
# back; reconcile_identity's third-party path only heals house/project, never the link
# itself). A genuinely new capability, one new oneOf branch (target, agent_id, because,
# override_live) plus one ACTION TABLE line — trimmed to the shortest true description
# already. Measured exact (103,857), not estimated.
# 103,857 -> 104,690 -> 103,857 (2026-09-05, Sekhmet, Thoth dispatch 7543 item 2, SAME
# REIGN ROUND-TRIP): `graph_census` shipped as a genuinely new tool (three obligations —
# a78b6987, 7917b404, b2208b94 — were stuck on a direct-DB-script workaround purely
# because no MCP verb could answer a plain population count, itself a house-law
# violation), then Thoth's own fold correction landed before this ceiling ever needed to
# hold at the raised number: a NEW NAMED TOOL was the wrong shape given the operator's
# fewer-tools direction, since `census` was already a composition Function. Re-exposed as
# two fixed-args saved compositions (census-seat-property-contradictions, census-cohort —
# DEFAULT_COMPOSITIONS, compositions.py) and `graph_census` itself demoted to a hidden
# deprecated alias (meta={"deprecated": True}), which this ratchet does not count at all
# — same mechanism run_composition/save_composition already use. Net: zero growth.
# Measured exact (103,857), not estimated.
# 103,857 -> 104,690 (2026-09-05, Sekhmet, Thoth dispatch 7543 item 2): `graph_census`, a
# genuinely new tool — three obligations (a78b6987, 7917b404, b2208b94) were stuck on a
# direct-DB-script workaround purely because no MCP verb could answer a plain population
# count, itself a house-law violation (raw SQL against the kernel is a defect report,
# never a shortcut). One new tool, one string param (`kind`), two supported kinds
# documented in the docstring. Measured exact (104,690), not estimated.
# 104,690 -> 105,134 (2026-09-06, Seshat, Thoth DM 7649, context diet round 2): dossier
# and roster each gain one opt-in bool param (`want_relationships`, `want_caveats`) —
# the two confirmed context-bloat offenders (decision a065171f: dossier's relationships
# measured 76% of its own bytes/call; roster's 10-paragraph caveats printed on every
# call) now default to a collapsed summary instead of the full list, same want_*
# convention orient()'s blind_spots already uses. No new tool; two new docstring
# sentences naming the opt-in. Measured exact (105,134), not estimated.
# — a genuinely new capability (msg 7677/7680, decision 76d43073's half-heal-detect backlog:
# no verb touched Agent.succeeded_by before this), docstring already trimmed to the category
# rule's bare minimum; the remainder is the new action's own inputSchema (agent_id, value,
# because, override_live), which cannot shrink further without dropping a param. Measured
# exact, not estimated.
# 104,301 -> 104,642 (2026-09-06, Seshat, Thoth DM 7667): tool_traffic() gains
# total_bytes/avg_bytes reporting alongside its existing total_ms/avg_ms (the
# mcp_tool_stats.response_bytes column round 2's own byte table had to substitute
# live-probe measurement for, decision 32b0c88f) — one docstring paragraph naming the
# new fields, no new tool, no new param. Measured exact (104,642), not estimated.
# 105,273 -> 105,548 (2026-09-06, Imhotep, Deckard msg 7719/93af8ced): project(action=
# 'rename') gains dry_run (was silently impure — dry_run=True performed the write
# regardless) and merge_into (the collision refusal now fires on ANY colliding status,
# not just active, so a deliberate reuse needs an explicit override) — two new schema
# fields plus the ACTION TABLE docstring line naming them. Measured exact (105,548), not
# estimated.
# 105,548 -> 106,023 (2026-09-06, Seshat, thread 4dcc1849, decision f9e47d3c): mount()
# gains a docstring paragraph naming its new lineage-memory-custody behavior
# (prior_lineage_memory_archived/memory_migration_needed) — no new tool, no new schema
# param, just prose naming what mount() now does on the caller's behalf. Measured exact
# (106,023), not estimated.
# 105,273 -> 105,817 (2026-09-06, Sekhmet, no-regrow hygiene item 2, practice 393be453,
# thread 1588bc73): open_thread gains one new param, `stale_after_days` (kind='obligation'
# only, default 14) — a docstring sentence plus the new inputSchema entry. No new tool.
# fleet_digest's own docstring also gained one clause naming item 4's obligation_pressure
# field. Measured exact (105,817), not estimated.
# 106,567 -> 106,940 (2026-09-07, Seshat, thread 68f1bafa, Thoth DM 7883): get_status
# gains render='text' -- a new param plus a docstring paragraph naming the read-triangle's
# server-side text mode (returns only {"text": <str>} instead of the structured receipt).
# No new tool. Measured exact (106,940), not estimated.
# 106,940 -> 107,376 (2026-09-07, Seshat, thread 68f1bafa, Thoth DM 7907, wave 2's settle.md
# dependency): get_status gains `handoff_pending` -- a bare pointer ({"from", "refs"}) to an
# unacknowledged ancestor handoff, so /settle can know one exists without paying orient()'s
# full succession-note cost. Docstring paragraph + one new result field, no schema change
# (no new param). Measured exact (107,376), not estimated.
# 106,567 -> 107,398 (2026-09-07, Sekhmet, thread 8686cba4): settle() gains two new
# top-level params, `standing_orders` and `because` — closes the standing-orders box
# honestly for a seat whose charter.md/CLAUDE.md genuinely didn't change this session,
# instead of reading complete:false forever (which starves self-compaction's own
# completeness requirement). One new docstring paragraph plus the two inputSchema
# entries. No new tool. Measured exact (107,398), not estimated.
# 107,376 -> 108,579 (2026-09-07, Seshat, thread 68f1bafa/3703a3a9, wave 2): backlog ships
# as a new tool (its own docstring + inputSchema for all_projects/render). Measured exact
# (108,579), not estimated.
# 108,579 -> 109,704 (2026-09-07, Seshat, thread 68f1bafa/3703a3a9, wave 2): threads ships
# as a new tool (its own docstring + inputSchema for project/render). Measured exact
# (109,704), not estimated.
# 109,704 -> 110,122 (2026-09-07, Seshat, thread 68f1bafa/3703a3a9, wave 2): roster gains
# render='text' -- a new param plus a docstring paragraph naming the read-triangle's
# server-side text mode. No new tool. Measured exact (110,122), not estimated.
# 110,122 -> 110,537 (2026-09-07, Seshat, thread 68f1bafa/3703a3a9, wave 2): inbox gains
# render='text' -- a new param plus a docstring paragraph naming the read-triangle's
# server-side text mode. No new tool. Measured exact (110,537), not estimated.
# 110,537 -> 111,502 (2026-09-07, Seshat, thread 68f1bafa/3703a3a9, wave 2): team ships as
# a new tool (its own docstring + inputSchema for render). Measured exact (111,502), not
# estimated.
# 111,502 -> 111,826 (2026-09-07, Seshat, thread 68f1bafa/3703a3a9, wave 2): inbox's own
# render='text' docstring paragraph grows to name the operator desk's own collapsed-band
# shape (desk.md's own rewrite). No new param, no new tool. Measured exact (111,826), not
# estimated.
# 106,567 -> 106,871 (2026-09-07, Imhotep, Thoth dispatch wave msg 7882 item 1, thread
# f4209591, specimen msg 7873): send() now refuses a broadcast whose body opens with a
# real seat's name/@handle when that seat's holder sits in a different project than `to`
# (binding_of_handle's own authoritative resolution, never a guess), naming the correct
# to_agent= instead of silently delivering to the wrong room — no new tool, no new schema
# param, one docstring paragraph naming the new refusal and the receipt's
# `addressee_resolved` field. Measured exact (106,871), not estimated.
# 112,961 -> 113,246 (2026-09-07, Sekhmet, thread b5ae6773, #203's write-time
# classification laws): open_thread() gains a one-sentence docstring note that `kind`
# is now REQUIRED (a missing kind refuses rather than minting a kindless thread) — no
# new tool, no new schema param. The owner-resolution and derived-write laws named in
# the same dispatch were held (broke a wide swath of the suite's own informal
# `agent:<name>` owner/assignee placeholders; flagged back rather than shipped as a
# guess), so only this one law's prose landed. Measured exact (113,246), not estimated.
# 113,246 -> 113,926 (2026-09-07, Sekhmet, thread b5ae6773, Thoth's ruling on the held
# question): open_thread()/thread(reclassify) finish the write-time classification
# laws — owner/assignee must resolve to an active seat or 'operator' (an agent:<id>
# owner requires lineage_head to resolve it to a currently HELD seat), and
# kind='obligation' additionally refuses from an unmounted caller. No new tool, no new
# schema param — one expanded docstring paragraph on open_thread naming both refusals.
# Measured exact (113,926), not estimated.
# 113,246 -> 113,496 (2026-09-07, Imhotep, dispatch 2589353a, wave 4, operator's own
# words "there has to be a verb that links the rename mechanically so agents don't get
# lost"): project(action='rename')'s own ACTION TABLE line gains one sentence naming
# the new cascade (every governing seat's pin/house/charter/office, under the verb's
# own elevated authority) and the receipt's `manifest` field — no new tool, no new
# schema param, the cascade itself lives entirely in project_identity.py/orchestrator
# code the model never sees. Measured exact (113,496), not estimated.
# 113,926 -> 114,316 (2026-09-07, Khnum, khnum-promotion-verb, operator 2026-09-07's
# "promotion should be a verb ... self managed"): seat() gains action='promote' — a new
# SEAT_INPUT_SCHEMA branch (target/workers/because) plus a one-line ACTION TABLE entry.
# A genuinely new binding-moving verb, load-bearing growth per this file's own escape
# valve, not incidental bloat trimmed away. No new tool. Measured exact (114,316), not
# estimated.
# 114,316 -> 114,808 (2026-09-07, mount-cache heal generalization, wave 6, dispatch
# 7dfc38a5): `_heal_mount_cache_for_seats` (extracted from promote's own inline heal)
# wired into charter/charter_for/attach-detach's own gap, plus a genuinely new self-
# service verb, seat(action='refresh_project') — a new SEAT_INPUT_SCHEMA branch (no
# params beyond action) plus a one-line ACTION TABLE entry. No new tool, load-bearing
# growth per this file's own escape valve. Measured exact (114,808), not estimated.
# 114,808 -> 115,087 (2026-09-08, Imhotep, wave 10, dispatch 8174/fbd22aef):
# `thread(action='resolve')`'s own ACTION TABLE line gains one clause documenting the
# new `repo:<name>@<hash>` artifact disambiguator (`_find_artifact`, capture.py) and the
# already-true finding that a bare commit hash was never project-scoped — a caller
# reading the tool's own contract needs to know the new shape exists, not just the code.
# No new tool, no new schema param, load-bearing growth per this file's own escape
# valve. Measured exact (115,087), not estimated.
# 114,808 -> 115,179 (2026-09-08, wave 8, thread 07ca68ca, "automatic mechanical fleet
# hygiene"): agent() gains action='fleet_prune' — a new AGENT_INPUT_SCHEMA branch
# (execute only) plus a one-line ACTION TABLE entry, mirroring fleet_reconcile's own
# shape. A genuinely new mechanical-hygiene verb, no new tool, load-bearing growth per
# this file's own escape valve. Measured exact (115,179), not estimated.
# 115,087 -> 115,855 (2026-09-08, Imhotep, wave 11, window-tag-gets-an-owner, decision
# 26f4f825's corollary): `project(action='set_tag')` — a genuinely new PROJECT_INPUT_
# SCHEMA branch (project/tag/because) plus its own ACTION TABLE line, closing the gap
# that left the window's `[TAG]` prefix a pure re-derivation with no persisted override
# and no way to stop fighting an operator's own hand-rename forever. No new tool, load-
# bearing growth per this file's own escape valve. Measured exact (115,855), not
# estimated.
# 115,458 -> 116,027 (2026-09-09, Khnum, thread 085039cc/0cc53329, Thoth DM 2469):
# object_events — a genuinely new read-only tool (merge/unmerge/split events plus
# same_as/not_same_as links for one object, the witness surface dossier() deliberately
# hides), trimmed to a minimal docstring already. New tool, load-bearing growth per
# this file's own escape valve. Measured exact (116,027), not estimated.
# 116,153 -> 116,969 (2026-09-09, operator's word via Thoth DM 8697 item 2): `agent(
# action='retire_governs')` — a genuinely new AGENT_INPUT_SCHEMA branch (agent_id/
# repos/because) plus a one-line ACTION TABLE entry, exposing a THIRD-PARTY per-edge
# governs retirement (agents.py's retire_governs_edges) that never existed: charter_for/
# set_charter only ever write Seat-origin governs edges, structurally blind to the
# Agent-origin ones a stale off-head generation can carry, and
# backfill_agent_project_links's own off-head repair MOVES edges onto the living head --
# exactly wrong for garbage. No new tool, load-bearing growth per this file's own
# escape valve. Measured exact (116,969), not estimated.
# 116,153 -> 116,943 (2026-09-09, operator's word via Thoth DM 8697 item 1): `agent(
# action='invalidate_works_in')` — a genuinely new AGENT_INPUT_SCHEMA branch (agent_id/
# project/because) plus a one-line ACTION TABLE entry, exposing the already-generic
# invalidate_works_in repair (agents.py) for a THIRD-PARTY agent — the existing
# seat(action='invalidate_works_in') door only ever acts on the caller's own mounted
# identity, so nobody could mechanically repair someone ELSE's duplicate works_in edge
# without a raw graph write. No new tool, load-bearing growth per this file's own
# escape valve. Measured exact (116,943), not estimated.
# 118,328 -> 119,302 (2026-09-10, Thoth's ruling via DM 8919/thread 8861 — THE ORPHAN
# LAWS item 2): widened #189's declare-or-refuse gate (_enforce_required_links) to two
# more object-minting doors, `ingest_reference` and `practice(action='record')`/
# `record_practice` — each gained `unlinked_because`/`unlinked_because_kind` params
# (practice's own PRACTICE_INPUT_SCHEMA branch plus both dispatcher docstrings) mirroring
# record_decision/open_thread's own existing shape. No new tool, load-bearing growth per
# this file's own escape valve. Measured exact (119,302), not estimated.
# 118,328 -> 118,628 (2026-09-10, Thoth mail 8921/8922, Metron's mechanism report):
# thread(action='annotate') gains optional corrected_summary/because -- one call now
# fixes a proven-false headline instead of requiring a caller to already know
# correct_summary is a separate verb (the exact affordance gap the report named). No
# new tool, load-bearing growth per this file's own escape valve. Measured exact
# (118,628), not estimated.
# 118,628 -> 118,856 (2026-09-10, Thoth mail 8921/8922, Metron's mechanism report, fix
# (b)): threads()'s own docstring gains one paragraph naming the new `contested` field
# and its `!` marker. No new tool, load-bearing growth per this file's own escape valve.
# Measured exact (118,856), not estimated.
# 116153 -> 118254 (2026-09-10, Imhotep, decision ac892cd9): the new `proposal` tool
# (2,101 chars) — miners as last resort, item 2. A genuinely new door, not prose growth
# on an existing one; raised deliberately, measured exact.
# 121931 -> 123377 (2026-09-10, Khnum, thread badb4040, Thoth mail 9122 item 4, wave
# 16): the new `retire_link` tool (1,446 chars) — retire_assertion's own sibling for
# the link-retraction half of "retract a wrongly-minted X". A genuinely new door
# (Actions.invalidate_link already existed at the kernel level, but reaching it
# directly from a caller would be the raw-mutation shortcut house law forbids — this
# is the MCP-facing door onto it), not prose growth on an existing one; raised
# deliberately, measured exact.
# 121931 -> 123464 (2026-09-10, Sekhmet, thread 7f547426/decision fba38e62, Thoth DM
# 9136, Graph-Engineering arc item 1): the new `record_evaluation` tool — the mint
# door for the new Evaluation object type. A genuinely new door, not prose growth on
# an existing one; raised deliberately, measured exact.
# 123464 -> 124664 (2026-09-10, Sekhmet, thread 7f547426/decision f47d14a7,
# Graph-Engineering arc item 2/3): the new `record_artifact` tool — the mint door for
# the new Artifact object type, gated by _enforce_required_links' new incoming
# direction. A genuinely new door, not prose growth on an existing one; raised
# deliberately, measured exact.
# RECONCILED AT REBASE (2026-09-10, scripts/reconcile_tool_contract_ceiling.py, sekhmet-
# work-lineage onto 8ad6d66): this branch's own 124664 (retire_link's parallel 123377
# never included) additively combined with main's own 123377 (retire_link) against the
# shared c6665b8 baseline of 121931 -> 126106 — never the smaller of the two independent
# raises, so neither door's own measured cost is silently dropped by the rebase.
# 121931 -> 122273 (2026-09-10, Thoth mail 9122 item 1, wave 16, THE RECEIPT LAW audit):
# thread(action='resolve')'s own ACTION TABLE entry gains one sentence disclosing that
# dry_run is inert for a single ref (the schema offers it uniformly for both the single
# and batch shapes) — the only prose growth in this fix; the sibling record_decision/
# reclassify receipt fields this same audit added are pure code, no schema/docstring
# growth of their own. No new tool, load-bearing growth per this file's own escape
# valve. Measured exact (122,273), not estimated.
# 126448 -> 128772 (2026-09-11, Sekhmet, thread 9d2aaf4d, decision c6d25164, Thoth DM
# 9377/9383, CITATION SHAPE): two new doors, cite_transcript and read_citation (see
# TOOL_CONTRACT_EXPECTED_COUNT's own 81->83 log entry just above). Measured exact.
# 126448 -> 126736 (2026-09-11, Khnum, Thoth mail 9382 item 3, 93b25ddc): seat()'s own
# ACTION TABLE entry for charter_for gains a clause naming the new ruling=<decision id>
# escape hatch (act under a standing operator ruling instead of manager authority,
# refused unless the ruling actually names charter_for) — the only prose growth in this
# fix; a genuinely new capability on an existing door, not padding. Measured exact
# (126,736), not estimated.
# 126448 -> 126865 (2026-09-11, decision 12efe065, thread 9d2aaf4d, "THE OPERATOR AS AN
# OBJECT"): record_decision gains one new param, `operator_authorized` — a rulings-carry-
# authority-to-a-real-Person-object mechanism, plus its own one-paragraph docstring entry
# and a matching docstring sentence on `authorized_by`'s widened LinkType. No new tool,
# load-bearing growth per this file's own escape valve. Measured exact (126,865).
# 126448 -> 127077 (2026-09-11, thread 879c97b9 piece 2, Thoth mail 9465, "PROMOTION"):
# seat(action='promote_visitor') — a NEW dispatcher action, not a new @mcp.tool() (tool
# count unchanged), so all of this growth is a new oneOf branch on `seat`'s own
# inputSchema plus a two-line ACTION TABLE entry, never prose on an existing branch. The
# verb's own detailed reasoning (authorization gate, why it calls set_charter rather
# than charter_for) lives in src/orchestrator/walkin.py's docstring, which this ratchet
# does not measure — same split the tree_cwd/bind_seat_tree precedent above already
# established. A genuinely new capability (the visitor-to-soul collapse for a THIRD
# PARTY, operator/manager/ruling-gated), not prose creep; raised deliberately, measured
# exact.
# 129471 -> 130916 (2026-09-11, Imhotep, thread 879c97b9 piece 3, Thoth mail 9559): the
# new `pulse` tool (see TOOL_CONTRACT_EXPECTED_COUNT's own 83->84 log entry just above) —
# a genuinely new capability, not prose growth on an existing tool. Measured exact.
# 130100 -> 130102 (2026-09-11, Khnum, Thoth mail 9541 item 3, cc05c70c, HOUSE VOCABULARY):
# seat()'s own ACTION TABLE line for establish_office corrected from "Osiris-owned home"
# to "Osiris-owned office" — matching its sweep_disk neighbor's own wording and every CLI
# description of the identical ceremony (a real vocabulary-drift specimen an audit found,
# not padding). Two characters, measured exact.
# 131545 -> 131805 (2026-09-11, Sekhmet, thread 6d01f21e, wave 18 item 1, "closed_by real
# sources"): `backfill`'s own docstring gains one new `target=` clause
# (closed_by_real_sources) documenting the sixth dispatch branch — a genuinely new
# capability (the compensating fold for the retired placeholder-Agent shape), not a new
# @mcp.tool() (tool count unchanged; dispatched through the existing `backfill` door, same
# as `lineage_repo_links`/`agent_project_links` before it). No prose growth on any other
# tool. Measured exact (131,805).
# 131807 -> 131996 (2026-09-11, Imhotep, thread 9dc3ce8b, wave 19 item 1, READ-SIDE
# ADOPTION OF THE VISIT CLASS): `fleet`'s own docstring gains one new diagnostic-field
# entry (`agent_classes` — vitals.py's one authority, named_souls/visit_families/
# unresolved_families beside the raw `count`) — a genuinely new field, not prose padding
# on an existing one. No new @mcp.tool(). Measured exact (131,996).
# 131998 -> 133019 (2026-09-11, Imhotep, thread c56f3d94, wave 19 item 2, MAIL IS
# UNSURFACEABLE): `inbox` gains two new params (`as_seat`, `include_settled`) — a
# genuinely new capability (a charter-gated coordinator read of another seat's mail),
# not prose growth on an existing param, dispatched through the existing `inbox` door
# rather than a new @mcp.tool() (tool count unchanged). Measured exact (133,019).
# 133019 -> 133261 (2026-09-11, Sekhmet, thread 1d5b9773, wave 21 piece 1, AUTHORITY BY
# CHARTER): `backfill`'s own docstring gains a seventh `target=` clause
# (operator_charter) documenting the operator's charter backfill (mints `governs` from
# person:operator to every active SoftwareProject it doesn't already cover) — a
# genuinely new capability, not prose growth on any other tool, not a new @mcp.tool()
# (tool count unchanged; dispatched through the existing `backfill` door). Measured
# exact (133,261).
# 135089 -> 136781 (2026-09-12, Imhotep, thread f4498ab304e4, THE SETTINGS MENU piece
# 1): new @mcp.tool() `settings` (list/get/write over the settings registry,
# src/config/settings_registry.py's SETTINGS tuple) — a genuinely new write door this
# domain never had, same class as backup_settings' own raise above. Docstring trimmed
# to the category rule's lean end before raising (dated citations/provenance cut).
# Tool count 85 -> 86. Measured exact (136,781).
# 136781 -> 136834 (2026-09-12, Imhotep, thread c5ba8681, Wave 22 piece 1): `settings`
# list/get gain a `live` counterpart per key (the running/shipped value, null when not
# cheap) — a genuinely new return field, not a new @mcp.tool() (tool count unchanged).
# Trimmed to one clause per action before raising. Measured exact (136,834).
# 136781 -> 137011 (2026-09-12, Sekhmet, thread c89a9873, wave 22): `backfill`'s own
# docstring gains one sentence naming its new CLI/UI siblings (osiris backfill, the
# Repairs panel) — no new @mcp.tool(), tool count unchanged, dispatched through the
# same door as always, now delegating to src.orchestrator.backfill.run_backfill.
# Measured exact (137,011).
# 137064 -> 137307 (2026-09-14, Imhotep, thread 3a9d9a5d89fa, project-owned obligations
# visibility): `threads`'s own docstring gains one clause naming its new
# `project_owned_not_shown` field — no new @mcp.tool(), tool count unchanged. Measured
# exact (137,307).
# 137307 -> 137474 (2026-09-14, Sekhmet, thread e332177f, wave 24 dispatch): `backfill`'s
# own docstring gains one new target, `provenance_possible_upstream` (the provenance
# backfill, back-stamping `possible_upstream` onto historical writes from each write's
# own transcript receipt) — genuinely new repair capability, not prose creep on an
# existing one, same class as a new tool. Trimmed to one clause before raising ("seven"
# corrected to "eight" in the same edit). No new @mcp.tool(), tool count unchanged.
# Measured exact (137,474).
# 137474 -> 137755 (2026-09-14, Sekhmet, thread e332177f, Thoth msg 10525 follow-on):
# `backfill` gains two new params, `limit`/`newest_first`, consulted only by
# provenance_possible_upstream — the fix for "the oldest-first default made the sample
# blind: pre-ledger generations can never match" (Thoth's own words). Genuinely new
# capability on an existing tool, not prose creep; the CLI door (`--limit`/
# `--newest-first`) gained matching flags in the same change, satisfying the CLI/MCP
# parity gate rather than declaring an exemption. No new @mcp.tool(), tool count
# unchanged. Measured exact (137,755).
# 137755 -> 138513 (2026-09-14, Khnum, THE MIGRATION DOOR, Thoth mail 10609): a NEW
# @mcp.tool() `layout_migrate` (86 -> 87 tools) — drives the layout heartbeat's own
# graph_layout.layout_batch to quiescence right now instead of waiting on its cron
# cadence, the SAME function the CLI's `osiris layout --migrate` door and the REST
# `/layout/migrate` route call. Genuinely new capability, no existing tool covers a
# bulk placement pass run to quiescence. Measured exact (138,513).
# 137755 -> 137863 (2026-09-14, Sekhmet, thread 0be2f790, Thoth mail 10626): a real
# 469MB transcript read on osiris-mcp's own event loop thread starved the shared
# fleet-wide connection for 19 minutes — provenance_possible_upstream's own docstring
# clause now says the target runs asynchronously (a job id back at once, the receipt as
# a thread annotation when osiris-worker finishes), a genuine caller-visible behavior
# change on an existing param set, not prose creep. Trimmed once before raising. No new
# @mcp.tool(), tool count unchanged. Measured exact (137,863).
# -> 139260 (2026-09-15, Khnum, thread 92dde6cc): retire_object gained a fourth kind
# ('object', a bare-junk-object retirement door) — a genuine new capability documented
# in its existing docstring, no new @mcp.tool(), tool count unchanged. Measured exact
# (139,260).
# -> 140267 (2026-09-15, Imhotep, thread 2619f011, ruling edb6b0fc): declare_machine_
# identity — a NEW @mcp.tool() (87 -> 88 tools, see TOOL_CONTRACT_EXPECTED_COUNT's own
# changelog entry just below), the manual override for git ingest's own MachineIdentity
# heuristic. Genuinely new capability, no existing tool it could parameterize. Measured
# exact (140,267).
# -> 141046 (2026-09-15, Khnum, Thoth mail 11047, ruling d7d55257): physics_layout_
# migrate — a NEW @mcp.tool() (88 -> 89 tools, see TOOL_CONTRACT_EXPECTED_COUNT's own
# changelog entry just below), THE PHYSICS LAYOUT's own one-shot migration door,
# genuinely different execution shape from layout_migrate (a single global force
# simulation, not a batch loop) so not a parameterization of it. Measured exact
# (141,046).
# -> 141889 (2026-09-17, Sekhmet, ruling 52a59652/70c001ec, ONE TAXONOMY WAVE 28): three
# new schema branches (seat's reissue_seat_dir/establish_seat_dir, agent's
# correct_project) — the canonical replacements for reissue_office/establish_office/
# correct_house, each needing its own oneOf branch by this file's own one-branch-per-
# action convention, the old branches kept unchanged as deprecated aliases for one
# release (never a parameterization of an existing branch, so trimming an existing
# entry would not have avoided this). Docstring prose already trimmed to the bare
# action-table line each; no further category-rule trim available without dropping a
# real action's own description. Tool count unchanged (no new @mcp.tool()). Measured
# exact (141,888).
# The tool-count history mirrors the character ceiling's own: raised for every
# genuinely new tool, unchanged when an existing tool's docstring merely grew or
# shrank, and lowered when several tools were folded into one shared dispatcher (with
# the old names kept as hidden, still-callable, deprecated aliases). Merge conflicts on
# this constant are resolved the same way as the character ceiling: by a fresh
# measurement against the merged tree, never by picking one branch's own number or
# summing the two deltas.
TOOL_CONTRACT_CEILING_CHARS = 140344

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
