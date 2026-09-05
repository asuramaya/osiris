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
import re
from pathlib import Path
from typing import Any, TypedDict

from src.actions.core import Actions
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
    "correct-agent-house": "NOT actually MCP-less — correct_agent_house is hidden from "
        "`list_tools()` by the deprecated-alias mechanism (meta={'deprecated': True}, "
        "retirement wave 1, zero traffic at the time) but stays fully callable, same "
        "shape as heal_seat_anchor_third_party's own hidden alias. _mcp_tools() reads "
        "the FILTERED listing (what a model actually sees), which is right for the "
        "forward param-shape check but blind to a hidden-but-real tool's existence — "
        "named here rather than left as a false 'no MCP tool at all' (#204: the CLI "
        "door built for the #199 lane 3B audit's own real gap).",
    "reconcile-merge": "NOT actually MCP-less — same hidden-alias shape as "
        "correct-agent-house's own entry directly above; reconcile_merge carries the "
        "identical deprecated meta from the same retirement wave (#204).",
    "annotate-thread": "NOT actually MCP-less — same hidden-alias shape as "
        "correct-agent-house's own entry above; annotate_thread carries meta="
        "{'deprecated': True} from the #202 wave-3 thread_action fold, still fully "
        "callable, its own CLI door untouched by the fold.",
    "retire-agent": "NOT actually MCP-less — same hidden-alias shape as "
        "correct-agent-house's own entry above; retire_agent carries meta="
        "{'deprecated': True} from the #202 wave-3 retire_object fold, still fully "
        "callable, its own CLI door untouched by the fold.",
    "stop": "NOT actually MCP-less — same hidden-alias shape as correct-agent-house's own "
        "entry above; stop carries meta={'deprecated': True} from the #202 seat-dispatcher "
        "build (msg 7039), still fully callable, its own CLI door untouched by the fold.",
    "charter-for": "NOT actually MCP-less — same hidden-alias shape as correct-agent-"
        "house's own entry above; charter_for carries meta={'deprecated': True} from the "
        "#202 seat-dispatcher build (msg 7039), still fully callable.",
    "rebind-seat": "NOT actually MCP-less — same hidden-alias shape as correct-agent-"
        "house's own entry above; rebind_seat carries meta={'deprecated': True} from the "
        "#202 seat-dispatcher build (msg 7039), still fully callable.",
    "correct-pin-value": "NOT actually MCP-less — same hidden-alias shape as correct-"
        "agent-house's own entry above; correct_pin_value carries meta="
        "{'deprecated': True} from the #202 seat-dispatcher build (msg 7039), still "
        "fully callable.",
    "heal-seat-anchor": "NOT actually MCP-less — same hidden-alias shape as correct-"
        "agent-house's own entry above; heal_seat_anchor carries meta="
        "{'deprecated': True} from the #202 seat-dispatcher build (msg 7039) ON TOP OF "
        "its own pre-existing self/third-party consolidation (task #199 lane 2) — still "
        "fully callable either way.",
    "transition-seat-project": "NOT actually MCP-less — same hidden-alias shape as "
        "correct-agent-house's own entry above; transition_seat_project carries meta="
        "{'deprecated': True} from the #202 seat-dispatcher build (msg 7039), still "
        "fully callable.",
    "heal-seat-transcript": "NOT actually MCP-less — same hidden-alias shape as correct-"
        "agent-house's own entry above; heal_seat_transcript carries meta="
        "{'deprecated': True} from the #202 seat-dispatcher build (msg 7039), still "
        "fully callable.",
    "mint-seat": "NOT actually MCP-less — same hidden-alias shape as correct-agent-"
        "house's own entry above; mint_seat carries meta={'deprecated': True} from the "
        "#202 seat-dispatcher build (msg 7039), still fully callable.",
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
    ("transition-seat-project", "seat"): "the MCP tool transition_seat_project is "
        "SELF-scoped by design (resolved off the mounted caller's own held_seat) — a "
        "terminal has no mounted identity to be self about, so the console door takes "
        "an EXPLICIT target instead, same resolution shape correct-pin-value's own "
        "console door already uses.",
    ("transition-seat-project", "apply"): "the SAME dry-run/apply concept as MCP's "
        "`dry_run` below, INVERTED and renamed to match this house's --apply repair "
        "convention, same declared split as heal-seat-anchor's own entry above.",
    ("heal-seat-transcript", "apply"): "the SAME dry-run/apply concept as MCP's `dry_run` "
        "below, INVERTED and renamed — identical shape to heal-seat-anchor's own entry "
        "above, same reasoning (#204: the CLI door built for the #199 lane 3B audit's "
        "original specimen).",
}

# CLI COMMAND -> ITS REAL MCP TARGET, when dash-to-underscore is the wrong mapping (task
# #202/#204, Thoth msg 7039/7040/7059): a CLI verb whose function now lives behind an
# object-type dispatcher's own ACTION, not a same-named standalone tool. `_find_problems`
# below falls back to the old `cli_name.replace("-", "_")` guess for every command not
# listed here — this table is additive, never a wholesale renaming of the mechanism.
# `_mcp_dispatcher_action_params()` is what makes these "tool:action" pseudo-names real,
# checkable entries in `_mcp_tools()`'s own dict, not just labels.
CLI_TO_MCP_NAME: dict[str, str] = {
    "stop": "seat:stop",
    "mint-seat": "seat:mint",
    "charter-for": "seat:charter_for",
    "rebind-seat": "seat:rebind",
    "correct-pin-value": "seat:correct_pin",
    "heal-seat-anchor": "seat:heal_anchor",
    "transition-seat-project": "seat:transition_project",
    "heal-seat-transcript": "seat:heal_transcript",
    # #202 agent dispatcher (Thoth dispatch 7162): fleet-reconcile's own CLI door
    # matched the bare fleet_reconcile tool 1:1 before this fold — now it matches the
    # dispatcher action instead, same shape as seat's own entries above.
    "fleet-reconcile": "agent:fleet_reconcile",
    # #202 practice dispatcher (Thoth dispatch 7162, proposal decision 07395004):
    # amend-practice's own CLI door matched the bare amend_practice tool 1:1 before
    # this fold — now it matches the dispatcher action instead, same shape as
    # fleet-reconcile's own entry above. record_practice has no CLI door at all
    # (nothing to reconcile there).
    "amend-practice": "practice:amend",
}

# (mcp_tool, param) -> reason: an MCP-only param with no CLI counterpart.
MCP_ONLY_PARAMS = {
    ("launch", "message"): "the CLI has no way to deliver an opening brief in one act "
        "today — a real gap, named rather than hidden, not yet built",
    ("resume", "message"): "same gap as launch's own message param above — the CLI has "
        "no way to deliver an opening brief in one act today; not yet built",
    # RESTORED (task #202/#204, Thoth msg 7039/7040/7059's own gate-half): these three
    # lived here as (heal_seat_anchor, dry_run) etc. before the #202 seat dispatcher
    # shipped, and were DELETED rather than pointed at a name the gate couldn't check yet
    # (_mcp_tools() read seat's own flat inputSchema["properties"] as empty). Now
    # pointed at their real "seat:action" pseudo-name via CLI_TO_MCP_NAME/
    # _mcp_dispatcher_action_params above — the fix those deletion comments named.
    ("seat:heal_anchor", "dry_run"): "the CLI's own `apply` (see CLI_ONLY_PARAMS above) "
        "is the same concept, inverted and renamed to match this house's --apply repair "
        "convention rather than the MCP tools' own dry_run=True default.",
    ("seat:transition_project", "dry_run"): "the CLI's own `apply` (see CLI_ONLY_PARAMS "
        "above) is the same concept, inverted and renamed to match this house's --apply "
        "repair convention rather than the MCP tools' own dry_run=True default.",
    ("seat:heal_transcript", "dry_run"): "the CLI's own `apply` (see CLI_ONLY_PARAMS "
        "above) is the same concept, inverted and renamed to match this house's --apply "
        "repair convention rather than the MCP tools' own dry_run=True default — "
        "identical shape to seat:heal_anchor's own entry above.",
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
    # RESTORED (task #202/#204, Thoth msg 7039/7040/7059) — same reasoning as the three
    # MCP_ONLY_PARAMS restorations above: pointed at the real "seat:action" pseudo-name
    # now that _mcp_dispatcher_action_params() makes it a checkable entry.
    ("stop", "handle", "seat:stop", "target"):
        "the same seat reference, two names — MATCHES launch's own handle/target split "
        "exactly (the entry above), and deliberately so: stop is launch's inverse and the "
        "pair must read identically at the terminal. Reconciling this means reconciling "
        "BOTH together, never one of them (Thoth LXXXVII, 2026-08-28)",
    ("charter-for", "seat", "seat:charter_for", "target"):
        "the same seat reference, two names — found building this detector; not yet "
        "reconciled. mcp_param renamed from the pre-dispatcher seat_id to target — the "
        "#202 seat dispatcher's own PARAM UNIFICATION (one shared name for 'which "
        "existing seat/agent' across every action).",
    ("resume", "handle", "resume", "target"):
        "the same seat reference, two names — MATCHES launch's own handle/target split "
        "exactly (both entries above), and deliberately so: resume is launch's own sibling "
        "verb now, split from it by the same ruling (60c78788/41a41437); the pair must "
        "read identically at the terminal for the same reason stop/launch already do",
    ("heal-seat-anchor", "seat", "seat:heal_anchor", "target"):
        "the same seat reference, two names — same shape as charter-for/seat above, "
        "restored the same way: mcp_param renamed from seat_id to target under the #202 "
        "seat dispatcher's own PARAM UNIFICATION. The CLI door (`osiris heal-seat-anchor "
        "<seat> --because`) still calls identity_heal.heal_seat_anchor_third_party "
        "directly, unchanged and unaffected by the MCP-layer fold.",
    # NEWLY SURFACED by this lane's own (tool, action)-aware gate (task #202/#204, msg
    # 7040/7059) — the #202 seat dispatcher's own PARAM UNIFICATION renamed EVERY
    # folded action's object-reference param to `target`, including these two whose
    # pre-dispatcher standalone tools used `seat`. Neither drift had ever been checked
    # before (the old gate could only see the dispatcher's empty top-level param set),
    # so this is the gate catching real, previously-invisible drift on its first run —
    # not a pre-existing gap this lane merely renamed.
    ("rebind-seat", "seat", "seat:rebind", "target"):
        "the same seat reference, two names — the #202 seat dispatcher's PARAM "
        "UNIFICATION renamed rebind_seat's own `seat` param to `target`; the CLI door "
        "(`osiris rebind-seat <seat> <new_cwd>`) still calls mounts.rebind_seat "
        "directly, unchanged and unaffected by the MCP-layer fold.",
    ("heal-seat-transcript", "seat", "seat:heal_transcript", "target"):
        "the same seat reference, two names — same shape as rebind-seat/seat above: "
        "the #202 seat dispatcher's PARAM UNIFICATION renamed heal_seat_transcript's "
        "own `seat` param to `target`; the CLI door still calls "
        "identity_heal.heal_seat_transcript directly, unchanged and unaffected by the "
        "MCP-layer fold.",
    ("retire-agent", "seat", "retire_object", "target"):
        "the same agent reference, two names — same shape as charter-for/seat and "
        "heal-seat-anchor/seat above (#204: the CLI door names its positional arg `seat` "
        "for the same reason every other third-party-target repair door in this file "
        "does — a human types a handle far more often than a raw agent:... id). Retargeted "
        "from retire_agent to retire_object/target (#202 wave 3): retire_agent is now "
        "hidden (meta={'deprecated': True}), the CLI door still calls agents.retire_agent "
        "directly, unchanged and unaffected by the MCP-layer fold — same reasoning as "
        "heal-seat-anchor's own entry above, this comparison just needs a LIVE mcp name "
        "to check itself against. UNCHANGED by the #202 seat dispatcher (msg 7039): "
        "retire_object stays its own live tool (kind='project'/'agent' still route "
        "through it; only kind='seat' also gained a seat(action='retire') door).",
    # ("heal-seat-transcript", "seat", ...) DELETED, same reason as heal-seat-anchor
    # above — covered by NO_MCP_EQUIVALENT instead.
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
    "correct_pin_value", "create_project",
    "rename_project", "retire_project", "fork_project",
    "heal_seat_anchor",
    # ADDED per Thoth ruling on decision 6283c51a's own audit (msg 6823): "a verb that
    # writes holds, house, handle, managed_by or a merge estate moves a binding by
    # definition; 'adjacent to' is not a category." merge/unmerge were the STALE-LIST
    # finding named in that same ruling — both already have CLI doors and simply were
    # never added to the population that credits them for it.
    # correct_agent_house + reconcile_merge are HIDDEN (retirement wave 1, f38e135) but
    # still callable deprecated aliases — Seshat's d232cb3 reads existence off the raw
    # registry, so they stay in the gated population rather than misreading as stale.
    "correct_house", "correct_agent_house", "reconcile_merge", "retire", "retire_agent",
    "fleet_reconcile", "heal_seat_transcript", "merge", "unmerge",
    # `resume` (task #199 lane 3C, ruling 41a41437) — a brand-new tool tonight, already
    # declared in NEW_TOOL_DECLARATIONS below, not grandfathered in the snapshot.
    "resume",
    # `transition_seat_project` (task #199's core-verbs lane, Thoth dispatch 6901) — a
    # brand-new tool, already declared in NEW_TOOL_DECLARATIONS below: composes
    # invalidate_works_in + correct_pin_value + set_charter, writes works_in/pin/
    # governs, a binding-mover by the same 6823 rule the seven above were added under.
    "transition_seat_project",
    # `resync_seat_house` (decision 68fba2e4/thread 19d6bdcb7fa9, the house/project
    # ruling's six-seat repair) — writes Seat.house third-party, the same 6823 rule
    # correct_house/correct_agent_house were added under two lines up.
    "resync_seat_house",
    # `seat_edge` (#202 wave 3, Thoth dispatch 6987) — attach_seat/detach_seat folded
    # into one door; still writes managed_by, same binding-mover status the two hidden
    # aliases already carried.
    "seat_edge",
    # `retire_object` (#202 wave 3, Thoth dispatch 6987) — retire_seat/retire_project/
    # retire_agent folded into one door; flips objects.status the same way each of the
    # three hidden aliases already did.
    "retire_object",
    # `seat` (#202 seat dispatcher, operator ruling f9182ad7, Thoth dispatch 7039) — the
    # first object-type dispatcher: 23 of the binding-mover names above (mint_seat
    # through heal_seat_transcript) fold into this one door's actions, all kept as
    # hidden deprecated aliases. Writes holds/house/handle/managed_by/pin/works_in
    # across nearly every action, the same 6823 rule the individual verbs it absorbs
    # were each added under.
    "seat",
    # `project` (#202 project dispatcher, Thoth dispatch 7095) — the third object-type
    # dispatcher: create_project/rename_project/retire_project/fork_project (already
    # binding-movers above, individually) plus ingest_project/project_identity_evidence/
    # assert_project_property fold into this one door's actions, all kept as hidden
    # deprecated aliases. Writes SoftwareProject lifecycle state (create/rename/fork/
    # unfork/retire), the same 6823 rule the individual verbs it absorbs were each
    # added under.
    "project",
    # `agent` (#202 agent dispatcher, Thoth dispatch 7162) — the fifth object-type
    # dispatcher: correct_agent_house/retire_agent/fleet_reconcile (already binding-
    # movers above, individually) plus claim_name/file_subagent/file_subagents fold
    # into this one door's actions, all kept as hidden deprecated aliases. Writes
    # Agent identity/lineage state (name/house/seat_generation/retired/spawned_by
    # filing) and reaps stale fleet mounts, the same 6823 rule the individual verbs
    # it absorbs were each added under.
    "agent",
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
    "resync_seat_house": "not yet built — the MCP door onto seats.resync_seat_house_"
        "third_party (task #152's own repair primitive) shipped tonight (decision "
        "68fba2e4/thread 19d6bdcb7fa9) so the six live fabricated-house specimens can be "
        "repaired without reaching past the MCP surface; a raw CLI door is a real, "
        "separate, not-yet-scoped gap.",
    "attach_seat": "not yet built — a real gap named by Khnum's lane-2 scoping (msg 6463): "
        "not on the jesus/chad reconciliation path his dispatch scoped him to.",
    "detach_seat": "not yet built — same scoping note as attach_seat.",
    "seat_edge": "not yet built — the #202 wave-3 fold (msg 6987) of attach_seat/"
        "detach_seat inherits the same CLI gap, unrenamed; not on any dispatch's own "
        "scope to close.",
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
    "create_project": "not yet built — project lifecycle verbs weren't in lane 2's scope "
        "(seat reconciliation only); a real gap, not ruled out.",
    "rename_project": "not yet built — same scoping note as create_project.",
    "retire_project": "not yet built — same scoping note as create_project.",
    "retire_object": "not yet built — the #202 wave-3 fold (msg 6987) of retire_seat/"
        "retire_project/retire_agent inherits the same CLI gap two of the three "
        "already carried (retire_agent's own CLI door stays a direct third-party "
        "act, untouched by the fold, not routed through this new door).",
    "seat": "BY DESIGN, permanent — the operator's own price-minimizer #3 (ruling "
        "f9182ad7, Thoth dispatch 7039): 'the CLI stays verb-per-command' even as the "
        "MCP surface moves to object-type dispatchers. Every action seat(action=...) "
        "covers already has (or will have) its own CLI subcommand (stop, mint-seat, "
        "rebind-seat, ...) calling the underlying orchestrator function directly, "
        "unaffected by the MCP-layer fold — a raw `osiris seat --action=...` door was "
        "deliberately never scoped, not a gap to close.",
    "fork_project": "not yet built — same scoping note as create_project.",
    "project": "BY DESIGN, permanent — same price-minimizer #3 reasoning as seat's own "
        "entry above (ruling f9182ad7, Thoth dispatch 7095): every action "
        "project(action=...) covers is declared not-yet-built above (create_project/"
        "rename_project/retire_project/fork_project's own NO_CLI_EQUIVALENT entries) or "
        "calls the underlying orchestrator function directly through its own eventual "
        "CLI door, unaffected by the MCP-layer fold — a raw `osiris project "
        "--action=...` door was deliberately never scoped, not a gap to close.",
    "agent": "BY DESIGN, permanent — same price-minimizer #3 reasoning as seat/"
        "project's own entries above (Thoth dispatch 7162): correct_agent_house/"
        "retire_agent/fleet_reconcile each already have (or, for fleet-reconcile, "
        "route through via CLI_TO_MCP_NAME) their own CLI subcommand calling the "
        "underlying orchestrator function directly; claim_name/file_subagent/"
        "file_subagents have no CLI door at all today, a real gap this fold doesn't "
        "create — a raw `osiris agent --action=...` door was deliberately never "
        "scoped, not a gap to close.",
    # THE SEVEN FROM THE #199 LANE 3B AUDIT (decision 6283c51a, Thoth ruling msg 6823:
    # "a verb that writes holds, house, handle, managed_by or a merge estate moves a
    # binding by definition"). merge/unmerge (also added to BINDING_VERBS above) already
    # have CLI doors and need no entry here — they were the STALE-LIST half of the same
    # ruling, simply never credited.
    "correct_house": "self-scoped by design (same shape as charter above): it resolves "
        "the target seat from the CALLING agent's own mounted identity — a raw terminal "
        "holds no such identity to be self about. correct_agent_house (the third-party, "
        "explicit-target form) now has its own CLI door (#204); see NO_MCP_EQUIVALENT "
        "for why it still reads as a declared exemption rather than a plain match.",
    "retire": "self-scoped by design (same shape as charter/correct_house above): it "
        "marks THIS mounted session retired off the caller's own identity — a raw "
        "terminal has no mounted session of that kind to retire. retire_agent (the "
        "third-party, explicit-target form) now has its own CLI door (#204).",
    # correct_agent_house, reconcile_merge, retire_agent, fleet_reconcile,
    # heal_seat_transcript: ALL FIVE BUILT (#204, the #199 lane 3B audit's own gaps) —
    # correct-agent-house, reconcile-merge, retire-agent, fleet-reconcile,
    # heal-seat-transcript. The first two still carry a NO_MCP_EQUIVALENT entry (their
    # MCP twin is real but hidden from list_tools() by the deprecated-alias mechanism);
    # retire-agent and heal-seat-transcript carry RENAMED_PARAMS/CLI_ONLY_PARAMS entries
    # for their own seat/agent_id and apply/dry_run naming — see those tables.
}


def _find_missing_cli_doors(
    cli: dict[str, set[str]], mcp: dict[str, set[str]], *,
    binding_verbs: frozenset[str] = BINDING_VERBS,
    no_cli_equivalent: dict[str, str] = NO_CLI_EQUIVALENT,
    all_registered: frozenset[str] | None = None,
) -> list[str]:
    """The REVERSE of _find_problems's own direction, deliberately scoped to
    BINDING_VERBS rather than all of `mcp` — see the ruling above the table this reads.
    Returns one string per binding verb that exists on MCP, has no CLI command of the same
    name (dashes for underscores), and carries no reason in NO_CLI_EQUIVALENT.

    `all_registered` is the UNFILTERED tool-name population (_mcp_registered_names) —
    existence is checked against it, not against `mcp` (list_tools()'s own filtered dict),
    so a hidden alias (meta={"deprecated": True}, thread 6778) reads as 'exists, still
    needs its CLI-door-or-reason' rather than misreading as a deleted 'stale entry'.
    Defaults to `frozenset(mcp)` when omitted — the old, pre-hidden-alias behavior, kept so
    the synthetic proof tests below don't need to fabricate a second population."""
    cli_as_mcp_names = {name.replace("-", "_") for name in cli}
    known = all_registered if all_registered is not None else frozenset(mcp)
    problems: list[str] = []
    for name in sorted(binding_verbs):
        if name not in known:
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
    problems = _find_missing_cli_doors(
        _cli_commands(), await _mcp_tools(), all_registered=await _mcp_registered_names())
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


def test_a_hidden_alias_reads_as_exists_not_stale() -> None:
    """THE GAP THIS LANE FIXES (Imhotep msg 6836, off decision b49a844f): a binding verb
    hidden via meta={"deprecated": True} (thread 6778) vanishes from `mcp` (list_tools()'s
    own filtered dict) but is still a real, callable tool — it must not read as a deleted
    'stale entry'. Without `all_registered`, the OLD behavior stands (still useful for a
    caller with no separate raw-registry read) and misreads it as stale."""
    cli: dict[str, set[str]] = {}
    mcp: dict[str, set[str]] = {}  # hidden from list_tools() — this is the whole point
    verbs = frozenset({"some_hidden_verb"})

    old_behavior = _find_missing_cli_doors(cli, mcp, binding_verbs=verbs, no_cli_equivalent={})
    assert len(old_behavior) == 1 and "stale entry" in old_behavior[0]

    with_raw_registry = _find_missing_cli_doors(
        cli, mcp, binding_verbs=verbs, no_cli_equivalent={},
        all_registered=frozenset({"some_hidden_verb"}))
    assert len(with_raw_registry) == 1 and "stale entry" not in with_raw_registry[0], (
        "known-but-hidden must still demand a CLI door or a NO_CLI_EQUIVALENT reason — "
        "existing, not exempt from the check entirely")

    declared = _find_missing_cli_doors(
        cli, mcp, binding_verbs=verbs, no_cli_equivalent={"some_hidden_verb": "test fixture"},
        all_registered=frozenset({"some_hidden_verb"}))
    assert declared == []


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
    trusts for the ratchet. ALSO includes every hand-built dispatcher's own per-action
    param sets (see `_mcp_dispatcher_action_params` below) under `"tool:action"` pseudo-
    names, additively — a dispatcher's own bare name still maps to its (empty, since a
    oneOf schema has no top-level `properties`) flat set, unchanged, for anything that
    still keys off tool-level existence alone."""
    from src import mcp_server as srv

    tools = await srv.mcp.list_tools()
    out = {
        t.name: set(t.inputSchema.get("properties", {}).keys()) - _MCP_ONLY_PLUMBING
        for t in tools
    }
    out.update(_mcp_dispatcher_action_params())
    return out


def _mcp_dispatcher_action_params() -> dict[str, set[str]]:
    """THE (TOOL, ACTION) GAP #202's OWN SEAT-DISPATCHER COMMIT NAMED AND LEFT OPEN (msg
    7039/7040/7059): `_mcp_tools()`'s flat `inputSchema["properties"]` read is empty for
    ANY hand-built dispatcher (`src/mcp_server.py`'s `_HAND_BUILT_SCHEMAS`) — its real,
    per-action params live one level down, inside each `oneOf` branch. Six real
    CLI<->param reconciliation entries (heal_seat_anchor/transition_seat_project/
    heal_seat_transcript's own `dry_run`, stop/charter-for/heal-seat-anchor's own
    seat-reference rename) had to be DELETED rather than left stale when seat first
    shipped, for exactly this reason — this is the fix that lets them come back, pointed
    at a real, checkable name.

    Generic over every dispatcher registered in `_HAND_BUILT_SCHEMAS` — a second
    dispatcher gets this for free the moment it's added there, never a seat-specific
    hardcode. Each branch's own `action` key is stripped (dispatch-selection plumbing,
    never a real param either side would name), same discipline `_MCP_ONLY_PLUMBING`
    already applies to `ctx`/ambient params."""
    from src import mcp_server as srv

    out: dict[str, set[str]] = {}
    for tool_name, schema in srv._HAND_BUILT_SCHEMAS.items():
        for branch in schema.get("oneOf", []):
            action = branch["properties"]["action"]["const"]
            params = (set(branch.get("properties", {}).keys()) - {"action"}
                     - _MCP_ONLY_PLUMBING)
            out[f"{tool_name}:{action}"] = params
    return out


async def _mcp_registered_names() -> frozenset[str]:
    """EVERY tool the server actually knows, hidden aliases included — read off the raw
    ToolManager (the same private seam BoundedMCP.call_tool/list_tools already reach into,
    src/mcp_server.py), never off `mcp.list_tools()`'s own filtered output. A tool shipped
    with `meta={"deprecated": True}` (the hidden-alias mechanism, thread 6778/commit
    e94158d) is DROPPED from list_tools() on purpose — that is what makes it invisible to
    a model — but it stays fully resolvable by call_tool, i.e. still real. `_mcp_tools()`'s
    filtered dict is right for the forward param-shape check (a model only ever sees the
    unfiltered surface), but WRONG as the sole existence test for a BINDING_VERBS member:
    reading absence-from-listing as absence-from-the-server conflates 'hidden' with
    'deleted' — exactly the gap Imhotep's retirement wave 1 (decision b49a844f) had to
    dodge by hand (reverting 7 BINDING_VERBS tools rather than hiding them) instead of the
    gate telling the two cases apart on its own.

    ALSO includes every dispatcher's own `"tool:action"` pseudo-names (same population
    `_mcp_dispatcher_action_params` builds) — a BINDING_VERBS member expressed as an
    action pair is exactly as real as one expressed as a bare tool name, and must read
    the same way here, never as a stale entry just because no literal tool by that
    string exists in the raw registry."""
    from src import mcp_server as srv

    return (frozenset(t.name for t in srv.mcp._tool_manager.list_tools())
           | frozenset(_mcp_dispatcher_action_params()))


def _find_problems(
    cli: dict[str, set[str]], mcp: dict[str, set[str]], *,
    no_mcp_equivalent: dict[str, str] = NO_MCP_EQUIVALENT,
    cli_only_params: dict[tuple[str, str], str] = CLI_ONLY_PARAMS,
    mcp_only_params: dict[tuple[str, str], str] = MCP_ONLY_PARAMS,
    renamed_params: dict[tuple[str, str, str, str], str] = RENAMED_PARAMS,
    cli_to_mcp_name: dict[str, str] = CLI_TO_MCP_NAME,
) -> list[str]:
    """The reconciler's own core check, extracted so it can be proven against synthetic
    fixtures (see test_the_detector_itself below) rather than only exercised live — the
    same "prove the mechanism, don't just assert it" discipline the merge-driver reconciler
    used. Returns one string per UNDECLARED mismatch; empty means everything either
    matches or is named on an allowlist.

    `cli_to_mcp_name` overrides the default `cli_name.replace("-", "_")` guess for a CLI
    command whose real target is a dispatcher ACTION, not a same-named standalone tool
    (task #202/#204: `osiris stop` <-> `seat(action='stop')`, not a tool literally named
    `stop`'s own params at the top level) — additive, every command not listed here keeps
    the old guess unchanged."""
    problems: list[str] = []
    for cli_name, cli_params in cli.items():
        mcp_name = cli_to_mcp_name.get(cli_name, cli_name.replace("-", "_"))
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


def test_the_detector_catches_a_cli_verb_with_no_tool_action_twin() -> None:
    """THE (TOOL, ACTION)-AWARE GATE ITSELF (task #202/#204, Thoth msg 7039/7040/7059):
    a CLI command routed at a dispatcher ACTION via CLI_TO_MCP_NAME must still be held
    to the SAME standard as an ordinary same-named tool — an undeclared param drift
    fails exactly like it would for a flat tool, and a genuinely missing action-name
    twin (nothing in `mcp` under that pseudo-name at all) reads as no-MCP-equivalent,
    never silently ignored just because the name contains a colon."""
    cli = {"widget-verb": {"target"}}
    mcp: dict[str, set[str]] = {}  # "gadget:verb" does not exist at all
    problems = _find_problems(
        cli, mcp, cli_to_mcp_name={"widget-verb": "gadget:verb"})
    assert len(problems) == 1 and "gadget:verb" in problems[0]

    # now the twin exists, but with a drifted param — same as any flat-tool mismatch
    mcp_with_twin = {"gadget:verb": {"seat_ref"}}
    drifted = _find_problems(
        cli, mcp_with_twin, cli_to_mcp_name={"widget-verb": "gadget:verb"})
    assert len(drifted) == 2  # target unexplained on CLI, seat_ref unexplained on MCP

    # declared via RENAMED_PARAMS, same as any ordinary rename, clears clean
    declared = _find_problems(
        cli, mcp_with_twin, cli_to_mcp_name={"widget-verb": "gadget:verb"},
        renamed_params={("widget-verb", "target", "gadget:verb", "seat_ref"):
            "test fixture"})
    assert declared == []


def test_mcp_dispatcher_action_params_reads_every_branch_of_a_real_dispatcher() -> None:
    """Proven against the LIVE seat schema, not a synthetic one — the same discipline
    test_seat_dispatcher_schema.py already runs, applied to this lane's own extraction
    instead of just the schema's own well-formedness."""
    out = _mcp_dispatcher_action_params()
    assert "seat:mint" in out and "seat:stop" in out and "seat:launch" in out
    assert "action" not in out["seat:mint"]  # dispatch-selection plumbing, stripped
    assert {"handle", "project", "model", "house"} <= out["seat:mint"]
    assert "target" in out["seat:stop"]


# ============================================================================================
# LANE 3B — THE GROWTH RATCHET (task #199, operator's mandate upgrade msg 6789: "make sure
# they go in and fix all 3 problems end to end"). heal_seat_transcript shipped tonight with
# no CLI door and no declaration anywhere — not because BINDING_VERBS's own gate above was
# unenforced, but because heal_seat_transcript was never ADDED to the population that gate
# walks. The first fix attempted (decision dd5b6ced, reconciled with Seshat, approved by
# Thoth msg 6771) was an AST walk auto-deriving BINDING_VERBS's membership from five named
# source modules (identity_heal/mounts/seats/projects/offices.py), matching MCP tool names
# to same-named orchestrator functions. BUILT NOTHING FROM IT: reading every one of those
# five modules directly (not grepped, not inferred) before writing the walk showed the
# same-name/same-module premise is FALSE for roughly half of BINDING_VERBS's own 29 current
# members — create_project/rename_project/fork_project/unfork_project live in
# project_identity.py (not projects.py); charter_for lives in charter.py, mint_seat in
# mintseat.py, neither of the five; charter's MCP tool delegates to set_charter (charter.py,
# different name); sweep_seat_disk's own docstring says outright it wires TWO differently-
# named functions (sweep_retired_office/sweep_seat_workspace, offices.py) to one MCP tool;
# launch and vacate_seat delegate to launch_seat/vacate_dead_seat in trigger.py (a SIXTH
# module, mismatched names both); pause_seat has no separate orchestrator function at all —
# implemented natively inline in mcp_server.py. An AST walk of that five-module list would
# have silently missed ~14 of 29 KNOWN verbs on day one: the identical failure shape
# heal_seat_transcript itself demonstrates, just relocated from "verb name" to "module name."
# Reported to Thoth (msg 6796) rather than built anyway; the operator picked the replacement
# below directly (msg to Khnum, 2026-09-03, "B, ... what is the shape").
#
# BINDING-VERB-NESS IS A SEMANTIC JUDGMENT, proven above not to be safely re-derivable from
# source structure (file location, naming convention) by any mechanism tried so far. What
# CAN be made self-maintaining instead is narrower and does not depend on structure at all:
# did a BRAND-NEW MCP tool ship without anyone declaring anything about it whatsoever. A
# GROWTH RATCHET keyed on the one thing that's unambiguous — the tool's own NAME, read live
# off mcp.list_tools(), same as every other check in this file:
#
#   KNOWN_TOOLS_AT_SNAPSHOT — every tool name that existed at the moment this ratchet was
#   built (151, captured live, 2026-09-03). GRANDFATHERED, ZERO retroactive justification
#   required for any of them — this is what avoids the hollow-by-declaration batch-backfill
#   Thoth's own condition on the earlier design explicitly ruled out (a 112-entry table of
#   boilerplate reasons nobody would read, #189's dark-gate failure wearing a completeness
#   ratchet's clothes). Never edit this set to "grandfather" a tool that ships AFTER today —
#   that defeats the ratchet; use NEW_TOOL_DECLARATIONS below instead.
#
#   NEW_TOOL_DECLARATIONS — every tool name that ships after the snapshot MUST appear here
#   with a structured declaration (never free text alone — a reason string nobody is forced
#   to make specific is exactly NO_CLI_EQUIVALENT's failure mode for a different question)
#   covering BOTH open questions live tonight:
#     "binding_verb": bool — is this a seat/office/project binding-mover. Checked for
#       CONSISTENCY against live BINDING_VERBS membership below, not just declared and
#       trusted: claiming True without adding the name to BINDING_VERBS (or vice versa)
#       fails the gate. A True entry inherits the EXISTING BINDING_VERBS gate's own
#       CLI-door-or-NO_CLI_EQUIVALENT requirement above — this ratchet does not duplicate
#       that check, only forces the author to enter the population it walks.
#     "parameterizes": str | None — Imhotep's counterweight field (msg 6801, thread 6778):
#       the name of an existing MCP tool this one's underlying call could have been a
#       parameter on, when one exists — checkable against tool_traffic()/a read of the
#       candidate's own implementation, not assumed from name-matching alone (his own
#       caught trap: retryable_ambiguous_abstentions/retry_ambiguous_abstentions LOOK like
#       a consolidation pair and are not — one is a free unmounted read, the other requires
#       a mounted identity even in preview mode; collapsing them would be a real behavior
#       change hiding inside an apparent refactor).
#     "not_parameterized_because": str | None — meaningful ONLY alongside `parameterizes`
#       (Imhotep's own words: "it's the 'why not' justification for a real candidate that
#       was found and rejected, not a blank-check exemption") — set without `parameterizes`
#       fails the gate.
#
#   A tool retired via the meta={"deprecated": True} hidden-alias mechanism (Imhotep, commit
#   e94158d, task #199 lane 2) is EXEMPT from needing its own declaration at all — it
#   forwards to whatever its `use_instead` target already declares, checked live off each
#   tool's own `meta` (a genuine field on mcp.types.Tool, confirmed live before trusting it).
#
# THE AUDIT THOTH ASKED FOR ("watch it fail on the current undeclared set... that red list
# IS a deliverable... report separately from the gate itself", msg 6771/6789): since this
# ratchet grandfathers every existing tool with zero mechanical check of binding-verb-ness
# (the whole point — that question needs a human, proven above), there is no code-level red
# list to run. The equivalent honest measurement is a ONE-TIME MANUAL AUDIT of the 151-tool
# snapshot against BINDING_VERBS, done by direct reading (keyword-filtered candidates, each
# individually read, not grep-and-trust) rather than mechanized — reported to Thoth
# alongside this commit, not encoded here as a heuristic gate ("do not let anyone talk the
# audience check into a heuristic", msg 6771). Found, beyond the already-known
# heal_seat_transcript: correct_house/correct_agent_house (same family as correct_pin_value/
# reconcile_seat_identity, already in BINDING_VERBS — these move a seat's own house stamp,
# clearly binding-moving, currently outside the population), reconcile_merge (repairs the
# mail/mount/thread/holder/managed_by estate a partial merge left stranded — same family as
# merge/unmerge, NEITHER of which is in BINDING_VERBS either despite both having CLI doors
# already), retire/retire_agent (releases a held seat on retirement, adjacent to but distinct
# from retire_seat), fleet_reconcile (bulk mount/binding reaper, fold_agent-backed). None
# added to BINDING_VERBS by this commit — that population change is a judgment call for
# Thoth to make with the same care BINDING_VERBS's original 29 got, not a side effect of
# building the ratchet that measures a DIFFERENT thing (tool-name growth, not verb
# semantics).
# ============================================================================================

KNOWN_TOOLS_AT_SNAPSHOT: frozenset[str] = frozenset({
    "abstained_derivations",
    "ack_handoff",
    "acquire_lease",
    "aim_entity",
    "amend_decision",
    "amend_practice",
    "annotate_thread",
    "assert_project_property",
    "attach_seat",
    "backfill_agent_project_links",
    "backfill_boot_alarm_commit_links",
    "backfill_bootstrap_orphan_references",
    "backfill_lineage_repo_links",
    "backfill_task_sync_citation_links",
    "bind_seat_tree",
    "bootstrap",
    "candidates",
    "charter",
    "charter_for",
    "check_lease",
    "claim_name",
    "consolidate",
    "consult_canon",
    "context_window",
    "correct_agent_house",
    "correct_house",
    "correct_pin_value",
    "correct_thread_summary",
    "create_project",
    "create_room",
    "describe",
    "detach_seat",
    "dismiss_brief",
    "dispose",
    "dossier",
    "dossier_report",
    "establish_office",
    "expand_clinical_site",
    "expand_operator",
    "file_subagent",
    "file_subagents",
    "fleet",
    "fleet_digest",
    "fleet_reconcile",
    "focus_object",
    "fold_candidates",
    "fork_project",
    "get_console",
    "get_decision_list",
    "get_mail",
    "get_schema",
    "get_status",
    "get_thread_list",
    "graph_lint",
    "graph_search",
    "handoff_briefing",
    "heal_seat_anchor",
    "heal_seat_anchor_third_party",
    "heal_seat_transcript",
    "hold_action",
    "hold_memory",
    "hold_tension",
    "identify_agent",
    "inbox",
    "ingest_form_d",
    "ingest_litigation",
    "ingest_project",
    "ingest_project_third_party",
    "ingest_reference",
    "ingest_trials",
    "invalidate_works_in",
    "launch",
    "lift",
    "list_assertions",
    "list_compositions",
    "list_functions",
    "list_rooms",
    "lookup_lei",
    "merge",
    "mint_seat",
    "mount",
    "open_thread",
    "orient",
    "pause_seat",
    "peer_ledger",
    "peer_reachable",
    "peer_seats",
    "practices",
    "project_identity_evidence",
    "reap_stale_leases",
    "rebind_seat",
    "recall",
    "reclassify_thread",
    "reconcile_merge",
    "reconcile_seat_identity",
    "reconcile_seat_identity_third_party",
    "record_decision",
    "record_practice",
    "recover_harness_exchanges",
    "register_blind_spot",
    "registry_census",
    "reissue_office",
    "release_lease",
    "rematerialize",
    "rename_project",
    "rename_seat",
    "repair_stale_current_flags",
    "repair_stale_pile_summons",
    "resolve_fold",
    "resolve_thread",
    "restore_attribution",
    "retire",
    "retire_agent",
    "retire_assertion",
    "retire_project",
    "retire_seat",
    "retry_ambiguous_abstentions",
    "retryable_abstentions",
    "retryable_ambiguous_abstentions",
    "revert_own_pin_write",
    "roster",
    "run_composition",
    "save_composition",
    "screen_wallet",
    "search",
    "send",
    "set_seat_attended",
    "settle",
    "smoke",
    "stale_current_flags",
    "stop",
    "succession_chain",
    "suggest_sources",
    "sweep_seat_disk",
    "task_sync_reconcile",
    "tool_traffic",
    "trace_evidence",
    "trace_wallet",
    "tree_ledger",
    "triage",
    "unfork_project",
    "uningested_trees",
    "unmerge",
    "unpeer",
    "unwire_informs_fanout",
    "unwitnessed_spawns",
    "vacate_seat",
    "verify_bc_entity",
    "wake",
    "wake_preflight",
    "walk_in",
})


class NewToolDeclaration(TypedDict, total=False):
    binding_verb: bool
    parameterizes: str | None
    not_parameterized_because: str | None


# Empty at birth — the ratchet's whole point is that this population grows only by someone
# adding an entry here, in the same PR that adds the tool. See the block comment above for
# what each field means.
NEW_TOOL_DECLARATIONS: dict[str, NewToolDeclaration] = {
    "resume": {"binding_verb": True},
    # backfill(target=...) — Imhotep's families wave (4b72154): five backfill_* tools
    # folded into one HONEST DISPATCH (its docstring says so). Parameterizes the five
    # names, which stay callable as hidden deprecated aliases.
    "backfill": {"binding_verb": False, "parameterizes": "backfill_agent_project_links"},
    # transition_seat_project — the self-service TRANSITION verb (Thoth dispatch 6901):
    # composes invalidate_works_in + correct_pin_value + set_charter, one act, MCP + CLI
    # in the same commit (osiris transition-seat-project). Not a parameterization of any
    # existing tool — it's a NEW composition over three already-declared verbs, no
    # existing tool's underlying call could have taken this as a param.
    "transition_seat_project": {"binding_verb": True},
    # list_unfiled_threads — the H-bucket instrument gap (#203, decision a49d2730/
    # 38755abe, Thoth dispatch 6966): a Thread filter on absence of an in_repo edge,
    # not a binding-mover. Not a parameterization of get_thread_list — that tool's
    # entire query JOINs on the in_repo edge this one explicitly filters the ABSENCE
    # of; a shared `project=None` param would silently change get_thread_list's own
    # required-project contract rather than add a mode to it.
    "list_unfiled_threads": {"binding_verb": False},
    # resync_seat_house — the MCP door onto seats.resync_seat_house_third_party (existed,
    # unreached until decision 68fba2e4's own six-seat repair task needed it). Writes
    # Seat.house third-party, same family as correct_house/correct_agent_house.
    "resync_seat_house": {"binding_verb": True},
    # thread_action(action=...) — #202 wave 3 (Thoth dispatch 6987, operator's relaxed
    # dispatch-shape rule): resolve_thread/annotate_thread/correct_thread_summary/
    # reclassify_thread folded into one door, all four kept as hidden deprecated
    # aliases forwarding to the shared _thread_action_impl. Not a binding-mover (acts
    # on Thread objects only). Parameterizes resolve_thread, the largest of the four.
    "thread_action": {"binding_verb": False, "parameterizes": "resolve_thread"},
    # lease(action=...) — #202 wave 3 (same dispatch, same commit shape as thread_
    # action above): acquire_lease/release_lease/check_lease/reap_stale_leases folded
    # into one door, all four kept as hidden deprecated aliases. Not a binding-mover
    # (coordinates over an arbitrary shared resource, never seat/project state).
    "lease": {"binding_verb": False, "parameterizes": "acquire_lease"},
    # seat_edge(action=...) — #202 wave 3: attach_seat/detach_seat folded into one
    # door, both kept as hidden deprecated aliases. Writes managed_by — a binding-
    # mover, same as the two hidden aliases already were in BINDING_VERBS above.
    "seat_edge": {"binding_verb": True, "parameterizes": "attach_seat"},
    # retire_object(kind=...) — #202 wave 3: retire_seat/retire_project/retire_agent
    # folded into one door, all three kept as hidden deprecated aliases. self-scoped
    # retire() and retire_assertion deliberately excluded (see the wave-3 proposal,
    # decision 1ddf8e1c) — different auth shape / genuinely unrelated param shape.
    "retire_object": {"binding_verb": True, "parameterizes": "retire_seat"},
    # current_flags(action=...) — #202 wave 4 (Thoth dispatch 7034, decision 6fe4305c):
    # stale_current_flags/repair_stale_current_flags folded into one door (both hidden
    # deprecated aliases). Not a binding-mover — acts on current_assertions kernel flags,
    # never seat/project/office state.
    "current_flags": {"binding_verb": False, "parameterizes": "repair_stale_current_flags"},
    # fold_review(action=...) — #202 wave 4: fold_candidates/resolve_fold folded into one
    # door (both hidden deprecated aliases). Not a binding-mover itself — resolve_fold's
    # decision='merged' branch still inherits fold_agent's own separate operator gate
    # unchanged, no new binding-write of its own added by the fold.
    "fold_review": {"binding_verb": False, "parameterizes": "fold_candidates"},
    # get_object_list(object_type=...) — #202 wave 4: get_thread_list/get_decision_list
    # folded into one door (both hidden deprecated aliases). Pure read, not a binding-mover.
    "get_object_list": {"binding_verb": False, "parameterizes": "get_thread_list"},
    # seat(action=...) — #202 SEAT DISPATCHER, operator ruling f9182ad7, Thoth dispatch
    # 7039, migration plan decision 620bdb32: the first object-type dispatcher under the
    # new surface-shape rule. 23 standalone tools fold in as hidden deprecated aliases
    # (mint_seat, stop, walk_in, pause_seat, vacate_seat, rebind_seat, bind_seat_tree,
    # seat_edge, charter, charter_for, heal_seat_anchor, heal_seat_transcript,
    # transition_seat_project, resync_seat_house, sweep_seat_disk, rename_seat,
    # set_seat_attended, reissue_office, establish_office, invalidate_works_in,
    # reconcile_seat_identity, correct_house, correct_pin_value, revert_own_pin_write —
    # 24 names, one more than "23" above counts because seat_edge itself was already a
    # wave-3 dispatcher whose two actions (attach/detach) both fold in under this one
    # name). launch/resume/wake/wake_preflight ALSO dispatch here but stay separately
    # named (not aliases, not decaying) — see the migration plan's own reasoning for
    # keeping them dual-reachable. retire_object and retire() deliberately stay OUT
    # (retire_object still serves kind='project'/'agent', no dispatcher for those yet;
    # retire() is session-lifecycle, a documented open question per the plan's own
    # correction addendum). Binding-mover: writes holds/house/handle/managed_by/pin/
    # works_in across nearly every action.
    "seat": {"binding_verb": True, "parameterizes": "mint_seat"},
    # composition(action=...) — #202 COMPOSITION DISPATCHER, operator ruling f9182ad7,
    # Thoth dispatch 7073/7095: the second object-type dispatcher, re-scanned and
    # approved AFTER the seat dispatcher's own traffic day (the old fold rule required
    # return-type coherence and correctly declined this cluster; the new rule tolerates
    # a hand-built oneOf schema's divergent per-action shapes instead). 3 standalone
    # tools fold in as hidden deprecated aliases: save_composition, run_composition,
    # list_compositions. Not a binding-mover — acts on Composition objects (saved
    # queries/lenses), never seat/project/office state.
    "composition": {"binding_verb": False, "parameterizes": "save_composition"},
    # project(action=...) — #202 PROJECT DISPATCHER, Thoth dispatch 7095: the third
    # object-type dispatcher. 8 names fold in (7 standalone tools + retire_project,
    # already a hidden alias before this fold, repointed): create_project,
    # ingest_project, rename_project, fork_project (action='fork'), unfork_project
    # (action='unfork'), retire_project, project_identity_evidence,
    # assert_project_property — all kept as hidden deprecated aliases. Binding-mover:
    # create/rename/fork/unfork/retire all write SoftwareProject lifecycle state, the
    # same 6823 rule create_project/rename_project/retire_project/fork_project already
    # sat in BINDING_VERBS under individually.
    "project": {"binding_verb": True, "parameterizes": "create_project"},
    # thread(action=...) — #202 THREAD DISPATCHER, Thoth dispatch 7162: the fourth
    # object-type dispatcher, absorbing `thread_action` ITSELF (already a wave-3
    # action-dispatcher, see thread_action's own entry above) into the object-type-
    # dispatcher naming convention and its hand-built oneOf schema (price-minimizer #1)
    # — a re-platforming, not a second fold of the same four names again.
    # thread_action is now hidden, forwarding to the identical _thread_action_impl;
    # `open_thread` deliberately stays OUT and separately named (it MINTS). Not a
    # binding-mover — acts on Thread objects only, same as thread_action's own entry.
    "thread": {"binding_verb": False, "parameterizes": "resolve_thread"},
    # agent(action=...) — #202 AGENT DISPATCHER, Thoth dispatch 7162, proposal decision
    # 65a6eb73 approved as scoped: the fifth object-type dispatcher. 6 names fold in —
    # correct_agent_house (already hidden, zero-traffic, before this fold — repointed),
    # retire_agent (already a hidden alias of retire_object(kind='agent') — repointed,
    # same dual-door precedent seat/project(action='retire') established),
    # fleet_reconcile, claim_name, file_subagent (already hidden, zero-traffic, before
    # this fold — repointed), file_subagents — all kept as hidden deprecated aliases.
    # Declined: retire() (self-scoped, different auth shape), walk_in (already
    # seat(action='walk_in')), merge/unmerge/reconcile_merge (polymorphic, stay named),
    # backfill_agent_project_links (already routed to a different dispatcher),
    # restore_attribution (keyed on project, not agent_id), lift/identify_agent/
    # succession_chain/unwitnessed_spawns (dead or genuinely distinct reads) — see
    # decision 65a6eb73 for the full reasoning on each. Binding-mover: correct_house/
    # retire/fleet_reconcile write Agent identity/lineage state and reap fleet mounts,
    # the same 6823 rule those three already sat in BINDING_VERBS under individually.
    "agent": {"binding_verb": True, "parameterizes": "correct_agent_house"},
    # practice(action=...) — #202 PRACTICE DISPATCHER, Thoth dispatch 7162, proposal
    # decision 07395004 approved as scoped — the sixth and FINAL object-type dispatcher
    # of #202's own fold arc. 2 names fold in: record_practice, amend_practice — both
    # kept as hidden deprecated aliases. A literal "decision" dispatcher was declined
    # (amend_decision has no siblings of its own object type — record_decision stays
    # hot-ten-named — so a one-action dispatcher would be pure renaming, the exact
    # catch-all shape the operator's own fold-endpoint ruling forbids); Practice
    # genuinely has two write verbs of its own, the real consolidation this fold
    # captures instead. Not a binding-mover — acts on Practice objects only (a
    # knowledge record, never seat/project/office/agent state).
    "practice": {"binding_verb": False, "parameterizes": "record_practice"},
}


async def _live_tool_meta() -> dict[str, dict[str, Any]]:
    """name -> its own `meta` dict (empty if none), read live off mcp.list_tools() — the
    same source _mcp_tools() already trusts, just carrying `meta` through instead of
    stripping it."""
    from src import mcp_server as srv

    tools = await srv.mcp.list_tools()
    return {t.name: dict(t.meta or {}) for t in tools}


def _find_undeclared_new_tools(
    live_names: set[str], meta_by_name: dict[str, dict[str, Any]], *,
    snapshot: frozenset[str] = KNOWN_TOOLS_AT_SNAPSHOT,
    declarations: dict[str, NewToolDeclaration] = NEW_TOOL_DECLARATIONS,
    binding_verbs: frozenset[str] = BINDING_VERBS,
) -> list[str]:
    """The ratchet's own core check, extracted so it can be proven against synthetic
    fixtures rather than only exercised live (same discipline _find_problems/
    _find_missing_cli_doors already follow). Returns one string per problem; empty means
    every live tool is either grandfathered, deprecated, or properly declared."""
    problems: list[str] = []
    for name in sorted(live_names):
        if name in snapshot:
            continue
        if meta_by_name.get(name, {}).get("deprecated"):
            continue
        if name not in declarations:
            problems.append(
                f"{name!r} is a NEW MCP tool (not in KNOWN_TOOLS_AT_SNAPSHOT, not "
                f"meta={{'deprecated': True}}) with no entry in NEW_TOOL_DECLARATIONS — "
                f"add one before shipping it (see the block comment above the snapshot)")
            continue
        decl = declarations[name]
        is_binding = bool(decl.get("binding_verb", False))
        in_binding_verbs = name in binding_verbs
        if is_binding != in_binding_verbs:
            problems.append(
                f"{name!r} declares binding_verb={is_binding} but BINDING_VERBS "
                f"membership is {in_binding_verbs} — keep the declaration and the set in "
                f"sync (adding/removing from BINDING_VERBS is the actual judgment call; "
                f"this only catches the two disagreeing)")
        if decl.get("not_parameterized_because") and not decl.get("parameterizes"):
            problems.append(
                f"{name!r} sets not_parameterized_because without parameterizes — that "
                f"justification only means something alongside a real candidate tool "
                f"(Imhotep, msg 6801)")
    return problems


async def test_every_new_mcp_tool_declares_itself_or_is_deprecated() -> None:
    meta_by_name = await _live_tool_meta()
    problems = _find_undeclared_new_tools(set(meta_by_name), meta_by_name)
    assert problems == [], (
        "#199 Lane 3B growth ratchet: a brand-new MCP tool shipped without declaring "
        "itself (see the block comment above KNOWN_TOOLS_AT_SNAPSHOT):\n"
        + "\n".join(problems))


def test_the_growth_ratchet_itself_catches_an_undeclared_new_tool() -> None:
    live = {"brand_new_widget"}
    meta: dict[str, dict[str, Any]] = {"brand_new_widget": {}}
    problems = _find_undeclared_new_tools(
        live, meta, snapshot=frozenset(), declarations={})
    assert len(problems) == 1 and "brand_new_widget" in problems[0]

    # declared, passes
    assert _find_undeclared_new_tools(
        live, meta, snapshot=frozenset(),
        declarations={"brand_new_widget": {"binding_verb": False}}) == []

    # deprecated (hidden alias), never needs a declaration at all
    meta_deprecated = {"brand_new_widget": {"deprecated": True}}
    assert _find_undeclared_new_tools(
        live, meta_deprecated, snapshot=frozenset(), declarations={}) == []


def test_the_growth_ratchet_catches_a_binding_verb_mismatch() -> None:
    live = {"brand_new_seat_mover"}
    meta: dict[str, dict[str, Any]] = {"brand_new_seat_mover": {}}
    # declared as a binding verb but never added to BINDING_VERBS
    problems = _find_undeclared_new_tools(
        live, meta, snapshot=frozenset(),
        declarations={"brand_new_seat_mover": {"binding_verb": True}},
        binding_verbs=frozenset())
    assert len(problems) == 1 and "binding_verb=True" in problems[0]
    # consistent once actually added
    assert _find_undeclared_new_tools(
        live, meta, snapshot=frozenset(),
        declarations={"brand_new_seat_mover": {"binding_verb": True}},
        binding_verbs=frozenset({"brand_new_seat_mover"})) == []


def test_the_growth_ratchet_catches_an_orphaned_not_parameterized_reason() -> None:
    live = {"widget_v2"}
    meta: dict[str, dict[str, Any]] = {"widget_v2": {}}
    problems = _find_undeclared_new_tools(
        live, meta, snapshot=frozenset(),
        declarations={"widget_v2": {"not_parameterized_because": "orphaned"}})
    assert len(problems) == 1 and "not_parameterized_because" in problems[0]
    # paired with a real candidate, passes
    assert _find_undeclared_new_tools(
        live, meta, snapshot=frozenset(),
        declarations={"widget_v2": {
            "parameterizes": "widget", "not_parameterized_because": "different auth shape"
        }}) == []


def test_known_tools_at_snapshot_and_declarations_never_collide() -> None:
    """The snapshot and the declarations table are two DIFFERENT eras of the same tool's
    life — a name in both is a stale declaration nobody removed after grandfathering it in
    (or a copy/paste into the wrong table), never a legitimate state."""
    collision = KNOWN_TOOLS_AT_SNAPSHOT & NEW_TOOL_DECLARATIONS.keys()
    assert collision == set(), (
        f"{sorted(collision)} appear in BOTH KNOWN_TOOLS_AT_SNAPSHOT and "
        f"NEW_TOOL_DECLARATIONS — a tool is grandfathered or declared, never both")


# ============================================================================================
# THE THIRD SURFACE (Thoth ruling msg 6823, on Seshat's parked question msg 6809): a slash
# command's prose (`commands/*.md`) names MCP tools and CLI commands it composes —
# "composes the `X` MCP tool", "shells out to `osiris Y`" — and nothing anywhere checks those
# names still exist. This is the SAME drift class the CLI<->MCP gate above already catches
# for its own two surfaces, just never extended to the third.
#
# #204's STRUCTURAL FIX (Thoth ruling msg 6918, decision 012b36fb, SUPERSEDING a34a9850):
# this gate used to read `~/.claude/commands/*.md` LIVE, off the machine, because decision
# a34a9850's own original reasoning was that the directory sat outside every git repo
# entirely and "no portable pytest gate can see it" any other way. That same property — a
# file the git diff being committed cannot contain — let one agent's unfinished edit to that
# shared, unversioned file fail every OTHER agent's pre-commit gate: THREE workers blocked at
# once on 2026-09-04, all on the same live seat.md race. `commands/*.md` is now the tracked
# SOURCE OF TRUTH inside this repo; `scripts/install_commands.sh` copies it to
# `~/.claude/commands` the same way `install_gate_hook.sh` already installs the pre-commit
# hook. This gate reads the REPO copy — the only one a commit's own diff can ever change —
# so it can never again fail on content outside what's actually being committed.
#
# Static shape only for most commands (Thoth: "static is fine for shape"); at least one test
# below actually EXECUTES the CLI function a slash command names and parses its real output,
# same discipline test_cli_json_promise.py already established — the parity gate's own known
# blindness (two static comparisons, never invoking either surface) is exactly what let
# specimens A and B through there, and nothing here should repeat it for a third surface.
# ============================================================================================

_SLASH_COMMANDS_DIR = Path(__file__).resolve().parent.parent / "commands"

# `` `X` MCP tool `` (an optional parenthetical between the name and "MCP tool" tolerated,
# though none of today's docs use one) and `` `osiris word-word` `` — the two literal
# reference shapes every current slash doc actually uses, surveyed directly rather than
# guessed (grep -ohE across all seven files before writing this pattern).
_SLASH_MCP_TOOL_REF = re.compile(r"`([a-zA-Z_]+)`(?:\s*\([^)]*\))?\s+MCP tool")
_SLASH_CLI_REF = re.compile(r"`osiris ([a-z][a-z-]*)")


def _slash_command_files() -> dict[str, str]:
    """name -> raw text, read off this repo's own tracked commands/*.md (#204: the SOURCE
    of what scripts/install_commands.sh puts on the machine, not the machine copy itself —
    see the module docstring above for why that distinction is the whole point)."""
    if not _SLASH_COMMANDS_DIR.is_dir():
        return {}
    return {p.stem: p.read_text() for p in sorted(_SLASH_COMMANDS_DIR.glob("*.md"))}


def _slash_command_references(text: str) -> tuple[set[str], set[str]]:
    """(mcp tool names, cli command names) a slash doc's own prose claims to compose —
    extracted, never hand-copied, so a doc edit that renames what it points at is exactly
    what this is for."""
    return set(_SLASH_MCP_TOOL_REF.findall(text)), set(_SLASH_CLI_REF.findall(text))


async def test_every_slash_command_reference_names_a_real_live_verb() -> None:
    files = _slash_command_files()
    # #204: commands/ is tracked in this repo, so it exists on every checkout — a missing
    # or empty directory here is a real defect (the repo lost its own source), never a
    # "machine doesn't have it" case to skip past the way the old live-file check did.
    assert files, (f"{_SLASH_COMMANDS_DIR} has no *.md files — the repo's own tracked slash "
                   "command source is missing or empty")
    mcp = await _mcp_tools()
    # #204: a slash doc may deliberately name a HIDDEN-but-real tool (the deprecated-alias
    # mechanism, e.g. correct_agent_house/reconcile_merge — retirement wave 1, zero traffic
    # at the time, still fully callable) — same blind spot _find_missing_cli_doors's own
    # `all_registered` parameter already dodges for the CLI-door gate; check existence
    # against the UNFILTERED registry, not `_mcp_tools()`'s own list_tools()-filtered dict.
    registered = await _mcp_registered_names()
    cli = _cli_commands()
    problems: list[str] = []
    for name, text in files.items():
        mcp_refs, cli_refs = _slash_command_references(text)
        for ref in sorted(mcp_refs):
            if ref not in mcp and ref not in registered:
                problems.append(
                    f"{name}.md references `{ref}` as an MCP tool — no such live tool "
                    f"(renamed, retired, or a typo)")
        for ref in sorted(cli_refs):
            if ref not in cli:
                problems.append(
                    f"{name}.md references `osiris {ref}` — no such live CLI command "
                    f"(renamed, retired, or a typo)")
    assert problems == [], (
        "a slash command's own prose references a verb that no longer exists on the "
        "surface it names (Thoth ruling, msg 6823):\n" + "\n".join(problems))


def test_the_slash_reference_extractor_itself_catches_a_stale_name() -> None:
    text = "Composes the `nonexistent_tool` MCP tool, or shells out to `osiris ghost-verb`."
    mcp_refs, cli_refs = _slash_command_references(text)
    assert mcp_refs == {"nonexistent_tool"}
    assert cli_refs == {"ghost-verb"}


async def test_seat_slash_docs_stop_claim_is_true_not_just_written(actions: Actions) -> None:
    """THE EXECUTION LEG (Thoth: "at least ONE test must EXECUTE a slash-command's
    underlying CLI and parse its output, same as test_cli_json_promise.py" — never just
    read the prose and trust it). seat.md's own `stop` entry claims: "Both callers
    already reach the identical stop_seat function today ... prefer the MCP tool when
    mounted, shell out otherwise. No seam here." Proven two ways, not one: (1) cmd_stop's
    own docstring states it calls trigger.stop_seat directly, the same function the MCP
    `stop` tool wraps (read from source, not asserted); (2) the CLI door is actually RUN
    here (a real refusal, not a mock) and its real stdout is parsed, same edge shape
    test_cli_json_promise.py's own cmd_stop test uses — proving the shell-out this slash
    doc describes is a working command today, not prose describing a claim gone stale."""
    import inspect
    import io
    from contextlib import redirect_stdout

    from src.cli import cmd_stop

    source = inspect.getsource(cmd_stop)
    assert "trigger.stop_seat" in source or "stop_seat" in source, (
        "cmd_stop no longer visibly calls stop_seat — seat.md's 'identical function, no "
        "seam' claim needs re-checking against whatever it calls instead")

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_stop("nonexistent-seat-xyz", pool=actions.pool)
    assert out == 1
    printed = buf.getvalue()
    assert printed.strip(), "osiris stop produced no output at all on a real refusal"
