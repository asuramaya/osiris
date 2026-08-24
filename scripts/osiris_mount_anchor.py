#!/usr/bin/env python3
"""PreToolUse hook — FORCE the durable anchor + STAMP the true caller (threads 883a24f4,
0344e536).

Two jobs, both zero-compliance (the harness's own record, never the agent's claim):

1. THE ANCHOR (mount only): inject job_dir derived from the session id off the hook's stdin
   JSON, so every mount carries a durable, reconnect-stable anchor. A FOREIGN anchor the
   agent supplied deliberately (a seat claim) is respected — the session's own dir travels
   as session_anchor so the server can BIND session → seat (thread 33838160).

2. THE SPAWN STAMP (every osiris tool that writes or orients): inside a sidechain the hook
   payload carries `agent_id`/`agent_type` — present ONLY for sub-agent calls. Stamp them
   into the tool input so the server attributes the call to the CHILD (spawned_by its
   parent), never to the seat: a spawn shares its parent's $CLAUDE_JOB_DIR and MCP
   connection, and without the stamp it was greeted as the seat itself, full authority
   (live repro, 2026-07-10). In MAIN-session calls the same keys are STRIPPED — an agent
   cannot masquerade DOWN as a spawn either. The stamp travels only to tools whose schema
   accepts it (SPAWN_AWARE); other tools pass untouched.

Hooks run BEFORE the permission/classifier gates and are never rejected by them. This one
only TIGHTENS. Fail-open: any glitch → emit nothing → the call proceeds unmodified.

Install (PreToolUse, matcher mcp__osiris__.*) via onboard.py --anchor; operator-gated.

THE ALLOWLIST ROTTED SILENTLY ONCE ALREADY (obligation 570dd7e8, found as a side effect of
#129's cost measurement, 2026-08-02): 11 verbs (wake, launch, lift, record_practice,
acquire_lease, release_lease, settle, register_blind_spot, hold_memory, annotate_thread,
amend_decision) were added to mcp_server.py AFTER the 2026-07-10 fix that first built
SPAWN_AWARE, each genuinely calling `_actor_for(ctx, subagent_id, subagent_type)` for its own
write attribution — but nobody remembered to extend this set, so a real sub-agent calling any
of them silently attributed its own writes to its PARENT, unnoticed, until this measurement
caught it. "Keep in lockstep with mcp_server" was a COMMENT, and a comment does not extend
itself (Thoth's own words, msg 3031) — tests/test_anchor.py now carries the mechanical guard
that makes the twelfth verb impossible to forget silently: it AST-scans mcp_server.py for
every `_actor_for` call site and asserts the caller's name is in this set, failing loudly at
test time instead of silently in production.

THE GUARD ALREADY CAUGHT ITS TWELFTH VERB, THE SAME NIGHT (amend_practice, commit f20cf0b,
added after this fix landed) — exactly the scenario it was built for: gate_hook's own armed
run refused a wholly unrelated commit touching mcp_server.py because this allowlist had
silently fallen one verb behind again, at gate time instead of in production. Fixed here by
adding the name, not by touching the guard.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# tools whose server signature accepts the spawn stamp — keep in lockstep with mcp_server.
# ENFORCED, not just commented (tests/test_anchor.py's own AST-scan guard, obligation
# 570dd7e8): every mcp_server.py function that calls `_actor_for` must be named here.
SPAWN_AWARE = {
    "mcp__osiris__mount", "mcp__osiris__orient", "mcp__osiris__record_decision",
    "mcp__osiris__open_thread", "mcp__osiris__resolve_thread", "mcp__osiris__hold_tension",
    "mcp__osiris__send", "mcp__osiris__inbox", "mcp__osiris__ingest_reference",
    "mcp__osiris__reclassify_thread", "mcp__osiris__wake", "mcp__osiris__launch",
    "mcp__osiris__lift", "mcp__osiris__record_practice", "mcp__osiris__acquire_lease",
    "mcp__osiris__release_lease", "mcp__osiris__settle", "mcp__osiris__register_blind_spot",
    "mcp__osiris__hold_memory", "mcp__osiris__annotate_thread", "mcp__osiris__amend_decision",
    "mcp__osiris__amend_practice", "mcp__osiris__ack_handoff",
    "mcp__osiris__correct_thread_summary", "mcp__osiris__stop",
}

# tools whose server signature accepts `session_anchor` — the RE-ATTACH hint, carried on EVERY
# call so an MCP reconnect costs a silent re-attach instead of a bounce or an anonymous write.
# KEEP IN LOCKSTEP WITH mcp_server: a name here whose tool does not take the param makes the call
# fail schema validation, which is a louder, worse bug than the one it fixes.
ANCHOR_AWARE = {
    "mcp__osiris__orient", "mcp__osiris__inbox", "mcp__osiris__send",
    "mcp__osiris__record_decision", "mcp__osiris__open_thread", "mcp__osiris__resolve_thread",
    "mcp__osiris__ack_handoff",
}


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:  # noqa: BLE001 — never block a tool call on a hook glitch
        return 0
    tool = str(payload.get("tool_name") or "")
    if not tool.startswith("mcp__osiris__"):
        return 0
    ti = dict(payload.get("tool_input") or {})
    changed = False
    child = str(payload.get("agent_id") or "")

    if tool in SPAWN_AWARE:
        if child:  # SIDECHAIN: stamp the true caller off the harness's own payload
            ti["subagent_id"] = child
            agent_type = str(payload.get("agent_type") or "")
            if agent_type:
                ti["subagent_type"] = agent_type
            if tool == "mcp__osiris__mount":
                ti.pop("job_dir", None)  # a spawn gets no seat anchor
                ti.pop("session_anchor", None)
                tp = str(payload.get("transcript_path") or "")
                if tp.endswith(".jsonl"):  # the child's OWN transcript — the model probe
                    handle = child.removeprefix("agent-")
                    ti["subagent_transcript"] = f"{tp[:-6]}/subagents/agent-{handle}.jsonl"
            changed = True
        else:  # MAIN session: nobody masquerades DOWN as a spawn
            for key in ("subagent_id", "subagent_type", "subagent_transcript"):
                if key in ti:
                    ti.pop(key)
                    changed = True

    sid = str(payload.get("session_id") or "")
    derived = str(Path.home() / ".claude" / "jobs" / sid[:8]) if len(sid) >= 8 else ""

    if tool == "mcp__osiris__mount" and not child:
        if derived:
            # respect an explicit, valid anchor the agent already supplied; only fill a
            # missing/empty/unexpanded one (the '$CLAUDE_JOB_DIR'-literal case)
            existing = str(ti.get("job_dir") or "")
            if existing.startswith("/") and "$" not in existing:
                if existing.rstrip("/") != derived:
                    # a FOREIGN anchor — a mind deliberately wearing a seat. Hand the server
                    # the session's own dir too, so it can BIND session → seat (33838160).
                    ti["session_anchor"] = derived
                    changed = True
            else:
                ti["job_dir"] = derived
                changed = True
        # THE TAB VIEW + THE BRIDGE (#48 piece 1, decision 424c4158): mount() the tool
        # never had these two automount() doors — this session's own transcript_path
        # (the hook's own observation, same lane as the spawn stamp above) and
        # CLAUDE_CODE_BRIDGE_SESSION_ID (this process's own env, same as
        # osiris_whisper.py's bridge leg) are zero-compliance facts, not agent-supplied
        # ones, so they are stamped here exactly like session_anchor/job_dir — never left
        # for the agent to guess or omit.
        tp = str(payload.get("transcript_path") or "")
        if tp.endswith(".jsonl") and not ti.get("transcript_path"):
            ti["transcript_path"] = tp
            changed = True
        bridge_id = os.environ.get("CLAUDE_CODE_BRIDGE_SESSION_ID") or ""
        if bridge_id and not ti.get("bridge_session_id"):
            ti["bridge_session_id"] = bridge_id
            changed = True

    # THE ANCHOR ON EVERY CALL, not only at mount (d5fdc94a / f8525d2c) — and this is the whole
    # fix for the most-reported bug in the fleet.
    #
    # The server's re-attach machinery has always existed. It was STARVED, not broken: it keys off
    # the X-Osiris-Job header, which .mcp.json fills from ${CLAUDE_JOB_DIR} — AND THAT IS EMPTY IN
    # EVERY INTERACTIVE SESSION. So after an MCP reconnect the server has nothing to re-attach BY,
    # and the call bounces with "mount first" (or, far worse, writes ANONYMOUSLY).
    #
    # This hook has the harness's own session_id on EVERY osiris call and can derive the durable
    # job_dir from it. It was already doing that — and then handing it over only for mount().
    #
    # Four independent sightings in one night (Khepri III/tony, the code seat, the xxit seat, and
    # Thoth XXVIII four times — once while reading the mail reporting it). Every one of us called
    # it "transient", because there was no way to know otherwise. A spawn is excluded: a child gets
    # no seat anchor, ever.
    if derived and not child and tool in ANCHOR_AWARE and not ti.get("session_anchor"):
        ti["session_anchor"] = derived
        changed = True

    if changed:
        print(json.dumps({"hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "allow",
            "updatedInput": ti,
        }}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
