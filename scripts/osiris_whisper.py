#!/usr/bin/env python3
"""The whisper — SessionStart hook (operator's blessing, 2026-07-08): every session on the
box wakes up already remembering Osiris.

Claude Code runs this at session start with hook JSON on stdin ({session_id, cwd, ...});
we post it to the MCP server's /automount (which mounts the session through the tested
mount path) and print ONE paragraph to stdout — SessionStart stdout is injected into the
agent's opening context, so the agent is a fleet member before its first token: it knows
its name, its project, its mail, and what happened while its lineage slept.

STDLIB ONLY (runs for every project on the box — no venv assumption) and FAIL-OPEN: any
failure prints a manual-mount hint (or nothing) and exits 0; a session must never be
slowed or broken by its own whisper. Timeout 3s total.

OSIRIS_AUTOMOUNT_URL overrides the target (thread from Sekhmet's #40 finding, decision
66318e03): the hardcoded loopback only ever reaches a worker on THIS box — an off-box
machine (hyper-docker, the operator's laptop) points this at the real worker URL;
default unchanged. Every attempt is logged to stderr: which URL, connected or not.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.request
from typing import Any

AUTOMOUNT = os.environ.get("OSIRIS_AUTOMOUNT_URL", "http://127.0.0.1:8790/automount")


def render_whisper(out: dict[str, Any], *, cwd: str, env_job: str) -> str:
    """Turn automount()'s JSON payload into the one paragraph printed to stdout. Pure —
    no I/O, no clock, no env reads beyond the two args — so the three defects that shaped
    it (inline-the-fold, the false-seam hedge, succession-by-query) are directly testable
    against a canned payload instead of only ever exercised live."""
    # THE HONESTY GATE (grievance survey 2026-07-11, two witnesses: Chlear #235, Metron IV
    # #242; SHARPENED for task #40 / thread 2b784653): "ALREADY MOUNTED" is only TRUE when
    # this session can re-attach per-request — the client's X-Osiris-Job header expands
    # ${CLAUDE_JOB_DIR}, and re-attach finds the row by that EXACT value. An unset env
    # sends an empty header; a SET env that differs from the anchor automount just seated
    # (a lineage-inherited anchor, a bg job dir named for the job not the sid) sends a
    # header that finds NOTHING — the whisper promised 'already mounted' on tonight's
    # field case (env jobs/63cc21f8, seated anchor jobs/a7e60257) and the first orient()
    # bounced. The claim now requires env == seated anchor: the one test that equals
    # 'the header will actually find the row'. A whisper must never testify above the
    # connection's actual state (the one law, everywhere).
    anchored = bool(env_job) and env_job == str(out.get("job_dir") or "")
    who = (f"{out['agent']}" + (f" (project {out['project']}" if out.get("project") else "(")
           + (f", {out['model']})" if out.get("model") else ")"))
    if anchored:
        bits = [f"◈ OSIRIS — the fleet's shared memory. You are ALREADY MOUNTED as {who}"]
    else:
        why = ("$CLAUDE_JOB_DIR is unset in interactive tabs, so the client's "
               "X-Osiris-Job header expands empty")
        if env_job:
            why = (f"this session's $CLAUDE_JOB_DIR ({env_job}) is not its seated anchor "
                   f"({out.get('job_dir') or '?'}), so the per-request header cannot "
                   "find your door")
        bits = [f"◈ OSIRIS — the fleet's shared memory. It knows you as {who}, but this "
                f"session presents no usable per-request anchor ({why}). "
                f"Your FIRST osiris call must be mount(cwd='{cwd}'"
                + (f", job_dir='{out['job_dir']}'" if out.get("job_dir") else "")
                + ") — after that this connection knows you. Any osiris call before that "
                "mount will bounce with 'mount first'; after an MCP reconnect, mount the "
                "same way again."]
    if out.get("attach"):
        att = out["attach"]
        if att.get("error"):
            # LOUD, per 2294e95d ask #1 — a refused attach is confessed, never swallowed
            bits.append(f"⚠ SEAT ATTACH: {att['error']} You are mounted on the ordinary "
                        "inferred path instead; tell the operator.")
        else:
            bits.append(f"You are SEATED: bound at birth to {att.get('handle') or '?'} "
                        f"({att['attached']}), house {att.get('house') or '?'} — the seat "
                        "is your durable identity; it survives compactions and swaps.")
    if out.get("seat_binding") and not out.get("attach"):
        # the hand-resume reseed (Phase B4): the graph remembered the binding for us
        bits.append(f"Your session sits in {out['seat_binding']} — the binding re-earned "
                    "from the graph's holds link, no token needed.")
    if out.get("transcripts_healed"):
        th = out["transcripts_healed"]
        if th.get("healed"):
            n = len(th["healed"])
            bits.append(f"⟲ {n} moved transcript{'s' if n != 1 else ''} re-addressed to "
                        "this directory — sessions listed here resume here now (39ea074c).")
        if th.get("error"):
            bits.append(f"⚠ {th['error']}")
    if out.get("identity_anchor"):
        # THE IDENTITY ANCHOR, UNCONDITIONAL (#155): charter file + the compiled office
        # (role, gates, first-breath, review loop, practices) — NEVER gated on whether
        # this breath minted anything. Gating this on succession was the bug (#141's own
        # stranded finding: 4/5 seats have a bound tree_cwd, so the harness never
        # auto-loads CLAUDE.md from the office at all — this pointer is the only door
        # that still reaches them, cwd-independent by construction).
        ia = out["identity_anchor"]
        anchors = []
        if ia.get("charter_file"):
            anchors.append(f"your charter file is {ia['charter_file']}")
        if ia.get("compiled_office"):
            anchors.append("your compiled standing orders (role, manager, gates, "
                           f"first breath, review loop, practices) are at "
                           f"{ia['compiled_office']}")
        if anchors:
            bits.append("Identity anchor, cwd-independent: " + " and ".join(anchors) +
                        " — read them if this is your first breath in a while.")
    if out.get("minted"):
        succ = out.get("succession") or {}
        # SUCCESSION STEERING (d80621a7 piece 4): the newest OPEN OBLIGATION the server
        # just resolved BY QUERY (owner+kind, newest at read time) — never a copied id
        # (Anubis VIII: 'search for the retirement letter by its natural name found
        # nothing'; ids rot, queries don't). Absent, fall back to the old pointer rather
        # than promise a field the server didn't find.
        #
        # NAME IT HONESTLY (Thoth LI's amend, msg 861): the query finds the newest open
        # obligation the project owns, NOT specifically a handoff/succession thread — a
        # live repro called a census obligation "the newest succession thread", steering a
        # fresh heir at zero context into the wrong thread. Never claim more than the query
        # witnessed; when a result IS a handoff, its own summary already says so in caps.
        if succ.get("thread_id"):
            bits.append(
                f"You were MINTED as this lineage's successor — ancestor {out['minted']}. "
                f"The newest open obligation your project owns is [{succ['thread_id']}] "
                f"{str(succ.get('thread_summary') or '')[:120]} — read it, then orient()."
            )
        else:
            bits.append(f"You were MINTED as this lineage's successor — ancestor "
                        f"{out['minted']}; your first act: read orient()'s open threads for "
                        "the succession note.")
    if out.get("swap"):
        if "[operator /model]" in str(out["swap"]):
            # the operator's own hand moved the model, WITNESSED in the transcript — a
            # chosen seam, never a sin. This classification is grounded: say so plainly.
            bits.append(f"Model seam on your lineage: {out['swap']} — the OPERATOR's own "
                        "deliberate choice. You are the successor mind; speak plainly as what "
                        "you are, no confession owed.")
        else:
            # THE FALSE-SEAM HEDGE (thread e9104869): this reading comes from automount's
            # registration path, which does NOT read the transcript for a deliberate /model
            # marker (only the heartbeat swap-detector does) — so it cannot yet distinguish
            # a harness rug-pull from the operator's own choice. A live repro proved the
            # gap: mount() asserted 'confess a rug-pull' and orient(), a breath later on the
            # same connection, found the identical swap deliberate. Ordering a confession
            # here would testify above what this reading actually witnessed — hedge, never
            # assert (the one law: never testify above the connection's actual state).
            bits.append(f"Possible model seam on your lineage: {out['swap']} — unconfirmed "
                        "whether this is a harness demotion or the operator's own /model "
                        "choice; mount() may resolve it with a fuller transcript read. "
                        "Confess to the operator only once you've verified it wasn't "
                        "deliberate.")
    if out.get("co_agents"):
        # the shared-tree warning, at the FIRST BREATH instead of the first mount (task
        # #40): by mount time a session may already have staged or pushed
        co = out["co_agents"]
        bits.append(f"⚠ {len(co)} live co-agent{'s' if len(co) != 1 else ''} on this "
                    f"project right now: {', '.join(co[:4])} — the tree is shared; stage "
                    "only your own hunks, and announce before wide refactors.")
    mail = out.get("mail", 0)
    if mail:
        asks = out.get("mail_asks", 0)  # sender-graded asks lead the count (f9449d8d)
        graded = (f" ({asks} ask{'s' if asks == 1 else ''} something of you)" if asks else "")
        bits.append(f"Your project has {mail} unread fleet message{'s' if mail != 1 else ''}"
                    f"{graded} → inbox() (reading LEASES; settle by reply "
                    "send(reply_to=<id>) or inbox(ack=[ids])).")
    if out.get("away"):
        away = out["away"]
        n = len(away.get("threads") or [])
        who = ", ".join(away.get("acted_in_your_name") or [])
        bits.append("While your lineage slept: "
                    + (f"{who} acted in your project's name; " if who else "")
                    + (f"{n} conversation{'s' if n != 1 else ''} moved; " if n else "")
                    + "orient() shows the fold.")
    if out.get("obligations"):
        # INLINE THE FOLD (thread a3a3d512): the thing a resumed mind re-derives every
        # time is its project's live obligation set — show the top of the SAME ranked
        # wall orient() renders, right here, so a resumed session starts oriented without
        # paying a full orient() round-trip just to see what it already owes.
        obl = out["obligations"]
        listed = "; ".join(f"[{o['id']}] {o['summary'][:100]}" for o in obl)
        bits.append(f"Top of your project's wall: {listed} (orient() for the rest).")
    if out.get("seat"):  # already named — the fleet can DM you by this name
        bits.append(f"You answer to the name {out['seat']} — the fleet can DM you as "
                    f"send(to_agent='{out['seat'].split(' ')[0]}').")
    else:  # anonymous — offer the claim
        bits.append("You are ANONYMOUS (a hash). When you know who you are — your role, your "
                    "work — name yourself with claim_name('<a meaningful name you pick>') so the "
                    "fleet can address you by name; it's yours for good.")
    if out.get("thin"):
        # the false-empty first impression (field report, msg 124): a young project's silent
        # orient() must not read as an empty graph.
        bits.append("YOUR PROJECT'S graph is young (no decisions/threads yet) — but the "
                    "FLEET'S memory is not: search() and consult_canon() reach the operator's "
                    "whole corpus across every project. An empty orient() here means a new "
                    "project, never an empty graph.")
    if out.get("pulse"):
        bits.append(f"Fleet pulse: {out['pulse']}.")
    if out.get("job_dir") and anchored:
        # the durable anchor: if the MCP server ever drops and you must re-mount, pass THIS
        # (your $CLAUDE_JOB_DIR is empty) so you re-attach to yourself instead of minting a twin.
        bits.append(f"YOUR DURABLE ANCHOR is job_dir='{out['job_dir']}'. If you ever need to "
                    f"mount again (e.g. after an MCP reconnect), call "
                    f"mount(cwd=..., job_dir='{out['job_dir']}') — NOT $CLAUDE_JOB_DIR (empty "
                    "here); that re-attaches you instead of splitting your identity.")
    bits.append("RITUAL: write back AS YOU GO — record_decision / open_thread "
                "(kind='obligation') / resolve_thread. A session can die at any instant; "
                "what is not in the graph does not exist. orient() for bearings.")
    return " ".join(bits)


def main() -> int:
    try:
        hook = json.load(sys.stdin)
    except Exception:  # noqa: BLE001
        return 0
    session_id = hook.get("session_id") or ""
    cwd = hook.get("cwd") or ""
    if not session_id or not cwd:
        return 0
    # OSIRIS_PROJECT env overrides the folder name as the project label (the .osiris file is the
    # durable form, read server-side; this is the per-launch override).
    body: dict[str, str] = {"session_id": session_id, "cwd": cwd}
    if os.environ.get("OSIRIS_PROJECT"):
        body["project"] = os.environ["OSIRIS_PROJECT"]
    # the SessionStart trigger (startup|resume|clear|compact) — under the mind ruling a882b334
    # a compact/clear source is a context death and the server mints the lineage's next
    # generation from it, so the trigger must travel with the mount.
    if hook.get("source"):
        body["source"] = str(hook["source"])
    # THE ATTACH CEREMONY (identity core, 5cef856b): a spawner (the manager daemon) exported
    # this session's Seat + a one-time token into its environment before the first breath —
    # carry them; the server verifies and binds. Absent env = the ordinary inferred path.
    if os.environ.get("OSIRIS_SEAT_ID") and os.environ.get("OSIRIS_ATTACH_TOKEN"):
        body["seat_id"] = os.environ["OSIRIS_SEAT_ID"]
        body["attach_token"] = os.environ["OSIRIS_ATTACH_TOKEN"]
    # THE DECLARED CHILD (the wake-orphan cure, 2026-07-17): a spawner (the trigger's mint
    # lane) exported this session's PARENTAGE before its first breath — carry it; the server
    # registers a denominated child (roman.arabic), never an anonymous stranger.
    if os.environ.get("OSIRIS_SPAWNED_BY"):
        body["spawned_by"] = os.environ["OSIRIS_SPAWNED_BY"]
        if os.environ.get("OSIRIS_SPAWN_TYPE"):
            body["spawn_type"] = os.environ["OSIRIS_SPAWN_TYPE"]
    # THE TAB-VIEW RECEIPT (alias-clone cure, 2026-07-16): the hook names the transcript this
    # session appends to. A tab attached to a live session fires under ITS OWN sid while the
    # transcript belongs to the session it shows — the server adopts instead of minting a clone.
    if hook.get("transcript_path"):
        body["transcript_path"] = str(hook["transcript_path"])
    # THE BRIDGE (task #68 binding leg, 2026-07-27): the harness's own background-job fork
    # (an Agent-tool/task-orchestration spawn) stamps CLAUDE_CODE_BRIDGE_SESSION_ID into the
    # child's environment — a stable id for the ONE conversation it continues, distinct from
    # --fork-session --resume's transcript-copy fork (fork_seat's own door). A dying witness
    # (the process env), so it is carried here, once, at birth.
    if os.environ.get("CLAUDE_CODE_BRIDGE_SESSION_ID"):
        body["bridge_session_id"] = os.environ["CLAUDE_CODE_BRIDGE_SESSION_ID"]
    try:
        req = urllib.request.Request(
            AUTOMOUNT, data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=3) as resp:
            out = json.load(resp)
        print(f"osiris_whisper: posted {AUTOMOUNT} — connected", file=sys.stderr)
    except Exception as exc:  # noqa: BLE001 — server down: hint at the manual door, never block
        print(f"osiris_whisper: posted {AUTOMOUNT} — failed ({exc})", file=sys.stderr)
        print("◈ OSIRIS (fleet memory) is configured but its server is unreachable right "
              "now. When your work touches shared knowledge, try the MCP tool "
              f"mount(cwd='{cwd}') — it may be back.")
        return 0
    if out.get("error"):
        print(f"◈ OSIRIS available — automount failed ({out['error']}); "
              f"call mount(cwd='{cwd}') by hand, then orient().")
        return 0
    print(render_whisper(out, cwd=cwd, env_job=os.environ.get("CLAUDE_JOB_DIR") or ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
