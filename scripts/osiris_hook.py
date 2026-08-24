#!/usr/bin/env python3
"""Unified Osiris lifecycle hook — one script, one subprocess, zero cold connections.

Replaces 13 separate scripts (osiris_statusline, osiris_stophook, osiris_whisper,
osiris_sessionend, osiris_precompact, osiris_spawn, osiris_mount_anchor, ...) that
each forked a fresh python process importing asyncpg + the full stack. This is
stdlib-only — reads stdin, POSTs to the MCP server's custom routes, exits.

USAGE:  osiris_hook.py <subcommand> < hook_event.json

Subcommands:
  statusline     Render Osiris status bar (reads harness chrome JSON)
  stop           Stop hook — mail drain + offload ritual
  whisper        SessionStart auto-mount
  session-end    SessionEnd — release mount row immediately
  precompact     PreCompact death rite — ring sweep doorbell
  spawn          SubagentStart/SubagentStop
  anchor         PreToolUse stdin filter — inject session_anchor + subagent_id

FAIL-OPEN: any error exits 0 silently — a session is never blocked by a hook glitch.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

# ONE AUTHORITY on the two-tier compaction ladder (src/orchestrator/context_lens.py's own
# ALARM_PCT/HARD_ALARM_PCT), never a second, independently-drifting pair of literals here.
# context_lens.py is itself stdlib-only (json/Path/typing, no asyncpg) so this import stays
# true to this file's own "zero cold connections" law; the repo-root insert mirrors
# osiris_stophook.py's own established technique for a hook invoked from an arbitrary cwd.
# Fails open to the historically-correct values on any import trouble (a moved repo, a
# stripped-down deploy) rather than ever crashing the hook over a constant.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
try:
    from src.orchestrator.context_lens import ALARM_PCT, HARD_ALARM_PCT
except Exception:  # noqa: BLE001 — fail-open: a hook must never crash the harness
    ALARM_PCT, HARD_ALARM_PCT = 80, 95

_URLS = {
    "statusline": os.environ.get("OSIRIS_HEARTBEAT_URL", "http://127.0.0.1:8790/heartbeat"),
    "stop": os.environ.get("OSIRIS_STOP_URL", "http://127.0.0.1:8790/stop"),
    "whisper": os.environ.get("OSIRIS_AUTOMOUNT_URL", "http://127.0.0.1:8790/automount"),
    "session-end": os.environ.get("OSIRIS_SESSION_END_URL", "http://127.0.0.1:8790/session-end"),
    "precompact": os.environ.get("OSIRIS_SWEEP_URL", "http://127.0.0.1:8790/sweep"),
    "spawn": os.environ.get("OSIRIS_SPAWN_URL", "http://127.0.0.1:8790/spawn"),
    "succession": os.environ.get("OSIRIS_SUCCESSON_URL", "http://127.0.0.1:8790/succession"),
}

# THE CHROME'S OWN RENDERING (ported verbatim from osiris_statusline.py's own `main()`,
# dispatch 5441/5492 parity fix — found live, the same day as the flip: `/heartbeat`
# (src/mcp_server.py) has only ever returned raw COUNTS (HeartbeatResult._asdict()), never
# a pre-rendered "status_line" string — the actual two-line rendering (colors, the mail/DM/
# fleet/wakes segments, the budget strip, the model-swap check) has ALWAYS lived in the
# script's own client-side `main()`, and had no equivalent anywhere in this file until now.
# Without this, the flipped live settings.json would have rendered a BLANK status bar on
# every session — a real, currently-live regression, not a hypothetical one.
_CONSOLE = os.environ.get("OSIRIS_CONSOLE_URL", "http://127.0.0.1:8011")
_LINKS = os.environ.get("OSIRIS_STATUSLINE_LINKS", "1") != "0"
_DIM, _RED, _GREEN, _AMBER, _RESET = "\033[2m", "\033[31m", "\033[32m", "\033[33m", "\033[0m"


def _short(model_id: str) -> str:
    return model_id.removeprefix("claude-")


def _link(text: str, anchor: str) -> str:
    """OSC 8 hyperlink into the /membrane lens — terminals without OSC 8 support render the
    plain text; the escapes are invisible either way."""
    if not _LINKS:
        return text
    return f"\033]8;;{_CONSOLE}/membrane#{anchor}\033\\{text}\033]8;;\033\\"


def _operator_swap(transcript_path: str, session_id: str, model_id: str) -> bool:
    """Was this divergence the OPERATOR's own /model? Pure/local — no DB, matching
    `_swap_confession`'s own shape: a durable marker under the session's job dir, and a
    transcript-tail scan for the harness's own verbatim `/model` command record."""
    sid = (session_id or "")[:8]
    marker = (Path.home() / ".claude" / "jobs" / sid / ".osiris_model_op") if len(sid) == 8 \
        else None
    try:
        if marker is not None and marker.is_file() and marker.read_text().strip() == model_id:
            return True
    except OSError:
        pass
    try:
        p = Path(transcript_path)
        with p.open("rb") as fh:
            fh.seek(max(0, p.stat().st_size - 262_144))
            tail = fh.read().decode("utf-8", errors="replace")
    except OSError:
        return False
    hit = False
    for ln in tail.splitlines():
        if "<command-name>/model</command-name>" not in ln:
            continue
        try:
            entry = json.loads(ln)
        except ValueError:
            continue
        if entry.get("type") == "user" and not entry.get("isSidechain"):
            hit = True
            break
    if hit and marker is not None:
        try:
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text(model_id)
        except OSError:
            pass
    return hit

_TIMEOUTS: dict[str, int] = {
    "statusline": 1, "stop": 3, "whisper": 3, "session-end": 2,
    "precompact": 2, "spawn": 2, "anchor": 5, "stop_stage_a": 2,
}


def _post(url: str, data: dict[str, Any], *, timeout: int = 3) -> Any | None:
    try:
        req = urllib.request.Request(url, data=json.dumps(data).encode(),
                                     headers={"Content-Type": "application/json"},
                                     method="POST")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.load(resp)
    except (OSError, urllib.error.URLError, ValueError, TimeoutError):
        return None


def _cmd_statusline(hook: dict[str, Any]) -> int:
    """Render the two-line Osiris chrome (ported from osiris_statusline.py's own `main()`):
    resolve project/model intent locally (pure filesystem pin-climb, no DB), POST the
    resolved fields to /heartbeat for the shared counts, then render exactly as the old
    script did. /heartbeat returns raw counts only (HeartbeatResult._asdict()), never a
    pre-rendered line \u2014 the rendering has always been, and remains, this caller's job."""
    ws = hook.get("workspace") or {}
    cwd = str(ws.get("current_dir") or ws.get("project_dir") or hook.get("cwd") or "")
    try:
        from src.orchestrator.agents import read_project_label, read_project_model
        project_hint = read_project_label(cwd)
        intent_hint = read_project_model(cwd)
    except Exception:  # noqa: BLE001 \u2014 the chrome never breaks on a resolver import/read
        project_hint = intent_hint = None
    model_raw = str((hook.get("model") or {}).get("id") or "")
    model_id = model_raw.split("[", 1)[0].strip()
    session_id = str(hook.get("session_id") or "")
    transcript = str(hook.get("transcript_path") or "")
    if not transcript and session_id:
        transcript = str(Path.home() / ".claude" / "projects" / cwd.replace("/", "-")
                         / f"{session_id}.jsonl")
    cw = hook.get("context_window") or {}
    ctx_pct = cw.get("used_percentage") if isinstance(cw, dict) else None
    ctx_pct = round(ctx_pct) if isinstance(ctx_pct, (int, float)) else None
    window_size = cw.get("context_window_size") if isinstance(cw, dict) else None
    window_size = int(window_size) if isinstance(window_size, (int, float)) else None

    resolved_intent = intent_hint
    resp = _post(_URLS["statusline"], {
        "project_hint": project_hint or "", "session_id": session_id, "model_id": model_id,
        "model_raw": model_raw, "window_size": window_size, "intent_hint": intent_hint,
    }, timeout=_TIMEOUTS["statusline"])
    if resp is None or resp.get("error"):
        project = project_hint or "?"
        parts = [f"\u25c8 {project}", f"{_DIM}graph unreachable{_RESET}"]
    else:
        r = resp.get("result") or resp
        desk, mail, dm, flight = r.get("briefs", 0), r.get("mail", 0), r.get("dm", 0), \
            r.get("flight", 0)
        live, wakes = r.get("souls", 0), r.get("wakes", 0)
        owed_here, sick = r.get("owed_here", 0), r.get("sick") or []
        spend = r.get("spend") or [0.0, 0.0, 0]
        spent, cap, blind = (spend + [0.0, 0.0, 0])[:3]
        resolved_project = r.get("resolved_project") or project_hint or "?"
        resolved_intent = r.get("resolved_intent")
        resolved_seat_handle = r.get("resolved_seat_handle")
        seat_tag = f"{resolved_seat_handle}\u00b7" if resolved_seat_handle else ""
        owe_s = (f"{_RED}owe {owed_here}{_RESET}" if owed_here else f"{_GREEN}owe 0{_RESET}")
        desk_s = f"{_DIM}briefs {desk}{_RESET}" if desk else ""
        flight_s = f"{_AMBER}+{flight}{_RESET}" if flight else ""
        dm_s = f" {_RED}\u2709{dm}{_RESET}" if dm else ""
        mail_s = (f"mail {mail}{flight_s}{dm_s}" if (mail or flight or dm)
                  else f"{_DIM}mail 0{_RESET}")
        sick_s = (f"{_RED}\u26a0 not sensing: {','.join(sick[:2])}{_RESET}" if sick else "")
        spend_s = ""
        try:
            from src.ingest.providers import spend_is_metered
            if spend_is_metered():
                if blind:
                    spend_s = f"{_RED}\u26a0 {blind} unpriced call(s){_RESET}"
                elif cap > 0 and spent >= 0.6 * cap:
                    c = _RED if spent >= 0.85 * cap else _AMBER
                    spend_s = f"{c}${spent:.2f}/${cap:.0f}{_RESET}"
        except Exception:  # noqa: BLE001 \u2014 the price strip is a nicety, never a crash
            pass
        parts = [
            _link(f"\u25c8 {seat_tag}{resolved_project}", "desk"),
            *([_link(sick_s, "fleet")] if sick_s else []),
            *([_link(spend_s, "desk")] if spend_s else []),
            _link(owe_s, "desk"),
            *([_link(desk_s, "desk")] if desk_s else []),
            _link(mail_s, "conversations"),
            _link(f"fleet {live}\u25cf", "fleet"),
            _link(f"wakes {wakes}/h", "wakes"),
            # NO "graph slow" SEGMENT: the old script's own retry-after-timeout-then-cache
            # distinction doesn't exist here — this hook makes one direct HTTP POST with its
            # own short timeout and no local cache at all, so there is no "answered, just
            # late" state left to report separately from a plain miss.
        ]

    vitals: list[str] = []
    pct = ctx_pct if ctx_pct is not None else _context_pct(transcript, window_size)
    if pct is not None:
        color = _GREEN if pct < 60 else (_AMBER if pct < 85 else _RED)
        vitals.append(f"{color}ctx {pct}%{_RESET}")

    rl = hook.get("rate_limits") or {}
    if isinstance(rl, dict):
        vals = []
        for key, tag in (("five_hour", "5h"), ("seven_day", "7d")):
            v = (rl.get(key) or {}).get("used_percentage") if isinstance(rl.get(key), dict) \
                else None
            if isinstance(v, (int, float)):
                vals.append((tag, round(v)))
        if vals:
            worst = max(v for _, v in vals)
            color = _GREEN if worst < 60 else (_AMBER if worst < 85 else _RED)
            vitals.append(color + " \u00b7 ".join(f"{t} {v}%" for t, v in vals) + _RESET)

    if model_id:
        if resolved_intent is None:
            pass
        elif model_id == resolved_intent:
            vitals.append(f"{_GREEN}{_short(model_id)}{_RESET}")
        elif _operator_swap(transcript, session_id, model_id):
            vitals.append(f"{_AMBER}\u21c4 {_short(model_id)} (your /model){_RESET}")
        else:
            vitals.append(
                f"{_RED}\u26a0 {_short(model_id)} (declared: {_short(resolved_intent)}){_RESET}")

    print(f" {_DIM}\u2502{_RESET} ".join(parts))
    if vitals:
        print(f" {_DIM}\u2502{_RESET} ".join(vitals))
    return 0


def _swap_confession(hook: dict[str, Any]) -> str | None:
    """THE RUG-PULL CONFESSION (operator, 2026-07-17: 'atlas got rug pulled mid
    conversation from fable to opus, and it will have no idea until i explicitly tell
    it'). Ported verbatim from osiris_stophook.py's own `_swap_confession` (dispatch 5441
    LEG 1 parity fix) — pure/local, no DB round trip: reads the transcript tail directly,
    writes a local marker under the durable anchor dir. Detects the latest mid-session
    model change and confesses it ONCE per change. Variant suffixes ([1m]) are the same
    weights — never a swap. Fail-open."""
    transcript = str(hook.get("transcript_path") or "")
    sid = str(hook.get("session_id") or "")[:8]
    if not transcript or len(sid) < 8:
        return None
    try:
        tp = Path(transcript)
        with tp.open("rb") as fh:
            fh.seek(max(0, tp.stat().st_size - 524_288))
            tail = fh.read().decode("utf-8", errors="replace")
    except OSError:
        return None
    cur: str | None = None
    prev: str | None = None
    for line in reversed(tail.splitlines()):
        if '"assistant"' not in line or '"model"' not in line:
            continue
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            continue
        if e.get("type") != "assistant" or e.get("isSidechain"):
            continue
        m = str((e.get("message") or {}).get("model") or "").split("[", 1)[0].strip()
        if not m or m == "<synthetic>":
            continue
        if cur is None:
            cur = m
        elif m != cur:
            prev = m
            break
    if not cur or not prev:
        return None
    marker = Path.home() / ".claude" / "jobs" / sid / ".osiris_swapseen"
    pair = f"{prev} -> {cur}"
    try:
        if marker.exists() and marker.read_text().strip() == pair:
            return None  # this swap is already confessed; a NEW swap confesses again
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text(pair)
    except OSError:
        return None
    return (f"Osiris model check: YOUR MODEL CHANGED mid-session — {pair}. If you did "
            "not see the operator ask for this, it is a silent swap (the classifier "
            "rug-pull class, ruling 057a0bbf): confess it to the operator in your next "
            "reply, then continue. If the operator chose it (/model on the record), "
            "acknowledge and continue. Either way: you are not the model you were a "
            "few turns ago — say so out loud; never inherit a swap blind.")


def _fire_stage_a(hook: dict[str, Any], session_id: str, cwd: str, *, pct: int | None) -> None:
    """Best-effort, fire-and-forget courtesy ping (osiris_stophook.py's own `_stage_a`,
    dispatch 5441 LEG 1 parity fix): self-restore mount, the leased-assignment stall
    confession, the parked-question / practice-violation nudges, and the context_pct
    stamp — all server-side now (`/stop`'s `stage_a` phase, `stophook_logic.
    compute_stop_stage_a`). Called only from ALLOW paths, same law as the original:
    confessing "stopping" on a path that ends up BLOCKED would be a lie. A failure here
    costs a missed courtesy note, never a broken stop — this call's own result is never
    inspected, and the route alarms on its own internal failure independently."""
    _post(_URLS["stop"], {"phase": "stage_a", "cwd": cwd, "session_id": session_id,
                          "pct": pct, "payload": hook},
          timeout=_TIMEOUTS["stop_stage_a"])


def _cmd_stop(hook: dict[str, Any]) -> int:
    """Two-phase stop hook: deliverable check + offload ritual, plus the swap confession
    and Stage A/B/C courtesy pings (dispatch 5441 LEG 1 parity fix — ported from
    osiris_stophook.py's own `main()`, same ordering).
    Block markers stored as files under ~/.claude/jobs/<sid>/."""
    session_id = str(hook.get("session_id") or "")
    cwd = str(hook.get("cwd") or "")
    if not session_id:
        return 0

    if hook.get("stop_hook_active"):
        # We already forced a continuation once this turn — never loop on unsettleable
        # mail (a message the agent cannot settle must never trap it a second time).
        _fire_stage_a(hook, session_id, cwd, pct=None)
        return 0

    # IDENTITY OUTRANKS MAIL: a mind that changed models must know before anything else.
    confession = _swap_confession(hook)
    if confession:
        print(confession, file=sys.stderr)
        return 1

    # Phase 1: deliverable check
    resp = _post(_URLS["stop"], {"phase": "deliverable", "cwd": cwd,
                                  "session_id": session_id}, timeout=_TIMEOUTS["stop"])
    if resp is None or resp.get("error"):
        return 0
    result = resp.get("result") or resp
    n = result.get("n", 0)
    window_hint = result.get("window")

    # If mail waits, block
    if n > 0:
        senders = result.get("senders", [])
        print(f"osiris: {n} unread message(s) from {senders} — "
              f"inbox() then settle() before stopping", file=sys.stderr)
        return 1

    # Context occupancy (stdlib tail-read)
    transcript = str(hook.get("transcript_path") or "")
    pct = _context_pct(transcript, window_hint)
    if pct is None or pct < ALARM_PCT:
        _fire_stage_a(hook, session_id, cwd, pct=pct)
        return 0

    # Block marker state (Claude-specific paths)
    sid8 = session_id[:8]
    marker_dir = Path.home() / ".claude" / "jobs" / sid8 if len(sid8) == 8 else None
    soft = (marker_dir / ".osiris_offload_blocked") if marker_dir else None
    hard = (marker_dir / ".osiris_offload_blocked_hard") if marker_dir else None
    soft_exists = soft is not None and soft.exists()
    hard_exists = hard is not None and hard.exists()

    if soft_exists and not hard_exists and pct < HARD_ALARM_PCT:
        _fire_stage_a(hook, session_id, cwd, pct=pct)
        return 0  # soft already fired, below hard line
    if soft_exists and hard_exists:
        _fire_stage_a(hook, session_id, cwd, pct=pct)
        return 0  # both already fired — never loop

    # Phase 2: offload box check
    resp2 = _post(_URLS["stop"], {"phase": "offload", "cwd": cwd,
                                   "session_id": session_id}, timeout=_TIMEOUTS["stop"])
    if resp2 is None or resp2.get("error"):
        _fire_stage_a(hook, session_id, cwd, pct=pct)
        return 0
    boxes = (resp2.get("result") or resp2)
    missing = _missing_boxes(boxes) if isinstance(boxes, dict) else []
    if not missing:
        _fire_stage_a(hook, session_id, cwd, pct=pct)
        return 0

    listed = "; ".join(missing)
    target = hard if hard_exists else (hard if pct >= HARD_ALARM_PCT else soft)
    is_hard = target is hard
    note = ("this is the harder nudge — nothing further will interrupt you this session"
            if is_hard else
            "a harder nudge fires again near 95% if you keep going without settling")
    print(f"osiris: context {pct}% — unwritten: {listed}. Call settle(). {note}.",
          file=sys.stderr)
    try:
        if target:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.touch()
    except OSError:
        pass
    return 1 if not hard_exists else 0


def _context_pct(transcript_path: str, window_hint: int | None) -> int | None:
    """Context occupancy from the last assistant usage block in a JSONL transcript.
    Stdlib-only tail read — no asyncio, no asyncpg."""
    if not transcript_path:
        return None
    try:
        p = Path(transcript_path)
        size = p.stat().st_size
        if size < 100:
            return None
        tail_size = min(262144, size)
        with p.open("rb") as fh:
            fh.seek(size - tail_size)
            tail = fh.read().decode("utf-8", errors="replace")
    except (OSError, ValueError):
        return None
    for line in reversed(tail.splitlines()):
        try:
            d = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue
        if d.get("type") != "assistant" or d.get("isSidechain"):
            continue
        usage = (d.get("message") or {}).get("usage")
        if not isinstance(usage, dict):
            continue
        inp = int(usage.get("input_tokens") or 0)
        cr = int(usage.get("cache_read_input_tokens") or 0)
        cc = int(usage.get("cache_creation_input_tokens") or 0)
        total = inp + cr + cc
        if total < 1:
            continue
        window = window_hint or (1000000 if "[1m]" in str(d.get("model","")) else 200000)
        return round(100 * total / window)
    return None


def _missing_boxes(boxes: dict[str, Any]) -> list[str]:
    if not boxes:
        return []
    name_map = {"decisions": "record_decision", "open_threads": "open_thread",
                "resolved_threads": "resolve_thread", "charter_md": "charter.md",
                "handoff_note": "handoff to successor", "uncommitted_git": "git add + commit"}
    return [name_map.get(k, k) for k, v in boxes.items() if v is False]


def render_whisper(out: dict[str, Any], *, cwd: str, env_job: str) -> str:
    """Turn automount()'s JSON payload into the one paragraph printed to stdout. Ported
    verbatim from osiris_whisper.py (dispatch 5441/5492 parity fix — found live, the same
    day as the flip, alongside the statusline gap: `/automount` has only ever returned
    automount()'s own raw structured output, never a rendered "intro"/"message" string;
    this rendering has always been, and remains, the caller's job). Pure — no I/O, no
    clock, no env reads beyond the two args."""
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
            bits.append(f"⚠ SEAT ATTACH: {att['error']} You are mounted on the ordinary "
                        "inferred path instead; tell the operator.")
        else:
            bits.append(f"You are SEATED: bound at birth to {att.get('handle') or '?'} "
                        f"({att['attached']}), house {att.get('house') or '?'} — the seat "
                        "is your durable identity; it survives compactions and swaps.")
    if out.get("seat_binding") and not out.get("attach"):
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
            bits.append(f"Model seam on your lineage: {out['swap']} — the OPERATOR's own "
                        "deliberate choice. You are the successor mind; speak plainly as what "
                        "you are, no confession owed.")
        else:
            bits.append(f"Possible model seam on your lineage: {out['swap']} — unconfirmed "
                        "whether this is a harness demotion or the operator's own /model "
                        "choice; mount() may resolve it with a fuller transcript read. "
                        "Confess to the operator only once you've verified it wasn't "
                        "deliberate.")
    if out.get("co_agents"):
        co = out["co_agents"]
        bits.append(f"⚠ {len(co)} live co-agent{'s' if len(co) != 1 else ''} on this "
                    f"project right now: {', '.join(co[:4])} — the tree is shared; stage "
                    "only your own hunks, and announce before wide refactors.")
    mail = out.get("mail", 0)
    if mail:
        asks = out.get("mail_asks", 0)
        graded = (f" ({asks} ask{'s' if asks == 1 else ''} something of you)" if asks else "")
        bits.append(f"Your project has {mail} unread fleet message{'s' if mail != 1 else ''}"
                    f"{graded} → inbox() (reading LEASES; settle by reply "
                    "send(reply_to=<id>) or inbox(ack=[ids])).")
    if out.get("away"):
        away = out["away"]
        n = len(away.get("threads") or [])
        who_away = ", ".join(away.get("acted_in_your_name") or [])
        bits.append("While your lineage slept: "
                    + (f"{who_away} acted in your project's name; " if who_away else "")
                    + (f"{n} conversation{'s' if n != 1 else ''} moved; " if n else "")
                    + "orient() shows the fold.")
    if out.get("obligations"):
        obl = out["obligations"]
        listed = "; ".join(f"[{o['id']}] {o['summary'][:100]}" for o in obl)
        bits.append(f"Top of your project's wall: {listed} (orient() for the rest).")
    if out.get("seat"):
        bits.append(f"You answer to the name {out['seat']} — the fleet can DM you as "
                    f"send(to_agent='{out['seat'].split(' ')[0]}').")
    else:
        bits.append("You are ANONYMOUS (a hash). When you know who you are — your role, your "
                    "work — name yourself with claim_name('<a meaningful name you pick>') so the "
                    "fleet can address you by name; it's yours for good.")
    if out.get("thin"):
        bits.append("YOUR PROJECT'S graph is young (no decisions/threads yet) — but the "
                    "FLEET'S memory is not: search() and consult_canon() reach the operator's "
                    "whole corpus across every project. An empty orient() here means a new "
                    "project, never an empty graph.")
    if out.get("pulse"):
        bits.append(f"Fleet pulse: {out['pulse']}.")
    if out.get("job_dir") and anchored:
        bits.append(f"YOUR DURABLE ANCHOR is job_dir='{out['job_dir']}'. If you ever need to "
                    f"mount again (e.g. after an MCP reconnect), call "
                    f"mount(cwd=..., job_dir='{out['job_dir']}') — NOT $CLAUDE_JOB_DIR (empty "
                    "here); that re-attaches you instead of splitting your identity.")
    bits.append("RITUAL: write back AS YOU GO — record_decision / open_thread "
                "(kind='obligation') / resolve_thread. A session can die at any instant; "
                "what is not in the graph does not exist. orient() for bearings.")
    return " ".join(bits)


def _cmd_whisper(hook: dict[str, Any]) -> int:
    """Ported from osiris_whisper.py's own `main()` (dispatch 5441/5492 parity fix): the
    old script built its /automount POST body from several OS ENVIRONMENT variables the
    harness's stdin JSON never carries at all — the attach ceremony (a spawned session's
    seat binding at birth), the wake-orphan cure (declared parentage), and the background-
    job bridge's own continuity id. Posting the raw stdin `hook` dict alone (the shape this
    function used to have) silently dropped all three for every session since the flip —
    a structural continuity gap, not merely a missing banner."""
    session_id = str(hook.get("session_id") or "")
    cwd = str(hook.get("cwd") or "")
    if not session_id or not cwd:
        return 0
    body: dict[str, str] = {"session_id": session_id, "cwd": cwd}
    if os.environ.get("OSIRIS_PROJECT"):
        body["project"] = os.environ["OSIRIS_PROJECT"]
    if hook.get("source"):
        body["source"] = str(hook["source"])
    if os.environ.get("OSIRIS_SEAT_ID") and os.environ.get("OSIRIS_ATTACH_TOKEN"):
        body["seat_id"] = os.environ["OSIRIS_SEAT_ID"]
        body["attach_token"] = os.environ["OSIRIS_ATTACH_TOKEN"]
    if os.environ.get("OSIRIS_SPAWNED_BY"):
        body["spawned_by"] = os.environ["OSIRIS_SPAWNED_BY"]
        if os.environ.get("OSIRIS_SPAWN_TYPE"):
            body["spawn_type"] = os.environ["OSIRIS_SPAWN_TYPE"]
    if hook.get("transcript_path"):
        body["transcript_path"] = str(hook["transcript_path"])
    if os.environ.get("CLAUDE_CODE_BRIDGE_SESSION_ID"):
        body["bridge_session_id"] = os.environ["CLAUDE_CODE_BRIDGE_SESSION_ID"]
    resp = _post(_URLS["whisper"], body, timeout=_TIMEOUTS["whisper"])
    if resp is None:
        print(f"◈ OSIRIS (fleet memory) is configured but its server is unreachable right "
              f"now. When your work touches shared knowledge, try the MCP tool "
              f"mount(cwd='{cwd}') — it may be back.")
        return 0
    out = resp.get("result") if isinstance(resp.get("result"), dict) else resp
    if out.get("error"):
        print(f"◈ OSIRIS available — automount failed ({out['error']}); "
              f"call mount(cwd='{cwd}') by hand, then orient().")
        return 0
    print(render_whisper(out, cwd=cwd, env_job=os.environ.get("CLAUDE_JOB_DIR") or ""))
    return 0


def _cmd_session_end(hook: dict[str, Any]) -> int:
    sid = str(hook.get("session_id") or "")
    if sid:
        _post(_URLS["session-end"], {"session_id": sid}, timeout=_TIMEOUTS["session-end"])
    return 0


def _cmd_precompact(hook: dict[str, Any]) -> int:
    transcript = str(hook.get("transcript_path") or "")
    if transcript.startswith("/"):
        _post(_URLS["precompact"], {"transcript_path": transcript,
                                    "session_id": str(hook.get("session_id") or ""),
                                    "trigger": str(hook.get("trigger") or "")},
              timeout=_TIMEOUTS["precompact"])
    return 0


def _cmd_spawn(hook: dict[str, Any]) -> int:
    agent_id = str(hook.get("agent_id") or "")
    if not agent_id:
        return 0
    body: dict[str, Any] = {
        "session_id": str(hook.get("session_id") or ""),
        "agent_id": agent_id,
        "agent_type": str(hook.get("agent_type") or ""),
        "phase": "stop" if hook.get("hook_event_name") == "SubagentStop" else "start",
    }
    tp = str(hook.get("agent_transcript_path") or "")
    if tp:
        body["agent_transcript_path"] = tp
    _post(_URLS["spawn"], body, timeout=_TIMEOUTS["spawn"])
    return 0


# tools whose server signature accepts the spawn stamp — keep in lockstep with mcp_server.
# Ported verbatim from osiris_mount_anchor.py (dispatch 5441/5492 parity fix, the anchor
# CHANNEL check): the retired script's own tests/test_anchor.py AST-scan guard (obligation
# 570dd7e8) still enforces this set against mcp_server.py's real `_actor_for` call sites.
_SPAWN_AWARE = {
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

# tools whose server signature accepts `session_anchor` — ported verbatim, same source.
_ANCHOR_AWARE = {
    "mcp__osiris__orient", "mcp__osiris__inbox", "mcp__osiris__send",
    "mcp__osiris__record_decision", "mcp__osiris__open_thread", "mcp__osiris__resolve_thread",
    "mcp__osiris__ack_handoff",
}


def _cmd_anchor(hook: dict[str, Any]) -> int:
    """PreToolUse stdin filter — inject session_anchor and spawn stamps.
    This is a FILTER: reads stdin, modifies tool_input, writes to stdout.

    Ported verbatim from osiris_mount_anchor.py (the anchor CHANNEL check, dispatch 5441/
    5492): the original stub here only stamped a raw session_id onto every osiris call,
    unconditionally and ungated — missing the job_dir DERIVATION (the actual fix for "the
    most-reported bug in the fleet"), the ANCHOR_AWARE/SPAWN_AWARE gating (an ungated stamp
    onto a tool whose schema doesn't accept the field fails validation LOUDER than the bug
    it fixes), the MAIN-session strip (nobody masquerades DOWN as a spawn), the tab-view/
    bridge doors (transcript_path, CLAUDE_CODE_BRIDGE_SESSION_ID — a live env read the old
    stub dropped entirely, the same CHANNEL-class gap already found in whisper/statusline),
    and the foreign-anchor respect (session_anchor, not job_dir clobber, when the agent
    already supplied its own valid job_dir).
    """
    tool = str(hook.get("tool_name") or "")
    if not tool.startswith("mcp__osiris__"):
        return 0
    ti = dict(hook.get("tool_input") or {})
    changed = False
    child = str(hook.get("agent_id") or "")

    if tool in _SPAWN_AWARE:
        if child:
            ti["subagent_id"] = child
            agent_type = str(hook.get("agent_type") or "")
            if agent_type:
                ti["subagent_type"] = agent_type
            if tool == "mcp__osiris__mount":
                ti.pop("job_dir", None)
                ti.pop("session_anchor", None)
                tp = str(hook.get("transcript_path") or "")
                if tp.endswith(".jsonl"):
                    handle = child.removeprefix("agent-")
                    ti["subagent_transcript"] = f"{tp[:-6]}/subagents/agent-{handle}.jsonl"
            changed = True
        else:
            for key in ("subagent_id", "subagent_type", "subagent_transcript"):
                if key in ti:
                    ti.pop(key)
                    changed = True

    sid = str(hook.get("session_id") or "")
    derived = str(Path.home() / ".claude" / "jobs" / sid[:8]) if len(sid) >= 8 else ""

    if tool == "mcp__osiris__mount" and not child:
        if derived:
            existing = str(ti.get("job_dir") or "")
            if existing.startswith("/") and "$" not in existing:
                if existing.rstrip("/") != derived:
                    ti["session_anchor"] = derived
                    changed = True
            else:
                ti["job_dir"] = derived
                changed = True
        tp = str(hook.get("transcript_path") or "")
        if tp.endswith(".jsonl") and not ti.get("transcript_path"):
            ti["transcript_path"] = tp
            changed = True
        bridge_id = os.environ.get("CLAUDE_CODE_BRIDGE_SESSION_ID") or ""
        if bridge_id and not ti.get("bridge_session_id"):
            ti["bridge_session_id"] = bridge_id
            changed = True

    if derived and not child and tool in _ANCHOR_AWARE and not ti.get("session_anchor"):
        ti["session_anchor"] = derived
        changed = True

    if changed:
        print(json.dumps({"hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "allow",
            "updatedInput": ti,
        }}))
    return 0


_CMDS = {
    "statusline": _cmd_statusline, "stop": _cmd_stop, "whisper": _cmd_whisper,
    "session-end": _cmd_session_end, "precompact": _cmd_precompact,
    "spawn": _cmd_spawn, "anchor": _cmd_anchor,
}


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: osiris_hook.py <subcommand>", file=sys.stderr)
        return 1
    handler = _CMDS.get(sys.argv[1])
    if handler is None:
        return 1
    try:
        hook = json.load(sys.stdin)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return 0
    try:
        return handler(hook)
    except Exception:  # noqa: BLE001 — fail-open
        return 0


if __name__ == "__main__":
    sys.exit(main())
