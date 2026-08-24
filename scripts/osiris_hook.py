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
    """POST session state to /heartbeat, print rendered status line."""
    resp = _post(_URLS["statusline"], hook, timeout=_TIMEOUTS["statusline"])
    if resp is None or resp.get("error"):
        cwd = str(hook.get("cwd") or "")
        project = str(hook.get("project") or cwd.rsplit("/", 1)[-1] or "?")
        print(f"\u25c8 {project} \u2502 graph unreachable")
        return 0
    result = resp.get("result") or resp
    line = result.get("status_line") or result.get("rendered") or ""
    if line:
        print(line)
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


def _cmd_whisper(hook: dict[str, Any]) -> int:
    resp = _post(_URLS["whisper"], hook, timeout=_TIMEOUTS["whisper"])
    if resp is None:
        return 0
    result = resp.get("result") or resp
    intro = result.get("intro") or result.get("message") or ""
    if intro:
        print(intro)
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


def _cmd_anchor(hook: dict[str, Any]) -> int:
    """PreToolUse stdin filter — inject session_anchor and spawn stamps.
    This is a FILTER: reads stdin, modifies tool_input, writes to stdout."""
    tool = str(hook.get("tool_name") or "")
    if not tool.startswith("mcp__osiris__"):
        json.dump(hook, sys.stdout, ensure_ascii=False)
        sys.stdout.flush()
        return 0
    ti = dict(hook.get("tool_input") or {})
    changed = False
    child = str(hook.get("agent_id") or "")
    if child:
        ti["subagent_id"] = child
        at = str(hook.get("agent_type") or "")
        if at:
            ti["subagent_type"] = at
        changed = True
    sid = str(hook.get("session_id") or "")
    if sid:
        ti["session_anchor"] = sid
        changed = True
    if changed:
        hook["tool_input"] = ti
    json.dump(hook, sys.stdout, ensure_ascii=False)
    sys.stdout.flush()
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
