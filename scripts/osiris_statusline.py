"""The membrane in the window chrome — Osiris's statusline for Claude Code (Lane A, 9d2aaf4d).

The operator lives in the terminal; Osiris's upward streams lived behind an agent-keyhole or
:8011. This renders them AMBIENT: one line, every turn, in the window itself — the operator's
desk (unread briefs), this project's mailbox, the fleet's mounted-live count, the wake chain's
last hour, and a LIVE model-identity check (the swap confession moved into the chrome, where a
demotion is visible the turn it happens — ruling f2ae6346's banner, made permanent).

Claude Code pipes session JSON on stdin; we print one line and exit. HARD BUDGET: this runs
per render, so one connection, one query, ~1s timeout, and ANY failure degrades to a quiet
minimal line — the statusline must never block or break the window it serves.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")
EXPECTED = os.environ.get("OSIRIS_EXPECTED_MODEL", "claude-fable-5")
CONSOLE = os.environ.get("OSIRIS_CONSOLE_URL", "http://127.0.0.1:8011")
SUCCESSION = os.environ.get("OSIRIS_SUCCESSION_URL", "http://127.0.0.1:8790/succession")
LINKS = os.environ.get("OSIRIS_STATUSLINE_LINKS", "1") != "0"  # kill switch if a terminal balks
LEASE_SECS = 900  # mirror osiris_mail_lease_secs — deliverable = unsettled + no live lease

DIM = "\033[2m"
RED = "\033[31m"
GREEN = "\033[32m"
AMBER = "\033[33m"
RESET = "\033[0m"


def _short(model_id: str) -> str:
    return model_id.removeprefix("claude-")


def _project_intent(cwd: str) -> str:
    """The repo's DECLARED model intent (.osiris: model = "…"), walking up to the repo root —
    the operator's per-project standing choice; the box default only as fallback. A fleet of
    onboarded repos does not all run fable, and the chrome must not paint the operator's own
    choice red every turn (complaint, 2026-07-10)."""
    try:
        import tomllib
        p = Path(cwd)
        for d in (p, *p.parents):
            f = d / ".osiris"
            if f.is_file():
                v = tomllib.loads(f.read_text()).get("model")
                return str(v).strip() if v else EXPECTED
            if (d / ".git").exists():
                break
    except Exception:  # noqa: BLE001 — the chrome never breaks on a config file
        pass
    return EXPECTED


def _operator_swap(transcript_path: str, session_id: str, model_id: str) -> bool:
    """Was this divergence the OPERATOR's own /model? The harness records the command verbatim
    in the transcript; a hit is remembered in the session's durable job dir (the command
    scrolls out of the tail long before the session ends — the marker doesn't)."""
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


def _ctx_pct(transcript_path: str, model_id: str) -> int | None:
    """Context occupancy % from the transcript's TAIL (the harness's own usage record) — the
    operator's ambient answer to 'how close is this tab to a compaction death'. Window tier
    from the display id ([1m] = 1M, else 200k). None = omit the segment, never lie. Mirrors
    src/orchestrator/context_lens.py (inlined: this script imports nothing from the repo)."""
    try:
        p = Path(transcript_path)
        size = p.stat().st_size
        with p.open("rb") as fh:
            fh.seek(max(0, size - 262_144))
            tail = fh.read().decode("utf-8", errors="replace")
    except OSError:
        return None
    for line in reversed(tail.splitlines()):
        if '"usage"' not in line:
            continue
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            continue
        if e.get("type") != "assistant" or e.get("isSidechain"):
            continue
        u = (e.get("message") or {}).get("usage")
        if not isinstance(u, dict) or "input_tokens" not in u:
            continue
        used = (int(u.get("input_tokens") or 0) + int(u.get("cache_read_input_tokens") or 0)
                + int(u.get("cache_creation_input_tokens") or 0))
        env = os.environ.get("OSIRIS_CONTEXT_WINDOW", "")
        if env.isdigit():
            window = int(env)
        elif "[1m]" in model_id or used > 200_000:
            # a bare-id tab past 200k and alive proves the default wrong (fable runs 1M here)
            window = 1_000_000
        else:
            window = 200_000
        return round(100 * used / window)
    return None


def _succession(session_id: str, model_id: str) -> str | None:
    """POST a live model seam to the server (ruling a882b334): the mind changed under this
    tab, so the seat passes NOW — the server mints the heir and moves the mount row. Returns
    the heir's agent id, or None on any failure (fail-open: the row kept the OLD model, so
    the very next render sees the same divergence and retries)."""
    import urllib.request
    try:
        req = urllib.request.Request(
            SUCCESSION, data=json.dumps({"session_id": session_id, "model": model_id}).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=0.8) as resp:
            out = json.load(resp)
        return str(out["minted"]) if out.get("minted") else None
    except Exception:  # noqa: BLE001 — the chrome never blocks on its own sensor
        return None


def _link(text: str, anchor: str) -> str:
    """OSC 8 hyperlink into the /membrane lens — the statusline's click-through. Terminals
    without OSC 8 support render the plain text; the escapes are invisible either way."""
    if not LINKS:
        return text
    return f"\033]8;;{CONSOLE}/membrane#{anchor}\033\\{text}\033]8;;\033\\"


async def _counts(
    project: str, session_id: str, model_id: str = "", model_raw: str = "",
    window_size: int | None = None,
) -> tuple[int, int, int, int, int, int]:
    import asyncpg

    conn = await asyncpg.connect(DSN, timeout=1.0)
    try:
        agent = None
        if session_id:
            # THE HEARTBEAT: a tab rendering its chrome is ALIVE — bump its registry row so
            # the wake dispatch never mints a twin beside a tab the operator is actively
            # driving (msg-78 lesson: 'live' must mean the tab, not the last osiris call).
            # The row's model is now succession-owned (ruling a882b334): first render STAMPS
            # it (COALESCE fills a NULL only); any later divergence is a live model seam —
            # the mind changed under this tab — and the SERVER mints the heir and moves the
            # row, so the chrome never overwrites the one signal that witnesses the seam.
            # NORMALIZED id: the payload decorates variants (claude-opus-4-8[1m] = the same
            # weights at 1M context) that transcripts record bare — same mind, never a seam
            bare = model_id.split("[", 1)[0].strip()
            row0 = await conn.fetchrow(
                "UPDATE agent_mounts SET last_seen=now(), "
                "model=COALESCE(model, NULLIF($2,'')), model_raw=NULLIF($3,''), "
                "context_window_size=COALESCE($4, context_window_size) "
                "WHERE job_dir LIKE '%/jobs/' || $1 RETURNING agent_id, model",
                session_id[:8], bare, model_raw, window_size)
            agent = row0["agent_id"] if row0 else None
            stored = row0["model"] if row0 else None
            if agent and bare and stored and stored.split("[", 1)[0].strip() != bare:
                agent = _succession(session_id, bare) or agent
        agent = agent or ""
        # counts are PER-RECIPIENT now (migration 0021): desk = operator's own unread, mail =
        # THIS agent's broadcasts+DMs unread, flight = a SIBLING's live lease on shared broadcasts.
        # An IDENTITY-LESS render (no session in the payload, or no mount row yet) must not
        # count as a phantom '' reader — that reader has no receipts, so it re-counts the
        # project's whole settled history (the operator's 'mail 5' ghost). It falls back to
        # PROJECT-OPEN semantics instead: broadcasts NOBODY has settled.
        mail_sub = (
            "(SELECT count(*) FROM fleet_messages m LEFT JOIN message_recipients r "
            "   ON r.message_id=m.id AND r.agent_id=$3 "
            "   WHERE ((m.to_agent=$3) OR (m.to_project=$1 AND m.to_agent IS NULL)) "
            "   AND m.read_at IS NULL AND r.read_at IS NULL "
            "   AND (r.delivered_at IS NULL "
            "     OR r.delivered_at < now() - make_interval(secs => $2)))"
            if agent else
            "(SELECT count(*) FROM fleet_messages m "
            "   WHERE m.to_project=$1 AND m.to_agent IS NULL AND m.read_at IS NULL AND $3='' "
            "   AND NOT EXISTS(SELECT 1 FROM message_recipients r2 WHERE r2.message_id=m.id "
            "     AND r2.read_at IS NOT NULL))"
        )
        # WHAT YOU OWE, WHERE YOU STAND (operator, 2026-07-11: "attack the chrome with the same
        # mentality"). `desk` counted NOTIFICATIONS — unread briefs, letters and eulogies mixed
        # in with real asks — and it read the same from every directory, which is the opposite
        # of contextual. The honest number is the DEBT: open threads owned by the human, minus
        # the ones he deferred. Split by NEIGHBORHOOD (the garden's primitive) so the chrome
        # answers the question he actually has standing in a repo: what do I owe HERE, and how
        # much is waiting everywhere else?
        row = await conn.fetchrow(
            "WITH ops AS (SELECT o.id, "
            "  (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
            "    AND a.name='deferred_until' "
            "    ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS defer, "
            "  (SELECT replace(p.canonical,'repo:','') FROM links l JOIN objects p ON p.id=l.to_id "
            "    WHERE l.from_id=o.id AND l.type='in_repo' AND p.type='SoftwareProject' "
            "    ORDER BY l.created_at DESC LIMIT 1) AS hood "
            "  FROM objects o WHERE o.type='Thread' AND o.status='active' "
            "  AND (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
            "    AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
            "    = 'operator' "
            "  AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
            "    WHERE a.object_id=o.id AND a.name='status' "
            "    ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'open') = 'open'), "
            " live AS (SELECT * FROM ops WHERE defer IS NULL "
            "   OR defer <= to_char(now(), 'YYYY-MM-DD')) "
            "SELECT (SELECT count(*) FROM live) AS owed, "
            " (SELECT count(*) FROM live WHERE hood = $1) AS owed_here, "
            " (SELECT count(*) FROM fleet_messages m WHERE m.to_project='operator' "
            "   AND m.to_agent IS NULL AND m.read_at IS NULL "
            "   AND NOT EXISTS(SELECT 1 FROM message_recipients r WHERE r.message_id=m.id "
            "     AND r.agent_id='operator' AND r.read_at IS NOT NULL) "
            "   AND NOT EXISTS(SELECT 1 FROM message_recipients r WHERE r.message_id=m.id "
            "     AND r.agent_id='operator' AND r.delivered_at >= now() "
            "       - make_interval(secs => $2))) AS desk, "
            f" {mail_sub} AS mail, "
            " (SELECT count(*) FROM fleet_messages m LEFT JOIN message_recipients r "
            "   ON r.message_id=m.id AND r.agent_id=$3 "
            "   WHERE m.to_agent=$3 AND m.to_agent <> '' AND m.read_at IS NULL "
            "   AND r.read_at IS NULL AND (r.delivered_at IS NULL "
            "     OR r.delivered_at < now() - make_interval(secs => $2))) AS dm, "
            " (SELECT count(*) FROM fleet_messages m JOIN message_recipients r "
            "   ON r.message_id=m.id WHERE m.to_project=$1 AND m.to_agent IS NULL "
            "   AND r.agent_id <> $3 AND r.read_at IS NULL AND r.delivered_at IS NOT NULL "
            "   AND r.delivered_at >= now() - make_interval(secs => $2)) AS flight, "
            " (SELECT count(*) FROM agent_mounts "
            "   WHERE last_seen > now() - interval '15 minutes') AS live_agents, "
            " (SELECT count(*) FROM agent_wakes "
            "   WHERE woke_at > now() - interval '1 hour') AS wakes",
            project, LEASE_SECS, agent)
        return (row["desk"], row["mail"], row["dm"], row["flight"], row["live_agents"],
                row["wakes"], row["owed"], row["owed_here"])
    finally:
        await conn.close()


def main() -> None:
    try:
        payload = json.load(sys.stdin)
    except Exception:  # noqa: BLE001 — chrome must render even on garbage input
        payload = {}
    ws = payload.get("workspace") or {}
    cwd = ws.get("current_dir") or ws.get("project_dir") or os.getcwd()
    project = Path(cwd).name
    model_raw = str((payload.get("model") or {}).get("id") or "")
    model_id = model_raw.split("[", 1)[0].strip()
    session_id = str(payload.get("session_id") or "")
    transcript = str(payload.get("transcript_path") or "")
    if not transcript and session_id:  # older payloads: derive by the harness's path scheme
        transcript = str(Path.home() / ".claude" / "projects" / cwd.replace("/", "-")
                         / f"{session_id}.jsonl")
    # the harness's own context accounting (v2.1.205+): the number /context shows, first-class
    # in the payload — no inference. Older payloads fall back to the transcript-tail heuristic.
    cw = payload.get("context_window") or {}
    ctx_pct = cw.get("used_percentage") if isinstance(cw, dict) else None
    ctx_pct = round(ctx_pct) if isinstance(ctx_pct, (int, float)) else None
    window_size = cw.get("context_window_size") if isinstance(cw, dict) else None
    window_size = int(window_size) if isinstance(window_size, (int, float)) else None

    try:
        desk, mail, dm, flight, live, wakes, owed, owed_here = asyncio.run(
            asyncio.wait_for(_counts(project, session_id, model_id, model_raw, window_size),
                             timeout=1.5))
        # THE DEBT, NOT THE DOORBELL. `owe 5·13` = five open duties in THIS tree, thirteen
        # across the garden. Red only when you owe something HERE — a fleet-wide number you
        # can do nothing about from this directory is not an alarm, it is wallpaper, and
        # wallpaper that is always red stops being read.
        elsewhere = f"{DIM}·{owed}{RESET}" if owed > owed_here else ""
        owe_s = (f"{RED}owe {owed_here}{RESET}{elsewhere}" if owed_here
                 else (f"{DIM}owe 0·{owed}{RESET}" if owed else f"{GREEN}owe 0{RESET}"))
        # briefs = unread mail on his desk. It is a NOTIFICATION, so it is dim: a letter owes
        # nothing, and it used to be summed into the same scary red number as a real duty.
        desk_s = f"{DIM}briefs {desk}{RESET}" if desk else ""
        # mail N(+M) ✉D — M = in flight (a sibling's live lease); ✉D = DMs addressed to YOU
        # specifically (phase 4: a private message outranks group traffic visually)
        flight_s = f"{AMBER}+{flight}{RESET}" if flight else ""
        dm_s = f" {RED}✉{dm}{RESET}" if dm else ""
        mail_s = (f"mail {mail}{flight_s}{dm_s}" if (mail or flight)
                  else f"{DIM}mail 0{RESET}")
        parts = [
            _link(f"◈ {project}", "desk"),
            _link(owe_s, "desk"),
            *([_link(desk_s, "desk")] if desk_s else []),
            _link(mail_s, "conversations"),
            _link(f"fleet {live}●", "fleet"),
            _link(f"wakes {wakes}/h", "wakes"),
        ]
    except Exception:  # noqa: BLE001 — the graph being down is information, not an error
        parts = [f"◈ {project}", f"{DIM}graph unreachable{RESET}"]

    # how close this tab is to a compaction death — ambient, every render. The payload's own
    # accounting wins; the transcript-tail heuristic covers older harness versions.
    pct = ctx_pct if ctx_pct is not None else (_ctx_pct(transcript, model_raw)
                                               if transcript else None)
    if pct is not None:
        color = GREEN if pct < 60 else (AMBER if pct < 85 else RED)
        parts.append(f"{color}ctx {pct}%{RESET}")

    # the operator's remaining budget, always in view (request 2026-07-09): the harness's own
    # rate-limit state — 5-hour and 7-day windows, colored by whichever is worse.
    rl = payload.get("rate_limits") or {}
    if isinstance(rl, dict):
        vals = []
        for key, tag in (("five_hour", "5h"), ("seven_day", "7d")):
            v = (rl.get(key) or {}).get("used_percentage") if isinstance(rl.get(key), dict) \
                else None
            if isinstance(v, (int, float)):
                vals.append((tag, round(v)))
        if vals:
            worst = max(v for _, v in vals)
            color = GREEN if worst < 60 else (AMBER if worst < 85 else RED)
            parts.append(color + " · ".join(f"{t} {v}%" for t, v in vals) + RESET)

    if model_id:  # the ambient model-identity check — against the REPO's intent, not the box's
        intent = _project_intent(cwd)
        if model_id == intent:
            parts.append(f"{GREEN}{_short(model_id)}{RESET}")
        elif _operator_swap(transcript, session_id, model_id):
            # the operator's own /model is on the record: a choice, acknowledged — never an error
            parts.append(f"{AMBER}⇄ {_short(model_id)} (your /model){RESET}")
        else:
            parts.append(f"{RED}⚠ {_short(model_id)} (intent: {_short(intent)}){RESET}")

    print(f" {DIM}│{RESET} ".join(parts))


if __name__ == "__main__":
    main()
