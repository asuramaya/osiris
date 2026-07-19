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
from datetime import UTC, datetime
from pathlib import Path

# THE ONE-AUTHORITY IMPORTS (operator ruling 2026-07-19: 'the chrome and the harness
# disagree on briefs, mail, owe'): this script used to carry its own COPIES of every
# count's SQL, and copies drift — its mail predicate predated the lineage rollup and the
# hold grace; its fleet number counted rows where the chrome counted souls. The formulas
# now live in src/orchestrator/{mailbox,vitals} and this script CALLS them; the hooks run
# us by absolute path from arbitrary cwds, so the repo root rides sys.path explicitly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

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
    window_size: int | None = None, *, connect_timeout: float = 1.0,
) -> tuple[int, int, int, int, int, int]:
    import asyncpg

    conn = await asyncpg.connect(DSN, timeout=connect_timeout)
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
        # ONE AUTHORITY PER FACT (operator ruling 2026-07-19: 'the chrome and the harness
        # disagree on briefs, mail, owe'): every number below comes from the shared
        # formulas in src/orchestrator/{mailbox,vitals} — the same functions orient, the
        # pulse, and the chrome desk call, so the same word always shows the same number.
        # This script owns NO count SQL anymore except `flight` (statusline-only: a
        # sibling's live lease on shared broadcasts). The old inline copies had drifted:
        # the mail predicate predated the lineage rollup and the hold grace; the fleet
        # number counted seated ROWS where the chrome counted seated SOULS.
        from src.orchestrator import vitals
        from src.orchestrator.mailbox import desk_briefs_from, unread_split

        desk = await desk_briefs_from(conn, agent or None)  # type: ignore[arg-type]
        split = await unread_split(conn, project, reader_agent=agent or None,  # type: ignore[arg-type]
                                   lease_secs=LEASE_SECS)
        debts = await vitals.operator_debts(conn, hood=project)
        souls = await vitals.live_souls(conn)
        wakes = await vitals.wakes_hour(conn)
        flight = await conn.fetchval(
            "SELECT count(*) FROM fleet_messages m JOIN message_recipients r "
            "  ON r.message_id=m.id WHERE m.to_project=$1 AND m.to_agent IS NULL "
            "  AND r.agent_id <> $3 AND r.read_at IS NULL AND r.delivered_at IS NOT NULL "
            "  AND r.delivered_at >= now() - make_interval(secs => $2)",
            project, LEASE_SECS, agent)
        # THE ORGANS — is Osiris still SENSING? The session-miner died at 08:50 on 2026-07-12
        # and stayed dead ten hours: the memory simply stopped forming and nothing said so.
        # Computed HERE, at read time, in a process that is alive by construction — a watchdog
        # cron would have lived inside the very worker that died (79e1328c). Each job stamps
        # its own cadence with its outcome, so "late" needs no magic number kept elsewhere.
        jobs = await conn.fetch("SELECT key, cursor FROM watermarks WHERE key LIKE 'job:%'")
        sick: list[str] = []
        for j in jobs:
            try:
                blob = json.loads(j["cursor"] or "{}")
            except ValueError:
                continue
            ok, every = blob.get("last_ok"), int(blob.get("every") or 600)
            if not ok:
                sick.append(j["key"][4:])
                continue
            age = (datetime.now(UTC) - datetime.fromisoformat(ok)).total_seconds()
            if age > 3 * every:  # three cadences missed is not a blip
                sick.append(j["key"][4:])
        return (desk, split["mail"], split["dm"], int(flight or 0), souls["souls"],
                wakes, debts["owed"], debts["owed_here"], sick)
    finally:
        await conn.close()


async def _fetch_counts(
    project: str, session_id: str, model_id: str, model_raw: str, window_size: int | None,
) -> tuple[tuple[int, int, int, int, int, int, int, int, list[str]], bool]:
    """SLOW IS NOT DOWN (field-witnessed tonight: the 1.0s connect timeout flapped "graph
    unreachable" under load while the graph was very much up). One retry, a wider budget,
    on a TIMEOUT ONLY — a refused connection, a DNS failure, a real Postgres error is
    actually down and gets no second knock; only a TIMEOUT earns one, because a timeout is
    the one failure mode that means "maybe just slow." Returns (counts, slow) — slow=True
    only when the FIRST attempt timed out and the retry succeeded; callers still see
    "unreachable" when both attempts fail (the retry's own exception propagates)."""
    try:
        return await asyncio.wait_for(
            _counts(project, session_id, model_id, model_raw, window_size), timeout=1.5), False
    except TimeoutError:
        return await asyncio.wait_for(
            _counts(project, session_id, model_id, model_raw, window_size,
                    connect_timeout=2.5), timeout=3.0), True


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
        (desk, mail, dm, flight, live, wakes, owed, owed_here, sick), slow = asyncio.run(
            _fetch_counts(project, session_id, model_id, model_raw, window_size))
        # THE DEBT, NOT THE DOORBELL — and only the debt HERE (operator ruling, 2026-07-16:
        # the fleet-wide total 'can disappear'). A number you can do nothing about from this
        # directory is not an alarm, it is wallpaper, and wallpaper that is always red stops
        # being read. `owed` still travels in the tuple for any consumer that wants it.
        owe_s = (f"{RED}owe {owed_here}{RESET}" if owed_here else f"{GREEN}owe 0{RESET}")
        # briefs = unread mail on his desk. It is a NOTIFICATION, so it is dim: a letter owes
        # nothing, and it used to be summed into the same scary red number as a real duty.
        desk_s = f"{DIM}briefs {desk}{RESET}" if desk else ""
        # mail N(+M) ✉D — M = in flight (a sibling's live lease); ✉D = DMs addressed to YOU
        # specifically (phase 4: a private message outranks group traffic visually)
        flight_s = f"{AMBER}+{flight}{RESET}" if flight else ""
        dm_s = f" {RED}✉{dm}{RESET}" if dm else ""
        mail_s = (f"mail {mail}{flight_s}{dm_s}" if (mail or flight)
                  else f"{DIM}mail 0{RESET}")
        # DARK UNTIL IT MATTERS. Nothing is rendered while the body is well — an alarm that is
        # always lit is wallpaper. But if Osiris has stopped SENSING, that outranks every other
        # number here: the graph is not forming memory, and everything else on this line is a
        # reading off a record that has quietly stopped growing.
        sick_s = (f"{RED}⚠ not sensing: {','.join(sick[:2])}{RESET}" if sick else "")
        parts = [
            _link(f"◈ {project}", "desk"),
            *([_link(sick_s, "fleet")] if sick_s else []),
            _link(owe_s, "desk"),
            *([_link(desk_s, "desk")] if desk_s else []),
            _link(mail_s, "conversations"),
            _link(f"fleet {live}●", "fleet"),
            _link(f"wakes {wakes}/h", "wakes"),
            # SLOW IS NOT DOWN: the first knock timed out but the retry got through — say so,
            # instead of either lying "all clear" or crying "unreachable" over a graph that
            # answered, just late (field-witnessed false-down, tonight, under load).
            *([_link(f"{AMBER}graph slow{RESET}", "fleet")] if slow else []),
        ]
    except Exception:  # noqa: BLE001 — the graph being down is information, not an error
        parts = [f"◈ {project}", f"{DIM}graph unreachable{RESET}"]

    # THE SECOND LINE — the AGENT's own vitals (ctx, budget, model), below the Osiris line
    # (operator ruling, 2026-07-16: 'the chrome needs to be healthy always' — narrow windows
    # chopped the tail, and the split reads as it should: this line is Osiris, that one is
    # your agent).
    vitals: list[str] = []
    # how close this tab is to a compaction death — ambient, every render. The payload's own
    # accounting wins; the transcript-tail heuristic covers older harness versions.
    pct = ctx_pct if ctx_pct is not None else (_ctx_pct(transcript, model_raw)
                                               if transcript else None)
    if pct is not None:
        color = GREEN if pct < 60 else (AMBER if pct < 85 else RED)
        vitals.append(f"{color}ctx {pct}%{RESET}")

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
            vitals.append(color + " · ".join(f"{t} {v}%" for t, v in vals) + RESET)

    if model_id:  # the ambient model-identity check — against the REPO's intent, not the box's
        intent = _project_intent(cwd)
        if model_id == intent:
            vitals.append(f"{GREEN}{_short(model_id)}{RESET}")
        elif _operator_swap(transcript, session_id, model_id):
            # the operator's own /model is on the record: a choice, acknowledged — never an error
            vitals.append(f"{AMBER}⇄ {_short(model_id)} (your /model){RESET}")
        else:
            vitals.append(f"{RED}⚠ {_short(model_id)} (intent: {_short(intent)}){RESET}")

    print(f" {DIM}│{RESET} ".join(parts))
    if vitals:
        print(f" {DIM}│{RESET} ".join(vitals))


if __name__ == "__main__":
    main()
