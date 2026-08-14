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
import re
import sys
import time
from pathlib import Path
from typing import Any

# THE ONE-AUTHORITY IMPORTS (operator ruling 2026-07-19: 'the chrome and the harness
# disagree on briefs, mail, owe'): this script used to carry its own COPIES of every
# count's SQL, and copies drift — its mail predicate predated the lineage rollup and the
# hold grace; its fleet number counted rows where the chrome counted souls. The formulas
# now live in src/orchestrator/{mailbox,vitals} and this script CALLS them; the hooks run
# us by absolute path from arbitrary cwds, so the repo root rides sys.path explicitly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")
# NO `EXPECTED`/OSIRIS_EXPECTED_MODEL HERE ANYMORE (Thoth's catch, msg 3951): a hardcoded
# "claude-fable-5" ambient fallback painted the operator's own live, correct /model choice
# red every ten seconds whenever `_project_intent`'s climb found no declared model — that
# constant is a swap-DETECTOR's own expectation about one harness (ruling f2ae6346), not
# any operator's declared choice or the fleet's default. `_project_intent` now returns None
# when nothing is declared, and `main()` renders silence for that state, never an alarm.
CONSOLE = os.environ.get("OSIRIS_CONSOLE_URL", "http://127.0.0.1:8011")
SUCCESSION = os.environ.get("OSIRIS_SUCCESSION_URL", "http://127.0.0.1:8790/succession")
LINKS = os.environ.get("OSIRIS_STATUSLINE_LINKS", "1") != "0"  # kill switch if a terminal balks
LEASE_SECS = 900  # mirror osiris_mail_lease_secs — deliverable = unsettled + no live lease
CACHE_TTL_SECS = float(os.environ.get("OSIRIS_STATUSLINE_CACHE_TTL") or "5")

DIM = "\033[2m"
RED = "\033[31m"
GREEN = "\033[32m"
AMBER = "\033[33m"
RESET = "\033[0m"


def _short(model_id: str) -> str:
    return model_id.removeprefix("claude-")


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
    from the display id ([1m] = 1M, else 200k). None = omit the segment, never lie. Delegates
    to context_lens.glance — the same tail-read this script used to carry its own copy of
    (seam 6, #148: two hand-parsers of the same undocumented shape is a hazard, not a
    tidiness complaint; the ONE-AUTHORITY IMPORTS note above already killed this class for
    mail/fleet/briefs, this closes the one holdout)."""
    from src.orchestrator.context_lens import glance
    result = glance(Path(transcript_path), model_id)
    return result["pct"] if result is not None else None


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
    project_hint: str, session_id: str, model_id: str = "", model_raw: str = "",
    window_size: int | None = None, *, intent_hint: str | None = None,
    connect_timeout: float = 1.0,
) -> tuple[int, int, int, int, int, int, int, int, list[str],
           tuple[float, float, int], str | None, str | None, str | None]:
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
            # the ONE session→row lookup (mounts.find_session_row, task #33): the old
            # inline anchor-name match missed any window whose durable anchor wears
            # another name — that window rendered identity-less (dm 0, desk 0)
            from src.orchestrator.mounts import find_session_row
            found = await find_session_row(conn, session_id)
            row0 = None
            if found is not None:
                row0 = await conn.fetchrow(
                    "UPDATE agent_mounts SET last_seen=now(), "
                    "model=COALESCE(model, NULLIF($2,'')), model_raw=NULLIF($3,''), "
                    "context_window_size=COALESCE($4, context_window_size) "
                    "WHERE job_dir = $1 RETURNING agent_id, model",
                    found["job_dir"], bare, model_raw, window_size)
            agent = row0["agent_id"] if row0 else None
            stored = row0["model"] if row0 else None
            if agent and bare and stored and stored.split("[", 1)[0].strip() != bare:
                agent = _succession(session_id, bare) or agent
        agent = agent or ""

        # THE SEAT FALLBACK (Thoth's catch, msg 3949/3951): a `.osiris` CLIMB from `cwd`
        # answers nothing for the bare seat-office container — it's a CHILD directory (the
        # office) that would answer, never an ancestor, and a climb only ever looks upward.
        # "resolve the displayed identity the way everything else is supposed to — through
        # the pin, THEN the seat, then the graph": the pin already ran (project_hint/
        # intent_hint, computed by the caller before this call); this is the SEAT leg, run
        # ONLY when the pin left a gap, on the SAME connection this call already opened —
        # no separate query budget spent for the common case where the climb already
        # answered. `house` (not the seat's own handle) fills `resolved_project`
        # deliberately: house IS project for a seat-derived identity (ruling 577988ed), so
        # the label and the numbers beside it stay about the SAME thing, never a silent mix
        # of a seat's name against a different project's counts.
        #
        # THE WINDOW TAG NEEDS A SEPARATE FIELD (ruling b8a18059, same resolution chain as
        # the mount-guard fix this session): 577988ed's reasoning stays right about the
        # NUMBERS (they must be about `house`, never a silent mix), but the operator reads
        # this line to answer "which MIND am I talking to", and `house` alone answers a
        # different question ("whose counts are these"). resolved_seat_handle rides beside
        # resolved_project rather than replacing it — the render layer decides how to show
        # both together; this layer only resolves the extra fact. Looked up whenever an
        # `agent` is known (not gated on resolved_project being None) because the handle is
        # needed even on the common path where the pin climb already answered the project.
        resolved_project = project_hint or None
        resolved_intent = intent_hint
        resolved_seat_handle: str | None = None
        if agent:
            from src.orchestrator.seats import held_seat, seat_facts

            seat = await held_seat(conn, agent)
            if seat:
                resolved_seat_handle = seat.get("handle")
                if resolved_project is None:
                    resolved_project = seat.get("house")
                if resolved_intent is None and seat.get("seat_id"):
                    facts = await seat_facts(conn, seat["seat_id"])
                    anchor = facts.get("anchor_cwd")
                    if anchor:
                        from src.orchestrator.agents import read_project_model
                        resolved_intent = read_project_model(anchor)

        # ONE AUTHORITY PER FACT, and now one AUTHORITY PER RULE too (ruling e9ef7373,
        # thread 109b6c48 — the render analog of the write-verb receipt law): this script
        # owns no fact-SQL and no presentation rule (thresholds/gates/severity) anymore —
        # surface.fetch is the single batched read every ambient number and every dark-
        # until-it-matters/metered-gate/dm-lights-alone rule comes from.
        from src.orchestrator import surface

        seg = await surface.fetch(conn, project=resolved_project or "", agent=agent or None,
                                  lease_secs=LEASE_SECS)
        return (seg.briefs_mine.data["briefs"], seg.mail.data["mail"], seg.mail.data["dm"],
                seg.mail.data["flight"], seg.live.data["souls"], seg.wakes.data["wakes"],
                seg.owed.data["owed"], seg.owed_here.data["owed_here"],
                seg.sensing.data["sick"],
                (seg.spend.data.get("spent", 0.0), seg.spend.data.get("cap", 0.0),
                 seg.spend.data.get("blind", 0)),
                resolved_project, resolved_intent, resolved_seat_handle)
    finally:
        await conn.close()


async def _fetch_counts(
    project_hint: str, session_id: str, model_id: str, model_raw: str,
    window_size: int | None, *, intent_hint: str | None,
) -> tuple[tuple[int, int, int, int, int, int, int, int, list[str],
                 tuple[float, float, int], str | None, str | None, str | None], bool]:
    """SLOW IS NOT DOWN (field-witnessed tonight: the 1.0s connect timeout flapped "graph
    unreachable" under load while the graph was very much up). One retry, a wider budget,
    on a TIMEOUT ONLY — a refused connection, a DNS failure, a real Postgres error is
    actually down and gets no second knock; only a TIMEOUT earns one, because a timeout is
    the one failure mode that means "maybe just slow." Returns (counts, slow) — slow=True
    only when the FIRST attempt timed out and the retry succeeded; callers still see
    "unreachable" when both attempts fail (the retry's own exception propagates)."""
    try:
        return await asyncio.wait_for(
            _counts(project_hint, session_id, model_id, model_raw, window_size,
                    intent_hint=intent_hint), timeout=1.5), False
    except TimeoutError:
        return await asyncio.wait_for(
            _counts(project_hint, session_id, model_id, model_raw, window_size,
                    intent_hint=intent_hint, connect_timeout=2.5), timeout=3.0), True


# THE FORK STORM (Ra's diagnosis, msg 958: load 20.5 on 20 vcpus, 17 concurrent
# osiris_statusline.py forks + 9 PG backends — every Claude Code pane re-renders its chrome
# on a timer, each render forking a fresh python that asyncpg-connects cold). desk/mail/dm/
# flight/owed_here are READER-scoped (src/orchestrator/{mailbox,vitals}.py resolve them
# against the rendering agent), so a cache shared ACROSS sessions would leak one agent's
# numbers into another's window — the cache key is the SESSION, not the project. That
# still kills the storm: its shape is one pane re-rendering far faster than the fleet
# numbers actually change, not many panes each needing a genuinely different answer.
_SAFE_CACHE_KEY = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


def _cache_dir() -> Path:
    override = os.environ.get("OSIRIS_STATUSLINE_CACHE_DIR")
    if override:
        return Path(override)
    base = os.environ.get("XDG_RUNTIME_DIR") or "/var/tmp/osiris-scratch"
    return Path(base) / "osiris-statusline"


def _cache_key(session_id: str) -> str:
    if _SAFE_CACHE_KEY.match(session_id):
        return session_id
    import hashlib
    return hashlib.sha256(session_id.encode()).hexdigest()[:32]


def _encode_counts(counts: tuple[Any, ...]) -> list[Any]:
    *head, spend, resolved_project, resolved_intent, resolved_seat_handle = counts
    return [*head, list(spend), resolved_project, resolved_intent, resolved_seat_handle]


def _decode_counts(obj: list[Any]) -> tuple[Any, ...]:
    *head, spend, resolved_project, resolved_intent, resolved_seat_handle = obj
    return (*head, tuple(spend), resolved_project, resolved_intent, resolved_seat_handle)


def _cache_read(path: Path) -> tuple[tuple[Any, ...], bool, float] | None:
    """(counts, slow, age_secs) if the file parses, else None — corrupt or missing is just
    a miss, never an error (the cache is an optimization, never a source of truth)."""
    try:
        obj = json.loads(path.read_text())
        counts = _decode_counts(obj["counts"])
        return counts, bool(obj["slow"]), time.time() - float(obj["ts"])
    except (OSError, ValueError, KeyError, TypeError, IndexError):
        return None


def _cache_write(path: Path, counts: tuple[Any, ...], slow: bool) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        tmp.write_text(json.dumps(
            {"ts": time.time(), "slow": slow, "counts": _encode_counts(counts)}))
        tmp.replace(path)  # atomic on POSIX — no reader ever sees a partial write
    except OSError:  # a scratch dir gone missing must never break the chrome
        pass


class _StatuslineDegrade(Exception):
    """No cache to serve and a sibling pane already holds the refresh lock — render the
    quiet minimal line (main()'s own except-Exception) rather than block on, or duplicate,
    an in-flight query."""


def _counts_cached(
    project_hint: str, session_id: str, model_id: str, model_raw: str,
    window_size: int | None, *, intent_hint: str | None,
) -> tuple[tuple[Any, ...], bool]:
    """Warm path never imports asyncpg — the cold-start cost Ra measured: a render within
    CACHE_TTL_SECS of this SAME session's last one reads a JSON file instead of opening a
    connection. Cold path is unchanged (one connection, the existing retry law) except the
    first pane through the door writes the answer down for whoever renders next, and every
    OTHER pane racing it (`flock` nonblocking) serves the stale answer rather than piling
    onto Postgres too. The resolved identity (project/model intent, msg 3949/3951's seat
    fallback) rides the SAME cache entry as the counts — it is only as fresh as they are,
    which is the existing, already-accepted staleness tolerance, not a new one."""
    if not session_id:  # nothing to key a cache on — exactly today's behavior
        return asyncio.run(_fetch_counts(project_hint, session_id, model_id, model_raw,
                                         window_size, intent_hint=intent_hint))
    cache_path = _cache_dir() / f"{_cache_key(session_id)}.json"
    cached = _cache_read(cache_path)
    if cached is not None and cached[2] <= CACHE_TTL_SECS:
        return cached[0], cached[1]
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        lock_fh = open(cache_path.with_suffix(".lock"), "a+")
    except OSError:
        return asyncio.run(_fetch_counts(project_hint, session_id, model_id, model_raw,
                                         window_size, intent_hint=intent_hint))
    try:
        import fcntl
        try:
            fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            # a sibling pane is refreshing this session's numbers RIGHT NOW — serve the
            # stale answer if there is one, never a duplicate query, never a block
            if cached is not None:
                return cached[0], cached[1]
            raise _StatuslineDegrade(
                "refresh in flight elsewhere, no cache to serve meanwhile") from None
        try:
            counts, slow = asyncio.run(
                _fetch_counts(project_hint, session_id, model_id, model_raw, window_size,
                              intent_hint=intent_hint))
        finally:
            fcntl.flock(lock_fh, fcntl.LOCK_UN)
    finally:
        lock_fh.close()
    _cache_write(cache_path, counts, slow)
    return counts, slow


def main() -> None:
    try:
        payload = json.load(sys.stdin)
    except Exception:  # noqa: BLE001 — chrome must render even on garbage input
        payload = {}
    ws = payload.get("workspace") or {}
    cwd = ws.get("current_dir") or ws.get("project_dir") or os.getcwd()
    # THE IDENTITY FIX (Thoth's catch, msg 3949/3951): `Path(cwd).name` used to be the WHOLE
    # resolution — a raw directory basename wearing identity's clothes, "seats" instead of
    # "thoth" from the bare seat-office container, next to numbers computed for something
    # else entirely. Now: the SAME canonical pin-climb every other resolver uses
    # (read_project_label/read_project_model, no hand-rolled duplicate), with a SEAT
    # fallback (via _counts_cached, only reached when the climb itself answers nothing —
    # `project_hint`/`intent_hint` are None exactly then) for the one shape a climb can
    # never solve: an office is a CHILD of the container, and a climb only looks upward.
    try:
        from src.orchestrator.agents import read_project_label, read_project_model
        project_hint = read_project_label(cwd)
        intent_hint = read_project_model(cwd)
    except Exception:  # noqa: BLE001 — the chrome never breaks on a resolver import/read
        project_hint = intent_hint = None
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

    resolved_intent = intent_hint
    try:
        ((desk, mail, dm, flight, live, wakes, owed, owed_here, sick,
          (spent, cap, blind), resolved_project, resolved_intent,
          resolved_seat_handle), slow) = _counts_cached(
            project_hint or "", session_id, model_id, model_raw, window_size,
            intent_hint=intent_hint)
        # NEVER A WRONG BASENAME (Thoth's constraint 1, msg 3949): the pin climb and the
        # seat fallback both ran (inside _counts_cached) and STILL found nothing — an
        # explicit unresolved marker, never Path(cwd).name wearing identity's clothes.
        project = resolved_project or "?"
        # THE SEAT RIDES BESIDE THE PROJECT, NEVER REPLACES IT (ruling b8a18059): the
        # operator's question is "which mind is this window" — `house` alone answers "whose
        # counts are these", a different question, correctly, but not the one being asked.
        # SEATLESS: resolved_seat_handle is None (no held_seat found) — say nothing extra,
        # today's project-only label is honest as-is, never an invented name.
        seat_tag = f"{resolved_seat_handle}·" if resolved_seat_handle else ""
        # THE DEBT, NOT THE DOORBELL — and only the debt HERE (operator ruling, 2026-07-16:
        # the fleet-wide total 'can disappear'). A number you can do nothing about from this
        # directory is not an alarm, it is wallpaper, and wallpaper that is always red stops
        # being read. `owed` still travels in the tuple for any consumer that wants it.
        owe_s = (f"{RED}owe {owed_here}{RESET}" if owed_here else f"{GREEN}owe 0{RESET}")
        # briefs = unread mail on his desk. It is a NOTIFICATION, so it is dim: a letter owes
        # nothing, and it used to be summed into the same scary red number as a real duty.
        desk_s = f"{DIM}briefs {desk}{RESET}" if desk else ""
        # mail N(+M) ✉D — M = in flight (a sibling's live lease); ✉D = DMs addressed to YOU
        # specifically (phase 4: a private message outranks group traffic visually).
        # `dm` MUST light the segment by itself: the Alfred chain (2026-07-19) was pure
        # DM traffic — mail 0, flight 0, seven DMs waiting — and this condition read
        # (mail or flight), so every window in the halted chain rendered a dim "mail 0".
        flight_s = f"{AMBER}+{flight}{RESET}" if flight else ""
        dm_s = f" {RED}✉{dm}{RESET}" if dm else ""
        mail_s = (f"mail {mail}{flight_s}{dm_s}" if (mail or flight or dm)
                  else f"{DIM}mail 0{RESET}")
        # DARK UNTIL IT MATTERS. Nothing is rendered while the body is well — an alarm that is
        # always lit is wallpaper. But if Osiris has stopped SENSING, that outranks every other
        # number here: the graph is not forming memory, and everything else on this line is a
        # reading off a record that has quietly stopped growing.
        sick_s = (f"{RED}⚠ not sensing: {','.join(sick[:2])}{RESET}" if sick else "")
        # THE PRICE, DARK UNTIL IT MATTERS (task #26's last mile): a healthy day renders
        # nothing — the pulse and the desk carry the ambient number. The strip speaks only
        # when spend crosses 60% of the cap (amber; red at 85%) or a call went UNPRICED
        # (spend the ceiling cannot see — never silently scored as zero).
        # ...but ONLY when the dollars are real. On a subscription the CLI's cost is notional
        # (spend_is_metered False), so this strip stays dark rather than flashing a phantom
        # '$12/$10' — the same reason the ceiling no longer gates and the pulse omits it
        # (Thoth LIII 2026-07-21). The segment returns the day Osiris runs on a keyed API backend.
        from src.ingest.providers import spend_is_metered
        spend_s = ""
        if spend_is_metered():
            if blind:
                spend_s = f"{RED}⚠ {blind} unpriced call(s){RESET}"
            elif cap > 0 and spent >= 0.6 * cap:
                c = RED if spent >= 0.85 * cap else AMBER
                spend_s = f"{c}${spent:.2f}/${cap:.0f}{RESET}"
        parts = [
            _link(f"◈ {seat_tag}{project}", "desk"),
            *([_link(sick_s, "fleet")] if sick_s else []),
            *([_link(spend_s, "desk")] if spend_s else []),
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
        # the graph never answered — no seat fallback was possible, so this is the BEST
        # local, offline answer (the pin climb alone), never a raw basename guess either.
        project = project_hint or "?"
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

    if model_id:  # the ambient model-identity check — against the SEAT's/repo's OWN intent
        # THREE STATES, KEPT DISTINCT (Thoth's constraint 3, msg 3951 — the worst 60bc15db
        # she found tonight, pointed at the human): "declared X, running Y" is a real
        # warning; "nothing declared anywhere" is SILENCE, never a red alarm over a
        # decision nobody made; a harness-DETECTOR's own expectation (ruling f2ae6346) is a
        # different statement again and no longer feeds this line at all (the removed
        # EXPECTED constant). Collapsing the first two into one red line is exactly what
        # painted the operator's own deliberate /model choice as if it were wrong.
        if resolved_intent is None:
            pass
        elif model_id == resolved_intent:
            vitals.append(f"{GREEN}{_short(model_id)}{RESET}")
        elif _operator_swap(transcript, session_id, model_id):
            # the operator's own /model is on the record: a choice, acknowledged — never an error
            vitals.append(f"{AMBER}⇄ {_short(model_id)} (your /model){RESET}")
        else:
            vitals.append(
                f"{RED}⚠ {_short(model_id)} (declared: {_short(resolved_intent)}){RESET}")

    print(f" {DIM}│{RESET} ".join(parts))
    if vitals:
        print(f" {DIM}│{RESET} ".join(vitals))


if __name__ == "__main__":
    main()
