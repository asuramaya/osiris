"""stophook_logic — the PURE HALVES of scripts/osiris_stophook.py's two DB reads (task
#180 piece 2 (b), msg 5253), extracted verbatim so the `/stop` route and the hook's own
direct-connect fallback share ONE implementation instead of drifting copies.

Every real stop-hook invocation used to open its OWN `asyncpg.connect()` — up to two per
call (`_deliverable` always, `_offload_boxes` conditionally) — the same per-process-fork
pattern Thoth measured on the statusline before `/heartbeat` (thread #180): a Stop hook
fires on every turn boundary, fleet-wide, so this was the SAME cost repeated on a
different trigger. `compute_stop_deliverable`/`compute_stop_offload` take an already-open
`conn`/`pool` (the MCP server's shared pool, warm) instead of opening one; the hook script
tries the new `/stop` route first and only falls back to its own direct connect (now
calling these same functions against a throwaway connection) on any failure — a route
outage costs exactly what today already costs, never more, same law `_heartbeat_via_http`
established."""
from __future__ import annotations

import re
from pathlib import Path as _Path
from typing import Any

import asyncpg

# The HOOK's patience window (osiris_stophook.py's own STOP_GRACE_SECS) — duplicated here
# rather than imported, because the hook script inserts the repo root onto sys.path itself
# (arbitrary cwd) and this module must not gain a reverse import back onto a scripts/ file.
STOP_GRACE_SECS = 3600


async def compute_stop_deliverable(
    conn: asyncpg.Pool | asyncpg.Connection, *, cwd: str, session_id: str,
) -> dict[str, Any]:
    """Verbatim extraction of osiris_stophook.py's own `_deliverable` body — see that
    function's docstring for the full rationale (the project resolution, the self-echo
    guard, the lineage rollup). Returns a JSON-shaped dict instead of a tuple so the /stop
    route can hand it back unchanged; the hook's own `_deliverable` wrapper unpacks it."""
    from src.orchestrator.mounts import find_session_row
    from src.orchestrator.seats import resolve_project

    row = await find_session_row(conn, session_id or "")
    if row is None or not row["agent_id"]:
        return {"n": 0, "senders": [], "window": None, "bands": {}, "project": None}
    project = await resolve_project(conn, str(row["agent_id"]), cwd)
    me = str(row["agent_id"])
    root, sep, suffix = me.rpartition("-")
    base = root if sep and root and suffix and set(suffix) <= set("ivxlcdm") else me
    n_row = await conn.fetchrow(
        "SELECT count(*) AS n, array_agg(DISTINCT m.from_agent) AS senders, "
        " count(*) FILTER (WHERE m.grade='ask') AS asks, "
        " count(*) FILTER (WHERE m.grade='fyi') AS fyis "
        "FROM fleet_messages m "
        "LEFT JOIN message_recipients r ON r.message_id=m.id AND r.agent_id=$1 "
        "WHERE ((m.to_agent=$1) "
        "   OR (m.to_agent = $4 OR m.to_agent LIKE $4 || '-%') "
        "   OR (m.to_project=$2 AND m.to_agent IS NULL AND m.from_agent <> $1)) "
        "AND m.read_at IS NULL "
        "AND NOT EXISTS (SELECT 1 FROM message_recipients r3 WHERE r3.message_id=m.id "
        "  AND (r3.agent_id=$1 OR r3.agent_id=$4 OR r3.agent_id LIKE $4 || '-%') "
        "  AND r3.read_at IS NOT NULL) "
        "AND (r.delivered_at IS NULL OR r.delivered_at < now() - make_interval(secs => $3))",
        row["agent_id"], project, STOP_GRACE_SECS, base)
    n = int(n_row["n"]) if n_row else 0
    senders = [s for s in (n_row["senders"] or []) if s] if n_row else []
    bands = ({"ask": int(n_row["asks"] or 0), "fyi": int(n_row["fyis"] or 0)}
             if n_row else {})
    return {
        "n": n, "senders": senders, "window": row["context_window_size"],
        "bands": bands, "project": project,
    }


async def compute_stop_offload(
    conn: asyncpg.Pool | asyncpg.Connection, *, session_id: str, cwd: str,
) -> dict[str, bool | None] | None:
    """Verbatim extraction of osiris_stophook.py's own `_offload_boxes` body — see that
    function's docstring for the full rationale (the seat-office cwd resolution, the
    shared `settle_boxes` delegation)."""
    from src.orchestrator.mounts import find_session_row
    from src.orchestrator.offices import _default_office_root
    from src.orchestrator.seats import held_seat
    from src.orchestrator.settle import settle_boxes

    row = await find_session_row(conn, session_id or "")
    if row is None or not row["agent_id"] or not row["mounted_at"]:
        return None
    charter_cwd = cwd
    seat = await held_seat(conn, str(row["agent_id"]))
    if seat and seat.get("handle"):
        charter_cwd = str(_default_office_root() / seat["handle"].lower())
    return await settle_boxes(conn, agent_id=str(row["agent_id"]),
                              mounted_at=row["mounted_at"], cwd=charter_cwd,
                              seat_id=seat["seat_id"] if seat else None)


# ═══════════ STAGE A/B/C — THE PIT WATCH + THE PRACTICE AUDIT (dispatch 5441 LEG 1,
# ported verbatim from osiris_stophook.py's own `_stage_a_async` and its helpers during the
# hook-migration parity fix; see that file's THE PIT WATCH / STAGE C section headers for the
# full founding rationale — reproduced here only where the porting itself changed something).
#
# THE ONE THING THAT CHANGED IN THE PORT: `_assert_pending`/`_assert_context_pct` used to
# open their OWN one-off `asyncpg.create_pool` (the hook script's bare `asyncpg.connect` had
# no jsonb codec registered for `assert_property`'s write). The MCP server's shared pool
# already IS Actions-ready (every other route here writes through `Actions(await
# _pool_get())`) — so this takes `pool` directly, no second pool, no codec workaround. Every
# other helper is unchanged: same queries, same fail-open discipline, same "detection only,
# never actuation, never blocks" law. Fire-and-forget from the /stop route's own caller
# (osiris_hook.py's `_cmd_stop`) — a failure here costs a missed courtesy note, never a
# broken stop.
_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+|\n+")
_QUOTE_NGRAM = 5


async def _self_restore_mount(
    conn: Any, *, agent_id: str, cwd: str, session_id: str,
) -> None:
    """#178 residual — a seat resolved solely through this module's own seat-binding
    fallback (one that stops every turn but never itself calls an osiris MCP tool this
    session) earns no `agent_mounts` row otherwise. Fail-open: a missed restore costs
    exactly what it already costs today, never more."""
    from src.orchestrator.handshake import _derive_job_dir
    from src.orchestrator.mounts import save_mount
    from src.orchestrator.seats import resolve_project

    job_dir = _derive_job_dir(session_id)
    if job_dir is None:
        return
    sid32 = (session_id or "").replace("-", "").strip().lower()
    try:
        project = await resolve_project(conn, agent_id, cwd)
        await save_mount(conn, job_dir=job_dir, agent_id=agent_id, project=project,
                         cwd=cwd, model=None, session_key=f"sid:{sid32}")
    except Exception:  # noqa: BLE001 — the Stop hook must never block a turn on this
        pass


async def _resolve_worker_identity(
    conn: Any, session_id: str, cwd: str,
) -> dict[str, Any] | None:
    """{agent_id, seat_id} for whoever is stopping, or None when neither door resolves (an
    ordinary code-repo cwd, a session with no mount row and no office of its own)."""
    from pathlib import Path

    from src.orchestrator.mounts import find_session_row
    from src.orchestrator.seats import binding_of_handle, held_seat

    row = await find_session_row(conn, session_id or "")
    if row is not None and row["agent_id"]:
        agent_id = str(row["agent_id"])
        bound = await held_seat(conn, agent_id)
        return {"agent_id": agent_id, "seat_id": bound["seat_id"] if bound else None}
    root = Path.home() / ".osiris" / "seats"
    p = Path(cwd or "")
    if p.parent != root or not p.name:
        return None
    bound = await binding_of_handle(conn, p.name)
    if bound is None:
        return None
    await _self_restore_mount(conn, agent_id=bound["holder"], cwd=cwd,
                              session_id=session_id)
    return {"agent_id": bound["holder"], "seat_id": bound["seat_id"]}


async def _leased_assignment(
    conn: Any, seat_id: str, agent_id: str,
) -> dict[str, Any] | None:
    """The freshest open obligation whose owner is this seat or this agent's lineage."""
    from src.orchestrator.agents import _generation

    base = _generation(agent_id)[0]
    row = await conn.fetchrow(
        "SELECT o.id, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS summary "
        "FROM objects o WHERE o.type='Thread' AND o.status='active' "
        "AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='kind' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'') "
        "   = 'obligation' "
        "AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'open') "
        "   = 'open' "
        "AND (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   = ANY($1::text[]) "
        "ORDER BY o.created_at DESC LIMIT 1", [seat_id, base])
    return {"id": row["id"], "summary": row["summary"]} if row is not None else None


async def _mail_gap(
    conn: Any, my_seat: str, manager_seat: str, agent_id: str,
) -> tuple[Any, Any]:
    """(manager's newest DM to my seat, my newest DM to the manager's seat)."""
    from src.orchestrator.agents import _generation

    base = _generation(agent_id)[0]
    manager_to_me = await conn.fetchval(
        "SELECT max(created_at) FROM fleet_messages WHERE to_agent=$1", my_seat)
    me_to_manager = await conn.fetchval(
        "SELECT max(created_at) FROM fleet_messages WHERE to_agent=$1 "
        "AND (from_agent=$2 OR from_agent LIKE $2 || '-%')", manager_seat, base)
    return manager_to_me, me_to_manager


def _last_assistant_text(transcript_path: str) -> str | None:
    """The literal text of the most recent real assistant turn — same tail-read shape as
    the swap-confession's model scan (osiris_hook.py's own local port): filter isSidechain,
    and unlike the model scan do NOT skip an empty-text entry — a turn that ends on a bare
    tool call has no visible question either, and that IS the answer."""
    import json as _json

    if not transcript_path:
        return None
    try:
        tp = _Path(transcript_path)
        with tp.open("rb") as fh:
            fh.seek(max(0, tp.stat().st_size - 524_288))
            tail = fh.read().decode("utf-8", errors="replace")
    except OSError:
        return None
    for line in reversed(tail.splitlines()):
        if '"assistant"' not in line:
            continue
        try:
            e = _json.loads(line)
        except _json.JSONDecodeError:
            continue
        if e.get("type") != "assistant" or e.get("isSidechain"):
            continue
        content = (e.get("message") or {}).get("content")
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            return "\n".join(
                b.get("text", "") for b in content
                if isinstance(b, dict) and b.get("type") == "text").strip()
        return ""
    return None


def _parked_on_a_question(text: str | None) -> bool:
    """A turn that ends on a question mark is a turn waiting for someone to answer it —
    fine if that someone is real, a bug if the room is empty."""
    return text is not None and text.rstrip().endswith("?")


async def _sent_a_real_ask(conn: Any, agent_id: str, within_secs: int = 300) -> bool:
    """True when this agent already sent a grade='ask' message inside the last
    `within_secs` — the signal that a trailing '?' is a REAL mail-routed ask, not a
    question narrated into an empty room."""
    from datetime import UTC, datetime, timedelta

    from src.orchestrator.agents import _generation

    base = _generation(agent_id)[0]
    since = datetime.now(UTC) - timedelta(seconds=within_secs)
    row = await conn.fetchval(
        "SELECT 1 FROM fleet_messages WHERE (from_agent=$1 OR from_agent LIKE $1 || '-%') "
        "AND grade='ask' AND created_at >= $2 LIMIT 1", base, since)
    return row is not None


def _stage_a_confession(
    *, leased: dict[str, Any], manager_dm_at: Any, my_dm_at: Any,
) -> str | None:
    """Confess only when the ball is provably in my court — the manager spoke (an
    assignment message exists) and I never spoke back since."""
    if manager_dm_at is None:
        return None
    if my_dm_at is not None and my_dm_at >= manager_dm_at:
        return None
    short = str(leased["id"])[:8]
    summary = (leased.get("summary") or "")[:60]
    tail = f" ({summary})" if summary else ""
    return f"stopping; assignment {short}{tail}: in progress"


async def _confess_if_parked(
    conn: Any, *, payload: dict[str, Any], agent_id: str, project: str | None,
    manager_seat: str,
) -> None:
    """Stage B: detection only, no actuation — a courtesy fyi DM to the manager. `project`
    is the CALLER's already-resolved `seats.resolve_project` result, never re-derived here."""
    text = _last_assistant_text(str(payload.get("transcript_path") or ""))
    if not _parked_on_a_question(text) or await _sent_a_real_ask(conn, agent_id):
        return
    assert text is not None
    q = text.strip().splitlines()[-1].strip()[-200:]
    from src.orchestrator.mailbox import send_message
    await send_message(
        conn, from_agent=agent_id, from_project=project, to_agent=manager_seat,
        body=f"stopping; last turn ended on an unanswered question with no mail ask sent "
             f"— likely parked, nobody's in the room to answer it: “{q}”",
        grade="fyi")


async def _active_practices(conn: Any, limit: int = 25) -> list[dict[str, Any]]:
    """Standing Practices only — refuted ones are excluded (dead law must never trip a
    live-turn audit), ordered by confirmed witness count."""
    rows = await conn.fetch(
        "SELECT o.id, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='statement' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS statement "
        "FROM objects o WHERE o.type='Practice' AND o.status='active' "
        "AND NOT EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='refuted_by') "
        "ORDER BY (SELECT count(*) FROM links l WHERE l.from_id=o.id AND l.type='witnesses') "
        "  DESC LIMIT $1", limit)
    return [{"id": str(r["id"]), "statement": r["statement"]} for r in rows if r["statement"]]


def _quotes_the_practice(sentence_words: list[str], stmt_words: list[str]) -> bool:
    """A sentence that reproduces a contiguous N-word run of the practice's OWN wording is
    citing it, not reversing it — checked on WORD ORDER, not vocabulary density."""
    if len(stmt_words) < _QUOTE_NGRAM:
        return False
    joined = " ".join(sentence_words)
    return any(
        " ".join(stmt_words[i:i + _QUOTE_NGRAM]) in joined
        for i in range(len(stmt_words) - _QUOTE_NGRAM + 1)
    )


def _practice_violation(
    text: str | None, practices: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Pure, no DB: the same lexical reversal fingerprint layer 1 uses at write time
    (`capture.practice_contradiction_cues`), applied to a turn's raw tail text instead of a
    Decision's summary. Returns the first (highest-confirmed) match, or None — a miss is
    not proof of compliance, only that this fingerprint found nothing."""
    if not text or not practices:
        return None
    from src.orchestrator.capture import practice_contradiction_cues

    text_words = text.lower().split()
    quoted_ids = {
        p["id"] for p in practices
        if _quotes_the_practice(text_words, (p.get("statement") or "").lower().split())
    }
    for sentence in _SENTENCE_SPLIT.split(text):
        cues = practice_contradiction_cues(sentence)
        if not cues:
            continue
        sent_words = re.findall(r"[a-z]{4,}", sentence.lower())
        if not sent_words:
            continue
        sent_topic = set(sent_words)
        for p in practices:
            if p["id"] in quoted_ids:
                continue  # cited verbatim somewhere in this turn — citation, not reversal
            stmt = p.get("statement") or ""
            stmt_topic = set(re.findall(r"[a-z]{4,}", stmt.lower()))
            if len(sent_topic & stmt_topic) < 2:
                continue
            return {"practice_id": p["id"][:8], "statement": stmt, "cues": cues}
    return None


async def _already_flagged_today(conn: Any, agent_id: str, practice_id: str) -> bool:
    """One Stage C flag per (agent, practice) per calendar day — an alert nobody believes
    is worse than no alert."""
    from datetime import UTC, datetime

    from src.orchestrator.agents import _generation

    base = _generation(agent_id)[0]
    since = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    row = await conn.fetchval(
        "SELECT 1 FROM fleet_messages WHERE (from_agent=$1 OR from_agent LIKE $1 || '-%') "
        "AND body LIKE '%violated standing Practice ' || $2 || '%' AND created_at >= $3 "
        "LIMIT 1",
        base, practice_id, since)
    return row is not None


async def _confess_if_practice_violated(
    conn: Any, *, payload: dict[str, Any], agent_id: str, project: str | None,
    manager_seat: str,
) -> None:
    """Stage C: DISARMED by default (`Settings.osiris_stage_c_practice_check_enabled`,
    ruling DM 3059 — 20 flags/24h, 14/14 individually verified false, zero confirmed true
    positives found in this mechanism's whole deployment history). Re-arming is one flag,
    never a silent revert of this function's own logic."""
    from src.config.settings import get_settings

    if not get_settings().osiris_stage_c_practice_check_enabled:
        return
    text = _last_assistant_text(str(payload.get("transcript_path") or ""))
    practices = await _active_practices(conn)
    hit = _practice_violation(text, practices)
    if hit is None:
        return
    if await _already_flagged_today(conn, agent_id, hit["practice_id"]):
        return
    from src.orchestrator.mailbox import send_message
    await send_message(
        conn, from_agent=agent_id, from_project=project, to_agent=manager_seat,
        body=f"stopping; this turn may have violated standing Practice {hit['practice_id']} "
             f"(\"{hit['statement']}\") — reversal language found ({', '.join(hit['cues'])}); "
             "a heuristic flag, not a verdict, worth a look",
        grade="fyi")


async def compute_stop_stage_a(
    pool: asyncpg.Pool, *, payload: dict[str, Any], session_id: str, cwd: str,
    pct: int | None = None,
) -> None:
    """Ported from osiris_stophook.py's own `_stage_a_async` — see the STAGE A/B/C banner
    above for what changed in the port (only the pool source). Fire-and-forget from the
    caller's own POV: never raises, never returns anything the caller needs to act on."""
    from src.actions.core import Actions
    from src.orchestrator.seats import manager_of_seat, resolve_project

    identity = await _resolve_worker_identity(pool, session_id, cwd)
    if identity is None:
        return  # nobody to attribute this to
    agent_id = identity["agent_id"]
    if pct is not None:
        actions = Actions(pool)
        obj = await actions.create_or_find_object("Agent", agent_id, agent_id)
        from datetime import UTC, datetime

        await actions.assert_property(
            obj, "context_pct", str(pct), agent_id, datetime.now(UTC), 1.0,
            evidence_class="direct_observation")
    if not identity.get("seat_id"):
        return  # unclaimed seat — nothing further to confess
    seat_id = identity["seat_id"]
    project = await resolve_project(pool, agent_id, cwd)
    manager_seat = await manager_of_seat(pool, seat_id)
    if manager_seat is not None:
        await _confess_if_parked(
            pool, payload=payload, agent_id=agent_id, project=project,
            manager_seat=manager_seat)
        await _confess_if_practice_violated(
            pool, payload=payload, agent_id=agent_id, project=project,
            manager_seat=manager_seat)
    leased = await _leased_assignment(pool, seat_id, agent_id)
    if leased is None:
        from datetime import UTC, datetime

        actions = Actions(pool)
        obj = await actions.create_or_find_object("Agent", agent_id, agent_id)
        await actions.assert_property(
            obj, "state", "pending", agent_id, datetime.now(UTC), 1.0,
            evidence_class="self_declared")
        return
    if manager_seat is None:
        return  # no manager of record — nobody to confess to
    manager_dm_at, my_dm_at = await _mail_gap(pool, seat_id, manager_seat, agent_id)
    body = _stage_a_confession(leased=leased, manager_dm_at=manager_dm_at, my_dm_at=my_dm_at)
    if body is None:
        return
    from src.orchestrator.mailbox import send_message
    await send_message(pool, from_agent=agent_id, from_project=project,
                       to_agent=manager_seat, body=body, grade="fyi")
