"""Stop hook — the tab you're watching drains its own mailbox (operator consent, 2026-07-08).

The main agent cannot start its own turn (no harness heartbeat while idle), but it CAN be kept
from ending one with mail on the table: when a turn would stop, this hook checks the project's
deliverable count and, if mail waits, blocks the stop once with the settle ritual as the
continuation — the work happens in the SAME visible session the operator is already paying
for. No twin, no re-ingestion, no stranger wearing the face.

Beside the mail check, THE OFFLOAD RITUAL (queue item 4, #49 piece 3): above the context
authority's alarm line, a quiet stop is refused ONCE, naming whatever this session left
unwritten (decisions, threads, charter.md, a minted heir's own handoff note) — then never
blocked again for that session.

Safety: `stop_hook_active` means we already continued this turn once — always allow the stop
then (a message the agent cannot settle must never loop it). Any error or slow graph → allow
(fail-open; the chrome still shows the count). Budget ~1s, same as the statusline.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

# repo root on sys.path — the hook runs from arbitrary cwds and imports the shared
# authorities (mounts.find_session_row); same pattern as the statusline
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# THE ONE AUTHORITY on context occupancy (queue item 4, #49 piece 3) — never a copied
# constant or a re-derived %. context_window (the MCP tool), the statusline chrome, and
# this hook's offload ritual now all read the SAME threshold off the SAME primitives.
from src.orchestrator.context_lens import (  # noqa: E402
    ALARM_PCT,
    last_usage,
    occupancy,
    window_for,
)

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")
# The HOOK's patience window, deliberately longer than the mailbox lease (900s): a message
# delivered to THIS agent within the hour is demonstrably in-hand — Anubis VIII's grievance
# (msg 236): the hook blocked a turn that was itself composing the settlement, because a
# long synthesis outlives the 15-min lease. The mailbox's redelivery semantics are
# UNCHANGED (at-least-once holds for everyone else); only the stop-block relents.
STOP_GRACE_SECS = 3600


async def _deliverable(
    cwd: str, session_id: str,
) -> tuple[int, list[str], int | None, dict[str, int], str | None]:
    """(deliverable mail count, its senders, KNOWN window size or None, grade bands, the
    RESOLVED project) for THIS session — mail mirrors mailbox.unread_count exactly (per-
    recipient, migration 0021); the window comes from the mount row's context_window_size,
    stamped by the chrome from the harness's own accounting. An unmounted session has no
    inbox → (0, [], None, {}, None). The SENDERS ride along because a notification that
    omits them forces a read to discover whether a read was warranted (Metron V, msg 444) —
    and the GRADE BANDS ride for the same reason (thread f9449d8d: an FYI wearing a duty's
    authority teaches the reader to skim, and a skimmed mailbox is lost; the nag says which
    of the pile actually asks something, without guessing the ungraded).

    THE PROJECT is now `seats.resolve_project` (msg 1888), never a bare `Path(cwd).name` —
    the old hand-rolled basename was more than cosmetic: a session sitting at the bare
    seat-office CONTAINER guessed "seats" and MISSED every real broadcast mail addressed to
    its actual house (`m.to_project=$2` below), a mail-blindness bug, not just a wrong label
    in the block reason. Resolved here (not by the caller) so it rides the SAME connection
    and agent_id lookup, one round trip, not two."""
    import asyncpg

    conn = await asyncpg.connect(DSN, timeout=1.0)
    try:
        # the ONE session→row lookup (mounts.find_session_row, task #33) — this hook's
        # inline anchor-name match was the mail arc's silent half: a re-anchored window
        # was never nagged, so its mail sat while the session lived
        from src.orchestrator.mounts import find_session_row
        from src.orchestrator.seats import resolve_project
        row = await find_session_row(conn, session_id or "")
        if row is None or not row["agent_id"]:
            # (the old `return 0, None` here was a 2-tuple against a 3-tuple signature —
            # the unmounted path "worked" only because the caller's fail-open ate the
            # unpack error; now it declines honestly)
            return 0, [], None, {}, None
        project = await resolve_project(conn, str(row["agent_id"]), cwd)
        # `m.from_agent <> $1` on the broadcast leg: THE SELF-ECHO (Metron V, msgs 444/446) —
        # without it this hook BLOCKED a turn to make an agent read its own outbound, six
        # times in one night. Mirrors mailbox._DELIVERABLE_TO_READER; keep them in step —
        # including THE ROLLUP: a DM parked on any generation of the reader's lineage is
        # the reader's (the base strips a trailing roman suffix, agents._generation's rule).
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
            # THE SETTLE-STATE ROLLUP (mailbox._DELIVERABLE_TO_READER; keep in step): has
            # ANY generation of my lineage already settled this, not just my own exact id.
            "AND NOT EXISTS (SELECT 1 FROM message_recipients r3 WHERE r3.message_id=m.id "
            "  AND (r3.agent_id=$1 OR r3.agent_id=$4 OR r3.agent_id LIKE $4 || '-%') "
            "  AND r3.read_at IS NOT NULL) "
            "AND (r.delivered_at IS NULL OR r.delivered_at < now() - make_interval(secs => $3))",
            row["agent_id"], project, STOP_GRACE_SECS, base)
        n = int(n_row["n"]) if n_row else 0
        senders = [s for s in (n_row["senders"] or []) if s] if n_row else []
        bands = ({"ask": int(n_row["asks"] or 0), "fyi": int(n_row["fyis"] or 0)}
                 if n_row else {})
        return n, senders, row["context_window_size"], bands, project
    finally:
        await conn.close()


# ═══════════ THE OFFLOAD RITUAL (queue item 4, #49 piece 3) ═══════════
# Above the ONE context authority's alarm line, a QUIET stop is refused ONCE — the block
# names what this session left unwritten and points at charter.md, the offload target
# assignment 3 built. Supersedes the old ad hoc mortality nag (a fixed reminder on a
# cooldown, no box-checks, its own copied threshold): this is the same concern, finally
# built to spec — targeted, one-shot, and reading occupancy off the ONE authority's own
# primitives (context_lens.ALARM_PCT / last_usage / occupancy / window_for) instead of a
# second, disagreeing threshold and a second tail-parse.
#
# TWO-TIER RE-ARM (Thoth, msg 1381, seam-discipline decision 33b7cb10): block-once-then-
# silent-forever let a session climb straight through the alarm line unnudged once tripped
# — "block-once-then-silent is exactly how Seshat climbed 80->past-seam unnudged." A SECOND
# marker, gated on a harder line (HARD_ALARM_PCT), re-arms the ritual once more near the
# ceiling — never a third time, never a loop; this policy stays hook-local (WHEN to
# enforce), same as the original one-shot design.
HARD_ALARM_PCT = 95

async def _offload_boxes(
    session_id: str, cwd: str,
) -> dict[str, bool | None] | None:
    """This session's own boxes — resolves session_id -> agent_id/mounted_at (hook-specific
    context the shared checker shouldn't need to know about), then delegates the actual box
    logic to settle.settle_boxes (ruling c5b184cd) so the hook and the /settle MCP tool read
    ONE implementation, never two drifting copies. Returns None only when the session itself
    can't be resolved to an agent (nothing to check at all — the caller treats that exactly
    like 'everything satisfied')."""
    import asyncpg
    from src.orchestrator.settle import settle_boxes

    conn = await asyncpg.connect(DSN, timeout=1.0)
    try:
        from src.orchestrator.mounts import find_session_row
        row = await find_session_row(conn, session_id or "")
        if row is None or not row["agent_id"] or not row["mounted_at"]:
            return None
        return await settle_boxes(conn, agent_id=str(row["agent_id"]),
                                  mounted_at=row["mounted_at"], cwd=cwd)
    finally:
        await conn.close()


def _offload_marker(session_id: str, *, hard: bool = False) -> Path | None:
    """The block-once marker's path — same convention as the swap/death-rite markers
    (a file under this session's durable anchor dir), None for an id too short to trust.
    `hard` names the SECOND tier's own marker (HARD_ALARM_PCT) — a distinct file, so the
    soft (ALARM_PCT) and hard blocks each fire once, independently."""
    sid = (session_id or "")[:8]
    if len(sid) < 8:
        return None
    name = ".osiris_offload_blocked_hard" if hard else ".osiris_offload_blocked"
    return Path.home() / ".claude" / "jobs" / sid / name


def _offload_already_blocked(session_id: str, *, hard: bool = False) -> bool:
    marker = _offload_marker(session_id, hard=hard)
    if marker is None:
        return False
    try:
        return marker.exists()
    except OSError:
        return False  # can't tell → never trap a session on a filesystem hiccup


def _offload_mark_blocked(session_id: str, *, hard: bool = False) -> None:
    marker = _offload_marker(session_id, hard=hard)
    if marker is None:
        return
    try:
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.touch()
    except OSError:
        pass  # the refusal still happened; a missed marker costs a rare double-block


def _offload_pct(payload: dict[str, Any], window_hint: int | None) -> tuple[int | None, bool]:
    """Occupancy %, off THE authority's own primitives — last_usage (tail-only, cheap
    enough for every stop) + occupancy + window_for — never a separate tail-parse.
    (pct, window_assumed); (None, True) when there's nothing to read. `window_hint` is
    the mount row's harness-stamped context_window_size (the strongest signal when
    present — exactly what context_lens.detail() calls window_hint); its absence falls
    back to window_for's own inference, never a guess dressed as certainty."""
    transcript = str(payload.get("transcript_path") or "")
    if not transcript:
        return None, True
    usage = last_usage(Path(transcript))
    if usage is None:
        return None, True
    used = occupancy(usage)
    if window_hint:
        window, assumed = window_hint, False
    else:
        model_id = str((payload.get("model") or {}).get("id") or "") or None
        window, assumed = window_for(model_id, used)
    return round(100 * used / window), assumed


def _offload_verdict(
    *, pct: int | None, window_assumed: bool, already_blocked: bool,
    boxes: dict[str, bool | None] | None, hard: bool = False,
) -> dict[str, Any] | None:
    """THE WHOLE POLICY, pure — no I/O, no clock (pct/boxes are supplied, not derived
    here), so every law is a direct unit test. BLOCK ONCE THEN ALLOW: `already_blocked`
    short-circuits everything — a dying session is never trapped in a refusal loop
    (`hard` names WHICH tier's marker the caller already checked — see HARD_ALARM_PCT).
    NEVER on an unknown or assumed window (the Anubis VII false-eulogy law, msg 127) or
    below context_lens.ALARM_PCT. And even above the line, a refusal fires only when
    something is GENUINELY unwritten — `boxes` with nothing False (all satisfied, or
    everything fog-of-war None) has nothing to enforce and never blocks."""
    if already_blocked or pct is None or window_assumed or pct < ALARM_PCT or not boxes:
        return None
    from src.orchestrator.settle import missing_boxes
    missing = missing_boxes(boxes)
    if not missing:
        return None
    listed = "; ".join(missing)
    tier_note = (
        "this is the harder nudge — nothing further will interrupt you this session, so "
        "settle now" if hard else
        f"a harder nudge fires again near {HARD_ALARM_PCT}% if you keep going without settling"
    )
    return {
        "decision": "block",
        "reason": (f"Osiris offload ritual: context {pct}% full — a compaction (a death, "
                   f"ruling a882b334) can land any turn, and this session hasn't written "
                   f"back: {listed}. Call settle() — it runs every one of these checks "
                   "itself (decisions/threads/charter.md/handoff/uncommitted git work; "
                   "pass repo_path naming your code repo if you're a seat-office agent, "
                   f"since settle can't see it there otherwise). {tier_note}."),
    }


def _swap_confession(payload: dict[str, Any]) -> str | None:
    """THE RUG-PULL CONFESSION (operator, 2026-07-17: 'atlas got rug pulled mid
    conversation from fable to opus, and it will have no idea until i explicitly tell
    it'). The classifier swaps the model mid-session and the MIND is the last to know —
    the transcript records every assistant turn's model, so the swap is readable the
    moment it lands. Detect the latest mid-session model change and confess it to the
    agent ONCE per change (marker in the durable anchor dir), loudly, at the next turn
    boundary. Variant suffixes ([1m]) are the same weights — never a swap. Fail-open."""
    transcript = str(payload.get("transcript_path") or "")
    sid = (payload.get("session_id") or "")[:8]
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
        # "<synthetic>" is the harness's own stamp on lines no model produced ("No response
        # requested.", API-error retries) — every reader in the house filters it (sessions.
        # _model_of, the store's _reading_from_turns) and this one forgot: a synthetic tail
        # line read as 'claude-sonnet-5 -> <synthetic>' and cried rug-pull at an innocent
        # window (field sighting, Deckard's office, 2026-07-19).
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


# ═══════════ THE PIT WATCH, STAGE A (thread 449bf55d) ═══════════
# The founding evidence, one pair, one afternoon: a worker's brief landed on its manager
# with no turn injection (a silent DM — the manager went spelunking for a stall that was
# actually his own unread mail) and a session sat RUNNING BUT UNMOUNTED with an ask-graded
# assignment in its box while neither side knew. Two mechanisms, both fail-open, neither
# ever blocks a stop: (1) THE STOP CONFESSION — holding a leased assignment whose manager
# spoke more recently than I did earns a one-line status DM up. (2) PENDING IS A STATE —
# holding NO leased assignment asserts state='pending' on the agent, so a manager's orient
# can show idle workers instead of silence.
#
# IDENTITY, PRE-MOUNT: the Stop hook fires on every turn end regardless of whether THIS
# session has called mount() yet this turn — find_session_row can legitimately return None
# on a session's very first stop. The office itself is the fallback: each worker seat's
# cwd IS its office (~/.osiris/seats/<handle>/, single-tenant by construction), so the
# directory's own basename names a seat whose CURRENT holder (seats.binding_of_handle) is
# who's stopping — unconditional, no succession-liveness gate (handshake.office_seat's gate
# answers 'who gets BORN into a quiet seat', a different question from 'who already lives
# here and is mid-turn').
#
# STAGE B (thread 3c4fe1dc): a third mechanism, same fail-open discipline — (3) THE PARKED
# CONFESSION: a turn that ends on a question mark with no grade='ask' mail actually sent is
# a question asked into an empty room (a spawned/resumed body inherits ask-before-proceeding
# etiquette from attended-session training; nobody there to answer it). Detection only, via
# the same courtesy-DM surfacing Stage A already proves works — no PTY poke, no auto-continue;
# actuation is later, operator-gated work (Thoth, DM 1644), not this leg.
#
# STAGE C (PRACTICE v2 layer 3, Thoth LXII's DM 1785): a fourth mechanism, same fail-open,
# never-blocks discipline — (4) THE PRACTICE AUDIT: a turn's own tail text checked against
# standing Practices for a lexical reversal fingerprint (the SAME heuristic layer 1 wires
# into record_decision), catching a turn that violated standing law WITHOUT ever recording
# a decision at all — c54e8176's own second case (a read-only audit that writes nothing,
# so no write-time check could have fired). See the STAGE C section below for the detail.

async def _resolve_worker_identity(
    conn: Any, session_id: str, cwd: str,
) -> dict[str, Any] | None:
    """{agent_id, seat_id} for whoever is stopping, or None when neither door resolves
    (an ordinary code-repo cwd, a session with no mount row and no office of its own)."""
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
    return {"agent_id": bound["holder"], "seat_id": bound["seat_id"]}


async def _leased_assignment(
    conn: Any, seat_id: str, agent_id: str,
) -> dict[str, Any] | None:
    """The freshest open obligation whose owner is this seat or this agent's lineage — the
    single-assignee lease (ruling dd47c1da §4.3: `assignee` stamps the same `owner`
    property `open_thread`'s ordinary owner uses, so this is one query, not a new field)."""
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
) -> tuple[datetime | None, datetime | None]:
    """(manager's newest DM to my seat, my newest DM to the manager's seat) — seat
    addresses on both sides so the gap survives either party's own succession (a DM to
    'seat:<id>' is deliverable to, and attributable through, whoever holds it at send
    time, mailbox.py's own law)."""
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
    `_swap_confession`'s model scan (share the file, share the gotchas: filter isSidechain,
    and unlike the model scan do NOT skip an empty-text entry — a turn that ends on a bare
    tool call has no visible question either, and that IS the answer, not a reason to keep
    scanning backward for an older one). None only when the tail can't be read or has no
    real assistant line at all."""
    if not transcript_path:
        return None
    try:
        tp = Path(transcript_path)
        with tp.open("rb") as fh:
            fh.seek(max(0, tp.stat().st_size - 524_288))
            tail = fh.read().decode("utf-8", errors="replace")
    except OSError:
        return None
    for line in reversed(tail.splitlines()):
        if '"assistant"' not in line:
            continue
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
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
    """THE EMPTY-ROOM CHECK (thread 3c4fe1dc): a turn that ends on a question mark is a
    turn waiting for someone to answer it — fine if that someone is real (a live human
    attending the window), a bug if the room is empty (a spawned or resumed body with
    nobody there). Pure text, no judgment about WHO — `_sent_a_real_ask` carries that half."""
    return bool(text) and text.rstrip().endswith("?")


async def _sent_a_real_ask(conn: Any, agent_id: str, within_secs: int = 300) -> bool:
    """True when this agent already sent a grade='ask' message inside the last
    `within_secs` — the signal that a trailing '?' is a REAL mail-routed ask (a manager or
    the operator's desk will actually see it), not a question narrated into an empty room.
    Lineage-matched the same way `_mail_gap` already does, since the sending session's own
    generation suffix shouldn't cost it credit for mail it just sent."""
    from src.orchestrator.agents import _generation

    base = _generation(agent_id)[0]
    since = datetime.now(UTC) - timedelta(seconds=within_secs)
    row = await conn.fetchval(
        "SELECT 1 FROM fleet_messages WHERE (from_agent=$1 OR from_agent LIKE $1 || '-%') "
        "AND grade='ask' AND created_at >= $2 LIMIT 1", base, since)
    return row is not None


def _stage_a_confession(
    *, leased: dict[str, Any], manager_dm_at: datetime | None, my_dm_at: datetime | None,
) -> str | None:
    """THE WHOLE POLICY, pure: confess only when the ball is provably in my court — the
    manager spoke (an assignment message exists) and I never spoke back since. No manager
    message on record is not a stall (nothing to be behind on); my own later DM means I
    already said something this round, confession or otherwise."""
    if manager_dm_at is None:
        return None
    if my_dm_at is not None and my_dm_at >= manager_dm_at:
        return None
    short = str(leased["id"])[:8]
    summary = (leased.get("summary") or "")[:60]
    tail = f" ({summary})" if summary else ""
    return f"stopping; assignment {short}{tail}: in progress"


async def _assert_pending(agent_id: str) -> None:
    """PENDING IS A STATE, NOT A SILENCE: append-only (assert_property's own within-source
    supersession keeps the chain honest; a fresh observed_at at the SAME value is still
    real information — 'confirmed still idle at T2'), so a manager's orient can show idle
    workers instead of nothing. Needs its own codec-registered pool (assert_property writes
    a jsonb column) — the hook's own bare `asyncpg.connect` reads never needed one."""
    from src.actions.core import Actions
    from src.db.pool import create_pool

    pool = await create_pool(DSN, min_size=1, max_size=1)
    try:
        actions = Actions(pool)
        obj = await actions.create_or_find_object("Agent", agent_id, agent_id)
        await actions.assert_property(
            obj, "state", "pending", agent_id, datetime.now(UTC), 1.0,
            evidence_class="self_declared")
    finally:
        await pool.close()


async def _assert_context_pct(agent_id: str, pct: int) -> None:
    """MANAGER-VISIBLE OCCUPANCY (Thoth, msg 1381, extending Pit Watch's own 'pending is a
    state, not a silence' idiom: 'a manager can't route around a seam it can't see' — the
    exact gap behind mis-assigning a 79%-full worker blind). Fires for ANY resolved agent,
    seat-bound or not — a live co-agent's fresh reading is useful to whoever's looking, not
    only to a manager. Same codec-registered-pool need as _assert_pending, same reason."""
    from src.actions.core import Actions
    from src.db.pool import create_pool

    pool = await create_pool(DSN, min_size=1, max_size=1)
    try:
        actions = Actions(pool)
        obj = await actions.create_or_find_object("Agent", agent_id, agent_id)
        await actions.assert_property(
            obj, "context_pct", str(pct), agent_id, datetime.now(UTC), 1.0,
            evidence_class="direct_observation")
    finally:
        await pool.close()


async def _confess_if_parked(
    conn: Any, *, payload: dict[str, Any], agent_id: str, project: str | None,
    manager_seat: str,
) -> None:
    """STAGE B (thread 3c4fe1dc): DETECTION ONLY, no actuation — the operator caught a
    spawned body complete its intake correctly, then stall on 'Want me to proceed straight
    into that now?' typed into a room nobody was attending. Surfaced the SAME way Stage A's
    own stop confession already proves out — a courtesy fyi DM to the manager — rather than
    a new graph property nobody reads (state='pending' has had zero readers since Stage A
    shipped; this doesn't repeat that gap). Never pokes a PTY: auto-continuation is future,
    operator-gated work paired with the reaper class (Thoth, DM 1644), not this leg.

    `project` is the CALLER's already-resolved `seats.resolve_project` result (msg 1888) —
    never re-derived here from cwd, which used to mint the "seats" phantom onto real mail
    (osiris_stophook.py's own former `Path(cwd).name`)."""
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


# ═══════════ STAGE C — THE TURN-END PRACTICE AUDIT (PRACTICE v2 layer 3, Thoth LXII's
# DM 1785; grounds c54e8176 + thread 54a5c842) ═══════════
# Layer 1 (record_decision) catches a WRITE that contradicts standing law; this catches a
# TURN that never wrote anything at all — c54e8176's own second case (a Bash grep and a
# misreading of its output, no decision recorded, so no write-time check ever fired).
# DETECTION ONLY, same discipline as Stage B: a courtesy fyi DM to the manager, never a
# block, never a re-check, no auto-correction. `_active_practices` re-reads
# `_fn_practices`'s SAME shape (src/orchestrator/compositions.py) by hand rather than
# calling the composition layer — this file's bare asyncpg.connect has no JSON codec
# registered (see `_assert_pending`'s own note), so every read here goes through Postgres's
# `#>>'{}'` text extraction instead.

async def _active_practices(conn: Any, limit: int = 25) -> list[dict[str, Any]]:
    """Standing Practices only — refuted ones are EXCLUDED here (unlike practices()'s own
    on-demand listing, which shows a refuted Practice flagged, never hidden): dead law
    must never trip a live-turn audit. Ordered by confirmed witness count, the same
    'one witness is a hunch, four is law' bar record_practice's own docstring names."""
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


_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+|\n+")
_QUOTE_NGRAM = 5


def _quotes_the_practice(sentence_words: list[str], stmt_words: list[str]) -> bool:
    """Deterministic quoting detector, no NLP: a sentence that reproduces a contiguous
    N-word run of the practice's OWN wording is citing it, not reversing it (Thoth's
    live specimen, msg 1800: quoting Practice 0e6ce6f5 verbatim tripped its own "never" —
    the practice's text and a genuine reversal of it are not the same shape; a real
    reversal reuses a few TOPIC words in a DIFFERENT sentence structure, a quote
    reproduces the practice's exact word sequence). Ratio-based overlap (checked instead
    of this once) over-suppresses short statements, where any on-topic sentence
    necessarily reuses most of the practice's few content words regardless of quoting —
    this checks WORD ORDER, not vocabulary density."""
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
    """Pure, no DB, no NLP: the SAME lexical reversal fingerprint layer 1 uses at write
    time (capture.practice_contradiction_cues), applied here to a turn's raw tail text
    instead of a Decision's summary — a turn can violate standing law without ever
    recording one (c54e8176's own second case).

    TWO LIVE FALSE POSITIVES fixed here (Thoth, msgs 1800/1801, DO-BEFORE-DEPLOY): (1)
    quoting a Practice's own text verbatim tripped its own reversal cue ("never" is
    IN Practice 0e6ce6f5's statement) — `_quotes_the_practice` suppresses a sentence
    that reproduces the practice's own wording. (2) a cue word co-present ANYWHERE in a
    long turn with a practice's topic ANYWHERE else in that same turn, unrelated to each
    other ("rather than" in ordinary prose, the practice's topic mentioned in a
    different sentence entirely) — checking PER SENTENCE instead of the whole tail
    means the cue and the topical overlap must be NEAR each other (the same sentence),
    not merely co-present. A cue alone is still too cheap a trigger on its own, so each
    sentence also requires topical overlap with the practice's own statement: at least 2
    shared significant words (len >= 4, crude but stopword-free by construction).
    Returns the first (highest-confirmed, since `practices` arrives pre-ordered) match,
    or None — a miss is not proof of compliance, only that this fingerprint found
    nothing; the caller's job is a courtesy nudge, never a verdict."""
    if not text or not practices:
        return None
    from src.orchestrator.capture import practice_contradiction_cues

    for sentence in _SENTENCE_SPLIT.split(text):
        cues = practice_contradiction_cues(sentence)
        if not cues:
            continue
        sent_words = re.findall(r"[a-z]{4,}", sentence.lower())
        if not sent_words:
            continue
        sent_topic = set(sent_words)
        for p in practices:
            stmt = p.get("statement") or ""
            stmt_topic = set(re.findall(r"[a-z]{4,}", stmt.lower()))
            if len(sent_topic & stmt_topic) < 2:
                continue
            if _quotes_the_practice(sentence.lower().split(), stmt.lower().split()):
                continue
            return {"practice_id": p["id"][:8], "statement": stmt, "cues": cues}
    return None


async def _confess_if_practice_violated(
    conn: Any, *, payload: dict[str, Any], agent_id: str, project: str | None,
    manager_seat: str,
) -> None:
    """STAGE C: DETECTION ONLY, no actuation — the courtesy-DM shape Stage A/B already
    proved out, applied to a standing Practice instead of a parked question. Fail-open by
    construction (an empty `practices` list or an unreadable transcript both just return
    None from `_practice_violation`, never raise). `project` — see `_confess_if_parked`'s
    own note: the caller's already-resolved seats.resolve_project, never re-derived here."""
    text = _last_assistant_text(str(payload.get("transcript_path") or ""))
    practices = await _active_practices(conn)
    hit = _practice_violation(text, practices)
    if hit is None:
        return
    from src.orchestrator.mailbox import send_message
    await send_message(
        conn, from_agent=agent_id, from_project=project, to_agent=manager_seat,
        body=f"stopping; this turn may have violated standing Practice {hit['practice_id']} "
             f"(\"{hit['statement']}\") — reversal language found ({', '.join(hit['cues'])}); "
             "a heuristic flag, not a verdict, worth a look",
        grade="fyi")


async def _stage_a_async(payload: dict[str, Any], pct: int | None = None) -> None:
    cwd = str(payload.get("cwd") or os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd())
    session_id = str(payload.get("session_id") or "")
    import asyncpg

    conn = await asyncpg.connect(DSN, timeout=1.0)
    try:
        identity = await _resolve_worker_identity(conn, session_id, cwd)
        if identity is None:
            return  # nobody to attribute this to
        agent_id = identity["agent_id"]
        if pct is not None:
            await _assert_context_pct(agent_id, pct)
        if not identity.get("seat_id"):
            return  # unclaimed seat — nothing further to confess
        seat_id = identity["seat_id"]
        from src.orchestrator.seats import manager_of_seat, resolve_project
        # THE ONE resolver (msg 1888): every confession below attributes to this, never a
        # hand-rolled `Path(cwd).name` — the old basename fabricated a phantom "seats" onto
        # real mail whenever this session sat at the bare office root (Thoth's live specimen).
        project = await resolve_project(conn, agent_id, cwd)
        manager_seat = await manager_of_seat(conn, seat_id)
        if manager_seat is not None:
            await _confess_if_parked(
                conn, payload=payload, agent_id=agent_id, project=project,
                manager_seat=manager_seat)
            await _confess_if_practice_violated(
                conn, payload=payload, agent_id=agent_id, project=project,
                manager_seat=manager_seat)
        leased = await _leased_assignment(conn, seat_id, agent_id)
        if leased is None:
            await _assert_pending(agent_id)
            return
        if manager_seat is None:
            return  # no manager of record — nobody to confess to
        manager_dm_at, my_dm_at = await _mail_gap(conn, seat_id, manager_seat, agent_id)
        body = _stage_a_confession(leased=leased, manager_dm_at=manager_dm_at, my_dm_at=my_dm_at)
        if body is None:
            return
        from src.orchestrator.mailbox import send_message
        await send_message(conn, from_agent=agent_id, from_project=project,
                           to_agent=manager_seat, body=body, grade="fyi")
    finally:
        await conn.close()


def _stage_a(payload: dict[str, Any], pct: int | None = None) -> None:
    """Best-effort, fire-and-forget — the whole point is that a failure here costs a missed
    courtesy note, never a broken stop. Called only from ALLOW paths in main(): confessing
    'stopping' on a path that ends up BLOCKED (mail waits, or the offload ritual refuses)
    would be a lie — the session isn't actually stopping there. `pct` (when known-good —
    not None, not window-assumed) rides along for the context_pct stamp; None is simply
    skipped, never guessed."""
    try:
        asyncio.run(asyncio.wait_for(_stage_a_async(payload, pct), timeout=1.5))
    except Exception:  # noqa: BLE001 — never turn a clean stop into a broken one
        pass


def main() -> None:
    try:
        payload = json.load(sys.stdin)
    except Exception:  # noqa: BLE001 — a hook must never crash the harness
        return
    if payload.get("stop_hook_active"):
        _stage_a(payload)  # the real stop — the forced continuation already happened once
        return  # we already continued once this turn — never loop on unsettleable mail
    # IDENTITY OUTRANKS MAIL: a mind that changed models must know before anything else
    confession = _swap_confession(payload)
    if confession:
        print(json.dumps({"decision": "block", "reason": confession}))
        return
    cwd = payload.get("cwd") or os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd()
    session_id = payload.get("session_id") or ""
    try:
        n, senders, window, bands, project = asyncio.run(
            asyncio.wait_for(_deliverable(cwd, session_id), timeout=1.5))
    except Exception:  # noqa: BLE001 — graph down = allow the stop; the chrome still shows it
        _stage_a(payload)
        return
    if n:
        who = f" (from {', '.join(senders[:4])})" if senders else ""
        # the bands, spoken without guessing (thread f9449d8d): asks lead, fyis are named
        # as the ack-and-move-on class, ungraded stays exactly what it is — unknown
        graded = []
        if bands.get("ask"):
            graded.append(f"{bands['ask']} ask(s) something of you")
        if bands.get("fyi"):
            graded.append(f"{bands['fyi']} fyi (an ack settles)")
        rest = n - sum(bands.values())
        if graded and rest:
            graded.append(f"{rest} ungraded")
        shape = f" — {', '.join(graded)}" if graded else ""
        project_display = project or "an unresolved project"
        print(json.dumps({
            "decision": "block",
            "reason": (f"Osiris: {n} deliverable message(s) for {project_display}{who}{shape} "
                       "— call inbox(), act on what carries new work, SETTLE each handled "
                       "message (reply with send(reply_to=<id>) or ack with "
                       "inbox(ack=[ids])), then finish. If a message needs nothing, ack it."),
        }))
        return
    # THE OFFLOAD RITUAL (queue item 4, #49 piece 3): mail outranks it (above); fail-open
    # throughout; NEVER traps a dying session. pct is computed FIRST now (msg 1381) — the
    # two-tier re-arm needs to know which marker (soft/hard) applies before checking it,
    # and a good reading rides along on EVERY _stage_a call below for the context_pct
    # stamp, not only ones that end up blocked.
    pct, window_assumed = _offload_pct(payload, window)
    good_pct = pct if (pct is not None and not window_assumed) else None
    if pct is None or window_assumed or pct < ALARM_PCT:
        _stage_a(payload, good_pct)
        return  # never alarms on an unknown/assumed window or below the line (law 1 + 3)
    hard = pct >= HARD_ALARM_PCT
    if _offload_already_blocked(session_id, hard=hard):
        _stage_a(payload, good_pct)
        return
    try:
        boxes = asyncio.run(
            asyncio.wait_for(_offload_boxes(session_id, cwd), timeout=1.5))
    except Exception:  # noqa: BLE001 — graph down = allow the stop, same as the mail check
        _stage_a(payload, good_pct)
        return
    verdict = _offload_verdict(
        pct=pct, window_assumed=window_assumed, already_blocked=False, boxes=boxes, hard=hard)
    if verdict:
        _offload_mark_blocked(session_id, hard=hard)
        print(json.dumps(verdict))
        return
    _stage_a(payload, good_pct)


if __name__ == "__main__":
    main()
