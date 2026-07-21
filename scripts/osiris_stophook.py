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
import sys
from datetime import UTC, datetime
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
    project: str, session_id: str,
) -> tuple[int, list[str], int | None, dict[str, int]]:
    """(deliverable mail count, its senders, KNOWN window size or None, grade bands) for
    THIS session — mail mirrors mailbox.unread_count exactly (per-recipient, migration
    0021); the window comes from the mount row's context_window_size, stamped by the
    chrome from the harness's own accounting. An unmounted session has no inbox →
    (0, [], None, {}). The SENDERS ride along because a notification that omits them
    forces a read to discover whether a read was warranted (Metron V, msg 444) — and the
    GRADE BANDS ride for the same reason (thread f9449d8d: an FYI wearing a duty's
    authority teaches the reader to skim, and a skimmed mailbox is lost; the nag says
    which of the pile actually asks something, without guessing the ungraded)."""
    import asyncpg

    conn = await asyncpg.connect(DSN, timeout=1.0)
    try:
        # the ONE session→row lookup (mounts.find_session_row, task #33) — this hook's
        # inline anchor-name match was the mail arc's silent half: a re-anchored window
        # was never nagged, so its mail sat while the session lived
        from src.orchestrator.mounts import find_session_row
        row = await find_session_row(conn, session_id or "")
        if row is None or not row["agent_id"]:
            # (the old `return 0, None` here was a 2-tuple against a 3-tuple signature —
            # the unmounted path "worked" only because the caller's fail-open ate the
            # unpack error; now it declines honestly)
            return 0, [], None, {}
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
            "AND m.read_at IS NULL AND r.read_at IS NULL "
            "AND (r.delivered_at IS NULL OR r.delivered_at < now() - make_interval(secs => $3))",
            row["agent_id"], project, STOP_GRACE_SECS, base)
        n = int(n_row["n"]) if n_row else 0
        senders = [s for s in (n_row["senders"] or []) if s] if n_row else []
        bands = ({"ask": int(n_row["asks"] or 0), "fyi": int(n_row["fyis"] or 0)}
                 if n_row else {})
        return n, senders, row["context_window_size"], bands
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

async def _offload_boxes(
    session_id: str, cwd: str,
) -> dict[str, bool | None] | None:
    """The boxes the ritual checks for THIS session, best-effort per box — a box this
    query could not evaluate is None and never appears in a refusal (fail open per-box,
    the same law the whole hook runs on). Returns None only when the session itself can't
    be resolved to an agent (nothing to check at all — the caller treats that exactly like
    'everything satisfied').

    `mounted_at` (agent_mounts) is this session's own first-mount stamp — set once at
    INSERT, never touched by a re-attach's UPDATE — so it is session start, not a guess.
    Decisions/threads are 'this session's' when their defining assertion's source_id is
    this EXACT agent_id (the identity that mounted this job_dir) at or after mounted_at."""
    import asyncpg

    conn = await asyncpg.connect(DSN, timeout=1.0)
    try:
        from src.orchestrator.mounts import find_session_row
        row = await find_session_row(conn, session_id or "")
        if row is None or not row["agent_id"] or not row["mounted_at"]:
            return None
        agent_id = str(row["agent_id"])
        mounted_at = row["mounted_at"]
        boxes: dict[str, bool | None] = {}
        try:
            boxes["decisions recorded this session"] = bool(await conn.fetchval(
                "SELECT 1 FROM assertions a JOIN objects o ON o.id = a.object_id "
                "WHERE o.type = 'Decision' AND a.name = 'summary' AND a.source_id = $1 "
                "AND a.observed_at >= $2 LIMIT 1", agent_id, mounted_at))
        except Exception:  # noqa: BLE001 — one box's failure never dooms the others
            boxes["decisions recorded this session"] = None
        try:
            boxes["threads trued this session (opened or resolved)"] = bool(await conn.fetchval(
                "SELECT 1 FROM assertions a JOIN objects o ON o.id = a.object_id "
                "WHERE o.type = 'Thread' AND a.name IN ('summary', 'status') "
                "AND a.source_id = $1 AND a.observed_at >= $2 LIMIT 1", agent_id, mounted_at))
        except Exception:  # noqa: BLE001
            boxes["threads trued this session (opened or resolved)"] = None
        boxes["charter.md touched this session"] = _charter_touched(cwd, mounted_at)
        # a live succession/handoff note — ONLY asked of a session whose own agent object
        # was itself born by a mint (minted_because stamped at birth, permanent on that
        # exact generation): a fresh heir owes its OWN heir at least one obligation left
        # behind, not just mail settled. No content-classifier (Thoth LI's amend, msg
        # 861's law extends here too) — the primitive is 'opened an obligation', not
        # 'looks like a handoff'.
        try:
            minted = bool(await conn.fetchval(
                "SELECT 1 FROM current_assertions a JOIN objects o ON o.id = a.object_id "
                "WHERE o.canonical = $1 AND a.name = 'minted_because' LIMIT 1", agent_id))
        except Exception:  # noqa: BLE001
            minted = False
        if minted:
            try:
                boxes["a live succession/handoff note (this lineage was minted)"] = bool(
                    await conn.fetchval(
                        "SELECT 1 FROM assertions a JOIN objects o ON o.id = a.object_id "
                        "WHERE o.type = 'Thread' AND a.name = 'kind' "
                        "AND a.value #>> '{}' = 'obligation' AND a.source_id = $1 "
                        "AND a.observed_at >= $2 LIMIT 1", agent_id, mounted_at))
            except Exception:  # noqa: BLE001
                boxes["a live succession/handoff note (this lineage was minted)"] = None
        return boxes
    finally:
        await conn.close()


def _charter_touched(cwd: str, mounted_at: datetime) -> bool | None:
    """None (can't evaluate, fails open) when this cwd has no charter.md at all — a
    session working in an ordinary repo, not an office, is never punished for a file
    that was never scaffolded here. Present: mtime at or after session start."""
    try:
        charter = Path(cwd) / "charter.md"
        if not charter.exists():
            return None
        return charter.stat().st_mtime >= mounted_at.timestamp()
    except OSError:
        return None


def _offload_marker(session_id: str) -> Path | None:
    """The block-once marker's path — same convention as the swap/death-rite markers
    (a file under this session's durable anchor dir), None for an id too short to trust."""
    sid = (session_id or "")[:8]
    if len(sid) < 8:
        return None
    return Path.home() / ".claude" / "jobs" / sid / ".osiris_offload_blocked"


def _offload_already_blocked(session_id: str) -> bool:
    marker = _offload_marker(session_id)
    if marker is None:
        return False
    try:
        return marker.exists()
    except OSError:
        return False  # can't tell → never trap a session on a filesystem hiccup


def _offload_mark_blocked(session_id: str) -> None:
    marker = _offload_marker(session_id)
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
    boxes: dict[str, bool | None] | None,
) -> dict[str, Any] | None:
    """THE WHOLE POLICY, pure — no I/O, no clock (pct/boxes are supplied, not derived
    here), so every law is a direct unit test. BLOCK ONCE THEN ALLOW: `already_blocked`
    short-circuits everything — a dying session is never trapped in a refusal loop.
    NEVER on an unknown or assumed window (the Anubis VII false-eulogy law, msg 127) or
    below context_lens.ALARM_PCT. And even above the line, a refusal fires only when
    something is GENUINELY unwritten — `boxes` with nothing False (all satisfied, or
    everything fog-of-war None) has nothing to enforce and never blocks."""
    if already_blocked or pct is None or window_assumed or pct < ALARM_PCT or not boxes:
        return None
    missing = [label for label, ok in boxes.items() if ok is False]
    if not missing:
        return None
    listed = "; ".join(missing)
    return {
        "decision": "block",
        "reason": (f"Osiris offload ritual: context {pct}% full — a compaction (a death, "
                   f"ruling a882b334) can land any turn, and this session hasn't written "
                   f"back: {listed}. charter.md is your offload target for live state the "
                   "graph's typed objects can't hold — write there, and record_decision / "
                   "open_thread (kind='obligation') / resolve_thread for the rest. This "
                   "refusal fires once per session; the next stop is never blocked again."),
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


async def _manager_seat(conn: Any, seat_id: str) -> str | None:
    return await conn.fetchval(  # type: ignore[no-any-return]
        "SELECT t.canonical FROM links l JOIN objects f ON f.id=l.from_id "
        "JOIN objects t ON t.id=l.to_id WHERE f.canonical=$1 AND l.type='managed_by' "
        "AND t.type='Seat' AND t.status='active' "
        "AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "ORDER BY l.first_seen DESC LIMIT 1", seat_id)


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


async def _stage_a_async(payload: dict[str, Any]) -> None:
    cwd = str(payload.get("cwd") or os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd())
    session_id = str(payload.get("session_id") or "")
    import asyncpg

    conn = await asyncpg.connect(DSN, timeout=1.0)
    try:
        identity = await _resolve_worker_identity(conn, session_id, cwd)
        if identity is None or not identity.get("seat_id"):
            return  # nobody to attribute this to, or an unclaimed seat — nothing to confess
        agent_id, seat_id = identity["agent_id"], identity["seat_id"]
        leased = await _leased_assignment(conn, seat_id, agent_id)
        if leased is None:
            await _assert_pending(agent_id)
            return
        manager_seat = await _manager_seat(conn, seat_id)
        if manager_seat is None:
            return  # no manager of record — nobody to confess to
        manager_dm_at, my_dm_at = await _mail_gap(conn, seat_id, manager_seat, agent_id)
        body = _stage_a_confession(leased=leased, manager_dm_at=manager_dm_at, my_dm_at=my_dm_at)
        if body is None:
            return
        from src.orchestrator.mailbox import send_message
        await send_message(conn, from_agent=agent_id, from_project=Path(cwd).name,
                           to_agent=manager_seat, body=body, grade="fyi")
    finally:
        await conn.close()


def _stage_a(payload: dict[str, Any]) -> None:
    """Best-effort, fire-and-forget — the whole point is that a failure here costs a missed
    courtesy note, never a broken stop. Called only from ALLOW paths in main(): confessing
    'stopping' on a path that ends up BLOCKED (mail waits, or the offload ritual refuses)
    would be a lie — the session isn't actually stopping there."""
    try:
        asyncio.run(asyncio.wait_for(_stage_a_async(payload), timeout=1.5))
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
    project = Path(cwd).name
    session_id = payload.get("session_id") or ""
    try:
        n, senders, window, bands = asyncio.run(
            asyncio.wait_for(_deliverable(project, session_id), timeout=1.5))
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
        print(json.dumps({
            "decision": "block",
            "reason": (f"Osiris: {n} deliverable message(s) for {project}{who}{shape} — call "
                       "inbox(), act on what carries new work, SETTLE each handled message "
                       "(reply with send(reply_to=<id>) or ack with inbox(ack=[ids])), then "
                       "finish. If a message needs nothing, ack it."),
        }))
        return
    # THE OFFLOAD RITUAL (queue item 4, #49 piece 3): mail outranks it (above); fail-open
    # throughout; NEVER traps a dying session (the marker check is the whole escape hatch,
    # and it runs FIRST — a session already refused this cycle pays no further cost).
    if _offload_already_blocked(session_id):
        _stage_a(payload)
        return
    pct, window_assumed = _offload_pct(payload, window)
    if pct is None or window_assumed or pct < ALARM_PCT:
        _stage_a(payload)
        return  # never alarms on an unknown/assumed window or below the line (law 1 + 3)
    try:
        boxes = asyncio.run(
            asyncio.wait_for(_offload_boxes(session_id, cwd), timeout=1.5))
    except Exception:  # noqa: BLE001 — graph down = allow the stop, same as the mail check
        _stage_a(payload)
        return
    verdict = _offload_verdict(
        pct=pct, window_assumed=window_assumed, already_blocked=False, boxes=boxes)
    if verdict:
        _offload_mark_blocked(session_id)
        print(json.dumps(verdict))
        return
    _stage_a(payload)


if __name__ == "__main__":
    main()
