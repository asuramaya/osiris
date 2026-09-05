"""OBLIGATION HYGIENE — the no-regrow rule (dispatch #204's own follow-on, decision
a44ab697161a proposing N1=14/N2=14; OPERATOR RULED TIGHTER, 2026-09-05 ~04:40Z, relayed
Thoth DM 7161): N1 = 7 idle days -> a DM nudge to the obligation's own owner. N2 = +7 more
days of continued silence past that nudge -> a STALE-CANDIDATE marker plus a desk brief,
surfaced for a human. NEVER AUTO-RESOLVED at either stage — this mechanism only nudges and
surfaces, it never closes, reclassifies away from 'open', or judges a thread dead on its
own authority.

THE SHAPE mirrors phantom_fold_reap.py's own two-phase discipline (a pure `_dry_run`
report, then a separately-gated `_execute`) rather than reinventing it, and reuses
open_thread_wall's own `last_touched`/`owner` reads (compositions.py) — the SAME
authoritative clock (`assertions.evidence_class='self_declared'`, max `observed_at`) that
already answers "has a MIND touched this" everywhere else in the fleet.

IDLE, exactly as proposed and ruled on: a thread's own `last_touched` is at least N1 days
old, AND the owner has made no self_declared graph write ANYWHERE in that window — an
owner who is visibly alive and working the graph gets the benefit of the doubt even before
they get to this particular thread. Both halves must hold; neither alone is idle.

TWO DURABLE MARKERS, both assertions on the thread itself (name `hygiene_stage`, values
'nudged' / 'stale_candidate'; `hygiene_nudged_at` records when N1 fired) — written at
`EvidenceClass.DERIVED`, DELIBERATELY NOT 'self_declared', so this sweep's own writes can
never count as the "touch" that resets a thread's own idle clock or fools
open_thread_wall's untouched/echo split. A thread genuinely re-annotated by a mind after a
nudge resets the clock and starts a fresh N1 window, exactly as if never nudged.

THE OWNER-ADDRESS FALLBACK (owner='operator' when the owner is a project name or no live
agent, exactly as ruled): the nudge always tries a DM to the declared owner first — a
project-name owner, or one `send_message` cannot resolve to a live agent, routes to the
operator's desk instead. `send_message`'s own ValueError on an unresolvable `to_agent` IS
that "no live agent" signal; this module never re-derives seat liveness a second way.

SCHEDULED LEG stays a normal cron switch (`osiris_obligation_hygiene_enabled`) — but per
the operator's own explicit instruction ("land it with the flag ON"), this ships TRUE by
default rather than the dark-by-default convention every sibling scheduled writer in this
house otherwise follows (fleet_reconcile, phantom_heal, phantom_fold_reap, landing_audit,
tree_ingest_alarm) — a deliberate, named exception, not an oversight."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.config.settings import Settings, get_settings
from src.parsers.base import EvidenceClass

N1_IDLE_DAYS = 7
N2_SILENCE_DAYS = 7

_HYGIENE_EC = EvidenceClass.DERIVED.value
_SANCTIONED_HYGIENE_ACTOR = "cron:obligation_hygiene_heartbeat"

# THE SAME summary-display COALESCE every wall/roadmap query uses (compositions.py's own
# _SUMMARY_DISPLAY_SQL) — a corrected summary wins over the original by default.
_SUMMARY_SQL = (
    "COALESCE("
    "(SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
    " AND a.name='corrected_summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), "
    "(SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
    " AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1))"
)

_KIND_SQL = (
    "(SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
    " AND a.name='kind' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1)"
)
_STATUS_SQL = (
    "COALESCE((SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
    " AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'open')"
)


async def _open_obligation_rows(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """Every OPEN kind='obligation' Thread fleet-wide, with `owner`, `last_touched` (the
    freshest self_declared assertion's observed_at — open_thread_wall's own clock), and
    this sweep's own prior markers read back (`hygiene_stage`/`hygiene_nudged_at`) so a
    tick is idempotent against its own last tick."""
    rows = await pool.fetch(
        "SELECT o.id, o.created_at, "
        f" {_SUMMARY_SQL} AS summary, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS owner, "
        " (SELECT max(sa.observed_at) FROM assertions sa WHERE sa.object_id=o.id "
        "   AND sa.evidence_class='self_declared') AS last_touched, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='hygiene_stage' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS hygiene_stage, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='hygiene_nudged_at' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS hygiene_nudged_at "
        "FROM objects o "
        f"WHERE o.type='Thread' AND o.merged_into IS NULL AND o.status='active' "
        f"  AND {_STATUS_SQL}='open' AND {_KIND_SQL}='obligation'")
    return [dict(r) for r in rows]


async def _owner_active_since(pool: asyncpg.Pool, owner: str, since: datetime) -> bool:
    """Has this owner made ANY self_declared graph write, anywhere, since `since`? — the
    idle definition's own "no graph write by its owner in the window" half, deliberately
    fleet-wide rather than scoped to one thread. Case-insensitive exact match on
    `assertions.source_id`, the same forgiving match rank_open_threads already applies to
    a bare seat handle."""
    return bool(await pool.fetchval(
        "SELECT 1 FROM assertions WHERE evidence_class='self_declared' "
        "  AND lower(source_id)=lower($1) AND observed_at > $2 LIMIT 1", owner, since))


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


async def hygiene_dry_run(pool: asyncpg.Pool, *, now: datetime | None = None) -> dict[str, Any]:
    """THE REPORT — every open obligation Thread, bucketed into would_nudge /
    would_stale_candidate / no_action with the reason named. Writes NOTHING."""
    now = now or datetime.now(UTC)
    n1_cutoff = now - timedelta(days=N1_IDLE_DAYS)
    rows = await _open_obligation_rows(pool)
    buckets: dict[str, list[dict[str, Any]]] = {
        "would_nudge": [], "would_stale_candidate": [], "no_action": [],
    }
    for r in rows:
        last_touched = r["last_touched"] or r["created_at"]
        owner = (r["owner"] or "").strip() or None
        stage = r["hygiene_stage"]
        nudged_at = _parse_dt(r["hygiene_nudged_at"])
        item: dict[str, Any] = {
            "thread_id": str(r["id"]), "owner": owner, "summary": r["summary"],
            "last_touched": last_touched.isoformat() if last_touched else None,
            "stage": stage,
        }

        if stage == "stale_candidate":
            buckets["no_action"].append({
                **item, "reason": "already a stale-candidate — never re-classified, "
                                   "never auto-resolved"})
            continue

        if stage == "nudged" and nudged_at is not None and last_touched <= nudged_at:
            # No touch on the thread since its own nudge — check the N2 silence window.
            if now - nudged_at >= timedelta(days=N2_SILENCE_DAYS):
                owner_silent = True
                if owner:
                    owner_silent = not await _owner_active_since(pool, owner, nudged_at)
                if owner_silent:
                    buckets["would_stale_candidate"].append(item)
                    continue
            buckets["no_action"].append({
                **item, "reason": "nudged — awaiting the N2 window or the owner's own "
                                   "activity"})
            continue

        # stage is None, OR 'nudged' but genuinely touched since (last_touched > nudged_at)
        # — a real touch resets the clock exactly as if never nudged.
        if now - last_touched >= timedelta(days=N1_IDLE_DAYS):
            owner_silent = True
            if owner:
                owner_silent = not await _owner_active_since(pool, owner, n1_cutoff)
            if owner_silent:
                buckets["would_nudge"].append(item)
                continue
        buckets["no_action"].append({**item, "reason": "not idle"})

    return {
        "buckets": buckets, "counts": {k: len(v) for k, v in buckets.items()},
        "generated_at": now.isoformat(),
    }


async def _nudge_owner(
    pool: asyncpg.Pool, owner: str | None, *, actor: str, body: str,
) -> dict[str, Any]:
    """The owner-address fallback: a project-name owner, an unset owner, or an owner
    `send_message` cannot resolve to a live agent all route to the operator's desk
    instead of a DM — 'owner=operator when the owner is a project name or no live agent',
    exactly as ruled."""
    from src.orchestrator.mailbox import send_message

    target = (owner or "").strip()
    is_project_name = False
    if target and target.lower() != "operator":
        is_project_name = bool(await pool.fetchval(
            "SELECT 1 FROM objects WHERE type='SoftwareProject' AND "
            "(lower(canonical)=lower($1) OR lower(canonical)=lower('repo:'||$1)) LIMIT 1",
            target))
    if not target or target.lower() == "operator" or is_project_name:
        return await send_message(
            pool, from_agent=actor, from_project="osiris", to_project="operator",
            body=body, desk_kind="fyi")
    try:
        return await send_message(
            pool, from_agent=actor, from_project="osiris", to_agent=target, body=body,
            grade="ask")
    except ValueError:
        # send_message's own refusal to resolve to_agent IS "no live agent" — never
        # re-derived by a second liveness lookup.
        return await send_message(
            pool, from_agent=actor, from_project="osiris", to_project="operator",
            body=body, desk_kind="fyi")


async def hygiene_execute(
    actions: Actions, *, actor: str = _SANCTIONED_HYGIENE_ACTOR, execute: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """THE ACTING HALF. DRY RUN IS THE DEFAULT (`execute=False`) — re-reads the tray itself
    via `hygiene_dry_run` (never trusts a caller-supplied stale report), returning the exact
    plan without writing anything.

    would_nudge: a DM (or desk brief, per the owner-address fallback) plus the two durable
    markers (`hygiene_nudged_at`, `hygiene_stage='nudged'`) — written even if the mail send
    itself fails, so a mail hiccup never masks the tick's own idle finding; the failed send
    is reported inline on the row instead.

    would_stale_candidate: `hygiene_stage='stale_candidate'` plus one desk `fyi` brief.
    NEVER a status change on the thread, never a resolve — the record's own law here is
    identical to phantom_fold_reap's: a single row's mail hiccup is caught and reported
    inline rather than aborting the batch."""
    now = now or datetime.now(UTC)
    report = await hygiene_dry_run(actions.pool, now=now)
    plan: dict[str, Any] = {
        "would_nudge": [dict(r) for r in report["buckets"]["would_nudge"]],
        "would_stale_candidate": [dict(r) for r in report["buckets"]["would_stale_candidate"]],
        "execute": execute,
    }
    if not execute:
        plan["note"] = "PLAN ONLY — call with execute=True to write. Nothing touched."
        return plan

    nudged: list[dict[str, Any]] = []
    for item in plan["would_nudge"]:
        tid = uuid.UUID(item["thread_id"])
        body = (
            f"OBLIGATION HYGIENE NUDGE — thread {item['thread_id'][:8]} has been idle "
            f"{N1_IDLE_DAYS}+ days ({item['summary']!r}). Touch it (annotate/resolve/"
            f"reclassify) or it becomes a STALE-CANDIDATE on the operator's desk after "
            f"{N2_SILENCE_DAYS} more days of silence. Never auto-resolved.")
        try:
            sent = await _nudge_owner(actions.pool, item["owner"], actor=actor, body=body)
        except Exception as exc:  # noqa: BLE001 — a mail hiccup must not skip the marker
            sent = {"error": f"{type(exc).__name__}: {exc}"}
        await actions.assert_property(
            tid, "hygiene_nudged_at", now.isoformat(), actor, now, 0.9,
            evidence_class=_HYGIENE_EC)
        await actions.assert_property(
            tid, "hygiene_stage", "nudged", actor, now, 0.9, evidence_class=_HYGIENE_EC)
        nudged.append({**item, "sent": sent})

    staled: list[dict[str, Any]] = []
    for item in plan["would_stale_candidate"]:
        tid = uuid.UUID(item["thread_id"])
        await actions.assert_property(
            tid, "hygiene_stage", "stale_candidate", actor, now, 0.9,
            evidence_class=_HYGIENE_EC)
        body = (
            f"OBLIGATION STALE-CANDIDATE — thread {item['thread_id'][:8]} "
            f"(owner={item['owner']!r}, {item['summary']!r}) drew a nudge and "
            f"{N2_SILENCE_DAYS}+ more days of silence since. NEVER auto-resolved — a "
            f"human call on whether it's still real.")
        try:
            from src.orchestrator.mailbox import send_message
            sent = await send_message(
                actions.pool, from_agent=actor, from_project="osiris",
                to_project="operator", body=body, desk_kind="fyi")
        except Exception as exc:  # noqa: BLE001 — the marker must land even if mail fails
            sent = {"error": f"{type(exc).__name__}: {exc}"}
        staled.append({**item, "desk_brief": sent})

    plan.update({
        "nudged": nudged, "staled": staled,
        "note": "EXECUTED — markers and mail attempted for every row named above.",
    })
    return plan


async def hygiene_status(pool: asyncpg.Pool) -> dict[str, Any]:
    """counts per stage per project — 'none' (never nudged), 'nudged', 'stale_candidate' —
    across every open kind='obligation' Thread fleet-wide. An unfiled obligation (no
    in_repo link) counts under '(unfiled)'. Read-only, no scheduling side effect."""
    rows = await pool.fetch(
        "SELECT COALESCE(p.canonical, '(unfiled)') AS project, "
        " COALESCE((SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='hygiene_stage' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1), "
        "   'none') AS stage, "
        " count(*) AS n "
        "FROM objects o "
        "LEFT JOIN links l ON l.from_id=o.id AND l.type='in_repo' "
        "  AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "LEFT JOIN objects p ON p.id=l.to_id "
        f"WHERE o.type='Thread' AND o.merged_into IS NULL AND o.status='active' "
        f"  AND {_STATUS_SQL}='open' AND {_KIND_SQL}='obligation' "
        "GROUP BY 1, 2 ORDER BY 1, 2")
    out: dict[str, dict[str, int]] = {}
    for r in rows:
        out.setdefault(r["project"], {})[r["stage"]] = r["n"]
    return {"counts_by_project": out, "generated_at": datetime.now(UTC).isoformat()}


async def obligation_hygiene_scheduled_tick(
    actions: Actions, *, settings: Settings | None = None, now: datetime | None = None,
) -> dict[str, Any]:
    """THE SCHEDULED LEG's own tick — `arq_worker.obligation_hygiene_heartbeat` calls this
    unconditionally, the same thin-shim shape every other scheduled writer in this house
    uses. OFF unless `osiris_obligation_hygiene_enabled` — but per the operator's own
    explicit instruction this ships TRUE by default, a named exception to every sibling
    switch's dark-by-default convention."""
    st = settings or get_settings()
    if not st.osiris_obligation_hygiene_enabled:
        return {"enabled": False, "nudged": [], "staled": [],
                "note": "the sweep's scheduled leg is dark "
                        "(osiris_obligation_hygiene_enabled=0)"}
    out = await hygiene_execute(actions, actor=_SANCTIONED_HYGIENE_ACTOR, execute=True,
                                now=now)
    return {"enabled": True, **out}
