"""THE NO-REGROW RULE (operator's word via Thoth msg 8606/8618, 2026-09-09, dispatched
in the same breath as the owner-law residue fix): an open obligation Thread that carries
a `stale_after` and has drawn NO annotate/owner-change/resolution touch for N=7 days
PAST that timestamp reclassifies to kind='task' by the heartbeat -- with a receipt on the
owner's own mail, never silent. NEVER RESOLVED by this sweep: `status` is left exactly as
it was, only `kind` moves, so the thread drops off the obligation wall's own ranking
without erasing it or lying about whether it actually closed.

DISTINCT FROM obligation_hygiene.py's OWN no-regrow leg (N1=7/N2=7 idle-since-last-touch,
a DM nudge then a STALE-CANDIDATE marker, never touches `kind`) -- that sweep already
ships and stays exactly as it is. This is a SEPARATE clock, keyed off `stale_after` (a
property every open_thread already writes, carrying its own decided window) rather than
idle-since-creation, and it actually MOVES the thread's own classification once the
window closes, matching #203's own no-regrow proposal by name ("an obligation with no
annotate, no owner change and no resolution for 7 days past its stale_after reclassifies
to task"). Reuses reclassify_thread (capture.py) for the write -- the SAME sanctioned
triage verb `thread(action='reclassify')` calls, never a second copy of the kind-mutation
logic -- and obligation_hygiene.py's own owner-resolution ladder (`resolve_owner_target`/
`_nudge_owner`) for the receipt, never a third re-derivation of "who does this owner
string address."

TOUCHED, exactly as ruled: any self_declared write on the thread AFTER its own
`stale_after` timestamp -- annotate, an owner reassignment, a corrected_summary, a
resolve, anything -- resets the window entirely; a thread genuinely worked after going
stale is never swept out from under a mind mid-conversation. Reuses `last_touched`, the
SAME authoritative clock (the freshest self_declared assertion's own observed_at) every
other wall/hygiene reader in this house already trusts -- never a second, drifting
definition of "touched."

Same mirrored plan/execute split as obligation_hygiene.py and phantom_fold_reap.py: a
pure `plan_no_regrow` dry-run report, then a separately-gated `apply_no_regrow`. A row's
own mail hiccup on the receipt is caught and reported inline on that row -- never aborts
the batch, same law as its siblings.

CONTESTED IS EXCLUDED, fix (d) (Metron's mechanism report, mail 8890/8921/8922): a
thread whose newest note disputes its own summary never reclassifies here, even past
the grace window -- doing so would drop a false headline off the obligation wall right
when it most needs a human's eye, backwards from the rule's own purpose. The exclusion
lifts once the summary is corrected or the thread resolves, same as the marker itself."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.config.settings import Settings, get_settings

N_GRACE_DAYS = 7  # operator 2026-09-09: "make it 7 days, flip it on" (was 21)

_SANCTIONED_NO_REGROW_ACTOR = "cron:no_regrow_heartbeat"

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
_STALE_AFTER_SQL = (
    "(SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
    " AND a.name='stale_after' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1)"
)


async def _candidate_rows(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    """Every OPEN kind='obligation' Thread carrying a `stale_after`, with `owner` and
    `last_touched` (the freshest self_declared write's own observed_at, or the thread's
    own creation time when never touched at all).

    `contested` (fix (d), Metron's mechanism report, mail 8890/8921/8922): present so
    `plan_no_regrow` can EXCLUDE a disputed thread from reclassification — reclassifying
    away from 'obligation' would let a false headline drop off the obligation wall right
    when a note has just proven it wrong, exactly backwards from what should happen."""
    from src.orchestrator.capture import CONTESTED_SQL

    rows = await pool.fetch(
        "SELECT o.id, o.created_at, "
        f" {_SUMMARY_SQL} AS summary, "
        f" {_STALE_AFTER_SQL} AS stale_after, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS owner, "
        " (SELECT max(sa.observed_at) FROM assertions sa WHERE sa.object_id=o.id "
        "   AND sa.evidence_class='self_declared') AS last_touched, "
        f" {CONTESTED_SQL} AS contested "
        "FROM objects o "
        f"WHERE o.type='Thread' AND o.merged_into IS NULL AND o.status='active' "
        f"  AND {_STATUS_SQL}='open' AND {_KIND_SQL}='obligation' "
        f"  AND {_STALE_AFTER_SQL} IS NOT NULL")
    return [dict(r) for r in rows]


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


async def plan_no_regrow(
    pool: asyncpg.Pool, *, now: datetime | None = None,
) -> dict[str, Any]:
    """THE REPORT — every open obligation Thread with a `stale_after`, bucketed into
    would_reclassify / no_action with the reason named. Writes NOTHING."""
    now = now or datetime.now(UTC)
    rows = await _candidate_rows(pool)
    would_reclassify: list[dict[str, Any]] = []
    no_action: list[dict[str, Any]] = []
    for r in rows:
        stale_after = _parse_dt(r["stale_after"])
        assert stale_after is not None  # the WHERE clause above already guarantees this
        last_touched = r["last_touched"] or r["created_at"]
        item: dict[str, Any] = {
            "thread_id": str(r["id"]), "owner": (r["owner"] or "").strip() or None,
            "summary": r["summary"], "stale_after": stale_after.isoformat(),
            "last_touched": last_touched.isoformat() if last_touched else None,
        }
        if last_touched is not None and last_touched > stale_after:
            no_action.append({**item, "reason": "touched since going stale — window reset"})
            continue
        if r["contested"]:
            # fix (d), mail 8890/8921/8922: a note has disputed this summary and nothing
            # has corrected it yet — reclassifying away from 'obligation' now would drop
            # a false headline off the wall right when it most needs a human's eye, the
            # exact opposite of what this rule exists to do.
            no_action.append({**item, "reason": "CONTESTED — a newer note disputes this "
                                                "summary; correct it or resolve it "
                                                "before this reclassifies"})
            continue
        if now - stale_after >= timedelta(days=N_GRACE_DAYS):
            would_reclassify.append(item)
            continue
        no_action.append({**item, "reason": "past stale_after but still inside the "
                                            f"{N_GRACE_DAYS}-day grace window"})
    return {
        "would_reclassify": would_reclassify, "no_action": no_action,
        "counts": {"would_reclassify": len(would_reclassify), "no_action": len(no_action)},
        "generated_at": now.isoformat(),
    }


async def apply_no_regrow(
    actions: Actions, *, actor: str = _SANCTIONED_NO_REGROW_ACTOR, execute: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """THE ACTING HALF. DRY RUN IS THE DEFAULT (`execute=False`) — re-reads the tray via
    `plan_no_regrow` (never trusts a caller-supplied stale report), returning the plan
    without writing anything. On `execute=True`: `reclassify_thread(kind='task')` per row
    (status untouched — never a resolve), plus a receipt DM to the owner (or the
    operator's desk, per the SAME owner-address fallback obligation_hygiene.py's own
    `_nudge_owner` already implements — never a second copy of that ladder). A row's own
    mail hiccup is caught and reported inline; the reclassification itself still lands."""
    from src.orchestrator.capture import reclassify_thread
    from src.orchestrator.obligation_hygiene import _nudge_owner

    now = now or datetime.now(UTC)
    report = await plan_no_regrow(actions.pool, now=now)
    plan: dict[str, Any] = {
        "would_reclassify": report["would_reclassify"], "execute": execute,
    }
    if not execute:
        plan["note"] = "PLAN ONLY — call with execute=True to write. Nothing touched."
        return plan

    reclassified: list[dict[str, Any]] = []
    for item in plan["would_reclassify"]:
        because = (
            f"no-regrow rule: no annotate/owner-change/resolution for {N_GRACE_DAYS}+ "
            f"days past stale_after ({item['stale_after']}) — auto-reclassified, never "
            "auto-resolved")
        tid = await reclassify_thread(
            actions, item["thread_id"], kind="task", because=because, source=actor)
        body = (
            f"NO-REGROW RECLASSIFICATION — thread {item['thread_id'][:8]} "
            f"({item['summary']!r}) went {N_GRACE_DAYS}+ days past its own stale_after "
            "with no touch, and has been reclassified kind='task' by the heartbeat. "
            "Status is UNCHANGED — this never resolves anything on its own authority. "
            "Annotate, reclassify back to 'obligation', or resolve it if it's still (or "
            "again) owed work.")
        try:
            sent = await _nudge_owner(actions.pool, item["owner"], actor=actor, body=body)
        except Exception as exc:  # noqa: BLE001 — a mail hiccup must not skip the write
            sent = {"error": f"{type(exc).__name__}: {exc}"}
        reclassified.append({**item, "thread": str(tid), "receipt": sent})

    plan.update({
        "reclassified": reclassified,
        "note": "EXECUTED — kind='task' and a receipt attempted for every row named above.",
    })
    return plan


async def no_regrow_scheduled_tick(
    actions: Actions, *, settings: Settings | None = None, now: datetime | None = None,
) -> dict[str, Any]:
    """THE SCHEDULED LEG's own tick — `arq_worker.no_regrow_heartbeat` calls this
    unconditionally, the same thin-shim shape every other scheduled writer in this house
    uses. OFF unless `osiris_no_regrow_enabled` (dark-by-default — no explicit "ship it
    ON" instruction accompanied this dispatch, unlike obligation_hygiene_enabled/
    retention_heartbeat_enabled's own named exceptions)."""
    st = settings or get_settings()
    if not st.osiris_no_regrow_enabled:
        return {"enabled": False, "reclassified": [],
                "note": "the sweep's scheduled leg is dark (osiris_no_regrow_enabled=0)"}
    out = await apply_no_regrow(actions, actor=_SANCTIONED_NO_REGROW_ACTOR, execute=True,
                                now=now)
    return {"enabled": True, **out}
