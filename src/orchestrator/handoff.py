"""Human-in-the-loop handoff tray + suspend/resume state machine (DESIGN §9).

A gated dispatch (or an open helper that hits a challenge) suspends to a handoff:
the run parks in helper_runs.status='awaiting_human' and a handoff row carries the
browser payload. The analyst's tray batches these; opening one moves it to
in_browser; posting back the scraped result runs the helper's parser and finishes
the run (downstream triggers then cascade normally); abandoning releases it.

State is authoritative on helper_runs.status; handoffs.resolved_at marks closure.
No business logic here beyond the transitions — the parser does the interpreting.
"""

from __future__ import annotations

import uuid
from typing import Any

from src.actions.core import Actions
from src.orchestrator.budgets import BudgetLedger
from src.orchestrator.manifests import Manifest
from src.orchestrator.runner import claim_run, execute_claimed, load_input_object

Json = dict[str, Any]


class HandoffError(Exception):
    pass


async def _hop(actions: Actions, case_id: uuid.UUID, object_id: uuid.UUID) -> int:
    hop = await actions.pool.fetchval(
        "SELECT hop_distance FROM case_objects WHERE case_id=$1 AND object_id=$2",
        case_id,
        object_id,
    )
    return int(hop) if hop is not None else 0


async def suspend(
    actions: Actions,
    ledger: BudgetLedger,
    manifest: Manifest,
    object_id: uuid.UUID,
    case_id: uuid.UUID,
    *,
    url: str | None,
    challenge_kind: str | None,
    partial_state: Json | None = None,
    cookies: Json | None = None,
    existing_run_id: uuid.UUID | None = None,
) -> int | None:
    """Park a run for the analyst. Returns the handoff id, or None if the human-
    handoff budget is exhausted (or the run is already active elsewhere)."""
    if not await ledger.reserve_handoff_credit(case_id):
        return None

    run_id = existing_run_id
    if run_id is None:
        run_id = await claim_run(
            actions, manifest.id, object_id, case_id, manifest.tier, status="awaiting_human"
        )
        if run_id is None:  # already active for this (helper, object, case)
            await ledger.refund_handoff_credit(case_id)
            return None
    else:
        await actions.pool.execute(
            "UPDATE helper_runs SET status='awaiting_human' WHERE id=$1", run_id
        )

    # Priority is a heuristic: objects closer to the seed tend to block more
    # downstream work (true fan-out isn't knowable before the run — see §9).
    priority = float(-(await _hop(actions, case_id, object_id)))
    handoff_id: int = await actions.pool.fetchval(
        "INSERT INTO handoffs (helper_run_id, helper_id, object_id, case_id, origin, url, "
        " challenge_kind, partial_state, cookies_snapshot, priority, expires_at) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, now() + interval '24 hours') RETURNING id",
        run_id,
        manifest.id,
        object_id,
        case_id,
        manifest.origin,
        url,
        challenge_kind,
        partial_state or {},
        cookies,
        priority,
    )
    return handoff_id


async def tray(
    actions: Actions, *, case_id: uuid.UUID | None = None, include_expired: bool = False
) -> list[Json]:
    """The batched tray: unresolved handoffs whose run still awaits a human,
    ordered by priority (most-blocking first) then age."""
    q = (
        "SELECT h.id, h.helper_id, h.object_id, h.case_id, h.url, h.challenge_kind, "
        "       h.priority, h.created_at, h.expires_at, r.status "
        "FROM handoffs h JOIN helper_runs r ON r.id = h.helper_run_id "
        "WHERE h.resolved_at IS NULL AND r.status = 'awaiting_human' "
    )
    args: list[Any] = []
    if case_id is not None:
        args.append(case_id)
        q += f"AND h.case_id = ${len(args)} "
    if not include_expired:
        q += "AND (h.expires_at IS NULL OR h.expires_at > now()) "
    q += "ORDER BY h.priority DESC, h.created_at"
    return [dict(r) for r in await actions.pool.fetch(q, *args)]


async def open_handoff(actions: Actions, handoff_id: int) -> None:
    """Analyst opened the card in their browser."""
    run_id = await actions.pool.fetchval(
        "SELECT helper_run_id FROM handoffs WHERE id=$1 AND resolved_at IS NULL", handoff_id
    )
    if run_id is None:
        raise HandoffError(f"handoff {handoff_id} not found or already resolved")
    await actions.pool.execute(
        "UPDATE helper_runs SET status='in_browser' WHERE id=$1", run_id
    )


async def post_back(
    actions: Actions, manifest: Manifest, handoff_id: int, response: Json
) -> dict[str, int]:
    """Analyst posted back the scraped result. Runs the helper's parser on it,
    finishes the run (done), and resolves the handoff so downstream cascades."""
    row = await actions.pool.fetchrow(
        "SELECT helper_run_id, object_id, case_id, resolved_at FROM handoffs WHERE id=$1",
        handoff_id,
    )
    if row is None or row["resolved_at"] is not None:
        raise HandoffError(f"handoff {handoff_id} not found or already resolved")
    run_id, object_id, case_id = row["helper_run_id"], row["object_id"], row["case_id"]

    await actions.pool.execute(
        "UPDATE helper_runs SET status='result_posted_back' WHERE id=$1", run_id
    )
    input_object = await load_input_object(actions.pool, object_id)
    input_hop = await _hop(actions, case_id, object_id)
    counts = await execute_claimed(
        actions, manifest, response, input_object, case_id, run_id, input_hop=input_hop
    )
    await actions.pool.execute("UPDATE handoffs SET resolved_at=now() WHERE id=$1", handoff_id)
    return counts


async def abandon(actions: Actions, handoff_id: int) -> None:
    """Analyst skipped it — release the claim, mark the run abandoned."""
    run_id = await actions.pool.fetchval(
        "SELECT helper_run_id FROM handoffs WHERE id=$1 AND resolved_at IS NULL", handoff_id
    )
    if run_id is None:
        raise HandoffError(f"handoff {handoff_id} not found or already resolved")
    await actions.pool.execute(
        "UPDATE helper_runs SET status='abandoned', finished_at=now() WHERE id=$1", run_id
    )
    await actions.pool.execute("UPDATE handoffs SET resolved_at=now() WHERE id=$1", handoff_id)
