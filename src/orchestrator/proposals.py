"""MINERS AS LAST RESORT (wave 15, operator's word verbatim, relayed by Thoth mail
8842: "the reason why miners held for so long is because they are the last resort
clean up, and if they are not treated as such they make a mess and we end up with a
worse messier graph"). Order of the graph's own self-healing: write-time laws refuse
first, mechanical sweeps (`derive_or_abstain`) derive-or-abstain second, and ONLY what
is left as a durable abstention is a miner's to look at. A miner never mints directly
again — it proposes; a mind accepts or rejects.

THE SCHEMA (decision ac892cd9, item 1 — this module is that item, one commit):
a `Proposal` is a graph object, never a raw table, same as everything else in this
kernel — `evidence_pointer` names the exact abstention it answers, `candidate` names
the object/link it would create if accepted, `confidence` is capped at the DERIVED
tier (0.4) regardless of what the caller passes (a miner's own guess is never graded
above what a mechanical sweep already earns), `owner` is resolved via the one owner
law (`resolve_owner_seat`) at write time — unresolvable refuses the whole call, never
mints an orphaned Proposal. `status` starts 'proposed'; `expires_at` is stamped once,
mint_time + 14 days.

THE LAST-RESORT LAW (Khnum mail 8849, Sekhmet mail 8857, both independently agreeing
on the identical predicate `backfill_lineage_repo_links` already runs, capture.py):
propose() refuses unless `evidence_pointer` names a `from_id`/`link_type` pair whose
CURRENT `derivation_abstained_<link_type>` property does NOT carry a `resolved` key —
a successful mint SUPERSEDES a live abstention via `supersede_assertion`, writing
`{"link_type", "resolved": True, "resolved_to": <id>}` as the new current value under
the SAME name, so a resolved abstention is a settled question, not a genuine gap. This
IS the "may only propose against an existing abstention record" law, verbatim.

INVISIBLE TO ORIENT/BACKLOG/DESK BANDS/EVERY COUNT, BY OMISSION: none of those
surfaces' own queries allowlist 'Proposal' — a caller extending any of them to a new
object type must do so explicitly, so this type never appears anywhere except the
read-only proposals band this same wave adds deliberately (item 2's own desk surface).

ITEM 2 (already landed, commit 8e44e78): accept()/reject() — accept() mints the REAL
object/link named by the Proposal's own `candidate`, verbatim, under the ACCEPTING
actor's own self_declared
testimony (never re-using the miner's derived grade), citing the Proposal back on the
minted thing itself (a link's own `properties.accepted_from`, or an object's own
`accepted_from_proposal` property — this kernel's only two provenance-carrying slots,
never a new edge type invented for this). reject() retires the Proposal with a
mandatory reason, written back where the SAME miner can read it on its next tick (the
rolling signal item 3's budget-throttle will read). Both refuse on anything but
status='proposed', and refuse an expired Proposal even before any sweep marks it so.

ITEM 3 (this pass), THE BUDGET (decision ac892cd9, Thoth mail 8920 confirming the two
window lengths): a daily budget per (miner, owner) PAIR that scales with that pair's
own trailing 30-day acceptance rate (accepted / (accepted+rejected) of resolved
Proposals in the window) — `_DAILY_BUDGET_BASE` proposals/day at a perfect record,
scaling down linearly, `_NEW_PAIR_STARTER_BUDGET` for a pair with no resolved history
yet (neither the full trust of a proven record nor the total silence of zero). A
7-day window with rejections but ZERO acceptances (Thoth's own explicit number) hard-
stops the budget to zero regardless of the 30-day rate — a pair actively producing
nothing but rejections right now doesn't get to coast on an old good record — and
fires a RECEIPT (a Thread, kind='fyi', owner=the Proposal owner) naming the rejection
count that triggered it. `open_thread`'s own idempotency-on-summary-hash is the dedup:
the receipt's summary embeds the day, so it fires at most once per (miner, owner, day)
without a second piece of state to track it.

ITEM 4 (this pass), TELEMETRY: made/accepted/rejected/expired-in-effect per (miner,
owner) lives in digest.py's own `_proposal_telemetry` (the per-project/per-seat
summary convention `fleet_digest` already holds), not here — this module carries only
`guarded_miner_tick`, the "a miner tick that raises writes a failure receipt first"
discipline (decision ac892cd9) a future miner's own periodic tick wraps itself in.
Dollar cost is deliberately NOT re-derived per pair — digest.py's own `costs` stream
already reads `ceiling()`'s measured vendor figure, and llm_usage carries no owner
dimension to split it by pair.

DELIBERATELY NOT BUILT HERE (Thoth's "one commit per item"): any miner wiring —
existing miners stay off, unwired, exactly as before this module existed."""
from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.config.settings import get_settings
from src.orchestrator.capture import open_thread
from src.orchestrator.owner_normalization import resolve_owner_seat
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_EC = EvidenceClass.DERIVED.value
_CONFIDENCE_CAP = confidence_for(EvidenceClass.DERIVED)
_EXPIRY_DAYS = 14
_LEGAL_CANDIDATE_KINDS = ("link", "object")
_TRAILING_WINDOW_DAYS = 30
# THE BUDGET KNOBS THEMSELVES moved to settings.py (osiris_miner_daily_budget_base /
# osiris_miner_new_pair_starter_budget / osiris_miner_zero_acceptance_window_days,
# THE SETTINGS MENU piece 1, thread f4498ab304e4) — registered with effect='next_tick'
# since every read below already calls get_settings() fresh, never once at import.


def _validate_candidate(candidate: dict[str, Any]) -> str | None:
    """The two legal shapes a graph write can ever be in this kernel, named plainly —
    a `link` candidate needs `from_id`/`to_id`/`link_type`, an `object` candidate needs
    `type`/`canonical`. Returns an error string, or None when the shape is legal."""
    kind = candidate.get("kind")
    if kind not in _LEGAL_CANDIDATE_KINDS:
        return (f"candidate['kind'] must be one of {_LEGAL_CANDIDATE_KINDS!r}, "
                f"got {kind!r}")
    if kind == "link":
        missing = [k for k in ("from_id", "to_id", "link_type") if not candidate.get(k)]
        if missing:
            return f"a link candidate needs {missing} (candidate={candidate!r})"
    else:
        missing = [k for k in ("type", "canonical") if not candidate.get(k)]
        if missing:
            return f"an object candidate needs {missing} (candidate={candidate!r})"
    return None


async def _live_abstention_exists(
    pool: asyncpg.Pool, from_id: uuid.UUID, link_type: str,
) -> bool:
    """The last-resort law's own precondition, verbatim per Khnum's (mail 8849) and
    Sekhmet's (mail 8857) independent agreement — the identical predicate
    `backfill_lineage_repo_links` already runs (capture.py) to find a stale abstention
    still worth retiring: a CURRENT `derivation_abstained_<link_type>` property on
    `from_id` whose value carries no `resolved` key. A row that HAS a `resolved` key
    was already answered by a real mint — proposing against it would re-litigate a
    settled question, not fill a genuine gap."""
    row = await pool.fetchval(
        "SELECT 1 FROM current_assertions WHERE object_id=$1 AND name=$2 "
        "AND NOT (value ? 'resolved') LIMIT 1",
        from_id, f"derivation_abstained_{link_type}",
    )
    return row is not None


async def _resolved_count_since(
    pool: asyncpg.Pool, miner: str, owner: str, status: str, since: datetime,
) -> int:
    """How many Proposals for this (miner, owner) pair reached `status` at or after
    `since` — the raw count `_throttle_status` combines into a rate, kept as its own
    query (never a Python-side filter of a wider fetch) so the window is the DATABASE's
    own comparison, matching this house's existing trailing-window convention
    (settle.py, digest.py: `observed_at >= $n` in the query itself)."""
    count = await pool.fetchval(
        "SELECT count(*) FROM objects o WHERE o.type='Proposal' "
        "AND EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='miner' AND a.value #>> '{}' = $1) "
        "AND EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='owner' AND a.value #>> '{}' = $2) "
        "AND (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) = $3 "
        "AND (SELECT a.observed_at FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) >= $4",
        miner, owner, status, since,
    )
    return int(count or 0)


async def _proposals_made_since(
    pool: asyncpg.Pool, miner: str, owner: str, since: datetime,
) -> int:
    """How many Proposals this (miner, owner) pair has MINTED (any status) since
    `since` — the daily budget's own spend counter. Mint time is the `miner` property's
    own `observed_at` (asserted once at propose(), never resuperseded), never the
    Proposal object's `created_at` (this kernel's own append-only convention keeps that
    off every read path — `object_events`, not a mutable column, is the only truth for
    "when")."""
    count = await pool.fetchval(
        "SELECT count(*) FROM objects o WHERE o.type='Proposal' "
        "AND EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='owner' AND a.value #>> '{}' = $2) "
        "AND (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='miner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) = $1 "
        "AND (SELECT a.observed_at FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='miner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) >= $3",
        miner, owner, since,
    )
    return int(count or 0)


async def _throttle_status(
    pool: asyncpg.Pool, miner: str, owner: str,
) -> dict[str, Any]:
    """The daily budget for this (miner, owner) pair, right now. Scales
    `osiris_miner_daily_budget_base` (settings.py — THE SETTINGS MENU piece 1) linearly
    by the pair's own trailing 30-day acceptance rate (accepted / (accepted+rejected) of
    RESOLVED Proposals in the window); a pair with no resolved history yet gets
    `osiris_miner_new_pair_starter_budget`, neither a proven record's
    full trust nor a bad one's zero. Overrides that entirely to budget=0, `throttled`
    True, when the trailing 7-day window holds at least one rejection and zero
    acceptances (Thoth's own explicit number, mail 8920) — a pair producing nothing but
    rejections right now doesn't coast on an old good rate."""
    st = get_settings()
    now = datetime.now(UTC)
    window_30 = now - timedelta(days=_TRAILING_WINDOW_DAYS)
    window_7 = now - timedelta(days=st.osiris_miner_zero_acceptance_window_days)
    accepted_30 = await _resolved_count_since(pool, miner, owner, "accepted", window_30)
    rejected_30 = await _resolved_count_since(pool, miner, owner, "rejected", window_30)
    accepted_7 = await _resolved_count_since(pool, miner, owner, "accepted", window_7)
    rejected_7 = await _resolved_count_since(pool, miner, owner, "rejected", window_7)
    total_30 = accepted_30 + rejected_30
    acceptance_rate = (accepted_30 / total_30) if total_30 else None
    if rejected_7 > 0 and accepted_7 == 0:
        return {"budget": 0, "throttled": True, "rejected_7d": rejected_7,
               "acceptance_rate": acceptance_rate}
    budget = (st.osiris_miner_new_pair_starter_budget if acceptance_rate is None
             else round(st.osiris_miner_daily_budget_base * acceptance_rate))
    return {"budget": budget, "throttled": False, "rejected_7d": rejected_7,
           "acceptance_rate": acceptance_rate}


async def propose(
    actions: Actions, *, from_id: uuid.UUID, link_type: str, candidate: dict[str, Any],
    confidence: float, owner: str, miner: str, actor: str,
) -> dict[str, Any]:
    """Mint a Proposal — never a real graph write. Refuses (nothing minted) when:
      (1) no LIVE `derivation_abstained_<link_type>` property exists on `from_id`
          (the last-resort law: a miner proposes only against an existing, unresolved
          abstention, never freehand);
      (2) `owner` does not resolve via `resolve_owner_seat` to an active Seat or the
          literal 'operator' (the one owner law, applied here exactly as it is on
          every other durable object this house mints);
      (3) `candidate` is not one of the two legal shapes (`kind`: 'link' or 'object');
      (4) the (miner, owner) pair's own daily budget is spent (item 3's throttle,
          `_throttle_status` — scales with the pair's trailing 30-day acceptance rate,
          hard-stopped to zero on a 7-day window of rejections with no acceptances,
          which also fires a receipt Thread to the owner naming why).
    `confidence` is capped at the DERIVED tier (0.4) regardless of what's passed — a
    miner's own guess is never graded above what a mechanical sweep already earns.
    Returns `{"error": ...}` on any refusal, naming which law refused it; otherwise
    the minted Proposal's own canonical, status, and expiry."""
    if not await _live_abstention_exists(actions.pool, from_id, link_type):
        return {"error": f"no live (unresolved) derivation_abstained_{link_type} "
                         f"property on {from_id} — a miner may only propose against "
                         "an existing abstention, never freehand (the last-resort law)"}
    candidate_error = _validate_candidate(candidate)
    if candidate_error is not None:
        return {"error": candidate_error}
    resolved_owner = await resolve_owner_seat(actions.pool, owner)
    if resolved_owner is None:
        return {"error": f"owner {owner!r} does not resolve to an active seat or "
                         "'operator' — a Proposal is never minted ownerless"}
    now = datetime.now(UTC)
    throttle = await _throttle_status(actions.pool, miner, resolved_owner)
    if throttle["throttled"]:
        today = now.date().isoformat()
        window_days = get_settings().osiris_miner_zero_acceptance_window_days
        await open_thread(
            actions,
            f"Miner {miner} throttled to zero proposals for {resolved_owner}: "
            f"{throttle['rejected_7d']} rejection(s) in the trailing "
            f"{window_days} days with zero acceptances (as of {today})",
            kind="fyi", owner=resolved_owner, source=miner)
        return {"error": f"{miner} is throttled to zero proposals for {resolved_owner} "
                         f"— {throttle['rejected_7d']} rejection(s) in the trailing "
                         f"{window_days} days with zero acceptances "
                         "(the last-resort budget's own hard stop); a receipt Thread "
                         "was opened for the owner"}
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    spent_today = await _proposals_made_since(actions.pool, miner, resolved_owner, day_start)
    if spent_today >= throttle["budget"]:
        return {"error": f"{miner}'s daily budget for {resolved_owner} is "
                         f"{throttle['budget']} and {spent_today} proposal(s) already "
                         "made today (trailing 30-day acceptance rate "
                         f"{throttle['acceptance_rate']!r}) — try again tomorrow"}
    capped_confidence = min(confidence, _CONFIDENCE_CAP)
    expires_at = (now + timedelta(days=_EXPIRY_DAYS)).isoformat()
    canonical = f"proposal:{uuid.uuid4()}"
    proposal_id = await actions.create_or_find_object("Proposal", canonical, actor)
    for name, value in (
        ("evidence_pointer", {"from_id": str(from_id), "link_type": link_type}),
        ("candidate", candidate),
        ("confidence", capped_confidence),
        ("owner", resolved_owner),
        ("miner", miner),
        ("status", "proposed"),
        ("expires_at", expires_at),
    ):
        await actions.assert_property(proposal_id, name, value, miner, now,
                                      _CONFIDENCE_CAP, evidence_class=_EC, actor=actor)
    return {"proposal": canonical, "owner": resolved_owner, "status": "proposed",
           "expires_at": expires_at, "confidence": capped_confidence}


async def _proposal_row(pool: asyncpg.Pool, proposal: str) -> dict[str, Any] | None:
    """The Proposal's own id plus its CURRENT status (with the status assertion's own
    row id, needed to supersede it)/candidate/owner/expires_at — read fresh every call,
    never cached, since accept()/reject() must see a status another caller just wrote.
    `status` transitions cross sources (propose()'s own miner, then a DIFFERENT actor
    accepting/rejecting) — `assert_property`'s own supersession is same-source-only, so
    a plain re-assert would leave 'proposed' AND 'accepted' simultaneously current
    (Khnum's own correct_agent_house precedent, actions/core.py's supersede_assertion
    docstring). `status_assertion_id` names the one row `supersede_assertion` must
    retire."""
    proposal_id = await pool.fetchval(
        "SELECT id FROM objects WHERE type='Proposal' AND canonical=$1", proposal)
    if proposal_id is None:
        return None
    out: dict[str, Any] = {"id": proposal_id}
    status_row = await pool.fetchrow(
        "SELECT id, value FROM current_assertions WHERE object_id=$1 AND name='status' "
        "ORDER BY confidence DESC, observed_at DESC LIMIT 1", proposal_id)
    out["status"] = status_row["value"] if status_row else None
    out["status_assertion_id"] = status_row["id"] if status_row else None
    for name in ("candidate", "owner", "expires_at"):
        out[name] = await pool.fetchval(
            "SELECT a.value FROM current_assertions a WHERE a.object_id=$1 AND a.name=$2 "
            "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1",
            proposal_id, name)
    return out


async def accept(
    actions: Actions, *, proposal: str, actor: str,
) -> dict[str, Any]:
    """Mint the REAL object/link the Proposal's own `candidate` names, verbatim, under
    `actor`'s own SELF_DECLARED testimony — never the miner's derived grade; accepting
    a proposal is a mind's act, the same evidence tier every other self-declared write
    in this house carries. Cites the Proposal back on the minted thing itself: a link
    candidate's own `properties.accepted_from`, an object candidate's own
    `accepted_from_proposal` property — the only two provenance-carrying slots this
    kernel has, never a new edge type invented for this. Refuses on anything but
    status='proposed', or a Proposal already past its own `expires_at` (checked here
    directly, never depending on a sweep having already marked it 'expired')."""
    row = await _proposal_row(actions.pool, proposal)
    if row is None:
        return {"error": f"no such Proposal: {proposal!r}"}
    if row["status"] != "proposed":
        return {"error": f"{proposal} is {row['status']!r}, not 'proposed' — nothing "
                         "to accept"}
    if datetime.fromisoformat(row["expires_at"]) <= datetime.now(UTC):
        return {"error": f"{proposal} expired at {row['expires_at']} — nothing to accept"}
    candidate = row["candidate"]
    now = datetime.now(UTC)
    conf = confidence_for(EvidenceClass.SELF_DECLARED)
    if candidate["kind"] == "link":
        from_id, to_id = uuid.UUID(candidate["from_id"]), uuid.UUID(candidate["to_id"])
        await actions.create_link(
            from_id, to_id, candidate["link_type"], actor, now, conf,
            properties={"accepted_from": proposal}, evidence_class="self_declared",
            actor=actor)
        minted = {"kind": "link", "from_id": str(from_id), "to_id": str(to_id),
                 "link_type": candidate["link_type"]}
    else:
        new_id = await actions.create_or_find_object(
            candidate["type"], candidate["canonical"], actor)
        for name, value in (candidate.get("properties") or {}).items():
            await actions.assert_property(new_id, name, value, actor, now, conf,
                                          evidence_class="self_declared", actor=actor)
        await actions.assert_property(new_id, "accepted_from_proposal", proposal, actor,
                                      now, conf, evidence_class="self_declared", actor=actor)
        minted = {"kind": "object", "type": candidate["type"],
                 "canonical": candidate["canonical"]}
    await actions.supersede_assertion(
        row["id"], "status", row["status_assertion_id"], "accepted", actor, now, conf,
        f"accepted by {actor}, minting {minted}", evidence_class="self_declared",
        actor=actor)
    await actions.assert_property(row["id"], "resolved_by", actor, actor, now, conf,
                                  evidence_class="self_declared", actor=actor)
    return {"proposal": proposal, "status": "accepted", "minted": minted}


async def reject(
    actions: Actions, *, proposal: str, reason: str, actor: str,
) -> dict[str, Any]:
    """Retire the Proposal (status='rejected') with a MANDATORY reason, written back
    where the SAME miner can read it on its own next tick — the rolling signal item 3's
    budget-throttle reads. Refuses on anything but status='proposed'."""
    if not reason.strip():
        return {"error": "reason is required — a rejection is testimony the miner "
                         "reads back, never a silent drop"}
    row = await _proposal_row(actions.pool, proposal)
    if row is None:
        return {"error": f"no such Proposal: {proposal!r}"}
    if row["status"] != "proposed":
        return {"error": f"{proposal} is {row['status']!r}, not 'proposed' — nothing "
                         "to reject"}
    now = datetime.now(UTC)
    conf = confidence_for(EvidenceClass.SELF_DECLARED)
    await actions.supersede_assertion(
        row["id"], "status", row["status_assertion_id"], "rejected", actor, now, conf,
        f"rejected by {actor}: {reason}", evidence_class="self_declared", actor=actor)
    await actions.assert_property(row["id"], "reject_reason", reason, actor, now, conf,
                                  evidence_class="self_declared", actor=actor)
    await actions.assert_property(row["id"], "resolved_by", actor, actor, now, conf,
                                  evidence_class="self_declared", actor=actor)
    return {"proposal": proposal, "status": "rejected", "reason": reason}


async def proposals_band(pool: asyncpg.Pool) -> dict[str, Any]:
    """READ-ONLY (Thoth mail 8920): the operator desk's own proposals band — total
    count of live (status='proposed', not yet expired) Proposals, and up to three per
    owner, newest first. Accept/reject happen through accept()/reject() from the
    owner's own tab, never from this band directly — this function never mutates
    anything."""
    now = datetime.now(UTC).isoformat()
    rows = await pool.fetch(
        "SELECT o.canonical AS proposal, "
        "  (SELECT a.value FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS owner, "
        "  (SELECT a.value FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='expires_at' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS expires_at, "
        "  (SELECT a.observed_at FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS proposed_at "
        "FROM objects o "
        "WHERE o.type='Proposal' AND EXISTS ("
        "  SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='status' AND a.value #>> '{}' = 'proposed')",
    )
    live = [r for r in rows if r["expires_at"] > now]
    by_owner: dict[str, list[dict[str, Any]]] = {}
    for r in sorted(live, key=lambda r: r["proposed_at"], reverse=True):
        by_owner.setdefault(r["owner"], []).append(
            {"proposal": r["proposal"], "expires_at": r["expires_at"]})
    return {"count": len(live),
           "by_owner": {owner: rows[:3] for owner, rows in by_owner.items()}}


async def guarded_miner_tick(
    actions: Actions, miner: str, fn: Callable[[], Awaitable[Any]],
) -> Any:
    """Item 4's own tick discipline (decision ac892cd9): "a miner tick that raises
    writes a failure receipt first" — before the exception is allowed to propagate or
    the tick is otherwise abandoned, so a crashed tick is a durable, queryable fact,
    never silent. Reuses `open_or_annotate_persisting_alarm` (capture.py) rather than a
    new alarm shape — the same door deploy_guard's own boot-drift alarm and
    fleet_reconcile's own blind-tick alarm already use, converging on the same live-
    desk `drift_alarms` filter for free. `fn` is a zero-arg async callable (never an
    already-created coroutine — this house's own footgun a coroutine can only be
    awaited once would otherwise invite) so a caller can retry the SAME tick through
    this guard more than once.

    No existing miner calls this yet — existing miners stay off, unwired, exactly as
    every other item in this wave — this is the primitive future miner wiring adopts."""
    from src.orchestrator.capture import open_or_annotate_persisting_alarm

    try:
        return await fn()
    except Exception as exc:
        await open_or_annotate_persisting_alarm(
            actions, f"miner {miner}'s tick raised {type(exc).__name__}",
            kind="obligation", severity="alarm", source=f"miner:{miner}")
        raise
