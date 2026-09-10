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
object type must do so explicitly, so this type simply never appears until someone
deliberately wires it in (the read-only proposals BAND is a LATER item, not this one).

ACCEPT/REJECT (item 2, telemetry item 4, the daily budget half of item 3) are
DELIBERATELY NOT built here — Thoth's own "one commit per item" sequencing. Existing
miners stay off, unwired, exactly as before this module existed."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.orchestrator.owner_normalization import resolve_owner_seat
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_EC = EvidenceClass.DERIVED.value
_CONFIDENCE_CAP = confidence_for(EvidenceClass.DERIVED)
_EXPIRY_DAYS = 14
_LEGAL_CANDIDATE_KINDS = ("link", "object")


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
      (3) `candidate` is not one of the two legal shapes (`kind`: 'link' or 'object').
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
