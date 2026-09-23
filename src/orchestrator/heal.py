"""Heals ACTLESS HUSK generations left behind by a 2026-07-14 onboarding defect.

A HUSK is a generation the machinery minted at a handoff boundary that no agent ever
occupied: born of a `minted_because` stamp, with zero acts beyond its own mint
bookkeeping, nothing sent, nothing settled. The automatic debounce retires this class
when it catches one inside its window; the eight healed here escaped because the
round-trip straddled two separate boundary observers racing each other. That race
condition was fixed at the source in the same change that ships this module.

THIS HEAL APPLIES THE SAME LOGIC THE DEBOUNCE USES, LATE: compensating events only
(false_mint + retired, constitution 3, never DELETE), unread mail re-addressed to the
agent that actually holds the seat, and mount rows re-pointed the same way. Naming an
identity and merging one are decisions reserved for a human operator (constitution 1),
but these events carry only INHERITED handles stamped by mint_heir's own logic, the
machine correcting its own bookkeeping, and this batch was explicitly approved before
running.

EVERY CANDIDATE IS RE-VERIFIED AT HEAL TIME rather than trusted from the original
diagnosis: an agent that has acted since diagnosis is refused, with the reason
recorded. A resemblance to a husk is not treated as proof of one.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from src.actions.core import Actions
from src.orchestrator.agents import _generation, agent_has_acted
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_SRC = "husk-heal"
_DO = EvidenceClass.DIRECT_OBSERVATION
_CONF = confidence_for(_DO)

# The eight husks identified on 2026-07-14; the default batch this script rehearses.
HUSKS_2026_07_14 = [
    "agent:628ef839-vi", "agent:628ef839-vii", "agent:628ef839-viii", "agent:628ef839-ix",
    "agent:c9b710cb-vi", "agent:ad1a1cb0-xxx", "agent:7118bf41-v", "agent:7118bf41-vi",
]


async def _prop(actions: Actions, oid: uuid.UUID, name: str) -> str | None:
    return await actions.pool.fetchval(  # type: ignore[no-any-return]
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name=$2 "
        "ORDER BY confidence DESC, observed_at DESC LIMIT 1", oid, name)


async def _oid(actions: Actions, canonical: str) -> uuid.UUID | None:
    return await actions.pool.fetchval(  # type: ignore[no-any-return]
        "SELECT id FROM objects WHERE canonical=$1 AND type='Agent' AND status='active'",
        canonical)


async def _chain(actions: Actions, member: str) -> list[str]:
    """The lineage as an ordered list, walked by succeeded_by from the base root. The base is
    the generation-1 canonical of `member`; a chain that never reaches `member` (a healed
    unwind cut it) still returns what the walk saw."""
    base = _generation(member)[0]
    out = [base]
    seen = {base}
    cur = base
    for _ in range(64):
        oid = await _oid(actions, cur)
        nxt = await _prop(actions, oid, "succeeded_by") if oid else None
        if not nxt or nxt in seen:
            break
        out.append(nxt)
        seen.add(nxt)
        cur = nxt
    return out


async def _verify_husk(actions: Actions, canonical: str) -> tuple[uuid.UUID | None, str]:
    """(oid, '') when `canonical` is verifiably a husk NOW; (None, why) when refused. The
    original diagnosis is re-derived at heal time: an agent that has acted since is a
    live occupant, not a husk."""
    oid = await _oid(actions, canonical)
    if oid is None:
        return None, "no such active Agent"
    if await _prop(actions, oid, "false_mint") == "true":
        return None, "already healed (false_mint stands)"
    because = await _prop(actions, oid, "minted_because")
    if not because:
        return None, "not a machine mint (no minted_because): healing it would be a judgement"
    ancestor = await _prop(actions, oid, "succeeded_from")
    anc_oid = await _oid(actions, ancestor) if ancestor else None
    exclude = [oid] + ([anc_oid] if anc_oid else [])
    # Mail read-state is INHERITED at mint time (mint_heir copies the ancestor's recipient
    # rows, so the heir already appears to have read them). Only a settle event AFTER its
    # own creation counts as the heir's own act. An earlier rehearsal run with
    # settled_after=None incorrectly refused all eight husks based on their ancestors'
    # read history; this was caught only by running the rehearsal, which is the purpose
    # of the dry-run step.
    minted_at = await actions.pool.fetchval(
        "SELECT observed_at FROM current_assertions WHERE object_id=$1 "
        "AND name='succeeded_from' ORDER BY confidence DESC, observed_at DESC LIMIT 1", oid)
    if minted_at is None:
        minted_at = await actions.pool.fetchval(
            "SELECT created_at FROM objects WHERE id=$1", oid)
    if await agent_has_acted(actions, canonical, exclude=exclude, settled_after=minted_at):
        return None, "it ACTED, an agent lived here however briefly; not ours to erase"
    return oid, ""


async def heal_husks(
    actions: Actions, husks: list[str], *, apply: bool = False,
) -> dict[str, Any]:
    """Rehearses the heal by default, or applies it when `apply=True`. Returns the full
    plan either way: what was verified, what was refused and why, where each husk's
    records are reassigned, and which chain tails get unwound. Idempotent: a husk that
    has already been healed re-runs as a refusal."""
    now = datetime.now(UTC)
    verified: dict[str, uuid.UUID] = {}
    refused: dict[str, str] = {}
    for h in dict.fromkeys(husks):
        oid, why = await _verify_husk(actions, h)
        if oid is None:
            refused[h] = why
        else:
            verified[h] = oid

    plan: list[dict[str, Any]] = []
    unwinds: list[dict[str, str]] = []
    for base in dict.fromkeys(_generation(h)[0] for h in verified):
        chain = await _chain(actions, base)
        husk_set = {c for c in chain if c in verified}
        real = [c for c in chain if c not in husk_set]
        for h in (c for c in chain if c in husk_set):
            i = chain.index(h)
            successor = next((c for c in chain[i + 1:] if c in real), None)
            ancestor = next((c for c in reversed(chain[:i]) if c in real), None)
            target = successor or ancestor
            plan.append({"husk": h, "estate_to": target,
                         "note": "unread mail + mount rows follow the target"})
        # TAIL UNWIND: when every generation after the last genuine agent is a husk, a
        # lookup for the current lineage head would still land on a retired husk
        # (lineage_head reads succeeded_by, not false_mint). Clearing the last genuine
        # agent's forward pointer restores it as the head, using the same
        # compensating-event pattern the debounce writes, never a delete.
        if real:
            last_real = real[-1]
            j = chain.index(last_real)
            if j < len(chain) - 1 and all(c in husk_set for c in chain[j + 1:]):
                unwinds.append({"lineage": base, "head_restored": last_real})

    out: dict[str, Any] = {
        "verified": sorted(verified), "refused": refused, "plan": plan, "unwinds": unwinds,
        "applied": False,
    }
    if not apply:
        out["note"] = "REHEARSAL: nothing written; pass apply=True to heal"
        return out

    for step in plan:
        h, oid = step["husk"], verified[step["husk"]]
        for k, v in (("false_mint", "true"), ("retired", "true"), ("retired_by", _SRC),
                     ("false_mint_because",
                      "actless machine mint at an onboarding boundary: the two boundary "
                      "observers raced each other and both fired; healed at the operator's "
                      "word, 2026-07-14")):
            await actions.assert_property(oid, k, v, _SRC, now, _CONF,
                                          evidence_class=_DO.value)
        if step["estate_to"]:
            await actions.pool.execute(
                "UPDATE fleet_messages SET to_agent=$1 WHERE to_agent=$2 AND read_at IS NULL",
                step["estate_to"], h)
            await actions.pool.execute(
                "UPDATE agent_mounts SET agent_id=$1 WHERE agent_id=$2", step["estate_to"], h)
    for u in unwinds:
        head_oid = await _oid(actions, u["head_restored"])
        if head_oid is not None:
            await actions.assert_property(head_oid, "succeeded_by", "", _SRC, now, _CONF,
                                          evidence_class=_DO.value)
    out["applied"] = True
    return out
