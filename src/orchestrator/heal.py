"""Healing ACTLESS HUSK generations — the onboarding clusterfuck's remains (2026-07-14).

A HUSK is a generation the machinery minted at a seam that no mind ever inhabited: born of a
`minted_because` stamp, zero acts beyond its own mint bookkeeping, nothing sent, nothing
settled. The seam debounce retires this class automatically when it catches one inside its
window; the eight healed here escaped because the round-trip straddled the two seam observers
(the ping-pong, thread a3d49d91) — cured at the source in the same change that ships this.

THE HEAL IS THE DEBOUNCE'S, APPLIED LATE: compensating events only (false_mint + retired,
constitution 3 — never DELETE), unread mail re-addressed to the mind that actually holds the
seat, mount rows re-pointed the same way. A name is testimony and an identity merge is the
operator's (constitution 1) — but these carry only INHERITED handles stamped by mint_heir's
own hand, the machine un-doing the machine, and the operator greenlit this batch explicitly.

EVERY CANDIDATE IS RE-VERIFIED AT HEAL TIME, never trusted from the ticket: an agent that
acted since diagnosis is refused with its reason. A resemblance must not speak in the voice
of a fact.
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

# the eight of 2026-07-14 (ruling f7a715a1) — the default batch the script rehearses
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
    ticket's diagnosis is re-derived at heal time — an agent that acted since is a mind."""
    oid = await _oid(actions, canonical)
    if oid is None:
        return None, "no such active Agent"
    if await _prop(actions, oid, "false_mint") == "true":
        return None, "already healed (false_mint stands)"
    because = await _prop(actions, oid, "minted_because")
    if not because:
        return None, "not a machine mint (no minted_because) — healing it would be a judgement"
    ancestor = await _prop(actions, oid, "succeeded_from")
    anc_oid = await _oid(actions, ancestor) if ancestor else None
    exclude = [oid] + ([anc_oid] if anc_oid else [])
    if await agent_has_acted(actions, canonical, exclude=exclude, settled_after=None):
        return None, "it ACTED — a mind lived here, however briefly; not ours to erase"
    return oid, ""


async def heal_husks(
    actions: Actions, husks: list[str], *, apply: bool = False,
) -> dict[str, Any]:
    """Rehearse (default) or apply the heal. Returns the full plan either way: what was
    verified, what was refused and why, where each husk's estate goes, and which chain tails
    get unwound. Idempotent — a healed husk re-runs as a refusal."""
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
        # THE TAIL UNWIND: when every generation past the last real mind is a husk, the head-
        # walk would still land on a corpse (lineage_head reads succeeded_by, not false_mint).
        # Clearing the last real mind's forward pointer restores it as the head — the same
        # compensating stamp the debounce writes, never a delete.
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
        out["note"] = "REHEARSAL — nothing written; pass apply=True to heal"
        return out

    for step in plan:
        h, oid = step["husk"], verified[step["husk"]]
        for k, v in (("false_mint", "true"), ("retired", "true"), ("retired_by", _SRC),
                     ("false_mint_because",
                      "actless machine mint at an onboarding seam — the two seam observers "
                      "ping-ponged (thread a3d49d91); healed at the operator's word, "
                      "2026-07-14")):
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
