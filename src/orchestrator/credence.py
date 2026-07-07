"""Credence — the upstream dual of the authority chain (rulings 637a7213 / f3959906 / 108ff2e8).

Authority attenuates going DOWN the spawned_by tree — a child can't wield more than it was
granted. Credence attenuates going UP it — a re-report can't carry more confidence than its
source. The SAME tree that bounds delegation is, read upward, the INDEPENDENCE ORACLE: two agents
asserting the same fact are either dependent (one is the other's ancestor — a relay, whose
confidence must CLAMP to the origin) or independent (different subtrees — genuine corroboration
that may stand). Naive grade-then-recency can't tell them apart, so a swarm could manufacture
confidence by echo (spawn N children, feed them one leaf's guess, harvest N "independent"
agreements = citogenesis).

The resolution, per fact, keyed on STRUCTURE not value (so a paraphrased relay dissolves — you
never compare the reworded string):

  * CLAMP    — an ANCESTOR that only re-reported (pure hearsay: it never looked) has its
               confidence for that fact capped at what its subtree actually established; it can't
               win on an inflated grade. Processed DEEPEST-FIRST, so an inner relay's inflation
               can't leak past its own clamp on the way up.
  * REBUTTAL — an ancestor that performed its OWN observation act (`backed_by_observation`, the
               Tier-1 floor of the act-detection ladder, captured in lineage.py) is NOT clamped:
               it may have verified, and verification is corroboration, not relay. Conservative —
               we only clamp agents that provably never looked, so a genuine double-check is never
               deflated (the failure the ladder exists to avoid).
  * FLAG     — a pure-hearsay ancestor carrying a fact ABOVE its origin's grade is LAUNDERING
               ("claimed you looked, only heard") — surfaced, never silently (membrane, rule #6).

A pure function (`resolve_credence`, trivially unit-testable) over a thin IO layer
(`credence_props`, fetching current_assertions + the spawned_by forest). A fact with NO ancestry
among its sources resolves IDENTICALLY to winning_props (grade DESC, recency DESC) — this is a
strict refinement that bites ONLY when a relay would otherwise out-rank its origin.

SCOPE (v1): the clamp acts among AGENT sources (`agent:*`) related by spawned_by. session-miner —
the universal re-teller that today swallows a sub-agent's origin identity — is not yet in the
tree, so its extractions aren't clamped here; routing miner extractions back to their originating
agent (so this subsumes the miner over-read, f34c572c) is the tracked feeder that makes it bite
at scale.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from src.actions.core import Actions


@dataclass(frozen=True)
class Claim:
    """One asserter's latest word on a fact — the pure resolver's input row."""

    object_id: str
    name: str
    value: Any
    source_id: str
    confidence: float
    observed_at: datetime


@dataclass(frozen=True)
class CredenceWinner:
    """The lineage-resolved winner for one (object, name). `confidence` is EFFECTIVE (post-clamp);
    `laundering` names the pure-hearsay ancestors that carried the fact above its origin grade."""

    object_id: str
    name: str
    value: Any
    confidence: float
    source_id: str
    clamped: bool
    laundering: tuple[str, ...]


def _is_agent(source_id: str) -> bool:
    """Only agent sources sit in the spawned_by tree; everything else resolves by raw grade."""
    return source_id.startswith("agent:")


def _ancestors(src: str, parent_of: Mapping[str, str]) -> set[str]:
    """Every source reachable UP the spawned_by chain from `src` (cycle-guarded — a spawn tree is
    acyclic, but a corrupt edge must not loop the resolver)."""
    seen: set[str] = set()
    cur = parent_of.get(src)
    while cur is not None and cur not in seen:
        seen.add(cur)
        cur = parent_of.get(cur)
    return seen


def _depth(src: str, parent_of: Mapping[str, str]) -> int:
    return len(_ancestors(src, parent_of))


def resolve_credence(
    claims: Sequence[Claim],
    parent_of: Mapping[str, str],
    looked: Mapping[str, bool],
) -> list[CredenceWinner]:
    """Resolve each (object, name) to its lineage-aware winner. `parent_of` is the spawned_by
    child→parent map; `looked` is backed_by_observation per agent source (default False = the
    conservative "never looked", which is the only state we clamp)."""
    groups: dict[tuple[str, str], list[Claim]] = {}
    for c in claims:
        groups.setdefault((c.object_id, c.name), []).append(c)

    winners: list[CredenceWinner] = []
    for (oid, name), group in groups.items():
        # Deepest-first: a node's descendants are scored before it, so its cap reads their
        # ALREADY-CLAMPED effective confidence — an inner relay's inflation can't leak upward.
        order = sorted(group, key=lambda r: _depth(r.source_id, parent_of), reverse=True)
        eff_by_src: dict[str, float] = {}
        laundering: list[str] = []
        scored: list[tuple[float, bool, Claim]] = []
        for r in order:
            eff = r.confidence
            clamped = False
            if _is_agent(r.source_id) and not looked.get(r.source_id, False):
                descs = [
                    d for d in group
                    if d.source_id != r.source_id and _is_agent(d.source_id)
                    and r.source_id in _ancestors(d.source_id, parent_of)
                ]
                if descs:  # r is an ancestor-relay of its asserting subtree
                    origin_conf = max(eff_by_src.get(d.source_id, d.confidence) for d in descs)
                    if r.confidence > origin_conf:  # inflated past what the subtree established
                        eff = origin_conf
                        clamped = True
                        laundering.append(r.source_id)
            eff_by_src[r.source_id] = eff
            scored.append((eff, clamped, r))

        # Winner: highest effective confidence; a tie prefers the un-clamped origin over a relay,
        # then the more recent. (Reduces to winning_props' grade-then-recency with no ancestry.)
        eff, _, claim = max(
            scored, key=lambda s: (s[0], not s[1], s[2].observed_at)
        )
        winners.append(CredenceWinner(
            object_id=oid, name=name, value=claim.value, confidence=eff,
            source_id=claim.source_id, clamped=any(s[1] for s in scored),
            laundering=tuple(laundering),
        ))
    return winners


async def _parent_forest(actions: Actions) -> dict[str, str]:
    """The whole spawned_by child→parent map (canonical→canonical). The delegation forest is
    small (one node per sub-agent), so we load it whole rather than walk per lookup."""
    rows = await actions.pool.fetch(
        "SELECT c.canonical AS child, p.canonical AS parent "
        "FROM links l JOIN objects c ON c.id = l.from_id JOIN objects p ON p.id = l.to_id "
        "WHERE l.type = 'spawned_by'")
    return {r["child"]: r["parent"] for r in rows}


async def _looked_map(actions: Actions, srcs: set[str]) -> dict[str, bool]:
    """backed_by_observation per agent source (the Tier-1 rebuttal signal, captured in lineage)."""
    if not srcs:
        return {}
    rows = await actions.pool.fetch(
        "SELECT o.canonical AS src, ca.value AS looked FROM objects o "
        "JOIN current_assertions ca ON ca.object_id = o.id AND ca.name = 'backed_by_observation' "
        "WHERE o.type = 'Agent' AND o.canonical = ANY($1::text[])", list(srcs))
    return {r["src"]: bool(r["looked"]) for r in rows}


async def credence_props(actions: Actions, oids: Sequence[Any]) -> list[CredenceWinner]:
    """The lineage-aware winner per (object, name) over `oids` — winning_props with the upstream
    credence discipline layered on. Fetches the latest per-source assertions, the spawned_by
    forest, and the observation-rebuttal signal, then resolves purely."""
    rows = await actions.pool.fetch(
        "SELECT object_id, name, value, source_id, confidence, observed_at "
        "FROM current_assertions WHERE object_id = ANY($1::uuid[])", list(oids))
    claims = [
        Claim(str(r["object_id"]), r["name"], r["value"], r["source_id"],
              float(r["confidence"]), r["observed_at"])
        for r in rows
    ]
    agent_srcs = {c.source_id for c in claims if _is_agent(c.source_id)}
    parent_of = await _parent_forest(actions) if agent_srcs else {}
    looked = await _looked_map(actions, agent_srcs)
    return resolve_credence(claims, parent_of, looked)
