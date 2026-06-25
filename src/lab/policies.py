"""Frontier policies — all behind one interface so the comparison is fair.

A policy sees the *discovered* subgraph (what's been revealed so far, with each
node's inbound edges = its evidence classes + distinct sources) and a remaining
probe budget, and returns a ranked list of unexpanded nodes to spend the next
probe on. That's the entire decision the cascade's frontier makes — here it's
swappable so we can race the current gate against conventional baselines and,
eventually, the biological couplings.

  * GatePolicy      — current Osiris: never expand a speculative-only node. Local.
  * PageRankPolicy  — evidence-weighted personalized PageRank. The non-local baseline.
  * BanditPolicy    — Thompson sampling over evidence-class arms. The naked control
                      that *learns* the gate's rule from data (and explores past it).
  * RandomPolicy    — floor.
  * Chemotaxis / Stigmergy / Physarum — the coupling hypotheses, left as stubs.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Protocol

from src.parsers.base import EvidenceClass
from src.parsers.evidence import (
    confidence_for,
    is_anchor_grade,
    is_speculative,
    strength,
)


@dataclass
class Disc:
    """A discovered node, as the policy sees it."""

    id: str
    type: str
    hop: int
    expanded: bool = False
    inbound: list[tuple[EvidenceClass, str]] = field(default_factory=list)

    def strongest(self) -> EvidenceClass | None:
        return max((c for c, _ in self.inbound), key=strength, default=None)

    def n_sources(self) -> int:
        return len({s for _, s in self.inbound})

    def expandable(self) -> bool:
        """The gate's rule: a real (non-speculative) reason to exist, or >=2 sources
        one of which is non-speculative."""
        classes = [c for c, _ in self.inbound]
        if any(is_anchor_grade(c) for c in classes):
            return True
        if self.n_sources() >= 2 and any(not is_speculative(c) for c in classes):
            return True
        return False


@dataclass
class SimState:
    seed: str
    discovered: dict[str, Disc]

    def candidates(self) -> list[Disc]:
        return [d for d in self.discovered.values() if not d.expanded and d.id != self.seed]


class Policy(Protocol):
    name: str

    def rank(self, state: SimState) -> list[str]: ...

    def update(self, node_id: str, was_core: bool, state: SimState) -> None: ...


class _Base:
    name = "base"

    def update(self, node_id: str, was_core: bool, state: SimState) -> None:
        return None


class GatePolicy(_Base):
    name = "gate"

    def rank(self, state: SimState) -> list[str]:
        ok = [d for d in state.candidates() if d.expandable()]
        # anchors first, then by best confidence, then shallower hop
        ok.sort(key=lambda d: (_conf(d), -d.hop), reverse=True)
        return [d.id for d in ok]


class RandomPolicy(_Base):
    name = "random"

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng

    def rank(self, state: SimState) -> list[str]:
        ids = [d.id for d in state.candidates()]
        self.rng.shuffle(ids)
        return ids


class PageRankPolicy(_Base):
    name = "pagerank"

    def __init__(self, damping: float = 0.85, iters: int = 30) -> None:
        self.damping = damping
        self.iters = iters

    def rank(self, state: SimState) -> list[str]:
        nodes = [state.seed, *(d.id for d in state.discovered.values() if d.id != state.seed)]
        out: dict[str, list[tuple[str, float]]] = {n: [] for n in nodes}
        for d in state.discovered.values():
            for cls, src in d.inbound:
                if src in out:
                    out[src].append((d.id, confidence_for(cls)))
        n = len(nodes)
        pr = {x: 1.0 / n for x in nodes}
        for _ in range(self.iters):
            nxt = {x: (1.0 - self.damping) * (1.0 if x == state.seed else 0.0) for x in nodes}
            leaked = 0.0
            for x in nodes:
                edges = out[x]
                if not edges:
                    leaked += self.damping * pr[x]
                    continue
                tot = sum(w for _, w in edges)
                for dst, w in edges:
                    nxt[dst] += self.damping * pr[x] * (w / tot)
            nxt[state.seed] += leaked  # dangling mass teleports home
            pr = nxt
        cands = state.candidates()
        cands.sort(key=lambda d: pr.get(d.id, 0.0), reverse=True)
        return [d.id for d in cands]


class BanditPolicy(_Base):
    name = "bandit"

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng
        self.a: dict[EvidenceClass, float] = {}
        self.b: dict[EvidenceClass, float] = {}

    def _arm(self, d: Disc) -> EvidenceClass | None:
        return d.strongest()

    def rank(self, state: SimState) -> list[str]:
        scored: list[tuple[float, str]] = []
        for d in state.candidates():
            arm = self._arm(d)
            a = self.a.get(arm, 1.0) if arm is not None else 1.0
            b = self.b.get(arm, 1.0) if arm is not None else 1.0
            theta = self.rng.betavariate(a, b)
            scored.append((theta, d.id))
        scored.sort(reverse=True)
        return [i for _, i in scored]

    def update(self, node_id: str, was_core: bool, state: SimState) -> None:
        d = state.discovered.get(node_id)
        arm = self._arm(d) if d is not None else None
        if arm is None:
            return
        if was_core:
            self.a[arm] = self.a.get(arm, 1.0) + 1.0
        else:
            self.b[arm] = self.b.get(arm, 1.0) + 1.0


# --- coupling hypotheses (to fill once the baseline harness is proven) ----------


class _Stub(_Base):
    def rank(self, state: SimState) -> list[str]:
        raise NotImplementedError(f"{self.name} coupling not implemented yet")


class ChemotaxisPolicy(_Stub):
    """Run-and-tumble: bandit whose exploration rate is coupled to the DERIVATIVE of
    recent yield — exploit the cluster while probes pay off, spike exploration (jump
    to a structurally distant frontier region) when the last k probes came back noise.
    Coupling to test: explore_rate = g(d(yield)/d(probe)). Baseline to beat: BanditPolicy."""

    name = "chemotaxis"


class StigmergyPolicy(_Stub):
    """Ant foraging: lay decaying 'pheromone' on lead *feature-types* (evidence_class x
    node_type) that produced verified core; bias future probes toward high-scent types.
    A contextual bandit with evaporation — gives the crawl MEMORY of what pays off."""

    name = "stigmergy"


class PhysarumPolicy(_Stub):
    """Slime-mold transport: flux-reinforced conductance with decay over the discovered
    graph (consolidation, not exploration). Expand by arriving flux. Expected to help
    only in the dynamic/redundant regime; provably ~shortest-path otherwise."""

    name = "physarum"


def _conf(d: Disc) -> float:
    c = d.strongest()
    return confidence_for(c) if c is not None else 0.0
