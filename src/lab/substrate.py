"""The world the policies compete to harvest.

A substrate is a *latent* graph: the policy can't see a node until it spends a
probe expanding the node's parent, which reveals the children and the
evidence_class of the edge that revealed them. Ground truth: each node is `core`
(real identity) or noise.

The synthetic generator embeds a core tree (mostly strong edges, a tunable
fraction revealed *weakly* — co-occurrence — and then optionally corroborated by a
second strong edge from another branch) inside a self-expanding noise field
(speculative edges whose children are always noise and yield only more noise). The
two phenomena that make the experiment non-trivial:
  * noise explodes (probing a stranger reveals the stranger's footprint, not yours);
  * some real identity is reachable only by taking a chance on a weak-looking lead
    ("locked" core) — the exact bet a strict gate refuses to make.
A recorded-from-prod substrate (later) implements the same `expand()` surface.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

from src.parsers.base import EvidenceClass

_STRONG = (EvidenceClass.AUTHORITATIVE_API, EvidenceClass.SELF_DECLARED)
_SPEC = (EvidenceClass.CO_OCCURRENCE, EvidenceClass.DERIVED)


@dataclass(frozen=True)
class Reveal:
    """One edge uncovered by expanding a node: who it points to, and how we know."""

    dst: str
    dst_type: str
    core: bool
    evidence_class: EvidenceClass
    source: str  # the node that revealed it — distinct sources => corroboration


@dataclass
class Substrate:
    seed: str
    seed_type: str
    reveal: dict[str, list[Reveal]]
    core_ids: frozenset[str]

    def expand(self, node_id: str) -> list[Reveal]:
        return self.reveal.get(node_id, [])

    @property
    def n_core(self) -> int:
        return len(self.core_ids)


@dataclass
class SynthParams:
    core_depth: int = 4          # depth of the true-identity tree
    core_branch: int = 2         # children per core node
    weak_core_frac: float = 0.35  # core first revealed via a speculative edge
    corroborate_frac: float = 0.6  # of weak core, how much gets a 2nd strong source
    noise_rate: float = 0.7      # prob a node spawns a noise subtree
    noise_branch: int = 3
    noise_depth: int = 3
    max_nodes: int = 600


def generate(p: SynthParams, rng: random.Random) -> Substrate:
    reveal: dict[str, list[Reveal]] = {}
    core_ids: set[str] = set()
    strong_core: list[str] = []  # already-strong core that can corroborate weak nodes
    counter = {"n": 0}

    def nid(prefix: str) -> str:
        counter["n"] += 1
        return f"{prefix}{counter['n']}"

    def add(src: str, r: Reveal) -> None:
        reveal.setdefault(src, []).append(r)

    def grow_noise(parent: str, depth: int) -> None:
        if depth <= 0 or counter["n"] > p.max_nodes:
            return
        for _ in range(rng.randint(0, p.noise_branch)):
            n = nid("n")
            add(parent, Reveal(n, "Account", False, rng.choice(_SPEC), parent))
            grow_noise(n, depth - 1)

    def grow_core(parent: str, depth: int) -> None:
        if depth <= 0 or counter["n"] > p.max_nodes:
            return
        for _ in range(p.core_branch):
            c = nid("c")
            core_ids.add(c)
            weak = rng.random() < p.weak_core_frac
            if not weak:
                add(parent, Reveal(c, "Account", True, rng.choice(_STRONG), parent))
                strong_core.append(c)
            else:
                # revealed weakly; maybe a second branch corroborates it later
                add(parent, Reveal(c, "Account", True, rng.choice(_SPEC), parent))
                if strong_core and rng.random() < p.corroborate_frac:
                    q = rng.choice(strong_core)
                    add(q, Reveal(c, "Account", True, rng.choice(_STRONG), q))
                # else: "locked" — weak forever; only a risk-taking policy reaches its subtree
            if rng.random() < p.noise_rate:
                grow_noise(c, p.noise_depth)
            grow_core(c, depth - 1)

    grow_core("seed", p.core_depth)
    grow_noise("seed", p.noise_depth)
    return Substrate("seed", "Username", reveal, frozenset(core_ids))
