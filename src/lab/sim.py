"""The gym: replay a substrate under a policy, score the trajectory.

One episode = grow the discovered graph from the seed under a fixed probe budget,
letting the policy choose which node to expand each step. The seed is expanded for
free (it's the operator's chosen anchor). Metrics are scored against the
substrate's ground-truth `core_ids`.
"""

from __future__ import annotations

from dataclasses import dataclass

from src.lab.policies import Disc, Policy, SimState
from src.lab.substrate import Substrate
from src.parsers.base import EvidenceClass


@dataclass
class Episode:
    curve: list[int]          # cumulative core discovered after each probe
    n_core: int
    core_found: int
    probes: int
    noise_expansions: int
    total_expansions: int
    stalled: bool             # frontier emptied before budget (the gate's deadlock)

    @property
    def recall(self) -> float:
        return self.core_found / self.n_core if self.n_core else 1.0

    @property
    def noise_ratio(self) -> float:
        return self.noise_expansions / self.total_expansions if self.total_expansions else 0.0


def run_episode(sub: Substrate, policy: Policy, budget: int) -> Episode:
    discovered: dict[str, Disc] = {}
    state = SimState(seed=sub.seed, discovered=discovered)

    def discover(parent_hop: int, dst: str, dst_type: str, ec: EvidenceClass, src: str) -> None:
        d = discovered.get(dst)
        if d is None:
            d = Disc(id=dst, type=dst_type, hop=parent_hop + 1)
            discovered[dst] = d
        d.inbound.append((ec, src))

    # seed is expanded for free
    discovered[sub.seed] = Disc(id=sub.seed, type=sub.seed_type, hop=0, expanded=True)
    for r in sub.expand(sub.seed):
        discover(0, r.dst, r.dst_type, r.evidence_class, r.source)

    def core_found() -> int:
        return sum(1 for nid in discovered if nid in sub.core_ids)

    curve: list[int] = []
    noise_exp = total_exp = 0
    stalled = False
    for _ in range(budget):
        ranked = [nid for nid in policy.rank(state)
                  if nid in discovered and not discovered[nid].expanded]
        if not ranked:
            stalled = True
            break
        nid = ranked[0]
        d = discovered[nid]
        d.expanded = True
        is_core = nid in sub.core_ids
        total_exp += 1
        noise_exp += 0 if is_core else 1
        for r in sub.expand(nid):
            discover(d.hop, r.dst, r.dst_type, r.evidence_class, r.source)
        policy.update(nid, is_core, state)
        curve.append(core_found())

    return Episode(
        curve=curve, n_core=sub.n_core, core_found=core_found(),
        probes=total_exp, noise_expansions=noise_exp, total_expansions=total_exp,
        stalled=stalled,
    )
