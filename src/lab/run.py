"""Race the frontier policies over a sweep of synthetic worlds and show the verdict.

    uv run python -m src.lab.run --worlds 300 --budget 40

Each world is generated once and run by EVERY policy (paired comparison, same seed).
Prints a metrics table and an averaged discovery curve per policy so you can watch
the shape of the race. The biological policies are stubs; the real question this
answers today is whether the current gate is leaving identity on the table that the
conventional baselines (PageRank, bandit) recover.
"""

from __future__ import annotations

import argparse
import random
import statistics
from collections.abc import Callable

from src.lab.policies import (
    BanditPolicy,
    GatePolicy,
    PageRankPolicy,
    Policy,
    RandomPolicy,
)
from src.lab.sim import Episode, run_episode
from src.lab.substrate import SynthParams, generate

_FACTORIES: dict[str, Callable[[random.Random], Policy]] = {
    "gate": lambda _r: GatePolicy(),
    "pagerank": lambda _r: PageRankPolicy(),
    "bandit": lambda r: BanditPolicy(r),
    "random": lambda r: RandomPolicy(r),
}

_BARS = " ▁▂▃▄▅▆▇█"


def _sparkline(curve: list[float], width: int, hi: float) -> str:
    if hi <= 0:
        return " " * width
    out = []
    for i in range(width):
        v = curve[i] if i < len(curve) else (curve[-1] if curve else 0.0)
        out.append(_BARS[min(len(_BARS) - 1, int((v / hi) * (len(_BARS) - 1)))])
    return "".join(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--worlds", type=int, default=300)
    ap.add_argument("--budget", type=int, default=40)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--policies", default="gate,pagerank,bandit,random")
    args = ap.parse_args()

    params = SynthParams()
    names = [n.strip() for n in args.policies.split(",")]
    rng = random.Random(args.seed)
    world_seeds = [rng.randrange(1 << 30) for _ in range(args.worlds)]

    results: dict[str, list[Episode]] = {n: [] for n in names}
    avg_curve: dict[str, list[float]] = {n: [0.0] * args.budget for n in names}
    n_core_mean = 0.0

    for ws in world_seeds:
        sub = generate(params, random.Random(ws))
        n_core_mean += sub.n_core
        for n in names:
            pol = _FACTORIES[n](random.Random(ws ^ 0x9E3779B9))
            ep = run_episode(sub, pol, args.budget)
            results[n].append(ep)
            for i in range(args.budget):
                v = ep.curve[i] if i < len(ep.curve) else (ep.curve[-1] if ep.curve else 0.0)
                avg_curve[n][i] += v / args.worlds
    n_core_mean /= args.worlds

    print(f"\n  {args.worlds} worlds · budget {args.budget} probes · "
          f"~{n_core_mean:.0f} core nodes/world  (params: {params})\n")
    hdr = f"  {'policy':<10} {'recall':>8} {'noise%':>8} {'probes':>8} {'stall%':>8}"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    order = sorted(names, key=lambda n: statistics.mean(e.recall for e in results[n]), reverse=True)
    for n in order:
        eps = results[n]
        recall = statistics.mean(e.recall for e in eps)
        noise = statistics.mean(e.noise_ratio for e in eps)
        probes = statistics.mean(e.probes for e in eps)
        stall = statistics.mean(1.0 if e.stalled else 0.0 for e in eps)
        print(f"  {n:<10} {recall:>7.1%} {noise:>7.1%} {probes:>8.1f} {stall:>7.1%}")

    print("\n  discovery curve (core found vs probes spent, averaged):\n")
    for n in order:
        print(f"  {n:<10} |{_sparkline(avg_curve[n], args.budget, n_core_mean)}| "
              f"{avg_curve[n][-1]:.1f}/{n_core_mean:.0f}")
    print()


if __name__ == "__main__":
    main()
