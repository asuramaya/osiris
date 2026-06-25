"""Race the frontier policies over a sweep of synthetic worlds and show the verdict.

    uv run python -m src.lab.run --worlds 300 --budget 40
    uv run python -m src.lab.run --sweep corroborate_frac=0,0.3,0.6,1.0

Each world is generated once and run by EVERY policy (paired, same seed). With
--sweep, one knob of the world is varied and the table reprinted per value, so you
can watch the regime boundaries move. The biological couplings (chemotaxis,
stigmergy) are now live and in the race; the only question that matters is whether
either claims the recall x low-noise corner the naked bandit doesn't.
"""

from __future__ import annotations

import argparse
import dataclasses
import random
import statistics
from collections.abc import Callable

from src.lab.policies import (
    BanditPolicy,
    ChemotaxisPolicy,
    GatePolicy,
    PageRankPolicy,
    Policy,
    RandomPolicy,
    StigmergyPolicy,
)
from src.lab.sim import Episode, run_episode
from src.lab.substrate import SynthParams, generate

_FACTORIES: dict[str, Callable[[random.Random], Policy]] = {
    "gate": lambda _r: GatePolicy(),
    "pagerank": lambda _r: PageRankPolicy(),
    "bandit": lambda r: BanditPolicy(r),
    "chemotaxis": lambda r: ChemotaxisPolicy(r),
    "stigmergy": lambda r: StigmergyPolicy(r),
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


def race(
    params: SynthParams, names: list[str], worlds: int, budget: int, seed: int
) -> tuple[dict[str, list[Episode]], dict[str, list[float]], float]:
    rng = random.Random(seed)
    world_seeds = [rng.randrange(1 << 30) for _ in range(worlds)]
    results: dict[str, list[Episode]] = {n: [] for n in names}
    avg_curve: dict[str, list[float]] = {n: [0.0] * budget for n in names}
    n_core_mean = 0.0
    for ws in world_seeds:
        sub = generate(params, random.Random(ws))
        n_core_mean += sub.n_core / worlds
        for n in names:
            ep = run_episode(sub, _FACTORIES[n](random.Random(ws ^ 0x9E3779B9)), budget)
            results[n].append(ep)
            for i in range(budget):
                v = ep.curve[i] if i < len(ep.curve) else (ep.curve[-1] if ep.curve else 0.0)
                avg_curve[n][i] += v / worlds
    return results, avg_curve, n_core_mean


def _summarize(results: dict[str, list[Episode]], names: list[str]) -> list[str]:
    order = sorted(names, key=lambda n: statistics.mean(e.recall for e in results[n]), reverse=True)
    lines = [f"  {'policy':<11}{'recall':>8}{'noise%':>8}{'probes':>8}{'stall%':>8}"]
    lines.append("  " + "-" * 43)
    for n in order:
        eps = results[n]
        lines.append(
            f"  {n:<11}{statistics.mean(e.recall for e in eps):>7.1%} "
            f"{statistics.mean(e.noise_ratio for e in eps):>7.1%} "
            f"{statistics.mean(e.probes for e in eps):>7.1f} "
            f"{statistics.mean(1.0 if e.stalled else 0.0 for e in eps):>7.1%}"
        )
    return lines


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--worlds", type=int, default=300)
    ap.add_argument("--budget", type=int, default=40)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--policies", default="gate,pagerank,bandit,chemotaxis,stigmergy,random")
    ap.add_argument("--sweep", default="", help="KNOB=v1,v2,... e.g. noise_rate=0.3,0.6,0.9")
    args = ap.parse_args()
    names = [n.strip() for n in args.policies.split(",")]

    if args.sweep:
        knob, raw = args.sweep.split("=", 1)
        cast = type(getattr(SynthParams(), knob))  # match the field's int/float type
        values = [cast(float(v)) for v in raw.split(",")]
        print(f"\n  sweep {knob} over {values}  ({args.worlds} worlds · budget {args.budget})\n")
        for val in values:
            params = dataclasses.replace(SynthParams(), **{knob: val})
            results, _, n_core = race(params, names, args.worlds, args.budget, args.seed)
            print(f"  ── {knob}={val}  (~{n_core:.0f} core/world) ──")
            for ln in _summarize(results, names):
                print(ln)
            print()
        return

    params = SynthParams()
    results, avg_curve, n_core = race(params, names, args.worlds, args.budget, args.seed)
    print(f"\n  {args.worlds} worlds · budget {args.budget} probes · ~{n_core:.0f} core/world\n")
    for ln in _summarize(results, names):
        print(ln)
    print("\n  discovery curve (core found vs probes spent, averaged):\n")
    order = sorted(names, key=lambda n: avg_curve[n][-1], reverse=True)
    for n in order:
        print(f"  {n:<11}|{_sparkline(avg_curve[n], args.budget, n_core)}| "
              f"{avg_curve[n][-1]:.1f}/{n_core:.0f}")
    print()


if __name__ == "__main__":
    main()
