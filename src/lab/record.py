"""Record a REAL substrate by crawling a live seed through the actual connectors +
parsers — gate OFF, expand everything by type up to a cap — so the policy race runs
against reality, not a synthetic world. Validity rule honored: graph growth uses the
exact prod parsers and evidence-class assignment; only the *frontier decision* is
removed (we expand the full reachable closure to record the superset every policy
then chooses subsets of). Responses are cached to disk so re-recording is offline.

    uv run python -m src.lab.record asuramaya Username

Writes fixtures/substrate/<seed>/{substrate.json, nodes.txt, cache.json}. Hand-label
the core identity in fixtures/substrate/<seed>/labels.yaml, then race with
`run.py --fixture …`.
"""

from __future__ import annotations

import asyncio
import json
import sys
import uuid
from pathlib import Path
from typing import Any

from src.connectors.registry import CONNECTORS
from src.lab.substrate import Reveal, Substrate, dump_substrate
from src.orchestrator.manifests import load_manifests
from src.parsers import get_parser
from src.parsers.base import EvidenceClass, InputObject

_HELPERS = Path(__file__).resolve().parent.parent.parent / "helpers"
_OUT = Path(__file__).resolve().parent.parent.parent / "fixtures" / "substrate"


async def _call(conn: Any, hid: str, inp: InputObject, cache: dict[str, Any]) -> Any:
    key = f"{hid}|{inp.canonical}"
    if key in cache:
        return cache[key]
    try:
        resp = await conn(inp)
    except Exception as exc:  # a dead source (searxng down, 403, timeout) just yields nothing
        resp = {"__error__": f"{type(exc).__name__}: {exc}"[:160]}
    cache[key] = resp
    return resp


async def record(seed_canon: str, seed_type: str, max_hop: int, max_nodes: int) -> None:
    manifests = load_manifests(_HELPERS)
    by_type: dict[str, list[Any]] = {}
    for m in manifests.values():
        by_type.setdefault(m.consumes.type, []).append(m)

    out = _OUT / seed_canon.replace(":", "_").replace("/", "_")
    out.mkdir(parents=True, exist_ok=True)
    cache_path = out / "cache.json"
    cache: dict[str, Any] = json.loads(cache_path.read_text()) if cache_path.exists() else {}

    reveal: dict[str, list[Reveal]] = {}
    types: dict[str, str] = {seed_canon: seed_type}
    queue: list[tuple[str, str, int]] = [(seed_canon, seed_type, 0)]
    expanded: set[str] = set()

    while queue and len(types) < max_nodes:
        canon, typ, hop = queue.pop(0)
        if canon in expanded or hop > max_hop:
            continue
        expanded.add(canon)
        inp = InputObject(id=str(uuid.uuid4()), type=typ, canonical=canon, properties={})
        for m in by_type.get(typ, []):
            conn = CONNECTORS.get(m.id)
            if conn is None:
                continue
            resp = await _call(conn, m.id, inp, cache)
            if not isinstance(resp, dict) or "__error__" in resp:
                continue
            try:
                result = get_parser(m.parser)(resp, inp)
            except Exception:
                continue
            cls_for: dict[str, EvidenceClass] = {
                lk.to_ref.ref: lk.evidence_class
                for lk in result.links
                if lk.to_ref.ref is not None and lk.evidence_class is not None
            }
            for spec in result.objects:
                if spec.canonical == canon:
                    continue
                types.setdefault(spec.canonical, spec.type)
                ec = cls_for.get(spec.canonical, EvidenceClass.DIRECT_OBSERVATION)
                reveal.setdefault(canon, []).append(
                    Reveal(spec.canonical, spec.type, False, ec, m.id)
                )
                if hop + 1 <= max_hop:
                    queue.append((spec.canonical, spec.type, hop + 1))
        print(f"  expanded {canon:<45} ({typ})  nodes={len(types)}", file=sys.stderr)

    sub = Substrate(seed_canon, seed_type, reveal, frozenset())
    cache_path.write_text(json.dumps(cache))
    dump_substrate(sub, types, out / "substrate.json")
    nodes = "\n".join(f"{t:<12} {c}" for c, t in sorted(types.items(), key=lambda kv: kv[1]))
    (out / "nodes.txt").write_text(nodes + "\n")
    print(f"\nrecorded {len(types)} nodes, {sum(len(v) for v in reveal.values())} edges -> {out}")
    print(f"label the real identity in {out / 'labels.yaml'} (core: [...]) then run --fixture")


def main() -> None:
    seed = sys.argv[1] if len(sys.argv) > 1 else "asuramaya"
    seed_type = sys.argv[2] if len(sys.argv) > 2 else "Username"
    max_hop = int(sys.argv[3]) if len(sys.argv) > 3 else 3
    asyncio.run(record(seed, seed_type, max_hop, max_nodes=400))


if __name__ == "__main__":
    main()
