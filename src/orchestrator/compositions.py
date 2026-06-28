"""Compositions — the composer's primitive: a saved, forkable spec over the graph.

The front end was never a page; it's the *composer* — the place where intent becomes a
composition over neutral primitives. A composition is a small op-tree the substrate
executes. It unifies the *watch* (a saved subscription) and the *lens* (a saved query)
into ONE first-class object, so opinion lives in the composition the USER owns — not
welded into engine code. Claude authors them from a sentence (the MCP tools); the
substrate runs them; the views render them.

Ops (neutral, composable — the equivalent of Notion's filter/relation/rollup):
  {"op":"subject"}                                 -> the object you're looking at
  {"op":"select","object_type":?,"where":[...]}    -> objects matching conditions
  {"op":"traverse","from":N,"direction":,"hops":}  -> objects N hops away (neighbourhood)
  {"op":"collect","from":N,"properties":[],"transform":?} -> the values of those props
  {"op":"subtract","left":N,"right":N}             -> values in left not in right

The old `discrepancy` read-model is just one composition (opinion left the engine):
  subtract( collect(location, country) over traverse(subject, 2 hops),
            collect(home-props, country) over subject )
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any

import asyncpg

from src.orchestrator.discrepancy import _HOME_PROPS, country_of
from src.orchestrator.monitor import match_condition

# Named pure transforms a `collect` op may apply to a value. Kept tiny and neutral —
# `country` is the only domain helper, shared with the (soon-vestigial) discrepancy code.
_TRANSFORMS: dict[str, Any] = {
    "identity": lambda v: v,
    "country": country_of,
    "lower": lambda v: v.lower() if isinstance(v, str) else v,
}


@dataclass
class Result:
    """A composition's output — either an object set or a value list."""

    kind: str  # "objects" | "values"
    objects: list[uuid.UUID] = field(default_factory=list)
    values: list[str] = field(default_factory=list)


def _coerce(v: Any) -> Any:
    return json.loads(v) if isinstance(v, str) else v


async def _props(pool: asyncpg.Pool, oid: uuid.UUID) -> dict[str, str]:
    rows = await pool.fetch(
        "SELECT DISTINCT ON (name) name, value #>> '{}' AS v FROM current_assertions "
        "WHERE object_id=$1 ORDER BY name, observed_at DESC",
        oid,
    )
    return {r["name"]: r["v"] for r in rows}


def _distinct(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


async def _eval(pool: asyncpg.Pool, node: dict[str, Any], subject: uuid.UUID | None) -> Result:
    op = node.get("op")

    if op == "subject":
        return Result("objects", objects=[subject] if subject else [])

    if op == "select":
        ot = node.get("object_type")
        where = node.get("where", []) or []
        rows = await pool.fetch(
            "SELECT id FROM objects WHERE status='active' AND ($1::text IS NULL OR type=$1)", ot
        )
        out: list[uuid.UUID] = []
        for r in rows:
            facts = await _props(pool, r["id"])
            if all(match_condition(facts.get(c.get("property")), c.get("op", "contains"),
                                   c.get("value")) for c in where):
                out.append(r["id"])
        return Result("objects", objects=out)

    if op == "traverse":
        base = await _eval(pool, node["from"], subject)
        seeds = base.objects
        direction = node.get("direction", "both")
        hops = int(node.get("hops", 1))
        ltype = node.get("link_type")
        seen, frontier = set(seeds), set(seeds)
        for _ in range(hops):
            if not frontier:
                break
            ids = list(frontier)
            clause = {
                "out": "from_id = ANY($1::uuid[])",
                "in": "to_id = ANY($1::uuid[])",
            }.get(direction, "(from_id = ANY($1::uuid[]) OR to_id = ANY($1::uuid[]))")
            rows = await pool.fetch(
                "SELECT CASE WHEN from_id = ANY($1::uuid[]) THEN to_id ELSE from_id END AS n "
                f"FROM links WHERE {clause} AND ($2::text IS NULL OR type=$2)",
                ids, ltype,
            )
            nxt = {r["n"] for r in rows} - seen
            seen |= nxt
            frontier = nxt
        # the neighbourhood is everything reached EXCEPT the seeds themselves
        return Result("objects", objects=[i for i in seen if i not in set(seeds)])

    if op == "collect":
        base = await _eval(pool, node["from"], subject)
        props = node.get("properties", []) or []
        transform = _TRANSFORMS.get(node.get("transform", "identity"), _TRANSFORMS["identity"])
        vals: list[str] = []
        for oid in base.objects:
            facts = await _props(pool, oid)
            for p in props:
                v = facts.get(p)
                if v is None:
                    continue
                t = transform(v)
                if t:
                    vals.append(t)
        return Result("values", values=_distinct(vals))

    if op == "subtract":
        left = await _eval(pool, node["left"], subject)
        right = await _eval(pool, node["right"], subject)
        rset = set(right.values)
        return Result("values", values=[v for v in left.values if v not in rset])

    raise ValueError(f"unknown composition op: {op!r}")


# --- persistence + run ------------------------------------------------------

async def save_composition(
    pool: asyncpg.Pool, name: str, spec: dict[str, Any], kind: str = "lens"
) -> uuid.UUID:
    """Save (or update) a composition by name. Fork = save under a new name."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "INSERT INTO compositions (name, kind, spec) VALUES ($1,$2,$3) "
        "ON CONFLICT (name) DO UPDATE SET spec=EXCLUDED.spec, kind=EXCLUDED.kind RETURNING id",
        name, kind, spec,
    )


async def _spec_of(pool: asyncpg.Pool, ref: str) -> dict[str, Any] | None:
    row = await pool.fetchrow(
        "SELECT spec FROM compositions WHERE name=$1 OR id::text=$1", ref
    )
    return _coerce(row["spec"]) if row else None


async def list_compositions(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    return [
        {"id": str(r["id"]), "name": r["name"], "kind": r["kind"], "spec": _coerce(r["spec"])}
        for r in await pool.fetch(
            "SELECT id, name, kind, spec FROM compositions ORDER BY created_at"
        )
    ]


async def _label(pool: asyncpg.Pool, oid: uuid.UUID) -> dict[str, str]:
    r = await pool.fetchrow(
        "SELECT o.type, o.canonical, (SELECT value #>> '{}' FROM current_assertions a "
        "WHERE a.object_id=o.id AND a.name='name' LIMIT 1) AS name FROM objects o WHERE o.id=$1",
        oid,
    )
    if r is None:
        return {"id": str(oid), "label": str(oid), "type": "?"}
    return {"id": str(oid), "label": r["name"] or r["canonical"], "type": r["type"]}


async def run_composition(
    pool: asyncpg.Pool, ref: str, subject: uuid.UUID | None = None
) -> dict[str, Any]:
    """Execute a saved composition (by name or id), optionally against a subject."""
    spec = await _spec_of(pool, ref)
    if spec is None:
        return {"error": f"no composition {ref!r}"}
    res = await _eval(pool, spec, subject)
    items: list[Any] = (
        [await _label(pool, oid) for oid in res.objects]
        if res.kind == "objects" else res.values
    )
    return {"composition": ref, "kind": res.kind, "count": len(items), "items": items}


# --- default compositions (templates — the engine's opinions, now forkable) --
# `operational-vs-disclosed-geography` IS the former `discrepancy` read-model, expressed
# as a composition: opinion has left the engine — it's a named, forkable spec the user
# owns. (Single-subject; the cluster-following nuance of discrepancy.py is left aside.)
GEOGRAPHY_DISCREPANCY: dict[str, Any] = {
    "op": "subtract",
    "left": {"op": "collect", "transform": "country", "properties": ["location"],
             "from": {"op": "traverse", "from": {"op": "subject"},
                      "direction": "both", "hops": 2}},
    "right": {"op": "collect", "transform": "country", "properties": list(_HOME_PROPS),
              "from": {"op": "subject"}},
}
DEFAULT_COMPOSITIONS: dict[str, dict[str, Any]] = {
    "operational-vs-disclosed-geography": GEOGRAPHY_DISCREPANCY,
}


async def seed_default_compositions(pool: asyncpg.Pool) -> int:
    for name, spec in DEFAULT_COMPOSITIONS.items():
        await save_composition(pool, name, spec, "lens")
    return len(DEFAULT_COMPOSITIONS)
