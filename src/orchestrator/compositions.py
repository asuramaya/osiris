"""Compositions — the composer's primitive: a saved, forkable spec over the graph.

The front end was never a page; it's the *composer* — the place where intent becomes a
composition over neutral primitives. A composition is a small op-tree the substrate
executes. It unifies the *watch* (a saved subscription) and the *lens* (a saved query)
into ONE first-class object, so opinion lives in the composition the USER owns — not
welded into engine code. Claude authors them from a sentence (the MCP tools); the
substrate runs them; the views render them.

The op set is small and CLOSED — grounded in Palantir's Object Set API + Notion's
rollups, which independently land on the same vocabulary (see docs/COMPOSER.md). Anything
the ops can't express is a Function (a named transform), never a new op.

Ops (neutral, composable — the equivalent of Notion's filter/relation/rollup):
  {"op":"subject"}                                 -> the object you're looking at
  {"op":"select","object_type":?,"where":[...]}    -> objects matching conditions (.filter)
  {"op":"traverse","from":N,"direction":,"hops":}  -> objects N hops away (.searchAround)
  {"op":"collect","from":N,"properties":[],"transform":?} -> the values of those props
  {"op":"subtract","left":N,"right":N}             -> values in left not in right (.subtract)
  {"op":"union","sets":[N,...]}                     -> combine sets (.union)
  {"op":"intersect","sets":[N,...]}                 -> objects/values in ALL sets (.intersect)
  {"op":"aggregate","from":N,"group_by":[],"metric":{...}} -> group + a metric (.groupBy / rollup)
  {"op":"order","from":N,"by":?,"dir":}            -> rank a set/rows (.orderBy)
  {"op":"take","from":N,"n":K}                      -> top-N (.take)
  {"op":"function","name":,"args":{}}              -> a registered Function (the escape hatch)

The old `discrepancy` read-model is just one composition (opinion left the engine):
  subtract( collect(location, country) over traverse(subject, 2 hops),
            collect(home-props, country) over subject )

There is deliberately NO generic `join` — relating two sets is `intersect` (set algebra)
or `traverse` (a link), and fuzzy matching (screening) is a Function. Caps (Palantir's,
load-tested): `traverse` ≤ 3 hops, `aggregate` ≤ 3 group_by dimensions.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

import asyncpg

from src.ontology.resolution import screen_network
from src.orchestrator.coinvest import coinvestment_ties
from src.orchestrator.discrepancy import _HOME_PROPS, country_of
from src.orchestrator.frontier import subject_report
from src.orchestrator.monitor import match_condition

# Named pure transforms a `collect` op may apply to a value. Kept tiny and neutral —
# `country` is the only domain helper, shared with the (soon-vestigial) discrepancy code.
_TRANSFORMS: dict[str, Any] = {
    "identity": lambda v: v,
    "country": country_of,
    "lower": lambda v: v.lower() if isinstance(v, str) else v,
}

# Functions — the escape hatch (Palantir's exact split: a small closed op set + arbitrary
# registered logic for anything the ops can't express). A read-model whose precision lives
# in domain logic — merge-aware cluster resolution, a platform-degree filter, multi-signal
# fuzzy matching — is a FUNCTION, not a worse pure-op rewrite. Registering it here lets a
# forkable composition REFERENCE it ({"op":"function","name":...}), so the opinion leaves
# engine code and becomes a named, listable, swappable artifact the user owns — without
# losing a drop of the analytics. The subject passed to `run_composition` is the function's
# anchor (an entity for coinvest/screen; a case for subject_report).
Function = Callable[[asyncpg.Pool, uuid.UUID, dict[str, Any]], Awaitable[Any]]


async def _fn_coinvest(pool: asyncpg.Pool, subject: uuid.UUID, args: dict[str, Any]) -> Any:
    return await coinvestment_ties(
        pool, subject,
        limit=int(args.get("limit", 25)), platform_degree=int(args.get("platform_degree", 12)),
    )


async def _fn_subject_report(
    pool: asyncpg.Pool, subject: uuid.UUID, args: dict[str, Any]
) -> Any:
    return await subject_report(pool, subject)  # `subject` is the case id here


async def _fn_screen(pool: asyncpg.Pool, subject: uuid.UUID, args: dict[str, Any]) -> Any:
    return await screen_network(pool, subject, min_len=int(args.get("min_len", 5)))


_FUNCTIONS: dict[str, Function] = {
    "coinvest": _fn_coinvest,
    "subject_report": _fn_subject_report,
    "screen_network": _fn_screen,
}


def list_functions() -> list[str]:
    """The registered Functions a composition may reference (the authoring channel reads
    this to know what's beyond the closed op set)."""
    return sorted(_FUNCTIONS)


# Guardrails adopted from Palantir's Object Set API (load-tested, not arbitrary).
MAX_TRAVERSE_HOPS = 3
MAX_AGGREGATE_DIMS = 3


@dataclass
class Result:
    """A composition's output — an object set, a value list, or aggregate rows."""

    kind: str  # "objects" | "values" | "rows" | "data"
    objects: list[uuid.UUID] = field(default_factory=list)
    values: list[str] = field(default_factory=list)
    rows: list[dict[str, Any]] = field(default_factory=list)
    data: Any = None  # a Function's native output (list/dict) — opaque to the ops


def _coerce(v: Any) -> Any:
    return json.loads(v) if isinstance(v, str) else v


def _num(v: Any) -> float | None:
    """Best-effort numeric coercion for ordering/aggregation; None if not a number."""
    if isinstance(v, bool):  # bool is an int subclass — exclude it
        return None
    if isinstance(v, int | float):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v.replace(",", "").strip())
        except ValueError:
            return None
    return None


async def _props(pool: asyncpg.Pool, oid: uuid.UUID) -> dict[str, str]:
    rows = await pool.fetch(
        "SELECT DISTINCT ON (name) name, value #>> '{}' AS v FROM current_assertions "
        "WHERE object_id=$1 ORDER BY name, observed_at DESC",
        oid,
    )
    return {r["name"]: r["v"] for r in rows}


def _distinct[T](values: list[T]) -> list[T]:
    seen: set[T] = set()
    out: list[T] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _setop[T](op: str, lists: list[list[T]]) -> list[T]:
    """union (concat+dedup) or intersect, preserving the first set's order."""
    if not lists:
        return []
    if op == "intersect":
        common = set(lists[0]).intersection(*(set(x) for x in lists[1:]))
        return [x for x in _distinct(lists[0]) if x in common]
    return _distinct([x for lst in lists for x in lst])


async def _eval(pool: asyncpg.Pool, node: dict[str, Any], subject: uuid.UUID | None) -> Result:
    op = node.get("op")

    if op == "subject":
        return Result("objects", objects=[subject] if subject else [])

    if op == "select":
        ot = node.get("object_type")
        cp = node.get("canonical_prefix")
        where = node.get("where", []) or []
        rows = await pool.fetch(
            "SELECT id FROM objects WHERE status='active' AND ($1::text IS NULL OR type=$1) "
            "AND ($2::text IS NULL OR canonical LIKE $2 || '%')", ot, cp
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
        hops = min(int(node.get("hops", 1)), MAX_TRAVERSE_HOPS)
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

    if op in ("union", "intersect"):
        sets = [await _eval(pool, s, subject) for s in node.get("sets", [])]
        if not sets:
            return Result("objects")
        kind = sets[0].kind
        if any(s.kind != kind for s in sets) or kind == "rows":
            raise ValueError(f"{op} requires same-kind object/value sets")
        if kind == "objects":
            return Result("objects", objects=_setop(op, [s.objects for s in sets]))
        return Result("values", values=_setop(op, [s.values for s in sets]))

    if op == "aggregate":
        base = await _eval(pool, node["from"], subject)
        group_by = node.get("group_by", []) or []
        if len(group_by) > MAX_AGGREGATE_DIMS:
            raise ValueError(f"aggregate supports ≤{MAX_AGGREGATE_DIMS} group_by dimensions")
        metric = node.get("metric", {"type": "count"}) or {"type": "count"}
        return Result("rows", rows=await _aggregate(pool, base.objects, group_by, metric))

    if op == "order":
        base = await _eval(pool, node["from"], subject)
        return await _order(pool, base, node.get("by"), node.get("dir", "asc"))

    if op == "take":
        base = await _eval(pool, node["from"], subject)
        n = max(0, int(node.get("n", 0)))
        return Result(base.kind, objects=base.objects[:n], values=base.values[:n],
                      rows=base.rows[:n], data=base.data)

    if op == "function":
        name = str(node.get("name", ""))
        fn = _FUNCTIONS.get(name)
        if fn is None:
            raise ValueError(f"unknown function: {name!r}")
        if subject is None:
            raise ValueError(f"function {name!r} requires a subject")
        return Result("data", data=await fn(pool, subject, node.get("args", {}) or {}))

    raise ValueError(f"unknown composition op: {op!r}")


async def _aggregate(
    pool: asyncpg.Pool, objects: list[uuid.UUID], group_by: list[str], metric: dict[str, Any]
) -> list[dict[str, Any]]:
    """Group objects by property values, compute one metric per group (Palantir groupBy /
    Notion rollup). group_by=[] aggregates the whole set into a single row."""
    mtype = metric.get("type", "count")
    field_name = metric.get("field")
    groups: dict[tuple[str | None, ...], list[dict[str, str]]] = {}
    for oid in objects:
        facts = await _props(pool, oid)
        key = tuple(facts.get(g) for g in group_by)
        groups.setdefault(key, []).append(facts)
    rows: list[dict[str, Any]] = []
    for key, members in groups.items():
        group = {g: k for g, k in zip(group_by, key, strict=True)}
        if mtype == "count":
            value: float | int = len(members)
        else:
            raw = [m.get(field_name) for m in members] if field_name else []
            if mtype == "cardinality":
                value = len({v for v in raw if v is not None})
            else:
                nums = [n for n in (_num(v) for v in raw) if n is not None]
                value = _metric_over(mtype, nums)
        rows.append({"group": group, "metric": value})
    return rows


def _metric_over(mtype: str, nums: list[float]) -> float:
    if not nums:
        return 0.0
    if mtype == "sum":
        return sum(nums)
    if mtype == "avg":
        return sum(nums) / len(nums)
    if mtype == "min":
        return min(nums)
    if mtype == "max":
        return max(nums)
    raise ValueError(f"unknown aggregate metric: {mtype!r}")


async def _order(
    pool: asyncpg.Pool, base: Result, by: str | None, direction: str
) -> Result:
    rev = direction == "desc"
    if base.kind == "rows":
        def rkey(r: dict[str, Any]) -> tuple[float, str]:
            v = r["metric"] if by in (None, "metric") else r.get("group", {}).get(by)
            n = _num(v)
            return (n if n is not None else 0.0, str(v))
        return Result("rows", rows=sorted(base.rows, key=rkey, reverse=rev))
    if base.kind == "values":
        def vkey(v: str) -> tuple[float, str]:
            n = _num(v)
            return (n if n is not None else float("inf"), v)
        return Result("values", values=sorted(base.values, key=vkey, reverse=rev))
    # objects — order by a property (numeric if possible, else lexical)
    keyed: list[tuple[float, str, uuid.UUID]] = []
    for oid in base.objects:
        raw = (await _props(pool, oid)).get(by) if by else None
        n = _num(raw)
        keyed.append((n if n is not None else float("inf"), str(raw or ""), oid))
    keyed.sort(key=lambda t: (t[0], t[1]), reverse=rev)
    return Result("objects", objects=[t[2] for t in keyed])


# --- persistence + run ------------------------------------------------------

async def save_composition(
    pool: asyncpg.Pool, name: str, spec: dict[str, Any], kind: str = "lens",
    *, webhook_url: str | None = None, active: bool = True, room_id: uuid.UUID | None = None,
) -> uuid.UUID:
    """Save (or update) a composition by name. Fork = save under a new name. `webhook_url`
    and `active` are a watch's execution metadata (a lens ignores them). `room_id` scopes it
    to a stance (NULL = unassigned; a re-save without a room keeps the existing one)."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "INSERT INTO compositions (name, kind, spec, webhook_url, active, room_id) "
        "VALUES ($1,$2,$3,$4,$5,$6) "
        "ON CONFLICT (name) DO UPDATE SET spec=EXCLUDED.spec, kind=EXCLUDED.kind, "
        "  webhook_url=EXCLUDED.webhook_url, active=EXCLUDED.active, "
        "  room_id=COALESCE(EXCLUDED.room_id, compositions.room_id) RETURNING id",
        name, kind, spec, webhook_url, active, room_id,
    )


async def save_watch(
    pool: asyncpg.Pool, name: str, object_type: str | None, where: list[dict[str, Any]],
    *, canonical_prefix: str | None = None, webhook_url: str | None = None,
    room_id: uuid.UUID | None = None,
) -> uuid.UUID:
    """Save a WATCH — a composition whose spec is a `select` op. The same spec runs as a
    lens (current members) and drives the evaluator (alert on a new member). One primitive."""
    spec: dict[str, Any] = {"op": "select", "object_type": object_type, "where": where}
    if canonical_prefix:
        spec["canonical_prefix"] = canonical_prefix
    return await save_composition(pool, name, spec, "watch", webhook_url=webhook_url,
                                  room_id=room_id)


async def _spec_of(pool: asyncpg.Pool, ref: str) -> dict[str, Any] | None:
    row = await pool.fetchrow(
        "SELECT spec FROM compositions WHERE name=$1 OR id::text=$1", ref
    )
    return _coerce(row["spec"]) if row else None


# --- rooms: the stance (a Room is a composition of compositions — authorable by Claude) --

async def create_room(
    pool: asyncpg.Pool, name: str, config: dict[str, Any] | None = None
) -> uuid.UUID:
    """Create (or update) a Room — a saved stance the operator switches between. The FDE
    move: Claude mints one from a sentence, then assigns compositions to it."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "INSERT INTO rooms (name, config) VALUES ($1,$2) "
        "ON CONFLICT (name) DO UPDATE SET config=EXCLUDED.config RETURNING id",
        name, config or {},
    )


async def resolve_room(pool: asyncpg.Pool, ref: str | None) -> uuid.UUID | None:
    """A room by name or id (None ref → None = the All/unassigned scope)."""
    if not ref:
        return None
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT id FROM rooms WHERE name=$1 OR id::text=$1", ref
    )


async def list_rooms(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    return [
        {"id": str(r["id"]), "name": r["name"], "config": _coerce(r["config"])}
        for r in await pool.fetch("SELECT id, name, config FROM rooms ORDER BY created_at")
    ]


async def list_compositions(
    pool: asyncpg.Pool, room_id: uuid.UUID | None = None
) -> list[dict[str, Any]]:
    """Saved compositions. `room_id` scopes to a stance (None = all rooms — the god view)."""
    return [
        {"id": str(r["id"]), "name": r["name"], "kind": r["kind"], "spec": _coerce(r["spec"]),
         "webhook_url": r["webhook_url"], "active": r["active"],
         "room_id": str(r["room_id"]) if r["room_id"] else None}
        for r in await pool.fetch(
            "SELECT id, name, kind, spec, webhook_url, active, room_id FROM compositions "
            "WHERE ($1::uuid IS NULL OR room_id=$1) ORDER BY created_at", room_id
        )
    ]


async def object_items(pool: asyncpg.Pool, ids: list[uuid.UUID]) -> list[dict[str, Any]]:
    """Label a result set's objects AND carry their compact properties — in two batch
    queries, not N. The view-switcher needs this: the Graph view uses label/type, the
    Table view shows property columns (sector, date, …) without a per-row fetch."""
    if not ids:
        return []
    objs = await pool.fetch(
        "SELECT id, type, canonical FROM objects WHERE id = ANY($1::uuid[])", ids
    )
    props: dict[uuid.UUID, dict[str, str]] = {}
    for r in await pool.fetch(
        "SELECT DISTINCT ON (object_id, name) object_id, name, value #>> '{}' AS v "
        "FROM current_assertions WHERE object_id = ANY($1::uuid[]) "
        "ORDER BY object_id, name, observed_at DESC",
        ids,
    ):
        props.setdefault(r["object_id"], {})[r["name"]] = r["v"]
    meta = {o["id"]: o for o in objs}
    out: list[dict[str, Any]] = []
    for oid in ids:  # preserve the composition's order
        o = meta.get(oid)
        if o is None:
            continue
        p = props.get(oid, {})
        out.append({"id": str(oid), "type": o["type"], "canonical": o["canonical"],
                    "label": p.get("name") or o["canonical"], "props": p})
    return out


async def run_spec(
    pool: asyncpg.Pool, spec: dict[str, Any], subject: uuid.UUID | None = None,
    name: str = "(spec)",
) -> dict[str, Any]:
    """Evaluate an op-tree and package the Result for the generic renderer. The inline
    composer (W4) runs an EPHEMERAL working spec through here as you edit chips — no save."""
    res = await _eval(pool, spec, subject)
    if res.kind == "objects":
        items: Any = await object_items(pool, res.objects)
    elif res.kind == "rows":
        items = res.rows
    elif res.kind == "data":
        items = res.data  # a Function's native output, passed through untouched
    else:
        items = res.values
    count = len(items) if isinstance(items, list | dict) else 1
    return {"composition": name, "kind": res.kind, "count": count, "items": items, "spec": spec}


async def run_composition(
    pool: asyncpg.Pool, ref: str, subject: uuid.UUID | None = None
) -> dict[str, Any]:
    """Execute a saved composition (by name or id), optionally against a subject."""
    spec = await _spec_of(pool, ref)
    if spec is None:
        return {"error": f"no composition {ref!r}"}
    return await run_spec(pool, spec, subject, name=ref)


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
    # the former bespoke read-models, now forkable compositions over named Functions —
    # opinion left engine code (no more hardcoded read-model + bespoke MCP tool per lens).
    "co-investment-ties": {"op": "function", "name": "coinvest"},
    "who-is-this": {"op": "function", "name": "subject_report"},
    "screen-financing-network": {"op": "function", "name": "screen_network"},
}


async def seed_default_compositions(pool: asyncpg.Pool) -> int:
    for name, spec in DEFAULT_COMPOSITIONS.items():
        await save_composition(pool, name, spec, "lens")
    return len(DEFAULT_COMPOSITIONS)
