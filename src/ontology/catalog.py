"""THE CATALOG STORE (task #97 workstream 1, ruling 52daab71) — the runtime type
catalog, graph-backed. A Type is an Object with type='Type' — fractal, exactly as the
operator asked for by name: it inherits search, links, decisions ABOUT types, threads
ON types, the same as anything else in the graph, instead of living in a side table.

schema.py's declared `_OBJECT_TYPES`/`_LINK_TYPES` tuples remain the SEED MANIFEST —
unchanged, still the source `render_reference()` generates docs/REFERENCE.md from.
Thoth's ruling: the doc snapshots what we SHIP, never a live/accretive query, so an
agent correctly minting a type through `ensure_type` can never turn CI red by using the
feature as designed. `seed_catalog()` is the ONE-TIME (idempotent, safe to re-run every
deploy) direction schema.py's tuples flow: INTO the graph, never derived back out of it.

THE BOOTSTRAP AXIOM: `Actions.create_or_find_object` is the only object-creation
primitive in the system, and it validates its own `type_` argument via
`check_object_type` before the insert. So creating the FIRST Type object would need
"Type" already declared as a known object type — which needs a Type object to exist.
One hardcoded exception breaks the cycle: `check_object_type("Type")` is always valid,
unconditionally, never consulting the catalog. This is the one axiom every
self-describing meta-schema needs; nothing else gets this treatment.

THE CACHE — FINGERPRINT-PER-CALL, not TTL-only (Thoth's ruling, msg 2099). The naive
read is a real cost this migration INTRODUCES, not one it fixes: today's
check_object_type/check_link_type are pure in-memory dict lookups, zero I/O, because
the old catalog is fully resident at import. A bare per-process module-dict cache with
a TTL would trade that for a NEW bug class: osiris runs >=3 processes (mcp/console/
worker) sharing one Postgres, so a type minted through the MCP surface would render
_DEFAULT grey in the console for up to the TTL — a freshly-accreted type invisible
cross-process. Checking a cheap fingerprint (count + max(created_at) over the Type
catalog's own assertions) UNCONDITIONALLY on every call closes that gap outright: the
full catalog only re-loads when the fingerprint has actually moved. Call volume stays
low regardless — check_object_type/check_link_type fire only from
create_or_find_object/create_link/invalidate_link, never from assert_property (the
system's actual highest-frequency write), so the added query is a small fraction of
the transaction cost already sitting on that path. The module dict + TTL is kept ONLY
as a defensive fallback if the fingerprint query itself ever errors — never the
primary freshness mechanism.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

import asyncpg

if TYPE_CHECKING:
    # type-only: actions/core.py imports check_object_type/check_link_type FROM this
    # module, so a real (runtime) import here would be circular. `Actions` is used
    # only in annotations, which `from __future__ import annotations` never evaluates.
    from src.actions.core import Actions

logger = logging.getLogger("osiris.catalog")

_EC = "self_declared"  # a declared type is a mind's word (seed) or a write's own
                       # accretion (stub) — never the machine guessing silently
_CONF = 0.9


@dataclass(frozen=True)
class TypeRecord:
    """The runtime shape of a declared Type — object-kind and link-kind share one
    record; fields irrelevant to a given `kind` simply stay at their default. `category`
    is a SET (tuple), not schema.py's old scalar — the operator's objection was
    literally "categories overlap," so overlap is represented, never resolved by
    force."""

    name: str
    kind: Literal["object", "link"]
    category: tuple[str, ...] = ()
    color: str | None = None
    shape: str | None = None
    description: str = ""
    schemes: tuple[str, ...] = ()
    label_field: str | None = None
    subtitle_field: str | None = None
    domain: tuple[str, ...] = ()
    range: tuple[str, ...] = ()
    required_link_kinds: tuple[str, ...] = ()  # task #189 — object kinds only; meaningless
                                                # on a link record, same as domain/range's
                                                # own object-only-vs-link-only split above


_DEFAULT_OBJECT = TypeRecord("Unknown", "object", ("Other",), "#6e7681", "ellipse",
                             "An ontology object.")
_DEFAULT_LINK = TypeRecord("unknown", "link", description="An undeclared relationship.")

# a Pool auto-acquires/releases a connection per call; a Connection is one already held
# open (e.g. inside Actions.atomic()'s bound transaction) — every read below accepts
# EITHER, because reusing an already-held connection (never drawing a fresh one from the
# same pool mid-transaction) is what keeps check_object_type/check_link_type from
# deadlocking a pool that Actions.atomic() callers have already exhausted (found live
# under test_concurrency.py's 40-way concurrent record_decision).
type _PoolOrConn = asyncpg.Pool | asyncpg.Connection

_CACHE_TTL = 120.0  # fail-safe only, see module docstring — the fingerprint is the real gate
_cache: dict[str, Any] = {}  # "v" -> (fingerprint, cached_at, {(kind,name): TypeRecord})

# THE USAGE-COUNT CACHE (task #121, ruling a4bd555c's "relevance is observed, not
# declared" — the two principles the flat catalog shipped without): a PLAIN TTL, never
# the Type catalog's own fingerprint gate. That gate exists because check_object_type
# needs byte-exact freshness (a just-minted type must validate immediately or a write
# fails that shouldn't). A usage count backing a RANKING has no such requirement — a
# count lagging a few seconds behind the newest write is still an honest ranking signal,
# so trading precision for one fewer query on every full_catalog call is the right
# call, not a shortcut. Measured cost at ~11k objects / ~31k links (EXPLAIN ANALYZE,
# both queries combined): ~7ms warm. Cheap enough that even the TTL is generous, not
# load-bearing — kept because full_catalog sits on get_schema, which every agent's
# orient/mount pays for.
_USAGE_TTL = 30.0
_usage_cache: dict[str, Any] = {}  # "v" -> (cached_at, {obj_type: n}, {link_type: n})


async def _usage_counts(pool: _PoolOrConn) -> tuple[dict[str, int], dict[str, int]]:
    """(object type name -> live instance count, link type name -> live instance count).
    'Live' mirrors the rest of the codebase's own definitions: objects.status='active'
    (seats.py, dossier.py's own convention), links valid_until IS NULL OR > now() (the
    same predicate dossier.py/seats.py already use everywhere else) — a merged object or
    an invalidated link should not inflate a type's apparent relevance."""
    hit = _usage_cache.get("v")
    if hit and time.monotonic() - hit[0] < _USAGE_TTL:
        return hit[1], hit[2]
    obj_rows = await pool.fetch(
        "SELECT type, count(*) AS n FROM objects WHERE status='active' GROUP BY type")
    link_rows = await pool.fetch(
        "SELECT type, count(*) AS n FROM links "
        "WHERE (valid_until IS NULL OR valid_until > now()) GROUP BY type")
    obj_counts = {r["type"]: r["n"] for r in obj_rows}
    link_counts = {r["type"]: r["n"] for r in link_rows}
    _usage_cache["v"] = (time.monotonic(), obj_counts, link_counts)
    return obj_counts, link_counts


def _canonical(name: str, kind: str) -> str:
    return f"type:{kind}:{name}"


async def _fingerprint(pool: _PoolOrConn) -> str | None:
    """A cheap, cheap-to-invalidate-on signal for "has the catalog changed" — count of
    Type-object assertions plus the freshest write time. None on a genuinely empty
    catalog (nothing seeded yet — the fresh-container / not-yet-migrated case)."""
    row = await pool.fetchrow(
        "SELECT count(*) AS n, max(a.created_at) AS latest "
        "FROM objects o JOIN current_assertions a ON a.object_id = o.id "
        "WHERE o.type = 'Type'")
    if row is None or not row["n"]:
        return None
    return f"{row['n']}:{row['latest']}"


async def _load_catalog(pool: _PoolOrConn) -> dict[tuple[str, str], TypeRecord]:
    rows = await pool.fetch(
        "SELECT o.canonical, a.name, a.value "
        "FROM objects o JOIN current_assertions a ON a.object_id = o.id "
        "WHERE o.type = 'Type'")
    fields: dict[str, dict[str, Any]] = {}
    for r in rows:
        fields.setdefault(r["canonical"], {})[r["name"]] = r["value"]
    out: dict[tuple[str, str], TypeRecord] = {}
    for props in fields.values():
        kind = props.get("kind")
        name = props.get("name")
        if kind not in ("object", "link") or not name:
            continue  # a Type row mid-write (kind/name not yet asserted) — skip, not fail
        out[(kind, name)] = TypeRecord(
            name=name, kind=kind,
            category=tuple(props.get("category") or ()),
            color=props.get("color"), shape=props.get("shape"),
            description=props.get("description") or "",
            schemes=tuple(props.get("schemes") or ()),
            label_field=props.get("label_field"), subtitle_field=props.get("subtitle_field"),
            domain=tuple(props.get("domain") or ()), range=tuple(props.get("range") or ()),
            required_link_kinds=tuple(props.get("required_link_kinds") or ()))
    return out


async def _catalog(pool: _PoolOrConn) -> dict[tuple[str, str], TypeRecord]:
    """The fingerprint-checked read — the cache's only entry point. Every caller goes
    through here; nothing reads `_cache` directly."""
    fp = await _fingerprint(pool)
    if fp is None:
        return {}
    hit = _cache.get("v")
    if hit and hit[0] == fp:
        return hit[2]  # type: ignore[no-any-return]
    data = await _load_catalog(pool)
    _cache["v"] = (fp, time.monotonic(), data)
    return data


def _invalidate() -> None:
    """Called at the end of every `ensure_type` write — same idiom as
    semantics.py's `_matrix_cache.clear()` on its own writes."""
    _cache.clear()


async def get_type(
    pool: _PoolOrConn, name: str, kind: Literal["object", "link"],
) -> TypeRecord | None:
    """The declared Type, or None if nothing has ever asserted it."""
    return (await _catalog(pool)).get((kind, name))


async def object_type(pool: _PoolOrConn, name: str) -> TypeRecord:
    """The declared object type, or a neutral default (so the UI never breaks on a
    new type) — schema.py's old object_type() contract, now graph-backed."""
    return await get_type(pool, name, "object") or _DEFAULT_OBJECT


async def link_type(pool: _PoolOrConn, name: str) -> TypeRecord:
    return await get_type(pool, name, "link") or _DEFAULT_LINK


async def is_known_object_type(pool: _PoolOrConn, name: str) -> bool:
    return (await get_type(pool, name, "object")) is not None


async def is_known_link_type(pool: _PoolOrConn, name: str) -> bool:
    return (await get_type(pool, name, "link")) is not None


async def categories(pool: _PoolOrConn) -> list[str]:
    """Every category tag currently in use, deduped, first-appearance order (by the
    owning Type's own creation order) — the graph-backed replacement for schema.py's
    old scalar-category categories()."""
    cat = await _catalog(pool)
    out: list[str] = []
    for rec in cat.values():
        for c in rec.category:
            if c not in out:
                out.append(c)
    return out


async def full_catalog(pool: _PoolOrConn) -> dict[str, Any]:
    """JSON-serializable catalog for the API/UI — the live, graph-backed semantic
    layer as data. Replaces schema.py's old catalog().

    TASK #121 (ruling a4bd555c, the half of the flat-catalog design left unbuilt): the
    catalog is COMPLETE AT THE RECORD — every declared type is always present here,
    never trimmed, never retired; that was explicitly ruled out (a journalist's graph
    and this graph disagree on which 24 of 25 STIX/ATT&CK types read as noise, so
    neither reading gets to delete the other's vocabulary). What's new is BOUNDED AT
    THE LENS: each entry carries its live `count` in THIS graph, and both lists are
    ordered by that count (ties broken by name, for a deterministic, diffable order) —
    "reveal by summoning," instance count standing in for the summon signal, never a
    declared category/domain tag deciding for the reader. A type with zero live
    instances still ships, just last. Recency and co-occurrence (the ruling's other two
    named relevance signals) are natural next steps, not built here — count alone
    already answers the question this task was dispatched to answer."""
    cat = await _catalog(pool)
    obj_counts, link_counts = await _usage_counts(pool)
    object_types = [
        {"name": r.name, "category": list(r.category), "color": r.color,
         "shape": r.shape, "description": r.description, "schemes": list(r.schemes),
         "required_link_kinds": list(r.required_link_kinds),
         "count": obj_counts.get(r.name, 0)}
        for r in cat.values() if r.kind == "object"
    ]
    link_types = [
        {"name": r.name, "description": r.description,
         "domain": list(r.domain), "range": list(r.range),
         "count": link_counts.get(r.name, 0)}
        for r in cat.values() if r.kind == "link"
    ]
    def _rank_key(t: dict[str, Any]) -> tuple[int, str]:
        return (-t["count"], t["name"])

    object_types.sort(key=_rank_key)
    link_types.sort(key=_rank_key)
    return {
        "object_types": object_types,
        "link_types": link_types,
        "categories": await categories(pool),
    }


# --- enforcement — same shape schema.py always had ---------------------------------
_STRICT = False


class UnknownTypeError(Exception):
    """Raised in strict mode when an undeclared object/link type is emitted."""


def set_strict(strict: bool) -> None:
    global _STRICT
    _STRICT = strict


def _complain(kind: str, name: str) -> None:
    msg = f"undeclared {kind} {name!r} — add it to the catalog (ensure_type)"
    if _STRICT:
        raise UnknownTypeError(msg)
    logger.warning(msg)


# THE ACCRETION HOOK (task #97 workstream 2): an undeclared type SELF-DECLARES as a bare
# stub instead of merely logging — the write that triggered the check always succeeds, and
# a real Type object materializes for it to accrete richness onto later (a human, or a
# future seed_catalog run, fills it in; ensure_type's own keep-prior-on-omission contract
# means neither this stub nor a later real declaration can clobber the other's fields).
# STRICT MODE ALWAYS WINS regardless of whether `actions`/`actor` are supplied — it exists
# so tests can catch a real typo as a hard failure, and silently accreting past that would
# defeat the entire point. `actions`/`actor` are optional because not every caller of these
# checks has a live Actions instance to accrete through (e.g. a bare pool-only read path);
# when absent, the original warn-only behavior is unchanged.
async def check_object_type(
    pool: _PoolOrConn, name: str, *, actions: Actions | None = None,
    actor: str | None = None,
) -> None:
    if name == "Type":
        return  # THE BOOTSTRAP AXIOM — see module docstring; never consults the catalog
    if await is_known_object_type(pool, name):
        return
    if not _STRICT and actions is not None and actor is not None:
        await ensure_type(actions, name=name, kind="object", actor=actor)
        return
    _complain("object type", name)


async def check_link_type(
    pool: _PoolOrConn, name: str, *, actions: Actions | None = None,
    actor: str | None = None,
) -> None:
    if await is_known_link_type(pool, name):
        return
    if not _STRICT and actions is not None and actor is not None:
        await ensure_type(actions, name=name, kind="link", actor=actor)
        return
    _complain("link type", name)


async def ensure_type(
    actions: Actions, *, name: str, kind: Literal["object", "link"], actor: str,
    category: list[str] | None = None, color: str | None = None, shape: str | None = None,
    description: str | None = None, schemes: list[str] | None = None,
    label_field: str | None = None, subtitle_field: str | None = None,
    domain: list[str] | None = None, range: list[str] | None = None,
    required_link_kinds: list[str] | None = None,
) -> TypeRecord:
    """Find-or-mint the Type for (kind, name) — idempotent, stub-on-miss, never raises.
    The write path's own tolerance for a novel type (schema.py's original _DEFAULT
    contract) is preserved by this existing; Seshat's accretion hook calls this bare
    (`ensure_type(actions, name=X, kind=Y, actor=Z)`) the moment `check_object_type`/
    `check_link_type` complain, so an undeclared type self-declares as a stub instead
    of merely logging.

    KEEP-PRIOR-ON-OMISSION (the section/room_id discipline, decision 89e67c49,
    generalized): a field left at its `None` default is NEVER asserted — only
    explicitly-passed fields write. A bare call on an ALREADY-RICH Type (one the seed
    step or a human already described) can never blank its description, label_field,
    or category out from under it; that hazard is invisible at the call site and would
    have been a silent data-loss bug found weeks later."""
    canonical = _canonical(name, kind)
    now = datetime.now(UTC)
    oid = await actions.create_or_find_object("Type", canonical, actor)
    fields: dict[str, Any] = {"kind": kind, "name": name}
    if category is not None:
        fields["category"] = list(category)
    if color is not None:
        fields["color"] = color
    if shape is not None:
        fields["shape"] = shape
    if description is not None:
        fields["description"] = description
    if schemes is not None:
        fields["schemes"] = list(schemes)
    if label_field is not None:
        fields["label_field"] = label_field
    if subtitle_field is not None:
        fields["subtitle_field"] = subtitle_field
    if domain is not None:
        fields["domain"] = list(domain)
    if range is not None:
        fields["range"] = list(range)
    if required_link_kinds is not None:
        fields["required_link_kinds"] = list(required_link_kinds)
    for prop_name, value in fields.items():
        await actions.assert_property(oid, prop_name, value, actor, now, _CONF,
                                      evidence_class=_EC)
    _invalidate()
    return (await get_type(actions.pool, name, kind)) or TypeRecord(name=name, kind=kind)


async def seed_catalog(actions: Actions, *, actor: str = "system:catalog-seed") -> dict[str, int]:
    """Migrate schema.py's declared `_OBJECT_TYPES`/`_LINK_TYPES` into graph Type
    objects. Idempotent (ensure_type's own guarantee) — safe to call every deploy, every
    test setup, as many times as needed. ONE-DIRECTIONAL: schema.py's tuples are the
    seed SOURCE, never rewritten from the graph — docs/REFERENCE.md and this seed both
    read the same static manifest, so neither drifts from a live, accretive catalog
    (Thoth's ruling, msg 2099)."""
    from src.ontology import schema

    n_object = n_link = 0
    for t in schema._OBJECT_TYPES:
        await ensure_type(actions, name=t.name, kind="object", actor=actor,
                          category=[t.category], color=t.color, shape=t.shape,
                          description=t.description, schemes=list(t.schemes),
                          label_field=t.label_field, subtitle_field=t.subtitle_field,
                          required_link_kinds=list(t.required_link_kinds))
        n_object += 1
    for lt in schema._LINK_TYPES:
        await ensure_type(actions, name=lt.name, kind="link", actor=actor,
                          description=lt.description, domain=list(lt.domain),
                          range=list(lt.range))
        n_link += 1
    return {"object_types": n_object, "link_types": n_link}
