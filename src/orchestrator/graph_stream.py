"""GRAPH VISUALIZATION STREAM: one endpoint streaming the whole graph as typed
arrays, whose shape was agreed with the client renderer team before it was frozen.
The three.js renderer is the sole consumer, built against this wire format.

WIRE FORMAT: a 4-byte little-endian uint32 header length, that many bytes of UTF-8 JSON
header, then the raw arrays concatenated back to back in the order the header's own
`arrays` map names them (see `_ARRAY_ORDER`): x/y (Float32), type_code/project_code
(Uint16), weight (Float32), status_flag (Uint8, a bitmask: bit0 reserved for a
retired/superseded state, bit1 = CONTESTED per capture.py's own shared CONTESTED_SQL,
the rest reserved), edge_src/edge_dst (Uint32), edge_type_code (Uint8). Array POSITION
is the object index, so there is no redundant index column, per the renderer team's own
read of the proposal.

FOUR ADDITIONS BEYOND THE FROZEN BINARY SHAPE, all header-only (never touching the
arrays the renderer team signed off on), flagged here and in the build report rather
than done quietly:
  1. `object_ids`: an index-aligned array of UUID strings in the JSON header. Needed
     so a consumer (or this module's own future delta poller) can resolve an array
     index back to a real object id; the frozen binary arrays deliberately carry none.
  2. Deltas key on object id (a string), not on array index. Building a persisted,
     cross-request index-assignment registry is out of scope for a spike-stage
     renderer that does not exist yet; a client already holds the snapshot's own
     `object_ids` array and can resolve a delta's id to its own local index in O(1)
     either way. This is a documented departure from the dispatch's literal "keyed
     by object index" wording, not a silent one.
  3. `edge_types`: a code table for `edge_type_code`, same shape as `types`/
     `projects`. A gap found while the renderer team wired against the live endpoint:
     the frozen shape resolved node type/project codes but left `edge_type_code`
     nameless, needed for real relationship color-coding rather than a raw
     hash-on-int placeholder.
  4. `link_type_class`: index-aligned to `edge_types`, "structural"|"semantic" (as
     of the physics layout work: "container"|"structural"|"semantic", where
     container is a flag nested inside the structural class, see
     src.ontology.link_classes.link_class's own docstring), agreed with the renderer
     team before this was committed. The renderer reads the classification off the
     wire rather than hardcoding a second copy client-side; graph_layout.py's own
     relax pass reads the same table to decide which edges are allowed to pull two
     objects together (semantic only, never structural/membership).

NOTE ON THE RETIRED BIT (worth naming, not a bug): today's snapshot query only ever
includes objects the layout heartbeat has actually placed, which itself only ever
places `status NOT IN ('archived','merged','retired')` objects (graph_layout.py), so
bit0 reads 0 for every object this endpoint can currently return. The bit is
reserved, not wired to anything live yet, exactly the same population boundary every
other /graph endpoint already draws.

THE LEGIBILITY PASS:
  5. `labels`: an index-aligned array of short display strings, chosen as a
     header table over a batched GET /graph/labels?ids= endpoint. Every object in the
     snapshot needs a label on first paint regardless, so a table costs one field on a
     payload the client already fetches rather than a second N-object round trip (or a
     second endpoint's own pagination/caching story) for data with the exact same
     lifetime as the snapshot itself. Built by `_short_label`, matching the renderer's
     own formatting rule exactly (Agent by handle, SoftwareProject by repo name,
     Person by name, else type plus a short title, 40 chars hard-truncated with an
     ellipsis) so the two renderings never disagree.
  6. `project_aggregates` / `type_aggregates`: level-of-detail summaries,
     computed in Python from the same positions already resident in memory (no extra
     query). Per project, and per (project, type), a centroid, member count, and a
     radius (the max member distance from that centroid, a tight, exact bound, a
     documented choice over a percentile estimate). `type` here is genuinely the
     object's own type, unlike placement (a separate legibility fix killed type
     rings there); the renderer's own LOD plan draws "type glyphs" at mid zoom, a
     display grouping independent of how objects are actually laid out.
  7. `cluster_edges`: one record per (project pair, link class) with a live
     edge count, for the far-zoom aggregated-edges view, computed from the same
     edge_src/edge_dst/project_code arrays already built, no extra query.

DENSITY NOT DISCS, plus two follow-ups flagged for later, folded into the same
change since all three ship together here:
  8. `edge_weight`: a new index-aligned Float32 array parallel to edge_src/edge_dst/
     edge_type_code (added to `_ARRAY_ORDER`, a real wire-format change agreed with
     the renderer team before commit). Raw, never a pre-normalised 0-1
     curve, per the renderer team's own steer: the client already log2-normalises
     client-side for cluster_edges' color-brightness mix, and raw keeps that
     flexibility rather than baking one curve into the wire. Flat 1.0 for every
     individual link for now: a later client-side change retires "strongest N" edge
     filtering entirely, so every edge now draws at every zoom, alpha scaled by
     on-screen count, and there is no live consumer asking this array to
     differentiate individual edges yet. The field exists on the wire because the
     dispatch asked for it and a future consumer may want it; `type_pair_edges`' own
     `count` below is the renderer's actual next real-weight consumer.
  9. `type_pair_edges`: one record per ((project,type) bucket pair, link class) with
     a live count, the same shape as `cluster_edges` one level finer. cluster_edges is
     project-to-project only (same-project pairs excluded), this is bucket-to-bucket
     (same-bucket pairs excluded, but same-project-different-type is included), a
     follow-up addressing "the mid tier now draws no edges because the stream has
     no (project,type)-pair aggregate."
     `count` doubles as this record's own weight (a live edge count is already a real
     ranking, unlike the individual-edge case `edge_weight` above exists to fix).
 10. THE NAMELESS-AGENT LABEL FIX: an Agent with no `handle` assertion used to fall
     through to `type_name + canonical` ("Agent agent:b5f0f4b4..."), the exact
     "labels show ids" shape the whole legibility pass was meant to kill, just for
     the one type that can genuinely lack a name (an anonymous swarm session that
     never claimed a name). Fixed per the rule: lineage handle plus generation
     when the Agent's own lineage currently holds a Seat (`seats.held_seat`, the same
     path orient()/mount() use), else, per THE AGENT IDENTITY FIX: "<Patronym> <ROMAN>
     · <model short>" from the `patronym`
     assertion, the canonical's own generation suffix, and a short-formed
     `source_model`, with a trailing " ⌊ sub" marker for a sidechain fork
     (`is_sidechain`), superseding an earlier "Agent · <model> in <project>" whose
     own `model` lookup came back empty for 7,618 live osiris agents, rendering
     "Agent · ? in osiris". Never the raw id either way: a patronym-less agent
     (disclosed as possible, not observed) falls back to its own canonical short
     id rather than a bare "?". THE IDENTITY FORMAT IS ALWAYS USED FOR AGENT: a bare
     `handle`/`name` assertion used to bypass this whole scheme and win as the label
     directly (258 live agents stuck reading as a bare handle, no numeral),
     resolved for every Agent row now, that raw string only ever seeding the
     patronym slot when a real `patronym` assertion is absent, never standing in
     for the label itself.
     THE 1,851 REMAINDER: of 19,540 Agent labels checked at the time,
     17,689 carried a numeral; the rest were an auto-generated `name`
     assertion ("claude in osiris", "general-purpose spawn") wrongly seeding
     the patronym slot as if it were a real name, or a generation-1 label
     dropping its own roman numeral outright. A LATER REGRESSION: the fix for
     that remainder (a per-shape `name`-string detector)
     both missed a third auto-shape ("<model> · <description>", lineage.py:247)
     and, for a spawned child whose `patronym` is already the compound "<stem>
     <parent ROMAN>.<ordinal>" `patronym_for` mints, appended a second fresh
     roman numeral on top (e.g. "I.1 I"). STOP PATTERN-MATCHING NAME STRINGS:
     `name`/`summary`/`title` are never read for an Agent's stem at all now,
     closed-endedly, and a compound patronym's own embedded roman+ordinal is
     read as the complete generation field rather than recomputed and
     appended again, see `_agent_identity_labels`'s own docstring.
 11. `created_at`: a new index-aligned Float32 array (epoch seconds, added to
     `_ARRAY_ORDER`, same real-wire-format-change convention as `edge_weight` above)
     for a lineage-timeline view: a focused Agent's own succession chain lays out on
     a real time axis, needing each session's own first-seen moment. `o.created_at`
     was already SELECTed (it drives this query's own stable ORDER BY) but never left
     the query as data, so it is genuinely free, no second query, no extra join.
     Precision is whatever `objects.created_at` already carries (row-insert time, not
     a curated "minted" assertion), named `created_at` rather than `minted_at` on the
     wire to say exactly that, honestly, rather than implying a richer provenance
     concept this one column doesn't carry. Float32, not float64, like every other
     array here: at today's epoch magnitude (~1.79e9) the ULP is 128s, so a value can
     round up to ~64s off its real timestamp, fine for a timeline spanning
     weeks/months/years, disclosed for any future consumer that reaches for
     sub-minute precision.
"""
from __future__ import annotations

import json
import math
import re
import struct
import uuid
from collections import defaultdict
from typing import Any

import asyncpg

from src.ontology.link_classes import link_class
from src.orchestrator.capture import CONTESTED_SQL
from src.orchestrator.project_identity import resolve_merge_survivors

SCHEMA_VERSION = 1
STATUS_RETIRED = 1 << 0
STATUS_CONTESTED = 1 << 1
_LABEL_MAX_CHARS = 40

# STOP PATTERN-MATCHING NAME STRINGS: the `name`/`summary`/`title` assertions are
# never read for an Agent's stem at all, not even through a detector for a known
# machine-generated shape (an earlier fix's own `_is_auto_shaped_name`, which this
# replaces). `name` on an Agent is always either a human-legible echo of a
# structural field (redundant) or one of several auto-generated shapes this module
# has no closed list of (agents.py:4060's "<model> in <project>", lineage.py:358's
# "<agent_type> spawn", lineage.py:247's "<model> · <description>", and whatever the
# next one turns out to be); excluding shapes one regex at a time chases an open
# set forever. The stem comes only from structured fields: `patronym`, else
# `handle` (stripped to its leading alphabetic run, since a legacy or malformed
# handle can carry noise no real name does), else the canonical short id.
_LEADING_ALPHA_RE = re.compile(r"[A-Za-z]+")

# A `patronym` assertion is minted by `patronym_for` (lineage.py) in exactly one
# compound shape for a spawned child: "<parent's own display name> <parent's
# ROMAN numeral>.<birth ordinal>" (e.g. "Alpha I.1", "Beta XIII.4"; the
# roman belongs to the parent, the child rides it dotted). THE DOUBLE NUMERAL BUG:
# treating that whole compound as a bare stem and then appending this child's own
# freshly-computed generation roman on top produced e.g. "Alpha I.1 I", two
# numerals, one already embedded in the compound, one appended fresh. This regex
# recognizes the same sanctioned shape `_PATRONYM_ORDINAL` (lineage.py) already
# parses for the same assertion, splitting a compound patronym into its real stem
# and its already-complete generation+ordinal, so nothing is appended twice.
_PATRONYM_COMPOUND_RE = re.compile(r"^(.+) ([IVXLCDM]+)\.(\d+)$")

# The trailing sidechain-fork marker `_agent_identity_labels` appends (module
# docstring, " ⌊ sub" marker); `_short_label`'s own truncation must never clip it.
_SUB_SUFFIX_RE = re.compile(r" ⌊ sub$")


def _short_label(
    type_name: str, canonical: str, handle: str | None, name: str | None,
    title: str | None, *, agent_fallback: str | None = None,
) -> str:
    """THE LEGIBILITY PASS: the client never guesses a
    label. Agent by its own resolved identity format (never a raw handle/name/id
    directly, see `agent_fallback` below), SoftwareProject by its own repo name
    (canonical minus the 'repo:' scheme), Person by name, everything else its own
    type plus a short title. `title` is the object's own current summary/title/
    subject/name assertion (whichever wins by confidence then recency, one query,
    see fetch_snapshot's own correlated subquery), never canonical, for any type
    that actually carries one (Decision/Thread/Message/Commit and friends). Fixed
    after a live check found the first cut of this function fell through to
    canonical for every type outside Agent/SoftwareProject/Person, so a Decision
    read "Decision decision:91da77..." on the deployed space, the original
    "labels show ids" problem in a new coat. Canonical is the last
    resort now, only when no title-shaped assertion exists at all. One line (any
    embedded newline/whitespace run collapsed to a single space, since a Decision's
    own summary is often multi-line prose), hard-truncated at 40 characters with an
    ellipsis, matching the renderer's own formatting rule exactly so the two
    renderings never disagree.

    `agent_fallback` (`_agent_identity_labels`, computed for every Agent row):
    a caller-resolved "lineage handle + generation" or "<patronym> <roman> ·
    <model>" string (see `fetch_snapshot`'s own resolution, a Seat lookup and
    a couple of assertion reads this pure function has no pool to make
    itself). Required for type Agent, never optional, so an Agent never reads
    its own raw handle/name/summary/subject assertion as a label directly, or,
    worse, canonical, the exact id-shaped label this whole rule exists to
    kill.

    THE IDENTITY FORMAT IS ALWAYS USED FOR AGENT: a bare `handle` assertion used to
    win before `agent_fallback` ever ran, so 258 live Agent labels read as a bare
    handle with no generation, because a raw handle stamp (the lineage's own
    name, no numeral) short-circuited past the seat/patronym resolver entirely.
    `agent_fallback` (renamed `_agent_identity_labels`, now computed for every
    Agent row, not just handle-less ones) is required for type Agent now; the
    handle/name fields only ever feed it as a patronym fallback, never bypass
    it as a label of their own."""
    if type_name == "Agent" and agent_fallback:
        label = agent_fallback
    elif type_name == "SoftwareProject":
        label = canonical.removeprefix("repo:") or canonical
    elif type_name == "Person" and name:
        label = name
    elif title:
        label = f"{type_name} {' '.join(title.split())}"
    else:
        label = f"{type_name} {canonical}"
    if len(label) > _LABEL_MAX_CHARS:
        # AGENT LABEL COSMETICS: a hard slice at
        # _LABEL_MAX_CHARS could land inside (or just past) a trailing sidechain
        # marker (" ⌊ sub"), clipping it into unreadable noise, so truncate the
        # stem/model body, never the marker itself. The marker is short and fixed
        # (6 chars), so this trades a few characters over the nominal ceiling in
        # the sidechain case for a marker that always reads whole.
        m = _SUB_SUFFIX_RE.search(label)
        suffix = m.group(0) if m else ""
        body = label[:len(label) - len(suffix)] if suffix else label
        label = body[:_LABEL_MAX_CHARS - len(suffix) - 1] + "…" + suffix
    return label


def _centroid_and_radius(points: list[tuple[float, float]]) -> tuple[float, float, float]:
    """A group's own centroid plus the max member distance from it: a tight, exact
    bound (never a percentile estimate) so a client-drawn LOD circle always fully
    contains every real member."""
    cx = sum(p[0] for p in points) / len(points)
    cy = sum(p[1] for p in points) / len(points)
    radius = max(math.hypot(px - cx, py - cy) for px, py in points)
    return cx, cy, radius


# name -> struct format code; order here IS the wire order.
_ARRAY_ORDER: tuple[tuple[str, str], ...] = (
    ("x", "f"), ("y", "f"),
    ("type_code", "H"), ("project_code", "H"),
    ("weight", "f"), ("status_flag", "B"),
    ("edge_src", "I"), ("edge_dst", "I"), ("edge_type_code", "B"), ("edge_weight", "f"),
    ("created_at", "f"),
)


def encode_snapshot(
    *,
    object_ids: list[str], x: list[float], y: list[float],
    type_code: list[int], project_code: list[int], weight: list[float],
    status_flag: list[int], edge_src: list[int], edge_dst: list[int],
    edge_type_code: list[int], edge_weight: list[float], types: list[str],
    projects: list[str], edge_types: list[str], link_type_class: list[str],
    labels: list[str], created_at: list[float],
    watermark: int = 0,
    project_aggregates: list[dict[str, Any]] | None = None,
    type_aggregates: list[dict[str, Any]] | None = None,
    cluster_edges: list[dict[str, Any]] | None = None,
    type_pair_edges: list[dict[str, Any]] | None = None,
    community_code: list[int] | None = None,
    communities: list[dict[str, Any]] | None = None,
) -> bytes:
    """Pure function, no DB: builds the exact wire bytes from already-resolved
    columns. The DB-facing half (`fetch_snapshot`) is the only caller that ever
    needs a live pool, so this half is fully unit-testable on its own."""
    count = len(object_ids)
    edge_count = len(edge_src)
    arrays: dict[str, list[Any]] = {
        "x": x, "y": y, "type_code": type_code, "project_code": project_code,
        "weight": weight, "status_flag": status_flag, "created_at": created_at,
        "edge_src": edge_src, "edge_dst": edge_dst, "edge_type_code": edge_type_code,
        "edge_weight": edge_weight,
    }
    for name in ("x", "y", "type_code", "project_code", "weight", "status_flag",
                 "created_at"):
        if len(arrays[name]) != count:
            raise ValueError(
                f"{name} has {len(arrays[name])} entries, expected {count} (count)")
    for name in ("edge_src", "edge_dst", "edge_type_code", "edge_weight"):
        if len(arrays[name]) != edge_count:
            raise ValueError(
                f"{name} has {len(arrays[name])} entries, expected {edge_count} "
                f"(edge_count)")
    if len(link_type_class) != len(edge_types):
        raise ValueError(
            f"link_type_class has {len(link_type_class)} entries, expected "
            f"{len(edge_types)} (len(edge_types))")
    if len(labels) != count:
        raise ValueError(f"labels has {len(labels)} entries, expected {count} (count)")
    community_code = community_code if community_code is not None else [0] * count
    if len(community_code) != count:
        raise ValueError(
            f"community_code has {len(community_code)} entries, expected {count} (count)")
    body = bytearray()
    offsets: dict[str, dict[str, int | str]] = {}
    for name, code in _ARRAY_ORDER:
        col = arrays[name]
        packed = struct.pack(f"<{len(col)}{code}", *col)
        offsets[name] = {"offset": len(body), "length": len(col), "dtype": code}
        body.extend(packed)

    header = {
        "schema_version": SCHEMA_VERSION, "count": count, "edge_count": edge_count,
        "types": types, "projects": projects, "edge_types": edge_types,
        "link_type_class": link_type_class, "object_ids": object_ids,
        "labels": labels, "watermark": watermark,
        "project_aggregates": project_aggregates or [],
        "type_aggregates": type_aggregates or [],
        "cluster_edges": cluster_edges or [],
        "type_pair_edges": type_pair_edges or [],
        "community_code": community_code,
        "communities": communities or [],
        "arrays": offsets,
    }
    header_bytes = json.dumps(header).encode()
    return struct.pack("<I", len(header_bytes)) + header_bytes + bytes(body)


def decode_snapshot(data: bytes) -> dict[str, Any]:
    """The reference decoder: a Python-side duplicate of what a JS client does, used
    by this module's own round-trip test and by the CLI mirror's summary line. A
    real renderer works straight off the typed-array offsets in the header instead."""
    (header_len,) = struct.unpack_from("<I", data, 0)
    header = json.loads(bytes(data[4:4 + header_len]).decode())
    body = data[4 + header_len:]
    out: dict[str, Any] = {
        k: header[k] for k in
        ("schema_version", "count", "edge_count", "types", "projects", "edge_types",
         "link_type_class", "object_ids", "labels", "watermark",
         "project_aggregates", "type_aggregates", "cluster_edges", "type_pair_edges",
         "community_code", "communities")
    }
    for name, meta in header["arrays"].items():
        code = str(meta["dtype"])
        n = int(meta["length"])
        off = int(meta["offset"])
        out[name] = list(struct.unpack_from(f"<{n}{code}", body, off))
    return out


def _model_short(model: str) -> str:
    """claude-sonnet-5 -> sonnet-5: the vendor prefix carries no information a
    reader of these labels needs; every model source_model ever
    asserts here is a Claude one, so a fixed prefix strip is exact, not a guess."""
    return model.removeprefix("claude-")


def _display_stem(stem: str) -> str:
    """AGENT LABEL COSMETICS: a seat's own `handle`
    assertion is stored in whatever casing it was first claimed with (e.g. 'alpha')
    next to a patronym-derived stem, which is always already correctly
    cased (e.g. 'Beta'), producing a visibly inconsistent header ('alpha CVII' beside
    'Beta LXXII'). `str.islower()` is true only when the string has at least one
    cased character and none of them are uppercase, so a stem with no letters at all
    (never happens for a real handle, but a defensive case) or one already carrying
    any capital passes through untouched: this only ever capitalises the single
    all-lowercase shape that actually needs it."""
    return stem[0].upper() + stem[1:] if stem.islower() else stem


async def _agent_identity_labels(
    pool: asyncpg.Pool, rows: list[asyncpg.Record],
) -> dict[uuid.UUID, str]:
    """THE IDENTITY FORMAT IS ALWAYS USED FOR AGENT: a live header
    check found 258 Agent labels still reading as a bare handle
    with no generation, because a raw `handle`/`name` assertion on the object was
    winning in `_short_label` before this function ever ran, since this
    function used to resolve only the handle-less subset. Now resolved for
    every Agent row, unconditionally; `_short_label` no longer has a bypass
    branch of its own.

    Lineage handle plus generation when the Agent's own lineage currently holds a
    Seat: that's the recognisable, load-bearing name a reader actually wants
    for a live seat-holder. Otherwise, per THE AGENT IDENTITY FIX (a live count found
    7,618 osiris agents were rendering "Agent · ? in osiris" because the old
    fallback's own `model` lookup came back empty for this subset and
    `patronym`, which does carry a real name for almost all of them, was never
    read): "<stem> <ROMAN>[.ordinal] · <model short>", with `source_model`
    short-formed (`_model_short`); a sidechain fork (`is_sidechain`) gets a
    trailing " ⌊ sub" marker on top, disclosure not suppression (mailbox.py's
    own is_sidechain disclosure rule). Never "?" and
    never a bare stem with no numeral.

    STRUCTURAL FIELDS ONLY, NEVER NAME/SUMMARY/TITLE: the stem is `patronym`, else
    `handle`'s own leading alphabetic run, else the canonical short id; `name` is
    never read for an Agent at all, closed-endedly (see `_LEADING_ALPHA_RE`'s own
    module comment for why a per-shape detector was the wrong fix). THE DOUBLE
    NUMERAL FIX: `patronym_for` (lineage.py) mints a spawned
    child's patronym as the compound "<parent stem> <parent ROMAN>.<birth
    ordinal>" (e.g. "Alpha I.1"); treating that as a bare stem and
    appending this child's own freshly-computed roman on top produced "Alpha
    I.1 I". `_PATRONYM_COMPOUND_RE` recognises the shape and uses its
    embedded roman+ordinal as the complete generation field, stem split out
    separately, nothing appended twice. EVERY AGENT LABEL CARRIES A NUMERAL:
    generation 1 shows "I", never omitted, in both the
    seat branch and this one."""
    agents = [r for r in rows if r["type"] == "Agent"]
    if not agents:
        return {}
    from src.orchestrator.agents import _generation, _roman_display
    from src.orchestrator.seats import held_seat

    ids = [r["id"] for r in agents]
    detail_rows = await pool.fetch(
        "SELECT o.id, "
        "  (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='source_model' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS model, "
        "  (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='patronym' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS patronym, "
        "  (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='seat_generation' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS seat_generation, "
        "  EXISTS(SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='is_sidechain' AND a.value #>> '{}' = 'true') AS is_sidechain "
        "FROM objects o WHERE o.id = ANY($1::uuid[])", ids)
    detail_by_id = {r["id"]: r for r in detail_rows}

    out: dict[uuid.UUID, str] = {}
    for r in agents:
        oid, canonical = r["id"], r["canonical"]
        detail = detail_by_id.get(oid)
        # THE MODEL SUFFIX ON EVERY LABEL: computed once,
        # applied uniformly to whichever branch below resolves the stem.
        # The seat branch used to omit it entirely (by the original design of
        # THE NAMELESS-AGENT LABEL FIX), and the canonical-stem fallback still
        # would have without this, same gap in a third place.
        model = detail["model"] if detail else None
        model_suffix = f" · {_model_short(model)}" if model else ""
        sub_suffix = " ⌊ sub" if detail and detail["is_sidechain"] else ""

        seat = await held_seat(pool, canonical)
        if seat and seat.get("handle"):
            # THE SEAT-GENERATION FIX: a
            # seat-succession canonical (agent:seat-<id>-g<N>) stamps its own
            # ordinal as a `seat_generation` assertion (`agents.seat_label`'s
            # own authoritative source), never encoded in the canonical's own
            # roman/g-suffix string the way an ordinary lineage id is.
            # `_generation(canonical)` silently returned 1 for any such id
            # below the "-g40" numeric-overflow threshold (a "-g3" or "-g45"
            # canonical parses as a brand-new root, not a real generation),
            # dropping the numeral and printing a bare handle. Read the real
            # assertion first, falling back to the canonical parse only when
            # it's genuinely absent (an agent claimed before the seat convention,
            # `seat_label`'s own documented fallback case).
            seat_gen_raw = detail["seat_generation"] if detail else None
            gen = int(seat_gen_raw) if seat_gen_raw else _generation(canonical)[1]
            # THE WRONG ROMAN FUNCTION: a prior regression called
            # `agents._to_roman`, the canonical-id-suffix formatter, capped
            # at 39 and falling back to a raw "g<n>" escape hatch above that
            # (agents.py's own docstring: "a lineage deeper than that gets a
            # plain numeric suffix"), exactly the shape it exists to produce
            # for a mintable id, never meant to reach a human-facing label.
            # `agents._roman_display` is the unbounded display formatter
            # (already uppercase) `seat_label` itself uses for a label like
            # "Alpha CVI", the one this module always should have called.
            # EVERY AGENT LABEL CARRIES A NUMERAL: generation 1
            # used to omit the roman suffix entirely, so a first-generation
            # seat-holder read as a bare handle indistinguishable from the old
            # bypass this whole arc exists to kill: show "I" rather than drop it.
            out[oid] = (f"{_display_stem(seat['handle'])} {_roman_display(gen)}"
                       f"{model_suffix}{sub_suffix}")
            continue
        # STOP PATTERN-MATCHING NAME STRINGS: the `name`
        # assertion is never read here at all any more, see the module-level
        # comment above `_LEADING_ALPHA_RE`. Stem is patronym, else handle's
        # own leading alphabetic run (never the raw handle verbatim, since a
        # legacy value can carry noise past the first word), else nothing
        # (falls to the canonical stem below).
        raw_patronym = detail["patronym"] if detail else None
        compound = _PATRONYM_COMPOUND_RE.match(raw_patronym) if raw_patronym else None
        # THE DOUBLE NUMERAL FIX: when the patronym is
        # already the compound "<stem> <ROMAN>.<ordinal>" shape `patronym_for`
        # mints for a spawned child, that is the complete generation+sub-index,
        # never recomputed fresh and appended a second time on top.
        if compound:
            stem: str | None = compound.group(1)
            gen_display = f"{compound.group(2)}.{compound.group(3)}"
        else:
            handle_match = _LEADING_ALPHA_RE.match(r["handle"]) if r["handle"] else None
            stem = raw_patronym or (handle_match.group(0) if handle_match else None)
            gen_display = None
        if not stem:
            out[oid] = f"{canonical.removeprefix('agent:')}{model_suffix}{sub_suffix}"
            continue
        if gen_display is None:
            # EVERY AGENT LABEL CARRIES A NUMERAL: same rule
            # as the seat branch above, "I" for generation 1, never a bare stem.
            gen = _generation(canonical)[1]
            gen_display = _roman_display(gen)
        # AGENT LABEL COSMETICS: applied once here,
        # after both the compound-patronym and the raw_patronym/handle-fallback
        # branches above have resolved `stem`. A compound patronym like "alpha
        # I.1" carries its own inherited casing from a lowercase-handled parent
        # (patronym_for stamps the parent's own display stem verbatim), the same
        # defect as the seat branch's raw handle, just one hop removed.
        out[oid] = f"{_display_stem(stem)} {gen_display}{model_suffix}{sub_suffix}"
    return out


async def fetch_snapshot(pool: asyncpg.Pool) -> bytes:
    """The live query: every already-placed object's position/type/project/weight/
    status, plus every live edge between two objects both in that set, encoded via
    `encode_snapshot`. Row order is `o.created_at ASC, o.id ASC` (stable within one
    call) but is not persisted as a registry anywhere; see the module docstring for
    why deltas key on object id rather than this order.

    THE DELTA CURSOR FIX: the header carries the
    outbox watermark this snapshot was built at, read first, before the position/
    edge queries below, so an event landing mid-snapshot is still covered by the
    main query (its row is simply included) rather than falling into the gap between
    "already in the snapshot" and "cursor starts after it." The one-sided risk this
    ordering accepts is a rare harmless replay (the client re-applies a delta whose
    effect the snapshot already carried), never a silent drop."""
    watermark = int(await pool.fetchval("SELECT COALESCE(max(id), 0) FROM outbox"))
    rows = await pool.fetch(
        "SELECT o.id, o.type, o.canonical, o.created_at, "
        "  (SELECT (a.value #>> '{}')::float8 FROM current_assertions a "
        "   WHERE a.object_id=o.id AND a.name='graph_x') AS x, "
        "  (SELECT (a.value #>> '{}')::float8 FROM current_assertions a "
        "   WHERE a.object_id=o.id AND a.name='graph_y') AS y, "
        "  (SELECT a.value #>> '{}' FROM current_assertions a "
        "   WHERE a.object_id=o.id AND a.name='handle' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS handle, "
        "  (SELECT a.value #>> '{}' FROM current_assertions a "
        "   WHERE a.object_id=o.id AND a.name='name' "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS name, "
        "  (SELECT a.value #>> '{}' FROM current_assertions a "
        "   WHERE a.object_id=o.id AND a.name IN ('summary','title','subject','name') "
        "   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS title, "
        "  COALESCE(p.id, ap.id) AS project_obj_id, "
        f"  {CONTESTED_SQL} AS contested "
        "FROM objects o "
        "LEFT JOIN links l ON l.from_id=o.id AND l.type='in_repo' "
        "  AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "LEFT JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
        "LEFT JOIN current_assertions pa ON pa.object_id=o.id AND pa.name='project' "
        "LEFT JOIN objects ap ON ap.type='SoftwareProject' "
        "  AND ap.canonical = 'repo:' || (pa.value #>> '{}') "
        "WHERE o.status NOT IN ('archived','merged','retired') "
        "  AND EXISTS (SELECT 1 FROM current_assertions a "
        "    WHERE a.object_id=o.id AND a.name='graph_x') "
        "ORDER BY o.created_at ASC, o.id ASC")
    # MEMBERSHIP FOLLOWS A MERGE: either join above can name a
    # SoftwareProject already folded into a survivor (in_repo minted, or a `project`
    # assertion's bare name still matching the merged object's own canonical),
    # resolved through the same merged_into walk graph_physics' own membership union
    # uses, id-batched rather than per-row, then mapped back to the survivor's live
    # canonical (never the folded object's own stale one).
    project_obj_ids = {r["project_obj_id"] for r in rows if r["project_obj_id"] is not None}
    survivors = await resolve_merge_survivors(pool, project_obj_ids)
    survivor_ids = set(survivors.values()) | (project_obj_ids - set(survivors))
    canon_rows = await pool.fetch(
        "SELECT id, canonical FROM objects WHERE id = ANY($1::uuid[])", list(survivor_ids))
    canonical_by_id = {r["id"]: r["canonical"] for r in canon_rows}
    weight_rows = await pool.fetch(
        "SELECT node, count(*) AS n FROM ("
        "  SELECT from_id AS node FROM links "
        "    WHERE valid_until IS NULL OR valid_until > now() "
        "  UNION ALL "
        "  SELECT to_id AS node FROM links "
        "    WHERE valid_until IS NULL OR valid_until > now()"
        ") x GROUP BY node")
    weight_by_id: dict[uuid.UUID, int] = {r["node"]: int(r["n"]) for r in weight_rows}
    agent_fallback_by_id = await _agent_identity_labels(pool, rows)

    type_index: dict[str, int] = {}
    project_index: dict[str, int] = {}
    id_index: dict[uuid.UUID, int] = {}
    object_ids: list[str] = []
    xs: list[float] = []
    ys: list[float] = []
    type_codes: list[int] = []
    project_codes: list[int] = []
    weights: list[float] = []
    statuses: list[int] = []
    labels: list[str] = []
    created_ats: list[float] = []
    project_membership: dict[uuid.UUID, uuid.UUID] = {}
    project_uuid_to_code: dict[uuid.UUID, int] = {}

    for i, r in enumerate(rows):
        oid = r["id"]
        id_index[oid] = i
        object_ids.append(str(oid))
        xs.append(float(r["x"] or 0.0))
        ys.append(float(r["y"] or 0.0))
        type_codes.append(type_index.setdefault(r["type"], len(type_index)))
        project_obj_id = r["project_obj_id"]
        resolved_id = survivors.get(project_obj_id, project_obj_id) if project_obj_id else None
        project_key = canonical_by_id.get(resolved_id, "unfiled") if resolved_id else "unfiled"
        pcode = project_index.setdefault(project_key, len(project_index))
        project_codes.append(pcode)
        weights.append(float(weight_by_id.get(oid, 0)))
        statuses.append(STATUS_CONTESTED if r["contested"] else 0)
        labels.append(_short_label(
            r["type"], r["canonical"], r["handle"], r["name"], r["title"],
            agent_fallback=agent_fallback_by_id.get(oid)))
        created_ats.append(r["created_at"].timestamp() if r["created_at"] else 0.0)
        if resolved_id is not None:
            project_membership[oid] = resolved_id
            project_uuid_to_code.setdefault(resolved_id, pcode)

    edge_src: list[int] = []
    edge_dst: list[int] = []
    edge_type_codes: list[int] = []
    edge_type_index: dict[str, int] = {}
    edge_rows: list[asyncpg.Record] = []
    if id_index:
        edge_rows = await pool.fetch(
            "SELECT from_id, to_id, type FROM links "
            "WHERE (valid_until IS NULL OR valid_until > now()) "
            "  AND from_id = ANY($1::uuid[]) AND to_id = ANY($1::uuid[])",
            list(id_index.keys()))
        for r in edge_rows:
            f, t = r["from_id"], r["to_id"]
            if f not in id_index or t not in id_index:
                continue
            edge_src.append(id_index[f])
            edge_dst.append(id_index[t])
            edge_type_codes.append(
                edge_type_index.setdefault(r["type"], len(edge_type_index)))

    # COMMUNITY CODE: reuses graph_physics._detect_communities
    # unchanged, the same Leiden partition the physics
    # migration computes, never a second, independently-drifting copy. Scoped
    # to projects already over `_COMMUNITY_MIN_MEMBERS` (today, effectively just
    # osiris), so the added cost is one Leiden run over a population this size,
    # not the whole 51k-object snapshot, paid on every /graph/stream connect
    # (this endpoint has no cache layer, matching every other field it computes
    # from the same in-memory query result). `community_code` is index-aligned
    # to `object_ids`/`project_code` exactly like the frozen arrays; 0 is the
    # sentinel for "no real community" (a small project, or a genuinely
    # noise-sized Leiden cluster). `communities` (the header table, not a new
    # DB table) never carries an entry for it, same convention `project_
    # aggregates`/`type_aggregates` already use for their own code spaces.
    from src.orchestrator.graph_physics import _detect_communities

    community_membership = _detect_communities(
        edge_rows, project_membership, set(id_index.keys()))
    community_index: dict[tuple[uuid.UUID, int], int] = {}
    community_codes: list[int] = [0] * len(object_ids)
    for oid, (pid, local_id) in community_membership.items():
        obj_idx = id_index.get(oid)
        if obj_idx is None:
            continue
        key = (pid, local_id)
        code = community_index.setdefault(key, len(community_index) + 1)
        community_codes[obj_idx] = code
    community_points: dict[int, list[tuple[float, float]]] = defaultdict(list)
    for i, code in enumerate(community_codes):
        if code:
            community_points[code].append((xs[i], ys[i]))
    communities = []
    for (pid, _local_id), code in sorted(community_index.items(), key=lambda kv: kv[1]):
        pts = community_points.get(code, [])
        cx, cy, radius = _centroid_and_radius(pts)
        communities.append({
            "community": code, "project": project_uuid_to_code.get(pid, 0),
            "count": len(pts), "cx": round(cx, 2), "cy": round(cy, 2),
            "radius": round(radius, 2),
        })

    # DENSITY NOT DISCS: a flat per-edge weight, raw, not a
    # pre-normalised 0-1 curve (the client already log2-normalises
    # client-side for cluster_edges' own color-brightness mix, and raw keeps that
    # flexibility rather than baking one curve into the wire). 1.0 for every
    # individual link for now: a later client-side change retires
    # "strongest N" filtering entirely (every edge draws at every zoom, alpha
    # scaled by on-screen count instead), so there is no live consumer asking this
    # array to differentiate individual edges yet; a real per-edge count/weight is
    # exactly what `type_pair_edges`' own `count` field already provides one level
    # up, which is named as the actual next consumer.
    edge_weights: list[float] = [1.0 for _ in edge_src]

    types = [name for name, _ in sorted(type_index.items(), key=lambda kv: kv[1])]
    projects = [name for name, _ in sorted(project_index.items(), key=lambda kv: kv[1])]
    edge_types = [name for name, _ in sorted(edge_type_index.items(), key=lambda kv: kv[1])]
    link_type_class = [link_class(name) for name in edge_types]

    # THE LEGIBILITY PASS: LOD aggregates, computed from the positions already
    # resident above, no extra query.
    proj_points: dict[int, list[tuple[float, float]]] = defaultdict(list)
    type_points: dict[tuple[int, int], list[tuple[float, float]]] = defaultdict(list)
    for i in range(len(object_ids)):
        pt = (xs[i], ys[i])
        proj_points[project_codes[i]].append(pt)
        type_points[(project_codes[i], type_codes[i])].append(pt)
    project_aggregates = []
    for pcode, pts in proj_points.items():
        cx, cy, radius = _centroid_and_radius(pts)
        project_aggregates.append({
            "project": pcode, "count": len(pts),
            "cx": round(cx, 2), "cy": round(cy, 2), "radius": round(radius, 2),
        })
    type_aggregates = []
    for (pcode, tcode), pts in type_points.items():
        cx, cy, radius = _centroid_and_radius(pts)
        type_aggregates.append({
            "project": pcode, "type": tcode, "count": len(pts),
            "cx": round(cx, 2), "cy": round(cy, 2), "radius": round(radius, 2),
        })

    # THE LEGIBILITY PASS: one record per (project pair, link class), same
    # edge/project arrays already built, no extra query.
    cluster_counts: dict[tuple[int, int, str], int] = defaultdict(int)
    for i in range(len(edge_src)):
        p1, p2 = project_codes[edge_src[i]], project_codes[edge_dst[i]]
        if p1 == p2:
            continue
        a, b = (p1, p2) if p1 <= p2 else (p2, p1)
        cluster_counts[(a, b, link_type_class[edge_type_codes[i]])] += 1
    cluster_edges = [
        {"a": a, "b": b, "class": cls, "count": n}
        for (a, b, cls), n in cluster_counts.items()
    ]

    # DENSITY NOT DISCS: one record per (project,type)-bucket pair,
    # the same shape one level finer. Same-bucket pairs excluded (a self-loop has
    # nothing to draw a mid-tier line between), but a same-project different-type
    # pair is included (unlike cluster_edges' cross-project-only rule), since the
    # mid tier needs edges between type clusters within one project too.
    type_pair_counts: dict[tuple[tuple[int, int], tuple[int, int], str], int] = defaultdict(int)
    for i in range(len(edge_src)):
        b1 = (project_codes[edge_src[i]], type_codes[edge_src[i]])
        b2 = (project_codes[edge_dst[i]], type_codes[edge_dst[i]])
        if b1 == b2:
            continue
        bucket_a, bucket_b = (b1, b2) if b1 <= b2 else (b2, b1)
        type_pair_counts[(bucket_a, bucket_b, link_type_class[edge_type_codes[i]])] += 1
    type_pair_edges = [
        {"a": {"project": bucket_a[0], "type": bucket_a[1]},
         "b": {"project": bucket_b[0], "type": bucket_b[1]},
         "class": cls, "count": n}
        for (bucket_a, bucket_b, cls), n in type_pair_counts.items()
    ]

    return encode_snapshot(
        object_ids=object_ids, x=xs, y=ys, type_code=type_codes,
        project_code=project_codes, weight=weights, status_flag=statuses,
        edge_src=edge_src, edge_dst=edge_dst, edge_type_code=edge_type_codes,
        edge_weight=edge_weights,
        types=types, projects=projects, edge_types=edge_types,
        link_type_class=link_type_class, labels=labels, created_at=created_ats,
        watermark=watermark,
        project_aggregates=project_aggregates, type_aggregates=type_aggregates,
        cluster_edges=cluster_edges, type_pair_edges=type_pair_edges,
        community_code=community_codes, communities=communities,
    )


async def outbox_watermark(pool: asyncpg.Pool) -> int:
    """The current tip of the outbox, right now: the same read `fetch_snapshot` uses
    to stamp a snapshot's own watermark, exposed separately for a client that connects
    to `/graph/stream/deltas` with no cursor of its own. Resolve to
    this, never 0, so a fresh connection with no prior snapshot starts listening from
    now instead of replaying the whole outbox."""
    return int(await pool.fetchval("SELECT COALESCE(max(id), 0) FROM outbox"))


async def resolve_deltas_start_cursor(
    pool: asyncpg.Pool, *, since: int | None, last_event_id: str | None,
) -> int:
    """THE DELTA CURSOR FIX: where a fresh
    `/graph/stream/deltas` connection starts, in order: `since` (a client passing
    /graph/stream's own watermark), then the standard SSE `Last-Event-ID` reconnect
    header, then, and only then, `outbox_watermark`, i.e. "now," never the backlog.
    Pulled out of the route itself so this decision is directly unit-testable without
    standing up a real SSE connection."""
    if since is not None:
        return since
    if last_event_id is not None:
        try:
            return int(last_event_id)
        except ValueError:
            pass
    return await outbox_watermark(pool)


_DELTAS_PAGE_LIMIT = 500


async def deltas_since(
    pool: asyncpg.Pool, cursor: int, *, limit: int = _DELTAS_PAGE_LIMIT,
) -> tuple[list[dict[str, Any]], int]:
    """Poll the outbox table for graph-relevant events past `cursor` (an outbox id):
    the same poll-and-diff idiom /cases/{id}/stream, /console/stream and /pane/{id}/
    stream already use (app.py), never a new push mechanism. Returns
    `(deltas, new_cursor)`; an empty list with an unchanged cursor means nothing
    graph-relevant happened since the last poll. Each delta is keyed by object id (a
    string), see the module docstring for why, not array index.

    THE DELTA CURSOR FIX: one page at a time
    (`limit`, never the whole backlog), coalesced to the latest event per object
    within that page (a live-state view only cares about an object's current
    position/status, not every intermediate event it passed through), positions
    resolved for the whole page with plain joins instead of two correlated subqueries
    per row. `new_cursor` advances to the highest outbox id actually seen in the page,
    including rows coalesced away, so nothing already read is ever re-scanned,
    even though only the survivor per object is reported.

    THE OUTBOX GAP: 'layout_moved' (graph_layout._LAYOUT_MOVED_EVENT)
    joins the type list here for exactly this reason and needs no new branch below --
    every type other than 'object_merged' already resolves to op='moved' and gets the
    same graph_x/graph_y join, which is exactly what a freshly-positioned object needs."""
    rows = await pool.fetch(
        "WITH raw AS ("
        "  SELECT o.id AS outbox_id, o.object_id, o.event_type "
        "  FROM outbox o "
        "  WHERE o.id > $1 AND o.event_type IN "
        "    ('object_created','property_added','object_merged','layout_moved') "
        "    AND o.object_id IS NOT NULL "
        "  ORDER BY o.id ASC "
        "  LIMIT $2"
        ") "
        "SELECT DISTINCT ON (r.object_id) "
        "  r.object_id, r.outbox_id, r.event_type, "
        "  (gx.value #>> '{}')::float8 AS x, (gy.value #>> '{}')::float8 AS y, "
        "  max(r.outbox_id) OVER () AS page_max "
        "FROM raw r "
        "LEFT JOIN current_assertions gx ON gx.object_id=r.object_id AND gx.name='graph_x' "
        "LEFT JOIN current_assertions gy ON gy.object_id=r.object_id AND gy.name='graph_y' "
        "ORDER BY r.object_id, r.outbox_id DESC",
        cursor, limit)
    if not rows:
        return [], cursor
    new_cursor = max(int(r["page_max"]) for r in rows)
    deltas: list[dict[str, Any]] = []
    for r in rows:
        op = "retired" if r["event_type"] == "object_merged" else "moved"
        delta: dict[str, Any] = {"op": op, "id": str(r["object_id"])}
        if r["x"] is not None and r["y"] is not None:
            delta["x"] = float(r["x"])
            delta["y"] = float(r["y"])
        deltas.append(delta)
    return deltas, new_cursor
