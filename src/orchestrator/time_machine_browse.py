"""TIME MACHINE, stage 2: read-only browsing of a past moment.

Everything here takes an asyncpg connection or pool pointed at a throwaway (the read-only
`tm_reader` role from `time_machine.start`) and runs FIXED statements with bound
parameters only: nothing a caller types ever becomes SQL text. The row shapes are the ones a
console renders, plain JSON-able dicts:

  type_counts()      -> [{type, count}]
  list_objects()     -> {rows: [{id, short_id, type, canonical, status, title, created_at}],
                         total, limit, offset}
  search_objects()   -> same shape as list_objects, plus the matched `name` of each hit
  object_detail()    -> {object, assertions: [{id, name, value, source_id, confidence,
                         observed_at, supersedes, current}], links_out, links_in}
                        or {ambiguous: [...]} or None
  transcript_sessions() / transcript_lines() -> the stored sessions, decrypted at read time
                        with the box's own key (the key never enters the throwaway)

A past moment's "current" value of a property is chosen the way every live reader chooses
it (confidence, then newest observation, then the later write), see current_order."""
from __future__ import annotations

import json
import re
from typing import Any

import asyncpg

PAGE_MAX = 200
VALUE_TEXT_MAX = 4000
LINKS_MAX = 200
# The properties a title and a text search read, in the order a title prefers them.
TITLE_NAMES = ("summary", "name", "handle", "description")
SEARCH_NAMES = ("summary", "rationale", "name", "handle", "description")

_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
                      r"[0-9a-fA-F]{12}$")
_HEX_PREFIX_RE = re.compile(r"^[0-9a-fA-F]{6,32}$")

def _title_sql(n: int) -> str:
    """The title subquery, reading its title-name list from bound parameter $n."""
    return _TITLE_SQL.replace("$T", f"${n}")


_TITLE_SQL = """
    (SELECT left(a.value #>> '{}', 240)
       FROM current_assertions a
      WHERE a.object_id = o.id AND a.name = ANY($T::text[])
        AND jsonb_typeof(a.value) = 'string'
      ORDER BY array_position($T::text[], a.name), a.confidence DESC, a.observed_at DESC,
               a.id DESC
      LIMIT 1)"""


def _clamp(limit: int, offset: int) -> tuple[int, int]:
    return max(1, min(int(limit), PAGE_MAX)), max(0, int(offset))


def _object_row(r: asyncpg.Record) -> dict[str, Any]:
    d = {
        "id": str(r["id"]), "short_id": str(r["id"])[:8], "type": r["type"],
        "canonical": r["canonical"], "status": r["status"], "title": r["title"],
        "created_at": r["created_at"].isoformat(),
    }
    if "matched" in r:
        d["matched"] = r["matched"]
    return d


async def type_counts(conn: Any) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        "SELECT type, count(*) AS n FROM objects GROUP BY type ORDER BY n DESC, type")
    return [{"type": r["type"], "count": int(r["n"])} for r in rows]


async def list_objects(conn: Any, *, type: str | None = None, limit: int = 50,
                       offset: int = 0) -> dict[str, Any]:
    """Newest first. `type` is an exact object type (Thread, Decision, Agent...)."""
    limit, offset = _clamp(limit, offset)
    total = await conn.fetchval(
        "SELECT count(*) FROM objects o WHERE ($1::text IS NULL OR o.type = $1)", type)
    rows = await conn.fetch(
        f"SELECT o.id, o.type, o.canonical, o.status, o.created_at, {_title_sql(1)} AS title "
        "FROM objects o WHERE ($2::text IS NULL OR o.type = $2) "
        "ORDER BY o.created_at DESC, o.id LIMIT $3 OFFSET $4",
        list(TITLE_NAMES), type, limit, offset)
    return {"rows": [_object_row(r) for r in rows], "total": int(total),
            "limit": limit, "offset": offset}


def _like(text: str) -> str:
    """A bound LIKE pattern that matches the text literally (wildcards escaped)."""
    return "%" + text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"


async def search_objects(conn: Any, query: str, *, type: str | None = None, limit: int = 50,
                         offset: int = 0) -> dict[str, Any]:
    """Objects whose canonical, or a current text property, contains `query` (case
    blind, literal). An empty query is an empty result, never the whole graph."""
    query = (query or "").strip()
    limit, offset = _clamp(limit, offset)
    if not query:
        return {"rows": [], "total": 0, "limit": limit, "offset": offset}
    pat = _like(query)
    hits = """
        WITH hits AS (
            SELECT o.id, 'canonical'::text AS matched FROM objects o
             WHERE o.canonical ILIKE $1 AND ($2::text IS NULL OR o.type = $2)
            UNION
            SELECT a.object_id, a.name FROM current_assertions a
              JOIN objects o ON o.id = a.object_id
             WHERE a.name = ANY($3::text[]) AND jsonb_typeof(a.value) = 'string'
               AND (a.value #>> '{}') ILIKE $1 AND ($2::text IS NULL OR o.type = $2)
        ), one AS (
            SELECT DISTINCT ON (id) id, matched FROM hits ORDER BY id, matched
        )"""
    total = await conn.fetchval(hits + " SELECT count(*) FROM one", pat, type,
                                list(SEARCH_NAMES))
    rows = await conn.fetch(
        hits + f" SELECT o.id, o.type, o.canonical, o.status, o.created_at, one.matched, "
        f"{_title_sql(4)} AS title FROM one JOIN objects o ON o.id = one.id "
        "ORDER BY o.created_at DESC, o.id LIMIT $5 OFFSET $6",
        pat, type, list(SEARCH_NAMES), list(TITLE_NAMES), limit, offset)
    return {"rows": [_object_row(r) for r in rows], "total": int(total or 0),
            "limit": limit, "offset": offset}


async def resolve_ref(conn: Any, ref: str) -> list[asyncpg.Record]:
    """A full uuid, a hex id prefix (6+ chars), or an exact canonical: the matching
    objects (usually one; several means the prefix or canonical was ambiguous)."""
    ref = (ref or "").strip()
    if _UUID_RE.match(ref):
        return list(await conn.fetch(
            "SELECT id, type, canonical, status, merged_into, created_at FROM objects "
            "WHERE id = $1::uuid", ref))
    if _HEX_PREFIX_RE.match(ref):
        rows = await conn.fetch(
            "SELECT id, type, canonical, status, merged_into, created_at FROM objects "
            "WHERE id::text LIKE $1 ORDER BY created_at LIMIT 10", ref.lower() + "%")
        if rows:
            return list(rows)
    if not ref:
        return []
    return list(await conn.fetch(
        "SELECT id, type, canonical, status, merged_into, created_at FROM objects "
        "WHERE canonical = $1 ORDER BY created_at LIMIT 10", ref))


def _value(raw: Any) -> Any:
    try:
        return json.loads(raw) if isinstance(raw, str) else raw
    except ValueError:
        return raw


def text_capped(value: Any) -> Any:
    if isinstance(value, str) and len(value) > VALUE_TEXT_MAX:
        return value[:VALUE_TEXT_MAX] + f"… [{len(value) - VALUE_TEXT_MAX} more characters]"
    return value


async def object_detail(conn: Any, ref: str) -> dict[str, Any] | None:
    """One object at that moment: its row, every assertion (current ones flagged, the
    superseded chain kept), and its links both ways with the other end named."""
    found = await resolve_ref(conn, ref)
    if not found:
        return None
    if len(found) > 1:
        return {"ambiguous": [{"id": str(r["id"]), "type": r["type"],
                               "canonical": r["canonical"]} for r in found]}
    o = found[0]
    oid = o["id"]
    current = {int(r["id"]) for r in await conn.fetch(
        "SELECT id FROM current_assertions WHERE object_id = $1", oid)}
    arows = await conn.fetch(
        "SELECT id, name, value, source_id, confidence, observed_at, supersedes, created_at "
        "FROM assertions WHERE object_id = $1 ORDER BY id", oid)
    assertions = [{
        "id": int(a["id"]), "name": a["name"], "value": text_capped(_value(a["value"])),
        "source_id": a["source_id"], "confidence": float(a["confidence"]),
        "observed_at": a["observed_at"].isoformat(),
        "supersedes": int(a["supersedes"]) if a["supersedes"] is not None else None,
        "current": int(a["id"]) in current,
    } for a in arows]
    link_sql = (
        "SELECT l.id, l.type, l.properties, l.source_id, l.confidence, l.valid_until, "
        "o2.id AS other_id, o2.type AS other_type, o2.canonical AS other_canonical "
        "FROM links l JOIN objects o2 ON o2.id = l.{other} WHERE l.{mine} = $1 "
        "ORDER BY l.id LIMIT $2")

    def link_row(r: asyncpg.Record) -> dict[str, Any]:
        return {
            "id": int(r["id"]), "type": r["type"], "properties": _value(r["properties"]),
            "source_id": r["source_id"], "confidence": float(r["confidence"]),
            "active": r["valid_until"] is None,
            "valid_until": r["valid_until"].isoformat() if r["valid_until"] else None,
            "other": {"id": str(r["other_id"]), "type": r["other_type"],
                      "canonical": r["other_canonical"]},
        }

    out = await conn.fetch(link_sql.format(other="to_id", mine="from_id"), oid, LINKS_MAX)
    inn = await conn.fetch(link_sql.format(other="from_id", mine="to_id"), oid, LINKS_MAX)
    return {
        "object": {
            "id": str(oid), "type": o["type"], "canonical": o["canonical"],
            "status": o["status"],
            "merged_into": str(o["merged_into"]) if o["merged_into"] else None,
            "created_at": o["created_at"].isoformat(),
        },
        "assertions": assertions,
        "links_out": [link_row(r) for r in out],
        "links_in": [link_row(r) for r in inn],
    }


async def transcript_sessions(conn: Any, *, query: str = "", limit: int = 50,
                              offset: int = 0) -> dict[str, Any]:
    """Stored sessions at that moment, newest ingest first; `query` matches the session id
    or its source path, literally."""
    limit, offset = _clamp(limit, offset)
    pat = _like(query.strip()) if query and query.strip() else None
    total = await conn.fetchval(
        "SELECT count(*) FROM soul_sessions "
        "WHERE $1::text IS NULL OR anchor_sid ILIKE $1 OR source_path ILIKE $1", pat)
    rows = await conn.fetch(
        "SELECT harness, anchor_sid, source_path, first_seen_at, last_ingested_at, "
        "last_line_idx FROM soul_sessions "
        "WHERE $1::text IS NULL OR anchor_sid ILIKE $1 OR source_path ILIKE $1 "
        "ORDER BY last_ingested_at DESC, anchor_sid LIMIT $2 OFFSET $3", pat, limit, offset)
    return {"rows": [{
        "harness": r["harness"], "anchor_sid": r["anchor_sid"], "source_path": r["source_path"],
        "first_seen_at": r["first_seen_at"].isoformat(),
        "last_ingested_at": r["last_ingested_at"].isoformat(),
        "lines": int(r["last_line_idx"]),
    } for r in rows], "total": int(total), "limit": limit, "offset": offset}


async def transcript_lines(pool: Any, anchor_sid: str, *, tail: int = 100,
                           harness: str = "claude-code") -> list[str] | None:
    """The last `tail` lines of one stored session, decrypted with the box's own key at
    read time. None when that session is not in the moment. Goes through the same reader
    the live store uses, so hot rows and the cold tier both work."""
    from src.ingest.soul_store import SoulStore

    tail = max(1, min(int(tail), 1000))
    known = await pool.fetchval(
        "SELECT 1 FROM soul_sessions WHERE harness = $1 AND anchor_sid = $2",
        harness, anchor_sid)
    if not known:
        return None
    lines = await SoulStore(pool).tail_lines(anchor_sid, tail, harness)
    return [ln.decode("utf-8", errors="replace") for ln in lines]
