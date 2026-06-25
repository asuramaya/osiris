"""Ingest OpenSanctions (FollowTheMoney) into the graph via Actions.

OpenSanctions publishes an open, keyless, self-hostable entity graph of sanctioned
parties, PEPs and their relationships in the FollowTheMoney (FtM) format — exactly
the kind of pre-normalized public base Osiris should federate rather than crawl.
This maps a FtM stream into the graph: an entity schema -> object type, its
properties -> assertions, and relationship-schema entities (Ownership, Directorship,
Family, ...) -> links between the entities they reference. Everything lands as
AUTHORITATIVE_API (a published authoritative dataset).

    uv run python -m src.ingest.opensanctions <url|file.ftm.json> [limit] [case_id]
"""

from __future__ import annotations

import asyncio
import json
import sys
import uuid
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from src.actions.core import Actions
from src.config.settings import get_settings
from src.db.pool import create_pool
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_SOURCE = "opensanctions"
_CONF = confidence_for(EvidenceClass.AUTHORITATIVE_API)
_EC = EvidenceClass.AUTHORITATIVE_API.value

# FtM entity schema -> Osiris object type (nodes we materialize)
_TYPE: dict[str, str] = {
    "Person": "Person",
    "Company": "Organization",
    "Organization": "Organization",
    "LegalEntity": "Organization",
    "PublicBody": "Organization",
}
# FtM relationship schema -> (source property, target property, link type)
_EDGES: dict[str, tuple[str, str, str]] = {
    "Ownership": ("owner", "asset", "owns"),
    "Directorship": ("director", "organization", "directs"),
    "Family": ("person", "relative", "family"),
    "Associate": ("person", "associate", "associate_of"),
    "Membership": ("member", "organization", "member_of"),
    "Employment": ("employer", "employee", "employs"),
    "Representation": ("agent", "client", "represents"),
    "UnknownLink": ("subject", "object", "linked_to"),
}
# name is stored scalar (primary) for cross-source matching; email/website/phone are
# the strong identifiers the footprint crawl can collide with.
_PROPS = ("name", "country", "topics", "birthDate", "nationality", "position",
          "email", "website", "phone")


def parse_jsonl(text: str) -> Iterator[dict[str, Any]]:
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            yield obj


async def ingest_ftm(
    actions: Actions,
    entities: Iterable[dict[str, Any]],
    *,
    case_id: uuid.UUID | None = None,
    observed_at: datetime | None = None,
    limit: int | None = None,
) -> dict[str, int]:
    ts = observed_at or datetime.now(UTC)
    ents = list(entities)
    if limit is not None:
        ents = ents[:limit]
    ids: dict[str, uuid.UUID] = {}
    n_obj = n_prop = n_link = 0

    # pass 1: real entities (Person/Organization) -> objects + properties
    for e in ents:
        fid = e.get("id")
        otype = _TYPE.get(e.get("schema") or "")
        if not fid or otype is None:
            continue
        oid = await actions.create_or_find_object(otype, fid, _SOURCE, case_id)
        ids[fid] = oid
        n_obj += 1
        props = e.get("properties") or {}
        for name in _PROPS:
            vals = props.get(name)
            if not vals:
                continue
            # keep name a scalar (primary) so cross-source matching is a simple join
            value: Any = vals[0] if (name == "name" or len(vals) == 1) else vals
            await actions.assert_property(
                oid, name, value, _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC
            )
            n_prop += 1

    # pass 2: relationship-entities -> links between the entities they reference
    for e in ents:
        edge = _EDGES.get(e.get("schema") or "")
        if edge is None:
            continue
        sp, tp, ltype = edge
        props = e.get("properties") or {}
        srcs = props.get(sp) or []
        tgts = props.get(tp) or []
        if not srcs or not tgts:
            continue
        a = ids.get(srcs[0])
        b = ids.get(tgts[0])
        if a is None or b is None:
            continue  # an endpoint isn't in this slice — skip (like a dangling ref)
        await actions.create_link(
            a, b, ltype, _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC
        )
        n_link += 1

    return {"objects": n_obj, "properties": n_prop, "links": n_link}


async def fetch_ftm(
    url: str, *, limit: int | None = None, timeout_s: float = 60.0
) -> list[dict[str, Any]]:
    """Stream a newline-delimited FtM file, stopping at `limit` entities."""
    out: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
        async with client.stream("GET", url, headers={"User-Agent": "osiris-osint"}) as r:
            r.raise_for_status()
            async for raw in r.aiter_lines():
                line = raw.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(obj, dict):
                    out.append(obj)
                if limit is not None and len(out) >= limit:
                    break
    return out


def main() -> None:
    src = sys.argv[1] if len(sys.argv) > 1 else ""
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else None
    case_id = uuid.UUID(sys.argv[3]) if len(sys.argv) > 3 else None
    if not src:
        print("usage: python -m src.ingest.opensanctions <url|file> [limit] [case_id]")
        return

    local = None if src.startswith("http") else list(parse_jsonl(Path(src).read_text()))

    async def run() -> None:
        pool = await create_pool(get_settings().database_url)
        try:
            ents = await fetch_ftm(src, limit=limit) if local is None else local
            counts = await ingest_ftm(Actions(pool), ents, case_id=case_id, limit=limit)
            print(f"ingested: {counts}")
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":
    main()
