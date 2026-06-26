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
import re
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
# inferred object type per relationship endpoint role — lets us stub an endpoint that
# isn't in the slice (its real entity lives elsewhere in the full dataset, keyed by
# the same id) so the edge still forms and a later ingest can enrich it in place.
_EDGE_TYPES: dict[str, dict[str, str]] = {
    "Ownership": {"owner": "Organization", "asset": "Organization"},
    "Directorship": {"director": "Person", "organization": "Organization"},
    "Family": {"person": "Person", "relative": "Person"},
    "Associate": {"person": "Person", "associate": "Person"},
    "Membership": {"member": "Person", "organization": "Organization"},
    "Employment": {"employer": "Organization", "employee": "Person"},
    "Representation": {"agent": "Person", "client": "Organization"},
    "UnknownLink": {"subject": "Organization", "object": "Organization"},
}
# name is stored scalar (primary) for cross-source matching; email/website/phone are
# the strong identifiers the footprint crawl can collide with.
_PROPS = ("name", "country", "topics", "birthDate", "nationality", "position",
          "email", "website", "phone")

_EVM_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


def _wallet_canonical(addr: str, currency: str | None) -> str:
    """Canonical that ALIGNS with the on-chain tracer (src/ingest/etherscan): an EVM
    address becomes `eth:1:<lower>` regardless of the OFAC currency label (ERC-20
    tokens share Ethereum's address space), so an OFAC-listed wallet and a later
    Etherscan trace of the same address dedupe into ONE object — for free."""
    a = addr.strip()
    if _EVM_RE.match(a):
        return f"eth:1:{a.lower()}"
    return f"wallet:{(currency or 'crypto').strip().lower()}:{a}"


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
    by_id = {e["id"]: e for e in ents if e.get("id")}
    n_obj = n_prop = n_link = 0

    async def resolve(fid: str, inferred: str | None) -> uuid.UUID | None:
        """Find-or-create the node for an entity id; if it isn't a materialized
        Person/Organization, stub it with the relationship-role-inferred type."""
        if fid in ids:
            return ids[fid]
        ent = by_id.get(fid)
        otype = _TYPE.get((ent or {}).get("schema") or "") if ent else None
        otype = otype or inferred
        if otype is None:
            return None
        oid = await actions.create_or_find_object(otype, fid, _SOURCE, case_id)
        ids[fid] = oid
        return oid

    # pass 1: real entities (Person/Organization) -> objects + properties
    for e in ents:
        fid = e.get("id")
        if not fid or _TYPE.get(e.get("schema") or "") is None:
            continue
        oid = await resolve(fid, None)
        if oid is None:
            continue
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
        # aliases (FtM alias/weakAlias + any secondary names) -> a single list-valued
        # 'alias' assertion so alias-aware screening matches over name ∪ alias.
        aliases = [
            a
            for a in dict.fromkeys(
                list(props.get("alias") or [])
                + list(props.get("weakAlias") or [])
                + list((props.get("name") or [])[1:])
            )
            if a
        ]
        if aliases:
            await actions.assert_property(
                oid, "alias", aliases, _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC
            )
            n_prop += 1

    # pass 1b: crypto wallets -> CryptoAddress nodes keyed on the tracer-aligned
    # canonical, so an OFAC wallet fuses with an on-chain trace of the same address.
    # The FtM `holder` property becomes a controlled_by edge to the sanctioned party.
    for e in ents:
        if (e.get("schema") or "") != "CryptoWallet":
            continue
        fid = e.get("id")
        props = e.get("properties") or {}
        keys = props.get("publicKey") or []
        if not fid or not keys:
            continue
        addr = str(keys[0]).strip()
        currency = (props.get("currency") or [None])[0]
        oid = await actions.create_or_find_object(
            "CryptoAddress", _wallet_canonical(addr, currency), _SOURCE, case_id
        )
        ids[fid] = oid
        n_obj += 1
        stored = addr.lower() if _EVM_RE.match(addr) else addr
        for name, val in (("address", stored), ("currency", currency), ("sanctioned", True)):
            if val not in (None, ""):
                await actions.assert_property(
                    oid, name, val, _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC
                )
                n_prop += 1
        for holder_fid in (props.get("holder") or []):
            h = await resolve(holder_fid, "Person")
            if h is not None:
                await actions.create_link(
                    oid, h, "controlled_by", _SOURCE, ts, _CONF,
                    case_id=case_id, evidence_class=_EC,
                )
                n_link += 1

    # pass 2: relationship-entities -> links; absent endpoints become typed stubs
    for e in ents:
        schema = e.get("schema") or ""
        edge = _EDGES.get(schema)
        if edge is None:
            continue
        sp, tp, ltype = edge
        roles = _EDGE_TYPES.get(schema, {})
        props = e.get("properties") or {}
        srcs = props.get(sp) or []
        tgts = props.get(tp) or []
        if not srcs or not tgts:
            continue
        a = await resolve(srcs[0], roles.get(sp))
        b = await resolve(tgts[0], roles.get(tp))
        if a is None or b is None:
            continue
        await actions.create_link(
            a, b, ltype, _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC
        )
        n_link += 1

    return {"objects": n_obj, "stubs": len(ids) - n_obj, "properties": n_prop, "links": n_link}


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
