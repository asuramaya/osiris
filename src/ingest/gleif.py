"""Ingest GLEIF — the global Legal Entity Identifier registry (keyless).

The LEI is the closest thing to a global primary key for legal entities: a single
20-char code per company, issued under ISO 17442, covering ~2.7M entities worldwide.
GLEIF's API is open and keyless. Two payoffs for follow-the-money work:

  * a DETERMINISTIC cross-base key — two objects carrying the same `lei` are the same
    entity, full stop (no name-normalization guesswork; solves the acronym problem for
    any entity that has an LEI);
  * ownership STRUCTURE — GLEIF's Level-2 data exposes each entity's direct and
    ultimate parent, so a subsidiary resolves up to who controls it.

This mints an Organization per LEI (canonical `lei:<LEI>`) with jurisdiction / status /
country and the LEI as a queryable property, plus `subsidiary_of` links to its parents.

    uv run python -m src.ingest.gleif "<entity name>" [case_id]
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx

from src.actions.core import Actions
from src.config.settings import get_settings
from src.db.pool import create_pool
from src.ontology.resolution import normalize_org_name
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_SOURCE = "gleif"
_EC = EvidenceClass.AUTHORITATIVE_API.value
_CONF = confidence_for(EvidenceClass.AUTHORITATIVE_API)
_API = "https://api.gleif.org/api/v1/lei-records"
_UA = {"User-Agent": "osiris-osint", "Accept": "application/vnd.api+json"}


def parse_lei_record(rec: dict[str, Any]) -> dict[str, Any]:
    """Pull the identity fields from one GLEIF lei-record."""
    at = rec.get("attributes") or {}
    ent = at.get("entity") or {}
    la = ent.get("legalAddress") or {}
    return {
        "lei": at.get("lei"),
        "name": (ent.get("legalName") or {}).get("name"),
        "jurisdiction": ent.get("jurisdiction"),
        "status": ent.get("status"),                       # ACTIVE / INACTIVE
        "registration_status": (at.get("registration") or {}).get("status"),  # ISSUED / LAPSED
        "country": la.get("country"),
        "city": la.get("city"),
        "parents": [],   # filled by fetch_parents
    }


async def search_lei(
    name: str, *, limit: int = 10, timeout_s: float = 30.0
) -> list[dict[str, Any]]:
    """Search GLEIF by legal name and keep the PRECISE matches. GLEIF's name filter is
    fuzzy ('Anthropic' returns ETFs like 'ProShares Ultra Anthropic'), so we post-filter
    to records whose normalized legal name equals the normalized query — the registry
    entity, not derivatives that merely reference it. Falls back to the single best hit
    if nothing matches exactly."""
    want = normalize_org_name(name)
    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True, headers=_UA) as client:
        r = await client.get(
            _API, params={"filter[entity.legalName]": name, "page[size]": "25"}
        )
        r.raise_for_status()
        data = r.json().get("data") or []
        parsed = [p for p in (parse_lei_record(x) for x in data) if p["lei"] and p["name"]]
        precise = [p for p in parsed if normalize_org_name(p["name"]) == want]
        records = (precise or parsed[:1])[:limit]
        for rec in records:
            rec["parents"] = await _fetch_parents(client, rec["lei"])
    return records


async def _fetch_parents(client: httpx.AsyncClient, lei: str) -> list[dict[str, Any]]:
    """The direct + ultimate parent LEI records (Level-2 ownership), if any."""
    out: list[dict[str, Any]] = []
    for kind in ("direct-parent", "ultimate-parent"):
        try:
            r = await client.get(f"{_API}/{lei}/{kind}")
            if r.status_code != 200:
                continue
            d = r.json().get("data")
            if not d:
                continue
            p = parse_lei_record(d)
            if p["lei"] and p["lei"] != lei:
                out.append({"lei": p["lei"], "name": p["name"], "kind": kind})
        except httpx.HTTPError:
            continue
    return out


async def ingest_lei(
    actions: Actions,
    records: list[dict[str, Any]],
    *,
    case_id: uuid.UUID | None = None,
    observed_at: datetime | None = None,
) -> dict[str, int]:
    """Materialize one Organization per LEI + `subsidiary_of` links to parents."""
    ts = observed_at or datetime.now(UTC)
    n_obj = n_prop = n_link = 0
    for p in records:
        if not p.get("lei") or not p.get("name"):
            continue
        oid = await actions.create_or_find_object(
            "Organization", f"lei:{p['lei']}", _SOURCE, case_id
        )
        n_obj += 1
        for name, val in (
            ("name", p.get("name")), ("lei", p.get("lei")),
            ("jurisdiction", p.get("jurisdiction")), ("status", p.get("status")),
            ("registration_status", p.get("registration_status")),
            ("country", p.get("country")),
        ):
            if val:
                await actions.assert_property(
                    oid, name, val, _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC
                )
                n_prop += 1
        for par in p.get("parents") or []:
            if not par.get("lei"):
                continue
            par_id = await actions.create_or_find_object(
                "Organization", f"lei:{par['lei']}", _SOURCE, case_id
            )
            if par.get("name"):
                await actions.assert_property(
                    par_id, "name", par["name"], _SOURCE, ts, _CONF,
                    case_id=case_id, evidence_class=_EC,
                )
            await actions.assert_property(
                par_id, "lei", par["lei"], _SOURCE, ts, _CONF,
                case_id=case_id, evidence_class=_EC,
            )
            rel = "ultimate_parent" if par["kind"] == "ultimate-parent" else "subsidiary_of"
            await actions.create_link(
                oid, par_id, rel, _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC
            )
            n_link += 1
    return {"objects": n_obj, "properties": n_prop, "links": n_link}


async def aim_gleif(
    actions: Actions, name: str, *, limit: int = 10, case_id: uuid.UUID | None = None
) -> dict[str, int]:
    """Search GLEIF for a name and ingest the matches with their ownership parents."""
    return await ingest_lei(actions, await search_lei(name, limit=limit), case_id=case_id)


def main() -> None:
    argv = sys.argv[1:]
    if not argv:
        print('usage: python -m src.ingest.gleif "<entity name>" [case_id]')
        return
    name = argv[0]
    case_id = uuid.UUID(argv[1]) if len(argv) > 1 else None

    async def run() -> None:
        pool = await create_pool(get_settings().database_url)
        try:
            print(f"ingested: {await aim_gleif(Actions(pool), name, case_id=case_id)}")
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":
    main()
