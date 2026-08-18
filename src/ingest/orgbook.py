"""Ingest OrgBook BC — British Columbia's open corporate registry (keyless).

US-only entity verification (EDGAR) is a real gap for a follow-the-money tool: Canada
is a major jurisdiction, and Vancouver in particular is a hub for pooled private-capital
vehicles. OrgBook BC (orgbook.gov.bc.ca) is the BC government's open, keyless API over
the provincial corporate registry — every registered company, partnership, and society
with its registration number, CRA business number, type, status, and home jurisdiction.

This mints an Organization per registration (canonical = the BC registration number,
the registry's stable key), all AUTHORITATIVE_API. Because the resulting nodes are
Organizations, the existing cross-base resolver buckets them by normalized name — so a
BC-registered entity and its EDGAR counterpart fuse on the shared name where both exist.

What OrgBook does NOT carry: directors/officers and beneficial owners (those live behind
the paid BC Registry corporate search and the non-public transparency register). So this
verifies *registration and legal existence*, not control — the honest keyless ceiling.

    uv run python -m src.ingest.orgbook "<entity or family name>" [case_id]
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
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_SOURCE = "orgbook"
_EC = EvidenceClass.AUTHORITATIVE_API.value
_CONF = confidence_for(EvidenceClass.AUTHORITATIVE_API)
_API = "https://orgbook.gov.bc.ca/api/v4/search/topic"
_UA = {"User-Agent": "osiris-osint"}
# BC registry status codes -> readable.
_STATUS = {"ACT": "active", "HIS": "historical", "HLD": "hold", "LIQ": "liquidation"}


def parse_topic(t: dict[str, Any]) -> dict[str, Any]:
    """Pull the registration record from an OrgBook topic."""
    names = t.get("names") or []
    attrs = {a.get("type"): a.get("value") for a in (t.get("attributes") or [])}

    def _name(kind: str) -> str | None:
        return next((n["text"] for n in names if n.get("type") == kind and n.get("text")), None)

    legal = _name("entity_name")
    bn = _name("business_number")
    status = attrs.get("entity_status")
    return {
        "source_id": t.get("source_id"),
        "name": legal,
        "business_number": bn,
        "entity_type": attrs.get("entity_type"),
        "status": _STATUS.get(status or "", status),
        "jurisdiction": attrs.get("home_jurisdiction") or attrs.get("registered_jurisdiction"),
        "registration_date": (attrs.get("registration_date") or "")[:10] or None,
    }


async def search_topics(
    query: str, *, limit: int = 50, timeout_s: float = 30.0
) -> list[dict[str, Any]]:
    """Search the BC registry by name; returns parsed registration records."""
    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True, headers=_UA) as client:
        r = await client.get(_API, params={"q": query})
        r.raise_for_status()
        data = r.json()
    out = [parse_topic(t) for t in (data.get("results") or [])[:limit]]
    return [p for p in out if p["source_id"] and p["name"]]


async def ingest_topics(
    actions: Actions,
    topics: list[dict[str, Any]],
    *,
    case_id: uuid.UUID | None = None,
    observed_at: datetime | None = None,
) -> dict[str, int]:
    """Materialize one Organization per BC registration."""
    ts = observed_at or datetime.now(UTC)
    n_obj = n_prop = 0
    for p in topics:
        if not p["source_id"] or not p["name"]:
            continue
        oid = await actions.create_or_find_object(
            "Organization", f"bc-reg:{p['source_id']}", _SOURCE, case_id
        )
        n_obj += 1
        for name in ("name", "business_number", "entity_type", "status",
                     "jurisdiction", "registration_date"):
            if p.get(name):
                await actions.assert_property(
                    oid, name if name != "status" else "registration_status",
                    p[name], _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC,
                )
                n_prop += 1
        # the BC registration number itself, as a queryable property
        await actions.assert_property(
            oid, "bc_registration", p["source_id"], _SOURCE, ts, _CONF,
            case_id=case_id, evidence_class=_EC,
        )
        n_prop += 1
    return {"objects": n_obj, "properties": n_prop}


async def aim_orgbook(
    actions: Actions, name: str, *, case_id: uuid.UUID | None = None
) -> dict[str, int]:
    """Search the BC registry for a name (or family) and ingest the matches. A family
    name like 'Brilliant Phoenix' pulls the whole corporate group in one call."""
    return await ingest_topics(actions, await search_topics(name), case_id=case_id)


def main() -> None:
    argv = sys.argv[1:]
    if not argv:
        print('usage: python -m src.ingest.orgbook "<entity or family name>" [case_id]')
        return
    name = argv[0]
    case_id = uuid.UUID(argv[1]) if len(argv) > 1 else None

    async def run() -> None:
        pool = await create_pool(
            get_settings().database_url, application_name="osiris-script:ingest-orgbook")
        try:
            print(f"ingested: {await aim_orgbook(Actions(pool), name, case_id=case_id)}")
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":
    main()
