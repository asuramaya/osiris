"""Ingest SEC EDGAR Form D (private-placement) filings — the buried-but-public layer.

The company_tickers loader (edgar.py) only sees PUBLIC companies. But private
companies that raise capital file **Form D**, which names their executive officers /
directors, the amount raised, the investor count, and the issuer's address — facts
that are public yet aggregated nowhere. Worse (better, for OSINT): a swarm of feeder
SPVs file their own Form Ds to repackage access to a hot private company, exposing a
financing structure no one connects.

This federates that layer: EDGAR full-text search (keyless) resolves a name to its
Form D filings; each filing's primary_doc.xml becomes an Organization (the issuer)
carrying offering facts, plus Person officers/directors linked to it. Cross-base
resolution then fuses the Form D issuer with the same company from Wikidata.

    uv run python -m src.ingest.edgar_formd <name>
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from datetime import UTC, datetime
from typing import Any
from xml.etree import ElementTree as ET

import httpx

from src.actions.core import Actions
from src.config.settings import get_settings
from src.db.pool import create_pool
from src.ontology.resolution import normalize_org_name
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

# the feeder link is INFERRED from the SPV's name referencing the core company —
# speculative, so the frontier won't crawl outward from it.
_DERIVED = EvidenceClass.DERIVED.value
_DERIVED_CONF = confidence_for(EvidenceClass.DERIVED)

_SOURCE = "edgar"
_EC = EvidenceClass.AUTHORITATIVE_API.value
_CONF = confidence_for(EvidenceClass.AUTHORITATIVE_API)
# SEC fair-access wants a descriptive contact UA (it 403s browser UAs).
_UA = {"User-Agent": "osiris-osint operator@example.com"}
_EFTS = "https://efts.sec.gov/LATEST/search-index"
_ARCHIVES = "https://www.sec.gov/Archives/edgar/data"

# Form D relationship -> link type (issuer -> person)
_REL = {"Executive Officer": "officer", "Director": "director", "Promoter": "promoter"}


def _txt(el: ET.Element | None, path: str) -> str | None:
    if el is None:
        return None
    found = el.findtext(path)
    return found.strip() if found and found.strip() else None


def parse_form_d(xml: str) -> dict[str, Any]:
    """Parse a Form D primary_doc.xml into issuer + related persons + offering facts."""
    root = ET.fromstring(xml)
    pi = root.find(".//primaryIssuer")
    issuer = _txt(pi, "entityName")
    cik = _txt(pi, "cik")

    persons: list[dict[str, Any]] = []
    for rp in root.findall(".//relatedPersonInfo"):
        first = _txt(rp, ".//firstName") or ""
        last = _txt(rp, ".//lastName") or ""
        name = f"{first} {last}".strip()
        rels = [e.text.strip() for e in rp.findall(".//relationship") if e.text and e.text.strip()]
        if name:
            persons.append({
                "name": name,
                "relationships": rels,
                "city": _txt(rp, ".//city"),
                "state": _txt(rp, ".//stateOrCountry"),
            })

    od = root.find(".//offeringData")
    offering = {
        "offering_amount": _txt(od, ".//totalOfferingAmount"),
        "amount_raised": _txt(od, ".//totalAmountSold"),
        "min_investment": _txt(od, ".//minimumInvestmentAccepted"),
        "investors": _txt(od, ".//totalNumberAlreadyInvested"),
        "first_sale": _txt(od, ".//dateOfFirstSale/value"),
    }
    return {
        "issuer": issuer,
        "cik": cik,
        "state": _txt(pi, ".//stateOrCountry"),
        "persons": persons,
        "offering": offering,
    }


async def ingest_form_d(
    actions: Actions,
    parsed: dict[str, Any],
    *,
    case_id: uuid.UUID | None = None,
    observed_at: datetime | None = None,
) -> dict[str, int]:
    """Materialize a parsed Form D: the issuer Organization (canonical cik:NNN, the
    same scheme as edgar.py, so it resolves cross-base to Wikidata), its offering
    facts as properties, and its related persons linked by role."""
    ts = observed_at or datetime.now(UTC)
    cik = parsed.get("cik")
    if not cik:
        return {"issuers": 0, "persons": 0, "links": 0, "properties": 0}
    issuer_id = await actions.create_or_find_object(
        "Organization", f"cik:{int(cik):010d}", _SOURCE, case_id
    )
    n_prop = n_person = n_link = 0

    async def put(name: str, value: Any) -> None:
        nonlocal n_prop
        if value:
            await actions.assert_property(
                issuer_id, name, value, _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC
            )
            n_prop += 1

    await put("name", parsed.get("issuer"))
    await put("incorporation_state", parsed.get("state"))
    for k, v in (parsed.get("offering") or {}).items():
        await put(k, v)

    for p in parsed.get("persons") or []:
        key = "sec-person:" + p["name"].strip().lower()
        person_id = await actions.create_or_find_object("Person", key, _SOURCE, case_id)
        n_person += 1
        await actions.assert_property(
            person_id, "name", p["name"], _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC
        )
        if p.get("city") or p.get("state"):
            loc = ", ".join(x for x in (p.get("city"), p.get("state")) if x)
            await actions.assert_property(
                person_id, "location", loc, _SOURCE, ts, _CONF,
                case_id=case_id, evidence_class=_EC,
            )
        for rel in p.get("relationships") or ["officer"]:
            await actions.create_link(
                issuer_id, person_id, _REL.get(rel, "officer"), _SOURCE, ts, _CONF,
                case_id=case_id, evidence_class=_EC,
            )
            n_link += 1
    return {"issuers": 1, "persons": n_person, "links": n_link, "properties": n_prop}


async def search_form_d(name: str, *, limit: int = 40) -> list[dict[str, str]]:
    """EDGAR full-text search (keyless) -> Form D filings whose issuer name matches."""
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    async with httpx.AsyncClient(timeout=30.0, headers=_UA, follow_redirects=True) as client:
        r = await client.get(
            _EFTS, params={"q": f'"{name}"', "forms": "D"}
        )
        r.raise_for_status()
        for hit in (r.json().get("hits", {}).get("hits", []))[:limit]:
            src = hit.get("_source", {})
            ciks = src.get("ciks", [])
            names = src.get("display_names", [])
            acc = (hit.get("_id") or "").split(":")[0]
            for cik, disp in zip(ciks, names, strict=False):
                if name.lower() in disp.lower() and cik not in seen:
                    seen.add(cik)
                    out.append({"cik": cik, "accession": acc, "issuer": disp})
    return out


async def fetch_form_d(cik: str, accession: str) -> dict[str, Any]:
    url = f"{_ARCHIVES}/{int(cik)}/{accession.replace('-', '')}/primary_doc.xml"
    async with httpx.AsyncClient(timeout=30.0, headers=_UA, follow_redirects=True) as client:
        r = await client.get(url)
        r.raise_for_status()
        return parse_form_d(r.text)


async def link_funnel(
    actions: Actions, name: str, *, observed_at: datetime | None = None
) -> dict[str, Any]:
    """Wire the financing funnel: a feeder SPV encodes its target in its NAME ("MAV
    Neuralink, LP" -> Neuralink), so we link every org whose normalized name *contains*
    the core company's name to the core via a `raises_for` edge. The core is the
    token-matching org with the largest amount_raised (the operating company, e.g.
    Neuralink Corp.'s $280M vs an SPV's $1M); its same-named cross-base duplicates are
    skipped. The edge is DERIVED (name-inferred), so it never spawns a crawl."""
    pool = actions.pool
    ts = observed_at or datetime.now(UTC)
    target = normalize_org_name(name)
    if len(target) < 4:
        return {"core": None, "spv_links": 0}

    rows = await pool.fetch(
        "SELECT o.id, a.value #>> '{}' AS nm, "
        "  (SELECT value #>> '{}' FROM current_assertions x "
        "   WHERE x.object_id=o.id AND x.name='amount_raised') AS raised "
        "FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id AND o.type='Organization' AND o.status='active' "
        "WHERE a.name='name'"
    )
    matches = [
        (r["id"], normalize_org_name(r["nm"] or ""), r["nm"], r["raised"])
        for r in rows
        if target in normalize_org_name(r["nm"] or "")
    ]
    if not matches:
        return {"core": None, "spv_links": 0}

    def raised_val(raised: str | None) -> int:
        try:
            return int(raised) if raised else -1
        except ValueError:
            return -1

    core = max(matches, key=lambda m: (raised_val(m[3]), m[1] == target))
    core_id, core_norm, core_name = core[0], core[1], core[2]

    n_link = 0
    for mid, mnorm, _nm, _raised in matches:
        if mid == core_id or mnorm == core_norm:  # skip the core + its cross-base dups
            continue
        if await pool.fetchval(
            "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='raises_for' LIMIT 1",
            mid, core_id,
        ):
            continue
        await actions.create_link(
            mid, core_id, "raises_for", "funnel", ts, _DERIVED_CONF, evidence_class=_DERIVED
        )
        n_link += 1
    return {"core": core_name, "spv_links": n_link}


async def aim_form_d(actions: Actions, name: str, *, limit: int = 40) -> dict[str, Any]:
    """Resolve a name to its Form D filings and ingest each — the private-financing
    layer of 'aim Osiris at <name>'."""
    filings = await search_form_d(name, limit=limit)
    totals: dict[str, Any] = {
        "filings": 0, "issuers": 0, "persons": 0, "links": 0, "properties": 0
    }
    for f in filings:
        try:
            parsed = await fetch_form_d(f["cik"], f["accession"])
        except (httpx.HTTPError, ET.ParseError):
            continue
        counts = await ingest_form_d(actions, parsed)
        totals["filings"] += 1
        for k in ("issuers", "persons", "links", "properties"):
            totals[k] += counts[k]
    funnel = await link_funnel(actions, name)
    totals["funnel_core"] = funnel["core"]
    totals["funnel_links"] = funnel["spv_links"]
    return totals


def main() -> None:
    name = " ".join(sys.argv[1:])
    if not name:
        print("usage: python -m src.ingest.edgar_formd <name>")
        return

    async def run() -> None:
        pool = await create_pool(get_settings().database_url)
        try:
            print(f"ingested: {await aim_form_d(Actions(pool), name)}")
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":
    main()
