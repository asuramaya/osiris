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

_SOURCE = "edgar"
_EC = EvidenceClass.AUTHORITATIVE_API.value
_CONF = confidence_for(EvidenceClass.AUTHORITATIVE_API)
# a feeder->core 'raises_for' link is INFERRED from the SPV's name referencing the
# company — real but speculative, so it's graded co-occurrence (a non-expanding leaf).
_CO = EvidenceClass.CO_OCCURRENCE.value
_CO_CONF = confidence_for(EvidenceClass.CO_OCCURRENCE)


def _to_int(s: Any) -> int:
    try:
        return int(s)
    except (TypeError, ValueError):
        return 0


async def _link_once(
    actions: Actions, a: uuid.UUID, b: uuid.UUID, type_: str, *,
    ts: datetime, conf: float, ec: str, case_id: uuid.UUID | None,
) -> bool:
    """create_link is append-only; these authoritative filings are re-ingested on
    re-aim, so guard against duplicating an identical (from,to,type) edge."""
    if await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type=$3 LIMIT 1", a, b, type_
    ):
        return False
    await actions.create_link(a, b, type_, _SOURCE, ts, conf, case_id=case_id, evidence_class=ec)
    return True
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
) -> dict[str, Any]:
    """Materialize a parsed Form D: the issuer Organization (canonical cik:NNN, the
    same scheme as edgar.py, so it resolves cross-base to Wikidata), its offering
    facts as properties, and its related persons linked by role."""
    ts = observed_at or datetime.now(UTC)
    cik = parsed.get("cik")
    if not cik:
        return {"issuers": 0, "persons": 0, "links": 0, "properties": 0,
                "issuer_id": None, "name": None, "amount": 0}
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
            if await _link_once(
                actions, issuer_id, person_id, _REL.get(rel, "officer"),
                ts=ts, conf=_CONF, ec=_EC, case_id=case_id,
            ):
                n_link += 1
    return {
        "issuers": 1, "persons": n_person, "links": n_link, "properties": n_prop,
        "issuer_id": issuer_id, "name": parsed.get("issuer"),
        "amount": _to_int((parsed.get("offering") or {}).get("amount_raised")),
    }


async def link_feeders(
    actions: Actions,
    issuers: list[dict[str, Any]],
    search_name: str,
    *,
    observed_at: datetime | None = None,
    case_id: uuid.UUID | None = None,
) -> tuple[uuid.UUID | None, int]:
    """Connect the feeder SPVs to the company they fund. The CORE is the issuer whose
    name normalizes to the search term (and, among ties, raised the most — the company
    out-raises its feeders); every other issuer whose name still references the term is
    an SPV that `raises_for` the core. Name-inferred, so the link is co-occurrence."""
    ts = observed_at or datetime.now(UTC)
    norm = normalize_org_name(search_name)
    exact = [i for i in issuers if normalize_org_name(i["name"] or "") == norm]
    pool = exact or issuers
    if not pool:
        return None, 0
    core = max(pool, key=lambda i: i.get("amount") or 0)
    n = 0
    for i in issuers:
        if i["id"] == core["id"]:
            continue
        if norm and norm in normalize_org_name(i["name"] or ""):
            if await _link_once(
                actions, i["id"], core["id"], "raises_for",
                ts=ts, conf=_CO_CONF, ec=_CO, case_id=case_id,
            ):
                n += 1
    return core["id"], n


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


async def aim_form_d(actions: Actions, name: str, *, limit: int = 40) -> dict[str, Any]:
    """Resolve a name to its Form D filings and ingest each — the private-financing
    layer of 'aim Osiris at <name>'."""
    filings = await search_form_d(name, limit=limit)
    totals: dict[str, Any] = {"filings": 0, "issuers": 0, "persons": 0, "links": 0, "properties": 0}
    issuers: list[dict[str, Any]] = []
    for f in filings:
        try:
            parsed = await fetch_form_d(f["cik"], f["accession"])
        except (httpx.HTTPError, ET.ParseError):
            continue
        counts = await ingest_form_d(actions, parsed)
        totals["filings"] += 1
        for k in ("issuers", "persons", "links", "properties"):
            totals[k] += counts[k]
        if counts["issuer_id"] is not None:
            issuers.append(
                {"id": counts["issuer_id"], "name": counts["name"], "amount": counts["amount"]}
            )
    core_id, feeders = await link_feeders(actions, issuers, name)
    totals["feeders"] = feeders
    totals["core"] = str(core_id) if core_id else None
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
