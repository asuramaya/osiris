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
import re
import sys
import uuid
from datetime import UTC, datetime
from typing import Any
from xml.etree import ElementTree as ET

import httpx

from src.actions.core import Actions
from src.config.settings import get_settings
from src.db.pool import create_pool
from src.ontology.entity_type import classify_entity_type, clean_entity_name
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
        # a Form D "related person" is OFTEN actually the GP entity ('Brilliant Phoenix
        # GP Inc.', 'LLC Sydecar'); classify so those become Organizations, not fake
        # people that pollute principals/screening and never cross-base-resolve.
        name = clean_entity_name(p["name"]) or p["name"]
        etype = classify_entity_type(name)
        prefix = "sec-org:" if etype == "Organization" else "sec-person:"
        ent_id = await actions.create_or_find_object(
            etype, prefix + name.strip().lower(), _SOURCE, case_id
        )
        n_person += 1
        await actions.assert_property(
            ent_id, "name", name, _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC
        )
        if p.get("city") or p.get("state"):
            loc = ", ".join(x for x in (p.get("city"), p.get("state")) if x)
            await actions.assert_property(
                ent_id, "location", loc, _SOURCE, ts, _CONF,
                case_id=case_id, evidence_class=_EC,
            )
        for rel in p.get("relationships") or ["officer"]:
            if await _link_once(
                actions, issuer_id, ent_id, _REL.get(rel, "officer"),
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


# tokens that mark the END of the portfolio-company name in an SPV's title — the
# company a feeder funds is its leading word(s) before the fund/structure boilerplate.
_STOP_TOKENS = frozenset({
    "spv", "fund", "funds", "series", "llc", "lp", "inc", "corp", "partners", "alternate",
    "investments", "investment", "capital", "ventures", "vc", "holdings", "coinvest",
    "co", "access", "opportunities", "opportunity", "trust", "vehicle", "gp", "the",
    "a", "of", "and", "al", "i", "ii", "iii", "iv", "v", "vi",
})
_MONTHS = frozenset({
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "sept", "oct", "nov", "dec",
    "january", "february", "march", "april", "june", "july", "august", "september",
    "october", "november", "december",
})


def _target_company(spv_name: str) -> str | None:
    """Best-effort: the portfolio company a feeder SPV funds, from its name. Form D does
    NOT disclose the underlying company, so the SPV title is the only signal — leading
    word(s) before the fund/structure/date boilerplate. Heuristic (hence the link it
    feeds is co-occurrence): 'Anthropic SPV2 ... a Series of CGF2021 LLC' -> Anthropic."""
    out: list[str] = []
    for raw in re.split(r"[\s,\-]+", spv_name.strip()):
        tok = raw.lower().strip(".")
        if not tok or tok.startswith("spv") or re.fullmatch(r"\d+", tok):
            break
        if tok in _STOP_TOKENS or tok in _MONTHS:
            break
        if raw.isupper() and len(raw) <= 4:
            # an operator acronym (MAV, DPV, BP…): skip it if it leads, stop if it trails
            if out:
                break
            continue
        out.append(raw)
        if len(out) >= 2:
            break
    company = " ".join(out).strip(" ,-")
    # drop short codes (CC, JFF, AC) and bare acronyms that slipped through
    if len(company) < 3 or (len(company) <= 4 and company.upper() == company):
        return None
    return company if not company.isdigit() else None


async def search_filings(
    query: str, *, forms: str = "D", limit: int = 60, match_issuer: bool = False,
) -> list[dict[str, str]]:
    """EDGAR full-text search (keyless, paginated). With match_issuer the hit's issuer
    name must contain the query (a company's own filings); without it, every filing
    that MENTIONS the query is returned — the way to pull a repeat player's portfolio."""
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    # the EFTS search-index endpoint returns ~100 hits per request and rejects a `from`
    # offset (500s), so one request is the page; limit just slices it.
    async with httpx.AsyncClient(timeout=30.0, headers=_UA, follow_redirects=True) as client:
        r = await client.get(_EFTS, params={"q": f'"{query}"', "forms": forms})
        r.raise_for_status()
        for hit in r.json().get("hits", {}).get("hits", []):
            src = hit.get("_source", {})
            acc = (hit.get("_id") or "").split(":")[0]
            for cik, disp in zip(src.get("ciks", []), src.get("display_names", []), strict=False):
                if match_issuer and query.lower() not in disp.lower():
                    continue
                key = (cik, acc)
                if key in seen:
                    continue
                seen.add(key)
                out.append({"cik": cik, "accession": acc, "issuer": disp})
                if len(out) >= limit:
                    return out
    return out


async def search_form_d(name: str, *, limit: int = 40) -> list[dict[str, str]]:
    """A company's own Form D filings (issuer-name match)."""
    return await search_filings(name, limit=limit, match_issuer=True)


async def link_spv_targets(
    actions: Actions,
    issuers: list[dict[str, Any]],
    *,
    observed_at: datetime | None = None,
    case_id: uuid.UUID | None = None,
) -> int:
    """For each ingested SPV, link it raises_for the portfolio company parsed from its
    name (creating a `company:<name>` node that cross-base resolution fuses to the real
    entity). This is what turns a repeat player's filings into a co-investment graph."""
    ts = observed_at or datetime.now(UTC)
    n = 0
    for i in issuers:
        company = _target_company(i["name"] or "")
        if not company:
            continue
        target = await actions.create_or_find_object(
            "Organization", f"company:{normalize_org_name(company)}", _SOURCE, case_id
        )
        if target == i["id"]:
            continue
        await actions.assert_property(
            target, "name", company, _SOURCE, ts, _CO_CONF, case_id=case_id, evidence_class=_CO
        )
        if await _link_once(
            actions, i["id"], target, "raises_for", ts=ts, conf=_CO_CONF, ec=_CO, case_id=case_id
        ):
            n += 1
    return n


async def expand_filings(actions: Actions, query: str, *, limit: int = 60) -> dict[str, Any]:
    """Pull a repeat player's thread: ingest every Form D that mentions them, link each
    SPV to the company it funds. The operator (a related person on every filing) ends up
    connected to their whole portfolio."""
    hits = await search_filings(query, limit=limit, match_issuer=False)
    totals: dict[str, Any] = {"filings": 0, "issuers": 0, "persons": 0, "links": 0}
    issuers: list[dict[str, Any]] = []
    for f in hits:
        try:
            parsed = await fetch_form_d(f["cik"], f["accession"])
        except (httpx.HTTPError, ET.ParseError):
            continue
        counts = await ingest_form_d(actions, parsed)
        totals["filings"] += 1
        for k in ("issuers", "persons", "links"):
            totals[k] += counts[k]
        if counts["issuer_id"] is not None:
            issuers.append(
                {"id": counts["issuer_id"], "name": counts["name"], "amount": counts["amount"]}
            )
    totals["targets"] = await link_spv_targets(actions, issuers)
    totals["fetched"] = len(hits)
    return totals


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
    argv = sys.argv[1:]
    if not argv:
        print("usage: python -m src.ingest.edgar_formd <company name>")
        print("       python -m src.ingest.edgar_formd expand <operator name>")
        return
    expand = argv[0] == "expand"
    name = " ".join(argv[1:] if expand else argv)

    async def run() -> None:
        pool = await create_pool(get_settings().database_url)
        try:
            actions = Actions(pool)
            result = (
                await expand_filings(actions, name) if expand
                else await aim_form_d(actions, name)
            )
            print(f"ingested: {result}")
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":
    main()
