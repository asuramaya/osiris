"""Ingest ClinicalTrials.gov studies — the trials, sites, and investigators.

The authoritative public record for human trials. For a device sponsor it exposes
the registered studies, their status (recruiting / terminated, with whyStopped), the
enrollment, the clinical SITES (the hospitals doing the procedures — a real-estate /
facility signal), the named INVESTIGATORS (the surgeons), and whether a results
section (adverse events, including deaths) has been posted yet.

What it does NOT and CANNOT show: adverse events for an INVESTIGATIONAL device are
reported to the FDA confidentially under the IDE; they do not enter MAUDE or a results
section until the trial completes and results are posted. That shielding is regulatory,
not a cover-up — and this ingest is how you watch for the moment it lifts (a trial
flips to TERMINATED with a whyStopped, or a results section appears).

    uv run python -m src.ingest.clinicaltrials <sponsor name>
"""

from __future__ import annotations

import asyncio
import re
import sys
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx

from src.actions.core import Actions
from src.config.settings import get_settings
from src.db.pool import create_pool
from src.ontology.entity_type import is_plausible_person_name
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_SOURCE = "clinicaltrials"
_EC = EvidenceClass.AUTHORITATIVE_API.value
_CONF = confidence_for(EvidenceClass.AUTHORITATIVE_API)
_UA = {"User-Agent": "osiris-osint operator@example.com"}
_API = "https://clinicaltrials.gov/api/v2/studies"


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


def parse_study(study: dict[str, Any]) -> dict[str, Any]:
    """Flatten a v2 study record into the facts we materialize."""
    p = study.get("protocolSection", {})
    idm = p.get("identificationModule", {})
    st = p.get("statusModule", {})
    dm = p.get("designModule", {})
    sm = p.get("sponsorCollaboratorsModule", {})
    cm = p.get("contactsLocationsModule", {})
    return {
        "nct": idm.get("nctId"),
        "title": idm.get("briefTitle"),
        "status": st.get("overallStatus"),
        "why_stopped": st.get("whyStopped"),
        "start": (st.get("startDateStruct") or {}).get("date"),
        "enrollment": (dm.get("enrollmentInfo") or {}).get("count"),
        "phase": ", ".join(dm.get("phases") or []) or None,
        "sponsor": (sm.get("leadSponsor") or {}).get("name"),
        "officials": [
            {"name": o.get("name"), "role": o.get("role"), "affiliation": o.get("affiliation")}
            for o in cm.get("overallOfficials", [])
            if o.get("name")
        ],
        "locations": [
            {"facility": loc.get("facility"), "city": loc.get("city"),
             "state": loc.get("state"), "country": loc.get("country")}
            for loc in cm.get("locations", [])
            if loc.get("facility")
        ],
        "has_results": "resultsSection" in study,
    }


async def ingest_study(
    actions: Actions,
    parsed: dict[str, Any],
    *,
    case_id: uuid.UUID | None = None,
    observed_at: datetime | None = None,
) -> dict[str, int]:
    """Materialize a trial: the ClinicalTrial node + its facts, the sponsor that runs
    it, the clinical sites, and the named investigators."""
    ts = observed_at or datetime.now(UTC)
    nct = parsed.get("nct")
    if not nct:
        return {"trials": 0, "sites": 0, "investigators": 0, "links": 0}
    trial = await actions.create_or_find_object("ClinicalTrial", f"nct:{nct}", _SOURCE, case_id)

    async def put(oid: uuid.UUID, name: str, value: Any) -> None:
        if value not in (None, ""):
            await actions.assert_property(
                oid, name, value, _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC
            )

    for name in ("title", "status", "why_stopped", "start", "enrollment", "phase"):
        await put(trial, name, parsed.get(name))
    await put(trial, "has_results", "yes" if parsed.get("has_results") else "no")

    n_site = n_inv = n_link = 0

    if parsed.get("sponsor"):
        sponsor = await actions.create_or_find_object(
            "Organization", f"ctgov-org:{_norm(parsed['sponsor'])}", _SOURCE, case_id
        )
        await put(sponsor, "name", parsed["sponsor"])
        await actions.create_link(
            sponsor, trial, "sponsors", _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC
        )
        n_link += 1

    for o in parsed.get("officials") or []:
        # 'overallOfficials' occasionally carries a contact string ("Call 1-877-...")
        # instead of an investigator — don't mint a junk Person from it.
        if not is_plausible_person_name(o["name"]):
            continue
        person = await actions.create_or_find_object(
            "Person", f"ctgov-person:{_norm(o['name'])}", _SOURCE, case_id
        )
        await put(person, "name", o["name"])
        await put(person, "affiliation", o.get("affiliation"))
        await actions.create_link(
            trial, person, "investigator", _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC
        )
        n_inv += 1
        n_link += 1

    for loc in parsed.get("locations") or []:
        site = await actions.create_or_find_object(
            "Organization", f"ctgov-org:{_norm(loc['facility'])}", _SOURCE, case_id
        )
        await put(site, "name", loc["facility"])
        where = ", ".join(x for x in (loc.get("city"), loc.get("state"), loc.get("country")) if x)
        await put(site, "location", where)
        await actions.create_link(
            trial, site, "site", _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC
        )
        n_site += 1
        n_link += 1

    return {"trials": 1, "sites": n_site, "investigators": n_inv, "links": n_link}


async def _fetch(
    query: dict[str, str], *, page_size: int = 100, max_pages: int = 5
) -> list[dict[str, Any]]:
    """Paginate the keyless v2 studies endpoint for an arbitrary query."""
    out: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=40.0, headers=_UA, follow_redirects=True) as client:
        token: str | None = None
        for _ in range(max_pages):
            params: dict[str, Any] = {**query, "pageSize": page_size}
            if token:
                params["pageToken"] = token
            r = await client.get(_API, params=params)
            r.raise_for_status()
            data = r.json()
            out.extend(data.get("studies", []))
            token = data.get("nextPageToken")
            if not token:
                break
    return out


async def fetch_studies(sponsor: str, *, page_size: int = 100) -> list[dict[str, Any]]:
    """All studies for a lead sponsor (keyless v2 API)."""
    return await _fetch({"query.spons": sponsor}, page_size=page_size)


async def expand_facility(actions: Actions, facility: str, *, limit: int = 60) -> dict[str, int]:
    """Ingest the trials run at a clinical SITE — revealing which other sponsors use it
    (the foreign-counterparty thread: who else operates at Cleveland Clinic Abu Dhabi).
    Other trials at the same facility link to the same site node, so co-tenancy emerges."""
    totals = {"trials": 0, "sites": 0, "investigators": 0, "links": 0}
    needle = facility.lower()
    for study in (await _fetch({"query.locn": facility}))[:limit]:
        parsed = parse_study(study)
        # keep ONLY the queried facility's site — a multinational trial lists dozens of
        # global sites and we don't want to slurp them all just to record co-tenancy.
        parsed["locations"] = [
            loc for loc in parsed["locations"] if needle in (loc.get("facility") or "").lower()
        ]
        counts = await ingest_study(actions, parsed)
        for k in totals:
            totals[k] += counts[k]
    return totals


async def aim_trials(actions: Actions, sponsor: str) -> dict[str, int]:
    """Ingest every registered trial for a sponsor."""
    totals = {"trials": 0, "sites": 0, "investigators": 0, "links": 0}
    for study in await fetch_studies(sponsor):
        parsed = parse_study(study)
        if parsed.get("sponsor") and sponsor.lower() not in parsed["sponsor"].lower():
            continue  # the v2 query is fuzzy; keep only this sponsor's own trials
        counts = await ingest_study(actions, parsed)
        for k in totals:
            totals[k] += counts[k]
    return totals


def main() -> None:
    sponsor = " ".join(sys.argv[1:])
    if not sponsor:
        print("usage: python -m src.ingest.clinicaltrials <sponsor name>")
        return

    async def run() -> None:
        pool = await create_pool(get_settings().database_url)
        try:
            print(f"ingested: {await aim_trials(Actions(pool), sponsor)}")
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":
    main()
