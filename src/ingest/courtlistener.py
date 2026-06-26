"""Ingest CourtListener — federal/state court dockets & opinions (keyless v4 API).

'Has this entity been sued or charged?' is the first question a follow-the-money
investigator asks. CourtListener (Free Law Project) is the keyless answer: RECAP
dockets (PACER mirror) and case-law opinions, searchable by party name. Each case
becomes a CourtCase node carrying the court, dates, docket number, judge, parties,
attorneys and firms — and is linked to the subject (DIRECT_OBSERVATION when the
subject is a named party, CO_OCCURRENCE when merely mentioned).

    uv run python -m src.ingest.courtlistener <entity name> [opinions|dockets]
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

_SOURCE = "courtlistener"
_EC = EvidenceClass.AUTHORITATIVE_API.value
_CONF = confidence_for(EvidenceClass.AUTHORITATIVE_API)
_OBS = EvidenceClass.DIRECT_OBSERVATION.value
_OBS_CONF = confidence_for(EvidenceClass.DIRECT_OBSERVATION)
_CO = EvidenceClass.CO_OCCURRENCE.value
_CO_CONF = confidence_for(EvidenceClass.CO_OCCURRENCE)
_UA = {"User-Agent": "osiris-osint operator@example.com"}
_API = "https://www.courtlistener.com/api/rest/v4/search/"
_BASE = "https://www.courtlistener.com"


def parse_case(r: dict[str, Any]) -> dict[str, Any]:
    key = r.get("docket_id") or r.get("cluster_id") or r.get("id")
    url = r.get("docket_absolute_url") or r.get("absolute_url")
    return {
        "key": str(key) if key else None,
        "case_name": r.get("caseName"),
        "court": r.get("court"),
        "date_filed": r.get("dateFiled"),
        "date_terminated": r.get("dateTerminated"),
        "docket_number": r.get("docketNumber"),
        "chapter": r.get("chapter"),
        "nature": r.get("suitNature"),
        "judge": r.get("assignedTo"),
        "parties": [p for p in (r.get("party") or []) if p],
        "attorneys": [a for a in (r.get("attorney") or []) if a],
        "firms": [f for f in (r.get("firm") or []) if f],
        "url": (_BASE + url) if url else None,
    }


async def ingest_cases(
    actions: Actions,
    parsed_cases: list[dict[str, Any]],
    *,
    subject_id: uuid.UUID | None = None,
    subject_name: str | None = None,
    case_id: uuid.UUID | None = None,
    observed_at: datetime | None = None,
) -> dict[str, int]:
    """Materialize CourtCase nodes and link the subject to each."""
    ts = observed_at or datetime.now(UTC)
    needle = (subject_name or "").lower()
    n_case = n_link = 0
    for p in parsed_cases:
        if not p["key"]:
            continue
        cobj = await actions.create_or_find_object(
            "CourtCase", f"courtlistener:{p['key']}", _SOURCE, case_id
        )
        n_case += 1
        # store the case title as 'name' (the convention dossier/search read)
        if p.get("case_name"):
            await actions.assert_property(
                cobj, "name", p["case_name"], _SOURCE, ts, _CONF,
                case_id=case_id, evidence_class=_EC,
            )
        for name in ("court", "date_filed", "date_terminated",
                     "docket_number", "chapter", "nature", "judge", "url"):
            if p.get(name):
                await actions.assert_property(
                    cobj, name, p[name], _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC
                )
        for name in ("parties", "attorneys", "firms"):
            if p.get(name):
                await actions.assert_property(
                    cobj, name, p[name], _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC
                )
        if subject_id is not None:
            # a named party is DIRECT_OBSERVATION; a mere text-match mention co-occurs.
            is_party = any(needle and needle in str(x).lower() for x in p["parties"])
            ec, conf = (_OBS, _OBS_CONF) if is_party else (_CO, _CO_CONF)
            await actions.create_link(
                subject_id, cobj, "litigation", _SOURCE, ts, conf,
                case_id=case_id, evidence_class=ec,
            )
            n_link += 1
    return {"cases": n_case, "links": n_link}


async def search_cases(
    name: str, *, kind: str = "r", limit: int = 40, timeout_s: float = 40.0
) -> list[dict[str, Any]]:
    """CourtListener v4 search (kind 'r'=RECAP dockets, 'o'=opinions), cursor-paged."""
    out: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=timeout_s, headers=_UA, follow_redirects=True) as client:
        url: str | None = _API
        params: dict[str, Any] | None = {"q": f'"{name}"', "type": kind, "order_by": "score desc"}
        while url and len(out) < limit:
            r = await client.get(url, params=params)
            r.raise_for_status()
            data = r.json()
            out.extend(parse_case(x) for x in data.get("results", []))
            url, params = data.get("next"), None  # `next` is a full cursor URL
    return out[:limit]


async def aim_litigation(actions: Actions, name: str, *, kind: str = "r") -> dict[str, int]:
    """Resolve a name to an entity, search its court cases, and link them."""
    subject = await actions.pool.fetchval(
        "SELECT a.object_id FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id AND o.status='active' "
        "WHERE a.name='name' AND a.value #>> '{}' ILIKE '%'||$1||'%' "
        "ORDER BY length(a.value #>> '{}') ASC LIMIT 1",
        name,
    )
    cases = await search_cases(name, kind=kind)
    return await ingest_cases(
        actions, cases,
        subject_id=uuid.UUID(str(subject)) if subject else None,
        subject_name=name,
    )


def main() -> None:
    argv = sys.argv[1:]
    if not argv:
        print("usage: python -m src.ingest.courtlistener <name> [opinions|dockets]")
        return
    kind = "o" if (len(argv) > 1 and argv[1].startswith("op")) else "r"
    name = argv[0]

    async def run() -> None:
        pool = await create_pool(get_settings().database_url)
        try:
            print(f"ingested: {await aim_litigation(Actions(pool), name, kind=kind)}")
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":
    main()
