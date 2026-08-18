"""Ingest the SEC EDGAR company list — a second open base, a different format.

`company_tickers.json` is one keyless SEC file listing every EDGAR-registered
company (CIK, ticker, name). It proves the bulk-ingest pattern generalizes beyond
FollowTheMoney to a flat JSON base: same Actions layer, same AUTHORITATIVE_API
class, canonical = the zero-padded CIK (EDGAR's stable key). SEC asks for a
descriptive User-Agent.

    uv run python -m src.ingest.edgar [limit] [case_id]
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from typing import Any

import httpx

from src.actions.core import Actions
from src.config.settings import get_settings
from src.db.pool import create_pool
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_SOURCE = "edgar"
_CONF = confidence_for(EvidenceClass.AUTHORITATIVE_API)
_EC = EvidenceClass.AUTHORITATIVE_API.value
_URL = "https://www.sec.gov/files/company_tickers.json"
# SEC fair-access INVERTS antibot: it 403s browser UAs and wants a contact-style UA.
_UA = {"User-Agent": "osiris-osint operator@example.com"}


def parse_company_tickers(data: dict[str, Any]) -> Iterator[dict[str, Any]]:
    for v in data.values():
        if isinstance(v, dict) and v.get("cik_str") is not None:
            yield v


async def ingest_companies(
    actions: Actions,
    companies: Iterable[dict[str, Any]],
    *,
    case_id: uuid.UUID | None = None,
    observed_at: datetime | None = None,
    limit: int | None = None,
) -> dict[str, int]:
    ts = observed_at or datetime.now(UTC)
    rows = list(companies)
    if limit is not None:
        rows = rows[:limit]
    n_obj = n_prop = 0
    for c in rows:
        cik = c.get("cik_str")
        if cik is None:
            continue
        oid = await actions.create_or_find_object(
            "Organization", f"cik:{int(cik):010d}", _SOURCE, case_id
        )
        n_obj += 1
        for name, val in (("name", c.get("title")), ("ticker", c.get("ticker")), ("cik", int(cik))):
            if val:
                await actions.assert_property(
                    oid, name, val, _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC
                )
                n_prop += 1
    return {"objects": n_obj, "properties": n_prop}


async def fetch_company_tickers(url: str = _URL, *, timeout_s: float = 30.0) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
        r = await client.get(url, headers=_UA)
        r.raise_for_status()
        data = r.json()
    return data if isinstance(data, dict) else {}


def main() -> None:
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None
    case_id = uuid.UUID(sys.argv[2]) if len(sys.argv) > 2 else None

    async def run() -> None:
        pool = await create_pool(
            get_settings().database_url, application_name="osiris-script:ingest-edgar")
        try:
            data = await fetch_company_tickers()
            counts = await ingest_companies(
                Actions(pool), parse_company_tickers(data), case_id=case_id, limit=limit
            )
            print(f"ingested: {counts}")
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":
    main()
