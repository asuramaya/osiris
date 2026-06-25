from __future__ import annotations

from src.actions.core import Actions
from src.ingest.edgar import ingest_companies, parse_company_tickers

_DATA = {
    "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
    "1": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft Corporation"},
    "2": {"ticker": "BAD"},  # no CIK -> skipped
}


async def test_ingest_edgar_companies(actions: Actions) -> None:
    counts = await ingest_companies(actions, parse_company_tickers(_DATA))
    assert counts["objects"] == 2

    oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='Organization' AND canonical='cik:0000320193'"
    )
    assert oid is not None
    name = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='name'", oid
    )
    assert name == "Apple Inc."
    ticker = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='ticker'", oid
    )
    assert ticker == "AAPL"
