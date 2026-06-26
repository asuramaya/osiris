"""Etherscan ingest: address -> aggregated counterparties + contract identity.

Hermetic — the network is the `fetch_address` seam; these drive the pure aggregator
and the materializer with a fixture bundle, never calling Etherscan.
"""
from __future__ import annotations

import pytest
import src.ingest.etherscan as eth
from src.actions.core import Actions
from src.ingest.etherscan import (
    _addr_canonical,
    _is_rate_limit,
    _parse_source,
    aggregate_counterparties,
    ingest_address,
)

ME = "0xaaaa000000000000000000000000000000000000"
EXCH = "0xbbbb000000000000000000000000000000000000"
VICTIM = "0xcccc000000000000000000000000000000000000"

_TXS = [
    # subject -> EXCH twice (out), and EXCH -> subject once (in)
    {"from": ME, "to": EXCH, "value": str(2 * 10**18), "timeStamp": "1700000000"},
    {"from": ME, "to": EXCH, "value": str(1 * 10**18), "timeStamp": "1700000100"},
    {"from": EXCH, "to": ME, "value": str(5 * 10**17), "timeStamp": "1700000200"},
    # VICTIM -> subject once (in)
    {"from": VICTIM, "to": ME, "value": str(3 * 10**18), "timeStamp": "1700000300"},
    # a self-transfer + a contract-creation (to=null) must be ignored
    {"from": ME, "to": ME, "value": "0", "timeStamp": "1700000400"},
    {"from": ME, "to": "", "value": "0", "timeStamp": "1700000500"},
]
_TOKEN_TXS = [
    {"from": ME, "to": EXCH, "tokenSymbol": "USDC", "tokenDecimal": "6",
     "value": str(1500 * 10**6), "timeStamp": "1700000600"},
]
BURN = "0x0000000000000000000000000000000000000000"


def test_aggregate_ranks_by_interaction_and_tracks_flow() -> None:
    cps = aggregate_counterparties(ME, _TXS, _TOKEN_TXS, top=10)
    assert [c.address for c in cps] == [EXCH, VICTIM]  # EXCH has more interactions

    exch = cps[0]
    assert exch.tx_count == 4  # 3 normal + 1 token
    assert exch.wei_out == 3 * 10**18  # subject sent 2 + 1
    assert exch.wei_in == 5 * 10**17   # subject received 0.5
    assert exch.tokens == ["USDC"]
    assert exch.token_out == {"USDC": 1500.0}  # decimal-adjusted token flow captured

    victim = cps[1]
    assert victim.tx_count == 1
    assert victim.wei_in == 3 * 10**18
    assert victim.wei_out == 0


def test_top_k_caps_counterparties() -> None:
    assert len(aggregate_counterparties(ME, _TXS, _TOKEN_TXS, top=1)) == 1


def test_burn_address_is_not_a_counterparty() -> None:
    txs = [
        {"from": ME, "to": BURN, "value": "0", "timeStamp": "1700000000"},
        {"from": ME, "to": EXCH, "value": str(10**18), "timeStamp": "1700000100"},
    ]
    cps = aggregate_counterparties(ME, txs, [], top=10)
    assert [c.address for c in cps] == [EXCH]  # mint/burn filtered out


def test_parse_source_distinguishes_contract_from_eoa() -> None:
    assert _parse_source([{"ContractName": "TetherToken", "ABI": "[...]"}]) == {
        "is_contract": True, "contract_name": "TetherToken",
    }
    eoa = _parse_source([{"ContractName": "", "ABI": "Contract source code not verified"}])
    assert eoa == {"is_contract": False}


async def test_ingest_materializes_subject_and_counterparties(actions: Actions) -> None:
    bundle = {
        "balance_wei": str(4 * 10**18),
        "txs": _TXS,
        "token_txs": _TOKEN_TXS,
        "source": [{"ContractName": "", "ABI": "Contract source code not verified"}],
    }
    out = await ingest_address(actions, ME, bundle, chain_id=1, top=25)
    assert out["counterparties"] == 2
    assert out["links"] == 2
    assert out["is_contract"] is False

    subject = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='CryptoAddress' AND canonical=$1",
        _addr_canonical(1, ME),
    )
    assert subject is not None
    bal = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='balance_eth'",
        subject,
    )
    assert bal == "4.000000"

    # the transacted_with link carries the aggregate + is graded as ground truth
    row = await actions.pool.fetchrow(
        "SELECT l.evidence_class, l.properties FROM links l "
        "JOIN objects o ON o.id=l.to_id "
        "WHERE l.from_id=$1 AND o.canonical=$2 AND l.type='transacted_with'",
        subject, _addr_canonical(1, EXCH),
    )
    assert row["evidence_class"] == "authoritative_api"
    assert row["properties"]["tx_count"] == 4
    assert row["properties"]["eth_out"] == 3.0
    assert row["properties"]["token_out"] == {"USDC": 1500.0}
    assert "USDC" in row["properties"]["tokens"]


class _FakeResp:
    def __init__(self, payload: dict[str, object], status_code: int = 200) -> None:
        self._p, self.status_code = payload, status_code

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict[str, object]:
        return self._p


class _FakeClient:
    def __init__(self, responses: list[_FakeResp]) -> None:
        self._r, self.calls = responses, 0

    async def get(self, url: str, params: dict[str, object] | None = None) -> _FakeResp:
        r = self._r[self.calls]
        self.calls += 1
        return r


def test_is_rate_limit() -> None:
    assert _is_rate_limit("NOTOK", "Max calls per sec rate limit reached (3/sec)")
    assert _is_rate_limit("", "429 too many requests")
    assert not _is_rate_limit("NOTOK", "Invalid API Key")


async def test_call_retries_then_succeeds_on_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []

    async def fake_sleep(s: float) -> None:
        sleeps.append(s)

    monkeypatch.setattr(eth.asyncio, "sleep", fake_sleep)
    client = _FakeClient([
        _FakeResp({"status": "0", "message": "NOTOK",
                   "result": "Max calls per sec rate limit reached (3/sec)"}),
        _FakeResp({"status": "1", "message": "OK", "result": "42"}),
    ])
    out = await eth._call(client, "k", 1, "account", "balance", address="0x")  # type: ignore[arg-type]
    assert out == "42"
    assert client.calls == 2 and len(sleeps) == 1  # backed off once, then succeeded


async def test_ingest_is_idempotent(actions: Actions) -> None:
    bundle = {"balance_wei": "0", "txs": _TXS, "token_txs": [], "source": []}
    first = await ingest_address(actions, ME, bundle, chain_id=1)
    second = await ingest_address(actions, ME, bundle, chain_id=1)
    assert first["links"] == 2
    assert second["links"] == 0  # _link_once guards the append-only create_link
