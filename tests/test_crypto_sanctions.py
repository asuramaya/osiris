"""The crawl×base edge for crypto: an OFAC-listed wallet and an on-chain trace of the
same address fuse into ONE object (shared canonical), so screening a traced address
surfaces a sanctioned counterparty + the named holder behind it — no merge required.
"""
from __future__ import annotations

import uuid

from src.actions.core import Actions
from src.ingest.etherscan import _addr_canonical, ingest_address, screen_against_sanctions
from src.ingest.opensanctions import _wallet_canonical, ingest_ftm

ME = "0xaaaa000000000000000000000000000000000000"
# OFAC lists this wallet in EIP-55 mixed case; the trace sees it lowercased. Both must
# canonicalize to the same key or the fusion silently fails.
SANCTIONED = "0xBBBB000000000000000000000000000000000000"

_HOLDER_FTM = {
    "id": "ofac-person-1", "schema": "Person",
    "properties": {"name": ["Ivan Sanctioned"], "topics": ["sanction"]},
}
_WALLET_FTM = {
    "id": "ofac-wallet-1", "schema": "CryptoWallet",
    "properties": {"publicKey": [SANCTIONED], "currency": ["ETH"], "holder": ["ofac-person-1"]},
}


def test_wallet_canonical_aligns_evm_with_tracer() -> None:
    # mixed-case OFAC address and the tracer's lowercased form must collide, and the
    # ERC-20 currency label must not change that (tokens share Ethereum's addresses)
    assert _wallet_canonical(SANCTIONED, "ETH") == _addr_canonical(1, SANCTIONED)
    assert _wallet_canonical(SANCTIONED, "USDT") == _addr_canonical(1, SANCTIONED)
    # a non-EVM (e.g. bitcoin) address keeps a currency-namespaced, case-sensitive key
    assert _wallet_canonical("1A1zP1eP5Q", "BTC") == "wallet:btc:1A1zP1eP5Q"


async def test_crypto_wallet_ingest_links_holder(actions: Actions) -> None:
    counts = await ingest_ftm(actions, [_HOLDER_FTM, _WALLET_FTM])
    assert counts["objects"] == 2  # holder + wallet

    wallet = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='CryptoAddress' AND canonical=$1",
        _wallet_canonical(SANCTIONED, "ETH"),
    )
    assert wallet is not None
    holder_name = await actions.pool.fetchval(
        "SELECT (SELECT value #>> '{}' FROM current_assertions a "
        "        WHERE a.object_id=l.to_id AND a.name='name') "
        "FROM links l WHERE l.from_id=$1 AND l.type='controlled_by'",
        wallet,
    )
    assert holder_name == "Ivan Sanctioned"


async def test_trace_fuses_with_sanctioned_wallet_and_screens(actions: Actions) -> None:
    # 1. federate the OFAC wallet + its holder
    await ingest_ftm(actions, [_HOLDER_FTM, _WALLET_FTM])

    # 2. trace a subject whose top counterparty IS that wallet (lowercased on-chain)
    txs = [
        {"from": ME, "to": SANCTIONED.lower(), "value": str(10**18), "timeStamp": "1700000000"},
        {"from": SANCTIONED.lower(), "to": ME, "value": str(10**18), "timeStamp": "1700000100"},
    ]
    bundle = {"balance_wei": "0", "txs": txs, "token_txs": [], "source": []}
    out = await ingest_address(actions, ME, bundle, chain_id=1)
    assert out["counterparties"] == 1

    # 3. the counterparty object is the SAME id as the OFAC wallet (canonical fusion):
    #    it now carries BOTH an etherscan and an opensanctions assertion.
    cp = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='CryptoAddress' AND canonical=$1",
        _addr_canonical(1, SANCTIONED),
    )
    sources = await actions.pool.fetch(
        "SELECT DISTINCT source_id FROM current_assertions WHERE object_id=$1", cp
    )
    assert {r["source_id"] for r in sources} >= {"etherscan", "opensanctions"}

    # 4. screening the trace flags the sanctioned counterparty + names the holder
    screen = await screen_against_sanctions(actions.pool, uuid.UUID(out["subject_id"]))
    assert screen["subject_sanctioned"] is False
    assert len(screen["sanctioned_hits"]) == 1
    hit = screen["sanctioned_hits"][0]
    assert hit["address"] == SANCTIONED.lower()
    assert "Ivan Sanctioned" in hit["holders"]
