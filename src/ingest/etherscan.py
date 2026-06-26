"""Ingest on-chain activity from Etherscan v2 — the follow-the-money blockchain layer.

Crypto-fraud is half the follow-the-money investigator's beat, and the chain is the
most authoritative source there is: an immutable public ledger. This turns an EVM
ADDRESS into a financial picture the same way the other ingests turn a name into one —
counterparties, balance, and contract/token identity, all graded AUTHORITATIVE_API
(the ledger doesn't lie) and attributed with provenance.

The noise lesson from the footprint crawl applies here too: an active address has
thousands of transactions, so we do NOT mint a node per tx. We AGGREGATE — accumulate
per-counterparty totals (count, value in/out, tokens, first/last seen) and materialize
only the top-K counterparties as `transacted_with` links carrying the aggregate. That
is the intelligence primitive: *who does this address move money with, and how much*.

Etherscan v2 is multichain by `chainid` (1 = Ethereum mainnet, 8453 = Base, …); one
key, one base URL. It is the single place the keyless constraint bends — the API
rejects unkeyed calls, so `ETHERSCAN_API_KEY` is required; absent it, this degrades to
an error dict rather than crashing a run.

    uv run python -m src.ingest.etherscan 0xADDRESS [chainid] [top]
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import asyncpg
import httpx

from src.actions.core import Actions
from src.config.settings import get_settings
from src.db.pool import create_pool
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_SOURCE = "etherscan"
# The chain is ground truth — on-chain facts are as authoritative as an API gets.
_EC = EvidenceClass.AUTHORITATIVE_API.value
_CONF = confidence_for(EvidenceClass.AUTHORITATIVE_API)
_API = "https://api.etherscan.io/v2/api"
_WEI = 10**18
# Bounded window: most-recent N rows per feed keeps an active whale tractable.
_PAGE = 1000
# The zero address is mint/burn/creation, not a counterparty; it also collects the
# scam-airdrop spam tokens that pollute any active address's token feed.
_BURN = frozenset({"0x0000000000000000000000000000000000000000"})


def _addr_canonical(chain_id: int, address: str) -> str:
    """Stable dedupe key: chain-namespaced, lowercased (EVM addresses are
    case-insensitive; the mixed-case form is only an EIP-55 checksum)."""
    return f"eth:{chain_id}:{address.strip().lower()}"


@dataclass
class Counterparty:
    """Aggregated movement between the subject and one other address."""

    address: str
    tx_count: int = 0
    wei_in: int = 0          # native ETH flowing subject <- counterparty
    wei_out: int = 0         # native ETH flowing subject -> counterparty
    token_in: dict[str, float] = field(default_factory=dict)   # {symbol: amount} <-
    token_out: dict[str, float] = field(default_factory=dict)  # {symbol: amount} ->
    first_ts: int | None = None
    last_ts: int | None = None

    @property
    def tokens(self) -> list[str]:
        return sorted(set(self.token_in) | set(self.token_out))

    def observe(self, ts: int) -> None:
        self.tx_count += 1
        self.first_ts = ts if self.first_ts is None else min(self.first_ts, ts)
        self.last_ts = ts if self.last_ts is None else max(self.last_ts, ts)


def aggregate_counterparties(
    address: str,
    txs: list[dict[str, Any]],
    token_txs: list[dict[str, Any]],
    *,
    top: int = 25,
) -> list[Counterparty]:
    """Collapse raw tx / token-transfer rows into per-counterparty totals, ranked by
    interaction count (the follow-the-money signal: who this address deals with most)."""
    me = address.strip().lower()
    book: dict[str, Counterparty] = {}

    def cp(other: str) -> Counterparty:
        other = other.strip().lower()
        if other not in book:
            book[other] = Counterparty(address=other)
        return book[other]

    for t in txs:
        frm, to = (t.get("from") or "").lower(), (t.get("to") or "").lower()
        other = to if frm == me else frm
        if not other or other == me or other in _BURN:
            continue  # contract creation / self-transfer / mint-burn
        c = cp(other)
        try:
            ts = int(t.get("timeStamp") or 0)
            wei = int(t.get("value") or 0)
        except ValueError:
            ts, wei = 0, 0
        c.observe(ts)
        if frm == me:
            c.wei_out += wei
        else:
            c.wei_in += wei

    for t in token_txs:
        frm, to = (t.get("from") or "").lower(), (t.get("to") or "").lower()
        other = to if frm == me else frm
        if not other or other == me or other in _BURN:
            continue
        c = cp(other)
        try:
            ts = int(t.get("timeStamp") or 0)
            dec = int(t.get("tokenDecimal") or 0)
            raw = int(t.get("value") or 0)
        except ValueError:
            ts, dec, raw = 0, 0, 0
        c.observe(ts)
        sym = (t.get("tokenSymbol") or "").strip()
        if sym:
            amt = raw / (10**dec) if dec else float(raw)
            book_side = c.token_out if frm == me else c.token_in
            book_side[sym] = round(book_side.get(sym, 0.0) + amt, 6)

    ranked = sorted(book.values(), key=lambda c: (c.tx_count, c.wei_in + c.wei_out), reverse=True)
    return ranked[:top]


def _parse_source(result: Any) -> dict[str, Any]:
    """getsourcecode returns a 1-element array; pull the contract identity from it."""
    if not isinstance(result, list) or not result:
        return {}
    row = result[0]
    if not isinstance(row, dict):
        return {}
    name = (row.get("ContractName") or "").strip()
    # An EOA returns an empty ABI / no ContractName; only contracts carry identity.
    abi = row.get("ABI")
    is_contract = bool(name) or abi not in (None, "", "Contract source code not verified")
    out: dict[str, Any] = {"is_contract": is_contract}
    if name:
        out["contract_name"] = name
    if impl := (row.get("Implementation") or "").strip():
        out["proxy_implementation"] = impl
    return out


# --- network seam -----------------------------------------------------------

class EtherscanError(RuntimeError):
    pass


async def _call(
    client: httpx.AsyncClient, key: str, chain_id: int, module: str, action: str, **params: Any
) -> Any:
    """One Etherscan v2 call. Returns `result`; an empty-set 'No transactions found'
    is normal (=> []), any other status '0' is an error."""
    q = {"chainid": chain_id, "module": module, "action": action, "apikey": key, **params}
    r = await client.get(_API, params=q)
    r.raise_for_status()
    data = r.json()
    if data.get("status") == "1":
        return data.get("result")
    msg = str(data.get("message") or "")
    result = data.get("result")
    if "No transactions found" in msg or "No records found" in msg or result == []:
        return []
    # `message` is a generic "NOTOK"; the actionable detail (bad key, rate limit) is in
    # `result` — surface it.
    detail = result if isinstance(result, str) else ""
    raise EtherscanError(f"{module}.{action}: {f'{msg} — {detail}'.strip(' —') or 'unknown error'}")


async def fetch_address(
    address: str, *, chain_id: int = 1, key: str | None = None, timeout_s: float = 40.0
) -> dict[str, Any]:
    """Pull the on-chain bundle for one address: balance, recent txs + token
    transfers (most-recent first), and contract identity."""
    key = key or get_settings().etherscan_api_key
    if not key:
        raise EtherscanError("ETHERSCAN_API_KEY not set (Etherscan v2 rejects unkeyed calls)")
    async with httpx.AsyncClient(timeout=timeout_s, follow_redirects=True) as client:
        balance = await _call(
            client, key, chain_id, "account", "balance", address=address, tag="latest"
        )
        txs = await _call(
            client, key, chain_id, "account", "txlist",
            address=address, startblock=0, endblock=99999999, page=1, offset=_PAGE, sort="desc",
        )
        token_txs = await _call(
            client, key, chain_id, "account", "tokentx",
            address=address, startblock=0, endblock=99999999, page=1, offset=_PAGE, sort="desc",
        )
        source = await _call(client, key, chain_id, "contract", "getsourcecode", address=address)
    return {
        "balance_wei": balance,
        "txs": txs if isinstance(txs, list) else [],
        "token_txs": token_txs if isinstance(token_txs, list) else [],
        "source": source,
    }


# --- materialize ------------------------------------------------------------

async def _link_once(
    actions: Actions, from_id: uuid.UUID, to_id: uuid.UUID, type_: str,
    *, ts: datetime, properties: dict[str, Any], case_id: uuid.UUID | None,
) -> bool:
    """create_link is append-only; guard so re-tracing an address doesn't double edges."""
    exists = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type=$3 LIMIT 1",
        from_id, to_id, type_,
    )
    if exists:
        return False
    await actions.create_link(
        from_id, to_id, type_, _SOURCE, ts, _CONF,
        properties=properties, case_id=case_id, evidence_class=_EC,
    )
    return True


async def ingest_address(
    actions: Actions,
    address: str,
    bundle: dict[str, Any],
    *,
    chain_id: int = 1,
    top: int = 25,
    case_id: uuid.UUID | None = None,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    """Materialize the subject address + its top counterparties from a fetched bundle."""
    ts = observed_at or datetime.now(UTC)
    subject = await actions.create_or_find_object(
        "CryptoAddress", _addr_canonical(chain_id, address), _SOURCE, case_id
    )

    async def prop(oid: uuid.UUID, name: str, val: Any) -> None:
        if val not in (None, "", []):
            await actions.assert_property(
                oid, name, val, _SOURCE, ts, _CONF, case_id=case_id, evidence_class=_EC
            )

    await prop(subject, "address", address.strip().lower())
    await prop(subject, "chain", f"evm:{chain_id}")
    try:
        bal = int(bundle.get("balance_wei") or 0)
        await prop(subject, "balance_eth", f"{bal / _WEI:.6f}")
    except (ValueError, TypeError):
        pass
    ident = _parse_source(bundle.get("source"))
    for name in ("is_contract", "contract_name", "proxy_implementation"):
        if name in ident:
            await prop(subject, name, ident[name])

    cps = aggregate_counterparties(
        address, bundle.get("txs", []), bundle.get("token_txs", []), top=top
    )
    n_link = 0
    for c in cps:
        other = await actions.create_or_find_object(
            "CryptoAddress", _addr_canonical(chain_id, c.address), _SOURCE, case_id,
            hop_distance=1,
        )
        await prop(other, "address", c.address)
        await prop(other, "chain", f"evm:{chain_id}")
        props = {
            "tx_count": c.tx_count,
            "eth_in": round(c.wei_in / _WEI, 6),
            "eth_out": round(c.wei_out / _WEI, 6),
            "token_in": c.token_in,
            "token_out": c.token_out,
            "tokens": c.tokens,
            "first_seen": c.first_ts,
            "last_seen": c.last_ts,
        }
        if await _link_once(
            actions, subject, other, "transacted_with",
            ts=ts, properties=props, case_id=case_id,
        ):
            n_link += 1
    return {
        "address": address.strip().lower(),
        "subject_id": str(subject),
        "counterparties": len(cps),
        "links": n_link,
        "is_contract": ident.get("is_contract", False),
    }


async def aim_address(
    actions: Actions, address: str, *, chain_id: int = 1, top: int = 25,
    case_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    """Fetch + ingest one address. The crutch-free entry point (MCP / CLI)."""
    key = get_settings().etherscan_api_key
    if not key:
        return {"error": "ETHERSCAN_API_KEY not set"}
    bundle = await fetch_address(address, chain_id=chain_id, key=key)
    return await ingest_address(
        actions, address, bundle, chain_id=chain_id, top=top, case_id=case_id
    )


# --- screen against the federated sanctions base ----------------------------

async def screen_against_sanctions(
    pool: asyncpg.Pool, address_id: uuid.UUID
) -> dict[str, Any]:
    """The crawl×base edge for crypto: flag the traced address, or any counterparty,
    that carries an OpenSanctions provenance — i.e. an OFAC-listed wallet whose
    canonical fused with this trace. Answers 'did my subject move money through a
    sanctioned wallet?'.

    Fusion here is by CANONICAL alignment (an OFAC wallet and the trace create the
    SAME object), not by a merge — so a direct per-node provenance check is exact;
    no merged_into expansion is needed for this path."""
    rows = await pool.fetch(
        "WITH nodes AS ("
        "  SELECT $1::uuid AS id, true AS is_subject "
        "  UNION "
        "  SELECT l.to_id, false FROM links l "
        "  WHERE l.from_id=$1 AND l.type='transacted_with'"
        ") "
        "SELECT DISTINCT n.id, n.is_subject, o.canonical, "
        "  (SELECT value #>> '{}' FROM current_assertions "
        "   WHERE object_id=n.id AND name='address' "
        "   ORDER BY confidence DESC LIMIT 1) AS addr, "
        "  EXISTS (SELECT 1 FROM current_assertions s "
        "          WHERE s.object_id=n.id AND s.source_id='opensanctions') AS sanctioned "
        "FROM nodes n JOIN objects o ON o.id=n.id AND o.status='active'",
        address_id,
    )
    hits: dict[str, dict[str, Any]] = {}
    subject_sanctioned = False
    for r in rows:
        if not r["sanctioned"]:
            continue
        if r["is_subject"]:
            subject_sanctioned = True
        holders = await pool.fetch(
            "SELECT (SELECT value #>> '{}' FROM current_assertions "
            "        WHERE object_id=l.to_id AND name='name' LIMIT 1) AS nm "
            "FROM links l WHERE l.from_id=$1 AND l.type='controlled_by'",
            r["id"],
        )
        key = str(r["id"])
        prev = hits.get(key)
        hits[key] = {
            "address": r["addr"] or r["canonical"],
            "canonical": r["canonical"],
            "is_subject": (prev["is_subject"] if prev else False) or r["is_subject"],
            "holders": [h["nm"] for h in holders if h["nm"]],
        }
    return {
        "address_id": str(address_id),
        "subject_sanctioned": subject_sanctioned,
        "sanctioned_hits": list(hits.values()),
    }


def main() -> None:
    argv = sys.argv[1:]
    if not argv:
        print("usage: python -m src.ingest.etherscan 0xADDRESS [chainid] [top]")
        return
    address = argv[0]
    chain_id = int(argv[1]) if len(argv) > 1 else 1
    top = int(argv[2]) if len(argv) > 2 else 25

    async def run() -> None:
        pool = await create_pool(get_settings().database_url)
        try:
            out = await aim_address(Actions(pool), address, chain_id=chain_id, top=top)
            print(f"traced: {out}")
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":
    main()
