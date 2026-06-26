"""Promote a federated entity's web presence into crawlable seeds.

A federated record (OpenSanctions / EDGAR / Wikidata) carries its ``website`` /
``domain`` as bare PROPERTIES — facts the keyless crawl cannot act on, because the
cascade fires on typed URL/Domain *objects*, not on assertion values. This is the
seam that bridges the two halves in the direction that actually has shared entity
space (base -> crawl): it mints linked, crawlable URL + Domain objects from those
properties and places them one hop past the entity, so the existing keyless
collectors (url_fetch, crtsh, wayback) enrich the registered entity with its live
open-web footprint — contact emails, social accounts, subdomains — each linked back
with provenance.

The has_url / has_domain links are AUTHORITATIVE_API: the base *declared* this site,
so the minted node is anchor-grade and the anchor-and-pivot frontier lets the cascade
crawl it (a derived guess would have stayed a leaf).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

from src.actions.core import Actions
from src.ontology.canonicalize import canonicalize
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_SOURCE = "web_presence"
_EC = EvidenceClass.AUTHORITATIVE_API.value  # the base declared this website
_CONF = confidence_for(EvidenceClass.AUTHORITATIVE_API)


def _as_values(v: Any) -> list[str]:
    if isinstance(v, list):
        return [str(x) for x in v if x]
    return [str(v)] if v else []


def _normalize_url(raw: str) -> str | None:
    raw = raw.strip()
    if not raw:
        return None
    if not raw.startswith(("http://", "https://")):
        raw = "http://" + raw
    if not urlparse(raw).netloc:
        return None
    return raw.rstrip("/")


def _host(url: str) -> str | None:
    h = urlparse(url).netloc.split("@")[-1].split(":")[0]
    return h or None


async def seed_web_presence(
    actions: Actions,
    object_id: uuid.UUID,
    *,
    case_id: uuid.UUID | None = None,
    observed_at: datetime | None = None,
) -> dict[str, int]:
    """Mint crawlable URL + Domain objects from the entity's website/domain
    properties, linked back and placed one hop deeper so the cascade enriches them.
    Idempotent (find-or-create + the durable claim dedupe the crawl)."""
    pool = actions.pool
    ts = observed_at or datetime.now(UTC)

    child_hop = 1
    if case_id is not None:
        h = await pool.fetchval(
            "SELECT hop_distance FROM case_objects WHERE case_id=$1 AND object_id=$2",
            case_id,
            object_id,
        )
        child_hop = (int(h) if h is not None else 0) + 1

    rows = await pool.fetch(
        "SELECT value FROM current_assertions "
        "WHERE object_id=$1 AND name IN ('website','domain','url')",
        object_id,
    )
    urls: set[str] = set()
    for r in rows:
        # asyncpg decodes jsonb to native Python (str for a scalar, list for a set)
        for v in _as_values(r["value"]):
            u = _normalize_url(v)
            if u:
                urls.add(u)

    n_url = n_dom = 0
    for u in sorted(urls):
        url_id = await actions.create_or_find_object(
            "URL", canonicalize("URL", u), _SOURCE, case_id, hop_distance=child_hop
        )
        await actions.create_link(
            object_id, url_id, "has_url", _SOURCE, ts, _CONF,
            case_id=case_id, evidence_class=_EC,
        )
        n_url += 1
        host = _host(u)
        if host:
            dom_id = await actions.create_or_find_object(
                "Domain", canonicalize("Domain", host), _SOURCE, case_id, hop_distance=child_hop
            )
            await actions.create_link(
                object_id, dom_id, "has_domain", _SOURCE, ts, _CONF,
                case_id=case_id, evidence_class=_EC,
            )
            n_dom += 1
    return {"urls": n_url, "domains": n_dom}
