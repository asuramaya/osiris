"""Federation: query a source in place, promote results on demand (DESIGN §2.4).

The graph isn't only a sink — an analyst can run a helper against an object and
*preview* what it would emit without persisting anything, then promote the
subset they want into the case graph. Promotion goes through the Actions layer
(audited, cascades via outbox) and records a 'manual' helper_run for provenance.
"""

from __future__ import annotations

import uuid
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.connectors.registry import Connector
from src.orchestrator.cache import cached_fetch
from src.orchestrator.runner import apply_result
from src.parsers import get_parser
from src.parsers.base import InputObject, ParseResult, TargetRef


async def federated_query(
    pool: asyncpg.Pool,
    connector: Connector,
    parser_name: str,
    input_object: InputObject,
    *,
    helper_id: str,
    cache_ttl: int = 3600,
) -> ParseResult:
    """Run the source + parser and return the result WITHOUT mutating the graph.
    The fetch is cached, so a later promote() reuses it rather than re-hitting."""
    response = await cached_fetch(pool, connector, helper_id, input_object, cache_ttl=cache_ttl)
    return get_parser(parser_name)(response, input_object)


def _ref_label(ref: TargetRef) -> str:
    if ref.input:
        return "<input>"
    return ref.ref or ref.external_id or ref.attack_name or "?"


def to_preview(result: ParseResult) -> dict[str, Any]:
    """Serialize a (non-persisted) result for the analyst to choose from."""
    return {
        "objects": [
            {"type": o.type, "canonical": o.canonical, "properties": o.properties}
            for o in result.objects
        ],
        "links": [
            {"from": _ref_label(link.from_ref), "to": _ref_label(link.to_ref), "type": link.type}
            for link in result.links
        ],
        "observed_at": result.observed_at.isoformat() if result.observed_at else None,
    }


def _endpoint_in_scope(ref: TargetRef, selected: set[str]) -> bool:
    if ref.input or ref.external_id or ref.attack_name:
        return True  # the input object, or an existing ATT&CK object
    return ref.ref in selected


def _select(result: ParseResult, selected: set[str]) -> ParseResult:
    objects = [o for o in result.objects if o.canonical in selected]
    links = [
        link
        for link in result.links
        if _endpoint_in_scope(link.from_ref, selected)
        and _endpoint_in_scope(link.to_ref, selected)
    ]
    return ParseResult(objects=objects, links=links, observed_at=result.observed_at)


async def promote(
    actions: Actions,
    result: ParseResult,
    *,
    source_id: str,
    input_object: InputObject,
    case_id: uuid.UUID,
    selected: list[str] | None = None,
) -> dict[str, int]:
    """Materialize (a subset of) a previewed result into the graph via Actions."""
    if selected is not None:
        result = _select(result, set(selected))

    input_id = uuid.UUID(input_object.id)
    hop = await actions.pool.fetchval(
        "SELECT hop_distance FROM case_objects WHERE case_id=$1 AND object_id=$2",
        case_id,
        input_id,
    )
    run_id = await actions.pool.fetchval(
        "INSERT INTO helper_runs (helper_id, object_id, case_id, status, tier, finished_at) "
        "VALUES ($1,$2,$3,'done','manual', now()) RETURNING id",
        source_id,
        input_id,
        case_id,
    )
    return await apply_result(
        actions,
        result,
        source_id=source_id,
        input_object=input_object,
        case_id=case_id,
        helper_run_id=run_id,
        child_hop=int(hop or 0) + 1,
    )


__all__ = ["federated_query", "promote", "to_preview"]
