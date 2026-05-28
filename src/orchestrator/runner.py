"""Synchronous helper runner (Phase 2).

Claims a helper_run (atomic, via the active-claim partial unique index),
resolves a parser's ParseResult against the graph, and applies it through the
Actions layer — every emitted object/property/link is audited and cascades via
the outbox. The async router, token buckets, and routing tiers are Phase 3;
here the response is fetched by the caller and handed in directly.
"""

from __future__ import annotations

import uuid
from typing import Any, cast

from src.actions.core import Actions
from src.config.settings import get_settings
from src.connectors.store import ArtifactStore
from src.orchestrator.manifests import Manifest
from src.parsers import get_parser
from src.parsers.base import InputObject, ParseResult, TargetRef


class HelperRunError(Exception):
    pass


async def claim_run(
    actions: Actions,
    helper_id: str,
    object_id: uuid.UUID,
    case_id: uuid.UUID,
    tier: str,
    *,
    status: str = "running",
) -> uuid.UUID | None:
    """Atomically claim a run. Returns the run id, or None if one is already
    active for this (helper, object, case) — the partial unique index decides.
    `status` lets a gated dispatch claim directly into 'awaiting_human'."""
    row = await actions.pool.fetchrow(
        "INSERT INTO helper_runs (helper_id, object_id, case_id, status, tier) "
        "VALUES ($1,$2,$3,$5,$4) "
        "ON CONFLICT DO NOTHING RETURNING id",
        helper_id,
        object_id,
        case_id,
        tier,
        status,
    )
    return row["id"] if row is not None else None


async def load_input_object(pool: Any, object_id: uuid.UUID) -> InputObject:
    """Materialize the InputObject a helper consumes (type, canonical, current
    property names) — shared by the cascade and the handoff resume path."""
    row = await pool.fetchrow("SELECT type, canonical FROM objects WHERE id=$1", object_id)
    props = await pool.fetch(
        "SELECT DISTINCT name FROM current_assertions WHERE object_id=$1", object_id
    )
    return InputObject(
        id=str(object_id),
        type=row["type"],
        canonical=row["canonical"],
        properties={r["name"]: None for r in props},
    )


async def _resolve(actions: Actions, ref: TargetRef, ids: dict[str, uuid.UUID],
                   input_id: uuid.UUID) -> uuid.UUID | None:
    if ref.input:
        return input_id
    if ref.ref is not None:
        return ids.get(ref.ref)
    if ref.external_id is not None:
        return cast(
            "uuid.UUID | None",
            await actions.pool.fetchval(
                "SELECT object_id FROM current_assertions "
                "WHERE name='external_id' AND value #>> '{}' = $1",
                ref.external_id,
            ),
        )
    if ref.attack_name is not None:
        return cast(
            "uuid.UUID | None",
            await actions.pool.fetchval(
                "SELECT object_id FROM current_assertions "
                "WHERE (name='name' AND value #>> '{}' = $1) "
                "   OR (name='aliases' AND value ? $1) LIMIT 1",
                ref.attack_name,
            ),
        )
    return None


async def apply_result(
    actions: Actions,
    result: ParseResult,
    *,
    source_id: str,
    input_object: InputObject,
    case_id: uuid.UUID,
    helper_run_id: uuid.UUID,
    child_hop: int = 0,
) -> dict[str, int]:
    settings = get_settings()
    store = ArtifactStore(settings.osiris_artifact_dir)
    observed_at = result.observed_at
    if observed_at is None:
        raise HelperRunError("parser must set observed_at (authoritative clock)")

    input_id = uuid.UUID(input_object.id)
    ids: dict[str, uuid.UUID] = {input_object.canonical: input_id}
    n_obj = n_prop = n_link = 0

    for spec in result.objects:
        obj_id = await actions.create_or_find_object(
            spec.type, spec.canonical, source_id, case_id, hop_distance=child_hop
        )
        ids[spec.canonical] = obj_id
        n_obj += 1
        evidence_uri = evidence_sha = None
        if spec.evidence is not None:
            evidence_uri, evidence_sha = store.put_json(spec.evidence)
        for name, value in spec.properties.items():
            if value is None:
                continue  # a None property is "unknown", not a fact — don't assert it
            await actions.assert_property(
                obj_id, name, value, source_id, observed_at, spec.confidence,
                case_id=case_id, helper_run_id=helper_run_id,
                evidence_uri=evidence_uri, evidence_sha256=evidence_sha,
            )
            n_prop += 1

    for link in result.links:
        from_id = await _resolve(actions, link.from_ref, ids, input_id)
        to_id = await _resolve(actions, link.to_ref, ids, input_id)
        if from_id is None or to_id is None:
            continue  # unresolved endpoint (e.g. ATT&CK object not ingested) — skip
        await actions.create_link(
            from_id, to_id, link.type, source_id, observed_at, link.confidence,
            case_id=case_id, helper_run_id=helper_run_id,
        )
        n_link += 1

    return {"objects": n_obj, "properties": n_prop, "links": n_link}


async def execute_claimed(
    actions: Actions,
    manifest: Manifest,
    response: dict[str, Any],
    input_object: InputObject,
    case_id: uuid.UUID,
    run_id: uuid.UUID,
    *,
    input_hop: int = 0,
) -> dict[str, int]:
    """Parse -> apply -> finalize for an already-claimed run. Emitted objects are
    placed one hop beyond the consumed object. Marks the run done/failed."""
    try:
        parser = get_parser(manifest.parser)
        result = parser(response, input_object)
        counts = await apply_result(
            actions, result, source_id=manifest.id, input_object=input_object,
            case_id=case_id, helper_run_id=run_id, child_hop=input_hop + 1,
        )
        await actions.pool.execute(
            "UPDATE helper_runs SET status='done', finished_at=now(), result=$2 WHERE id=$1",
            run_id, counts,
        )
        return counts
    except Exception as exc:
        await actions.pool.execute(
            "UPDATE helper_runs SET status='failed', finished_at=now(), error=$2 WHERE id=$1",
            run_id, str(exc),
        )
        raise


async def run_helper(
    actions: Actions,
    manifest: Manifest,
    response: dict[str, Any],
    input_object: InputObject,
    case_id: uuid.UUID,
    *,
    input_hop: int = 0,
) -> dict[str, int]:
    """Claim -> execute (the standalone synchronous path)."""
    object_id = uuid.UUID(input_object.id)
    run_id = await claim_run(actions, manifest.id, object_id, case_id, manifest.tier)
    if run_id is None:
        raise HelperRunError(f"{manifest.id} already running on {object_id}")
    return await execute_claimed(
        actions, manifest, response, input_object, case_id, run_id, input_hop=input_hop
    )
