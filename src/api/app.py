"""Object Set Service — REST read/query surface over the typed graph (DESIGN §2.4).

Powers the Cytoscape/MapLibre UI: object search, an object's current (multi-source)
properties, an N-hop subgraph for the canvas, the helpers available on an object
(from the manifest registry, not hardcoded — DESIGN §8 UI), the handoff + merge
review trays, and time-travel snapshots ("what did we know on Tuesday", §12) which
the event-sourced design makes a bounded query.

Mutations stay in the Actions layer; this surface is read-only plus a couple of
analyst decisions (resolve a merge candidate, resolve a handoff) that themselves
go through actions/services.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncpg
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from src.actions.core import Actions
from src.config.settings import get_settings
from src.connectors.registry import CONNECTORS
from src.db.pool import create_pool
from src.db.redis import create_redis
from src.ontology.classify import classify
from src.ontology.intake import intake
from src.ontology.resolution import review_tray
from src.orchestrator.budgets import BudgetLedger
from src.orchestrator.cascade import CascadeContext, run_cascade
from src.orchestrator.federation import federated_query, promote, to_preview
from src.orchestrator.handoff import tray as handoff_tray
from src.orchestrator.manifests import load_manifests
from src.orchestrator.ratelimit import RateLimiter
from src.orchestrator.runner import load_input_object

_HELPERS_DIR = Path(__file__).resolve().parent.parent.parent / "helpers"
_UI_DIR = Path(__file__).resolve().parent.parent / "ui" / "static"


def get_pool(request: Request) -> asyncpg.Pool:
    pool: asyncpg.Pool = request.app.state.pool
    return pool


def create_app(pool: asyncpg.Pool | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        settings = get_settings()
        own = pool is None
        app.state.pool = pool or await create_pool(settings.database_url)
        app.state.manifests = load_manifests(_HELPERS_DIR)
        app.state.connectors = dict(CONNECTORS)
        app.state.redis = create_redis(settings.redis_url)
        try:
            yield
        finally:
            await app.state.redis.aclose()
            if own:
                await app.state.pool.close()

    app = FastAPI(title="Osiris Object Set Service", lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/cases")
    async def create_case(body: NewCaseBody, p: asyncpg.Pool = Depends(get_pool)) -> dict[str, Any]:
        cid = await p.fetchval(
            "INSERT INTO cases (name, owner, budgets) VALUES ($1,$2,$3) RETURNING id",
            body.name,
            get_settings().osiris_actor,
            body.budgets or {},
        )
        return {"id": str(cid), "name": body.name}

    @app.post("/cases/{case_id}/intake")
    async def case_intake(
        case_id: uuid.UUID, body: IntakeBody, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, Any]:
        """Paste anything: classify it, create the typed object in the case."""
        type_ = body.type or classify(body.raw)
        object_id = await intake(Actions(p), type_, body.raw, get_settings().osiris_actor, case_id)
        return {"object_id": str(object_id), "type": type_, "canonical": body.raw.strip()}

    @app.post("/cases/{case_id}/expand")
    async def case_expand(case_id: uuid.UUID, request: Request) -> dict[str, int]:
        """Drain the outbox once — fire matching helpers across the case, bounded
        by its budgets. Cached fetches make repeated expansion cheap."""
        ctx = CascadeContext(
            actions=Actions(request.app.state.pool),
            limiter=RateLimiter(request.app.state.redis),
            ledger=BudgetLedger(request.app.state.pool, request.app.state.redis),
            manifests=request.app.state.manifests,
            connectors=request.app.state.connectors,
        )
        return {"processed": await run_cascade(ctx)}

    @app.get("/cases")
    async def list_cases(p: asyncpg.Pool = Depends(get_pool)) -> list[dict[str, Any]]:
        rows = await p.fetch(
            "SELECT c.id, c.name, c.owner, count(DISTINCT co.object_id) AS object_count "
            "FROM cases c LEFT JOIN case_objects co ON co.case_id = c.id "
            "WHERE c.archived_at IS NULL "
            "GROUP BY c.id ORDER BY c.created_at DESC"
        )
        return [dict(r) for r in rows]

    @app.get("/objects")
    async def list_objects(
        p: asyncpg.Pool = Depends(get_pool),
        case_id: uuid.UUID | None = None,
        type: str | None = None,
        q: str | None = None,
        limit: int = Query(100, le=500),
    ) -> list[dict[str, Any]]:
        rows = await p.fetch(
            "SELECT id, type, canonical, status FROM objects o "
            "WHERE ($1::uuid IS NULL OR EXISTS (SELECT 1 FROM case_objects co "
            "        WHERE co.object_id = o.id AND co.case_id = $1)) "
            "  AND ($2::text IS NULL OR type = $2) "
            "  AND ($3::text IS NULL OR canonical ILIKE '%' || $3 || '%') "
            "ORDER BY type, created_at DESC LIMIT $4",
            case_id,
            type,
            q,
            limit,
        )
        return [dict(r) for r in rows]

    @app.get("/objects/{object_id}")
    async def get_object(
        object_id: uuid.UUID, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, Any]:
        obj = await p.fetchrow(
            "SELECT id, type, canonical, status, merged_into FROM objects WHERE id=$1", object_id
        )
        if obj is None:
            raise HTTPException(404, "object not found")
        props = await p.fetch(
            "SELECT name, value, source_id, confidence FROM current_assertions "
            "WHERE object_id=$1 ORDER BY name, source_id",
            object_id,
        )
        return {**dict(obj), "properties": [dict(r) for r in props]}

    @app.get("/objects/{object_id}/graph")
    async def object_graph(
        object_id: uuid.UUID,
        p: asyncpg.Pool = Depends(get_pool),
        hops: int = Query(1, ge=0, le=4),
    ) -> dict[str, list[dict[str, Any]]]:
        seen: set[uuid.UUID] = {object_id}
        frontier: set[uuid.UUID] = {object_id}
        for _ in range(hops):
            if not frontier:
                break
            rows = await p.fetch(
                "SELECT to_id AS n FROM links WHERE from_id = ANY($1::uuid[]) "
                "UNION SELECT from_id AS n FROM links WHERE to_id = ANY($1::uuid[])",
                list(frontier),
            )
            nxt = {r["n"] for r in rows} - seen
            seen |= nxt
            frontier = nxt

        node_rows = await p.fetch(
            "SELECT o.id, o.type, o.canonical, "
            "  (SELECT value #>> '{}' FROM current_assertions a "
            "   WHERE a.object_id=o.id AND a.name='name' LIMIT 1) AS name "
            "FROM objects o WHERE o.id = ANY($1::uuid[])",
            list(seen),
        )
        nodes = [
            {"id": str(r["id"]), "type": r["type"], "label": r["name"] or r["canonical"]}
            for r in node_rows
        ]
        edge_rows = await p.fetch(
            "SELECT from_id, to_id, type FROM links "
            "WHERE from_id = ANY($1::uuid[]) AND to_id = ANY($1::uuid[])",
            list(seen),
        )
        edges = [
            {"source": str(r["from_id"]), "target": str(r["to_id"]), "type": r["type"]}
            for r in edge_rows
        ]
        return {"nodes": nodes, "edges": edges}

    @app.get("/objects/{object_id}/helpers")
    async def available_helpers(
        object_id: uuid.UUID, request: Request, p: asyncpg.Pool = Depends(get_pool)
    ) -> list[dict[str, Any]]:
        obj = await p.fetchrow("SELECT type FROM objects WHERE id=$1", object_id)
        if obj is None:
            raise HTTPException(404, "object not found")
        manifests = request.app.state.manifests
        return [
            {"id": m.id, "name": m.name, "tier": m.tier, "description": m.description}
            for m in manifests.values()
            if m.consumes.type == obj["type"] and m.enabled
        ]

    @app.get("/cases/{case_id}/tray")
    async def case_tray(
        case_id: uuid.UUID, p: asyncpg.Pool = Depends(get_pool)
    ) -> list[dict[str, Any]]:
        return await handoff_tray(Actions(p), case_id=case_id)

    @app.get("/merge-candidates")
    async def merge_candidates(p: asyncpg.Pool = Depends(get_pool)) -> list[dict[str, Any]]:
        return await review_tray(p)

    @app.get("/cases/{case_id}/snapshot")
    async def snapshot(
        case_id: uuid.UUID,
        at: str,
        p: asyncpg.Pool = Depends(get_pool),
    ) -> dict[str, Any]:
        """Objects in the case that existed at time `at` (ISO-8601), with their
        property values as of then — a bounded query over the append-only ledger."""
        try:
            as_of = datetime.fromisoformat(at)
        except ValueError as exc:
            raise HTTPException(422, f"invalid 'at' timestamp: {at}") from exc
        rows = await p.fetch(
            "SELECT o.id, o.type, o.canonical FROM objects o "
            "JOIN case_objects co ON co.object_id = o.id AND co.case_id = $1 "
            "WHERE EXISTS (SELECT 1 FROM object_events e WHERE e.object_id=o.id "
            "  AND e.event_type='create' AND e.created_at <= $2)",
            case_id,
            as_of,
        )
        return {"as_of": at, "objects": [dict(r) for r in rows]}

    @app.post("/federate")
    async def federate(body: FederateBody, request: Request) -> dict[str, Any]:
        """Query a source against an object IN PLACE — returns a preview, no writes."""
        manifest, connector, input_object = await _federation_ctx(request, body)
        result = await federated_query(
            request.app.state.pool, connector, manifest.parser, input_object,
            helper_id=manifest.id, cache_ttl=manifest.cache_ttl,
        )
        return to_preview(result)

    @app.post("/promote")
    async def promote_endpoint(body: PromoteBody, request: Request) -> dict[str, int]:
        """Materialize the selected previewed results into the case graph."""
        manifest, connector, input_object = await _federation_ctx(request, body)
        result = await federated_query(
            request.app.state.pool, connector, manifest.parser, input_object,
            helper_id=manifest.id, cache_ttl=manifest.cache_ttl,
        )
        return await promote(
            Actions(request.app.state.pool),
            result,
            source_id=manifest.id,
            input_object=input_object,
            case_id=body.case_id,
            selected=body.selected,
        )

    if _UI_DIR.is_dir():
        app.mount("/ui", StaticFiles(directory=str(_UI_DIR), html=True), name="ui")

    return app


class NewCaseBody(BaseModel):
    name: str
    budgets: dict[str, Any] | None = None


class IntakeBody(BaseModel):
    raw: str
    type: str | None = None


class FederateBody(BaseModel):
    helper_id: str
    object_id: uuid.UUID


class PromoteBody(FederateBody):
    case_id: uuid.UUID
    selected: list[str] | None = None


async def _federation_ctx(request: Request, body: FederateBody) -> tuple[Any, Any, Any]:
    manifest = request.app.state.manifests.get(body.helper_id)
    connector = request.app.state.connectors.get(body.helper_id)
    if manifest is None or connector is None:
        raise HTTPException(404, f"no federatable helper {body.helper_id!r}")
    input_object = await load_input_object(request.app.state.pool, body.object_id)
    return manifest, connector, input_object


app = create_app()
