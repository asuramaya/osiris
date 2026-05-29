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

import asyncio
import json as _json
import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from src.actions.core import Actions
from src.config.settings import get_settings
from src.connectors.leases import LeaseStore
from src.connectors.osint4all import suggest_manifests
from src.connectors.registry import CONNECTORS
from src.connectors.searxng import search_manifests, searxng_search
from src.db.pool import create_pool
from src.db.redis import create_redis
from src.dissemination.brief import build_case_brief
from src.ontology.classify import classify
from src.ontology.intake import intake
from src.ontology.resolution import (
    find_person_merge_candidates,
    resolve_candidate,
    review_tray,
)
from src.orchestrator.budgets import BudgetLedger
from src.orchestrator.cascade import CascadeContext, expand_case
from src.orchestrator.cobrowse import cobrowse_open
from src.orchestrator.federation import federated_query, promote, to_preview
from src.orchestrator.handoff import abandon, open_handoff, post_back
from src.orchestrator.handoff import tray as handoff_tray
from src.orchestrator.manifests import load_manifests, project_triggers
from src.orchestrator.ratelimit import RateLimiter
from src.orchestrator.runner import load_input_object

_HELPERS_DIR = Path(__file__).resolve().parent.parent.parent / "helpers"
_UI_DIR = Path(__file__).resolve().parent.parent / "ui" / "static"


_log = logging.getLogger("osiris.api")


def _on_expand_done(task: asyncio.Task[Any]) -> None:
    """Surface background-expand failures instead of swallowing them."""
    if not task.cancelled() and task.exception() is not None:
        _log.error("background expand failed: %r", task.exception())


def get_pool(request: Request) -> asyncpg.Pool:
    pool: asyncpg.Pool = request.app.state.pool
    return pool


async def compute_stats(pool: asyncpg.Pool, redis: Any, case_id: uuid.UUID) -> dict[str, Any]:
    by_type = {
        r["type"]: r["n"]
        for r in await pool.fetch(
            "SELECT o.type, count(*) AS n FROM objects o "
            "JOIN case_objects co ON co.object_id = o.id "
            "WHERE co.case_id = $1 GROUP BY o.type ORDER BY n DESC",
            case_id,
        )
    }
    pending = await pool.fetchval(
        "SELECT count(*) FROM handoffs h JOIN helper_runs r ON r.id = h.helper_run_id "
        "WHERE h.case_id = $1 AND h.resolved_at IS NULL AND r.status = 'awaiting_human'",
        case_id,
    )
    rate_left = await redis.get(f"budget:{case_id}:rate")
    return {
        "by_type": by_type,
        "total": sum(by_type.values()),
        "pending_handoffs": pending,
        "rate_credits_remaining": int(rate_left) if rate_left is not None else None,
    }


def create_app(pool: asyncpg.Pool | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        settings = get_settings()
        own = pool is None
        app.state.pool = pool or await create_pool(settings.database_url)
        # manifests = file helpers + search-engine dorking + osint4all suggest sources
        searches = search_manifests()
        app.state.manifests = {**load_manifests(_HELPERS_DIR), **searches, **suggest_manifests()}
        app.state.connectors = {**CONNECTORS, **{hid: searxng_search for hid in searches}}
        app.state.redis = create_redis(settings.redis_url)
        app.state.tasks = set()  # holds background expand tasks so they aren't GC'd
        # triggers are a projection of manifests (#5) — (re)project on startup so a
        # fresh deployment actually fires helpers (else Expand finds no triggers).
        await project_triggers(app.state.pool, app.state.manifests)
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
    async def case_expand(case_id: uuid.UUID, request: Request) -> dict[str, bool]:
        """Fire helpers across the case in the BACKGROUND and return immediately;
        the SSE stream surfaces entities as they arrive (non-blocking expand)."""
        ctx = CascadeContext(
            actions=Actions(request.app.state.pool),
            limiter=RateLimiter(request.app.state.redis),
            ledger=BudgetLedger(request.app.state.pool, request.app.state.redis),
            manifests=request.app.state.manifests,
            connectors=request.app.state.connectors,
        )
        task = asyncio.create_task(expand_case(ctx, case_id))
        request.app.state.tasks.add(task)
        task.add_done_callback(_on_expand_done)
        return {"started": True}

    @app.patch("/cases/{case_id}")
    async def patch_case(
        case_id: uuid.UUID, body: CasePatch, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, bool]:
        if body.name is not None:
            await p.execute("UPDATE cases SET name=$2 WHERE id=$1", case_id, body.name)
        if body.budgets is not None:
            await p.execute("UPDATE cases SET budgets=$2 WHERE id=$1", case_id, body.budgets)
        return {"updated": True}

    @app.post("/cases/{case_id}/archive")
    async def archive_case(
        case_id: uuid.UUID, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, bool]:
        await p.execute("UPDATE cases SET archived_at=now() WHERE id=$1", case_id)
        return {"archived": True}

    @app.get("/cases/{case_id}")
    async def get_case(case_id: uuid.UUID, p: asyncpg.Pool = Depends(get_pool)) -> dict[str, Any]:
        row = await p.fetchrow(
            "SELECT id, name, owner, budgets FROM cases WHERE id=$1", case_id
        )
        if row is None:
            raise HTTPException(404, "case not found")
        return dict(row)

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
            "WHERE status NOT IN ('archived','merged') "
            "  AND ($1::uuid IS NULL OR EXISTS (SELECT 1 FROM case_objects co "
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

    # --- node management (all via the audited Actions layer) ----------------
    @app.post("/objects/{object_id}/properties")
    async def add_property(
        object_id: uuid.UUID, body: PropertyBody, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, int]:
        aid = await Actions(p).assert_property(
            object_id, body.name, body.value, get_settings().osiris_actor,
            datetime.now(UTC), 1.0,
        )
        return {"assertion_id": aid}

    @app.post("/objects/{object_id}/tags")
    async def add_tag(
        object_id: uuid.UUID, body: TagBody, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, str]:
        await Actions(p).tag_object(object_id, body.tag, body.scope, get_settings().osiris_actor)
        return {"tagged": body.tag}

    @app.post("/objects/{object_id}/subject")
    async def mark_subject(
        object_id: uuid.UUID, body: SubjectBody, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, str]:
        """Pin an object as the case subject ('this is me'). Tags it, mints/links a
        per-case Person hub (subject:<case_id>) when the object is an identity
        fragment, and seeds Person merge-candidate generation so the hub starts
        attracting matches in the review tray."""
        actor = get_settings().osiris_actor
        row = await p.fetchrow("SELECT type FROM objects WHERE id=$1", object_id)
        if row is None:
            raise HTTPException(status_code=404, detail="object not found")
        await Actions(p).tag_object(object_id, "subject", "case", actor, case_id=body.case_id)
        hub_id = await Actions(p).create_or_find_object(
            "Person", f"subject:{body.case_id}", actor, body.case_id
        )
        await Actions(p).tag_object(hub_id, "subject", "case", actor, case_id=body.case_id)
        link_type = {"Account": "has_account", "Email": "has_email",
                     "Username": "has_username"}.get(row["type"])
        if link_type and object_id != hub_id:
            exists = await p.fetchval(
                "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type=$3 LIMIT 1",
                hub_id, object_id, link_type,
            )
            if not exists:
                await Actions(p).create_link(
                    hub_id, object_id, link_type, actor, datetime.now(UTC), 1.0,
                    case_id=body.case_id,
                )
        await find_person_merge_candidates(p)
        return {"subject": str(object_id), "hub": str(hub_id)}

    @app.post("/objects/{object_id}/archive")
    async def archive_object(
        object_id: uuid.UUID, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, str]:
        await Actions(p).set_status(
            object_id, "archived", "analyst archived", get_settings().osiris_actor
        )
        return {"status": "archived"}

    @app.post("/links")
    async def create_link_ep(body: LinkBody, p: asyncpg.Pool = Depends(get_pool)) -> dict[str, int]:
        lid = await Actions(p).create_link(
            body.from_id, body.to_id, body.type, get_settings().osiris_actor,
            datetime.now(UTC), 1.0,
        )
        return {"link_id": lid}

    @app.get("/objects/{object_id}/graph")
    async def object_graph(
        object_id: uuid.UUID,
        p: asyncpg.Pool = Depends(get_pool),
        hops: int = Query(1, ge=0, le=12),
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

    @app.post("/merge-candidates/{candidate_id}/resolve")
    async def resolve_merge(
        candidate_id: int, body: ResolveBody, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, str]:
        await resolve_candidate(
            Actions(p), candidate_id, body.decision, get_settings().osiris_actor
        )
        return {"resolved": body.decision}

    @app.post("/handoffs/{handoff_id}/open")
    async def handoff_open(handoff_id: int, p: asyncpg.Pool = Depends(get_pool)) -> dict[str, str]:
        await open_handoff(Actions(p), handoff_id)
        return {"status": "in_browser"}

    @app.post("/handoffs/{handoff_id}/abandon")
    async def handoff_abandon(
        handoff_id: int, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, str]:
        await abandon(Actions(p), handoff_id)
        return {"status": "abandoned"}

    @app.post("/handoffs/{handoff_id}/postback")
    async def handoff_postback(
        handoff_id: int, body: PostbackBody, request: Request
    ) -> dict[str, int]:
        pool = request.app.state.pool
        helper_id = await pool.fetchval("SELECT helper_id FROM handoffs WHERE id=$1", handoff_id)
        manifest = request.app.state.manifests.get(helper_id)
        if manifest is None:
            raise HTTPException(404, f"no manifest for handoff helper {helper_id!r}")
        return await post_back(Actions(pool), manifest, handoff_id, body.result)

    @app.get("/cases/{case_id}/stats")
    async def case_stats(case_id: uuid.UUID, request: Request) -> dict[str, Any]:
        return await compute_stats(request.app.state.pool, request.app.state.redis, case_id)

    @app.get("/cases/{case_id}/stream")
    async def case_stream(case_id: uuid.UUID, request: Request) -> StreamingResponse:
        """SSE: push case stats as they change (DESIGN §4 — one-way, SSE). The UI
        watches this so the graph/badges update live as the cascade expands."""
        async def gen() -> AsyncIterator[str]:
            last = ""
            while not await request.is_disconnected():
                stats = await compute_stats(
                    request.app.state.pool, request.app.state.redis, case_id
                )
                payload = _json.dumps(stats)
                if payload != last:
                    yield f"data: {payload}\n\n"
                    last = payload
                else:
                    yield ": keep-alive\n\n"
                await asyncio.sleep(1.5)

        return StreamingResponse(gen(), media_type="text/event-stream")

    @app.post("/handoffs/{handoff_id}/cobrowse")
    async def handoff_cobrowse(handoff_id: int, request: Request) -> dict[str, Any]:
        """Drive a real browser to the parked URL, capture the session as a lease,
        and return a summary for the analyst to review."""
        return await cobrowse_open(
            Actions(request.app.state.pool),
            LeaseStore(request.app.state.pool),
            handoff_id,
            bound_ip="127.0.0.1",
            issued_by=get_settings().osiris_actor,
        )

    @app.get("/cases/{case_id}/brief.pdf")
    async def case_brief(case_id: uuid.UUID, request: Request) -> Response:
        pdf = await build_case_brief(
            request.app.state.pool, case_id, generated_at=datetime.now(UTC)
        )
        return Response(content=pdf, media_type="application/pdf")

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


class PropertyBody(BaseModel):
    name: str
    value: Any


class TagBody(BaseModel):
    tag: str
    scope: str = "case"


class SubjectBody(BaseModel):
    case_id: uuid.UUID


class LinkBody(BaseModel):
    from_id: uuid.UUID
    to_id: uuid.UUID
    type: str


class CasePatch(BaseModel):
    name: str | None = None
    budgets: dict[str, Any] | None = None


class ResolveBody(BaseModel):
    decision: str  # 'merged' | 'rejected'


class PostbackBody(BaseModel):
    result: dict[str, Any]


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
