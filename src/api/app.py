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
from arq import create_pool as create_arq_pool
from arq.connections import RedisSettings
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
from src.ingest.harris_foreclosure import demo_fetch, make_harris_foreclosure_watcher
from src.ontology.classify import classify
from src.ontology.intake import intake
from src.ontology.resolution import (
    find_person_merge_candidates,
    resolve_candidate,
    review_tray,
)
from src.ontology.schema import catalog as ontology_catalog
from src.orchestrator.cobrowse import cobrowse_open
from src.orchestrator.compositions import (
    list_compositions,
    run_composition,
    save_composition,
    save_watch,
)
from src.orchestrator.dossier import entity_dossier
from src.orchestrator.federation import federated_query, promote, to_preview
from src.orchestrator.frontier import subject_report
from src.orchestrator.handoff import abandon, open_handoff, post_back
from src.orchestrator.handoff import tray as handoff_tray
from src.orchestrator.manifests import load_manifests, project_triggers
from src.orchestrator.monitor import (
    evaluate_watches,
    match_condition,
    tick,
)
from src.orchestrator.runner import load_input_object

_HELPERS_DIR = Path(__file__).resolve().parent.parent.parent / "helpers"
_UI_DIR = Path(__file__).resolve().parent.parent / "ui" / "static"


_log = logging.getLogger("osiris.api")


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
        # the Arq queue: the API ENQUEUES heavy jobs (e.g. case expansion) onto the
        # worker via this pool instead of running them inline — the worker⊥surface cut.
        app.state.arq = await create_arq_pool(RedisSettings.from_dsn(settings.redis_url))
        # triggers are a projection of manifests (#5) — (re)project on startup so a
        # fresh deployment actually fires helpers (else Expand finds no triggers).
        await project_triggers(app.state.pool, app.state.manifests)
        try:
            yield
        finally:
            await app.state.arq.aclose()
            await app.state.redis.aclose()
            if own:
                await app.state.pool.close()

    app = FastAPI(title="Osiris Object Set Service", lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/schema")
    async def get_schema() -> dict[str, Any]:
        """The semantic layer — the declared Object-Type + Link-Type catalog. Every
        surface reads its types/colours/shapes from here (one source of truth)."""
        return ontology_catalog()

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
    async def case_expand(case_id: uuid.UUID, request: Request) -> dict[str, Any]:
        """ENQUEUE the case expansion onto the worker and return immediately. The
        heavy crawl never runs in the API's event loop (the worker⊥surface cut), so a
        runaway expansion can't block or crash the console. The SSE stream surfaces
        progress, reading the same Postgres the worker writes to."""
        job = await request.app.state.arq.enqueue_job("expand_case_job", str(case_id))
        return {"started": True, "job_id": job.job_id if job else None}

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
            "SELECT id, type, canonical, status, "
            "  (SELECT value #>> '{}' FROM current_assertions a "
            "   WHERE a.object_id=o.id AND a.name='name' LIMIT 1) AS name "
            "FROM objects o "
            "WHERE status NOT IN ('archived','merged') "
            "  AND ($1::uuid IS NULL OR EXISTS (SELECT 1 FROM case_objects co "
            "        WHERE co.object_id = o.id AND co.case_id = $1)) "
            "  AND ($2::text IS NULL OR type = $2) "
            "  AND ($3::text IS NULL OR canonical ILIKE '%' || $3 || '%' "
            "       OR EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
            "          AND a.name='name' AND a.value #>> '{}' ILIKE '%' || $3 || '%')) "
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
            "SELECT name, value, source_id, confidence, evidence_class, observed_at "
            "FROM current_assertions WHERE object_id=$1 ORDER BY name, source_id",
            object_id,
        )
        return {
            **dict(obj),
            "properties": [
                {
                    "name": r["name"], "value": r["value"], "source_id": r["source_id"],
                    "confidence": float(r["confidence"]) if r["confidence"] is not None else None,
                    "evidence_class": r["evidence_class"],
                    "how": _HOW_LABELS.get(r["evidence_class"] or "", r["evidence_class"]),
                    "source_label": _SOURCE_LABELS.get(r["source_id"], r["source_id"]),
                    "observed": r["observed_at"].isoformat() if r["observed_at"] else None,
                }
                for r in props
            ],
        }

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

    @app.get("/objects/{object_id}/subject-report")
    async def subject_report_ep(
        object_id: uuid.UUID, case_id: uuid.UUID, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, Any]:
        """The 'who is this?' answer: identity fragments in the case bucketed into
        Verified Core / Corroborated / Speculative, each annotated with why it is
        believed (evidence_class + source count + confidence)."""
        buckets = await subject_report(p, case_id)
        return {"subject": str(object_id), **buckets}

    @app.get("/objects/{object_id}/dossier")
    async def entity_dossier_ep(
        object_id: uuid.UUID, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, Any]:
        """The 'who is this?' answer for a FEDERATED entity (sanctioned party / PEP /
        company): its identity properties plus its ownership/family/director network,
        each endpoint named. Complements subject-report (the footprint/tier lens)."""
        dossier = await entity_dossier(p, object_id)
        if not dossier:
            raise HTTPException(404, "object not found")
        return dossier

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
        """The merge-review tray, each pair annotated with both sides' label + type so
        the analyst can resolve in place (no raw uuids)."""
        out: list[dict[str, Any]] = []
        for c in await review_tray(p):
            a, b = await _label(p, c["a_id"]), await _label(p, c["b_id"])
            out.append({
                "id": c["id"], "score": float(c["score"]), "reasons": _coerce_json(c["reasons"]),
                "a_id": str(c["a_id"]), "a_label": a["label"], "a_type": a["type"],
                "b_id": str(c["b_id"]), "b_label": b["label"], "b_type": b["type"],
            })
        return out

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

    # --- compositions: the composer's primitive (lenses + watches as one) ----
    @app.get("/compositions")
    async def list_compositions_route(
        p: asyncpg.Pool = Depends(get_pool)
    ) -> list[dict[str, Any]]:
        """Every saved composition (lens + watch) — the user's questions, as objects."""
        return await list_compositions(p)

    @app.post("/compositions")
    async def save_composition_route(
        body: CompositionBody, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, Any]:
        """Save (or fork) a composition — a named op-tree the substrate runs. Authoring is
        usually Claude-over-MCP; this is the human save/fork channel."""
        cid = await save_composition(p, body.name, body.spec, body.kind)
        return {"id": str(cid), "name": body.name}

    @app.post("/compositions/{name}/run")
    async def run_composition_route(
        name: str, body: RunCompositionBody, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, Any]:
        """Run a saved composition (the LENS execution), optionally against a subject.
        Returns its Result — objects / values / rows / data — for the generic renderer."""
        subject = uuid.UUID(body.subject) if body.subject else None
        try:
            return await run_composition(p, name, subject)
        except ValueError as exc:  # e.g. a Function that needs a subject, or a bad op
            return {"error": str(exc)}

    @app.post("/subscriptions")
    async def create_subscription_route(
        body: SubscriptionBody, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, Any]:
        """Save a WATCH — a kind='watch' composition whose `select` spec is the beat. The
        same spec runs as a lens (current members) and drives the tripwire (alert on a new
        member). The posted criteria (object_type + where) become that select spec."""
        wid = await save_watch(
            p, body.name, body.criteria.get("object_type"),
            body.criteria.get("where", []) or [], webhook_url=body.webhook_url,
        )
        return {"id": str(wid), "name": body.name}

    @app.get("/subscriptions")
    async def list_subscriptions(p: asyncpg.Pool = Depends(get_pool)) -> list[dict[str, Any]]:
        """The active watches. `criteria` is reconstructed from the select spec so the
        watch console keeps its shape; the storage is a composition underneath."""
        rows = await p.fetch(
            "SELECT id, name, spec, webhook_url, active, created_at "
            "FROM compositions WHERE kind='watch' ORDER BY created_at DESC"
        )
        out: list[dict[str, Any]] = []
        for r in rows:
            spec = _coerce_json(r["spec"]) or {}
            out.append({
                "id": str(r["id"]), "name": r["name"],
                "criteria": {"object_type": spec.get("object_type"),
                             "where": spec.get("where", [])},
                "webhook_url": r["webhook_url"], "active": r["active"],
                "created_at": r["created_at"].isoformat(),
            })
        return out

    @app.get("/alerts")
    async def list_alerts(
        subscription_id: uuid.UUID | None = None,
        limit: int = Query(100, le=1000),
        p: asyncpg.Pool = Depends(get_pool),
    ) -> list[dict[str, Any]]:
        """The dumb sink, read back: fired alerts newest-first (optionally one watch)."""
        rows = await p.fetch(
            "SELECT a.id, a.composition_id, c.name AS subscription, a.object_id, "
            "       a.event_type, a.matched, a.created_at, a.delivered_at "
            "FROM alerts a JOIN compositions c ON c.id = a.composition_id "
            "WHERE ($1::uuid IS NULL OR a.composition_id = $1) "
            "ORDER BY a.created_at DESC LIMIT $2",
            subscription_id,
            limit,
        )
        return [
            {
                "id": str(r["id"]), "subscription_id": str(r["composition_id"]),
                "subscription": r["subscription"],
                "object_id": str(r["object_id"]) if r["object_id"] else None,
                "event_type": r["event_type"], "matched": _coerce_json(r["matched"]),
                "created_at": r["created_at"].isoformat(),
                "delivered_at": r["delivered_at"].isoformat() if r["delivered_at"] else None,
            }
            for r in rows
        ]

    @app.get("/matches")
    async def watch_matches(
        subscription_id: uuid.UUID,
        limit: int = Query(200, le=1000),
        p: asyncpg.Pool = Depends(get_pool),
    ) -> list[dict[str, Any]]:
        """The FEED for a watch: the objects currently matching its criteria, each
        rendered GENERICALLY as a sourced card (type · graded properties · provenance).
        Type-driven, not vertical — a Property reads like a foreclosure lead and an
        Organization like a company because the DATA is, never because this surface
        knows the words. The same console serves any beat over the public record."""
        spec = _coerce_json(
            await p.fetchval(
                "SELECT spec FROM compositions WHERE id=$1 AND kind='watch'", subscription_id
            )
        )
        if spec is None:
            raise HTTPException(404, "no such watch")
        object_type = spec.get("object_type")
        where = spec.get("where", []) or []
        rows = await p.fetch(
            "SELECT id FROM objects WHERE status='active' AND ($1::text IS NULL OR type=$1)",
            object_type,
        )
        cards: list[dict[str, Any]] = []
        for r in rows:
            card = await _object_card(p, r["id"])
            if card is None:
                continue
            facts = {pp["name"]: pp["value"] for pp in card["properties"]}
            facts["name"] = card["title"]
            if all(match_condition(facts.get(c.get("property")), c.get("op", "contains"),
                                   c.get("value")) for c in where):
                cards.append(card)
        cards.sort(key=lambda c: c["provenance"]["observed"] or "", reverse=True)
        return cards[:limit]

    @app.post("/demo/foreclosure-seed")
    async def demo_foreclosure_seed(p: asyncpg.Pool = Depends(get_pool)) -> dict[str, Any]:
        """Demo LOADER (clearly namespaced /demo/). Ingests the SYNTHETIC Harris County
        notices and ensures one demo watch so the generic feed has something to show. The
        foreclosure vertical lives ONLY here — the watch console and /matches never name it."""
        watch_id = await save_watch(
            p, "Harris County foreclosures (demo)", "Property",
            [{"property": "county", "op": "eq", "value": "Harris"}],
        )
        watcher = make_harris_foreclosure_watcher(fetch=demo_fetch)
        ingested = await tick(Actions(p), "harris_county_clerk", watcher)
        fired = await evaluate_watches(p)  # also raise alerts (the bell), prospectively
        return {"ingested": ingested, "alerts_fired": fired, "watch_id": str(watch_id)}

    if _UI_DIR.is_dir():
        app.mount("/ui", StaticFiles(directory=str(_UI_DIR), html=True), name="ui")

    return app


# Friendly labels for provenance display — generic over every source/class, no vertical.
_SOURCE_LABELS = {
    "harris_county_clerk": "Harris County Clerk",
    "edgar": "SEC EDGAR", "harris_county_clerk_demo": "Harris County Clerk",
}
_HOW_LABELS = {
    "authoritative_api": "authoritative record",
    "self_declared": "self-declared",
    "direct_observation": "directly observed",
    "co_occurrence": "co-occurrence (unconfirmed)",
    "derived": "inferred",
    "corroborated": "corroborated by ≥2 sources",
}
# strength order so a card's provenance reports its STRONGEST evidence (mirrors evidence.py).
_EC_STRENGTH = {"co_occurrence": 0, "derived": 1, "direct_observation": 2,
                "authoritative_api": 3, "self_declared": 4, "corroborated": 5}


async def _object_card(p: asyncpg.Pool, object_id: uuid.UUID) -> dict[str, Any] | None:
    """Render ANY object as a generic sourced card: its type, a title, its current
    graded properties as key/values, and a provenance block (sources · how · date ·
    confidence). The renderer is type-driven — it knows nothing about foreclosures or
    any vertical; the domain shows through entirely from the data."""
    o = await p.fetchrow("SELECT type, canonical FROM objects WHERE id=$1", object_id)
    if o is None:
        return None
    rows = await p.fetch(
        "SELECT DISTINCT ON (name) name, value #>> '{}' AS v, source_id, evidence_class, "
        "  confidence, observed_at FROM current_assertions WHERE object_id=$1 "
        "ORDER BY name, observed_at DESC",
        object_id,
    )
    title = o["canonical"]
    props: list[dict[str, Any]] = []
    sources: set[str] = set()
    strongest: str | None = None
    confidence: float | None = None
    observed: datetime | None = None
    demo = False
    for r in rows:
        name = r["name"]
        if name == "tag":
            continue
        if name == "demo":
            demo = str(r["v"]).lower() == "true"
            continue
        if name == "name":
            title = r["v"] or title
        else:
            props.append({"name": name, "value": r["v"]})
        if r["source_id"]:
            sources.add(r["source_id"])
        ec = r["evidence_class"]
        if ec and (strongest is None or _EC_STRENGTH.get(ec, -1) > _EC_STRENGTH.get(strongest, -1)):
            strongest = ec
        if r["confidence"] is not None:
            confidence = max(confidence or 0.0, float(r["confidence"]))
        if r["observed_at"] and (observed is None or r["observed_at"] > observed):
            observed = r["observed_at"]
    return {
        "object_id": str(object_id), "type": o["type"], "title": title,
        "properties": props,
        "provenance": {
            "source": ", ".join(sorted(sources)) or None,
            "source_label": ", ".join(_SOURCE_LABELS.get(s, s) for s in sorted(sources)) or None,
            "how": _HOW_LABELS.get(strongest or "", strongest or "unknown"),
            "evidence_class": strongest,
            "confidence": confidence,
            "observed": observed.isoformat() if observed else None,
            "demo": demo,
        },
    }


async def _label(p: asyncpg.Pool, object_id: uuid.UUID) -> dict[str, str]:
    """An object's display label (name → canonical) + type — for review/list rendering."""
    r = await p.fetchrow(
        "SELECT o.type, o.canonical, (SELECT value #>> '{}' FROM current_assertions a "
        "WHERE a.object_id=o.id AND a.name='name' LIMIT 1) AS name "
        "FROM objects o WHERE o.id=$1",
        object_id,
    )
    if r is None:
        return {"label": str(object_id), "type": "?"}
    return {"label": r["name"] or r["canonical"], "type": r["type"]}


def _coerce_json(v: Any) -> Any:
    """asyncpg returns jsonb as a str unless a codec is set; normalize for responses."""
    return _json.loads(v) if isinstance(v, str) else v


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


class SubscriptionBody(BaseModel):
    name: str
    criteria: dict[str, Any]
    webhook_url: str | None = None


class CompositionBody(BaseModel):
    name: str
    spec: dict[str, Any]
    kind: str = "lens"


class RunCompositionBody(BaseModel):
    subject: str | None = None


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
