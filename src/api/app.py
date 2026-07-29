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
import os
import re
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
from src.api import chrome
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
    object_items,
    run_composition,
    run_spec,
    save_composition,
    save_watch,
)
from src.orchestrator.console import get_console, set_console
from src.orchestrator.dossier import entity_dossier
from src.orchestrator.federation import federated_query, promote, to_preview
from src.orchestrator.handoff import abandon, open_handoff, post_back
from src.orchestrator.handoff import tray as handoff_tray
from src.orchestrator.manifests import load_manifests, project_triggers
from src.orchestrator.monitor import (
    evaluate_watches,
    heartbeat_age_secs,
    match_condition,
    tick,
)
from src.orchestrator.runner import load_input_object
from src.orchestrator.watermark import graph_watermark

_HELPERS_DIR = Path(__file__).resolve().parent.parent.parent / "helpers"
_UI_DIR = Path(__file__).resolve().parent.parent / "ui" / "static"
_INBOX_STATIC_DIR = Path(__file__).resolve().parent / "inbox" / "static"


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

    @app.get("/health/worker")
    async def worker_health(p: asyncpg.Pool = Depends(get_pool)) -> dict[str, Any]:
        """The dead-man's-switch: is the worker (the tripwire) alive? Reports the age of its
        last heartbeat vs. the staleness threshold. 'never' = it has not beaten yet."""
        age = await heartbeat_age_secs(p)
        threshold = get_settings().osiris_worker_heartbeat_stale_secs
        if age is None:
            return {"status": "never", "age_secs": None, "threshold_secs": threshold}
        return {"status": "ok" if age <= threshold else "stale",
                "age_secs": round(age, 1), "threshold_secs": threshold}

    @app.get("/schema")
    async def get_schema() -> dict[str, Any]:
        """The semantic layer — the declared Object-Type + Link-Type catalog. Every
        surface reads its types/colours/shapes from here (one source of truth)."""
        return ontology_catalog()

    @app.post("/cases")
    async def create_case(body: NewCaseBody, p: asyncpg.Pool = Depends(get_pool)) -> dict[str, Any]:
        cid = await p.fetchval(
            "INSERT INTO cases (name, owner, budgets, room_id) VALUES ($1,$2,$3,$4) RETURNING id",
            body.name,
            get_settings().osiris_actor,
            body.budgets or {},
            body.room_id,
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
    async def list_cases(
        room: uuid.UUID | None = None, p: asyncpg.Pool = Depends(get_pool)
    ) -> list[dict[str, Any]]:
        """Analyses, optionally scoped to a Room (the stance). `room` omitted = all."""
        rows = await p.fetch(
            "SELECT c.id, c.name, c.owner, count(DISTINCT co.object_id) AS object_count "
            "FROM cases c LEFT JOIN case_objects co ON co.case_id = c.id "
            "WHERE c.archived_at IS NULL AND ($1::uuid IS NULL OR c.room_id = $1) "
            "GROUP BY c.id ORDER BY c.created_at DESC",
            room,
        )
        return [dict(r) for r in rows]

    # --- rooms: the stance switcher (segmentation over the shared graph) ------
    @app.get("/rooms")
    async def list_rooms(p: asyncpg.Pool = Depends(get_pool)) -> list[dict[str, Any]]:
        """The Rooms (stances). Each carries how many cases + compositions it scopes — the
        graph itself is never room-scoped, so resolution and search stay global."""
        rows = await p.fetch(
            "SELECT r.id, r.name, r.config, "
            "  (SELECT count(*) FROM cases c "
            "     WHERE c.room_id=r.id AND c.archived_at IS NULL) AS cases, "
            "  (SELECT count(*) FROM compositions x WHERE x.room_id=r.id) AS compositions "
            "FROM rooms r ORDER BY r.created_at"
        )
        return [
            {"id": str(r["id"]), "name": r["name"], "config": _coerce_json(r["config"]) or {},
             "cases": r["cases"], "compositions": r["compositions"]}
            for r in rows
        ]

    @app.post("/rooms")
    async def create_room(
        body: RoomBody, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, str]:
        rid = await p.fetchval(
            "INSERT INTO rooms (name, config) VALUES ($1,$2) "
            "ON CONFLICT (name) DO UPDATE SET config=EXCLUDED.config RETURNING id",
            body.name, body.config,
        )
        return {"id": str(rid), "name": body.name}

    @app.get("/search")
    async def knowledge_search(
        q: str, limit: int = Query(12, le=50),
        p: asyncpg.Pool = Depends(get_pool),
    ) -> dict[str, Any]:
        """ONE ENGINE (thread 0deaec4f rung 1, closed): the console search bar rides the
        SAME fn_search as the MCP tool — grade × recency ranking, testimony per hit, and
        the search lands in search_log so console searches feed the retrieval telemetry
        instead of being invisible to it. /objects?q= below remains the object BROWSER's
        typed filter (a different job: enumerate by type, not rank knowledge)."""
        out = await run_spec(
            p, {"op": "function", "name": "search",
                "args": {"q": q, "limit": limit, "caller": "console"}},
            None, name="search")
        items: dict[str, Any] = out["items"]
        return items

    @app.get("/objects")
    async def list_objects(
        p: asyncpg.Pool = Depends(get_pool),
        case_id: uuid.UUID | None = None,
        type: str | None = None,
        q: str | None = None,
        exclude_types: str | None = None,
        limit: int = Query(100, le=2000),
    ) -> list[dict[str, Any]]:
        # Word-order-proof recall: tokenize `q` on whitespace; an object matches when EVERY
        # token appears SOMEWHERE in its searched content (canonical or a name/summary/title/
        # rationale assertion) — the tokens may land in different properties. So "idempotent
        # claim" and "atomic ingest" hit even though adjacency/order differ from the stored
        # text. Single-token behaviour is unchanged. SQL-side and bounded (≤6 tokens): the
        # NOT-EXISTS says "no token failed to match anywhere" = all tokens matched.
        tokens = (q.split()[:6] if q else None) or None
        # exclude_types (comma list): the shell's default set drops the 900 dead Agent
        # hulls (10 live of 920 measured 2026-07-11) — agents belong to the fleet lens;
        # a toggle brings them back deliberately
        excl = [t.strip() for t in exclude_types.split(",") if t.strip()] \
            if exclude_types else None
        rows = await p.fetch(
            "SELECT id, type, canonical, status, " + _OBJ_LABEL + " AS name "
            "FROM objects o "
            "WHERE status NOT IN ('archived','merged') "
            "  AND ($1::uuid IS NULL OR EXISTS (SELECT 1 FROM case_objects co "
            "        WHERE co.object_id = o.id AND co.case_id = $1)) "
            "  AND ($2::text IS NULL OR type = $2) "
            "  AND ($5::text[] IS NULL OR NOT (type = ANY($5::text[]))) "
            "  AND ($3::text[] IS NULL OR NOT EXISTS ("
            "        SELECT 1 FROM unnest($3::text[]) AS tok "
            "        WHERE o.canonical NOT ILIKE '%' || tok || '%' "
            "          AND NOT EXISTS (SELECT 1 FROM current_assertions a "
            "             WHERE a.object_id=o.id "
            "             AND a.name IN ('name','summary','title','rationale') "
            "             AND a.value #>> '{}' ILIKE '%' || tok || '%'))) "
            # recency across ALL types — type-first ordering let two alphabetically early types
            # eat the whole cap (Decision+Commit = 1500; Threads never appeared)
            "ORDER BY created_at DESC LIMIT $4",
            case_id,
            type,
            tokens,
            limit,
            excl,
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

    @app.get("/objects/{object_id}/content")
    async def object_content(
        object_id: uuid.UUID, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, Any]:
        """The renderable CONTENT of a node — the document-viewer primitive. A Commit returns
        its `git show` DIFF (the git backbone made readable); a Reference / any object with a
        `body` returns its markdown; a PDF source returns a url. Generic, no per-type UI code."""
        row = await p.fetchrow(
            "SELECT o.type, o.canonical, " + _OBJ_LABEL + " AS name FROM objects o WHERE o.id=$1",
            object_id,
        )
        if row is None:
            raise HTTPException(404, "object not found")
        title = row["name"] or row["canonical"]
        if row["type"] == "Commit" and row["canonical"].startswith("commit:"):
            diff = await _git_show(row["canonical"].split(":", 1)[1])
            if diff:
                return {"kind": "diff", "title": title, "content": diff}
        if row["type"] == "SoftwareProject":
            readme = await _git_file("HEAD:README.md")
            if readme:
                return {"kind": "markdown", "title": title, "content": readme}
        body = await p.fetchval(
            "SELECT value #>> '{}' FROM current_assertions "
            "WHERE object_id=$1 AND name IN ('body','rationale') "
            "ORDER BY (name='body') DESC LIMIT 1",
            object_id,
        )
        if body:
            return {"kind": "markdown", "title": title, "content": body}
        return {"kind": "none", "title": title, "content": ""}

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

    @app.post("/objects/{object_id}/claim-identity")
    async def claim_identity(
        object_id: uuid.UUID, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, str]:
        """'This is me' for the developer persona. The resolver unifies dev identities that
        share a name/handle (asuramaya ↔ asuramaya), but CAN'T infer that a real name and a
        handle are the same person (hector ↔ asuramaya share no key). That's a human call.
        The first claim TAGS a Person as the canonical operator identity ('self'); a later
        claim MERGES that identity into the self — so you unify across repos by asserting it.
        Merge is event-sourced + reversible; the loser's commits re-attribute to the winner."""
        actor = get_settings().osiris_actor
        row = await p.fetchrow(
            "SELECT type FROM objects WHERE id=$1 AND status='active'", object_id)
        if row is None:
            raise HTTPException(status_code=404, detail="object not found")
        if row["type"] != "Person":
            raise HTTPException(status_code=400, detail="only a Person can be claimed as identity")
        self_id = await p.fetchval(
            "SELECT o.id FROM objects o JOIN current_assertions a ON a.object_id=o.id "
            "WHERE o.type='Person' AND o.status='active' "
            "AND a.name='tag' AND a.value->>'tag'='self' LIMIT 1"
        )
        if self_id is None:
            await Actions(p).tag_object(object_id, "self", "operator", actor)
            return {"action": "designated", "self": str(object_id)}
        if self_id == object_id:
            return {"action": "already-self", "self": str(object_id)}
        await Actions(p).merge_objects(
            self_id, object_id, "operator: this is me (cross-repo identity)", actor
        )
        return {"action": "merged", "self": str(self_id), "merged": str(object_id)}

    @app.get("/objects/{object_id}/dossier")
    async def entity_dossier_ep(
        object_id: uuid.UUID, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, Any]:
        """The 'who is this?' answer for a FEDERATED entity (sanctioned party / PEP /
        company): its identity properties plus its ownership/family/director network,
        each endpoint named. Complements the `who-is-this` composition (the footprint/
        tier lens)."""
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
            "SELECT o.id, o.type, o.canonical, " + _OBJ_LABEL + " AS name "
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

    @app.get("/objects/{object_id}/related")
    async def object_related(
        object_id: uuid.UUID,
        p: asyncpg.Pool = Depends(get_pool),
        type: str | None = None,
        direction: str | None = None,
    ) -> dict[str, Any]:
        """The neighbours reached by a link type/direction, as a RESULT SET (enriched
        objects, table-ready). This is the typed pivot behind 'open as set' (W3): a
        relationship group → an object set → a view. `direction` = out | in | both."""
        rows = await p.fetch(
            "SELECT DISTINCT CASE WHEN from_id=$1 THEN to_id ELSE from_id END AS n "
            "FROM links WHERE (from_id=$1 OR to_id=$1) "
            "  AND ($2::text IS NULL OR type=$2) "
            "  AND ($3::text IS NULL OR (CASE WHEN from_id=$1 THEN 'out' ELSE 'in' END)=$3)",
            object_id, type, direction,
        )
        ids = [r["n"] for r in rows]
        items = await object_items(p, ids)
        return {"kind": "objects", "count": len(items), "items": items}

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

    # --- watermark: auto-refresh's whole mechanism (ruling cf9286b2) — poll THIS, never
    # the composition. See src/orchestrator/watermark.py's own docstring for the four
    # markers, why they're separate rather than combined, and the measured cost.
    @app.get("/watermark")
    async def watermark_route(p: asyncpg.Pool = Depends(get_pool)) -> dict[str, Any]:
        return await graph_watermark(p)

    # --- compositions: the composer's primitive (lenses + watches as one) ----
    @app.get("/compositions")
    async def list_compositions_route(
        room: uuid.UUID | None = None, p: asyncpg.Pool = Depends(get_pool)
    ) -> list[dict[str, Any]]:
        """Saved compositions (lens + watch), optionally scoped to a Room. `room` omitted = all."""
        return await list_compositions(p, room)

    @app.post("/compositions")
    async def save_composition_route(
        body: CompositionBody, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, Any]:
        """Save (or fork) a composition — a named op-tree the substrate runs. Authoring is
        usually Claude-over-MCP; this is the human save/fork channel."""
        cid = await save_composition(p, body.name, body.spec, body.kind, room_id=body.room_id)
        return {"id": str(cid), "name": body.name}

    @app.post("/compositions/{name}/run")
    async def run_composition_route(
        name: str, body: RunCompositionBody, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, Any]:
        """Run a saved composition (the LENS execution), optionally against a subject.
        Returns its Result — objects / values / rows / data — for the generic renderer."""
        subject = uuid.UUID(body.subject) if body.subject else None
        try:
            # the console is the OPERATOR'S surface (6c18709f): its lenses see every house
            return await run_composition(p, name, subject, caller="console")
        except ValueError as exc:  # e.g. a Function that needs a subject, or a bad op
            return {"error": str(exc)}

    @app.post("/compositions/run-spec")
    async def run_spec_route(
        body: RunSpecBody, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, Any]:
        """Run an EPHEMERAL op-tree (the inline composer's working spec — W4). The chips
        edit a working spec and re-run it here without saving; 'Save as' persists it."""
        subject = uuid.UUID(body.subject) if body.subject else None
        try:
            return await run_spec(p, body.spec, subject, name=body.name or "(working)",
                                  caller="console")
        except ValueError as exc:
            return {"error": str(exc)}

    # THE TRIAGE VERBS (ruling 923c380f — the operator's own word amends the read-only
    # console): deliberate clicks write through the Actions waist, signed analyst:operator —
    # the highest-grade testimony in the system. resolve closes; obligation/question/task
    # reclassify WITHOUT touching status (untouched ≠ resolved, 758ded94). Bulk-capable:
    # the echo pile drains by stories, not one call per click.
    @app.post("/threads/triage")
    async def triage_threads(
        body: ThreadTriageBody, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, Any]:
        # THE FOUR DOORS OFF THE DESK (operator, 2026-07-11 — "it snowballs into infinity"):
        # a debt used to have two exits, do-it or rot, so everything he was ever cc'd on piled
        # up on him. resolve = done · assign = NOT MINE (hand it back to the project that owes
        # it; orient puts it on THEIR wall) · defer = mine, not now (hidden at the lens until
        # its date) · obligation/question/task = it isn't what it claims to be. Only `resolve`
        # touches status — the other three never lie about state (758ded94).
        from src.orchestrator.capture import (
            assign_thread,
            defer_thread,
            reclassify_thread,
            resolve_thread,
        )
        verbs = ("resolve", "obligation", "question", "task", "assign", "defer")
        if body.verb not in verbs:
            return {"error": f"verb must be one of {' | '.join(verbs)}"}
        if body.verb == "assign" and not (body.owner or "").strip():
            return {"error": "assign needs an owner — a project name, 'agent:<id>', or 'operator'"}
        acts = Actions(p)
        out: list[dict[str, str]] = []
        for ref in body.ids[:200]:
            if body.verb == "resolve":
                tid = await resolve_thread(acts, ref, because=body.because,
                                           source="analyst:operator")
            elif body.verb == "assign":
                tid = await assign_thread(acts, ref, owner=(body.owner or "").strip(),
                                          because=body.because, source="analyst:operator")
            elif body.verb == "defer":
                tid = await defer_thread(acts, ref, days=body.days, because=body.because,
                                         source="analyst:operator")
            else:
                tid = await reclassify_thread(acts, ref, kind=body.verb,
                                              because=body.because,
                                              source="analyst:operator")
            out.append({"ref": ref, **({"id": str(tid), "ok": "1"} if tid
                                       else {"error": "no match"})})
        done = sum(1 for o in out if o.get("ok"))
        return {"verb": body.verb, "acted": done, "missed": len(out) - done,
                "results": out, "by": "analyst:operator"}

    @app.post("/desk/settle")
    async def desk_settle(
        body: DeskSettleBody, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, Any]:
        """DISMISS briefs from the operator's desk. The membrane holds in the only way that
        matters: an AGENT still cannot settle this desk (dim_brief annotates, never clears) —
        this route exists solely so the HUMAN'S OWN CLICK can, without him having to summon a
        mind to run inbox(ack=[…]) for him. His hand, his signature."""
        from src.orchestrator.mailbox import OPERATOR_ADDR, ack_messages
        ids = body.ids[:200]
        out = await ack_messages(p, OPERATOR_ADDR, ids, reader_agent=OPERATOR_ADDR)
        return {"settled": len(out["settled"]), "asked": len(ids), "by": "operator",
                **({"skipped": out["skipped"]} if out["skipped"] else {}),
                "note": "dismissed by the human's own click"}

    @app.post("/act")
    async def act(body: ActBody, p: asyncpg.Pool = Depends(get_pool)) -> dict[str, Any]:
        """THE GENERIC ACTION-BINDING INVOCATION (ruling c5b184cd, thread d56e7073/#44) — any
        composition-rendered row carrying `_action` fires here through ONE shared route,
        instead of one bespoke endpoint per verb family (/threads/triage, /desk/settle).
        Same write-safety those two already established, just no longer per-route: `action`
        is looked up in a CLOSED registry (actions.ACTION_VERBS) — an unknown name refuses,
        never falls through to anything dynamic; each registry entry reads only its own
        named `args` keys and hardcodes `source="analyst:operator"` itself — this route
        never trusts the request body for WHO is acting, only WHAT."""
        from src.api.actions import ACTION_VERBS
        verb = ACTION_VERBS.get(body.action)
        if verb is None:
            return {"error": f"unknown action {body.action!r} — "
                             f"must be one of {sorted(ACTION_VERBS)}"}
        return await verb(p, body.args)

    # ---- the shared console cursor (real-time Claude↔front sync) -------------
    @app.get("/console")
    async def console_get(p: asyncpg.Pool = Depends(get_pool)) -> dict[str, Any]:
        """The current shared cursor — room / composition / view / focused object."""
        return await get_console(p)

    @app.post("/console")
    async def console_set(
        body: ConsoleBody, p: asyncpg.Pool = Depends(get_pool)
    ) -> dict[str, Any]:
        """Move the shared cursor. Both the browser (by='human') and Claude (by='claude')
        write here; `rev` + `updated_by` let each side ignore its own echo."""
        fields = body.model_dump(exclude={"by"}, exclude_unset=True)
        return await set_console(p, by=body.by, **fields)

    @app.get("/console/stream")
    async def console_stream(request: Request) -> StreamingResponse:
        """SSE: push the shared cursor whenever its `rev` changes (same poll-and-diff loop
        as the case stream). The browser applies remote moves; its own it suppresses by rev."""
        async def gen() -> AsyncIterator[str]:
            last = ""
            while not await request.is_disconnected():
                state = await get_console(request.app.state.pool)
                payload = _json.dumps(state)
                if payload != last:
                    yield f"data: {payload}\n\n"
                    last = payload
                else:
                    yield ": keep-alive\n\n"
                await asyncio.sleep(1.0)

        return StreamingResponse(gen(), media_type="text/event-stream")

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
            room_id=body.room_id,
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

    # THE MEMBRANE ROUTE IS RETIRED (task #71, ruling 0b3dd431, msg 1811/1818): THE INBOX
    # (src/api/inbox/) is :8011's new front door, mounted below. render_membrane's own
    # module (src/api/membrane.py) stays in the tree, unrouted, for one deploy cycle per
    # Thoth's explicit instruction — a rollback needs it reachable without a revert.

    # THE CHROME OPENED (operator, 2026-07-11): /desk /mail /fleet — clickable, openable,
    # ~4s-fresh lenses so the human looks WITHOUT calling an agent. Same read-only
    # constitution as /membrane; ?partial=1 serves just the content div for the poller.
    @app.get("/desk")
    async def desk_page(
        partial: int = 0, p_: str | None = Query(None, alias="p"),
        p: asyncpg.Pool = Depends(get_pool),
    ) -> Response:
        """THE DESK, PER PROJECT (operator, 2026-07-11: "the desk is better off as a
        per-project thing, like the mail. the overwhelming kill here is that i get flooded
        with my entire fleet worth of backlog on one tab").

        No arg → the ROSTER: one line per project (owed · asked · age). `?p=<project>` walks
        into one — its debts with the four doors, and the briefs that asked. Reading leases
        nothing; the WRITES are the human's own clicks (ruling 923c380f), signed
        analyst:operator through the Actions waist."""
        from src.orchestrator.mailbox import read_desk
        desk = await read_desk(p)
        inner = (chrome.render_desk_project(desk, p_) if p_
                 else chrome.render_desk(desk))
        title = f"desk · {p_}" if p_ else "desk"
        return Response(inner if partial
                        else chrome.page(title, "desk", inner, actions=True),
                        media_type="text/html")

    @app.get("/mail")
    async def mail_page(
        box: str | None = None, partial: int = 0, p: asyncpg.Pool = Depends(get_pool)
    ) -> Response:
        """The fleet's mail, walkable: all mailboxes → one box's conversations, threads
        opening in place. Raw reads — a glance here never leases anyone's mail."""
        if box:
            inner = chrome.render_mail_box(box, await chrome.mail_threads(p, box))
        else:
            inner = chrome.render_mail_overview(await chrome.mail_overview(p))
        return Response(inner if partial else chrome.page("mail", "mail", inner),
                        media_type="text/html")

    # /live-desk RETIRED (ruling d42c543b): its own docstring already said "this page has
    # no bespoke filtering of its own, only the composition + the generic renderer" — a pure
    # duplicate of the "live-desk" composition already roomed in /ui, through the SAME
    # chrome.render_composition. Verified live before deletion: opened "live-desk" in /ui,
    # confirmed the owed/decisions/drift-alarm bands and real `_action` buttons render there
    # identically to what this route produced.

    @app.get("/fleet")
    async def fleet_page(partial: int = 0, p: asyncpg.Pool = Depends(get_pool)) -> Response:
        """Who is mounted RIGHT NOW (live dots, seats, models, worktrees) + the wake ledger
        and the hourly spend against its budget."""
        data = await chrome.fleet_data(
            p, wake_budget=get_settings().osiris_wake_hourly_budget)
        inner = chrome.render_fleet(data)
        return Response(inner if partial else chrome.page("fleet", "fleet", inner),
                        media_type="text/html")

    # /roadmap RETIRED (ruling d42c543b): a thin wrapper over the "roadmap" composition
    # through chrome.render_composition, no bespoke logic of its own beyond a `?p=` project
    # default/lookup — verified live before deletion: focused the "osiris" SoftwareProject
    # in /ui, ran "roadmap", confirmed the same open/resolved-by-arc/owner bands this route
    # produced. Losing the auto-default-to-osiris convenience (a manual focus click in /ui
    # replaces it) is a UX nuance, not a filter/scope/band/count the composition itself lacks.

    @app.get("/canon")
    async def canon_page(
        p_: str = Query("osiris", alias="p"), partial: int = 0,
        p: asyncpg.Pool = Depends(get_pool),
    ) -> Response:
        """The doc canon, topic-sectioned (thread 521ae613a6f4, migrated to a composition —
        ruling c5b184cd, thread d56e7073/#44) — the "docs" nav tab, routed at /canon rather
        than /docs: FastAPI reserves /docs for its own Swagger UI, and a second route at the
        same path is silently shadowed by it (caught live by the route test). `?p=<project>`
        names which project's chrome this is (defaults to osiris's own) — the "docs"
        composition itself is not yet project-scoped, so every project currently renders the
        same canon. The fixed section order used to be a route-level re-sort; it now lives
        in DOCS's own `sequence` (ruling d42c543b, Thoth msg 1937) — this route just renders
        whatever the composition returns, no post-step of its own."""
        title = f"docs · {p_}"
        res = await run_composition(p, "docs")
        inner = chrome.render_composition(res)
        return Response(inner if partial else chrome.page(title, "docs", inner),
                        media_type="text/html")

    @app.get("/overhead")
    async def overhead_page(
        partial: int = 0, p: asyncpg.Pool = Depends(get_pool),
    ) -> Response:
        """THE OVERHEAD LENS (neo's eye, task #34): what the harness itself costs —
        hidden channels, cache vs fresh, reminder injections, compaction churn — read
        from the transcript store. Below it, the retained-telemetry forensics (task #35)."""
        from src.ingest.telemetry import TelemetryStore
        from src.ingest.transcript_store import TranscriptStore
        data = await TranscriptStore(p).overhead_fleet(top=20)
        telemetry = await TelemetryStore(p).summary()
        inner = chrome.render_overhead(data, telemetry)
        return Response(inner if partial else chrome.page("overhead", "overhead", inner),
                        media_type="text/html")

    if _UI_DIR.is_dir():
        app.mount("/ui", StaticFiles(directory=str(_UI_DIR), html=True), name="ui")

    # THE INBOX (task #71, ruling 0b3dd431): :8011's new front door, replacing /membrane
    # (retired above). Frozen static assets (vendored datastar.js, app.css) mounted
    # separately from /ui (that mount is the OLD Cytoscape/MapLibre SPA, unrelated).
    from src.api.inbox.app import router as inbox_router

    app.include_router(inbox_router)
    if _INBOX_STATIC_DIR.is_dir():
        app.mount("/static", StaticFiles(directory=str(_INBOX_STATIC_DIR)), name="inbox-static")

    return app


# The best HUMAN label for an object — never a raw hash/id when a name/title/summary exists.
# A Commit has no `name` but has `summary`; without this the UI shows `commit:7f0…` for every
# node and row (the hairball / hash-wall). Generic, no per-type code.
_LABEL_PROPS = ("name", "title", "summary", "subject")
_OBJ_LABEL = "COALESCE(" + ", ".join(
    f"(SELECT value #>> '{{}}' FROM current_assertions a "
    f"WHERE a.object_id=o.id AND a.name='{_p}' "
    f"ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1)" for _p in _LABEL_PROPS
) + ", o.canonical)"


# The git backbone, made viewable: a Commit's "content" is its diff. Resolve the short sha
# from the canonical and `git show` it in the tracked repo (the same repo gitlog ingested).
_SHA_RE = re.compile(r"^[0-9a-f]{7,40}$")
_REPO_DIR = os.environ.get("OSIRIS_REPO_DIR", ".")


async def _git_show(sha: str) -> str | None:
    if not _SHA_RE.match(sha):
        return None
    try:
        proc = await asyncio.create_subprocess_exec(
            "git", "-C", _REPO_DIR, "show", "--stat", "-p", "--no-color", sha,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await proc.communicate()
    except OSError:
        return None
    return out.decode("utf-8", "replace")[:200_000] if proc.returncode == 0 else None


async def _git_file(ref: str) -> str | None:
    """`git show <ref>` for a file (e.g. HEAD:README.md) — the repo node's own doc."""
    if not re.match(r"^[\w./:-]+$", ref):
        return None
    try:
        proc = await asyncio.create_subprocess_exec(
            "git", "-C", _REPO_DIR, "show", ref,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await proc.communicate()
    except OSError:
        return None
    return out.decode("utf-8", "replace")[:200_000] if proc.returncode == 0 else None


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
        "SELECT o.type, o.canonical, " + _OBJ_LABEL + " AS name "
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
    room_id: uuid.UUID | None = None


class RoomBody(BaseModel):
    name: str
    config: dict[str, Any] = {}


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
    room_id: uuid.UUID | None = None


class CompositionBody(BaseModel):
    name: str
    spec: dict[str, Any]
    kind: str = "lens"
    room_id: uuid.UUID | None = None


class RunCompositionBody(BaseModel):
    subject: str | None = None


class RunSpecBody(BaseModel):
    spec: dict[str, Any]
    subject: str | None = None
    name: str | None = None


class ThreadTriageBody(BaseModel):
    """`assign` needs `owner` (the project/agent taking it); `defer` needs `days`."""
    ids: list[str]
    verb: str  # resolve | obligation | question | task | assign | defer
    because: str | None = None
    owner: str | None = None
    days: int = 30


class DeskSettleBody(BaseModel):
    """The operator DISMISSING briefs from his own desk — his click, his signature."""
    ids: list[int]


class ActBody(BaseModel):
    """The generic action-binding invocation (ruling c5b184cd, thread d56e7073/#44) — a
    composition-declared control's own click, POSTed exactly as `_action` named it. `action`
    is looked up in `actions.ACTION_VERBS` (a closed registry); `args` is whatever that
    specific row's own `row_action` template resolved to server-side (see
    `compositions._table`) — the client never constructs `args` itself, only echoes what the
    row it clicked already carried."""
    action: str
    args: dict[str, Any] = {}


class ConsoleBody(BaseModel):
    """A partial move of the shared cursor. `by` is who moved (claude / human); only the
    fields actually set are written (exclude_unset), so a focus-only move keeps the rest."""
    by: str = "human"
    room_id: uuid.UUID | None = None
    composition: str | None = None
    view: str | None = None
    focused_object_id: uuid.UUID | None = None
    working_spec: dict[str, Any] | None = None


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
