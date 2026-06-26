"""Osiris MCP server — the AI-facing surface over the engine.

Exposes Osiris's capabilities as typed MCP tools, so any MCP client (Claude Desktop /
Code, a scheduled agent, or none at all) can DRIVE an investigation through a stable
interface — the formalization of what was previously ad-hoc Python. The same engine
backs the human front-end (the FastAPI app); the AI is an external, optional, audited
client, never embedded in the kernel, and every tool still flows through the audited
Actions layer.

    uv run python -m src.mcp_server        # stdio transport
"""

from __future__ import annotations

import uuid
from typing import Any

import asyncpg
from mcp.server.fastmcp import FastMCP

from src.actions.core import Actions
from src.config.settings import get_settings
from src.db.pool import create_pool
from src.ingest.clinicaltrials import aim_trials, expand_facility
from src.ingest.courtlistener import aim_litigation
from src.ingest.edgar_formd import aim_form_d, expand_filings
from src.ingest.wikidata import aim as wikidata_aim
from src.ontology.resolution import (
    consolidate_companies,
    find_cross_base_candidates,
    resolve_cross_base,
)
from src.orchestrator.coinvest import coinvestment_ties
from src.orchestrator.discrepancy import footprint_discrepancy
from src.orchestrator.dossier import entity_dossier
from src.orchestrator.sources import as_dicts, suggest

mcp = FastMCP("osiris")
_pool: asyncpg.Pool | None = None


async def _pool_get() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await create_pool(get_settings().database_url)
    return _pool


async def _resolve(pool: asyncpg.Pool, ref: str) -> uuid.UUID | None:
    """Accept a UUID or a name; resolve to an active object id. Tries exact name, then
    substring — and among matches prefers the most-described (the merged canonical)."""
    try:
        return uuid.UUID(ref)
    except ValueError:
        pass
    # exact name (most-described wins), then substring (shortest name wins — closest to
    # the query, so 'Neuralink' picks 'Neuralink Corp.' over 'Neuralink Jun 2025 …').
    for predicate, order in (
        ("lower(a.value #>> '{}') = lower($1)",
         "(SELECT count(*) FROM current_assertions x WHERE x.object_id=a.object_id) DESC"),
        ("a.value #>> '{}' ILIKE '%'||$1||'%'",
         "length(a.value #>> '{}') ASC"),
    ):
        row = await pool.fetchval(
            "SELECT a.object_id FROM current_assertions a "
            "JOIN objects o ON o.id=a.object_id AND o.status='active' "
            f"WHERE a.name='name' AND {predicate} ORDER BY {order} LIMIT 1",
            ref,
        )
        if row is not None:
            return uuid.UUID(str(row))
    return None


# --- orientation ------------------------------------------------------------

@mcp.tool()
async def suggest_sources(object_ref: str) -> dict[str, Any]:
    """The playbook for an object (UUID or name): which sources to collect and which
    analyses apply, given its type. Start here — this is 'what can I do with this?'."""
    pool = await _pool_get()
    oid = await _resolve(pool, object_ref)
    if oid is None:
        return {"error": f"no object matches {object_ref!r}"}
    otype = await pool.fetchval("SELECT type FROM objects WHERE id=$1", oid)
    return {"object_id": str(oid), "type": otype, "capabilities": as_dicts(suggest(otype or ""))}


@mcp.tool()
async def search(query: str, limit: int = 15) -> list[dict[str, Any]]:
    """Find objects by name substring → their id, type, canonical."""
    pool = await _pool_get()
    rows = await pool.fetch(
        "SELECT DISTINCT o.id, o.type, o.canonical, a.value #>> '{}' AS name "
        "FROM current_assertions a JOIN objects o ON o.id=a.object_id AND o.status='active' "
        "WHERE a.name='name' AND a.value #>> '{}' ILIKE '%'||$1||'%' LIMIT $2",
        query, limit,
    )
    return [{"id": str(r["id"]), "type": r["type"], "name": r["name"]} for r in rows]


# --- collect (federate a base) ----------------------------------------------

@mcp.tool()
async def aim_entity(name: str) -> dict[str, Any]:
    """Resolve a name on Wikidata and ingest the entity + relationships + official
    social accounts; the broadest first pull for a company or person."""
    return await wikidata_aim(Actions(await _pool_get()), name)


@mcp.tool()
async def ingest_form_d(name: str) -> dict[str, Any]:
    """SEC Form D: a private company's financing rounds — officers, amounts, and the
    feeder SPVs that fund it (linked into the graph)."""
    return await aim_form_d(Actions(await _pool_get()), name)


@mcp.tool()
async def expand_operator(name: str) -> dict[str, Any]:
    """Pull a repeat player's thread: every Form D mentioning this operator → their
    whole portfolio, exposing the co-investment network."""
    return await expand_filings(Actions(await _pool_get()), name)


@mcp.tool()
async def ingest_trials(sponsor: str) -> dict[str, int]:
    """ClinicalTrials.gov: a sponsor's registered human trials — status, sites
    (facilities), named investigators."""
    return await aim_trials(Actions(await _pool_get()), sponsor)


@mcp.tool()
async def ingest_litigation(name: str, opinions: bool = False) -> dict[str, int]:
    """Court records (CourtListener): lawsuits & enforcement actions naming this
    entity — dockets, parties, judges. opinions=True searches case law instead of
    RECAP dockets. Answers 'has this entity been sued or charged?'."""
    return await aim_litigation(Actions(await _pool_get()), name, kind="o" if opinions else "r")


@mcp.tool()
async def expand_clinical_site(facility: str) -> dict[str, int]:
    """The trials at a clinical SITE — revealing which other sponsors use it."""
    return await expand_facility(Actions(await _pool_get()), facility)


@mcp.tool()
async def consolidate() -> dict[str, int]:
    """Graph hygiene: queue + resolve cross-base merges (same company across bases) and
    collapse SPV-name company variants. Run after collecting to de-fragment entities."""
    actions = Actions(await _pool_get())
    await find_cross_base_candidates(actions.pool)
    return {
        "cross_base_merges": await resolve_cross_base(actions),
        "company_variants_merged": await consolidate_companies(actions),
    }


# --- analyze (read-model lenses) --------------------------------------------

@mcp.tool()
async def dossier(object_ref: str) -> dict[str, Any]:
    """Who is this? Identity properties + the named relationship network."""
    pool = await _pool_get()
    oid = await _resolve(pool, object_ref)
    return await entity_dossier(pool, oid) if oid else {"error": f"no object {object_ref!r}"}


@mcp.tool()
async def discrepancy(object_ref: str) -> dict[str, Any]:
    """Footprint discrepancy: operational geography the disclosed home omits."""
    pool = await _pool_get()
    oid = await _resolve(pool, object_ref)
    return await footprint_discrepancy(pool, oid) if oid else {"error": f"no object {object_ref!r}"}


@mcp.tool()
async def coinvestment(object_ref: str) -> list[dict[str, Any]] | dict[str, str]:
    """Co-investment ties: companies funded by SPVs that share an operator with this one."""
    pool = await _pool_get()
    oid = await _resolve(pool, object_ref)
    return await coinvestment_ties(pool, oid) if oid else {"error": f"no object {object_ref!r}"}


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
