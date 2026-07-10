"""Co-investment analysis — which companies share the people who fund this one.

The SPV graph (company <- raises_for <- SPV -> officer -> operator) makes a latent
network explicit: two private companies are entangled when the SAME operator runs
feeder funds for both. This ranks a company's co-investment ties by how many operators
they share — the tightest ties are the companies wired into the same capital plumbing.
"""

from __future__ import annotations

import uuid
from typing import Any

import asyncpg

# Fund-administration platforms sign as officer/director on SPVs for unrelated deals
# (Sydecar admins SPVs for Neuralink AND for dentists), so they are NOT a co-investment
# signal — they're plumbing. We drop them two ways: by name, and (the robust one) by
# DEGREE — an operator wired into more than `platform_degree` distinct companies is a
# platform/serial-admin, not a thesis investor. The degree cut catches platform
# *signatories* (a person like a Sydecar ops lead) that no name list would.
_PLATFORM_RE = (
    r"sydecar|carta|angellist|assure|belltower|sextant|glassboard|venture360|"
    r"fund admin|spv mgr|spv manager|roll ?up vehicle|fund services|administ"
)


async def _identity_set(pool: asyncpg.Pool, object_id: uuid.UUID) -> list[uuid.UUID]:
    """Every object id that IS this entity: follow merged_into up to the winner, then
    gather all ids merged into it. Links on a pre-merge node still point at the old id,
    so analytics must treat the whole set as one (resolve-on-read)."""
    winner = object_id
    for _ in range(50):
        nxt = await pool.fetchval("SELECT merged_into FROM objects WHERE id=$1", winner)
        if nxt is None:
            break
        winner = nxt
    rows = await pool.fetch(
        "WITH RECURSIVE m(id) AS ("
        "  SELECT $1::uuid UNION SELECT o.id FROM objects o JOIN m ON o.merged_into = m.id) "
        "SELECT id FROM m",
        winner,
    )
    return [r["id"] for r in rows]


async def coinvestment_ties(
    pool: asyncpg.Pool, company_id: uuid.UUID, *, limit: int = 25, platform_degree: int = 12
) -> list[dict[str, Any]]:
    """For a target company, the other companies funded by SPVs that share an operator
    with it — ranked by the number of shared operators (the strength of the tie).

    Fund-admin platforms are filtered out (by name + by degree), so a shared operator
    means a shared *thesis* sponsor, not just a shared SPV-formation service."""
    cluster = await _identity_set(pool, company_id)
    rows = await pool.fetch(
        """
        WITH seed_ops AS (  -- operators behind the SPVs that raise for this company
            SELECT DISTINCT ol.to_id AS op
            FROM links rf
            JOIN links ol ON ol.from_id = rf.from_id AND ol.type IN ('officer', 'director')
            WHERE rf.to_id = ANY($1::uuid[]) AND rf.type = 'raises_for'
        ),
        op_degree AS (  -- how many DISTINCT companies each operator is wired into
            SELECT s.op,
                   (SELECT value #>> '{}' FROM current_assertions a
                    WHERE a.object_id = s.op AND a.name = 'name'
                    ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS opname,
                   count(DISTINCT rf2.to_id) AS deg
            FROM seed_ops s
            JOIN links ol2 ON ol2.to_id = s.op AND ol2.type IN ('officer', 'director')
            JOIN links rf2 ON rf2.from_id = ol2.from_id AND rf2.type = 'raises_for'
            GROUP BY s.op
        ),
        ops AS (  -- thesis operators only: bounded degree, not a named admin platform
            SELECT op FROM op_degree
            WHERE deg <= $3 AND coalesce(lower(opname), '') !~ $4
        )
        SELECT tgt.id,
               (SELECT value #>> '{}' FROM current_assertions a
                WHERE a.object_id = tgt.id AND a.name = 'name'
                ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS company,
               count(DISTINCT ops.op) AS shared,
               array_agg(DISTINCT (
                   SELECT value #>> '{}' FROM current_assertions a
                   WHERE a.object_id = ops.op AND a.name = 'name'
                   ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1
               )) AS operators
        FROM ops
        JOIN links ol3 ON ol3.to_id = ops.op AND ol3.type IN ('officer', 'director')
        JOIN links rf3 ON rf3.from_id = ol3.from_id AND rf3.type = 'raises_for'
        JOIN objects tgt ON tgt.id = rf3.to_id AND tgt.id <> ALL($1::uuid[])
            AND tgt.status = 'active'
        -- a co-investment target RECEIVES funding; exclude feeder SPVs, which are
        -- themselves raises_for sources (so an 'Anthropic 4 …' SPV doesn't masquerade
        -- as a portfolio company alongside the real Anthropic).
        WHERE NOT EXISTS (
            SELECT 1 FROM links x WHERE x.from_id = tgt.id AND x.type = 'raises_for'
        )
        GROUP BY tgt.id
        ORDER BY shared DESC, company
        LIMIT $2
        """,
        cluster,
        limit,
        platform_degree,
        _PLATFORM_RE,
    )
    return [
        {
            "id": str(r["id"]),
            "company": r["company"],
            "shared_operators": r["shared"],
            "operators": [o for o in r["operators"] if o],
        }
        for r in rows
        if r["company"]
    ]
