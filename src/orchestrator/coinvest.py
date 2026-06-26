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


async def coinvestment_ties(
    pool: asyncpg.Pool, company_id: uuid.UUID, *, limit: int = 25
) -> list[dict[str, Any]]:
    """For a target company, the other companies funded by SPVs that share an operator
    with it — ranked by the number of shared operators (the strength of the tie)."""
    rows = await pool.fetch(
        """
        WITH ops AS (  -- operators behind the SPVs that raise for this company
            SELECT DISTINCT ol.to_id AS op
            FROM links rf
            JOIN links ol ON ol.from_id = rf.from_id AND ol.type IN ('officer', 'director')
            WHERE rf.to_id = $1 AND rf.type = 'raises_for'
        )
        SELECT tgt.id,
               (SELECT value #>> '{}' FROM current_assertions a
                WHERE a.object_id = tgt.id AND a.name = 'name' LIMIT 1) AS company,
               count(DISTINCT ops.op) AS shared,
               array_agg(DISTINCT (
                   SELECT value #>> '{}' FROM current_assertions a
                   WHERE a.object_id = ops.op AND a.name = 'name' LIMIT 1
               )) AS operators
        FROM ops
        JOIN links ol2 ON ol2.to_id = ops.op AND ol2.type IN ('officer', 'director')
        JOIN links rf2 ON rf2.from_id = ol2.from_id AND rf2.type = 'raises_for'
        JOIN objects tgt ON tgt.id = rf2.to_id AND tgt.id <> $1 AND tgt.status = 'active'
        GROUP BY tgt.id
        ORDER BY shared DESC, company
        LIMIT $2
        """,
        company_id,
        limit,
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
