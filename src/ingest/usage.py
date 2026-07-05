"""LLM usage telemetry — what the inference seam actually spent, per call.

Until this, the auto-ingest's cost was an ESTIMATE (per-call size x the 10-minute rate cap).
Each completion now records its real tokens — and, on the CLI backend, the real cost_usd from
the envelope — so `usage_summary` answers "what did sensing burn?" from data, not arithmetic.
Operational telemetry, a plain append-only table, never the event-sourced graph.
"""
from __future__ import annotations

from typing import Any

import asyncpg

from src.ingest.providers import Usage


async def record_usage(pool: asyncpg.Pool, *, purpose: str, usage: Usage) -> None:
    """Append one completion's usage. `purpose` names the call-site ('session-extract')."""
    await pool.execute(
        "INSERT INTO llm_usage (purpose, model, input_tokens, output_tokens, "
        "cache_read_tokens, cache_creation_tokens, cost_usd, duration_ms) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
        purpose, usage.model, usage.input_tokens, usage.output_tokens,
        usage.cache_read_tokens, usage.cache_creation_tokens, usage.cost_usd,
        usage.duration_ms,
    )


async def usage_summary(pool: asyncpg.Pool, *, hours: int = 24) -> dict[str, Any]:
    """Totals over the last `hours`, grouped by purpose+model, plus a grand total. Reads
    telemetry, not the graph — the answer to 'what did the auto-ingest burn?', from record."""
    rows = await pool.fetch(
        "SELECT purpose, model, count(*) AS calls, "
        " coalesce(sum(input_tokens),0) AS input_tokens, "
        " coalesce(sum(output_tokens),0) AS output_tokens, "
        " coalesce(sum(cache_read_tokens),0) AS cache_read_tokens, "
        " coalesce(sum(cache_creation_tokens),0) AS cache_creation_tokens, "
        " sum(cost_usd) AS cost_usd "
        "FROM llm_usage WHERE ran_at > now() - make_interval(hours => $1) "
        "GROUP BY purpose, model ORDER BY calls DESC",
        hours,
    )
    groups = [dict(r) for r in rows]
    in_tok = sum(int(g["input_tokens"]) for g in groups)
    out_tok = sum(int(g["output_tokens"]) for g in groups)
    cost = sum(float(g["cost_usd"]) for g in groups if g["cost_usd"] is not None)
    return {
        "hours": hours,
        "calls": sum(int(g["calls"]) for g in groups),
        "input_tokens": in_tok,
        "output_tokens": out_tok,
        "total_tokens": in_tok + out_tok,
        "cost_usd": round(cost, 4) if cost else None,
        "by_group": groups,
    }
