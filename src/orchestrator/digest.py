"""The membrane — the operator's window into the autonomous fleet (credence phase 2).

Authority flows DOWN the fleet; results, accountability, and danger flow back UP to the operator.
This is that return path made visible — a stateless rolling-window digest that surfaces, with no
new writes:

  * ROSTER + HEALTH — every agent, its model/project, and whether its identity RESOLVED cleanly
    (an onboarding that fell back to a best-effort id shows here).
  * ACTIVITY — what the fleet DECIDED / opened in your name since the window opened (agent-authored
    Decisions/Threads only — the miner's DERIVED backfill is excluded; this is deliberate work).
  * DANGER MAP — which agents were model-SWAPPED: the fable harness's silent demotion (ruling
    f2ae6346), i.e. where the harness got nervous.
  * LAUNDERING — credence_props run LIVE over objects several agents co-asserted: a relay that
    carried a fact ABOVE its origin grade (the citogenesis the credence floor exists to catch).

Read-side only; the window is a rolling `since` (no stored watermark — that is a v2). This is
where credence_props finally meets a live surface.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from src.actions.core import Actions
from src.orchestrator.credence import credence_props


async def _roster(actions: Actions) -> list[dict[str, Any]]:
    """Every Agent with its model/project and the two health signals — identity_resolved (did the
    onboarding get a clean id?) and model_swapped (was it silently demoted?)."""
    # each scalar subquery takes the grade-then-recency WINNER (ORDER BY … LIMIT 1) — a bare
    # subquery assumes ≤1 row per (agent, property) and 500s the whole digest the moment any Agent
    # property lands from two sources.
    rows = await actions.pool.fetch(
        "SELECT o.canonical AS agent, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='project' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1"
        " ) AS project, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='source_model' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1"
        " ) AS model, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='identity_resolved' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1"
        " ) AS resolved, "
        " (SELECT value#>>'{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='model_swapped' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1"
        " ) AS swapped "
        "FROM objects o WHERE o.type='Agent' ORDER BY project NULLS FIRST, o.canonical")
    return [
        {"agent": r["agent"], "project": r["project"], "model": r["model"],
         "resolved": r["resolved"] != "false",  # None (pre-hardening) or 'true' → treated resolved
         "swapped": r["swapped"]}
        for r in rows
    ]


async def _activity(actions: Actions, since: datetime, limit: int = 50) -> list[dict[str, Any]]:
    """What the fleet deliberately did in your name since the window opened — agent-authored
    Decisions/Threads (the miner's DERIVED backfill, sourced to session-miner, is excluded)."""
    rows = await actions.pool.fetch(
        "SELECT o.type AS type, a.source_id AS agent, a.value#>>'{}' AS summary, "
        "       a.observed_at AS at "
        "FROM objects o JOIN current_assertions a ON a.object_id=o.id AND a.name='summary' "
        "WHERE o.type IN ('Decision','Thread') AND a.source_id LIKE 'agent:%' "
        "  AND a.observed_at >= $1 "
        "ORDER BY a.observed_at DESC LIMIT $2", since, limit)
    return [
        {"type": r["type"], "agent": r["agent"],
         "summary": (r["summary"] or "")[:200], "at": r["at"].isoformat()}
        for r in rows
    ]


async def _laundering(actions: Actions, since: datetime) -> list[dict[str, Any]]:
    """credence_props run LIVE over objects >1 agent co-asserted in the window — surfacing any
    relay that carried a fact above its origin grade. Empty while co-assertion is nascent; this is
    the wire that makes credence bite as the fleet grows."""
    # candidates = objects an agent touched IN the window whose (object,name) ALSO carries another
    # agent source ALL-TIME. The archetypal laundering re-reports an OLDER origin (origin before the
    # window, relay inside it) — requiring both sources inside the window would miss exactly that.
    oids = [
        r["object_id"] for r in await actions.pool.fetch(
            "SELECT DISTINCT ca.object_id FROM current_assertions ca "
            "WHERE ca.source_id LIKE 'agent:%' AND ca.observed_at >= $1 "
            "  AND EXISTS (SELECT 1 FROM current_assertions c2 "
            "    WHERE c2.object_id = ca.object_id AND c2.name = ca.name "
            "      AND c2.source_id LIKE 'agent:%' AND c2.source_id <> ca.source_id)", since)
    ]
    if not oids:
        return []
    winners = await credence_props(actions, oids)
    return [
        {"object_id": str(w.object_id), "name": w.name, "value": w.value,
         "laundering_sources": list(w.laundering)}
        for w in winners if w.laundering
    ]


async def fleet_digest(actions: Actions, *, since: datetime) -> dict[str, Any]:
    """The membrane: the four upward streams over the window since `since`, with a summary head.
    Read-only — nothing here writes to the graph."""
    roster = await _roster(actions)
    activity = await _activity(actions, since)
    laundering = await _laundering(actions, since)
    danger = [r for r in roster if r["swapped"]]
    unresolved = [r for r in roster if not r["resolved"]]
    return {
        "since": since.isoformat(),
        "summary": {
            "agents": len(roster), "unresolved": len(unresolved),
            "swapped": len(danger), "activity": len(activity), "laundering": len(laundering),
        },
        "roster": roster,
        "activity": activity,
        "danger": danger,
        "laundering": laundering,
    }
