"""THE VITALS — one authority per fact for every number a surface shows the operator.

Operator ruling, 2026-07-19: "the chrome and the harness disagree on briefs, mail, owe" —
and they disagreed because each surface carried its own COPY of each count's SQL, written
in different weeks, diverging clause by clause as the laws moved underneath them: the
statusline's mail count predated the lineage rollup and the hold grace; the pulse's live
count predated the seated/visitor split; the chrome desk's owed number was capped by its
own display LIMIT. A copy is a fork that forgets it is one.

THE LAW: same word, same number, same SQL. The facts live HERE (and in mailbox.py for the
mail-domain facts, which already had its `_DELIVERABLE_TO_READER` authority); the chrome,
the statusline, the pulse, and orient CALL these — none of them owns a formula anymore.

Every function takes a pool-or-connection (the statusline renders on one short-lived
connection; the servers hold pools — both quack fetchrow/fetchval).
"""
from __future__ import annotations

from typing import Any, Protocol

from src.orchestrator.agents import SOUL_SQL_TEMPLATE


class _DB(Protocol):
    """The slice of asyncpg's pool/connection API the vitals need — both satisfy it."""

    async def fetch(self, query: str, *args: Any) -> Any: ...
    async def fetchrow(self, query: str, *args: Any) -> Any: ...
    async def fetchval(self, query: str, *args: Any) -> Any: ...


# A row is SEATED when its agent is deliberately bound — the visitor gate's own
# discriminator, read at the counting site: the agent base differs from the sid-derived
# base (a whisper echoes the sid back for strangers), or an active object stands behind
# the id. A live stranger (a bg-pty host, a spare) is real and is counted BESIDE the
# fleet number as a visitor, never inside it.
_SEATED_ROW = (
    "(substring(m2.agent_id from 7 for 8) IS DISTINCT FROM "
    "  substring(split_part(coalesce(m2.session_key,''), ':', 2) from 1 for 8) "
    " OR EXISTS (SELECT 1 FROM objects so WHERE so.canonical = m2.agent_id "
    "      AND so.status='active'))"
)

# A SOUL is the lineage, not the row: a seat with three doors — its anchor, a tab view,
# a resume bridge — is ONE mind (operator, 2026-07-17: 'fleet is showing 7 agents when
# really its 4 live'). The suffix strip folds generations to their base — SOUL_SQL_TEMPLATE
# (agents.py, thread 25b57dca) so this shares the roman-AND-g<N> alternation with every
# other site, instead of carrying its own roman-only copy that a g<N> id silently defeats.
_SOUL = SOUL_SQL_TEMPLATE.format(col="m2.agent_id")


async def live_souls(db: _DB, *, live_secs: int = 900) -> dict[str, int]:
    """{souls, visitors} — distinct live minds (seated) and distinct live strangers.

    CACHE-BASED, CONFESSED, DELIBERATELY NOT ROUTED THROUGH THE HARNESS AUTHORITY (door
    census item 2, Thoth msg 5772/5741, thread 2c3c2b9a): `agent_mounts.last_seen`
    freshness only, never cross-checked against registry_census/is_occupied_by_a_live_body.
    This feeds fleet_pulse's "N live" — read by EVERY agent on EVERY mount/orient call
    fleet-wide, which is exactly why it stays cheap on purpose rather than paying a real
    subprocess+/proc census on that hot a path. The obligation this leaves is DISCLOSURE,
    not verification: a caller (or a human) reading "N live" must know it is a 15-minute
    mount-row guess, not a harness-confirmed count — never silently presented as the
    latter, the same "cache in both directions" law every other liveness fix in this house
    now applies."""
    row = await db.fetchrow(
        f"SELECT count(DISTINCT {_SOUL}) FILTER (WHERE {_SEATED_ROW}) AS souls, "
        f"       count(DISTINCT {_SOUL}) FILTER (WHERE NOT {_SEATED_ROW}) AS visitors "
        "FROM agent_mounts m2 WHERE m2.last_seen > now() - make_interval(secs => $1)",
        float(live_secs))
    return {"souls": int(row["souls"] or 0), "visitors": int(row["visitors"] or 0)}


async def operator_debts(db: _DB, *, hood: str | None = None) -> dict[str, int]:
    """{owed, owed_here} — the RED NUMBER: open threads a mind deliberately placed on the
    human (owner='operator'), minus the miner's guesses (a DERIVED summary is an inference
    wearing a duty's clothes — it may ask, never assert) and minus what the human deferred.
    UNCAPPED — a count must never inherit a display list's LIMIT (the chrome desk showed
    len(a-100-row-fetch) while the statusline counted the table; that was one of the
    disagreements this module exists to end). `hood` scopes owed_here to one project's
    in_repo neighborhood; owed is always the whole desk."""
    row = await db.fetchrow(
        "WITH ops AS (SELECT o.id, "
        "  (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "    AND a.name='deferred_until' "
        "    ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS defer, "
        "  (SELECT replace(p.canonical,'repo:','') FROM links l "
        "    JOIN objects p ON p.id=l.to_id "
        "    WHERE l.from_id=o.id AND l.type='in_repo' AND p.type='SoftwareProject' "
        "    AND (l.valid_until IS NULL OR l.valid_until > now()) "
        "    ORDER BY l.created_at DESC LIMIT 1) AS hood "
        "  FROM objects o WHERE o.type='Thread' AND o.status='active' "
        "  AND (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "    AND a.name='owner' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "    = 'operator' "
        "  AND COALESCE((SELECT a.evidence_class FROM current_assertions a "
        "    WHERE a.object_id=o.id AND a.name='summary' "
        "    ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'') <> 'derived' "
        "  AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a "
        "    WHERE a.object_id=o.id AND a.name='status' "
        "    ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'open') = 'open'), "
        " live AS (SELECT * FROM ops WHERE defer IS NULL "
        "   OR defer <= to_char(now(), 'YYYY-MM-DD')) "
        "SELECT (SELECT count(*) FROM live) AS owed, "
        "       (SELECT count(*) FROM live WHERE hood = $1) AS owed_here",
        hood or "")
    return {"owed": int(row["owed"] or 0), "owed_here": int(row["owed_here"] or 0)}


async def wakes_hour(db: _DB) -> int:
    """Wakes spent in the last hour — the pulse's and the statusline's shared meter."""
    n = await db.fetchval(
        "SELECT count(*) FROM agent_wakes WHERE woke_at > now() - interval '1 hour'")
    return int(n or 0)


# THE VISIT CLASS'S OWN READ-SIDE PREDICATE (9dc3ce8b, the Great Fold's read-side
# adoption): greatfold.py's `demote_visit_families` marks a registration-only family
# `agent_class='visit'` — a DOORBELL RING, never a mind, never folded into a soul. A
# handle-claiming family is `named` — deliberately claimed the seat's own name, the
# strongest positive signal this house has. Neither predicate is retyped anywhere else;
# every headline that counts Agent objects reads it from HERE.
_NAMED_SQL = ("EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
             "AND a.name='handle')")
_VISIT_SQL = ("EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
             "AND a.name='agent_class' AND a.value #>> '{}' = 'visit')")


async def agent_class_counts(db: _DB) -> dict[str, int]:
    """{named_souls, visit_families, unresolved_families} — the STATIC population split
    (every Agent object ever minted, folded to its soul), distinct from `live_souls`
    above (a 900s liveness WINDOW, a completely different axis — a soul can be named yet
    long asleep, or live yet unresolved). This is the promoted READ-SIDE half of
    greatfold.py's own `fold_census`, which computes the identical named/visit/
    unresolved split in Python over the same two predicates for the fold tool's own
    diagnostics (plus fold-specific facts — seat_objects, labels_folded — no read
    surface needs); `fold_census` now calls this instead of re-deriving it, so the two
    never drift. named/visit are DISJOINT BY CONSTRUCTION (fold_agent refuses to demote
    anything carrying a handle) but computed as `named - visit` regardless, defensively
    — the same posture greatfold.py's own fold_census already took.

    9dc3ce8b: "every headline that counts agents... deflates visit-class agents out of
    the named-soul count and names the visit count beside it, one shared predicate,
    never five" — this IS that one predicate. fleet()'s `count` and graph_lint's
    `orphan_census` (Agent bucket) both call this rather than counting raw Agent rows."""
    from src.orchestrator.agents import _generation

    rows = await db.fetch(
        f"SELECT o.canonical, {_NAMED_SQL} AS named, {_VISIT_SQL} AS visit "
        "FROM objects o WHERE o.type='Agent' AND o.status='active'")
    named: set[str] = set()
    visit: set[str] = set()
    families: set[str] = set()
    for r in rows:
        base = _generation(str(r["canonical"]))[0]
        families.add(base)
        if r["named"]:
            named.add(base)
        if r["visit"]:
            visit.add(base)
    return {
        "named_souls": len(named - visit),
        "visit_families": len(visit),
        "unresolved_families": len(families - named - visit),
    }
