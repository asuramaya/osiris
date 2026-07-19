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


class _DB(Protocol):
    """The slice of asyncpg's pool/connection API the vitals need — both satisfy it."""

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
# really its 4 live'). The roman-suffix strip folds generations to their base.
_SOUL = "regexp_replace(m2.agent_id, '-[ivxlcdm]+$', '')"


async def live_souls(db: _DB, *, live_secs: int = 900) -> dict[str, int]:
    """{souls, visitors} — distinct live minds (seated) and distinct live strangers."""
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
