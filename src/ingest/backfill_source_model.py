"""Backfill the missing provenance dimension on history (task #21).

source_model — which Claude authored a write — is stamped at write time only on the
session-miner path (`ingest/sessions.py`). A fleet agent that captures deliberately
(`capture.record_decision` / `open_thread` with `source='agent:<sid>'`) predates that
stamping, so its historical Decisions/Threads carry every other fact but not the model.
The datum is recoverable: the authoring agent registered itself (`orchestrator/agents.py`)
with a `source_model` on its `agent:<sid>` object. This offline, idempotent pass reads that
registration and stamps the record.

Graded DERIVED, deliberately: it is an INFERENCE from the agent's registered identity, not
the write-time harness word — and if the agent warm-swapped mid-session, its registered
source_model is the latest model, not provably the one at THIS write. So it grades below any
real (observed / self-declared) source_model and never out-ranks one; a record that already
carries the dimension is left untouched.

OFFLINE ONLY — a one-shot repair the operator runs deliberately (Osiris has no hands): it
reads the graph's own Agent registrations and writes back an inference through the Actions
waist (append-only; it asserts a new property, never UPDATEs a row). Never a cron.

    python -m src.ingest.backfill_source_model
"""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import asyncpg

from src.actions.core import Actions
from src.config.settings import get_settings
from src.db.pool import create_pool
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

# A dedicated source so the DERIVED backfill coexists with (never masquerades as) the real
# stamps, and so idempotency reads cleanly (a stamped record already carries source_model).
_SOURCE = "source-model-backfill"
_EC = EvidenceClass.DERIVED.value
_CONF = confidence_for(EvidenceClass.DERIVED)


async def _agent_model(pool: asyncpg.Pool, agent_canonical: str) -> str | None:
    """The authoring Agent's WINNING source_model (winning_props, migration 0015: grade DESC,
    then recency), or None when the Agent object is absent or never resolved a model."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT wp.value #>> '{}' "
        "FROM objects o, winning_props(ARRAY[o.id]::uuid[]) wp "
        "WHERE o.type='Agent' AND o.canonical=$1 AND wp.name='source_model'",
        agent_canonical,
    )


async def backfill_source_model(actions: Actions) -> dict[str, int]:
    """Stamp source_model on agent-authored Decisions/Threads that predate model stamping.

    For each such record MISSING a source_model, read the authoring Agent's winning
    source_model and assert it DERIVED. Idempotent: a record that already carries a
    source_model (from ANY source — the dimension is present, real or a prior backfill) is
    not a candidate, so a re-run is a no-op. Returns counts."""
    pool = actions.pool
    # Candidates: an active Decision/Thread whose DEFINING assertion (summary) was authored by
    # a fleet agent (agent:<sid>), that does NOT yet carry any source_model. One row per object
    # (DISTINCT ON) anchored on the EARLIEST agent author — the original writer when a summary's
    # hash-canonical was reached by more than one agent.
    rows = await pool.fetch(
        "SELECT DISTINCT ON (o.id) o.id AS obj_id, a.source_id AS agent "
        "FROM objects o "
        "JOIN current_assertions a ON a.object_id=o.id AND a.name='summary' "
        "WHERE o.type IN ('Decision','Thread') AND o.status='active' "
        "  AND a.source_id LIKE 'agent:%' "
        "  AND NOT EXISTS (SELECT 1 FROM assertions sm WHERE sm.object_id=o.id "
        "                  AND sm.name='source_model') "
        "ORDER BY o.id, a.observed_at ASC"
    )
    now = datetime.now(UTC)
    models: dict[str, str | None] = {}  # cache the winning model per authoring agent
    stamped = 0
    skipped_no_model = 0
    for r in rows:
        agent: str = r["agent"]
        if agent not in models:
            models[agent] = await _agent_model(pool, agent)
        model = models[agent]
        if model is None:  # the agent carries no source_model — nothing to infer from
            skipped_no_model += 1
            continue
        await actions.assert_property(r["obj_id"], "source_model", model, _SOURCE, now,
                                      _CONF, evidence_class=_EC)
        stamped += 1
    return {"candidates": len(rows), "stamped": stamped, "skipped_no_model": skipped_no_model}


def main() -> None:  # pragma: no cover - CLI
    """Run the backfill once against the configured graph (OFFLINE, explicit — never a cron)."""

    async def run() -> None:
        pool = await create_pool(
            get_settings().database_url, application_name="osiris-script:ingest-backfill-model")
        try:
            print(await backfill_source_model(Actions(pool)))
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":  # pragma: no cover
    main()
