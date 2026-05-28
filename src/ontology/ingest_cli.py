"""CLI: seed the ontology from the live MITRE ATT&CK bundle.

    uv run python -m src.ontology.ingest_cli [enterprise|mobile|ics]

Creates (or reuses) a 'mitre-attack-seed' case and ingests the bundle through
the Actions layer. Idempotent: re-running re-asserts against existing objects
(canonical = STIX id) rather than duplicating.
"""

from __future__ import annotations

import asyncio
import sys

from src.actions.core import Actions
from src.config.settings import get_settings
from src.connectors.mitre import fetch_attack_bundle
from src.db.pool import create_pool
from src.ontology.ingest import ingest_bundle


async def _main(domain: str) -> None:
    settings = get_settings()
    pool = await create_pool(settings.database_url)
    try:
        actions = Actions(pool)
        case_name = f"mitre-attack-{domain}"
        case_id = await pool.fetchval("SELECT id FROM cases WHERE name=$1", case_name)
        if case_id is None:
            case_id = await pool.fetchval(
                "INSERT INTO cases (name, owner) VALUES ($1,$2) RETURNING id",
                case_name,
                settings.osiris_actor,
            )
        print(f"fetching ATT&CK {domain} bundle ...", flush=True)
        bundle = await fetch_attack_bundle(domain)
        print(f"ingesting {len(bundle.get('objects', []))} STIX objects ...", flush=True)
        report = await ingest_bundle(actions, bundle, case_id=case_id)
        print(
            f"done: {report.objects} objects, {report.links} links, "
            f"{report.skipped} skipped, {report.dangling_refs} dangling refs"
        )
    finally:
        await pool.close()


if __name__ == "__main__":
    domain = sys.argv[1] if len(sys.argv) > 1 else "enterprise"
    asyncio.run(_main(domain))
