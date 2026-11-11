"""osiris init: take a fresh migrated DB to a usable console.

A fresh install is an empty shell: the migrations create the tables but seed NOTHING, so a
clean DB has 0 compositions and an empty design canon. The console lands on nothing and
`run_composition('decision-log')` returns "no composition", the README's promised question
can't be answered. This one idempotent command seeds the default compositions (mirroring the
proven dev-instance shape) and ingests the design canon (docs/reference/ + own docs, the
substrate `consult_canon` reads), so a fresh box has a working console right after
`alembic upgrade head`.

    python -m src.init
    python -m src.init --compositions-only   # deploy step: sync DEFAULT_COMPOSITIONS into a
                                              # LIVE DB, skip the (slow, unrelated) canon
                                              # ingest, ruling 2ee43411, task #63: adding a
                                              # default composition is a first-class deploy step,
                                              # never a raw asyncpg heredoc against the live DB.

ROOM RETIREMENT (decision 31717ca7, Thoth DM 10884/10792, thread 10820): rooms are retired —
the operator's own word was "scope really died and made itself obsolete... gotta remove that
too." Every default composition seeds GLOBAL (room_id NULL); there is no ROOM_CONFIG here to
re-grow the concept on a fresh box. Migration 0070 backfills the 31 compositions that were
scoped on existing installs into `former_room_id`, a standing record, never re-populated by
this command; the `rooms` table itself stays (read-only, never dropped).

Idempotent: compositions upsert by name, the canon find-or-creates on canonical, so re-running
only fixes drift and never duplicates (asserted in tests/test_init.py).

NB the canon step reads repo-relative doc paths (docs/reference/, docs/COMPOSER.md, …) via
`ingest_canon`: same convention as `python -m src.ingest.reference`, so run it from the repo
root (the CLI `main()` chdir's there to be safe; the pure `init()` relies on CWD).
"""

from __future__ import annotations

from typing import Any

from src.actions.core import Actions
from src.ingest.reference import ingest_canon
from src.orchestrator.compositions import DEFAULT_COMPOSITIONS, seed_default_compositions


async def init(actions: Actions, *, canon: bool = True) -> dict[str, Any]:
    """Seed compositions + the design canon on a fresh migrated DB. Idempotent.

    `canon=False` skips the canon ingest, for a caller not at the repo root (the canon reads
    repo-relative doc paths). Returns a summary of what's now present.
    """
    pool = actions.pool
    # every default composition, global (room_id NULL) — rooms are retired (decision 31717ca7);
    # seed_default_compositions never passes a room_id, and save_composition's own "engineer
    # room" fallback only fires when such a room already exists, which a fresh box never has.
    await seed_default_compositions(pool)
    # the design canon (docs/reference/ + own docs) — what `consult_canon` / design-canon read.
    # Guarded to run only when ABSENT: init is a bootstrap ("empty → usable"), and ingest_canon
    # find-or-creates the Reference objects + dedups informs/mentions BUT its `cites` wiring is a
    # plain append (reference.py), so re-ingesting would duplicate the COMPOSER→vendor cites
    # edges. A canon re-sync after adding docs is `python -m src.ingest.reference`, not init.
    canon_result: dict[str, Any] | None = None
    if canon:
        already = await pool.fetchval("SELECT count(*) FROM objects WHERE type='Reference'")
        canon_result = ({"skipped": "canon already present"} if already
                        else await ingest_canon(actions))
    return {
        "compositions": len(DEFAULT_COMPOSITIONS),
        "canon": canon_result,
    }


def _print_next_steps(result: dict[str, Any]) -> None:  # pragma: no cover - CLI cosmetics
    c = result.get("canon") or {}
    if "skipped" in c:
        canon_line = "  · design canon: already present"
    elif c:
        canon_line = f"  · design canon: {c.get('vendor', 0)} vendor + {c.get('own', 0)} own docs"
    else:
        canon_line = "  · design canon: skipped"
    print(
        f"osiris init: ready.\n"
        f"  · compositions: {result['compositions']} seeded (global, rooms are retired)\n"
        f"{canon_line}\n"
        f"\nnext:\n"
        f"  · ingest a repo:   uv run python -m src.ingest.project /path/to/your/repo\n"
        f"  · start the pulse: OSIRIS_DEV_REPOS=/path/a,/path/b "
        f"uv run python -m src.orchestrator.pulse --watch 600\n"
        f"  · drive it with AI: the repo's .mcp.json registers the `osiris` MCP server for "
        f"Claude Code automatically\n"
    )


def main() -> None:  # pragma: no cover - CLI
    import argparse
    import asyncio
    import os
    from pathlib import Path

    from src.config.settings import get_settings
    from src.db.pool import create_pool

    # ruling 2ee43411 (task #63, thread bb763977): adding a DEFAULT composition needs a
    # first-class deploy step, never a raw asyncpg heredoc against the live DB — this flag
    # IS `init()`'s existing `canon=False` path (seed + room compositions, skip the slow,
    # unrelated doc ingest), just reachable without a full fresh-install run.
    p = argparse.ArgumentParser()
    p.add_argument("--compositions-only", action="store_true",
                    help="seed + room DEFAULT_COMPOSITIONS only; skip rooms' canon ingest step "
                         "(rooms themselves still upsert, idempotent, safe on a live DB)")
    args = p.parse_args()

    # the canon step reads repo-relative doc paths; run from the repo root regardless of CWD
    # (src/init.py → src → repo root).
    os.chdir(Path(__file__).resolve().parent.parent)

    async def run() -> None:
        pool = await create_pool(
            get_settings().database_url, application_name="osiris-script:init")
        try:
            _print_next_steps(await init(Actions(pool), canon=not args.compositions_only))
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":  # pragma: no cover
    main()
