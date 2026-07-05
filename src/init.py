"""osiris init — take a fresh migrated DB to a usable console.

A fresh install is an empty shell: the migrations create the tables but seed NOTHING, so a
clean DB has 0 rooms, 0 compositions, and an empty design canon. The console lands on nothing
and `run_composition('decision-log')` returns "no composition" — the README's promised
question can't be answered. This one idempotent command seeds the default rooms + compositions
(mirroring the proven dev-instance shape) and ingests the design canon (docs/reference/ + own
docs, the substrate `consult_canon` reads), so a fresh box has a working console right after
`alembic upgrade head`.

    python -m src.init

Idempotent: rooms/compositions upsert by name, the canon find-or-creates on canonical — so
re-running only fixes drift and never duplicates (asserted in tests/test_init.py).

NB the canon step reads repo-relative doc paths (docs/reference/, docs/COMPOSER.md, …) via
`ingest_canon` — same convention as `python -m src.ingest.reference`, so run it from the repo
root (the CLI `main()` chdir's there to be safe; the pure `init()` relies on CWD).
"""

from __future__ import annotations

from typing import Any

from src.actions.core import Actions
from src.ingest.reference import ingest_canon
from src.orchestrator.compositions import (
    DEFAULT_COMPOSITIONS,
    create_room,
    save_composition,
    seed_default_compositions,
)

# The default stance shape, mirroring the proven dev instance (CLAUDE.md: land→engineer/briefing;
# analyst opts into collection). `home` = the composition the console lands on; `collect` = show
# the entity-intake chrome. BOTH keys are read by the UI (src/ui/static/index.html). The
# ENGINEER stance is created first so a fresh box lands on it (list_rooms orders by created_at).
ROOM_CONFIG: dict[str, dict[str, Any]] = {
    "engineer": {"home": "briefing"},
    "analyst": {"home": "who-is-this", "collect": True},
}
# Which default lens goes in which stance. ENGINEER = the developer project-memory lenses (repos
# come in via CLI, no entity collection); ANALYST = the public-record entity lenses. Every name
# here MUST exist in DEFAULT_COMPOSITIONS (a typo KeyErrors at assign time; coverage is asserted
# in the test) — this seeds NO new compositions, it only rooms the ones the composer already owns.
ROOM_COMPOSITIONS: dict[str, tuple[str, ...]] = {
    "engineer": (
        "briefing", "project-briefing", "decision-log", "design-canon", "family-consistency",
        "family-drift", "portfolio", "pulse-digest", "projects", "project", "fleet",
    ),
    "analyst": (
        "who-is-this", "operational-vs-disclosed-geography", "co-investment-ties",
        "screen-financing-network",
    ),
}


async def init(actions: Actions, *, canon: bool = True) -> dict[str, Any]:
    """Seed rooms + compositions + the design canon on a fresh migrated DB. Idempotent.

    `canon=False` skips the canon ingest — for a caller not at the repo root (the canon reads
    repo-relative doc paths). Returns a summary of what's now present.
    """
    pool = actions.pool
    # 1. ensure every default composition exists (unassigned to a room at this point).
    await seed_default_compositions(pool)
    # 2. rooms + assign their lenses. Re-saving with a room_id upserts it (COALESCE keeps a room
    #    on a later seed pass, so a second full init never orphans a composition from its stance).
    roomed = 0
    for name, config in ROOM_CONFIG.items():
        room_id = await create_room(pool, name, config)
        for comp in ROOM_COMPOSITIONS[name]:
            await save_composition(pool, comp, DEFAULT_COMPOSITIONS[comp], "lens", room_id=room_id)
            roomed += 1
    # 3. the design canon (docs/reference/ + own docs) — what `consult_canon` / design-canon read.
    #    Guarded to run only when ABSENT: init is a bootstrap ("empty → usable"), and ingest_canon
    #    find-or-creates the Reference objects + dedups informs/mentions BUT its `cites` wiring is a
    #    plain append (reference.py), so re-ingesting would duplicate the COMPOSER→vendor cites
    #    edges. A canon re-sync after adding docs is `python -m src.ingest.reference`, not init.
    canon_result: dict[str, Any] | None = None
    if canon:
        already = await pool.fetchval("SELECT count(*) FROM objects WHERE type='Reference'")
        canon_result = ({"skipped": "canon already present"} if already
                        else await ingest_canon(actions))
    return {
        "rooms": len(ROOM_CONFIG),
        "compositions": len(DEFAULT_COMPOSITIONS),
        "roomed": roomed,
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
        f"osiris init — ready.\n"
        f"  · rooms: {result['rooms']} (engineer, analyst)\n"
        f"  · compositions: {result['compositions']} seeded, {result['roomed']} roomed\n"
        f"{canon_line}\n"
        f"\nnext:\n"
        f"  · ingest a repo:   uv run python -m src.ingest.project /path/to/your/repo\n"
        f"  · start the pulse: OSIRIS_DEV_REPOS=/path/a,/path/b "
        f"uv run python -m src.orchestrator.pulse --watch 600\n"
        f"  · drive it with AI: the repo's .mcp.json registers the `osiris` MCP server for "
        f"Claude Code automatically\n"
    )


def main() -> None:  # pragma: no cover - CLI
    import asyncio
    import os
    from pathlib import Path

    from src.config.settings import get_settings
    from src.db.pool import create_pool

    # the canon step reads repo-relative doc paths; run from the repo root regardless of CWD
    # (src/init.py → src → repo root).
    os.chdir(Path(__file__).resolve().parent.parent)

    async def run() -> None:
        pool = await create_pool(get_settings().database_url)
        try:
            _print_next_steps(await init(Actions(pool)))
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":  # pragma: no cover
    main()
