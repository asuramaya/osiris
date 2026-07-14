#!/usr/bin/env python
"""Rehearse (default) or apply (--apply) the husk heal — see src/orchestrator/heal.py.

    PYTHONPATH=$PWD uv run python scripts/osiris_heal_husks.py [--apply] [agent:... ...]

With no agents named, runs against the eight husks of 2026-07-14 (ruling f7a715a1). Every
candidate is re-verified at heal time; an agent that acted is refused, loudly. Dry-run by
default — the fold rehearsal's law: an identity write is machine-applied only after a human
has read the exact plan it will apply.
"""
from __future__ import annotations

import asyncio
import json
import sys

from src.actions.core import Actions
from src.config.settings import get_settings
from src.db.pool import create_pool
from src.orchestrator.heal import HUSKS_2026_07_14, heal_husks


async def main() -> None:
    argv = sys.argv[1:]
    apply = "--apply" in argv
    husks = [a for a in argv if not a.startswith("--")] or HUSKS_2026_07_14
    pool = await create_pool(get_settings().database_url)
    try:
        out = await heal_husks(Actions(pool), husks, apply=apply)
        print(json.dumps(out, indent=2, default=str))
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
