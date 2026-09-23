#!/usr/bin/env python3
"""Migrate existing plaintext soul_lines/soul_lines_cold rows to encrypted-at-rest.
A thin CLI over src.ingest.soul_store.encrypt_existing_soul_lines: every write from now
on already encrypts itself; this is the one-time backward pass over what was already in
the table before this build landed.

Dry-run by default, writes nothing. Idempotent: a second run (or a run interrupted
mid-way) finds only what genuinely still needs migrating — each row is decrypted first
under the CURRENT key, and a row that already opens is skipped, never re-encrypted.
Batched (default 2000 rows/UPDATE), keyset-paginated so a box still ingesting live
sessions during the run is never at risk of a skipped or duplicated row.

Usage: .venv/bin/python scripts/osiris_encrypt_soul_lines.py [--apply] [--batch-size N]
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

from src.config.dev_env import refuse_silent_live_db
from src.db.pool import create_pool
from src.ingest.soul_store import encrypt_existing_soul_lines

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")


async def run(apply: bool, batch_size: int) -> None:
    refusal = refuse_silent_live_db("osiris_encrypt_soul_lines")
    if refusal is not None:
        print(refusal, file=sys.stderr)
        raise SystemExit(1)
    pool = await create_pool(
        DSN, min_size=1, max_size=2,
        application_name="osiris-script:encrypt-soul-lines")
    try:
        out = await encrypt_existing_soul_lines(pool, batch_size=batch_size, dry_run=not apply)
    finally:
        await pool.close()
    print(f"soul_lines: {out['hot_migrated']} migrated, "
          f"{out['hot_already_encrypted']} already encrypted")
    print(f"soul_lines_cold: {out['cold_migrated']} migrated, "
          f"{out['cold_already_encrypted']} already encrypted")
    if not apply:
        print("dry run — pass --apply to write")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--apply", action="store_true", help="write; default is a dry-run report")
    p.add_argument("--batch-size", type=int, default=2000)
    args = p.parse_args()
    asyncio.run(run(args.apply, args.batch_size))
