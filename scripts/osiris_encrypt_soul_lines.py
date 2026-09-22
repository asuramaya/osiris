#!/usr/bin/env python3
"""Migrate existing plaintext soul_lines/soul_lines_cold rows to encrypted-at-rest.
A thin CLI over src.ingest.soul_store.encrypt_existing_soul_lines: every write from now
on already encrypts itself; this is the one-time backward pass over what was already in
the table before this build landed.

Dry-run by default, writes nothing. Idempotent: a second run (or a run interrupted
mid-way) finds only what genuinely still needs migrating. Each row is decrypted first
under the CURRENT key, and a row that already opens is skipped, never re-encrypted.
Batched (default 2000 rows/UPDATE), keyset-paginated so a box still ingesting live
sessions during the run is never at risk of a skipped or duplicated row.

--APPLY READINESS (TIP B, ruling 2c01e222's own follow-up thread): a genuinely missing
key now refuses with the exact `osiris soul-key init`/`--restart` remedy instead of an
uncaught SoulKeyMissing traceback — the same honesty every other soul-key-aware door in
this house already gives (soul_crypto.py's own docstrings). Live progress prints per
hot-tier batch (a box measured at 1,255,671 rows once already — a silent multi-minute
--apply with nothing on stdout reads as a hang, not progress). A structured JSON receipt
(counts, elapsed_secs, completion status) prints at the end AND is what `run()` returns,
durable evidence an operator can paste straight into a decision/thread.

Usage: .venv/bin/python scripts/osiris_encrypt_soul_lines.py [--apply] [--batch-size N]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from typing import Any

from src.config.dev_env import refuse_silent_live_db
from src.db.pool import create_pool
from src.ingest.soul_crypto import SoulKeyMissing
from src.ingest.soul_store import encrypt_existing_soul_lines

DSN = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5601/osiris")


async def run(apply: bool, batch_size: int) -> dict[str, Any]:
    refusal = refuse_silent_live_db("osiris_encrypt_soul_lines")
    if refusal is not None:
        print(refusal, file=sys.stderr)
        raise SystemExit(1)
    pool = await create_pool(
        DSN, min_size=1, max_size=2,
        application_name="osiris-script:encrypt-soul-lines")
    started_at = time.monotonic()

    def _progress(migrated_so_far: int, already_so_far: int, batch_number: int) -> None:
        # ONE LINE PER BATCH, never a silent multi-minute run — a human watching an
        # --apply against a real box's own million-row table needs to see it's alive.
        print(f"  batch {batch_number}: {migrated_so_far} migrated, "
              f"{already_so_far} already encrypted so far "
              f"({time.monotonic() - started_at:.1f}s elapsed)", file=sys.stderr)

    try:
        out = await encrypt_existing_soul_lines(
            pool, batch_size=batch_size, dry_run=not apply, on_batch=_progress)
    except SoulKeyMissing as exc:
        # THE SAME HONESTY EVERY OTHER SOUL-KEY DOOR GIVES (soul_crypto.py) — never an
        # uncaught traceback naming an internal function an operator never called.
        print(f"osiris_encrypt_soul_lines: {exc}", file=sys.stderr)
        print("run `osiris soul-key init` (or the console's Init button) first, then "
              "restart osiris-mcp/osiris-worker (--restart does this), then re-run this "
              "script.", file=sys.stderr)
        await pool.close()
        raise SystemExit(1) from exc
    finally:
        await pool.close()
    receipt = {
        "ok": True, "apply": apply, "batch_size": batch_size,
        "hot_migrated": out["hot_migrated"],
        "hot_already_encrypted": out["hot_already_encrypted"],
        "hot_batches": out["hot_batches"],
        "cold_migrated": out["cold_migrated"],
        "cold_already_encrypted": out["cold_already_encrypted"],
        "elapsed_secs": out["elapsed_secs"],
    }
    print(f"soul_lines: {out['hot_migrated']} migrated, "
          f"{out['hot_already_encrypted']} already encrypted "
          f"({out['hot_batches']} batches, {out['elapsed_secs']}s)")
    print(f"soul_lines_cold: {out['cold_migrated']} migrated, "
          f"{out['cold_already_encrypted']} already encrypted")
    if not apply:
        print("dry run — pass --apply to write")
    print(json.dumps(receipt))
    return receipt


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--apply", action="store_true", help="write; default is a dry-run report")
    p.add_argument("--batch-size", type=int, default=2000)
    args = p.parse_args()
    asyncio.run(run(args.apply, args.batch_size))
