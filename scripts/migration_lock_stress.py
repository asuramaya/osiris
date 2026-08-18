#!/usr/bin/env python3
"""Does this migration deadlock — or just stall — a LIVE reader? (thread 2a280e07's
own migration, Thoth's reproduced-twice deadlock on DROP VIEW current_assertions, msg
4228; decision 259e5c5b's fix, decision a8026bf0's own ad hoc version of this script).

The isolation that makes ordinary migration testing safe (conftest.py's pg_dsn, a fresh
testcontainers Postgres per test worker) is EXACTLY what blinds it to lock contention —
there is no concurrent load in that container. A migration can pass every gate, be
provably correct against the real corpus, and still be structurally undeployable against
a live fleet, because the isolated test that proves correctness cannot also prove it
won't block the thing it's replacing. This is the general instrument for that specific,
different question: spins up a throwaway testcontainers Postgres, migrates it to the
revision UNDER TEST's own `down_revision` (the baseline), seeds it, starts a background
reader loop hammering a query you supply, then runs `alembic upgrade <rev>` as a REAL
subprocess against the SAME database while the reader loop is still live. Reports
whether the migration completed, how long it took, and whether the reader loop ever saw
an error (a lock wait, a deadlock, or a "relation does not exist" gap).

Usage:
    scripts/migration_lock_stress.py 0047 \\
        --query "SELECT object_id, name, value FROM current_assertions LIMIT 50" \\
        --seed-objects 500 --seed-depth 20

`--seed-objects`/`--seed-depth` build a synthetic assertions_supersedes chain (N objects,
each with a D-deep same-source supersession chain) — the general shape most migrations
touching `assertions`/`current_assertions` care about; pass `--seed-sql <file>` instead
for anything else the migration under test actually needs live in the table.

Exit code 0 = migration completed with zero reader errors. Exit code 1 = either the
migration itself failed (its own stdout/stderr are printed) or the reader loop recorded
at least one error while it ran (printed, first 10).
"""
from __future__ import annotations

import argparse
import asyncio
import importlib.util
import os
import subprocess
import sys
import time
from pathlib import Path

import asyncpg
from testcontainers.postgres import PostgresContainer

REPO_ROOT = Path(__file__).resolve().parent.parent
# resolve alongside THIS interpreter, same as every other repo script that shells out
# to a sibling tool — never relies on the caller's own PATH having the venv active
ALEMBIC = str(Path(sys.executable).parent / "alembic")


def _down_revision(target_rev: str) -> str:
    """Read the target revision's own `down_revision` straight off its migration file —
    never guessed, never hand-maintained, so this stays correct as new migrations land."""
    matches = list((REPO_ROOT / "alembic" / "versions").glob(f"{target_rev}_*.py"))
    if not matches:
        raise SystemExit(f"no migration file found for revision {target_rev!r}")
    spec = importlib.util.spec_from_file_location("target_migration", matches[0])
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    down = module.down_revision
    if not down:
        raise SystemExit(f"{target_rev} has no down_revision — nothing to baseline against")
    return str(down)


async def _reader_loop(
    dsn: str, query: str, interval: float, stop: asyncio.Event, errors: list,
) -> int:
    pool = await asyncpg.create_pool(dsn, min_size=4, max_size=4)
    n = 0
    while not stop.is_set():
        try:
            async with pool.acquire() as c:
                await c.fetch(query)
            n += 1
        except Exception as exc:  # noqa: BLE001 - every failure shape is the finding here
            errors.append((time.time(), repr(exc)))
        await asyncio.sleep(interval)
    await pool.close()
    return n


async def _seed(dsn: str, n_objects: int, depth: int) -> None:
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=1)
    async with pool.acquire() as c:
        await c.execute(
            "INSERT INTO objects (id, type, canonical, status) "
            "SELECT gen_random_uuid(), 'Type', 'stress-obj-'||g, 'active' "
            "FROM generate_series(1, $1) g", n_objects)
        oids = [r["id"] for r in await c.fetch(
            "SELECT id FROM objects WHERE canonical LIKE 'stress-obj-%'")]
        for oid in oids:
            prior = None
            for i in range(depth):
                prior = await c.fetchval(
                    "INSERT INTO assertions (object_id,name,value,source_id,observed_at,"
                    "confidence,supersedes) VALUES ($1,'val',$2::jsonb,'seed',now(),0.9,$3) "
                    "RETURNING id",
                    oid, f'{{"v": {i}}}', prior)
    await pool.close()
    print(f"seeded {n_objects * depth} assertion rows across {n_objects} objects, "
          f"{depth}-deep chains each")


def _run_alembic(rev: str, sync_env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [ALEMBIC, "upgrade", rev], cwd=REPO_ROOT, env={**os.environ, **sync_env},
        capture_output=True, text=True)


async def run(args: argparse.Namespace, seed_sql_text: str | None) -> int:
    baseline = _down_revision(args.revision)
    print(f"baselining at {baseline} (the revision under test's own down_revision)")

    with PostgresContainer("postgres:16", username="test", password="test", dbname="test") as pg:
        dsn = pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        sync_env = {"DATABASE_URL": dsn.replace("postgresql://", "postgresql+psycopg://")}

        baseline_result = await asyncio.to_thread(_run_alembic, baseline, sync_env)
        if baseline_result.returncode != 0:
            print(baseline_result.stdout, baseline_result.stderr)
            raise SystemExit(f"baseline migration to {baseline} failed")

        if seed_sql_text is not None:
            pool = await asyncpg.create_pool(dsn, min_size=1, max_size=1)
            async with pool.acquire() as c:
                await c.execute(seed_sql_text)
            await pool.close()
            print(f"seeded via {args.seed_sql}")
        elif args.seed_objects:
            await _seed(dsn, args.seed_objects, args.seed_depth)

        stop = asyncio.Event()
        errors: list = []
        reader = asyncio.create_task(
            _reader_loop(dsn, args.query, args.interval, stop, errors))
        await asyncio.sleep(0.3)  # let the reader loop get going before the migration starts

        print(f"running `alembic upgrade {args.revision}` WHILE the reader loop is live...")
        t0 = time.perf_counter()
        result = await asyncio.to_thread(_run_alembic, args.revision, sync_env)
        t1 = time.perf_counter()
        print(f"migration finished in {t1-t0:.2f}s, returncode={result.returncode}")
        if result.returncode != 0:
            print("--- STDOUT ---\n", result.stdout)
            print("--- STDERR ---\n", result.stderr)

        await asyncio.sleep(0.3)
        stop.set()
        n_ok = await reader

        print(f"\nreader loop: {n_ok} successful queries, {len(errors)} errors")
        for _, e in errors[:10]:
            print(f"  reader error: {e}")

        if result.returncode != 0 or errors:
            print("\nFAILED — this migration blocks or errors a live concurrent reader")
            return 1
        print("\nPASSED — completed with zero reader errors under live load")
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                      formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("revision", help="the migration revision under test, e.g. 0047")
    parser.add_argument("--query", required=True,
                         help="the read query a live agent would run during this migration")
    parser.add_argument("--interval", type=float, default=0.01,
                         help="seconds between reader-loop queries (default 0.01)")
    parser.add_argument("--seed-objects", type=int, default=0,
                         help="synthetic object count for a generic assertions/"
                              "supersedes seed (skip if --seed-sql is given)")
    parser.add_argument("--seed-depth", type=int, default=20,
                         help="supersession chain depth per seeded object (default 20)")
    parser.add_argument("--seed-sql", default=None,
                         help="path to a .sql file to run instead of the generic seed, "
                              "for migrations that need a different shape live")
    args = parser.parse_args()
    seed_sql_text = Path(args.seed_sql).read_text() if args.seed_sql else None
    return asyncio.run(run(args, seed_sql_text))


if __name__ == "__main__":
    raise SystemExit(main())
