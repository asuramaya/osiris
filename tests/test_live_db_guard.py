"""The conftest-level mechanical guard (thread 9b9ba394): agent:repro-test-same
(10 writes) and agent:repro-test-0 (1 write) were a reproduction harness that reached
the LIVE fleet graph instead of an isolated fixture DB. conftest.py now patches
asyncpg.create_pool/asyncpg.connect for the whole pytest process so nothing — a
fixture, a script imported and driven by a repro test, a CLI function called directly —
can open a connection outside this session's own testcontainer, keyed on the DSN's own
host:port (never an env flag a caller could forget, never cwd)."""
from __future__ import annotations

import asyncpg
import pytest


async def test_live_db_guard_refuses_the_live_dsn_shape() -> None:
    """The exact shape agent:repro-test-same/repro-test-0 actually hit — the box's real
    fleet DB (postgresql://osiris:osiris@127.0.0.1:5601/osiris) — must be refused before
    any network attempt, not time out trying to reach it."""
    with pytest.raises(RuntimeError, match="LIVE-DB GUARD"):
        await asyncpg.create_pool("postgresql://osiris:osiris@127.0.0.1:5601/osiris")


async def test_live_db_guard_refuses_connect_too() -> None:
    """asyncpg.connect is a separate entrypoint from create_pool (osiris_stophook.py's
    own bare-connection callers use it) — the guard must cover both, not just one."""
    with pytest.raises(RuntimeError, match="LIVE-DB GUARD"):
        await asyncpg.connect("postgresql://osiris:osiris@127.0.0.1:5601/osiris")


async def test_live_db_guard_refuses_a_mismatched_port_on_the_same_host() -> None:
    """A near-miss (right host, wrong port — not just an obviously different box) must
    still refuse: the check is exact host:port equality, not a same-host heuristic."""
    with pytest.raises(RuntimeError, match="LIVE-DB GUARD"):
        await asyncpg.create_pool("postgresql://test:test@127.0.0.1:1/test")


async def test_live_db_guard_passes_the_sessions_own_testcontainer(pg_dsn: str) -> None:
    """The whole rest of this suite already proves this path works (every `actions`-
    fixture test routes through it) — this test names the guarantee explicitly rather
    than leaving it implicit in the suite's own success."""
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=1)
    try:
        assert await pool.fetchval("SELECT 1") == 1
    finally:
        await pool.close()
