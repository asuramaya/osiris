from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from alembic import command
from alembic.config import Config
from src.actions.core import Actions
from src.db.pool import create_pool
from src.db.redis import create_redis
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

_TABLES = (
    "cases,objects,case_objects,object_events,assertions,links,helper_runs,"
    "triggers,audit_log,outbox,merge_candidates,cookie_leases"
)


@pytest.fixture(scope="session")
def pg_dsn() -> Iterator[str]:
    """A real Postgres in Docker (not SQLite, not mocks), migrated to head."""
    with PostgresContainer("postgres:16", username="test", password="test", dbname="test") as pg:
        host = pg.get_container_host_ip()
        port = pg.get_exposed_port(5432)
        dsn = f"postgresql://test:test@{host}:{port}/test"
        os.environ["DATABASE_URL"] = dsn  # env.py reads this and converts to a sync psycopg URL
        command.upgrade(Config("alembic.ini"), "head")
        yield dsn


@pytest_asyncio.fixture
async def actions(pg_dsn: str) -> AsyncIterator[Actions]:
    pool = await create_pool(pg_dsn)
    async with pool.acquire() as conn:
        await conn.execute(f"TRUNCATE {_TABLES} RESTART IDENTITY CASCADE")
    try:
        yield Actions(pool)
    finally:
        await pool.close()


@pytest_asyncio.fixture
async def case_id(actions: Actions) -> str:
    cid = await actions.pool.fetchval(
        "INSERT INTO cases (name, owner) VALUES ('test-case','analyst:test') RETURNING id"
    )
    return str(cid)


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    """A real Redis in Docker for token buckets / budget counters."""
    with RedisContainer("redis:7") as rc:
        host = rc.get_container_host_ip()
        port = rc.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"


@pytest_asyncio.fixture
async def redis_client(redis_url: str) -> AsyncIterator[aioredis.Redis]:
    client = create_redis(redis_url)
    await client.flushall()
    try:
        yield client
    finally:
        await client.aclose()
