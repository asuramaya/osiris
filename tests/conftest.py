from __future__ import annotations

import http.server
import os
import threading
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
    "triggers,audit_log,outbox,merge_candidates,cookie_leases,handoffs,helper_cache,"
    "alerts,watermarks,collection_jobs,compositions,rooms,"
    "fleet_messages,message_recipients,agent_wakes,agent_mounts,llm_usage,search_log,"
    # LEAKED ACROSS TESTS UNTIL 2026-07-14. `dev_pulses` was never truncated, so the FIRST test to
    # call pulse() left a row behind and every later one saw a "previous" pulse that belonged to
    # another test entirely. test_pulse's baseline assertion passed only because it happened to run
    # first — for its whole life it was resting on alphabetical luck, and a new test file sorting
    # before it (test_orphan_rows) was enough to break it.
    # A SHARED FIXTURE THAT FORGETS ONE TABLE DOES NOT FAIL — IT MAKES EVERY TEST'S RESULT DEPEND
    # ON WHO RAN BEFORE IT, which is the same class as everything else we killed this week: state
    # written at a seam and never reconciled.
    "dev_pulses,console_state,search_vectors"
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


@pytest.fixture(autouse=True)
def _strict_schema() -> Iterator[None]:
    """Enforce the semantic layer in CI: any object/link type a test emits that the
    catalog (ontology/schema.py) doesn't declare RAISES. Runtime stays warn-only."""
    from src.ontology import schema

    schema.set_strict(True)
    try:
        yield
    finally:
        schema.set_strict(False)


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


class _SiteHandler(http.server.BaseHTTPRequestHandler):
    """Tiny site: '/' sets a session cookie; '/protected' needs it (else a
    Cloudflare-ish 403). Lets co-browse capture a real cookie and prove reuse."""

    def log_message(self, *args: object) -> None:  # silence test noise
        pass

    def do_GET(self) -> None:  # noqa: N802 (BaseHTTPRequestHandler API)
        if self.path == "/":
            self.send_response(200)
            self.send_header("Set-Cookie", "session=secret123; Path=/")
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(
                b"<html><head><title>DPRK News</title></head><body>"
                b"<div id='subs'>12345</div><div id='desc'>state media mirror</div>"
                b"</body></html>"
            )
        elif self.path == "/protected":
            if "session=secret123" in self.headers.get("Cookie", ""):
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(b"<html><body><span id='secret'>topsecret</span></body></html>")
            else:
                self.send_response(403)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(b"<title>Just a moment...</title>")
        else:
            self.send_response(404)
            self.end_headers()


@pytest.fixture
def local_site() -> Iterator[str]:
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _SiteHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
