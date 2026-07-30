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
    # rooms/cases/objects/assertions are DELIBERATELY ABSENT (task #97): the
    # `actions` fixture below truncates everything in THIS string first, then
    # handles those four separately (scoped deletes for objects/assertions, plain
    # DELETEs for cases/rooms) — sparing the persistent Type catalog. All four were
    # in this string once; if you're re-adding any of them, you are almost
    # certainly re-introducing one of two bugs already caught live, THE SECOND ONE
    # TWICE (it has two hops):
    #   (1) Imhotep's catch (msg 2116): re-adding objects/assertions means the
    #       blanket TRUNCATE wipes the Type rows before the scoped delete runs (which
    #       then no-ops on an already-empty table) — every test raises UnknownTypeError.
    #   (2) THE CASCADE LEAK (found independently, same night, TWO HOPS DEEP): even
    #       with objects/assertions/cases correctly absent, TRUNCATE ... CASCADE
    #       reaches `assertions` via a chain that starts somewhere that looks
    #       harmless: `rooms` (a normal, no-preservation-needed table, easy to
    #       assume safe to leave in this string) has `cases.room_id REFERENCES
    #       rooms(id)` pointing AT it — so truncating `rooms` cascades to `cases`
    #       (a table this string already knows to exclude, but CASCADE doesn't care
    #       what your OWN exclusion list intended), which THEN cascades again to
    #       `assertions` via `case_id REFERENCES cases(id)`. Confirmed empirically
    #       via `conn.add_log_listener` on a live TRUNCATE — Postgres's own NOTICE
    #       output says exactly this: "truncate cascades to table cases" then
    #       "truncate cascades to table assertions". The visible symptom was
    #       identical to bug (1) — `is_known_object_type` reads False and `actions`
    #       below re-seeds on EVERY test, not just the first, each time emitting
    #       ~700 fresh audit_log/outbox rows that silently starve evaluators with a
    #       small default LIMIT (found via test_monitor.py's alert tests returning 0
    #       fired instead of 1 — no exception, no obvious signal, just a quietly
    #       wrong count) — but the FIRST fix attempt (excluding only cases) did NOT
    #       resolve it, because the cascade's actual entry point was rooms, not
    #       cases itself. Lesson: TRUNCATE CASCADE's reachability is NOT limited to
    #       the direct FK neighbors of what you can see in one table's own CREATE
    #       TABLE statement — a LATER migration can add a new FK to an old, innocent-
    #       looking table (0010_rooms.py added cases.room_id years after cases was
    #       first created) and silently extend the cascade graph. Don't reason this
    #       by hand a third time: if a future migration adds a new FK anywhere near
    #       objects/assertions/cases/rooms, re-verify with a live NOTICE listener
    #       (see git history for this comment) rather than re-deriving the graph by
    #       reading CREATE TABLE statements. `rooms` and `cases` are each cleared by
    #       a plain DELETE (never TRUNCATE) issued AFTER the scoped assertions/
    #       objects delete — `rooms`'s own referencing FKs (cases.room_id,
    #       compositions.room_id, console_state.room_id) are all `ON DELETE SET
    #       NULL`, which a real DELETE honors and TRUNCATE never does, so deleting
    #       rooms this way never touches — let alone cascades into — anything else.
    "case_objects,object_events,links,helper_runs,"
    "triggers,audit_log,outbox,merge_candidates,cookie_leases,handoffs,helper_cache,"
    "alerts,watermarks,collection_jobs,compositions,"
    "fleet_messages,message_recipients,agent_wakes,agent_mounts,llm_usage,search_log,"
    # LEAKED ACROSS TESTS UNTIL 2026-07-14. `dev_pulses` was never truncated, so the FIRST test to
    # call pulse() left a row behind and every later one saw a "previous" pulse that belonged to
    # another test entirely. test_pulse's baseline assertion passed only because it happened to run
    # first — for its whole life it was resting on alphabetical luck, and a new test file sorting
    # before it (test_orphan_rows) was enough to break it.
    # A SHARED FIXTURE THAT FORGETS ONE TABLE DOES NOT FAIL — IT MAKES EVERY TEST'S RESULT DEPEND
    # ON WHO RAN BEFORE IT, which is the same class as everything else we killed this week: state
    # written at a seam and never reconciled.
    "dev_pulses,console_state,search_vectors,body_usage"
    # THE HARNESS-AGNOSTIC TRANSCRIPT STORE (ruling be741d3e): per-turn index fed by
    # adapters; truncated like every other sidecar so tests start clean.
    ",harness_sessions,harness_turns"
    # THE TELEMETRY READER (task #35): the retained-events sidecar, same law.
    ",harness_telemetry,harness_telemetry_files"
    # PIT WATCH STAGE B's own ledger (thread 449bf55d): append-only, deriving state by
    # aggregate query — exactly the shape that bites hardest when forgotten here, since a
    # leftover 'escalated' tombstone on a message_id a later test's own fresh sequence
    # reuses (RESTART IDENTITY resets fleet_messages, not this table) silently forges a
    # false "already escalated" for a message that was never touched this test.
    ",pit_watch_alarms"
    # THE DEATH RITE'S OWN COMPLETION LEDGER (Finding A, thread 5177057a): one row per
    # enqueue attempt, same append-only shape as pit_watch_alarms above, same leak risk.
    ",sweep_ledger"
)


@pytest.fixture(scope="session")
def pg_dsn() -> Iterator[str]:
    """A real Postgres in Docker (not SQLite, not mocks), migrated to head. The Type
    catalog (task #97) is seeded lazily by the `actions` fixture below (a cheap
    existence check, real seed only on the first test that needs it) rather than
    here — a session-scoped ASYNC fixture proved unreliable under pytest-asyncio's
    per-function event loop default (see `actions`'s own docstring)."""
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
    catalog doesn't declare RAISES. Runtime stays warn-only. Flips BOTH the legacy
    ontology/schema.py flag (still consulted by not-yet-migrated callers: labels.py,
    app.py, mcp_server.py) and the new graph-backed catalog.py flag (actions/core.py's
    own write path) — remove the schema.py half once every caller has migrated off it."""
    from src.ontology import catalog, schema

    schema.set_strict(True)
    catalog.set_strict(True)
    try:
        yield
    finally:
        schema.set_strict(False)
        catalog.set_strict(False)


@pytest_asyncio.fixture
async def actions(pg_dsn: str) -> AsyncIterator[Actions]:
    pool = await create_pool(pg_dsn)
    async with pool.acquire() as conn:
        # THE CATALOG SURVIVES THE RESET (task #97): everything in _TABLES truncates
        # as before, FIRST — rooms/cases/objects/assertions are excluded from that
        # string (see its own comment for the two-hop CASCADE leak this avoids:
        # rooms -> cases -> assertions).
        await conn.execute(f"TRUNCATE {_TABLES} RESTART IDENTITY CASCADE")
        # objects/assertions: spare exactly the Type rows the catalog check below
        # seeds. assertions.id is bigserial and this scoped delete does NOT reset it
        # (unlike the TRUNCATE ... RESTART IDENTITY every other table gets) — checked
        # before landing this: no test asserts a literal assertions.id value (grepped
        # tests/ + src/ for one; every read is by a dynamically-fetched id, never a
        # hardcoded literal).
        await conn.execute(
            "DELETE FROM assertions a USING objects o "
            "WHERE a.object_id = o.id AND o.type <> 'Type'")
        await conn.execute("DELETE FROM objects WHERE type <> 'Type'")
        # cases and rooms LAST, after the two deletes above — by now nothing in
        # `assertions` still holds a case_id (Type rows never carry one; every other
        # row is already gone), so plain DELETEs (never TRUNCATE, which would need
        # CASCADE again and reopen the exact leak this whole block exists to close)
        # satisfy every FK with zero referencing rows left to violate.
        await conn.execute("DELETE FROM cases")
        await conn.execute("DELETE FROM rooms")
    actions_ = Actions(pool)
    # SEED ONCE, CHEAPLY CHECKED EVERY TEST (not a session-scoped async fixture: pytest-
    # asyncio's per-function event loop default makes that scoping unreliable — a
    # session-scoped async fixture silently re-running, or not surviving, across
    # per-test loops is exactly the kind of framework interaction to verify rather than
    # trust). The scoped delete above preserves Type rows across every test's reset, so
    # after the real first-test seed this is one fast indexed existence check, not a
    # re-seed — cheap enough it was never worth the fixture-scoping risk to avoid.
    from src.ontology.catalog import is_known_object_type, seed_catalog

    if not await is_known_object_type(pool, "Organization"):
        await seed_catalog(actions_)
    try:
        yield actions_
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
