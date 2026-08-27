from __future__ import annotations

import contextlib
import contextvars
import http.server
import os
import sys
import threading
from collections.abc import AsyncIterator, Iterator

import psycopg
import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from alembic import command
from alembic.config import Config
from psycopg import sql
from src.actions.core import Actions
from src.db.pool import create_pool
from src.db.redis import create_redis
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

# rooms/cases/objects/assertions are DELIBERATELY ABSENT (task #97): the `actions`
# fixture below resets everything in THIS tuple first, then handles those four
# separately (scoped deletes for objects/assertions, plain DELETEs for cases/rooms) —
# sparing the persistent Type catalog. All four were in this set once; if you're
# re-adding any of them, you are almost certainly re-introducing one of two bugs
# already caught live, THE SECOND ONE TWICE (it has two hops):
#   (1) Imhotep's catch (msg 2116): re-adding objects/assertions means the blanket
#       reset wipes the Type rows before the scoped delete runs (which then no-ops
#       on an already-empty table) — every test raises UnknownTypeError.
#   (2) THE CASCADE LEAK (found independently, same night, TWO HOPS DEEP): even with
#       objects/assertions/cases correctly absent, TRUNCATE ... CASCADE (this set's
#       reset statement BEFORE task #100) reached `assertions` via a chain that
#       starts somewhere that looks harmless: `rooms` (a normal, no-preservation-
#       needed table, easy to assume safe to leave in this set) has `cases.room_id
#       REFERENCES rooms(id)` pointing AT it — so truncating `rooms` cascaded to
#       `cases` (a table this set already knew to exclude, but CASCADE doesn't care
#       what your OWN exclusion list intended), which THEN cascaded again to
#       `assertions` via `case_id REFERENCES cases(id)`. Confirmed empirically via
#       `conn.add_log_listener` on a live TRUNCATE — Postgres's own NOTICE output
#       said exactly this: "truncate cascades to table cases" then "truncate
#       cascades to table assertions". Lesson, still true under DELETE even though
#       the specific TRUNCATE-CASCADE mechanism is gone (see below): a LATER
#       migration can add a new FK to an old, innocent-looking table (0010_rooms.py
#       added cases.room_id years after cases was first created) and silently widen
#       what a reset touches. Don't reason this by hand: if a future migration adds
#       a new FK anywhere near objects/assertions/cases/rooms/_RESET_TABLES, re-
#       verify with a live query against pg_constraint (see _RESET_TABLES's own
#       comment for the exact query) rather than re-deriving the graph by reading
#       CREATE TABLE statements. `rooms` and `cases` are each cleared by a plain
#       DELETE issued AFTER the scoped assertions/objects delete — `rooms`'s own
#       referencing FKs (cases.room_id, compositions.room_id, console_state.room_id)
#       are all `ON DELETE SET NULL`, which DELETE honors, so deleting rooms this
#       way never touches — let alone cascades into — anything else.
#
# TASK #100 (Thoth msg 2144/2152/2161): this used to be one `TRUNCATE {_TABLES}
# RESTART IDENTITY CASCADE` statement. Measured live (decision 335ddd13): TRUNCATE
# pays a FIXED catalog/lock cost per table REGARDLESS of row count — ~270ms average
# even against empty tables, ~73-83% of the whole suite's 588s, dwarfing pool
# creation (~19ms) and everything else in this fixture combined. Since every one of
# these tables is nearly empty at reset time (a handful of rows a single test wrote,
# at most), DELETE beats TRUNCATE by roughly two orders of magnitude — DELETE's cost
# scales with row count, TRUNCATE's doesn't (decision 41c47976: ~9.88ms average
# against the same empty-table benchmark, ~27x). Sequential DELETEs need the FK-
# dependency order TRUNCATE's own CASCADE used to compute for you — get that order
# from Postgres itself, never by hand (the exact lesson bug (2) above already
# taught): `SELECT conrelid::regclass::text AS child, confrelid::regclass::text AS
# parent FROM pg_constraint WHERE contype = 'f'`, filtered to edges where both sides
# are in _RESET_TABLES, topologically sorted child-before-parent. Re-run that query
# and re-sort if a migration adds a new FK among these tables — a stale order fails
# LOUDLY (a FK violation on the misordered DELETE), never silently, which is the one
# way this is safer than TRUNCATE CASCADE's silent-widening failure mode.
#
# ONE REAL BEHAVIOR CHANGE: DELETE does not reset sequences the way TRUNCATE
# RESTART IDENTITY did — bigserial ids climb across the whole session instead of
# restarting at 1 every test. Verified before landing this: no test anywhere in
# tests/ or src/ asserts a literal id for any of these 31 tables (regex-swept, not
# just spot-checked — one incidental false positive, a UA-string, zero real hits).
_RESET_TABLES = (
    "agent_mounts", "agent_wakes", "alerts", "audit_log", "body_usage", "case_objects",
    "collection_jobs", "console_state", "cookie_leases", "dev_pulses", "handoffs",
    "harness_telemetry", "harness_telemetry_files", "harness_turns", "helper_cache",
    "links", "llm_usage", "mcp_tool_stats", "merge_candidates", "message_recipients",
    "object_events", "outbox", "pit_watch_alarms", "resource_leases", "search_log",
    "search_vectors",
    "sweep_ledger", "triggers", "watermarks",
    # these four must come LAST, in this order — each is the PARENT side of an
    # internal FK from a table above it in this tuple (alerts->compositions,
    # handoffs->helper_runs, harness_turns->harness_sessions,
    # message_recipients->fleet_messages), so a CHILD row can still reference it
    # while everything above deletes. Deleting a parent before its child violates
    # the FK — this order was wrong once already (caught live: the first version of
    # this tuple had the direction backwards, an inverted topological sort that a
    # timing-only benchmark against empty tables never exercised; the real suite's
    # FK violations on handoffs->helper_runs are what caught it). Everything above
    # this line has no FK to anything else in this tuple (verified via the
    # pg_constraint query above, then re-verified against REAL referencing rows —
    # not just an empty-table benchmark — for all four pairs before landing this
    # fix) and can run in any order relative to each other.
    "compositions", "fleet_messages", "harness_sessions", "helper_runs",
)


# PYTEST-XDIST PARALLELIZATION (task #100's tail, Thoth msg 2224): pg_dsn used to be
# one session-scoped PostgresContainer per pytest PROCESS. Under `-n auto`, xdist
# runs each worker as its OWN process — that fixture, unmodified, would silently
# start N separate containers (one per worker), which is legal but exactly the
# "one container per run" docker load this build exists to keep flat, multiplied by
# worker count instead of collapsed to one. ONE container instead, shared by every
# worker, each worker with its OWN DATABASE inside it (test_gw0, test_gw1, ... —
# "test" itself, unchanged, outside xdist) — parallelism at the pytest level, on top
# of (not instead of) task #100's own per-database ordered-DELETE isolation between
# tests within a worker.
#
# WHY HOOKS, NOT A FIXTURE, OWN THE CONTAINER: xdist's controller process never runs
# a test and never evaluates a test-scoped fixture — only worker processes do. A
# session-scoped fixture body only runs inside a worker, so nothing "session-scoped"
# can single-flight across workers; every worker would independently race to be the
# one that starts it. `pytest_configure`/`pytest_unconfigure` are different: real
# pytest hooks, and they DO fire once in the controller too. `hasattr(config,
# "workerinput")` is xdist's own documented way to tell a worker's pytest_configure
# from the controller's (or a plain non-xdist run's, which takes the same branch as
# the controller since it IS the only process either way) — a worker's config
# carries that attribute, the others don't. So the controller starts the ONE
# container in its own pytest_configure and stops it in pytest_unconfigure; a
# worker's pytest_configure sees workerinput and does nothing.
#
# HANDOFF VIA workerinput, NOT A SHARED TEMP FILE: `pytest_configure_node(node)` is
# xdist's own controller-side hook, fired once per worker right before that worker
# starts, specifically for handing controller-computed data down
# (`node.workerinput[...] = ...`) — the documented channel for exactly this, no
# polling and no lock file needed.
#
# NO RYUK-DISABLE NEEDED: testcontainers' default reaper ties a container's cleanup
# to the process that started it staying connected; that process here is the
# controller, which by construction stays alive for the whole run (xdist's
# controller doesn't finish until every worker has finished and reported back) — the
# same lifetime invariant that already held before this change, now spanning N
# workers instead of one process's own tests.
_CONTAINER: dict[str, PostgresContainer] = {}


# THE LIVE-DB GUARD (thread 9b9ba394): agent:repro-test-same/agent:repro-test-0 were a
# reproduction harness that reached the LIVE fleet graph instead of an isolated fixture
# DB — the twin of dev_env.refuse_silent_live_db (e4a3f3c) one layer over: that guard
# stops a bare SCRIPT invocation from silently defaulting to the live DSN; this one stops
# a pytest PROCESS from ever opening one at all, no matter how deep the call — a fixture,
# a script imported and driven by a repro test, a CLI/orchestrator function called
# directly. Same law (never infer from cwd, key on the DSN itself): the only DSNs this
# process may ever open are THIS session's own testcontainer's host:port, learned once at
# container start and never trusted from an env var or a caller's own claim. Patches BOTH
# asyncpg entrypoints (create_pool — src.db.pool.create_pool's own primitive, and every
# scripts/*.py direct caller; connect — the bare-connection callers like
# osiris_stophook.py/osiris_fleet_glance.py) so nothing in this process can route around
# it by skipping src.db.pool. Installed once per process (the controller's own copy is
# inert dead weight under xdist — a controller never runs a test — but harmless to
# install there too; the worker's own copy, in its own process, is the one that matters).
def _dsn_host_port(dsn: str | None, host: object = None, port: object = None) -> tuple[str, str]:
    """Extract (host, port) the same way asyncpg itself would resolve a connection
    target — either a `dsn` string (the common case, every call site in this repo) or
    bare `host=`/`port=` kwargs (asyncpg.connect's own alternate calling form, unused
    today but a guard keyed only on `dsn` would silently miss it)."""
    from urllib.parse import urlsplit

    if dsn:
        parsed = urlsplit(dsn)
        return str(parsed.hostname), str(parsed.port)
    return str(host), str(port)


# THE ONE NAMED EXEMPTION: a test that deliberately dials a target NOTHING listens on
# (test_manager's "postgres is unreachable" probe dials 127.0.0.1:1) is not opening a live
# DB — but the guard cannot tell "dead port" from "live DSN" without a heuristic, and a
# heuristic is exactly what 9b9ba394 forbids. So the exemption is an EXPLICIT ACT at the
# call site: `with unreachable_dsn_allowed(): ...` — visible in the test, greppable, and
# scoped to that block only. Never a global flag, never an env var.
_FOREIGN_DSN_ALLOWED: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "osiris_test_foreign_dsn_allowed", default=False)


@contextlib.contextmanager
def unreachable_dsn_allowed() -> Iterator[None]:
    """Let THIS block open a non-testcontainer DSN — for tests that prove failure paths
    against a target nothing listens on. The name says what it is for; using it to reach
    a real database is the defect the guard exists to catch."""
    token = _FOREIGN_DSN_ALLOWED.set(True)
    try:
        yield
    finally:
        _FOREIGN_DSN_ALLOWED.reset(token)


def _install_live_db_guard(host: str, port: str) -> None:
    import asyncpg

    allowed = (host, str(port))
    real_create_pool = asyncpg.create_pool
    real_connect = asyncpg.connect

    def _refuse(kind: str, got: tuple[str, str]) -> None:
        raise RuntimeError(
            f"LIVE-DB GUARD (thread 9b9ba394): a pytest process tried {kind} against "
            f"{got[0]}:{got[1]}, not this session's own testcontainer ({allowed[0]}:"
            f"{allowed[1]}). Refused before any network attempt. Route through the "
            "`actions`/`pg_dsn` fixtures — never a hardcoded or env-sourced DSN inside "
            "a test, fixture, or a script called from one."
        )

    async def _guarded_create_pool(dsn: str | None = None, **kwargs: object) -> object:
        got = _dsn_host_port(dsn, kwargs.get("host"), kwargs.get("port"))
        if got != allowed and not _FOREIGN_DSN_ALLOWED.get():
            _refuse("asyncpg.create_pool", got)
        return await real_create_pool(dsn, **kwargs)  # type: ignore[arg-type]

    async def _guarded_connect(dsn: str | None = None, **kwargs: object) -> object:
        got = _dsn_host_port(dsn, kwargs.get("host"), kwargs.get("port"))
        if got != allowed and not _FOREIGN_DSN_ALLOWED.get():
            _refuse("asyncpg.connect", got)
        return await real_connect(dsn, **kwargs)  # type: ignore[arg-type]

    asyncpg.create_pool = _guarded_create_pool  # type: ignore[assignment]
    asyncpg.connect = _guarded_connect  # type: ignore[assignment]


def _install_tool_contract_ceiling_merge_driver() -> None:
    """Self-installs the merge driver for TOOL_CONTRACT_CEILING_CHARS (dispatch 26686b77,
    Thoth msg 3658) into this repo's SHARED git config — `.gitattributes` alone names the
    driver, but a custom driver's own COMMAND is local config only (`git config
    merge.<name>.driver`), never version-controlled, so `.gitattributes` by itself is
    silent machinery a stranger would never find. Worktrees share ONE `.git/config`
    (confirmed: `git rev-parse --git-common-dir` is identical across every seat's own
    worktree here), so registering it from any ONE worktree's first pytest run arms it
    fleet-wide — exactly the discoverability gap a bare merge driver would otherwise have.

    The registered command is deliberately WORKTREE-AGNOSTIC (no absolute path to this
    checkout, no `sys.executable`): it `cd`s to `$(git rev-parse --show-toplevel)` first,
    then runs a bare `python3` (stdlib-only script, no venv needed) against the
    repo-relative script path — correct no matter which worktree's merge invokes it,
    including one that didn't exist yet when this was registered.

    Idempotent (only writes when the registered value differs) and never fails the test
    run: a git-config write failure (e.g. read-only `.git`) degrades to a printed warning,
    not a collection error — this is a merge-time safety net, not a test-time one, so its
    own absence must never block the gate it doesn't touch.
    """
    import shlex
    import subprocess

    inner = (
        'cd "$(git rev-parse --show-toplevel)" && '
        'exec python3 scripts/reconcile_tool_contract_ceiling.py "$1" "$2" "$3" "$4"'
    )
    driver_cmd = f"sh -c {shlex.quote(inner)} -- %O %A %B %P"
    try:
        current = subprocess.run(
            ["git", "config", "--local", "--get", "merge.tool_contract_ceiling.driver"],
            capture_output=True, text=True,
        ).stdout.strip()
        if current != driver_cmd:
            subprocess.run(
                ["git", "config", "--local", "merge.tool_contract_ceiling.driver", driver_cmd],
                check=True, capture_output=True,
            )
    except Exception as exc:  # pragma: no cover - defensive, see docstring
        print(f"tool-contract-ceiling merge driver NOT installed: {exc}", file=sys.stderr)


# AF_UNIX SOCKET PATH LENGTH (Sekhmet's find, msg 2261, task #119's gate): pytest's own
# default basetemp is "/tmp/pytest-of-<user>/pytest-<N>/", N an EVER-GROWING counter
# shared fleet-wide across every pytest invocation on this box (already past 270 the
# night this was found, from this house's own overnight test-running alone) — plus, under
# `-n auto`, xdist's own "popen-gwN/" worker segment, plus a long test function name (a
# tests/test_pty_broker.py socket test's tmp_path directory alone is its function name)
# comfortably exceeds AF_UNIX's ~108-byte sun_path limit on Linux. Confirmed live: 121
# bytes for that file's two longest-named socket tests under -n auto; 111 even under BARE
# pytest once the counter climbs high enough — this was never xdist-exclusive, only newly
# EXPOSED by the extra worker segment tipping an already-fragile path over TODAY's counter
# value (a length bug waiting to recur in bare pytest too, on a long-lived box, xdist or
# not). A short, PID-keyed basetemp — not pytest's own incrementing counter, which only
# grows and is shared fleet-wide, and unrelated to _CONTAINER's per-run Postgres — buys
# back the byte budget without colliding with any other concurrent pytest invocation on
# this box (a PID is unique to the OS process that holds it, controller or worker alike,
# no coordination needed). Only applied when the caller hasn't already passed --basetemp
# themselves (CI or a human override always wins).
def pytest_configure(config: pytest.Config) -> None:
    if config.option.basetemp is None:
        config.option.basetemp = f"/tmp/pt-{os.getpid()}"
    if hasattr(config, "workerinput"):
        wi = config.workerinput  # type: ignore[attr-defined]
        _install_live_db_guard(str(wi["pg_host"]), str(wi["pg_port"]))
        return  # an xdist worker: the controller (or, outside xdist, this same
        # process, since it then takes this same branch itself) owns the container
    _install_tool_contract_ceiling_merge_driver()
    pg = PostgresContainer("postgres:16", username="test", password="test", dbname="test")
    pg.start()
    _CONTAINER["pg"] = pg
    _install_live_db_guard(pg.get_container_host_ip(), str(pg.get_exposed_port(5432)))


def pytest_configure_node(node: pytest.Item) -> None:  # xdist controller-only hook
    pg = _CONTAINER["pg"]
    node.workerinput["pg_host"] = pg.get_container_host_ip()  # type: ignore[attr-defined]
    node.workerinput["pg_port"] = pg.get_exposed_port(5432)  # type: ignore[attr-defined]


def pytest_unconfigure(config: pytest.Config) -> None:
    pg = _CONTAINER.pop("pg", None)
    if pg is not None:
        pg.stop()


@pytest.fixture(scope="session")
def pg_dsn(request: pytest.FixtureRequest, worker_id: str) -> Iterator[str]:
    """The shared container's DSN for THIS worker's own database. `worker_id`
    ("master" outside xdist, "gw0"/"gw1"/... under it) is pytest-xdist's own
    fixture. Migrated to head once per worker process (session-scoped: once per
    worker under xdist, once total otherwise) since each worker's database starts
    empty. The Type catalog (task #97) is seeded lazily by the `actions` fixture
    below (a cheap existence check, real seed only on the first test that needs it)
    rather than here — a session-scoped ASYNC fixture proved unreliable under
    pytest-asyncio's per-function event loop default (see `actions`'s own
    docstring); unaffected by this change, it already ran per-database, not
    per-container."""
    workerinput = getattr(request.config, "workerinput", None)
    if workerinput is not None:
        host, port = workerinput["pg_host"], workerinput["pg_port"]
    else:
        pg = _CONTAINER["pg"]
        host, port = pg.get_container_host_ip(), pg.get_exposed_port(5432)

    # "test" (the container's own default db) IS worker "master"'s database — matches
    # this fixture's exact pre-xdist behavior 1:1 when nobody passes -n. Only actual
    # xdist workers get a freshly CREATEd database.
    db_name = "test" if worker_id == "master" else f"test_{worker_id}"
    admin_dsn = f"postgresql://test:test@{host}:{port}/test"
    if db_name != "test":
        # no existence check needed: db_name is unique per (fresh, ephemeral)
        # container per worker, and this fixture body runs at most once per worker
        # process (session scope) — nothing else can have created it first.
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))

    dsn = f"postgresql://test:test@{host}:{port}/{db_name}"
    os.environ["DATABASE_URL"] = dsn  # env.py reads this and converts to a sync psycopg URL
    command.upgrade(Config("alembic.ini"), "head")
    yield dsn


@pytest.fixture(autouse=True)
def _no_inherited_git_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """No test inherits git's per-invocation GIT_* variables. Defence in depth.

    THE INCIDENT (2026-08-27, ruling 06029cbf): git exports GIT_DIR and GIT_INDEX_FILE,
    both ABSOLUTE, into every hook it runs FROM A LINKED WORKTREE. The pre-commit gate runs
    in worktrees and spawned pytest with the ambient environment. GIT_DIR overrides
    repository discovery for the whole subprocess tree, and `git -C <dir>` does NOT rescope
    it -- `-C` chdirs, nothing more. So every fixture in this suite that builds its own
    throwaway repo, all of them correctly `-C`-scoped, was writing into the fleet's SHARED
    repository: identity overwritten to test/test@test, HEADs repointed to a fabricated
    orphan branch, core.bare set on the main checkout, and both installed hooks replaced
    with stubs -- which disarmed the gate and push_guard fleet-wide, silently.

    scripts/gate_hook.py:_pytest_env() is the choke point and closes that path. THIS is the
    belt to its braces, and it covers what the choke point cannot: the suite run any OTHER
    way under a poisoned environment -- a bare `pytest` from a shell that has GIT_DIR set,
    CI wired differently, a future hook that shells out to tests. Every one of those was
    hypothetical the day before the incident, and so was the incident.

    Deleted, never blanked: an empty GIT_DIR is not "unset", it is a git directory whose
    path is the empty string, which fails differently and just as wrongly. monkeypatch
    restores the real environment at teardown, so a test that deliberately sets GIT_* for
    its own subprocess is unaffected -- this only removes what was INHERITED."""
    for key in [k for k in os.environ if k.startswith("GIT_")]:
        monkeypatch.delenv(key, raising=False)


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
    async with pool.acquire() as conn, conn.transaction():
        # THE CATALOG SURVIVES THE RESET (task #97): every table in _RESET_TABLES is
        # cleared FIRST, in its own dependency order (see that tuple's comment) —
        # rooms/cases/objects/assertions are excluded, same as before task #100's
        # TRUNCATE -> DELETE change (see its own comment for the two-hop CASCADE leak
        # this still avoids: rooms -> cases -> assertions). Wrapped in one explicit
        # transaction (conn.transaction(), new under task #100) so 31 sequential
        # DELETEs commit atomically together — a mid-sequence failure now rolls back
        # everything instead of leaving a partially-reset database for the next test,
        # which a single TRUNCATE statement never risked but never needed either.
        for table in _RESET_TABLES:
            await conn.execute(f"DELETE FROM {table}")
        # objects/assertions: spare exactly the Type rows the catalog check below
        # seeds. assertions.id is bigserial and this scoped delete does NOT reset it
        # (DELETE never resets a sequence, unlike the old TRUNCATE ... RESTART
        # IDENTITY) — checked before landing this: no test asserts a literal
        # assertions.id value (grepped tests/ + src/ for one; every read is by a
        # dynamically-fetched id, never a hardcoded literal).
        await conn.execute(
            "DELETE FROM assertions a USING objects o "
            "WHERE a.object_id = o.id AND o.type <> 'Type'")
        await conn.execute("DELETE FROM objects WHERE type <> 'Type'")
        # cases and rooms LAST, after the two deletes above — by now nothing in
        # `assertions` still holds a case_id (Type rows never carry one; every other
        # row is already gone), so plain DELETEs satisfy every FK with zero
        # referencing rows left to violate.
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
