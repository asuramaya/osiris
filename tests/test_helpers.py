from __future__ import annotations

import asyncio
from pathlib import Path

import asyncpg
import pytest
from src.actions.core import Actions
from src.orchestrator.manifests import load_manifests, project_triggers
from src.orchestrator.triggers import matching_helpers

HELPERS_DIR = Path(__file__).parent.parent / "helpers"


def test_load_manifests_validates() -> None:
    manifests = load_manifests(HELPERS_DIR)
    assert "threatfox_malware_iocs" in manifests
    m = manifests["threatfox_malware_iocs"]
    assert m.consumes.type == "Malware"
    assert m.tier == "open"
    assert m.parser == "threatfox_malware_iocs"


async def test_project_triggers_from_manifests(actions: Actions) -> None:
    manifests = load_manifests(HELPERS_DIR)
    n = await project_triggers(actions.pool, manifests)
    assert n == len(manifests)
    row = await actions.pool.fetchrow(
        "SELECT on_event, match, enabled FROM triggers WHERE helper_id='threatfox_malware_iocs'"
    )
    assert row["on_event"] == "object_created"
    assert row["match"]["type"] == "Malware"
    assert row["enabled"] is True


async def test_project_triggers_preserves_enabled_flag(actions: Actions) -> None:
    manifests = load_manifests(HELPERS_DIR)
    await project_triggers(actions.pool, manifests)
    # analyst disables the trigger; re-projection must not re-enable it
    await actions.pool.execute(
        "UPDATE triggers SET enabled=false WHERE helper_id='threatfox_malware_iocs'"
    )
    await project_triggers(actions.pool, manifests)
    assert await actions.pool.fetchval(
        "SELECT enabled FROM triggers WHERE helper_id='threatfox_malware_iocs'"
    ) is False


async def test_project_triggers_no_longer_needs_access_exclusive(actions: Actions) -> None:
    """A live incident (Thoth mail 10214): the old TRUNCATE-based rebuild needed ACCESS
    EXCLUSIVE, which a concurrent pg_dump's own ACCESS SHARE lock on this table (pg_dump
    explicitly locks every table it dumps in ACCESS SHARE MODE, to hold DDL off during
    the dump) was enough to block, hanging console startup. DELETE + upsert-by-helper_id
    needs only ROW EXCLUSIVE — proved directly here by holding ACCESS SHARE open in a
    second connection while project_triggers runs concurrently; ROW EXCLUSIVE and ACCESS
    SHARE do not conflict, so this must complete without blocking."""
    manifests = load_manifests(HELPERS_DIR)
    conn = await actions.pool.acquire()
    tr = conn.transaction()
    await tr.start()
    try:
        await conn.execute("LOCK TABLE triggers IN ACCESS SHARE MODE")
        n = await asyncio.wait_for(project_triggers(actions.pool, manifests), timeout=30.0)
        assert n == len(manifests)
    finally:
        await tr.rollback()
        await actions.pool.release(conn)


async def test_project_triggers_raises_promptly_under_a_genuinely_conflicting_lock(
    actions: Actions,
) -> None:
    """Its own `SET LOCAL lock_timeout = '2s'` is the last line of defense for a lock
    class DELETE/upsert genuinely DOES conflict with (ACCESS EXCLUSIVE, held by some
    OTHER exclusive-lock holder this fix doesn't anticipate) — this must raise
    LockNotAvailableError within a few seconds, never hang. The caller (the lifespan
    startup path) is the one that turns this into "leave the previous projection in
    place" rather than crashing; this proves the primitive itself is bounded."""
    manifests = load_manifests(HELPERS_DIR)
    conn = await actions.pool.acquire()
    tr = conn.transaction()
    await tr.start()
    try:
        await conn.execute("LOCK TABLE triggers IN ACCESS EXCLUSIVE MODE")
        with pytest.raises(asyncpg.exceptions.LockNotAvailableError):
            await asyncio.wait_for(project_triggers(actions.pool, manifests), timeout=60.0)
    finally:
        await tr.rollback()
        await actions.pool.release(conn)


async def test_matching_helpers(actions: Actions) -> None:
    manifests = load_manifests(HELPERS_DIR)
    await project_triggers(actions.pool, manifests)
    # each helper matches its consumed type
    assert "threatfox_malware_iocs" in await matching_helpers(
        actions.pool, "object_created", "Malware"
    )
    assert "crtsh_subdomains" in await matching_helpers(
        actions.pool, "object_created", "Domain"
    )
    # a type no helper consumes matches nothing
    assert await matching_helpers(actions.pool, "object_created", "Vessel") == []
    # disabled triggers don't match
    await actions.pool.execute("UPDATE triggers SET enabled=false")
    assert await matching_helpers(actions.pool, "object_created", "Malware") == []
