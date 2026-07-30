"""resource_lease — task #103/#119. The bar this module exists to clear (Thoth's brief,
msg 2252): prove the ATOMICITY, not just the happy path. A test that only checks "the second
caller sees the holder" would pass on the broken (read-then-write) version too — these tests
fire real concurrent claims at the real Postgres constraint and assert exactly one winner,
the same style test_concurrency.py already uses for the kernel's own ON-CONFLICT claims.
"""
from __future__ import annotations

import asyncio

from src.actions.core import Actions
from src.orchestrator import resource_lease as rl


async def test_acquire_succeeds_when_free(actions: Actions) -> None:
    result = await rl.acquire(actions, "src/mcp_server.py", "agent:sekhmet")
    assert result.acquired is True
    assert result.holder == "agent:sekhmet"
    assert result.resource_id == "src/mcp_server.py"


async def test_second_claim_on_a_held_resource_is_refused_and_surfaced(
    actions: Actions,
) -> None:
    first = await rl.acquire(actions, "docker-daemon", "agent:khnum")
    second = await rl.acquire(actions, "docker-daemon", "agent:seshat")
    assert first.acquired is True
    assert second.acquired is False
    # visible-conflict UX (Alfred's #4.3): the SECOND caller learns who actually holds it,
    # never a silent duplicate mint.
    assert second.holder == "agent:khnum"
    # same resource -> same companion Thread, regardless of who's asking
    assert second.thread_id == first.thread_id


async def test_release_then_reacquire_by_a_different_holder_succeeds(
    actions: Actions,
) -> None:
    first = await rl.acquire(actions, "compose-merge", "agent:imhotep")
    released = await rl.release(actions.pool, "compose-merge", "agent:imhotep")
    assert released is True
    second = await rl.acquire(actions, "compose-merge", "agent:thoth")
    assert second.acquired is True
    assert second.holder == "agent:thoth"
    assert second.thread_id == first.thread_id  # same ledger, new holder


async def test_release_by_a_non_holder_is_refused(actions: Actions) -> None:
    await rl.acquire(actions, "src/orchestrator/capture.py", "agent:seshat")
    refused = await rl.release(actions.pool, "src/orchestrator/capture.py", "agent:khnum")
    assert refused is False
    # the real holder still holds it — a wrong-holder release call is a no-op, not a bug
    holder = await rl.current_holder(actions.pool, "src/orchestrator/capture.py")
    assert holder is not None and holder["holder"] == "agent:seshat"


async def test_release_of_an_unheld_resource_is_refused(actions: Actions) -> None:
    assert await rl.release(actions.pool, "never-claimed", "agent:anyone") is False


async def test_current_holder_is_none_when_free(actions: Actions) -> None:
    assert await rl.current_holder(actions.pool, "nothing-here") is None


async def test_distinct_resource_ids_get_distinct_threads(actions: Actions) -> None:
    a = await rl.acquire(actions, "resource-a", "agent:x")
    b = await rl.acquire(actions, "resource-b", "agent:x")
    assert a.thread_id != b.thread_id


async def test_the_same_resource_id_always_resolves_to_the_same_thread(
    actions: Actions,
) -> None:
    """The whole point of Q7's fix: EXACT-key lookup, not fuzzy summary similarity. Acquire,
    release, and reacquire the same resource_id three times over — every call must resolve
    to the identical companion Thread, never a near-duplicate mint."""
    first = await rl.acquire(actions, "src/api/chrome.py", "agent:a")
    await rl.release(actions.pool, "src/api/chrome.py", "agent:a")
    second = await rl.acquire(actions, "src/api/chrome.py", "agent:b")
    await rl.release(actions.pool, "src/api/chrome.py", "agent:b")
    third = await rl.acquire(actions, "src/api/chrome.py", "agent:c")
    assert first.thread_id == second.thread_id == third.thread_id


async def test_reap_stale_releases_an_old_held_lease(actions: Actions) -> None:
    await rl.acquire(actions, "abandoned-resource", "agent:crashed")
    # not stale yet under a generous window
    assert await rl.reap_stale(actions.pool, older_than_secs=3600) == 0
    # a 0-second window makes every held lease immediately "stale" for the test's purposes
    reaped = await rl.reap_stale(actions.pool, older_than_secs=0)
    assert reaped == 1
    assert await rl.current_holder(actions.pool, "abandoned-resource") is None


async def test_reaping_frees_the_resource_for_a_fresh_claim(actions: Actions) -> None:
    await rl.acquire(actions, "wedged", "agent:crashed")
    await rl.reap_stale(actions.pool, older_than_secs=0)
    fresh = await rl.acquire(actions, "wedged", "agent:survivor")
    assert fresh.acquired is True
    assert fresh.holder == "agent:survivor"


async def test_empty_resource_id_or_holder_refuses_loudly(actions: Actions) -> None:
    try:
        await rl.acquire(actions, "", "agent:x")
        raise AssertionError("expected ValueError on empty resource_id")
    except ValueError:
        pass
    try:
        await rl.acquire(actions, "some-resource", "  ")
        raise AssertionError("expected ValueError on empty holder")
    except ValueError:
        pass


# --- the bar: prove atomicity against the real constraint, not the happy path ------------

async def test_fifty_concurrent_claims_on_one_resource_produce_exactly_one_holder(
    actions: Actions,
) -> None:
    """The load-bearing test. 50 agents race to acquire the SAME resource_id at once via
    asyncio.gather (real concurrent connections against the real Postgres testcontainer,
    same style as test_concurrency.py's racing-creators proof). Exactly one must win — not
    asserted from the Python-side result list alone, but cross-checked against the table
    the partial unique index actually governs, so a bug that let the APPLICATION layer
    believe the wrong thing while the DB disagreed would still be caught."""
    results = await asyncio.gather(*[
        rl.acquire(actions, "the-one-true-resource", f"agent:{i:03d}") for i in range(50)
    ])
    winners = [r for r in results if r.acquired]
    losers = [r for r in results if not r.acquired]
    assert len(winners) == 1
    assert len(losers) == 49
    # every loser must report the SAME actual winner — no split-brain, no stale reads
    assert {r.holder for r in losers} == {winners[0].holder}
    # cross-checked directly against the table the constraint actually governs
    held_rows = await actions.pool.fetch(
        "SELECT holder FROM resource_leases WHERE resource_id='the-one-true-resource' "
        "AND status='held'")
    assert len(held_rows) == 1
    assert held_rows[0]["holder"] == winners[0].holder


async def test_concurrent_claims_on_distinct_resources_all_succeed(
    actions: Actions,
) -> None:
    """The negative control for the test above: concurrency itself isn't the thing being
    refused — only a genuine collision on the SAME resource_id is. 30 agents claiming 30
    DIFFERENT resources at once must all win; nothing here should serialize unrelated
    claims against each other."""
    results = await asyncio.gather(*[
        rl.acquire(actions, f"distinct-resource-{i}", f"agent:{i:03d}") for i in range(30)
    ])
    assert all(r.acquired for r in results)
    assert len({r.thread_id for r in results}) == 30


async def test_release_then_fifty_concurrent_reclaims_still_produce_exactly_one_holder(
    actions: Actions,
) -> None:
    """A released resource is genuinely re-contestable — the partial unique index's WHERE
    status='held' clause proven under the same race, not just asserted sequentially."""
    first = await rl.acquire(actions, "recontested-resource", "agent:seed")
    await rl.release(actions.pool, "recontested-resource", "agent:seed")
    results = await asyncio.gather(*[
        rl.acquire(actions, "recontested-resource", f"agent:{i:03d}") for i in range(50)
    ])
    winners = [r for r in results if r.acquired]
    assert len(winners) == 1
    assert winners[0].thread_id == first.thread_id  # same ledger, re-contested cleanly


# --- the MCP tool wrappers (task #103's Q7 fix, exposed to the fleet) --------------------
# Same pattern test_doors.py's test_the_mcp_tool_wrapper_delegates_to_doors uses: the
# @mcp.tool()-decorated functions are plain awaitable coroutines, calling module-global
# _pool_get() internally — swap srv._pool to the test's own actions.pool, restore after.

async def test_acquire_lease_wrapper_claims_and_defaults_holder_to_the_caller(
    actions: Actions,
) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.acquire_lease("src/api/chrome.py")
    finally:
        srv._pool = saved_pool
    assert out["acquired"] is True
    assert out["resource_id"] == "src/api/chrome.py"
    assert out["holder"]  # defaulted to the (unmounted-test) caller identity, non-empty
    assert "held_since" in out and "thread_id" in out


async def test_acquire_lease_wrapper_refusal_names_holder_and_since(
    actions: Actions,
) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        first = await srv.acquire_lease("docker-daemon", holder="agent:khnum")
        second = await srv.acquire_lease("docker-daemon", holder="agent:seshat")
    finally:
        srv._pool = saved_pool
    assert first["acquired"] is True
    assert second["acquired"] is False
    assert second["holder"] == "agent:khnum"
    assert second["held_since"] == first["held_since"]
    assert "agent:khnum" in second["note"] and second["held_since"] in second["note"]


async def test_release_lease_wrapper_only_the_holder_can_release(actions: Actions) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        await srv.acquire_lease("compose-merge", holder="agent:imhotep")
        wrong = await srv.release_lease("compose-merge", holder="agent:thoth")
        right = await srv.release_lease("compose-merge", holder="agent:imhotep")
    finally:
        srv._pool = saved_pool
    assert wrong["released"] is False
    assert right["released"] is True


async def test_check_lease_wrapper_is_read_only_and_never_mints(actions: Actions) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        free = await srv.check_lease("never-touched")
        await srv.acquire_lease("src/orchestrator/capture.py", holder="agent:seshat")
        held = await srv.check_lease("src/orchestrator/capture.py")
    finally:
        srv._pool = saved_pool
    assert free == {"resource_id": "never-touched", "held": False}
    assert held["held"] is True and held["holder"] == "agent:seshat"
    # a pure read never claims anything of its own — re-checking a free resource twice
    # must never itself become a holder
    assert await rl.current_holder(actions.pool, "never-touched") is None


async def test_reap_stale_leases_wrapper_recovers_an_abandoned_claim(
    actions: Actions,
) -> None:
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        await srv.acquire_lease("wedged-by-a-crash", holder="agent:crashed")
        out = await srv.reap_stale_leases(older_than_secs=0)
    finally:
        srv._pool = saved_pool
    assert out == {"reaped": 1, "older_than_secs": 0}
    assert await rl.current_holder(actions.pool, "wedged-by-a-crash") is None


async def test_reap_leases_is_scheduled_as_a_cron_job() -> None:
    """The backstop rides a cron every 5 minutes, same shape as reap_stale_runs' own
    wiring for helper_runs (test_sweep_ledger.py's test_reap_stuck_sweeps_is_scheduled is
    the exact precedent this mirrors) — explicit release_lease stays the norm; this just
    proves the backstop isn't a dangling function nobody ever calls."""
    from src.workers.arq_worker import WorkerSettings

    crons = {c.coroutine.__name__ for c in WorkerSettings.cron_jobs}
    assert "reap_leases" in crons
