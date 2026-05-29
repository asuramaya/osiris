from __future__ import annotations

import uuid

import redis.asyncio as aioredis
from src.actions.core import Actions
from src.orchestrator.budgets import BudgetLedger
from src.orchestrator.cascade import CascadeContext, dispatch, run_cascade
from src.orchestrator.manifests import Manifest, Rate
from src.orchestrator.ratelimit import RateLimiter
from src.parsers.base import InputObject

# crt.sh manifest with rate limiting effectively disabled, so these tests isolate
# the cascade/budget logic from throttling (the token bucket is tested separately).
_CRT = Manifest.model_validate(
    {
        "id": "crtsh_subdomains",
        "name": "crt.sh",
        "consumes": {"type": "Domain"},
        "parser": "crtsh_subdomains",
        "origin": "crt.sh",
        "tier": "open",
        "rate": {"per_origin_rps": 1e9, "per_origin_concurrent": 1e9},
    }
).model_copy(update={"rate": Rate(per_origin_rps=1e9, per_origin_concurrent=1e9)})


async def _fake_crtsh(input_object: InputObject) -> dict:
    """Each domain yields exactly one child a.<domain> — unbounded without a hop cap."""
    d = input_object.canonical
    return {"domain": d, "certs": [{"name_value": f"a.{d}"}]}


def _ctx(actions: Actions, redis: aioredis.Redis) -> CascadeContext:
    return CascadeContext(
        actions=actions,
        limiter=RateLimiter(redis),
        ledger=BudgetLedger(actions.pool, redis),
        manifests={"crtsh_subdomains": _CRT},
        connectors={"crtsh_subdomains": _fake_crtsh},
    )


async def _make_case(actions: Actions, budgets: dict) -> uuid.UUID:
    cid = await actions.pool.fetchval(
        "INSERT INTO cases (name, owner, budgets) VALUES ('c','analyst:test',$1) RETURNING id",
        budgets,
    )
    # triggers must be projected for the relay to match helpers
    await actions.pool.execute(
        "INSERT INTO triggers (on_event, match, helper_id, enabled) "
        "VALUES ('object_created', $1, 'crtsh_subdomains', true)",
        {"type": "Domain"},
    )
    return uuid.UUID(str(cid))


async def test_cascade_fires_expands_and_terminates_on_hop_budget(
    actions: Actions, redis_client: aioredis.Redis
) -> None:
    case_id = await _make_case(
        actions, {"max_hop_distance": 1, "rate_credits": 100, "max_helpers_per_object": 9}
    )
    await actions.create_or_find_object("Domain", "evil.kp", "analyst:test", case_id)

    processed = await run_cascade(_ctx(actions, redis_client))
    assert processed > 0

    # the unbounded self-similar feed would loop forever; the hop budget stops it.
    # Runs happen on hop0 (evil.kp) and hop1 (a.evil.kp); hop2 (a.a.evil.kp) is blocked.
    done = await actions.pool.fetchval(
        "SELECT count(*) FROM helper_runs WHERE helper_id='crtsh_subdomains' AND status='done'"
    )
    assert done == 2

    # objects exist out to hop2, but the hop2 object never ran a helper
    domains = {
        r["canonical"]: r["hop_distance"]
        for r in await actions.pool.fetch(
            "SELECT o.canonical, co.hop_distance FROM objects o "
            "JOIN case_objects co ON co.object_id=o.id WHERE o.type='Domain'"
        )
    }
    assert domains == {"evil.kp": 0, "a.evil.kp": 1, "a.a.evil.kp": 2}

    blocked_obj = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='a.a.evil.kp'"
    )
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM helper_runs WHERE object_id=$1", blocked_obj
    ) == 0

    # has_subdomain edges link each parent to its child
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE type='has_subdomain'"
    ) == 2
    # two server-worker runs consumed two rate credits
    assert int(await redis_client.get(f"budget:{case_id}:rate")) == 98

    # re-draining is a no-op (all outbox rows published)
    assert await run_cascade(_ctx(actions, redis_client)) == 0


async def test_flaky_connector_fails_gracefully(
    actions: Actions, redis_client: aioredis.Redis
) -> None:
    """A connector that errors (timeout, 5xx) marks the run failed and lets the
    cascade continue — it must not 500 the whole expansion."""
    case_id = await _make_case(actions, {"rate_credits": 100, "max_hop_distance": 1})
    await actions.create_or_find_object("Domain", "flaky.kp", "analyst:test", case_id)

    async def boom(input_object: InputObject) -> dict:
        raise TimeoutError("source timed out")

    ctx = CascadeContext(
        actions=actions, limiter=RateLimiter(redis_client),
        ledger=BudgetLedger(actions.pool, redis_client),
        manifests={"crtsh_subdomains": _CRT}, connectors={"crtsh_subdomains": boom},
    )
    processed = await run_cascade(ctx)  # does not raise
    assert processed > 0
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM helper_runs WHERE status='failed'"
    ) == 1
    # the failed run released its claim and refunded its credit
    assert int(await redis_client.get(f"budget:{case_id}:rate")) == 100


async def test_cross_case_cached_result_rematerializes_into_new_case(
    actions: Actions, redis_client: aioredis.Redis
) -> None:
    """A helper that ran in case A is router-CACHED when the same object appears in
    case B. Instead of silently skipping (leaving B empty), the cascade re-links the
    cached result into B — without touching the network (cache hit)."""
    case_a = await _make_case(actions, {"max_hop_distance": 0, "rate_credits": 100})
    obj = await actions.create_or_find_object("Domain", "shared.kp", "analyst:test", case_a)

    assert await dispatch(_ctx(actions, redis_client), _CRT, obj, case_a) == "ran"
    # case A now has a done run and a global cache row for (crtsh, shared.kp)
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM helper_runs WHERE object_id=$1 AND case_id=$2 AND status='done'",
        obj, case_a,
    ) == 1

    # case B: same object, fresh case. A connector that raises proves the re-materialize
    # path reads the cache and never egresses.
    case_b = await _make_case(actions, {"max_hop_distance": 0, "rate_credits": 100})

    async def no_network(input_object: InputObject) -> dict:
        raise AssertionError("network egress on a cache hit")

    ctx_b = CascadeContext(
        actions=actions, limiter=RateLimiter(redis_client),
        ledger=BudgetLedger(actions.pool, redis_client),
        manifests={"crtsh_subdomains": _CRT}, connectors={"crtsh_subdomains": no_network},
    )
    assert await dispatch(ctx_b, _CRT, obj, case_b) == "rematerialized"

    # the child materialized INTO case B (not just case A)
    child = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='a.shared.kp'")
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM case_objects WHERE case_id=$1 AND object_id=$2", case_b, child
    ) == 1
    # a done run now exists for case B, and a re-materialize spends no rate credit
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM helper_runs WHERE object_id=$1 AND case_id=$2 AND status='done'",
        obj, case_b,
    ) == 1
    assert await redis_client.get(f"budget:{case_b}:rate") is None  # never seeded/decremented

    # a genuine same-case repeat is now a plain CACHED skip
    assert await dispatch(ctx_b, _CRT, obj, case_b) == "cached"


async def test_cascade_halts_when_rate_credits_exhausted(
    actions: Actions, redis_client: aioredis.Redis
) -> None:
    case_id = await _make_case(
        actions, {"max_hop_distance": 9, "rate_credits": 1, "max_helpers_per_object": 9}
    )
    await actions.create_or_find_object("Domain", "seed.kp", "analyst:test", case_id)

    await run_cascade(_ctx(actions, redis_client))

    # only one helper run got a credit; the cascade halted despite hop budget remaining
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM helper_runs WHERE status='done'"
    ) == 1
    # the seed's run still created its child object (which then couldn't be processed)
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE canonical='a.seed.kp'"
    ) == 1
    assert int(await redis_client.get(f"budget:{case_id}:rate")) == 0
