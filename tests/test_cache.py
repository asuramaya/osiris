from __future__ import annotations

import uuid

from src.actions.core import Actions
from src.orchestrator.cache import cached_fetch
from src.parsers.base import InputObject


async def test_cached_fetch_hits_source_once(actions: Actions, case_id: str) -> None:
    calls = {"n": 0}

    async def connector(inp: InputObject) -> dict:
        calls["n"] += 1
        return {"domain": inp.canonical, "certs": [{"name_value": f"a.{inp.canonical}"}]}

    inp = InputObject(id=str(uuid.uuid4()), type="Domain", canonical="corp.kp")

    r1 = await cached_fetch(actions.pool, connector, "crtsh_subdomains", inp, cache_ttl=3600)
    r2 = await cached_fetch(actions.pool, connector, "crtsh_subdomains", inp, cache_ttl=3600)
    assert r1 == r2
    assert calls["n"] == 1  # second call served from the persistent cache

    # the cache row persists (survives restarts) and is keyed per (helper, object)
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM helper_cache WHERE helper_id='crtsh_subdomains' "
        "AND object_canonical='corp.kp'"
    ) == 1


async def test_cache_ttl_zero_always_fetches(actions: Actions, case_id: str) -> None:
    calls = {"n": 0}

    async def connector(inp: InputObject) -> dict:
        calls["n"] += 1
        return {"v": calls["n"]}

    inp = InputObject(id=str(uuid.uuid4()), type="Domain", canonical="x.kp")
    await cached_fetch(actions.pool, connector, "h", inp, cache_ttl=0)
    await cached_fetch(actions.pool, connector, "h", inp, cache_ttl=0)
    assert calls["n"] == 2  # ttl=0 (e.g. windowed helpers) bypasses the cache
