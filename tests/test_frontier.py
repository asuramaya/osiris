"""Anchor-and-pivot frontier policy (real Postgres via the `actions` fixture)."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

import redis.asyncio as aioredis
from src.actions.core import Actions
from src.orchestrator.budgets import BudgetLedger
from src.orchestrator.cascade import CascadeContext, fire_triggers
from src.orchestrator.frontier import Tier, is_expandable, tier_of
from src.orchestrator.manifests import Manifest, Rate
from src.orchestrator.ratelimit import RateLimiter
from src.parsers.base import EvidenceClass, InputObject
from src.parsers.evidence import confidence_for

NOW = datetime(2026, 6, 24, tzinfo=UTC)

_ACCT_HELPER = Manifest.model_validate(
    {
        "id": "fake_acct", "name": "fake", "consumes": {"type": "Account"},
        "parser": "github", "origin": "x", "tier": "open",
        "rate": {"per_origin_rps": 1e9, "per_origin_concurrent": 1e9},
    }
).model_copy(update={"rate": Rate(per_origin_rps=1e9, per_origin_concurrent=1e9)})


async def _noop_conn(input_object: InputObject) -> dict:
    return {"found": False}  # emits nothing; we only assert whether it RAN


async def _seed(actions: Actions, cid: uuid.UUID, type_: str, canon: str) -> uuid.UUID:
    return await actions.create_or_find_object(type_, canon, "analyst:test", cid)


async def _child(actions: Actions, cid: uuid.UUID, type_: str, canon: str) -> uuid.UUID:
    """An object created by a helper run (added_by_run set) — judged by its links."""
    oid = await actions.create_or_find_object(type_, canon, "helper", cid)
    await actions.pool.execute(
        "UPDATE case_objects SET added_by_run=$3 WHERE case_id=$1 AND object_id=$2",
        cid, oid, uuid.uuid4(),
    )
    return oid


async def _link(
    actions: Actions, cid: uuid.UUID, src: uuid.UUID, dst: uuid.UUID,
    type_: str, ec: EvidenceClass, source: str = "helper",
) -> None:
    await actions.create_link(
        src, dst, type_, source, NOW, confidence_for(ec), case_id=cid, evidence_class=ec.value,
    )


async def test_seed_is_always_an_anchor(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    seed = await _seed(actions, cid, "Username", "asuramaya")
    assert await tier_of(actions.pool, cid, seed) is Tier.ANCHOR
    assert await is_expandable(actions.pool, cid, seed)


async def test_self_declared_link_is_an_anchor(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    seed = await _seed(actions, cid, "Account", "github:asuramaya")
    acct = await _child(actions, cid, "Account", "soundcloud:wrenaudio7")
    await _link(actions, cid, seed, acct, "declares", EvidenceClass.SELF_DECLARED)
    assert await tier_of(actions.pool, cid, acct) is Tier.ANCHOR
    assert await is_expandable(actions.pool, cid, acct)


async def test_lone_co_occurrence_is_a_speculative_leaf(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    seed = await _seed(actions, cid, "Username", "asuramaya")
    # an enumerated same-handle account: real object, but only a co-occurrence link
    stranger = await _child(actions, cid, "Account", "soundcloud:asuramaya")
    await _link(actions, cid, seed, stranger, "has_account", EvidenceClass.CO_OCCURRENCE)
    assert await tier_of(actions.pool, cid, stranger) is Tier.SPECULATIVE
    assert not await is_expandable(actions.pool, cid, stranger)


async def test_derived_handle_is_a_speculative_leaf(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    seed = await _seed(actions, cid, "Email", "priya@kowalski.dev")
    guess = await _child(actions, cid, "Username", "hector")
    await _link(actions, cid, seed, guess, "derived_handle", EvidenceClass.DERIVED)
    assert not await is_expandable(actions.pool, cid, guess)


async def test_second_real_source_corroborates_and_promotes(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    seed = await _seed(actions, cid, "Username", "asuramaya")
    node = await _child(actions, cid, "Account", "soundcloud:wrenaudio7")
    # round 1: only a co-occurrence guess -> leaf, not crawled
    await _link(actions, cid, seed, node, "has_account", EvidenceClass.CO_OCCURRENCE, "enum")
    assert not await is_expandable(actions.pool, cid, node)
    # round 2: a second, non-speculative source (e.g. the README declares it) ->
    # the strongest inbound reason rises and the node becomes expandable
    await _link(actions, cid, seed, node, "declares", EvidenceClass.SELF_DECLARED, "github_deep")
    assert await tier_of(actions.pool, cid, node) is Tier.ANCHOR
    assert await is_expandable(actions.pool, cid, node)


async def test_subject_anchor_overrides_weak_links(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    seed = await _seed(actions, cid, "Username", "asuramaya")
    me = await _child(actions, cid, "Person", "subject:me")
    await _link(actions, cid, seed, me, "co_occurs", EvidenceClass.CO_OCCURRENCE)
    # without the anchor it would be a leaf...
    assert await tier_of(actions.pool, cid, me) is Tier.SPECULATIVE
    # ...marking it the subject makes it an anchor regardless of link strength
    await actions.tag_object(me, "subject", "case", "analyst:test", cid)
    assert await tier_of(actions.pool, cid, me) is Tier.ANCHOR


async def test_unclassified_link_still_expands(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    # a not-yet-ported (threat-intel) parser emits links with NULL evidence_class;
    # those must keep their prior expand-everything behaviour (treated as OBSERVED).
    seed = await _seed(actions, cid, "Malware", "applejeus")
    ind = await _child(actions, cid, "Indicator", "ind:abc")
    await actions.create_link(seed, ind, "indicates", "threatfox", NOW, 0.7, case_id=cid)
    assert await tier_of(actions.pool, cid, ind) is Tier.OBSERVED
    assert await is_expandable(actions.pool, cid, ind)


async def test_fire_triggers_crawls_anchor_skips_speculative_leaf(
    actions: Actions, case_id: str, redis_client: aioredis.Redis
) -> None:
    cid = uuid.UUID(case_id)
    await actions.pool.execute(
        "INSERT INTO triggers (on_event, match, helper_id, enabled) "
        "VALUES ('object_created', $1, 'fake_acct', true)",
        {"type": "Account"},
    )
    ctx = CascadeContext(
        actions=actions, limiter=RateLimiter(redis_client),
        ledger=BudgetLedger(actions.pool, redis_client),
        manifests={"fake_acct": _ACCT_HELPER}, connectors={"fake_acct": _noop_conn},
    )
    seed = await _seed(actions, cid, "Username", "asuramaya")
    spec = await _child(actions, cid, "Account", "soundcloud:asuramaya")
    await _link(actions, cid, seed, spec, "has_account", EvidenceClass.CO_OCCURRENCE)
    anchor = await _child(actions, cid, "Account", "soundcloud:wrenaudio7")
    await _link(actions, cid, seed, anchor, "declares", EvidenceClass.SELF_DECLARED)

    assert await fire_triggers(ctx, "object_created", spec, cid) == ["leaf:not_expandable"]
    assert await fire_triggers(ctx, "object_created", anchor, cid) == ["ran"]
    # the speculative leaf spawned no crawl; the anchor did
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM helper_runs WHERE object_id=$1", spec
    ) == 0
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM helper_runs WHERE object_id=$1", anchor
    ) == 1
