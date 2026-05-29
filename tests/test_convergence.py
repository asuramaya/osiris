from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from src.actions.core import Actions
from src.ontology.resolution import find_footprint_merge_candidates, review_tray
from src.orchestrator.hygiene import converge_identities

NOW = datetime(2026, 5, 28, tzinfo=UTC)


async def _account(actions: Actions, cid: uuid.UUID, canonical: str, **props: object) -> uuid.UUID:
    oid = await actions.create_or_find_object("Account", canonical, "analyst:test", cid)
    for name, value in props.items():
        await actions.assert_property(oid, name, value, "analyst:test", NOW, 0.8, case_id=cid)
    return oid


async def test_shared_handle_queues_account_candidate(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    a = await _account(actions, cid, "github:jdoe", platform="github", handle="jdoe")
    b = await _account(actions, cid, "gitlab:jdoe", platform="gitlab", handle="jdoe")

    assert await find_footprint_merge_candidates(actions.pool) == 1
    tray = await review_tray(actions.pool)
    assert {tray[0]["a_id"], tray[0]["b_id"]} == {a, b}
    assert tray[0]["score"] == pytest.approx(0.6, abs=1e-5)
    # weak signal never auto-merges
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Account' AND status='active'"
    ) == 2


async def test_bio_email_forms_person_hub(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    acc = await _account(
        actions, cid, "github:asuramaya",
        platform="github", handle="asuramaya", email="priya@kowalski.dev",
    )
    email = await actions.create_or_find_object(
        "Email", "priya@kowalski.dev", "analyst:test", cid
    )

    res = await converge_identities(actions, case_id=cid)
    assert res["hubs"] >= 1

    hub = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE type='Person' AND canonical='cluster:priya@kowalski.dev'"
    )
    assert hub is not None
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND to_id=$2 AND type='has_account'", hub, acc
    ) == 1
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND to_id=$2 AND type='has_email'", hub, email
    ) == 1
    # hub joined the case so it shows in the case graph
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM case_objects WHERE case_id=$1 AND object_id=$2", cid, hub
    ) == 1
    # idempotent: re-running convergence does not duplicate the hub's links
    await converge_identities(actions, case_id=cid)
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='has_account'", hub
    ) == 1


async def test_rel_me_shared_source_converges(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    src = await actions.create_or_find_object("URL", "https://asuramaya.com", "analyst:test", cid)
    a = await _account(actions, cid, "github:asuramaya", platform="github", handle="asuramaya")
    b = await _account(actions, cid, "mastodon:asuramaya", platform="mastodon", handle="asuramaya")
    await actions.create_link(src, a, "rel_me", "analyst:test", NOW, 0.8, case_id=cid)
    await actions.create_link(src, b, "rel_me", "analyst:test", NOW, 0.8, case_id=cid)

    await converge_identities(actions, case_id=cid)

    # rel=me beats the shared-handle signal for the same pair (0.9 > 0.6)
    tray = await review_tray(actions.pool)
    cand = next(t for t in tray if {t["a_id"], t["b_id"]} == {a, b})
    assert cand["score"] == pytest.approx(0.9, abs=1e-5)
    assert "rel=me identity link" in cand["reasons"]["signals"]
    # the shared rel=me source page becomes a hub grouping both accounts
    hub = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='cluster:https://asuramaya.com'"
    )
    assert hub is not None
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='has_account'", hub
    ) == 2


async def test_reject_suppresses_footprint_candidate(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    await _account(actions, cid, "github:jdoe", platform="github", handle="jdoe")
    await _account(actions, cid, "gitlab:jdoe", platform="gitlab", handle="jdoe")
    assert await find_footprint_merge_candidates(actions.pool) == 1
    from src.ontology.resolution import resolve_candidate
    cand_id = (await review_tray(actions.pool))[0]["id"]
    await resolve_candidate(actions, cand_id, "rejected", "analyst:test")
    # rejected pair (now not_same_as) is never re-queued
    assert await find_footprint_merge_candidates(actions.pool) == 0
