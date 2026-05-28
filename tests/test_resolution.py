from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
from src.actions.core import Actions
from src.ontology.ingest import ingest_bundle
from src.ontology.resolution import (
    find_behavioral_merge_candidates,
    find_person_merge_candidates,
    resolve_candidate,
    review_tray,
)

NOW = datetime(2026, 5, 28, tzinfo=UTC)
DPRK_BUNDLE = Path(__file__).parent / "fixtures" / "dprk_attack_bundle.json"


async def _person(actions: Actions, cid: uuid.UUID, canonical: str, **props: object) -> uuid.UUID:
    oid = await actions.create_or_find_object("Person", canonical, "analyst:test", cid)
    for name, value in props.items():
        await actions.assert_property(oid, name, value, "analyst:test", NOW, 0.9, case_id=cid)
    return oid


async def test_person_candidate_on_shared_email(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    a = await _person(actions, cid, "p-a", name="Kim", email="kim@dprk.example")
    b = await _person(actions, cid, "p-b", name="Kim Variant", email="kim@dprk.example")

    assert await find_person_merge_candidates(actions.pool) == 1
    tray = await review_tray(actions.pool)
    assert len(tray) == 1
    assert {tray[0]["a_id"], tray[0]["b_id"]} == {a, b}
    assert "shared email" in tray[0]["reasons"]["signals"]
    # never auto-merged: both objects still active
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Person' AND status='active'"
    ) == 2


async def test_person_candidate_name_plus_dob(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    await _person(actions, cid, "q-a", name="Pak Jin", dob="1980-01-01")
    await _person(actions, cid, "q-b", name="Pak Jin", dob="1980-01-01")
    assert await find_person_merge_candidates(actions.pool) == 1
    assert (await review_tray(actions.pool))[0]["score"] == pytest.approx(0.85, abs=1e-5)


async def test_confirm_merges(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    a = await _person(actions, cid, "m-a", name="X", email="x@e.com")
    b = await _person(actions, cid, "m-b", name="X2", email="x@e.com")
    await find_person_merge_candidates(actions.pool)
    cand = (await review_tray(actions.pool))[0]["id"]

    await resolve_candidate(actions, cand, "merged", "analyst:test")
    # one side merged into the other (event-sourced)
    statuses = sorted(
        r["status"]
        for r in await actions.pool.fetch(
            "SELECT status FROM objects WHERE id = ANY($1::uuid[])", [a, b]
        )
    )
    assert statuses == ["active", "merged"]
    assert await review_tray(actions.pool) == []  # resolved, off the tray


async def test_reject_writes_not_same_as_and_suppresses(actions: Actions, case_id: str) -> None:
    cid = uuid.UUID(case_id)
    a = await _person(actions, cid, "r-a", name="Y", email="y@e.com")
    b = await _person(actions, cid, "r-b", name="Y2", email="y@e.com")
    await find_person_merge_candidates(actions.pool)
    cand = (await review_tray(actions.pool))[0]["id"]

    await resolve_candidate(actions, cand, "rejected", "analyst:test")
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE type='not_same_as' AND from_id=ANY($1::uuid[])", [a, b]
    ) == 2  # both directions
    # re-running ER does NOT re-propose the rejected pair (negative memory)
    assert await find_person_merge_candidates(actions.pool) == 0
    assert await review_tray(actions.pool) == []


async def test_behavioral_merge_on_shared_ttps(actions: Actions, case_id: str) -> None:
    """Two IntrusionSets sharing techniques + tools converge (DESIGN §11)."""
    cid = uuid.UUID(case_id)
    bundle = json.loads(DPRK_BUNDLE.read_text())
    await ingest_bundle(actions, bundle, case_id=cid)

    async def oid(canonical: str) -> uuid.UUID:
        return await actions.pool.fetchval("SELECT id FROM objects WHERE canonical=$1", canonical)

    # craft a second intrusion-set that reuses Lazarus's techniques + tools
    lazarus = await oid("intrusion-set--c93fccb1-e8e8-42cf-ae33-2ad1d183913a")
    twin = await actions.create_or_find_object("IntrusionSet", "intrusion-set--twin", "a", cid)
    used = await actions.pool.fetch(
        "SELECT l.to_id AS to_id, o.type AS type FROM links l JOIN objects o ON o.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='uses'",
        lazarus,
    )
    for u in used:  # twin uses the same malware + attack-patterns
        await actions.create_link(twin, u["to_id"], "uses", "analyst:test", NOW, 0.9, case_id=cid)

    queued = await find_behavioral_merge_candidates(actions.pool, min_techniques=1, min_tools=1)
    assert queued == 1
    pair = (await review_tray(actions.pool))[0]
    assert {pair["a_id"], pair["b_id"]} == {lazarus, twin}
