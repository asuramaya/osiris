"""Restoring one item from a past moment: additive, planned first, conflicts chosen."""
from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql
from src.actions.core import Actions
from src.db.pool import create_pool
from src.orchestrator import time_machine_restore as tr

_T0 = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
_EC = "direct_observation"


def _split(dsn: str) -> tuple[str, str]:
    head, _, db = dsn.rpartition("/")
    return head, db


@pytest_asyncio.fixture
async def moment(pg_dsn: str) -> AsyncIterator[Actions]:
    """A second database cloned from the test one, standing in for a restored past moment
    (the same schema, its own rows). Made BEFORE `actions` opens its pool: a template must
    have no other connections."""
    head, db = _split(pg_dsn)
    name = f"tm_moment_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(f"{head}/test", autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
            sql.Identifier(name), sql.Identifier(db)))
    pool = await create_pool(f"{head}/{name}")
    from src.ontology.catalog import is_known_object_type, seed_catalog

    world = Actions(pool)
    if not await is_known_object_type(pool, "Organization"):
        await seed_catalog(world)
    try:
        yield world
    finally:
        await pool.close()
        with psycopg.connect(f"{head}/test", autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(name)))


async def _put(a: Actions, typ: str, canonical: str, props: dict[str, Any] | None = None,
               *, source: str = "src1", conf: float = 0.8, at: datetime = _T0) -> uuid.UUID:
    oid = await a.create_or_find_object(typ, canonical, "test")
    for name, value in (props or {}).items():
        await a.assert_property(oid, name, value, source, at, conf, evidence_class=_EC)
    return oid


async def _displayed(a: Actions, oid: uuid.UUID, name: str) -> Any:
    row = await a.pool.fetchrow(
        "SELECT value::text AS v FROM current_assertions WHERE object_id=$1 AND name=$2 "
        "ORDER BY confidence DESC, observed_at DESC, id DESC LIMIT 1", oid, name)
    import json
    return json.loads(row["v"])


async def _run(moment: Actions, actions: Actions, ref: str, **kw: Any) -> dict[str, Any]:
    plan = await tr.plan_restore(moment.pool, actions, ref, moment_label="dump X")
    return await tr.apply_restore(moment.pool, actions, ref, expect_digest=plan["digest"],
                                  actor="operator", moment_label="dump X", **kw)


@pytest.mark.asyncio
async def test_an_absent_object_is_created_with_its_current_assertions(
        moment: Actions, actions: Actions) -> None:
    await _put(moment, "Thread", "tm-r-absent", {"summary": "old thread", "status": "open"})
    plan = await tr.plan_restore(moment.pool, actions, "tm-r-absent")
    assert plan["object"]["action"] == "create" and plan["counts"]["copy"] == 2
    rec = await _run(moment, actions, "tm-r-absent", note="by mistake retracted")
    assert rec["changed"] and rec["created"] and rec["assertions_copied"] == 2
    oid = uuid.UUID(rec["object_id"])
    assert await _displayed(actions, oid, "summary") == "old thread"
    prov = await _displayed(actions, oid, "restored_from")
    assert prov["moment"] == "dump X" and prov["note"] == "by mistake retracted"
    ev = await actions.pool.fetchrow(
        "SELECT actor, payload::text AS p FROM object_events WHERE object_id=$1 "
        "AND event_type='restore'", oid)
    assert ev["actor"] == "operator" and "dump X" in ev["p"]


@pytest.mark.asyncio
async def test_a_repeat_is_a_no_op_and_writes_nothing(moment: Actions, actions: Actions) -> None:
    await _put(moment, "Thread", "tm-r-repeat", {"summary": "same"})
    first = await _run(moment, actions, "tm-r-repeat")
    n_assert = await actions.pool.fetchval("SELECT count(*) FROM assertions")
    n_events = await actions.pool.fetchval(
        "SELECT count(*) FROM object_events WHERE event_type='restore'")
    again = await _run(moment, actions, "tm-r-repeat")
    assert first["changed"] and again["changed"] is False
    assert await actions.pool.fetchval("SELECT count(*) FROM assertions") == n_assert
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM object_events WHERE event_type='restore'") == n_events


@pytest.mark.asyncio
async def test_an_existing_object_keeps_its_uuid_and_history(
        moment: Actions, actions: Actions) -> None:
    live = await _put(actions, "Thread", "tm-r-exists", {"summary": "kept", "owner": "x"})
    await _put(moment, "Thread", "tm-r-exists", {"summary": "kept", "note": "was here"})
    plan = await tr.plan_restore(moment.pool, actions, "tm-r-exists")
    assert plan["object"]["live_id"] == str(live) and plan["object"]["action"] == "add_to_existing"
    assert [s["name"] for s in plan["assertions"]["skip"]] == ["summary"]
    assert [c["name"] for c in plan["assertions"]["copy"]] == ["note"]
    assert plan["live_only"] == ["owner"]
    rec = await _run(moment, actions, "tm-r-exists")
    assert rec["object_id"] == str(live) and not rec["created"]
    assert await _displayed(actions, live, "owner") == "x"           # live-only untouched
    assert await _displayed(actions, live, "note") == "was here"


@pytest.mark.asyncio
async def test_same_source_change_is_a_conflict_kept_by_default(
        moment: Actions, actions: Actions) -> None:
    live = await _put(actions, "Thread", "tm-r-conf", {"summary": "new live text"})
    await _put(moment, "Thread", "tm-r-conf", {"summary": "old text"})
    plan = await tr.plan_restore(moment.pool, actions, "tm-r-conf")
    (c,) = plan["assertions"]["conflicts"]
    assert c["kind"] == "same_source_changed" and c["default"] == "keep"
    assert c["live"]["value"] == "new live text"
    rec = await _run(moment, actions, "tm-r-conf")
    assert not rec["changed"] and rec["conflicts_kept"] == [c["key"]]
    assert await _displayed(actions, live, "summary") == "new live text"
    # choosing restore supersedes it as a NEW write; the live row is kept in the chain
    rec2 = await _run(moment, actions, "tm-r-conf", resolutions={c["key"]: "restore"})
    assert rec2["changed"] and rec2["assertions_copied"] == 1
    assert await _displayed(actions, live, "summary") == "old text"
    chain = await actions.pool.fetch(
        "SELECT value::text AS v, is_current FROM assertions WHERE object_id=$1 "
        "AND name='summary' ORDER BY id", live)
    assert [r["is_current"] for r in chain] == [False, True] and len(chain) == 2


@pytest.mark.asyncio
async def test_equal_confidence_and_time_with_a_different_value_is_a_conflict_never_a_silent_win(
        moment: Actions, actions: Actions) -> None:
    """A restored row gets a NEW, higher id, and id is the last tiebreak of which value is
    current: appended blindly it would beat a live value it merely ties."""
    live = await _put(actions, "Thread", "tm-r-tie", {"summary": "live B"},
                      source="live-src", conf=0.8, at=_T0)
    await _put(moment, "Thread", "tm-r-tie", {"summary": "moment A"},
               source="old-src", conf=0.8, at=_T0)
    plan = await tr.plan_restore(moment.pool, actions, "tm-r-tie")
    assert plan["counts"]["copy"] == 0
    (c,) = plan["assertions"]["conflicts"]
    assert c["kind"] == "tie" and c["live"]["source_id"] == "live-src"
    rec = await _run(moment, actions, "tm-r-tie")                         # no choice made
    assert not rec["changed"]
    assert await _displayed(actions, live, "summary") == "live B"
    await _run(moment, actions, "tm-r-tie", resolutions={c["key"]: "restore"})
    assert await _displayed(actions, live, "summary") == "moment A"      # only by choice


@pytest.mark.asyncio
async def test_a_restored_value_that_outranks_live_is_marked_and_an_older_one_is_not(
        moment: Actions, actions: Actions) -> None:
    await _put(actions, "Thread", "tm-r-eff", {"summary": "live"}, source="a", conf=0.7,
               at=_T0)
    await _put(moment, "Thread", "tm-r-eff", {"summary": "stronger"}, source="b", conf=0.9,
               at=_T0)
    await _put(moment, "Thread", "tm-r-eff", {"rationale": "x"}, source="b", conf=0.5, at=_T0)
    plan = await tr.plan_restore(moment.pool, actions, "tm-r-eff")
    eff = {c["name"]: c["effect"] for c in plan["assertions"]["copy"]}
    assert eff == {"summary": "becomes_displayed", "rationale": "new_property"}
    await _put(actions, "Thread", "tm-r-eff", {"rationale": "live r"}, source="a", conf=0.9,
               at=_T0 + timedelta(days=1))
    plan2 = await tr.plan_restore(moment.pool, actions, "tm-r-eff")
    eff2 = {c["name"]: c["effect"] for c in plan2["assertions"]["copy"]}
    assert eff2["rationale"] == "alongside"


@pytest.mark.asyncio
async def test_links_copy_only_with_both_ends_live(moment: Actions, actions: Actions) -> None:
    a = await _put(moment, "Thread", "tm-l-a", {"summary": "a"})
    b = await _put(moment, "Decision", "tm-l-b", {"summary": "b"})
    c = await _put(moment, "Decision", "tm-l-c", {"summary": "c"})
    d = await _put(moment, "Decision", "tm-l-d", {"summary": "d"})
    for other in (b, c, d):
        await moment.create_link(a, other, "answers", "src1", _T0, 0.8, evidence_class=_EC)
    la = await _put(actions, "Thread", "tm-l-a", {"summary": "a"})
    lb = await _put(actions, "Decision", "tm-l-b", {"summary": "b"})
    await _put(actions, "Decision", "tm-l-d", {"summary": "d"})
    await actions.create_link(la, (await _put(actions, "Decision", "tm-l-d")), "answers",
                              "src9", _T0, 0.8, evidence_class=_EC)
    plan = await tr.plan_restore(moment.pool, actions, "tm-l-a")
    copy = {i["other"]["canonical"] for i in plan["links"]["copy"]}
    skip = {i["other"]["canonical"]: i["reason"] for i in plan["links"]["skip"]}
    assert copy == {"tm-l-b"}
    assert skip["tm-l-c"] == "endpoint absent live"
    assert "already exists" in skip["tm-l-d"]
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE canonical='tm-l-c'") == 0      # no stub made
    rec = await _run(moment, actions, "tm-l-a")
    assert rec["links_copied"] == 1
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND to_id=$2 AND type='answers'",
        la, lb) == 1


@pytest.mark.asyncio
async def test_a_merged_endpoint_follows_its_head(moment: Actions, actions: Actions) -> None:
    a = await _put(moment, "Thread", "tm-m-a", {"summary": "a"})
    old = await _put(moment, "Decision", "tm-m-old", {"summary": "o"})
    await moment.create_link(a, old, "answers", "src1", _T0, 0.8, evidence_class=_EC)
    la = await _put(actions, "Thread", "tm-m-a", {"summary": "a"})
    lold = await _put(actions, "Decision", "tm-m-old")
    head = await _put(actions, "Decision", "tm-m-head")
    await actions.merge_objects(head, lold, "same thing", "test")
    plan = await tr.plan_restore(moment.pool, actions, "tm-m-a")
    (item,) = plan["links"]["copy"]
    assert "merged" in item["note"]
    await _run(moment, actions, "tm-m-a")
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND to_id=$2", la, head) == 1


@pytest.mark.asyncio
async def test_stale_digest_and_bad_choices_are_refused(
        moment: Actions, actions: Actions) -> None:
    await _put(moment, "Thread", "tm-r-stale", {"summary": "s", "owner": "o"})
    plan = await tr.plan_restore(moment.pool, actions, "tm-r-stale")
    await _put(actions, "Thread", "tm-r-stale", {"summary": "s"})       # live moved after the plan
    with pytest.raises(tr.RestoreError, match="changed since"):
        await tr.apply_restore(moment.pool, actions, "tm-r-stale",
                               expect_digest=plan["digest"], actor="operator")
    fresh = await tr.plan_restore(moment.pool, actions, "tm-r-stale")
    with pytest.raises(tr.RestoreError, match="unknown choice"):
        await tr.apply_restore(moment.pool, actions, "tm-r-stale", expect_digest=fresh["digest"],
                               actor="operator", resolutions={"a:1": "overwrite"})
    with pytest.raises(tr.RestoreError, match="no such conflict"):
        await tr.apply_restore(moment.pool, actions, "tm-r-stale", expect_digest=fresh["digest"],
                               actor="operator", resolutions={"a:999999": "restore"})
    with pytest.raises(tr.RestoreError, match="nothing in that moment"):
        await tr.plan_restore(moment.pool, actions, "tm-never-existed")


@pytest.mark.asyncio
async def test_an_earlier_restore_note_in_the_moment_is_not_copied(
        moment: Actions, actions: Actions) -> None:
    await _put(moment, "Thread", "tm-r-note", {"summary": "s", "restored_from": {"x": 1}})
    plan = await tr.plan_restore(moment.pool, actions, "tm-r-note")
    names = [c["name"] for c in plan["assertions"]["copy"]]
    assert names == ["summary"]


@pytest.mark.asyncio
async def test_only_the_chosen_links_are_added(moment: Actions, actions: Actions) -> None:
    a = await _put(moment, "Thread", "tm-k-a", {"summary": "a"})
    for name in ("tm-k-b", "tm-k-c"):
        o = await _put(moment, "Decision", name, {"summary": name})
        await moment.create_link(a, o, "answers", "src1", _T0, 0.8, evidence_class=_EC)
        await _put(actions, "Decision", name)
    la = await _put(actions, "Thread", "tm-k-a", {"summary": "a"})
    plan = await tr.plan_restore(moment.pool, actions, "tm-k-a")
    by_other = {i["other"]["canonical"]: i["key"] for i in plan["links"]["copy"]}
    assert set(by_other) == {"tm-k-b", "tm-k-c"}
    rec = await tr.apply_restore(moment.pool, actions, "tm-k-a", expect_digest=plan["digest"],
                                 actor="operator", link_keys=[by_other["tm-k-c"]])
    assert rec["links_copied"] == 1
    rows = await actions.pool.fetch(
        "SELECT o.canonical FROM links l JOIN objects o ON o.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='answers'", la)
    assert [r["canonical"] for r in rows] == ["tm-k-c"]
    fresh = await tr.plan_restore(moment.pool, actions, "tm-k-a")
    assert [i["other"]["canonical"] for i in fresh["links"]["copy"]] == ["tm-k-b"]
    with pytest.raises(tr.RestoreError, match="no such planned link"):
        await tr.apply_restore(moment.pool, actions, "tm-k-a", expect_digest=fresh["digest"],
                               actor="operator", link_keys=["l:0"])
