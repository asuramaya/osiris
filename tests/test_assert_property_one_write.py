"""One write per fact. A property assertion is recorded once, in the assertions table, which
already holds who spoke (source_id), when (created_at), what it replaced (supersedes), and the
object and name. It used to be written a second time into audit_log (6.6M of that table's 7.0M
rows). The audit row now exists only when the actor is not the source, and the duplicates
already written are retired by a batched job that deletes a row only when an assertion says
the same thing."""
from __future__ import annotations

import ast
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator.retention import assert_property_audit_retirement
from src.orchestrator.watermark import graph_watermark

ROOT = Path(__file__).resolve().parent.parent
NOW = datetime.now(UTC)


async def _audit_for(actions: Actions, obj: Any) -> list[Any]:
    return await actions.pool.fetch(
        "SELECT actor, payload FROM audit_log WHERE action='assert_property' "
        "AND payload->>'object_id' = $1::text ORDER BY id", str(obj))


async def _obj(actions: Actions, canonical: str) -> Any:
    return await actions.create_or_find_object("Domain", canonical, "analyst:test")


async def _assert(actions: Actions, obj: Any, value: Any, *, source: str = "src:a",
                  actor: str | None = None) -> int:
    return await actions.assert_property(
        obj, "label", value, source, NOW, 0.9, evidence_class="authoritative_api",
        actor=actor)


async def _restate_in_audit(actions: Actions, obj: Any, assertion_id: int,
                            *, actor: str, supersedes: int | None) -> int:
    """Write the audit row the OLD code wrote for an assertion: same transaction timestamp."""
    created = await actions.pool.fetchval(
        "SELECT created_at FROM assertions WHERE id=$1", assertion_id)
    return await actions.pool.fetchval(
        "INSERT INTO audit_log (action, actor, payload, created_at) "
        "VALUES ('assert_property', $1, $2::jsonb, $3) RETURNING id", actor,
        {"object_id": str(obj), "name": "label", "supersedes": supersedes}, created)


async def test_an_assertion_by_its_own_source_writes_no_audit_row(actions: Actions) -> None:
    obj = await _obj(actions, "one-write-1.example")
    aid = await _assert(actions, obj, "v1")
    assert aid > 0
    assert await _audit_for(actions, obj) == []
    # the fact is fully recorded where it belongs
    row = await actions.pool.fetchrow(
        "SELECT source_id, supersedes, created_at FROM assertions WHERE id=$1", aid)
    assert row is not None and row["source_id"] == "src:a" and row["supersedes"] is None
    # and its outbox event is untouched
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM outbox WHERE event_type='property_added' AND object_id=$1",
        obj) == 1


async def test_a_replacement_names_what_it_replaced_without_an_audit_row(
    actions: Actions,
) -> None:
    obj = await _obj(actions, "one-write-2.example")
    first = await _assert(actions, obj, "v1")
    second = await _assert(actions, obj, "v2")
    assert await _audit_for(actions, obj) == []
    assert await actions.pool.fetchval(
        "SELECT supersedes FROM assertions WHERE id=$1", second) == first


async def test_an_actor_that_is_not_the_source_still_leaves_an_audit_row(
    actions: Actions,
) -> None:
    """The one thing the assertions table cannot hold is a writer acting for another source."""
    obj = await _obj(actions, "one-write-3.example")
    await _assert(actions, obj, "v1", source="src:a", actor="agent:somebody-else")
    rows = await _audit_for(actions, obj)
    assert len(rows) == 1 and rows[0]["actor"] == "agent:somebody-else"


async def test_a_no_op_reassertion_writes_nothing(actions: Actions) -> None:
    obj = await _obj(actions, "one-write-4.example")
    first = await _assert(actions, obj, "v1")
    again = await _assert(actions, obj, "v1")
    assert again == first
    assert await _audit_for(actions, obj) == []
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM assertions WHERE object_id=$1 AND name='label'", obj) == 1


async def test_the_watermark_still_moves_on_a_property_write(actions: Actions) -> None:
    """The audit row used to be the signal that an assertion landed; it has its own marker."""
    obj = await _obj(actions, "one-write-5.example")
    before = await graph_watermark(actions.pool)
    await _assert(actions, obj, "v1")
    after = await graph_watermark(actions.pool)
    assert after["assertions"] is not None and after["assertions"] != before["assertions"]
    # a replacement moves it again
    await _assert(actions, obj, "v2")
    third = await graph_watermark(actions.pool)
    assert third["assertions"] != after["assertions"]


def test_no_code_reads_the_assert_property_audit_rows() -> None:
    """Proof the duplicate was unread: the only code string in src naming both audit_log and
    'assert_property' is the retirement job that deletes those rows (docstrings excluded)."""
    offenders: set[str] = set()
    for path in sorted((ROOT / "src").rglob("*.py")):
        tree = ast.parse(path.read_text())
        docstrings = {
            id(n.body[0].value) for n in ast.walk(tree)
            if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
            and n.body and isinstance(n.body[0], ast.Expr)
            and isinstance(n.body[0].value, ast.Constant)}
        for node in ast.walk(tree):
            if (isinstance(node, ast.Constant) and isinstance(node.value, str)
                    and id(node) not in docstrings
                    and "audit_log" in node.value and "assert_property" in node.value):
                offenders.add(str(path.relative_to(ROOT)))
    assert offenders == {"src/orchestrator/retention.py"}


async def _seed_old_duplicates(actions: Actions) -> dict[str, Any]:
    """Two assertions (a first and a replacement) with the audit rows the old code wrote,
    one audit row whose actor is not the source (it carries information), one that matches
    no assertion at all, and an unrelated audit row."""
    obj = await _obj(actions, "retire-1.example")
    first = await _assert(actions, obj, "v1")
    second = await _assert(actions, obj, "v2")
    dup_first = await _restate_in_audit(actions, obj, first, actor="src:a", supersedes=None)
    dup_second = await _restate_in_audit(actions, obj, second, actor="src:a", supersedes=first)
    by_proxy = await _restate_in_audit(
        actions, obj, second, actor="agent:acting-for", supersedes=first)
    orphan = await actions.pool.fetchval(
        "INSERT INTO audit_log (action, actor, payload) VALUES ('assert_property', 'src:a', "
        "$1::jsonb) RETURNING id",
        {"object_id": str(obj), "name": "label", "supersedes": 987654321})
    unrelated = await actions.pool.fetchval(
        "INSERT INTO audit_log (action, actor, payload) VALUES ('create_link', 'src:a', "
        "'{}'::jsonb) RETURNING id")
    return {"dups": [dup_first, dup_second], "keep": [by_proxy, orphan, unrelated]}


async def _present(actions: Actions, ids: list[int]) -> set[int]:
    rows = await actions.pool.fetch("SELECT id FROM audit_log WHERE id = ANY($1::bigint[])", ids)
    return {r["id"] for r in rows}


async def test_retirement_dry_run_counts_and_deletes_nothing(actions: Actions) -> None:
    seeded = await _seed_old_duplicates(actions)
    out = await assert_property_audit_retirement(actions.pool)
    assert out["executed"] is False and out["eligible"] >= 2
    assert await _present(actions, seeded["dups"] + seeded["keep"]) == set(
        seeded["dups"] + seeded["keep"])


async def test_retirement_deletes_only_rows_an_assertion_restates(actions: Actions) -> None:
    seeded = await _seed_old_duplicates(actions)
    out = await assert_property_audit_retirement(actions.pool, execute=True)
    assert out["executed"] is True and out["finished"] is True and out["deleted"] >= 2
    left = await _present(actions, seeded["dups"] + seeded["keep"])
    assert left == set(seeded["keep"]), "only the two true duplicates may go"


async def test_retirement_is_batched_and_resumes_where_it_stopped(actions: Actions) -> None:
    seeded = await _seed_old_duplicates(actions)
    first = await assert_property_audit_retirement(
        actions.pool, execute=True, batch_size=1, max_seconds=0.0)
    assert first["finished"] is False and first["deleted"] == 1
    second = await assert_property_audit_retirement(actions.pool, execute=True, batch_size=1)
    assert second["finished"] is True
    assert await _present(actions, seeded["dups"]) == set()
    assert await _present(actions, seeded["keep"]) == set(seeded["keep"])


async def test_retirement_run_twice_removes_nothing_more(actions: Actions) -> None:
    await _seed_old_duplicates(actions)
    await assert_property_audit_retirement(actions.pool, execute=True)
    again = await assert_property_audit_retirement(actions.pool, execute=True)
    assert again["deleted"] == 0 and again["finished"] is True


async def test_the_heartbeat_retires_duplicates_and_says_so(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.orchestrator.mailbox as mailbox
    from src.workers.arq_worker import storage_housekeeping_heartbeat

    seeded = await _seed_old_duplicates(actions)
    captured: dict[str, Any] = {}

    async def _fake_send(pool: Any, **kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {"sent": 1}

    monkeypatch.setattr(mailbox, "send_message", _fake_send)
    await storage_housekeeping_heartbeat({"cascade": SimpleNamespace(actions=actions)})

    assert await _present(actions, seeded["dups"]) == set()
    assert "only repeated an assertion" in captured["body"]


async def test_the_housekeeping_job_is_scheduled_daily_and_not_at_startup() -> None:
    """A startup run would hold the boot lock that serializes every other cron's own startup
    run while it works through millions of rows."""
    from src.workers.arq_worker import WorkerSettings

    jobs = [c for c in WorkerSettings.cron_jobs
            if getattr(c, "name", "").endswith("storage_housekeeping_heartbeat")]
    assert len(jobs) == 1
    assert jobs[0].run_at_startup is False
