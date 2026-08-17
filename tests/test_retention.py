"""outbox/audit_log retention — thread e6fd3772 piece 1. Cold by default: execute=False
(the parameter default in src.orchestrator.retention, and the CLI's own --execute flag
default) only counts; nothing is ever deleted without an explicit opt-in.

Every row this file inserts carries a MARKER (event_type/action = _MARKER) distinct from
anything real code ever writes (`Actions._audit`/outbox emission use real action/
event_type names like 'object_created') — the `actions` fixture's own Type-catalog seed
writes real rows into both tables as a side effect of every create_or_find_object/
assert_property call it makes, so a bare `SELECT count(*) FROM outbox` after this file's
own inserts is NOT this file's own row count. Retention's window filtering (eligible =
older than N days) already keeps those recent seed rows out of any DRY-RUN count
correctly; the marker is only needed for POST-DELETE assertions, which must distinguish
this test's own surviving/deleted rows from the seed's."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.actions.core import Actions
from src.orchestrator.retention import audit_log_retention, outbox_retention

NOW = datetime.now(UTC)
OLD = NOW - timedelta(days=100)
RECENT = NOW - timedelta(days=1)
_MARKER = "osiris_test_retention_marker"


async def _outbox_row(actions: Actions, *, created_at: datetime, published: bool) -> None:
    await actions.pool.execute(
        f"INSERT INTO outbox (event_type, payload, created_at, published_at) "
        f"VALUES ('{_MARKER}', '{{}}', $1, $2)",
        created_at, created_at if published else None)


async def _audit_row(actions: Actions, *, created_at: datetime) -> None:
    await actions.pool.execute(
        f"INSERT INTO audit_log (action, actor, payload, created_at) "
        f"VALUES ('{_MARKER}', 'agent:test', '{{}}', $1)", created_at)


async def _marked_outbox_count(actions: Actions) -> int:
    return await actions.pool.fetchval(
        "SELECT count(*) FROM outbox WHERE event_type=$1", _MARKER)


async def _marked_audit_count(actions: Actions) -> int:
    return await actions.pool.fetchval(
        "SELECT count(*) FROM audit_log WHERE action=$1", _MARKER)


async def test_outbox_retention_dry_run_counts_without_deleting(actions: Actions) -> None:
    await _outbox_row(actions, created_at=OLD, published=True)
    await _outbox_row(actions, created_at=RECENT, published=True)

    out = await outbox_retention(actions.pool, days=30)
    assert out["executed"] is False
    assert out["eligible"] >= 1  # at least the OLD, published row; window excludes seed noise

    assert await _marked_outbox_count(actions) == 2, "a dry run must never delete anything"


async def test_outbox_retention_never_counts_or_deletes_unpublished_rows(
    actions: Actions,
) -> None:
    """An unpublished row is still awaiting the worker's own drain — no matter its age,
    retention must never touch it; a stuck backlog is a different alarm, not a reason
    for retention to quietly erase it."""
    await _outbox_row(actions, created_at=OLD, published=False)

    dry = await outbox_retention(actions.pool, days=30)
    assert dry["eligible"] == 0

    applied = await outbox_retention(actions.pool, days=30, execute=True)
    assert applied["deleted"] == 0
    assert await _marked_outbox_count(actions) == 1


async def test_outbox_retention_execute_deletes_in_batches(actions: Actions) -> None:
    for _ in range(12):
        await _outbox_row(actions, created_at=OLD, published=True)
    await _outbox_row(actions, created_at=RECENT, published=True)  # must survive

    out = await outbox_retention(actions.pool, days=30, execute=True, batch_size=5)
    assert out["executed"] is True
    assert out["deleted"] == 12

    remaining = await actions.pool.fetch(
        "SELECT created_at FROM outbox WHERE event_type=$1", _MARKER)
    assert len(remaining) == 1
    assert remaining[0]["created_at"] > OLD


async def test_audit_log_retention_dry_run_and_execute(actions: Actions) -> None:
    await _audit_row(actions, created_at=OLD)
    await _audit_row(actions, created_at=RECENT)

    dry = await audit_log_retention(actions.pool)  # default 90 days
    assert dry["executed"] is False
    assert dry["eligible"] >= 1

    applied = await audit_log_retention(actions.pool, execute=True)
    assert applied["deleted"] == 1
    assert await _marked_audit_count(actions) == 1  # only the recent row survives


async def test_audit_log_retention_respects_a_custom_window(actions: Actions) -> None:
    await _audit_row(actions, created_at=NOW - timedelta(days=10))

    out = await audit_log_retention(actions.pool, days=30)
    marked_eligible_30 = await actions.pool.fetchval(
        "SELECT count(*) FROM audit_log WHERE action=$1 AND created_at < now() - "
        "interval '30 days'", _MARKER)
    assert marked_eligible_30 == 0  # 10 days old, inside a 30-day window
    assert out["eligible"] >= marked_eligible_30

    out2 = await audit_log_retention(actions.pool, days=5)
    marked_eligible_5 = await actions.pool.fetchval(
        "SELECT count(*) FROM audit_log WHERE action=$1 AND created_at < now() - "
        "interval '5 days'", _MARKER)
    assert marked_eligible_5 == 1  # the same row, outside a 5-day window
    assert out2["eligible"] >= marked_eligible_5
