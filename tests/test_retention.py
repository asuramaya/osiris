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
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator.retention import _apply, _dry_run, audit_log_retention, outbox_retention

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


# ═══ THE GRAPH IS NEVER A RETENTION TARGET (wave 12 item 1's own acceptance test) ══════════
# `table` is f-string-interpolated straight into the DELETE inside _dry_run/_apply — this
# proves the one guard standing between this module and a graph-eating prune actually
# refuses every graph table by name, never just outbox/audit_log's own two callers by
# convention. pool=None is safe: the guard raises before either function ever touches it.

_GRAPH_TABLES = (
    "objects", "assertions", "current_assertions", "links", "soul_lines",
    "harness_turns", "fleet_messages",
)


@pytest.mark.parametrize("table", _GRAPH_TABLES)
async def test_dry_run_refuses_every_graph_table(table: str) -> None:
    with pytest.raises(ValueError, match="unlisted table"):
        await _dry_run(None, table, "1=1", 90)  # type: ignore[arg-type]


@pytest.mark.parametrize("table", _GRAPH_TABLES)
async def test_apply_refuses_every_graph_table(table: str) -> None:
    with pytest.raises(ValueError, match="unlisted table"):
        await _apply(None, table, "1=1", 90, 100)  # type: ignore[arg-type]


# ═══ the daily cron shim (wave 12 item 1, Thoth DM 8378) ═══════════════════════════════════

async def test_retention_heartbeat_is_a_no_op_when_the_flag_is_off(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    import src.config.settings as settings_mod
    from src.config.settings import Settings
    from src.workers.arq_worker import retention_heartbeat

    await _outbox_row(actions, created_at=OLD, published=True)
    monkeypatch.setattr(
        settings_mod, "get_settings",
        lambda: Settings(osiris_retention_heartbeat_enabled=False))
    ctx = {"cascade": SimpleNamespace(actions=actions)}
    assert await retention_heartbeat(ctx) == 0
    assert await _marked_outbox_count(actions) == 1  # untouched


async def test_retention_heartbeat_deletes_from_both_tables_and_briefs_the_desk(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    import src.orchestrator.mailbox as mailbox
    from src.workers.arq_worker import retention_heartbeat

    await _outbox_row(actions, created_at=OLD, published=True)
    await _audit_row(actions, created_at=OLD)

    captured: dict[str, Any] = {}

    async def _fake_send(pool: Any, **kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {"sent": 1}

    monkeypatch.setattr(mailbox, "send_message", _fake_send)
    ctx = {"cascade": SimpleNamespace(actions=actions)}
    deleted = await retention_heartbeat(ctx)

    assert deleted >= 2
    assert await _marked_outbox_count(actions) == 0
    assert await _marked_audit_count(actions) == 0
    assert captured["to_project"] == "operator"
    assert captured["desk_kind"] == "fyi"
    assert "outbox" in captured["body"] and "audit_log" in captured["body"]


async def test_retention_heartbeat_survives_the_desk_being_unreachable(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    import src.orchestrator.mailbox as mailbox
    from src.workers.arq_worker import retention_heartbeat

    await _outbox_row(actions, created_at=OLD, published=True)

    async def _boom(*a: Any, **k: Any) -> None:
        raise RuntimeError("mailbox down")

    monkeypatch.setattr(mailbox, "send_message", _boom)
    ctx = {"cascade": SimpleNamespace(actions=actions)}
    deleted = await retention_heartbeat(ctx)  # the delete must still land
    assert deleted >= 1
    assert await _marked_outbox_count(actions) == 0
