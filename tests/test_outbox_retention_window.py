"""The daily heartbeat prunes the outbox at the outbox window (30 days), through the real
heartbeat and not the retention function it calls: the heartbeat once passed its own 90 while
the module default said 30, and a test of the function alone could not see that."""
from __future__ import annotations

from types import SimpleNamespace

from src.actions.core import Actions
from src.orchestrator.retention import OUTBOX_RETENTION_DAYS, outbox_retention
from src.workers.arq_worker import retention_heartbeat

_SEED = ("INSERT INTO outbox (event_type, payload, created_at, published_at) "
         "VALUES ('window_probe', jsonb_build_object('tag', $1::text), "
         "now() - make_interval(days => $2), "
         "CASE WHEN $3 THEN now() - make_interval(days => $2) END)")


async def _seed(actions: Actions, tag: str, age_days: int, *, published: bool) -> None:
    await actions.pool.execute(_SEED, tag, age_days, published)


async def _surviving(actions: Actions) -> set[str]:
    rows = await actions.pool.fetch(
        "SELECT payload->>'tag' AS tag FROM outbox WHERE event_type = 'window_probe'")
    return {r["tag"] for r in rows}


def test_the_window_is_thirty_days() -> None:
    assert OUTBOX_RETENTION_DAYS == 30


async def test_the_heartbeat_prunes_published_rows_past_thirty_days_and_nothing_else(
    actions: Actions,
) -> None:
    await _seed(actions, "pub-29", 29, published=True)
    await _seed(actions, "pub-31", 31, published=True)
    await _seed(actions, "pub-100", 100, published=True)
    await _seed(actions, "unpub-100", 100, published=False)

    await retention_heartbeat({"cascade": SimpleNamespace(actions=actions)})

    assert await _surviving(actions) == {"pub-29", "unpub-100"}


async def test_the_cli_default_and_the_heartbeat_share_one_window(actions: Actions) -> None:
    await _seed(actions, "pub-31", 31, published=True)

    dry = await outbox_retention(actions.pool)  # no days given: the default

    assert dry["days"] == OUTBOX_RETENTION_DAYS
    assert dry["eligible"] >= 1
