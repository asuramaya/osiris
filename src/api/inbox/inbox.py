"""PURE BUILDERS: graph -> Block tree (task #71). No HTML, no Jinja — only blocks.py
types. Data source is the SAME sanctioned read-path /live-desk already uses
(run_composition(pool, "live-desk"), ruling c5b184cd) — never a parallel hand-written
query duplicating what that composition already answers. The three live-desk sections map
onto InboxItem.item_kind (msg 1818's ruling 3: this axis is distinct from Badge/status):

  owed_to_you              (open Thread, owner='operator')      -> item_kind='review'
  decisions_awaiting_a_call (fleet_messages, desk_kind='decision') -> item_kind='question'
  drift_alarms              (open Thread, severity='alarm')     -> item_kind='notify'

Each row's own `_action` (already the exact {action, args} shape src/api/actions.py's
ACTION_VERBS registry expects) becomes ONE Button, `style='primary'` — the only action
offered per item in v0, so it is by definition "the single most-likely action" ActionRow's
own gate (msg 1818) requires."""
from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

import asyncpg

from src.api.inbox.blocks import ActionRow, Button, InboxItem, InboxList
from src.orchestrator.compositions import run_composition

_TITLE_CAP = 160  # matches orient()'s own terse-summary cap (task #60) — one house rule


def _short_title(summary: str) -> str:
    summary = " ".join(summary.split())  # collapse newlines/whitespace to one line
    return summary if len(summary) <= _TITLE_CAP else summary[: _TITLE_CAP - 1] + "…"


def _age(now: datetime, when: datetime) -> str:
    delta = now - when
    secs = int(delta.total_seconds())
    if secs < 3600:
        return f"{max(1, secs // 60)}m ago"
    if secs < 86400:
        return f"{secs // 3600}h ago"
    return f"{secs // 86400}d ago"


_ACTION_LABEL = {"resolve_thread": "Resolve", "settle": "Settle"}


def _button_for(row: dict[str, Any]) -> ActionRow | None:
    action = row.get("_action")
    if not action or "action" not in action:
        return None
    name = str(action["action"])
    return ActionRow(buttons=[
        Button(label=_ACTION_LABEL.get(name, name.replace("_", " ").title()),
               action=name, style="primary"),
    ])


async def _thread_ages(pool: asyncpg.Pool, short_ids: list[str]) -> dict[str, datetime]:
    """created_at for a batch of Thread short-ids (the composition's own `table` op
    doesn't select it — this is the one small enrichment query on top of the shared,
    UNTOUCHED live-desk composition, not a parallel rewrite of it)."""
    if not short_ids:
        return {}
    patterns = [f"{sid}%" for sid in short_ids if re.fullmatch(r"[0-9a-f]{8}", sid)]
    if not patterns:
        return {}
    rows = await pool.fetch(
        "SELECT id::text AS full_id, created_at FROM objects "
        "WHERE type='Thread' AND id::text LIKE ANY($1::text[])", patterns)
    by_prefix = {r["full_id"][:8]: r["created_at"] for r in rows}
    return {sid: by_prefix[sid] for sid in short_ids if sid in by_prefix}


async def build_inbox(pool: asyncpg.Pool) -> InboxList:
    """The whole Inbox, one call: run live-desk, fold its three sections into one
    triage-to-zero queue (Linear's own discipline, research-prior-art.md mechanism 4) —
    ALL admitted items in one list, newest first within each kind's own natural order."""
    out = await run_composition(pool, "live-desk")
    sections: dict[str, Any] = out["items"]
    now = datetime.now(UTC)

    owed = sections.get("owed_to_you", [])
    alarms = sections.get("drift_alarms", [])
    ages = await _thread_ages(pool, [r["id"] for r in (*owed, *alarms)])

    items: list[InboxItem] = []
    for row in sections.get("decisions_awaiting_a_call", []):
        when = row.get("when")
        when_dt = datetime.fromisoformat(when) if isinstance(when, str) else when
        items.append(InboxItem(
            id=str(row["id"]), item_kind="question", title=_short_title(row["summary"]),
            age=_age(now, when_dt) if when_dt else "—", actions=_button_for(row)))
    for row in owed:
        when_dt = ages.get(row["id"])
        items.append(InboxItem(
            id=row["id"], item_kind="review", title=_short_title(row["summary"]),
            age=_age(now, when_dt) if when_dt else "—", actions=_button_for(row)))
    for row in alarms:
        when_dt = ages.get(row["id"])
        items.append(InboxItem(
            id=row["id"], item_kind="notify", title=_short_title(row["summary"]),
            age=_age(now, when_dt) if when_dt else "—", actions=_button_for(row)))

    return InboxList(items=items)
