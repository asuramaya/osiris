"""The shared console cursor — real-time Claude↔front sync.

A SINGLETON view-state both Claude (via MCP) and the browser read/write, so the front end
becomes the conversation: Claude focuses an object or runs a lens → the screen follows; the
operator navigates → Claude reads their cursor. `set_console` bumps a monotonic `rev` and
stamps `updated_by`; each side ignores stream events it authored (echo-suppression). This is
VIEW state (room / composition / view / focused object / working spec) — never entity data;
the graph stays global, exactly like rooms scope work but not the store.
"""
from __future__ import annotations

from typing import Any

import asyncpg

# the only fields a writer may set (view state, not entity data)
_ALLOWED = ("room_id", "composition", "view", "focused_object_id", "working_spec")
_EMPTY: dict[str, Any] = {
    "id": "default", "rev": 0, "updated_by": "human", "room_id": None,
    "composition": None, "view": None, "focused_object_id": None, "working_spec": None,
}


def _row(row: asyncpg.Record | None) -> dict[str, Any]:
    if row is None:
        return dict(_EMPTY)
    d = dict(row)
    for k in ("room_id", "focused_object_id"):
        if d.get(k) is not None:
            d[k] = str(d[k])
    if d.get("updated_at") is not None:
        d["updated_at"] = d["updated_at"].isoformat()
    return d


async def get_console(pool: asyncpg.Pool) -> dict[str, Any]:
    """The current shared cursor — what the console is showing right now."""
    return _row(await pool.fetchrow("SELECT * FROM console_state WHERE id='default'"))


async def set_console(pool: asyncpg.Pool, *, by: str, **fields: Any) -> dict[str, Any]:
    """Partial-update the cursor (only the given fields), bump `rev`, stamp who moved.

    `by` is 'claude' or 'human'. Unknown fields are rejected — this writes view state only.
    Upsert so it's robust if the singleton is absent; returns the new full state (the writer
    learns its own `rev` for echo-suppression).
    """
    # rev=1 on a fresh insert, rev+1 on conflict — so every write bumps monotonically even
    # if the singleton was absent (the migration seeds it; tests wipe it via CASCADE).
    cols, vals, args = ["id", "updated_by", "rev"], ["'default'", "$1", "1"], [by]
    updates = ["updated_by=excluded.updated_by", "rev=console_state.rev+1", "updated_at=now()"]
    for k, v in fields.items():
        if k not in _ALLOWED:
            raise ValueError(f"unknown console field: {k}")
        args.append(v)
        cols.append(k)
        vals.append(f"${len(args)}")
        updates.append(f"{k}=excluded.{k}")
    query = (f"INSERT INTO console_state ({', '.join(cols)}) VALUES ({', '.join(vals)}) "
             f"ON CONFLICT (id) DO UPDATE SET {', '.join(updates)} RETURNING *")
    return _row(await pool.fetchrow(query, *args))
