"""The fleet mailbox — directed coordination on top of the shared memory graph.

The fleet co-writes MEMORY; this is the directed CHANNEL: a message addressed to a PROJECT,
read by whoever works there (the project is stable; a session id is not). PULL, never push
(Osiris has no hands): the recipient reads its inbox when it next mounts/orients — the server
never taps a live agent on the shoulder, and an agent only perceives mail when it takes a turn.
Messages are ephemeral coordination, not durable memory, so they live in their own table,
never the event-sourced graph — for durable knowledge, agents use record_decision/open_thread.
"""
from __future__ import annotations

from typing import Any

import asyncpg


def _norm(project: str) -> str:
    """A project address, with the optional `repo:` canonical prefix stripped."""
    return project.removeprefix("repo:").strip()


async def send_message(
    pool: asyncpg.Pool, *, from_agent: str, from_project: str | None, to_project: str, body: str
) -> int:
    """Post a message to a project's inbox. Returns the message id. `from_agent` is provenance."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "INSERT INTO fleet_messages (from_agent, from_project, to_project, body) "
        "VALUES ($1,$2,$3,$4) RETURNING id",
        from_agent, from_project, _norm(to_project), body,
    )


async def unread_count(pool: asyncpg.Pool, to_project: str) -> int:
    """How many unread messages await this project — the number mount()/orient() surface."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT count(*) FROM fleet_messages WHERE to_project=$1 AND read_at IS NULL",
        _norm(to_project),
    )


async def read_inbox(
    pool: asyncpg.Pool, to_project: str, *, mark_read: bool = True, limit: int = 50
) -> list[dict[str, Any]]:
    """A project's unread messages, oldest first. Reading MARKS them read (mailbox semantics)
    unless mark_read=False (a peek). Each carries its sender agent, so provenance travels."""
    proj = _norm(to_project)
    rows = await pool.fetch(
        "SELECT id, from_agent, from_project, body, created_at FROM fleet_messages "
        "WHERE to_project=$1 AND read_at IS NULL ORDER BY created_at LIMIT $2",
        proj, limit,
    )
    if mark_read and rows:
        await pool.execute(
            "UPDATE fleet_messages SET read_at=now() WHERE id = ANY($1::bigint[])",
            [r["id"] for r in rows],
        )
    return [{"from": r["from_agent"], "from_project": r["from_project"], "body": r["body"],
             "when": r["created_at"].isoformat()} for r in rows]
