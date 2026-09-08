"""A desk alarm callable from anywhere, including a bash script that has no MCP or asyncpg of
its own (osiris_backup.sh's disk guard, item 5 of the vault lane). Same mailbox path as
osiris_preflight.py's/osiris_smoke.py's own `brief_operator` — this is that same act, factored
out so a non-Python caller can raise one too:

    .venv/bin/python scripts/osiris_alarm.py "message text"
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

DSN = "postgresql://osiris:osiris@127.0.0.1:5601/osiris"


async def send_alarm(body: str, *, from_agent: str) -> None:
    """Uses `src.db.pool.create_pool`, NOT bare `asyncpg.create_pool` (thread 8542ee89):
    the former registers the jsonb codec every graph write through
    `Actions.assert_property` depends on, which send_message's own graph-edge half
    needs — a bare pool silently degrades every alarm this function sends to
    'relational row committed, graph edge write failed', found live via
    osiris_preflight.py's own --drill run."""
    from src.db.pool import create_pool
    from src.orchestrator.mailbox import send_message

    pool = await create_pool(
        DSN, min_size=1, max_size=1, application_name=f"osiris-script:{from_agent}")
    try:
        await send_message(pool, from_agent=f"system:{from_agent}", from_project="osiris",
                           to_project="operator", body=body)
    finally:
        await pool.close()


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if not argv:
        print("usage: osiris_alarm.py <message> [--from <tag>]", file=sys.stderr)
        return 2
    from_agent = "alarm"
    if "--from" in argv:
        i = argv.index("--from")
        from_agent = argv[i + 1]
        del argv[i:i + 2]
    body = argv[0]
    try:
        asyncio.run(send_alarm(body, from_agent=from_agent))
        print("(alarm placed on the operator's desk)")
        return 0
    except Exception as e:  # noqa: BLE001 — the desk being down is itself printed, never swallowed
        print(f"(could not raise alarm: {e})", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
