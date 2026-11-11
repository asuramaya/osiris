"""The deliverable-to-reader predicate checks the reader's own receipt first.

`_DELIVERABLE_TO_READER` gained `r.read_at IS NULL` so a reader who has already settled a
broadcast is dropped before the lineage probe scans every other reader's receipt. The clause is
implied by the existing NOT EXISTS (it includes the reader's own receipt row), so the answer
must not change: this pins that against the predicate as it was."""
from __future__ import annotations

from typing import Any

from src.orchestrator import mailbox
from src.orchestrator.agents import _generation
from src.orchestrator.mailbox import ack_messages, read_inbox, send_message
from src.orchestrator.mounts import save_mount

# the predicate as it read before the clause was added
_OLD = mailbox._DELIVERABLE_TO_READER.replace("AND r.read_at IS NULL ", "", 1)


async def _ids_old(pool: Any, agent: str, project: str) -> list[int]:
    q = ("SELECT m.id FROM fleet_messages m "
         "LEFT JOIN message_recipients r ON r.message_id=m.id AND r.agent_id=$1 "
         "WHERE " + _OLD + " ORDER BY m.id")
    q = (q.replace("$agent", "$1").replace("$project", "$2").replace("$lease", "$3")
         .replace("$grace", "$4").replace("$lineage", "$5"))
    rows = await pool.fetch(q, agent, project, 900, mailbox._HOLD_GRACE_SECS,
                            _generation(agent)[0])
    return [r["id"] for r in rows]


async def test_the_receipt_clause_changes_nothing_for_any_reader(actions: Any) -> None:
    pool = actions.pool
    await save_mount(pool, job_dir="/test/seed/demo", agent_id="agent:seed-demo",
                     project="demo", cwd="/test", model=None, session_key=None, alive=False)
    ids = [(await send_message(pool, from_agent="agent:sender0001", from_project="other",
                               to_project="demo", body=f"broadcast {i}"))["id"]
           for i in range(4)]
    dm = (await send_message(pool, from_agent="agent:sender0001", from_project="other",
                             to_agent="agent:reader01", body="a DM"))["id"]
    # reader01 settles broadcast 0 itself; its lineage mate (-ii) settles broadcast 1;
    # a stranger settles broadcast 2; broadcast 3 and the DM stay unread
    for reader, mid in (("agent:reader01", ids[0]), ("agent:reader01-ii", ids[1]),
                        ("agent:stranger01", ids[2])):
        await ack_messages(pool, "demo", [mid], reader_agent=reader)

    for reader in ("agent:reader01", "agent:reader01-ii", "agent:reader01-iii",
                   "agent:stranger01", "agent:newcomer01"):
        new = [m["id"] for m in await read_inbox(
            pool, "demo", reader_agent=reader, mark_read=False)]
        assert sorted(new) == await _ids_old(pool, reader, "demo"), reader
        assert new, f"{reader} should still have unread mail, or this compares nothing"
    seen = {m["id"] for m in await read_inbox(
        pool, "demo", reader_agent="agent:reader01", mark_read=False)}
    assert ids[0] not in seen and ids[1] not in seen        # own and lineage receipts exclude
    assert ids[2] in seen and ids[3] in seen and dm in seen  # a stranger's read does not
