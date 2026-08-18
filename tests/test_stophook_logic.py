"""stophook_logic (task #180 piece 2 (b)) — the pure halves osiris_stophook.py's own
`_deliverable`/`_offload_boxes` now delegate to, and the /stop route calls directly
against the shared pool. tests/test_stophook.py already covers the FULL behavioral
surface (project resolution, self-echo, the settle-state rollup, seat-office cwd
correction) through those wrappers; this file only proves the extracted functions work
against a bare Pool (not just a Connection) — the shape the /stop route actually uses."""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.mounts import save_mount
from src.orchestrator.stophook_logic import compute_stop_deliverable, compute_stop_offload
from src.parsers.base import EvidenceClass


async def test_compute_stop_deliverable_works_against_a_pool_not_just_a_connection(
    actions: Actions,
) -> None:
    out = await compute_stop_deliverable(actions.pool, cwd="/nowhere", session_id="")
    assert out == {"n": 0, "senders": [], "window": None, "bands": {}, "project": None}


async def test_compute_stop_offload_unresolvable_session_returns_none(
    actions: Actions,
) -> None:
    out = await compute_stop_offload(actions.pool, session_id="00000000-none", cwd="/nowhere")
    assert out is None


async def test_compute_stop_deliverable_counts_unread_mail_for_a_mounted_session(
    actions: Actions,
) -> None:
    a = "agent:stophooklogic1"
    obj = await actions.create_or_find_object("Agent", a, a)
    await actions.assert_property(obj, "project", "logicproj", a, datetime.now(UTC), 0.9,
                                  evidence_class=EvidenceClass.SELF_DECLARED.value)
    sid = "logicses-0000-4000-8000-000000000000"  # find_session_row's lane 1 matches on
    # job_dir containing '/jobs/' + sid[:8] ("logicses")
    await save_mount(actions.pool, job_dir="/j/jobs/logicses", agent_id=a, project="logicproj",
                     cwd="/lp/office", model="claude-fable-5", session_key=None)
    from src.orchestrator.mailbox import send_message

    await send_message(actions.pool, from_agent="agent:other", from_project="logicproj",
                       to_agent=a, body="hi", grade="ask")

    out = await compute_stop_deliverable(actions.pool, cwd="/lp/office", session_id=sid)
    assert out["n"] == 1
    assert out["bands"] == {"ask": 1, "fyi": 0}
