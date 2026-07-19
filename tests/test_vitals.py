"""THE VITALS — one authority per fact (operator ruling 2026-07-19: 'the chrome and the
harness disagree on briefs, mail, owe'). These tests pin the shapes the old inline copies
got wrong, so a future copy-drift fails loudly instead of quietly disagreeing."""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator import vitals
from src.orchestrator.mounts import save_mount


async def test_live_counts_souls_not_rows_and_confesses_visitors(
    actions: Actions,
) -> None:
    """A mind with two fresh doors (its anchor + a tab view) is ONE soul; a whisper-echo
    stranger is a visitor beside the number, never inside it."""
    p = actions.pool
    # one seated soul, two doors (same lineage, different generations)
    await save_mount(p, job_dir="/j/soul1", agent_id="agent:ab12cd34-ii",
                     project="osiris", cwd="/w", model=None, session_key="sid:realconn")
    await save_mount(p, job_dir="/j/soul2", agent_id="agent:ab12cd34-iii",
                     project="osiris", cwd="/w", model=None, session_key="sid:realconn2")
    # a stranger: id is the sid echoed back, no object behind it
    await save_mount(p, job_dir="/j/vis", agent_id="agent:feed0001",
                     project="atlas", cwd="/w2", model=None,
                     session_key="whisper:feed0001")
    out = await vitals.live_souls(p)
    assert out == {"souls": 1, "visitors": 1}


async def test_operator_debts_empty_desk_is_zero(actions: Actions) -> None:
    out = await vitals.operator_debts(actions.pool, hood="osiris")
    assert out == {"owed": 0, "owed_here": 0}
