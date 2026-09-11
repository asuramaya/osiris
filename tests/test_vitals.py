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


async def test_live_counts_a_g_n_generation_as_the_same_soul_not_a_new_one(
    actions: Actions,
) -> None:
    """The live specimen (thread 25b57dca): a lineage past generation 39 carries a
    `-g<N>` suffix, not a roman numeral — the SQL soul-fold used to only strip
    `[ivxlcdm]+`, so a g<N> door counted as a brand-new soul beside its own ancestor."""
    p = actions.pool
    await save_mount(p, job_dir="/j/soulg1", agent_id="agent:ab99cd99-g40",
                     project="osiris", cwd="/w", model=None, session_key="sid:realconng1")
    await save_mount(p, job_dir="/j/soulg2", agent_id="agent:ab99cd99-g41",
                     project="osiris", cwd="/w", model=None, session_key="sid:realconng2")
    out = await vitals.live_souls(p)
    assert out == {"souls": 1, "visitors": 0}


async def test_operator_debts_empty_desk_is_zero(actions: Actions) -> None:
    out = await vitals.operator_debts(actions.pool, hood="osiris")
    assert out == {"owed": 0, "owed_here": 0}


async def _mint_agent(actions: Actions, canonical: str, *, handle: str | None = None,
                      visit: bool = False) -> None:
    from datetime import UTC, datetime

    oid = await actions.create_or_find_object("Agent", canonical, "test")
    now = datetime.now(UTC)
    if handle:
        await actions.assert_property(oid, "handle", handle, "test", now, 0.9,
                                      evidence_class="self_declared")
    if visit:
        await actions.assert_property(oid, "agent_class", "visit", "test", now, 0.9,
                                      evidence_class="self_declared")


async def test_agent_class_counts_splits_named_visit_and_unresolved(
    actions: Actions,
) -> None:
    """THE VISIT CLASS (9dc3ce8b): a handle-claiming generation is named; an agent_class=
    'visit' generation (greatfold.py's own doorbell-ring demotion) is a visit family,
    never counted as a soul; anything with neither is unresolved — not yet examined by
    a fold pass. A named generation beside an unnamed sibling of the SAME soul (the
    fold's own generation-suffix folding) still counts as one named soul, not two."""
    await _mint_agent(actions, "agent:aaaa1111", handle="Alfred")
    await _mint_agent(actions, "agent:aaaa1111-ii")  # same soul, no handle on this gen
    await _mint_agent(actions, "agent:bbbb2222", visit=True)
    await _mint_agent(actions, "agent:cccc3333")  # examined by nobody yet
    out = await vitals.agent_class_counts(actions.pool)
    assert out == {"named_souls": 1, "visit_families": 1, "unresolved_families": 1}


async def test_agent_class_counts_empty_graph_is_all_zero(actions: Actions) -> None:
    out = await vitals.agent_class_counts(actions.pool)
    assert out == {"named_souls": 0, "visit_families": 0, "unresolved_families": 0}
