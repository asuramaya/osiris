"""`folds.living_heads` is `living_head` over many labels with the per-node reads done once.
It exists for the graph lint's stale-off-head-link check, which cost ~160 s live (a Python loop of
`living_head` per lineage base: five-plus queries per generation). The contract is EQUALITY with
the real function, so the tests build every lineage shape the walk distinguishes and compare."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator import folds, mounts
from src.parsers.base import EvidenceClass

NOW = datetime(2026, 11, 1, tzinfo=UTC)
_SD = EvidenceClass.SELF_DECLARED.value
T = "agent:teller"


async def _agent(actions: Actions, canonical: str) -> Any:
    return await actions.create_or_find_object("Agent", canonical, T)


async def _succeeded(actions: Actions, a: Any, nxt: str, *, source: str = T, conf: float = 0.9,
                     minute: int = 0) -> None:
    await actions.assert_property(
        a, "succeeded_by", nxt, source, NOW.replace(minute=minute), conf, evidence_class=_SD)


async def _mount(actions: Actions, agent_id: str) -> None:
    await mounts.save_mount(actions.pool, job_dir=f"/x/jobs/{agent_id[-8:]}", agent_id=agent_id,
                            project="p", cwd="/w", model=None, session_key=None)


async def _build(actions: Actions) -> list[str]:
    """Every shape `lineage_head` / `living_head` tell apart, as separate lineages."""
    ids: list[str] = []

    # a plain chain to an active tail
    a1 = await _agent(actions, "agent:chain0001")
    a2 = await _agent(actions, "agent:chain0001-ii")
    await _agent(actions, "agent:chain0001-iii")
    await _succeeded(actions, a1, "agent:chain0001-ii")
    await _succeeded(actions, a2, "agent:chain0001-iii")
    ids += ["agent:chain0001", "agent:chain0001-ii", "agent:chain0001-iii"]

    # a husk tail (false_mint) after a real head: the head stops one hop early
    h1, h2 = await _agent(actions, "agent:husk0002"), await _agent(actions, "agent:husk0002-ii")
    await _succeeded(actions, h1, "agent:husk0002-ii")
    await actions.assert_property(h2, "false_mint", True, T, NOW, 0.9, evidence_class=_SD)
    ids += ["agent:husk0002", "agent:husk0002-ii"]

    # a chain that dead-ends on a missing successor object
    m1 = await _agent(actions, "agent:gone0003")
    await _succeeded(actions, m1, "agent:gone0003-ii")
    ids += ["agent:gone0003"]

    # a cycle
    c1, c2 = await _agent(actions, "agent:ring0004"), await _agent(actions, "agent:ring0004-ii")
    await _succeeded(actions, c1, "agent:ring0004-ii")
    await _succeeded(actions, c2, "agent:ring0004")
    ids += ["agent:ring0004", "agent:ring0004-ii"]

    # a fork: two distinct successors from two sources (the real walk arbitrates by liveness)
    f1 = await _agent(actions, "agent:fork0005")
    await _agent(actions, "agent:fork0005-ii")
    await _agent(actions, "agent:fork0005-iii")
    await _succeeded(actions, f1, "agent:fork0005-ii", source="agent:s1")
    await _succeeded(actions, f1, "agent:fork0005-iii", source="agent:s2", minute=5)
    ids += ["agent:fork0005"]

    # a merged base: folded into a winner that has its own successor
    d = await _agent(actions, "agent:dupe0006")
    w1, w2 = await _agent(actions, "agent:win0006"), await _agent(actions, "agent:win0006-ii")
    await _succeeded(actions, w1, "agent:win0006-ii")
    await actions.merge_objects(w1, d, justification="test", actor="operator")
    del w2
    ids += ["agent:dupe0006", "agent:win0006"]

    # a mount newer than the walk's head wins (the lineage's freshest live generation)
    n1 = await _agent(actions, "agent:mount0007")
    await _agent(actions, "agent:mount0007-ii")
    await _succeeded(actions, n1, "agent:mount0007-ii")
    await _mount(actions, "agent:mount0007-iv")
    ids += ["agent:mount0007", "agent:mount0007-ii"]

    # a mount OLDER than the head does not win
    o1 = await _agent(actions, "agent:oldmount0008")
    await _agent(actions, "agent:oldmount0008-iii")
    await _succeeded(actions, o1, "agent:oldmount0008-iii")
    await _mount(actions, "agent:oldmount0008-ii")
    ids += ["agent:oldmount0008"]

    # an unsucceeded base, retired (non-active) and not, and a label nothing knows
    await _agent(actions, "agent:solo0009")
    r = await _agent(actions, "agent:solo0010")
    await actions.pool.execute("UPDATE objects SET status='retired' WHERE id=$1", r)
    ids += ["agent:solo0009", "agent:solo0010", "agent:nobody0011"]

    # a deep chain, longer than the old per-path bound would have been comfortable with
    prev = await _agent(actions, "agent:deep0012")
    for i in range(2, 30):
        suffix = "-ii" if i == 2 else f"-g{i + 38}" if i > 2 else ""
        nxt_name = f"agent:deep0012{suffix}"
        nxt = await _agent(actions, nxt_name)
        await _succeeded(actions, prev, nxt_name)
        prev = nxt
    ids += ["agent:deep0012"]
    return ids


async def test_bulk_equals_the_real_living_head_for_every_shape(actions: Actions) -> None:
    ids = await _build(actions)

    real = {a: await folds.living_head(actions.pool, a) for a in ids}
    bulk = await folds.living_heads(actions.pool, ids)

    assert bulk == real


async def test_only_forks_fall_back_to_the_real_walk(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    ids = await _build(actions)
    fallbacks: list[str] = []
    real_fn = folds.living_head

    async def counting(pool: Any, agent_id: str) -> str:
        fallbacks.append(agent_id)
        return await real_fn(pool, agent_id)

    monkeypatch.setattr(folds, "living_head", counting)

    await folds.living_heads(actions.pool, ids)

    # the one fork, and the deep chain that reaches the hop-budget guard's neighbourhood or not:
    # everything else must be answered from the bulk reads (no per-label walk at all)
    assert "agent:fork0005" in fallbacks
    assert len(fallbacks) <= 2, fallbacks


async def test_an_empty_request_reads_nothing(actions: Actions) -> None:
    assert await folds.living_heads(actions.pool, []) == {}
