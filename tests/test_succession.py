"""succession — the full generation-chain walk (task #64, ruling ad19a779). NOT lineage.py
(that module is the swarm/sub-agent spawn-tree reconstruction, a different concept entirely
— caught by Read-before-Write before this module's own name clobbered it)."""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.succession import succession_chain

NOW = datetime(2026, 7, 27, tzinfo=UTC)
_SD = "self_declared"


async def _agent(actions: Actions, canonical: str, *, generation: str, minted_because: str,
                 succeeded_from: str | None = None, wrote: bool = True,
                 session: str | None = None) -> None:
    a = await actions.create_or_find_object("Agent", canonical, "test")
    await actions.assert_property(a, "seat_generation", generation, "test", NOW, 0.9,
                                  evidence_class=_SD)
    await actions.assert_property(a, "minted_because", minted_because, "test", NOW, 0.9,
                                  evidence_class=_SD)
    if succeeded_from:
        await actions.assert_property(a, "succeeded_from", succeeded_from, "test", NOW, 0.9,
                                      evidence_class=_SD)
    if wrote:
        await actions.assert_property(a, "note", "did something real", "test", NOW, 0.9,
                                      evidence_class=_SD)
    if session:
        await actions.assert_property(a, "session", session, "test", NOW, 0.9,
                                      evidence_class=_SD)


async def test_walks_the_whole_chain_one_entry_per_hop(actions: Actions) -> None:
    await _agent(actions, "agent:root-i", generation="1", minted_because="bootstrap",
                wrote=True)
    await _agent(actions, "agent:root-ii", generation="2", minted_because="compaction",
                succeeded_from="agent:root-i", wrote=True)
    await _agent(actions, "agent:root-iii", generation="3", minted_because="compaction",
                succeeded_from="agent:root-ii", wrote=False)  # a zero-write phantom

    chain = await succession_chain(actions.pool, "agent:root-iii")
    assert [c["agent_id"] for c in chain] == [
        "agent:root-iii", "agent:root-ii", "agent:root-i"]
    assert chain[0]["wrote_anything"] is False
    assert chain[1]["minted_because"] == "compaction"
    assert chain[2]["generation"] == "1"


async def test_session_rides_along_per_hop(actions: Actions) -> None:
    """7fa4b599's own named additive step (2026-08-04): the walker already read three
    sibling properties off the identical current_assertions row — session is the fourth,
    the transcript filename's own stem, asserted at mount() same as the other three. A
    generation that never mounted (no session asserted) reads None, not a crash or a
    guess."""
    await _agent(actions, "agent:sess-i", generation="1", minted_because="bootstrap",
                session="11111111-aaaa-4bbb-8ccc-000000000001")
    await _agent(actions, "agent:sess-ii", generation="2", minted_because="compaction",
                succeeded_from="agent:sess-i", session="22222222-aaaa-4bbb-8ccc-000000000002")
    await _agent(actions, "agent:sess-iii", generation="3", minted_because="compaction",
                succeeded_from="agent:sess-ii")  # no session — never mounted

    chain = await succession_chain(actions.pool, "agent:sess-iii")
    assert [c["session"] for c in chain] == [
        None, "22222222-aaaa-4bbb-8ccc-000000000002", "11111111-aaaa-4bbb-8ccc-000000000001"]


async def test_stops_at_a_root_with_no_predecessor(actions: Actions) -> None:
    await _agent(actions, "agent:lonely-i", generation="1", minted_because="bootstrap")
    chain = await succession_chain(actions.pool, "agent:lonely-i")
    assert len(chain) == 1
    assert chain[0]["agent_id"] == "agent:lonely-i"


async def test_max_hops_bounds_the_walk_never_widens(actions: Actions) -> None:
    prev = None
    for i in range(6):
        canon = f"agent:deep-{i}"
        await _agent(actions, canon, generation=str(i), minted_because="compaction",
                    succeeded_from=prev)
        prev = canon
    chain = await succession_chain(actions.pool, prev, max_hops=3)
    assert len(chain) == 3
    assert chain[0]["agent_id"] == "agent:deep-5"


async def test_ref_accepts_a_short_id_like_any_other_verb(actions: Actions) -> None:
    a = await actions.create_or_find_object("Agent", "agent:shorty", "test")
    await actions.assert_property(a, "seat_generation", "1", "test", NOW, 0.9,
                                  evidence_class=_SD)
    chain = await succession_chain(actions.pool, str(a)[:8])
    assert chain and chain[0]["agent_id"] == "agent:shorty"


async def test_unknown_ref_is_an_empty_chain_not_an_error(actions: Actions) -> None:
    assert await succession_chain(actions.pool, "agent:never-existed-xyz") == []


async def test_the_mcp_tool_wraps_the_core_walk(actions: Actions) -> None:
    """srv._pool swap (mirrors test_describe.py's own pattern) — proves the ACTUAL MCP tool,
    not just the core function."""
    from src import mcp_server as srv

    await _agent(actions, "agent:mcp-i", generation="1", minted_because="bootstrap")
    await _agent(actions, "agent:mcp-ii", generation="2", minted_because="compaction",
                succeeded_from="agent:mcp-i")

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.succession_chain("agent:mcp-ii")
        missing = await srv.succession_chain("agent:never-existed-xyz")
    finally:
        srv._pool = saved_pool
    assert out["ref"] == "agent:mcp-ii"
    assert [c["agent_id"] for c in out["chain"]] == ["agent:mcp-ii", "agent:mcp-i"]
    assert missing == {"error": "no agent matches 'agent:never-existed-xyz'"}
