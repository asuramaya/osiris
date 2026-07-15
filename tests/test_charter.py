"""THE CHARTER — a house is what a seat RULES, not where it sits (Phase 1 §4.1, `dd47c1da`).

set_charter mints `governs` links (Agent → SoftwareProject); a repo dropped from a later call
is healed by a COMPENSATING EVENT (`links.valid_until`), never deleted — these tests prove the
mint, the read-back, and the heal.
"""
from __future__ import annotations

from src.actions.core import Actions
from src.orchestrator.charter import charter_of, set_charter


async def _agent(actions: Actions, canonical: str) -> None:
    await actions.create_or_find_object("Agent", canonical, canonical)


async def test_set_charter_mints_governs_links(actions: Actions) -> None:
    await _agent(actions, "agent:steward")
    out = await set_charter(actions, "agent:steward", ["osiris", "bytebye"])
    assert out["charter"] == ["bytebye", "osiris"]  # sorted
    assert out["added"] == ["bytebye", "osiris"]
    assert out["removed"] == []
    assert await charter_of(actions.pool, "agent:steward") == ["bytebye", "osiris"]
    # real graph edges, not a property — governs, Agent -> SoftwareProject, active
    links = await actions.pool.fetch(
        "SELECT p.canonical, l.valid_until FROM links l "
        "JOIN objects a ON a.id=l.from_id AND a.canonical='agent:steward' "
        "JOIN objects p ON p.id=l.to_id WHERE l.type='governs' ORDER BY p.canonical")
    assert [r["canonical"] for r in links] == ["repo:bytebye", "repo:osiris"]
    assert all(r["valid_until"] is None for r in links)


async def test_set_charter_is_idempotent(actions: Actions) -> None:
    await _agent(actions, "agent:steward2")
    await set_charter(actions, "agent:steward2", ["a", "b"])
    again = await set_charter(actions, "agent:steward2", ["a", "b"])
    assert again == {"agent": "agent:steward2", "charter": ["a", "b"], "added": [], "removed": []}
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects a ON a.id=l.from_id "
        "WHERE a.canonical='agent:steward2' AND l.type='governs'")
    assert n == 2  # no duplicate mint on a no-op call


async def test_set_charter_removal_heals_by_compensating_event(actions: Actions) -> None:
    """A repo dropped from the charter is healed (valid_until stamped), never deleted — the
    row survives, readable, exactly like every other retraction in this graph."""
    await _agent(actions, "agent:steward3")
    await set_charter(actions, "agent:steward3", ["osiris", "sibling-two", "bytebye"])
    amend = await set_charter(actions, "agent:steward3", ["osiris", "bytebye"])
    assert amend["added"] == []
    assert amend["removed"] == ["sibling-two"]
    assert await charter_of(actions.pool, "agent:steward3") == ["bytebye", "osiris"]
    # the dropped repo's link STILL EXISTS — healed, not deleted
    row = await actions.pool.fetchrow(
        "SELECT l.valid_until FROM links l "
        "JOIN objects a ON a.id=l.from_id AND a.canonical='agent:steward3' "
        "JOIN objects p ON p.id=l.to_id AND p.canonical='repo:sibling-two' "
        "WHERE l.type='governs'")
    assert row is not None and row["valid_until"] is not None
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects a ON a.id=l.from_id "
        "WHERE a.canonical='agent:steward3' AND l.type='governs'")
    assert n == 3  # nothing deleted — all three rows remain


async def test_set_charter_readd_after_removal_mints_a_fresh_link(actions: Actions) -> None:
    """A repo that left the charter and returns gets a NEW governs link (a fresh grant), not a
    resurrection of the healed one — the old row's heal stays true forever."""
    await _agent(actions, "agent:steward4")
    await set_charter(actions, "agent:steward4", ["osiris"])
    await set_charter(actions, "agent:steward4", [])  # drop everything
    assert await charter_of(actions.pool, "agent:steward4") == []
    back = await set_charter(actions, "agent:steward4", ["osiris"])
    assert back["added"] == ["osiris"] and back["removed"] == []
    assert await charter_of(actions.pool, "agent:steward4") == ["osiris"]
    n = await actions.pool.fetchval(
        "SELECT count(*) FROM links l JOIN objects a ON a.id=l.from_id "
        "WHERE a.canonical='agent:steward4' AND l.type='governs'")
    assert n == 2  # the healed grant + the fresh one, both on the record


async def test_charter_of_is_empty_for_an_unchartered_seat(actions: Actions) -> None:
    await _agent(actions, "agent:plain")
    assert await charter_of(actions.pool, "agent:plain") == []


def test_schema_declares_governs() -> None:
    from src.ontology.schema import LINK_TYPES

    assert "governs" in LINK_TYPES
    assert LINK_TYPES["governs"].domain == ("Agent",)
    assert LINK_TYPES["governs"].range == ("SoftwareProject",)
