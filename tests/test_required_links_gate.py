"""THE DECLARE-OR-REFUSE GATE (task #189, ruling 5ac06206, decision 7ea187b9). Two
layers: the mechanism itself (`capture._enforce_required_links`, proven against a
throwaway test-only Type so nothing here touches the real Decision/Thread catalog
entries other tests may rely on), and the real wiring through record_decision/
open_thread (proven by temporarily declaring a requirement on the REAL types, restored
in a finally so no other test in the suite ever sees it)."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from src.actions.core import Actions
from src.ontology import catalog
from src.ontology.catalog import ensure_type
from src.orchestrator import capture


@pytest.fixture(autouse=True)
def _clear_cache() -> None:
    catalog._cache.clear()


async def _mint_bare(actions: Actions, type_name: str) -> uuid.UUID:
    return await actions.create_or_find_object(
        type_name, f"{type_name.lower()}:{uuid.uuid4()}", "test")


# --- the mechanism, isolated on a throwaway type -----------------------------------

async def test_enforce_required_links_refuses_and_rolls_back_when_nothing_satisfies(
    actions: Actions,
) -> None:
    await ensure_type(actions, name="GateWidget", kind="object", actor="test",
                      required_link_kinds=["repo"])
    with pytest.raises(ValueError, match="repo"):
        async with actions.atomic() as a:
            oid = await _mint_bare(a, "GateWidget")
            await capture._enforce_required_links(
                a, oid, "GateWidget", kinds_in_scope=("repo",),
                unlinked_because=None, source="test", observed=datetime.now(UTC))
    # THE ROLLBACK, not just the raise: nothing this transaction touched persisted —
    # a real refuse-at-door, never a post-hoc alarm on an object already committed.
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='GateWidget'") == 0


async def test_enforce_required_links_passes_when_a_self_declared_link_exists(
    actions: Actions,
) -> None:
    await ensure_type(actions, name="GateWidget2", kind="object", actor="test",
                      required_link_kinds=["repo"])
    async with actions.atomic() as a:
        oid = await _mint_bare(a, "GateWidget2")
        other = await _mint_bare(a, "SoftwareProject")
        await a.create_link(oid, other, "in_repo", "test", datetime.now(UTC), 0.9,
                            evidence_class="self_declared")
        await capture._enforce_required_links(
            a, oid, "GateWidget2", kinds_in_scope=("repo",),
            unlinked_because=None, source="test", observed=datetime.now(UTC))
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE id=$1", oid) == 1


async def test_enforce_required_links_a_direct_observation_link_never_satisfies_it(
    actions: Actions,
) -> None:
    """Seshat's own instruction (msg 5790): a mount-derived/observed link must never be
    laundered into satisfying a caller's own declaration requirement."""
    await ensure_type(actions, name="GateWidget3", kind="object", actor="test",
                      required_link_kinds=["repo"])
    with pytest.raises(ValueError, match="repo"):
        async with actions.atomic() as a:
            oid = await _mint_bare(a, "GateWidget3")
            other = await _mint_bare(a, "SoftwareProject")
            await a.create_link(oid, other, "in_repo", "test", datetime.now(UTC), 0.6,
                                evidence_class="direct_observation")
            await capture._enforce_required_links(
                a, oid, "GateWidget3", kinds_in_scope=("repo",),
                unlinked_because=None, source="test", observed=datetime.now(UTC))


async def test_enforce_required_links_unlinked_because_hatch_is_recorded_and_countable(
    actions: Actions,
) -> None:
    await ensure_type(actions, name="GateWidget4", kind="object", actor="test",
                      required_link_kinds=["repo"])
    async with actions.atomic() as a:
        oid = await _mint_bare(a, "GateWidget4")
        await capture._enforce_required_links(
            a, oid, "GateWidget4", kinds_in_scope=("repo",),
            unlinked_because="no project exists for this yet", source="test",
            observed=datetime.now(UTC))
    recorded = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='unlinked_because'", oid)
    assert recorded == "no project exists for this yet"


async def test_enforce_required_links_never_checks_an_unenforced_type(
    actions: Actions,
) -> None:
    """Negative control: a type that declares NOTHING (the real, current state of
    every shipped type in this pass) is never touched by this gate at all."""
    await ensure_type(actions, name="GateWidget5", kind="object", actor="test")
    async with actions.atomic() as a:
        oid = await _mint_bare(a, "GateWidget5")
        await capture._enforce_required_links(
            a, oid, "GateWidget5", kinds_in_scope=("repo", "grounds", "resolves"),
            unlinked_because=None, source="test", observed=datetime.now(UTC))
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE id=$1", oid) == 1


async def test_enforce_required_links_ignores_a_kind_outside_this_doors_scope(
    actions: Actions,
) -> None:
    """A type requiring 'grounds' but checked by a door whose kinds_in_scope is only
    ('repo',) (open_thread's own scope) must not refuse over a kind it cannot even
    attest to — a different door's problem, not this call's."""
    await ensure_type(actions, name="GateWidget6", kind="object", actor="test",
                      required_link_kinds=["grounds"])
    async with actions.atomic() as a:
        oid = await _mint_bare(a, "GateWidget6")
        await capture._enforce_required_links(
            a, oid, "GateWidget6", kinds_in_scope=("repo",),
            unlinked_because=None, source="test", observed=datetime.now(UTC))
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE id=$1", oid) == 1


# --- the real wiring, proven on the actual Decision/Thread types, restored after ---

@pytest.fixture
async def decision_requires_repo(actions: Actions):
    await ensure_type(actions, name="Decision", kind="object", actor="test",
                      required_link_kinds=["repo"])
    catalog._cache.clear()
    try:
        yield
    finally:
        await ensure_type(actions, name="Decision", kind="object", actor="test",
                          required_link_kinds=[])
        catalog._cache.clear()


@pytest.fixture
async def thread_requires_repo(actions: Actions):
    await ensure_type(actions, name="Thread", kind="object", actor="test",
                      required_link_kinds=["repo"])
    catalog._cache.clear()
    try:
        yield
    finally:
        await ensure_type(actions, name="Thread", kind="object", actor="test",
                          required_link_kinds=[])
        catalog._cache.clear()


async def test_record_decision_refuses_when_declared_and_nothing_links_it(
    actions: Actions, decision_requires_repo: None,
) -> None:
    with pytest.raises(ValueError, match="unlinked_because"):
        await capture.record_decision(actions, "an undeclared, unlinked ruling")
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Decision'") == 0


async def test_record_decision_passes_when_repo_is_caller_declared(
    actions: Actions, decision_requires_repo: None,
) -> None:
    d = await capture.record_decision(actions, "a properly linked ruling",
                                      repo="osiris")
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE id=$1", d) == 1


async def test_record_decision_extension_link_only_write_gets_a_distinct_machine_reason(
    actions: Actions, decision_requires_repo: None,
) -> None:
    """Thoth's condition 2 (msg 5802/5811): a Decision whose ONLY requested connectivity
    is an extension-link param (implements=, here) mints outside record_decision's own
    atomic block and can't satisfy the gate at its own commit — so it falls into the
    hatch, but with a MACHINE-SET reason, never indistinguishable from a genuinely
    standalone caller-typed one. Exercises the real mcp_server.py wrapper, the layer
    that actually knows whether an extension-link param was requested."""
    from src import mcp_server as srv
    from src.mcp_server import _EXTENSION_LINK_PENDING_REASON

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        parent = await srv.record_decision(
            "a properly linked parent ruling, repo declared", repo="osiris")
        child = await srv.record_decision(
            "a decision reached only via implements, no repo/grounds/resolves",
            implements=parent["id"])
    finally:
        srv._pool = saved_pool
    assert "error" not in child
    recorded = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='unlinked_because'", uuid.UUID(child["id"]))
    assert recorded == _EXTENSION_LINK_PENDING_REASON


async def test_record_decision_extension_link_present_but_gate_already_satisfied_skips_hatch(
    actions: Actions, decision_requires_repo: None,
) -> None:
    """The other half of condition 2: a caller who ALSO gave a satisfying repo= must
    NEVER see the machine-set reason land, even though an extension-link param was
    requested too — the hatch count would otherwise be inflated by writes that never
    needed it."""
    from src import mcp_server as srv

    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        parent = await srv.record_decision(
            "another properly linked parent ruling", repo="osiris")
        child = await srv.record_decision(
            "a decision with BOTH repo and implements", repo="osiris",
            implements=parent["id"])
    finally:
        srv._pool = saved_pool
    assert "error" not in child
    recorded = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='unlinked_because'", uuid.UUID(child["id"]))
    assert recorded is None


async def test_record_decision_mount_defaulted_repo_does_not_satisfy_the_gate(
    actions: Actions, decision_requires_repo: None,
) -> None:
    """The exact laundering Seshat's tier fix (6c3c647) exists to prevent: a
    server-observed, mount-defaulted repo= must not count as a caller's declaration."""
    with pytest.raises(ValueError, match="unlinked_because"):
        await capture.record_decision(
            actions, "a mount-defaulted-only ruling", repo="osiris",
            repo_evidence_class="direct_observation")


async def test_record_decision_unlinked_because_hatch_passes_and_is_recorded(
    actions: Actions, decision_requires_repo: None,
) -> None:
    d = await capture.record_decision(
        actions, "a deliberately unlinked ruling",
        unlinked_because="no project exists to file this under yet")
    recorded = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='unlinked_because'", d)
    assert recorded == "no project exists to file this under yet"


async def test_open_thread_refuses_when_declared_and_nothing_links_it(
    actions: Actions, thread_requires_repo: None,
) -> None:
    with pytest.raises(ValueError, match="unlinked_because"):
        await capture.open_thread(actions, "an undeclared, unlinked thread")
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Thread'") == 0


async def test_open_thread_passes_when_repo_is_caller_declared(
    actions: Actions, thread_requires_repo: None,
) -> None:
    t = await capture.open_thread(actions, "a properly linked thread", repo="osiris")
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE id=$1", t) == 1


# --- negative control: the suite's own everyday callers must stay green ------------

async def test_record_decision_with_no_declared_requirement_is_unaffected(
    actions: Actions,
) -> None:
    """No fixture here — Decision's REAL, shipped required_link_kinds is empty in this
    pass. Every existing caller of record_decision (thousands, fleet-wide) must keep
    working exactly as before; this is the negative control proving the gate is a
    no-op until Khnum's content pass turns it on."""
    d = await capture.record_decision(actions, "an ordinary unlinked decision, today")
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE id=$1", d) == 1
