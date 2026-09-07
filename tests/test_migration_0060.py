"""MIGRATION 0060 (thread 0af7b202): the three classification laws -- owner (a Seat
canonical or 'operator', nothing else, never a fold thread on failure), kind (a derived
thread is never an obligation; a kindless thread's own summary evidence_class decides
its kind), and expiry (a stale derived thread with no cites/noted_in activity closes).
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

from src.actions.core import Actions
from src.orchestrator.charter import set_charter
from src.orchestrator.migration_0060 import apply_migration_0060, plan_migration_0060
from src.orchestrator.seats import ensure_seat

NOW = datetime.now(UTC)
_SRC = "test-source"


async def _repo(actions: Actions, name: str) -> None:
    await actions.create_or_find_object("SoftwareProject", f"repo:{name}", "test")


async def _seat(actions: Actions, handle: str, *, house: str = "test") -> str:
    out = await ensure_seat(actions, house=house, handle=handle, source="test")
    return str(out["seat_id"])


async def _thread(
    actions: Actions, canonical: str, *, owner: str | None = None, repo: str | None = None,
    kind: str | None = "obligation", summary_evidence_class: str = "self_declared",
    status: str = "open", age_days: int = 0,
) -> str:
    t = await actions.create_or_find_object("Thread", canonical, _SRC)
    now = NOW
    if age_days:
        await actions.pool.execute(
            "UPDATE objects SET created_at=$2 WHERE id=$1", t,
            now - timedelta(days=age_days))
    await actions.assert_property(t, "summary", canonical, _SRC, now, 0.9,
                                  evidence_class=summary_evidence_class)
    await actions.assert_property(t, "status", status, _SRC, now, 0.9,
                                  evidence_class="self_declared")
    if kind is not None:
        await actions.assert_property(t, "kind", kind, _SRC, now, 0.9,
                                      evidence_class="self_declared")
    if owner is not None:
        await actions.assert_property(t, "owner", owner, _SRC, now, 0.9,
                                      evidence_class="self_declared")
    if repo is not None:
        from src.orchestrator.capture import link_repo
        await link_repo(actions, t, repo, now, source=_SRC)
    return canonical


async def _current_prop(actions: Actions, canonical: str, name: str) -> str | None:
    return await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a "
        "JOIN objects o ON o.id=a.object_id WHERE o.canonical=$1 AND a.name=$2 "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", canonical, name)


# ═══ (1) OWNER LAW ═══════════════════════════════════════════════════════════════════

async def test_owner_law_resolves_a_bare_handle_to_its_seat(actions: Actions) -> None:
    seat_id = await _seat(actions, "M60HandleSeat")
    t = await _thread(actions, "m60-thread-handle", owner="M60HandleSeat")

    await apply_migration_0060(actions)

    assert await _current_prop(actions, t, "owner") == seat_id


async def test_owner_law_resolves_an_empty_owner_via_the_project_coordinator(
    actions: Actions,
) -> None:
    await _repo(actions, "m60-proj-empty")
    seat_id = await _seat(actions, "M60EmptyOwnerSeat")
    await set_charter(actions, seat_id, ["m60-proj-empty"], actor="test")
    t = await _thread(actions, "m60-thread-empty-owner", owner=None, repo="m60-proj-empty")

    await apply_migration_0060(actions)

    assert await _current_prop(actions, t, "owner") == seat_id


async def test_owner_law_resolves_a_literal_project_name_owner_via_the_in_repo_link(
    actions: Actions,
) -> None:
    """Migration 0061, census 583e2669: the `repo` this migration reads for a thread
    comes ONLY from the real `in_repo` LINK (`link_repo`'s own convention) -- a `repo`
    PROPERTY assertion structurally never exists in this graph. A present-but-
    unresolvable owner (a bare project name, same as production's literal-'osiris'
    residue) must resolve through that link-derived project just as reliably as the
    empty-owner rung above."""
    await _repo(actions, "m60-proj-literal")
    seat_id = await _seat(actions, "M60LiteralOwnerSeat")
    await set_charter(actions, seat_id, ["m60-proj-literal"], actor="test")
    t = await _thread(actions, "m60-thread-literal-owner", owner="m60-proj-literal",
                      repo="m60-proj-literal")

    await apply_migration_0060(actions)

    assert await _current_prop(actions, t, "owner") == seat_id


async def test_owner_law_leaves_an_empty_owner_with_no_repo_untouched(
    actions: Actions,
) -> None:
    t = await _thread(actions, "m60-thread-empty-no-repo", owner=None, repo=None)

    await apply_migration_0060(actions)

    assert await _current_prop(actions, t, "owner") is None


async def test_owner_law_leaves_an_unresolvable_owner_untouched_never_a_fold_thread(
    actions: Actions,
) -> None:
    await _repo(actions, "m60-proj-orphan")  # real project, nobody charters it
    t = await _thread(actions, "m60-thread-unresolvable", owner="m60-proj-orphan",
                      repo="m60-proj-orphan")
    before = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Thread' AND status='active'")

    await apply_migration_0060(actions)

    after = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Thread' AND status='active'")
    assert await _current_prop(actions, t, "owner") == "m60-proj-orphan"  # untouched
    assert after == before, "migration 0060 must never mint a folding thread"


# ═══ (2) KIND LAW ═════════════════════════════════════════════════════════════════════

async def test_kind_law_a_kindless_derived_thread_becomes_finding(actions: Actions) -> None:
    t = await _thread(actions, "m60-thread-kindless-derived", kind=None,
                      summary_evidence_class="derived")

    await apply_migration_0060(actions)

    assert await _current_prop(actions, t, "kind") == "finding"


async def test_kind_law_a_kindless_self_declared_thread_becomes_task(
    actions: Actions,
) -> None:
    t = await _thread(actions, "m60-thread-kindless-self-declared", kind=None,
                      summary_evidence_class="self_declared")

    await apply_migration_0060(actions)

    assert await _current_prop(actions, t, "kind") == "task"


async def test_kind_law_a_derived_obligation_is_reclassified_off_obligation(
    actions: Actions,
) -> None:
    t = await _thread(actions, "m60-thread-derived-obligation", kind="obligation",
                      summary_evidence_class="derived")

    await apply_migration_0060(actions)

    assert await _current_prop(actions, t, "kind") == "finding"


async def test_kind_law_a_self_declared_obligation_is_left_alone(actions: Actions) -> None:
    t = await _thread(actions, "m60-thread-self-declared-obligation", kind="obligation",
                      summary_evidence_class="self_declared")

    await apply_migration_0060(actions)

    assert await _current_prop(actions, t, "kind") == "obligation"


# ═══ (3) EXPIRY ═══════════════════════════════════════════════════════════════════════

async def test_expiry_closes_a_stale_unclaimed_derived_thread(actions: Actions) -> None:
    t = await _thread(actions, "m60-thread-stale", kind="finding",
                      summary_evidence_class="derived", age_days=35)

    await apply_migration_0060(actions)

    assert await _current_prop(actions, t, "status") == "resolved"
    assert await _current_prop(actions, t, "resolved_because") == "expired unclaimed"


async def test_expiry_leaves_a_stale_thread_with_a_cites_link_open(actions: Actions) -> None:
    t = await _thread(actions, "m60-thread-stale-cited", kind="finding",
                      summary_evidence_class="derived", age_days=35)
    tid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical=$1", t)
    other = await actions.create_or_find_object("Decision", "decision:m60-citer", "test")
    await actions.create_link(other, tid, "cites", "test", NOW, 0.9,
                              evidence_class="self_declared")

    await apply_migration_0060(actions)

    assert await _current_prop(actions, t, "status") == "open"


async def test_expiry_leaves_a_fresh_derived_thread_open(actions: Actions) -> None:
    t = await _thread(actions, "m60-thread-fresh", kind="finding",
                      summary_evidence_class="derived", age_days=20)

    await apply_migration_0060(actions)

    assert await _current_prop(actions, t, "status") == "open"


async def test_expiry_leaves_a_stale_self_declared_thread_open(actions: Actions) -> None:
    t = await _thread(actions, "m60-thread-stale-self-declared", kind="task",
                      summary_evidence_class="self_declared", age_days=35)

    await apply_migration_0060(actions)

    assert await _current_prop(actions, t, "status") == "open"


# ═══ end-to-end plan sanity ═══════════════════════════════════════════════════════════

async def test_plan_reports_every_bucket(actions: Actions) -> None:
    seat_id = await _seat(actions, "M60PlanSeat")
    await _thread(actions, "m60-plan-owner", owner="M60PlanSeat")
    await _thread(actions, "m60-plan-kindless", kind=None, summary_evidence_class="derived")
    await _thread(actions, "m60-plan-expiring", kind="finding",
                  summary_evidence_class="derived", age_days=40)

    plan = await plan_migration_0060(actions.pool)

    assert any(e["new_owner"] == seat_id for e in plan["owner_resolved"])
    assert any(e["thread"] == "m60-plan-kindless" for e in plan["kind_assigned"])
    assert any(e["thread"] == "m60-plan-expiring" for e in plan["to_expire"])
    assert plan["threads_scanned"] >= 3
