"""roadmap(project) — open + resolved/retracted Threads, grouped status→owner (thread
521ae613a6f4 / d56e7073, Thoth's go msg 1227: v1 status→owner only, no `arc` yet)."""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.capture import open_thread, resolve_thread
from src.orchestrator.roadmap import roadmap

NOW = datetime(2026, 7, 25, tzinfo=UTC)


async def _project(actions: Actions, name: str) -> None:
    proj = await actions.create_or_find_object("SoftwareProject", f"repo:{name}", "test")
    await actions.assert_property(proj, "name", name, "test", NOW, 0.9,
                                  evidence_class="self_declared")


async def test_roadmap_refuses_an_unknown_project(actions: Actions) -> None:
    out = await roadmap(actions.pool, "no-such-project")
    assert "error" in out and "no such project" in out["error"]


async def test_roadmap_groups_open_threads_by_owner(actions: Actions) -> None:
    await _project(actions, "rmtest")
    await open_thread(actions, "an unowned duty", repo="rmtest",
                      kind="obligation", source="agent:me")
    await open_thread(actions, "an operator blocker", repo="rmtest",
                      kind="obligation", owner="operator", source="agent:me")

    out = await roadmap(actions.pool, "rmtest")

    open_status = next(s for s in out["statuses"] if s["status"] == "open")
    owners = {o["owner"] for o in open_status["owners"]}
    assert owners == {"unowned", "operator"}
    unowned = next(o for o in open_status["owners"] if o["owner"] == "unowned")
    assert unowned["threads"][0]["summary"] == "an unowned duty"
    # obligation-first / whose-move ordering (rank_open_threads): unowned ("mine to act")
    # ranks above the operator group — the owner GROUPS inherit that same order
    assert [o["owner"] for o in open_status["owners"]][0] == "unowned"


async def test_roadmap_excludes_untouched_miner_echoes(actions: Actions) -> None:
    """Same discipline the wall already enforces (ruling 61c1b20d) — roadmap.py must not
    reintroduce the guessed-duty snowball by bypassing open_thread_wall's echo filter."""
    await _project(actions, "rmtest")
    t = await actions.create_or_find_object("Thread", "thread:mined-echo", "session-miner")
    for n, v in (("summary", "a guessed duty nobody touched"), ("status", "open"),
                 ("kind", "obligation")):
        await actions.assert_property(t, n, v, "session-miner", NOW, 0.4,
                                      evidence_class="derived")
    proj = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:rmtest'")
    await actions.create_link(t, proj, "in_repo", "session-miner", NOW, 0.4,
                              evidence_class="derived")

    out = await roadmap(actions.pool, "rmtest")

    open_status = next((s for s in out["statuses"] if s["status"] == "open"), None)
    assert open_status is None or all(
        "guessed duty" not in th["summary"]
        for o in open_status["owners"] for th in o["threads"])


async def test_roadmap_includes_resolved_and_retracted_grouped_separately(
    actions: Actions,
) -> None:
    await _project(actions, "rmtest")
    resolved_id = await open_thread(actions, "shipped work", repo="rmtest",
                                    kind="obligation", owner="agent:builder",
                                    source="agent:me")
    await resolve_thread(actions, str(resolved_id), because="done", source="agent:me")
    retracted_id = await open_thread(actions, "turned out to be nothing", repo="rmtest",
                                     owner="agent:builder", source="agent:me")
    await actions.assert_property(retracted_id, "status", "retracted", "agent:me", NOW, 0.9,
                                  evidence_class="self_declared")

    out = await roadmap(actions.pool, "rmtest")

    by_status = {s["status"]: s for s in out["statuses"]}
    assert set(by_status) == {"resolved", "retracted"}       # no open threads left
    resolved_threads = by_status["resolved"]["owners"][0]["threads"]
    assert resolved_threads[0]["summary"] == "shipped work"
    assert resolved_threads[0]["owner"] == "agent:builder"
    retracted_threads = by_status["retracted"]["owners"][0]["threads"]
    assert retracted_threads[0]["summary"] == "turned out to be nothing"


async def test_roadmap_status_groups_render_in_a_fixed_order(actions: Actions) -> None:
    await _project(actions, "rmtest")
    await open_thread(actions, "still open", repo="rmtest", source="agent:me")
    shipped = await open_thread(actions, "shipped", repo="rmtest", source="agent:me")
    await resolve_thread(actions, str(shipped), source="agent:me")

    out = await roadmap(actions.pool, "rmtest")

    assert [s["status"] for s in out["statuses"]] == ["open", "resolved"]
    assert out["note"].startswith("v1: status")
