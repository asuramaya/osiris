"""osiris — the console-script (task #69, ruling 45b074bf). The pure decision layer
(`match_session`, `resolve_model`, `_house_tag`) is fully covered with no IO at all; the async
commands are tested with a REAL pool (this repo's own "never mock the DB" rule) but a FAKE
`manager` callable — the manager daemon itself is test_manager.py's territory, and actually
spawning a claude process is exactly what these tests must never risk doing by accident.
"""
from __future__ import annotations

from typing import Any

from src.actions.core import Actions
from src.cli import (
    cmd_attach,
    cmd_launch,
    cmd_seed,
    match_session,
    resolve_model,
)
from src.orchestrator.seats import ensure_seat

# --- match_session: pure ----------------------------------------------------------------------

def test_match_session_exact_match() -> None:
    sessions = [{"name": "[OS] imhotep", "alive": True}, {"name": "[OS] thoth", "alive": True}]
    name, candidates = match_session(sessions, "imhotep")
    assert name == "[OS] imhotep"
    assert candidates == []


def test_match_session_is_case_insensitive() -> None:
    sessions = [{"name": "[OS] Imhotep", "alive": True}]
    name, _ = match_session(sessions, "IMHOTEP")
    assert name == "[OS] Imhotep"


def test_match_session_no_match_lists_live_candidates_as_empty() -> None:
    sessions = [{"name": "[OS] thoth", "alive": True}]
    name, candidates = match_session(sessions, "nobody")
    assert name is None
    assert candidates == []


def test_match_session_loose_substring_match_when_unambiguous() -> None:
    sessions = [{"name": "[OS] imhotep", "alive": True}]
    name, _ = match_session(sessions, "imho")
    assert name == "[OS] imhotep"


def test_match_session_ambiguous_returns_no_name_but_names_candidates() -> None:
    sessions = [{"name": "[OS] imhotep", "alive": True}, {"name": "[AL] imhotep", "alive": True}]
    name, candidates = match_session(sessions, "imhotep")
    assert name is None
    assert set(candidates) == {"[OS] imhotep", "[AL] imhotep"}


def test_match_session_ignores_malformed_rows() -> None:
    sessions: list[dict[str, Any]] = [{"alive": True}, "not-a-dict", {"name": 5}]  # type: ignore[list-item]
    name, candidates = match_session(sessions, "imhotep")
    assert name is None
    assert candidates == []


def test_match_session_blank_handle_matches_nothing() -> None:
    sessions = [{"name": "[OS] imhotep", "alive": True}]
    assert match_session(sessions, "  ") == (None, [])


# --- resolve_model: pure -----------------------------------------------------------------------

def test_resolve_model_explicit_wins_outright() -> None:
    assert resolve_model("claude-opus-5", "claude-sonnet-5", "claude-haiku-4-5") == "claude-opus-5"


def test_resolve_model_falls_to_seat_intended_when_no_explicit() -> None:
    assert resolve_model(None, "claude-sonnet-5", "claude-haiku-4-5") == "claude-sonnet-5"


def test_resolve_model_falls_to_wake_default_when_neither_explicit_nor_seat() -> None:
    assert resolve_model(None, None, "claude-haiku-4-5") == "claude-haiku-4-5"


def test_resolve_model_none_when_nothing_is_set() -> None:
    assert resolve_model(None, None, None) is None


def test_resolve_model_treats_empty_strings_as_unset() -> None:
    """An empty-string seat stamp (never asserted) must fall through, not win as ''."""
    assert resolve_model("", "", "claude-haiku-4-5") == "claude-haiku-4-5"


# --- cmd_attach: fake manager, no real daemon ---------------------------------------------------

async def test_cmd_attach_reports_a_dark_daemon_honestly() -> None:
    async def _dark(req: dict[str, Any]) -> dict[str, Any]:
        raise OSError("no such file or directory")

    assert await cmd_attach("imhotep", manager=_dark) == 1


async def test_cmd_attach_reports_no_match_honestly() -> None:
    async def _roster(req: dict[str, Any]) -> dict[str, Any]:
        return {"sessions": [{"name": "[OS] thoth", "alive": True}]}

    assert await cmd_attach("nobody-here", manager=_roster) == 1


async def test_cmd_attach_dispatches_to_the_resolved_name(monkeypatch: Any) -> None:
    async def _roster(req: dict[str, Any]) -> dict[str, Any]:
        return {"sessions": [{"name": "[OS] imhotep", "alive": True}]}

    calls: list[list[str]] = []

    def _fake_main(argv: list[str]) -> int:
        calls.append(argv)
        return 0

    monkeypatch.setattr("src.manager.attach.main", _fake_main)
    assert await cmd_attach("imhotep", manager=_roster) == 0
    assert calls == [["[OS] imhotep"]]


# --- cmd_seed: a real pool (never mocked), just injected instead of self-created ----------------

async def test_cmd_seed_compositions_only_seeds_a_real_pool(actions: Actions) -> None:
    assert await cmd_seed(compositions_only=True, pool=actions.pool) == 0
    seeded = await actions.pool.fetchval("SELECT count(*) FROM compositions")
    assert seeded > 0


# --- cmd_launch: a real pool for seat facts, a fake manager so nothing is ever really spawned ---

async def test_cmd_launch_unknown_handle_is_honest(actions: Actions) -> None:
    async def _unreachable(req: dict[str, Any]) -> dict[str, Any]:
        raise AssertionError("should never be called — the seat lookup fails first")

    out = await cmd_launch("no-such-handle-at-all", model=None, pool=actions.pool,
                           manager=_unreachable)
    assert out == 1


async def test_cmd_launch_ambiguous_handle_is_honest(actions: Actions) -> None:
    await ensure_seat(actions, house="alfred", handle="twin", source="test")
    await ensure_seat(actions, house="osiris", handle="twin", source="test")

    async def _unreachable(req: dict[str, Any]) -> dict[str, Any]:
        raise AssertionError("should never be called — the ambiguity check fails first")

    assert await cmd_launch("twin", model=None, pool=actions.pool, manager=_unreachable) == 1


async def test_cmd_launch_no_anchor_cwd_is_honest(actions: Actions) -> None:
    await ensure_seat(actions, house="osiris", handle="roomless", source="test")

    async def _unreachable(req: dict[str, Any]) -> dict[str, Any]:
        raise AssertionError("should never be called — the office check fails first")

    assert await cmd_launch("roomless", model=None, pool=actions.pool,
                            manager=_unreachable) == 1


async def test_cmd_launch_returns_the_existing_window_instead_of_twinning(
    actions: Actions,
) -> None:
    await ensure_seat(actions, house="osiris", handle="already-live",
                      anchor_cwd="/home/x/.osiris/seats/already-live", source="test")

    calls: list[dict[str, Any]] = []

    async def _fake(req: dict[str, Any]) -> dict[str, Any]:
        calls.append(req)
        return {"sessions": [{"name": "[OS] already-live", "alive": True}]}

    out = await cmd_launch("already-live", model=None, pool=actions.pool, manager=_fake)
    assert out == 0
    # only the roster was ever asked for — pty_spawn was never reached
    assert [c["op"] for c in calls] == ["pty_list"]


async def test_cmd_launch_reports_a_dark_manager_after_facts_resolve(actions: Actions) -> None:
    await ensure_seat(actions, house="osiris", handle="darkbody",
                      anchor_cwd="/home/x/.osiris/seats/darkbody", source="test")

    async def _dark(req: dict[str, Any]) -> dict[str, Any]:
        raise TimeoutError("no reply")

    assert await cmd_launch("darkbody", model=None, pool=actions.pool, manager=_dark) == 1


async def test_cmd_launch_spawns_and_confirms_an_honest_mount(actions: Actions) -> None:
    """The full honest path: pty_list (empty), pty_spawn (accepted), then poll — a matching
    agent_mounts row inserted BEFORE the call gives the confirmation an immediate hit, so the
    test needs no real sleep-bound wait to prove the receipt names what actually mounted."""
    seat = await ensure_seat(actions, house="osiris", handle="freshbody",
                             anchor_cwd="/home/x/.osiris/seats/freshbody", source="test")
    await actions.pool.execute(
        "INSERT INTO agent_mounts (job_dir, agent_id, project, cwd, model, last_seen) "
        "VALUES ($1, $2, $3, $4, $5, now())",
        f"/tmp/{seat['seat_id']}", "agent:freshbody-i", "osiris",
        "/home/x/.osiris/seats/freshbody", "claude-sonnet-5")

    calls: list[dict[str, Any]] = []

    async def _fake(req: dict[str, Any]) -> dict[str, Any]:
        calls.append(req)
        if req["op"] == "pty_list":
            return {"sessions": []}
        assert req["op"] == "pty_spawn"
        return {"spawned": req["name"]}

    out = await cmd_launch("freshbody", model="claude-sonnet-5", pool=actions.pool,
                           manager=_fake)
    assert out == 0
    ops = [c["op"] for c in calls]
    assert ops[0] == "pty_list"
    assert "pty_spawn" in ops
    spawn_req = next(c for c in calls if c["op"] == "pty_spawn")
    assert spawn_req["argv"] == ["claude", "--model", "claude-sonnet-5"]
    assert spawn_req["cwd"] == "/home/x/.osiris/seats/freshbody"


async def test_cmd_launch_names_a_model_mismatch_honestly(actions: Actions) -> None:
    """thread 20e4feb6's own bug class, caught by the CLI's own receipt: requested one model,
    the body that actually mounted reports a different one."""
    await ensure_seat(actions, house="osiris", handle="wrongmodel",
                      anchor_cwd="/home/x/.osiris/seats/wrongmodel", source="test")
    await actions.pool.execute(
        "INSERT INTO agent_mounts (job_dir, agent_id, project, cwd, model, last_seen) "
        "VALUES ($1, $2, $3, $4, $5, now())",
        "/tmp/wrongmodel", "agent:wrongmodel-i", "osiris",
        "/home/x/.osiris/seats/wrongmodel", "claude-fable-5")

    async def _fake(req: dict[str, Any]) -> dict[str, Any]:
        if req["op"] == "pty_list":
            return {"sessions": []}
        return {"spawned": req["name"]}

    # capture stdout to assert the mismatch line was actually printed
    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_launch("wrongmodel", model="claude-sonnet-5", pool=actions.pool,
                               manager=_fake)
    assert out == 0
    assert "MISMATCH" in buf.getvalue()
    assert "claude-fable-5" in buf.getvalue()
