"""osiris — the console-script (task #69, ruling 45b074bf). The pure decision layer
(`match_session`, `resolve_model`, `_house_tag`) is fully covered with no IO at all; the async
commands are tested with a REAL pool (this repo's own "never mock the DB" rule) but a FAKE
`manager` callable — the manager daemon itself is test_manager.py's territory, and actually
spawning a claude process is exactly what these tests must never risk doing by accident.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from src.actions.core import Actions
from src.cli import (
    DEPLOY_UNITS,
    _apply_pending_migrations,
    _collapse_resume_log,
    _composition_gaps,
    _find_repo_root,
    _real_install_user_units,
    _synthetic_automount_probe,
    _wait_for_health,
    _wait_for_smoke,
    alembic_gap_note,
    cmd_amend_decision,
    cmd_amend_practice,
    cmd_annotate_thread,
    cmd_attach,
    cmd_boot_status,
    cmd_bootstrap,
    cmd_charter_for,
    cmd_correct_pin_value,
    cmd_deploy,
    cmd_desk,
    cmd_fold_project,
    cmd_launch,
    cmd_merge,
    cmd_migrate,
    cmd_mint_seat,
    cmd_new,
    cmd_rebind_seat,
    cmd_rematerialize,
    cmd_resume,
    cmd_retention,
    cmd_seed,
    cmd_show,
    cmd_smoke_chaos,
    cmd_unmerge,
    commit_deployed_notes,
    composition_drift_notes,
    composition_gap_notes,
    composition_room_gap_notes,
    diff_tool_lists,
    dirty_tracked_src_files,
    main,
    match_session,
    oneshot_deployed_scripts,
    resolve_model,
    user_unit_sources,
)
from src.config.settings import Settings
from src.orchestrator.seats import bind_holder, ensure_seat

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


def test_collapse_resume_log_passes_through_all_distinct_entries() -> None:
    log = ["gen 3: minted but never mounted, no session to check",
           "gen 2 (session aaaaaaaa, 2MB): resumable, 1 hop(s) back"]
    assert _collapse_resume_log(log) == "; ".join(log)


def test_collapse_resume_log_collapses_a_run_sharing_one_session() -> None:
    """#153 live specimen: several generations sharing a session must report ONCE,
    naming the generation range and a repeat count, not once per generation."""
    log = ["gen 28 (session 4d9c6a03, 1MB): NOT resumable — over the ceiling",
           "gen 27 (session 4d9c6a03, 1MB): NOT resumable — over the ceiling"]
    assert (_collapse_resume_log(log) ==
            "gens 27-28 (session 4d9c6a03, 1MB): NOT resumable — over the ceiling (2x)")


def test_collapse_resume_log_ranks_distinct_entries_above_collapsed_repeats() -> None:
    """The Thoth msg 3802 shape: two sessions each repeated across generations, then the
    one line that actually matters (a crossed-registry refusal). The refusal must lead,
    not sit lost after a wall of repeats."""
    log = [
        "gen 28 (session 4d9c6a03, 1MB): NOT resumable — over the ceiling",
        "gen 27 (session 4d9c6a03, 1MB): NOT resumable — over the ceiling",
        "gen 25 (session 65f67b32, 1MB): NOT resumable — over the ceiling",
        "gen 24 (session 65f67b32, 1MB): NOT resumable — over the ceiling",
        "gen 23 (session 65f67b32, 1MB): NOT resumable — over the ceiling",
        "gen 22 (session 65f67b32, 1MB): NOT resumable — over the ceiling",
        "gen 21 (session 03a4a2d5, 1MB): resumable, 6 hop(s) back",
        "crossed-registry guard refused it: the registry's door for this addressee leads "
        "to a session whose own signed testimony names a different mind",
    ]
    collapsed = _collapse_resume_log(log)
    parts = collapsed.split("; ")
    assert parts[0] == "gen 21 (session 03a4a2d5, 1MB): resumable, 6 hop(s) back"
    assert parts[1].startswith("crossed-registry guard refused it:")
    assert parts[2] == ("gens 27-28 (session 4d9c6a03, 1MB): NOT resumable — over the "
                        "ceiling (2x)")
    assert parts[3] == ("gens 22-25 (session 65f67b32, 1MB): NOT resumable — over the "
                        "ceiling (4x)")
    assert len(parts) == 4


def test_collapse_resume_log_empty_is_empty() -> None:
    assert _collapse_resume_log([]) == ""


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


# --- cmd_desk / cmd_show (thread 00913be9): thin doors onto inbox/recall — the MCP round
# trip is mocked, same shape as cmd_attach's own fake `manager` above. -----------------

async def test_cmd_desk_calls_inbox_pinned_to_the_operator_project(monkeypatch: Any) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    async def _fake_call(url: str, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        calls.append((name, arguments))
        return {"owed": 0, "letters": 0, "needs_decision": [], "needs_hands": [], "fyi": []}

    monkeypatch.setattr("src.orchestrator.mcp_client.call_mcp_tool", _fake_call)
    assert await cmd_desk() == 0
    assert calls == [("inbox", {"project": "operator", "peek": True})]


async def test_cmd_desk_reports_a_dark_daemon_honestly(
    monkeypatch: Any, capsys: Any,
) -> None:
    async def _dark(url: str, name: str, arguments: dict[str, Any]) -> str:
        return "error: connection refused"

    monkeypatch.setattr("src.orchestrator.mcp_client.call_mcp_tool", _dark)
    assert await cmd_desk() == 1
    assert "osiris-mcp" in capsys.readouterr().err


async def test_cmd_show_calls_recall_with_the_ref(monkeypatch: Any) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    async def _fake_call(url: str, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        calls.append((name, arguments))
        return {"id": "5f234a1c", "type": "Thread", "summary": "found"}

    monkeypatch.setattr("src.orchestrator.mcp_client.call_mcp_tool", _fake_call)
    assert await cmd_show("5f234a1c") == 0
    assert calls == [("recall", {"ref": "5f234a1c"})]


async def test_cmd_show_exits_nonzero_when_recall_refuses(monkeypatch: Any) -> None:
    async def _no_match(url: str, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        return {"error": "no thread/decision matches 'nope' — recall never guesses"}

    monkeypatch.setattr("src.orchestrator.mcp_client.call_mcp_tool", _no_match)
    assert await cmd_show("nope") == 1


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
                           manager=_unreachable, debug=True)
    assert out == 1


async def test_cmd_launch_ambiguous_handle_is_honest(actions: Actions) -> None:
    await ensure_seat(actions, house="alfred", handle="twin", source="test")
    await ensure_seat(actions, house="osiris", handle="twin", source="test")

    async def _unreachable(req: dict[str, Any]) -> dict[str, Any]:
        raise AssertionError("should never be called — the ambiguity check fails first")

    assert await cmd_launch("twin", model=None, pool=actions.pool, manager=_unreachable,
                           debug=True) == 1


async def test_cmd_launch_no_anchor_cwd_is_honest(actions: Actions) -> None:
    await ensure_seat(actions, house="osiris", handle="roomless", source="test")

    async def _unreachable(req: dict[str, Any]) -> dict[str, Any]:
        raise AssertionError("should never be called — the office check fails first")

    assert await cmd_launch("roomless", model=None, pool=actions.pool,
                            manager=_unreachable, debug=True) == 1


async def test_cmd_launch_returns_the_existing_window_instead_of_twinning(
    actions: Actions,
) -> None:
    await ensure_seat(actions, house="osiris", handle="already-live",
                      anchor_cwd="/home/x/.osiris/seats/already-live", source="test")

    calls: list[dict[str, Any]] = []

    async def _fake(req: dict[str, Any]) -> dict[str, Any]:
        calls.append(req)
        return {"sessions": [{"name": "[OS] already-live", "alive": True}]}

    out = await cmd_launch("already-live", model=None, pool=actions.pool, manager=_fake,
                           debug=True)
    assert out == 0
    # only the roster was ever asked for — pty_spawn was never reached
    assert [c["op"] for c in calls] == ["pty_list"]


async def test_cmd_launch_reports_a_dark_manager_after_facts_resolve(actions: Actions) -> None:
    await ensure_seat(actions, house="osiris", handle="darkbody",
                      anchor_cwd="/home/x/.osiris/seats/darkbody", source="test")

    async def _dark(req: dict[str, Any]) -> dict[str, Any]:
        raise TimeoutError("no reply")

    assert await cmd_launch("darkbody", model=None, pool=actions.pool, manager=_dark,
                           debug=True) == 1


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
                           manager=_fake, debug=True)
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
                               manager=_fake, debug=True)
    assert out == 0
    assert "MISMATCH" in buf.getvalue()
    assert "claude-fable-5" in buf.getvalue()


# --- cmd_launch harness-native default lane (task #72) — same "never risk a real spawn" law,
# a fake spawn/agents_json instead of a fake manager ---------------------------------------------

async def test_cmd_launch_harness_unknown_handle_is_honest(actions: Actions) -> None:
    async def _unreachable(*a: Any, **k: Any) -> Any:
        raise AssertionError("should never be called — the seat lookup fails first")

    out = await cmd_launch("no-such-handle-at-all", model=None, pool=actions.pool,
                           spawn=_unreachable, agents_json=_unreachable)
    assert out == 1


async def test_cmd_launch_harness_returns_the_existing_body_instead_of_twinning(
    actions: Actions, tmp_path: Path,
) -> None:
    office = tmp_path / "already-live-bg"
    office.mkdir()
    await ensure_seat(actions, house="osiris", handle="already-live-bg",
                      anchor_cwd=str(office), source="test")

    async def _spawn(*a: Any, **k: Any) -> None:
        raise AssertionError("should never be called — a live body already holds this seat")

    async def _agents_json(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        return [{"name": "[OS] already-live-bg", "cwd": cwd, "sessionId": "sess-1"}]

    out = await cmd_launch("already-live-bg", model=None, pool=actions.pool,
                           spawn=_spawn, agents_json=_agents_json)
    assert out == 0


async def test_cmd_resume_refuses_an_already_live_seat_same_occupancy_gate_as_launch(
    actions: Actions, tmp_path: Path,
) -> None:
    """A VERB SPLIT MUST NOT BECOME A GUARD HOLE (operator ruling 60c78788, Thoth's own
    explicit instruction): `osiris resume` shares launch's exact twin/occupancy check
    via `_resolve_and_guard_launch` — a seat already holding a live body refuses resume
    too, same success-shaped exit 0, never a spawn attempt."""
    office = tmp_path / "already-live-resume"
    office.mkdir()
    await ensure_seat(actions, house="osiris", handle="already-live-resume",
                      anchor_cwd=str(office), source="test")

    async def _boom_resume(*a: Any, **k: Any) -> None:
        raise AssertionError("should never be called — a live body already holds this seat")

    async def _agents_json(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        return [{"name": "[OS] already-live-resume", "cwd": cwd, "sessionId": "sess-1"}]

    out = await cmd_resume("already-live-resume", pool=actions.pool,
                           agents_json=_agents_json, resume_spawn=_boom_resume)
    assert out == 0


async def test_cmd_launch_harness_catches_a_resumed_body_the_harness_roster_cannot_see(
    actions: Actions, tmp_path: Path,
) -> None:
    """Task #148's contested seam 4, the CLI's own copy of the same shared bug: `claude
    agents --json` is invisible to a resumed (`-p --resume`) body by construction, so an
    EMPTY harness roster used to mean "safe to mint" even with a genuinely live resumed
    session at this exact cwd, reachable only via agent_mounts. Shares launch_seat's own
    _launch_twin_check now (ruling 983ec87a, two doors one receipt) — never a second,
    differently-shaped guard on the same class."""
    from src.orchestrator.mounts import save_mount

    office = tmp_path / "resumed-cli"
    office.mkdir()
    await ensure_seat(actions, house="osiris", handle="resumed-cli",
                      anchor_cwd=str(office), source="test")
    await save_mount(actions.pool, job_dir="/tmp/jobs/resumed-cli-job",
                     agent_id="agent:resumed-cli-body", project="p",
                     cwd=str(office), model=None, session_key=None)

    async def _spawn(*a: Any, **k: Any) -> None:
        raise AssertionError("should never be called — a resumed body already holds this "
                             "seat, invisible to the harness roster alone")

    async def _agents_json(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        return []  # the harness roster sees NOTHING here — that used to mean "mint fresh"

    out = await cmd_launch("resumed-cli", model=None, pool=actions.pool,
                           spawn=_spawn, agents_json=_agents_json)
    assert out == 0


async def test_cmd_launch_harness_spawns_and_confirms(
    actions: Actions, tmp_path: Path,
) -> None:
    """The full honest path: no live body yet, `claude --bg` fires with the mount+claim_name
    boot prompt, then a bounded poll confirms the body shows up in `claude agents --json` —
    the fake resolves on the FIRST poll iteration, so this needs no real sleep-bound wait."""
    office = tmp_path / "freshbg"
    office.mkdir()
    await ensure_seat(actions, house="osiris", handle="freshbg",
                      anchor_cwd=str(office), source="test")

    spawn_calls: list[dict[str, Any]] = []
    poll_count = 0

    async def _spawn(repo: str, *, name: str, model: str | None, prompt: str) -> None:
        spawn_calls.append({"repo": repo, "name": name, "model": model, "prompt": prompt})

    async def _agents_json(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        nonlocal poll_count
        poll_count += 1
        if poll_count < 2:  # call 1 = the pre-spawn already-live check: nothing there yet
            return []
        return [{"name": "[OS] freshbg", "cwd": cwd, "sessionId": "sess-fresh"}]

    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_launch("freshbg", model="claude-sonnet-5", pool=actions.pool,
                               spawn=_spawn, agents_json=_agents_json)
    assert out == 0
    assert len(spawn_calls) == 1
    assert spawn_calls[0]["repo"] == str(office)
    assert spawn_calls[0]["model"] == "claude-sonnet-5"
    assert f'mount(cwd="{office}"' in spawn_calls[0]["prompt"]
    assert 'claim_name("freshbg")' in spawn_calls[0]["prompt"]
    # CONTRACT CHANGED BY THE VERB SPLIT (operator ruling 60c78788). This used to assert
    # "not resumed" — the line that explained why launch fell THROUGH its resume branch to
    # a fresh spawn. There is no such branch any more: `osiris launch` always spawns fresh,
    # so reporting a resume decision it never made would be noise about a road not taken.
    # What must still hold is the same thing the old assertion was protecting: launch says
    # nothing about resume machinery it did not run.
    out_text = buf.getvalue()
    assert "spawned" in out_text and "claude --bg" in out_text
    assert "resumed" not in out_text          # launch never speaks of resuming, either way
    assert "min_tail_bytes" not in out_text and "ceiling=" not in out_text


async def test_cmd_launch_harness_confesses_dormant_history_to_stderr(
    actions: Actions, monkeypatch: Any, tmp_path: Path,
) -> None:
    """Thread fc69b9b4: a substantial transcript already sitting at the target office is
    named — not silently spawned into — before `claude --bg` fires. Disclosure only: the
    spawn still happens."""
    import io
    from contextlib import redirect_stderr

    from src.cli import _cmd_launch_harness

    office = tmp_path / "ooblek-cli"
    office.mkdir()
    await ensure_seat(actions, house="osiris", handle="ooblek-cli",
                      anchor_cwd=str(office), source="test")

    fake_info = {"path": "/whatever/b5f04f84.jsonl", "size_bytes": 20_300_000,
                 "last_touched": "2026-08-02T17:57:18+00:00"}
    monkeypatch.setattr(
        "src.ingest.sessions.dormant_history_confession",
        lambda cwd, **k: fake_info if cwd == str(office) else None,
    )

    async def _spawn(*a: Any, **k: Any) -> None:
        pass

    poll_count = 0

    async def _agents_json(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        nonlocal poll_count
        poll_count += 1
        if poll_count < 2:  # call 1 = the pre-spawn already-live check: nothing there yet
            return []
        return [{"name": "[OS] ooblek-cli", "cwd": cwd, "sessionId": "sess-ooblek"}]

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await _cmd_launch_harness("ooblek-cli", model=None, pool=actions.pool,
                                        wake_default=None, spawn=_spawn,
                                        agents_json=_agents_json)
    assert out == 0
    assert "20.3MB" in buf.getvalue()
    assert "2026-08-02T17:57:18+00:00" in buf.getvalue()


async def test_cmd_launch_harness_gives_up_honestly_when_never_visible(
    actions: Actions, tmp_path: Path,
) -> None:
    """The bounded-poll honesty law (same discipline as `_await_launch_confirmation` and
    `_wait_for_smoke`): a body that never shows up in `claude agents --json` gets a plain
    confession, never a false 'launched: true', and the poll itself is bounded — never an
    indefinite wait."""
    from src.cli import _cmd_launch_harness

    office = tmp_path / "neverup"
    office.mkdir()
    await ensure_seat(actions, house="osiris", handle="neverup",
                      anchor_cwd=str(office), source="test")

    async def _spawn(*a: Any, **k: Any) -> None:
        return None

    async def _agents_json(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        return []

    slept: list[float] = []

    async def _no_sleep(secs: float) -> None:
        slept.append(secs)

    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await _cmd_launch_harness("neverup", model=None, pool=actions.pool,
                                        wake_default=None, spawn=_spawn,
                                        agents_json=_agents_json, sleep=_no_sleep)
    assert out == 0
    assert len(slept) == 8  # the full bounded poll, never an indefinite wait
    assert "not yet visible" in buf.getvalue()


# ═══ cmd_resume's own RESUME lane (task #136, 2026-08-05, decision 536de12f; split into
# its own verb by operator ruling 60c78788, 2026-09-01): mirrors launch_seat's own
# already-proven resume branch exactly — _lineage_resume_candidate/_resume_guard/
# resume_spawn, reused verbatim, never reimplemented (test_trigger.py's own identically-
# shaped fixtures already exhaust the underlying gate logic — the guard, the compaction/
# ceiling math, the lineage walk — so these tests only prove _cmd_resume_harness WIRES it
# correctly, not re-derive it).
#
# VISIBILITY RIDES OSIRIS'S OWN REGISTRY, NEVER `claude agents --json` (decision 536de12f,
# confirming a829a15d, proven live twice, independently — a `-p --resume` body cannot
# appear there even when it explicitly calls mount() mid-turn): a successful resume never
# polls agents_json for it, so the fake below would raise if the code ever tried. ═══

_RESUME_SID = "b5f04f84-0000-4000-8000-000000000000"


async def _resumable_seat(
    actions: Actions, tmp_path: Path, *, handle: str, agent_id: str,
    anchor_cwd: str, compacted: bool = False, transcript_bytes: int = 16,
) -> Path:
    """A seat whose holder left a resumable session as a graph `session` property
    (succession_chain's own shape — the ONLY record `_lineage_resume_candidate` trusts)
    plus a real transcript on disk anchored to that session id. Returns the sense root to
    pass as osiris_sense_sessions."""
    import os
    import time as _time

    sense = tmp_path / "projects"
    proj = sense / "-repo-demo"
    proj.mkdir(parents=True, exist_ok=True)
    t = proj / f"{_RESUME_SID}.jsonl"
    signed = ('{"type":"user","toolUseResult":'
              '"{\\"sent\\":1,\\"from\\":\\"' + agent_id + '\\"}"}\n').encode()
    body = (signed + (b'{"type":"system","subtype":"compact_boundary"}\n' if compacted else b"")
            + b"x" * transcript_bytes)
    t.write_bytes(body)
    old = _time.time() - 3600
    os.utime(t, (old, old))

    # THE ONE-SIDED GUARD FAMILY fix (decision 27259e4d, thread bc11a2d3): osiris launch
    # now requires a seat's own anchor_cwd to exist on disk — this helper already has real
    # filesystem access (the transcript above), so it makes the real thing.
    Path(anchor_cwd).mkdir(parents=True, exist_ok=True)
    seat = await ensure_seat(actions, house="osiris", handle=handle, anchor_cwd=anchor_cwd,
                             source="test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id=agent_id)
    obj = await actions.create_or_find_object("Agent", agent_id, "test")
    now = datetime(2026, 8, 5, tzinfo=UTC)
    await actions.assert_property(obj, "seat_generation", "1", "test", now, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(obj, "session", _RESUME_SID, "test", now, 0.9,
                                  evidence_class="self_declared")
    return sense


def _resume_settings(sense: Path, *, min_tail_bytes: int = 0) -> SimpleNamespace:
    return SimpleNamespace(osiris_sense_sessions=str(sense), osiris_resume_ceiling_bytes=8_000_000,
                           osiris_resume_min_tail_bytes=min_tail_bytes,
                           osiris_wake_allowed_tools="mcp__osiris")


async def test_cmd_resume_harness_resumes_a_stale_but_resumable_holder(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE PAYOFF, now cmd_resume's own (operator ruling 60c78788's verb split): a seat
    whose holder left a resumable session is CONTINUED via resume_spawn's own `-p
    --resume` lane — and never polled through `agents_json` for it either (that
    registry cannot retain it, proven live; the fake below asserts it is never even
    asked). `agents_json` is still consulted ONCE, for the shared pre-resume
    already-live twin check both verbs use."""
    from src.cli import _cmd_resume_harness

    sense = await _resumable_seat(
        actions, tmp_path, handle="cliresume", agent_id="agent:cliresume01",
        anchor_cwd="/tmp/cliresume-office")

    resumed: list[dict[str, Any]] = []

    async def _resume_spawn(repo: str, prompt: str, **kw: Any) -> None:
        resumed.append({"repo": repo, "prompt": prompt, **kw})

    async def _boom_agents_json(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        return []  # only ever asked once, for the pre-resume already-live check

    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await _cmd_resume_harness(
            "cliresume", model="claude-sonnet-5", pool=actions.pool, wake_default=None,
            agents_json=_boom_agents_json, resume_spawn=_resume_spawn,
            settings=_resume_settings(sense))

    assert out == 0
    assert len(resumed) == 1
    call = resumed[0]
    assert call["repo"] == "/tmp/cliresume-office"          # the seat's own launch_cwd
    assert call.get("resume_session") == _RESUME_SID
    assert "job_dir" not in call                             # a resume is not a birth
    assert call.get("model") == "claude-sonnet-5"
    assert "private" in call["prompt"] and "seat" in call["prompt"]  # _DM_RESUME_PROMPT itself
    out_text = buf.getvalue()
    assert "resumed session" in out_text and _RESUME_SID[:8] in out_text
    # THE HONEST MESSAGE (Thoth's msg 6260, the operator's real "marquee was not
    # launched into the claude agents list" complaint): no defensive "a harness fact,
    # not a bug" framing — state the consequence and the two real next steps. This
    # text is now cmd_resume's own, unchanged word for word from before the split.
    assert "a harness fact, not a bug" not in out_text
    assert "gone from `claude agents --json` for good" in out_text
    assert "send it mail, it wakes on the next dispatch" in out_text
    assert f"claude -p --resume {_RESUME_SID}" in out_text


async def test_cmd_resume_harness_resumes_a_zero_hop_candidate_with_no_signed_testimony(
    actions: Actions, tmp_path: Path,
) -> None:
    """#173a's own door, mirrored through this CLI-facing lane (ruling 983ec87a, two doors
    one receipt — launch_seat's own sibling fix): the seat's CURRENT holder (0 hops back)
    has a real, anchored, resumable transcript that never wrote a single signed osiris
    act. The testimony arm alone would refuse this resident-unknown; the zero-hop graph
    door corroborates it via the graph's own session pointer + the seat's own launch
    location, and the resume proceeds through THIS door too, not just launch_seat's."""
    from src.cli import _cmd_resume_harness

    sense = await _resumable_seat_no_signed_testimony(
        actions, tmp_path, handle="clizerohop", agent_id="agent:clizerohop01",
        anchor_cwd="/tmp/clizerohop-office")

    resumed: list[dict[str, Any]] = []

    async def _resume_spawn(repo: str, prompt: str, **kw: Any) -> None:
        resumed.append({"repo": repo, "prompt": prompt, **kw})

    async def _agents_json(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        return []

    out = await _cmd_resume_harness(
        "clizerohop", model=None, pool=actions.pool, wake_default=None,
        agents_json=_agents_json, resume_spawn=_resume_spawn,
        settings=_resume_settings(sense))

    assert out == 0
    assert len(resumed) == 1
    assert resumed[0]["repo"] == "/tmp/clizerohop-office"
    assert resumed[0].get("resume_session") == _RESUME_SID


async def test_cmd_launch_harness_falls_through_with_a_named_reason_when_not_resumable(
    actions: Actions, tmp_path: Path,
) -> None:
    """NEVER FALLS THROUGH TO FRESH ANYMORE (operator ruling 60c78788's verb split, the
    whole point of it): a holder whose transcript compacted past the gate used to fall
    through to `claude --bg`; `osiris resume` now REFUSES cleanly instead — nothing
    spawned, the refusal reason still NAMED, never silent. Minting fresh on a failed
    resume is `osiris launch`'s own separate act now, never this verb's."""
    from src.cli import _cmd_resume_harness

    sense = await _resumable_seat(
        actions, tmp_path, handle="clicompact", agent_id="agent:clicompact01",
        anchor_cwd="/tmp/clicompact-office", compacted=True)

    async def _boom_resume(*a: Any, **k: Any) -> None:
        raise AssertionError("a tail closed at the seam itself must never be resumed")

    async def _agents_json(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        return []  # only ever asked once, for the pre-resume already-live check

    import io
    from contextlib import redirect_stderr

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await _cmd_resume_harness(
            "clicompact", model=None, pool=actions.pool, wake_default=None,
            agents_json=_agents_json, resume_spawn=_boom_resume,
            settings=_resume_settings(sense, min_tail_bytes=1000))

    assert out == 1  # refused — never spawns anything, fresh or otherwise
    out_text = buf.getvalue()
    assert "nothing resumable" in out_text
    assert "seam itself" in out_text
    assert "min_tail_bytes=1000" in out_text


async def _resumable_seat_no_signed_testimony(
    actions: Actions, tmp_path: Path, *, handle: str, agent_id: str, anchor_cwd: str,
) -> Path:
    """Same graph shape as `_resumable_seat` — a real, uncompacted transcript the seat's own
    `session` property points at — but with NO signed testimony anywhere in it: thread
    ef88e2bb's own specimen (ferryman's real, sizeable transcript, 0 hops back, nothing
    that scans as signed), the `resident-unknown` class."""
    import os
    import time as _time

    sense = tmp_path / "projects"
    proj = sense / "-repo-demo"
    proj.mkdir(parents=True, exist_ok=True)
    t = proj / f"{_RESUME_SID}.jsonl"
    t.write_bytes(b'{"type":"assistant","text":"just harness chrome, nothing signed"}\n'
                  + b"x" * 16)
    old = _time.time() - 3600
    os.utime(t, (old, old))

    # same fix as _resumable_seat's own (decision 27259e4d, thread bc11a2d3)
    Path(anchor_cwd).mkdir(parents=True, exist_ok=True)
    seat = await ensure_seat(actions, house="osiris", handle=handle, anchor_cwd=anchor_cwd,
                             source="test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id=agent_id)
    obj = await actions.create_or_find_object("Agent", agent_id, "test")
    now = datetime(2026, 8, 5, tzinfo=UTC)
    await actions.assert_property(obj, "seat_generation", "1", "test", now, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(obj, "session", _RESUME_SID, "test", now, 0.9,
                                  evidence_class="self_declared")
    return sense


async def test_cmd_launch_harness_refuses_outright_one_hop_back_with_no_signed_testimony(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE FIX FOR ef88e2bb (operator, 2026-08-17), through the CLI-facing door: a
    resumable session with NO signed testimony, ONE HOP BACK (the predecessor's own
    session, not the seat's current holder) — not eligible for #173a's zero-hop graph
    door (see the sibling zero-hop test above, and test_trigger.py's own
    `test_zero_hop_graph_door_never_fires_one_hop_back` for the launch_seat side of this
    same contract) — must refuse the WHOLE launch. Before ef88e2bb this fell through to
    the same `claude --bg` mint as a genuine crossed-registry finding, exactly how
    ferryman's real, resumable session got a stranger minted over it."""
    from src.cli import _cmd_resume_harness

    sense = tmp_path / "projects"
    proj = sense / "-repo-demo"
    proj.mkdir(parents=True, exist_ok=True)
    t = proj / f"{_RESUME_SID}.jsonl"
    t.write_bytes(b'{"type":"assistant","text":"booting"}\n' + b"x" * 16)  # no signed act
    import os
    import time as _time
    old = _time.time() - 3600
    os.utime(t, (old, old))

    office = tmp_path / "clihopback-office"
    office.mkdir()
    seat = await ensure_seat(actions, house="osiris", handle="clihopback",
                             anchor_cwd=str(office), source="test")
    pred = await actions.create_or_find_object("Agent", "agent:clihopback01", "test")
    now = datetime(2026, 8, 18, tzinfo=UTC)
    await actions.assert_property(pred, "seat_generation", "1", "test", now, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(pred, "session", _RESUME_SID, "test", now, 0.9,
                                  evidence_class="self_declared")
    heir = await actions.create_or_find_object("Agent", "agent:clihopback01-ii", "test")
    await actions.assert_property(heir, "succeeded_from", "agent:clihopback01", "test", now,
                                  0.9, evidence_class="self_declared")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:clihopback01-ii")

    async def _boom_resume(*a: Any, **k: Any) -> None:
        raise AssertionError("resident-unknown must refuse, never resume blind either")

    async def _agents_json(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        return []  # only the pre-resume already-live check may ever call this

    import io
    from contextlib import redirect_stderr

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await _cmd_resume_harness(
            "clihopback", model=None, pool=actions.pool, wake_default=None,
            agents_json=_agents_json, resume_spawn=_boom_resume,
            settings=_resume_settings(sense))

    assert out == 1  # refused, not the ordinary success-with-fresh-spawn path
    err = buf.getvalue()
    assert "REFUSING" in err
    assert _RESUME_SID in err
    assert "claude -p --resume" in err and _RESUME_SID in err.split("claude -p --resume")[1]


# ═══ tree_cwd (task #135/#136, 2026-08-03, ruling 983ec87a): `osiris launch` had drifted
# from launch_seat's own #103 update — hardcoded to `office`, never reading `tree_cwd` at
# all. Same three proofs test_trigger.py already carries for launch_seat itself, mirrored
# here for the CLI door — two doors onto one act must return the same receipt. ═══

async def test_cmd_launch_harness_refuses_a_tree_cwd_that_does_not_exist_on_disk(
    actions: Actions, tmp_path: Path,
) -> None:
    """OSIRIS NEVER PROVISIONS THE TREE (ff3bdc37: harness owns isolation) — a seat naming a
    tree_cwd the harness never actually created is refused, cleanly, before anything spawns,
    exactly matching launch_seat's own refusal shape.

    ALSO PROVES THE COPY-PASTE FIX (Thoth's msg 6256, call (b) — the same verb-naming
    treatment as anchor_cwd's own refusal): the message must print the actual
    `bind_seat_tree(seat_id=..., tree_cwd=..., because=...)` call with the real seat_id
    filled in, not a generic "osiris expects the harness to have created it" dead end."""
    import io
    from contextlib import redirect_stderr

    from src.orchestrator.seats import bind_seat_tree

    office = tmp_path / "office"
    office.mkdir()
    seat = await ensure_seat(actions, house="osiris", handle="clinotree",
                             anchor_cwd=str(office), source="test")
    ghost_tree = str(tmp_path / "never-created")
    bind = await bind_seat_tree(actions, seat_id=seat["seat_id"], tree_cwd=ghost_tree,
                                actor="operator", because="test: CLI refusal proof")
    assert bind.get("error") is None

    async def _unreachable(*a: Any, **k: Any) -> Any:
        raise AssertionError("should never be called — the tree check refuses first")

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_launch("clinotree", model=None, pool=actions.pool,
                               spawn=_unreachable, agents_json=_unreachable)
    assert out == 1
    err = buf.getvalue()
    assert "tree_cwd" in err
    assert f"bind_seat_tree(seat_id={seat['seat_id']!r}" in err
    assert "tree_cwd=" in err and "because=" in err


async def test_cmd_launch_harness_refuses_an_anchor_cwd_that_does_not_exist_on_disk(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE ONE-SIDED GUARD FAMILY, NEXT SPECIMEN (decision 27259e4d, thread bc11a2d3): the
    operator's real specimen — `osiris launch marquee` died with a raw FileNotFoundError
    because tree_cwd got an existence check and anchor_cwd/office never did. A seat with NO
    tree_cwd binding (the marquee shape exactly) falls back to office as launch_cwd, so a
    stale/wrong anchor must refuse cleanly here too, before anything spawns — and the
    message must name `anchor_cwd`, not `tree_cwd`, so the operator can tell which field
    disagrees.

    ALSO PROVES THE COPY-PASTE FIX (Thoth's msg 6256, #97's stranger's test): the operator
    literally went looking in ~/.osiris/seats/marquee/.osiris for the anchor and it was not
    there — the anchor is a graph assertion, not a pin field. The message must say that AND
    print the actual `rebind_seat(seat=..., new_cwd=...)` call with the real handle filled
    in, not a generic "repoint it" with no verb to do so."""
    import io
    from contextlib import redirect_stderr

    seat = await ensure_seat(actions, house="osiris", handle="clighostanchor",
                             anchor_cwd=str(tmp_path / "never-created-either"),
                             source="test")
    assert seat.get("error") is None

    async def _unreachable(*a: Any, **k: Any) -> Any:
        raise AssertionError("should never be called — the anchor check refuses first")

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_launch("clighostanchor", model=None, pool=actions.pool,
                               spawn=_unreachable, agents_json=_unreachable)
    assert out == 1
    err = buf.getvalue()
    assert "anchor_cwd" in err
    assert "GRAPH assertion" in err
    assert "rebind_seat(seat='clighostanchor'" in err
    assert "new_cwd=" in err


async def test_cmd_launch_harness_spawns_into_tree_cwd_not_office(
    actions: Actions, tmp_path: Path,
) -> None:
    """The office (identity) and the tree (code) are DISTINCT — a bound, real tree_cwd is
    where the body actually spawns; the boot prompt still anchors mount() at the office."""
    from src.orchestrator.seats import bind_seat_tree

    office = tmp_path / "office"
    office.mkdir()
    tree = tmp_path / "worktree"
    tree.mkdir()
    seat = await ensure_seat(actions, house="osiris", handle="clitreewalker",
                             anchor_cwd=str(office), source="test")
    await bind_seat_tree(actions, seat_id=seat["seat_id"], tree_cwd=str(tree),
                         actor="operator", because="test: CLI spawn location proof")

    spawn_calls: list[dict[str, Any]] = []

    async def _spawn(repo: str, *, name: str, model: str | None, prompt: str) -> None:
        spawn_calls.append({"repo": repo, "prompt": prompt})

    poll_count = 0

    async def _agents_json(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        nonlocal poll_count
        poll_count += 1
        if poll_count < 2:
            return []
        return [{"name": "[OS] clitreewalker", "cwd": cwd, "sessionId": "sess-tree"}]

    out = await cmd_launch("clitreewalker", model=None, pool=actions.pool,
                           spawn=_spawn, agents_json=_agents_json)
    assert out == 0
    assert spawn_calls[0]["repo"] == str(tree)            # spawned INTO the tree
    assert str(office) in spawn_calls[0]["prompt"]         # identity still anchors at office
    assert str(tree) not in spawn_calls[0]["prompt"]


async def test_cmd_launch_harness_idempotency_matches_on_tree_cwd_not_office(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE CORRECTNESS PROOF: a tree-bound seat's live process sits at tree_cwd. Matching
    idempotency on `office` alone (the pre-fix shape) would never find it and would twin on
    every relaunch."""
    from src.orchestrator.seats import bind_seat_tree

    office = tmp_path / "office2"
    office.mkdir()
    tree = tmp_path / "worktree2"
    tree.mkdir()
    seat = await ensure_seat(actions, house="osiris", handle="clitreewalker2",
                             anchor_cwd=str(office), source="test")
    await bind_seat_tree(actions, seat_id=seat["seat_id"], tree_cwd=str(tree),
                         actor="operator", because="test: CLI idempotency proof")

    async def _unreachable(*a: Any, **k: Any) -> Any:
        raise AssertionError("should never be called — a live body already holds this seat")

    async def _agents_json(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        return [{"name": "[OS] clitreewalker2", "cwd": str(tree), "sessionId": "sess-live"}]

    out = await cmd_launch("clitreewalker2", model=None, pool=actions.pool,
                           spawn=_unreachable, agents_json=_agents_json)
    assert out == 0  # returned the existing body, never twinned


# --- osiris deploy: pure decision layer (task e51a841c) -----------------------------------------

def test_dirty_tracked_src_files_flags_modified_tracked_files() -> None:
    status = [(" M", "src/foo.py"), ("M ", "src/bar.py"), ("??", "src/new_untracked.py"),
             (" M", "tests/test_foo.py"), ("D ", "src/gone.py")]
    assert dirty_tracked_src_files(status) == ["src/bar.py", "src/foo.py", "src/gone.py"]


def test_dirty_tracked_src_files_ignores_untracked_and_non_src() -> None:
    status = [("??", "src/brand_new.py"), (" M", "docs/README.md"), (" M", "scripts/x.py")]
    assert dirty_tracked_src_files(status) == []


def test_dirty_tracked_src_files_clean_tree_is_empty() -> None:
    assert dirty_tracked_src_files([]) == []


def test_oneshot_deployed_scripts_reads_real_deploy_dir() -> None:
    """Against the REAL repo's own deploy/ — proves the parser actually matches this
    project's real oneshot units rather than a synthetic fixture only."""
    repo_root = Path(__file__).resolve().parent.parent
    found = oneshot_deployed_scripts(repo_root)
    assert found.get("scripts/osiris_preflight.py") == "osiris-preflight"
    assert found.get("scripts/osiris_backup.sh") == "osiris-backup"
    # a non-oneshot unit (Type=simple, e.g. osiris-mcp.service) must never appear
    assert not any(unit == "osiris-mcp" for unit in found.values())


def test_oneshot_deployed_scripts_missing_deploy_dir_is_empty(tmp_path: Path) -> None:
    assert oneshot_deployed_scripts(tmp_path) == {}


def test_commit_deployed_notes_names_a_dirty_oneshot_script() -> None:
    status = [(" M", "scripts/osiris_preflight.py"), (" M", "src/foo.py")]
    oneshot = {"scripts/osiris_preflight.py": "osiris-preflight"}
    notes = commit_deployed_notes(status, oneshot)
    assert len(notes) == 1
    assert "scripts/osiris_preflight.py" in notes[0]
    assert "osiris-preflight" in notes[0]
    assert "NOT gated" in notes[0]


def test_commit_deployed_notes_untracked_oneshot_script_still_named() -> None:
    """An uncommitted NEW oneshot script is just as immediately live as a modified one."""
    status = [("??", "scripts/osiris_preflight.py")]
    oneshot = {"scripts/osiris_preflight.py": "osiris-preflight"}
    assert len(commit_deployed_notes(status, oneshot)) == 1


def test_commit_deployed_notes_clean_is_silent() -> None:
    status = [(" M", "src/foo.py")]
    oneshot = {"scripts/osiris_preflight.py": "osiris-preflight"}
    assert commit_deployed_notes(status, oneshot) == []


def test_composition_gap_notes_flags_the_missing_default_by_name() -> None:
    notes = composition_gap_notes({"a", "b"}, {"a", "b", "c"})
    assert len(notes) == 1
    assert "'c'" in notes[0]


def test_composition_gap_notes_silent_when_caught_up() -> None:
    assert composition_gap_notes({"a", "b"}, {"a", "b"}) == []
    assert composition_gap_notes({"a", "b", "extra"}, {"a", "b"}) == []  # extra rows, no gap


def test_composition_gap_notes_extra_user_saved_rows_never_mask_a_missing_default() -> None:
    """The exact bug (thread a25365a9): a count comparison reads 'caught up' as long as the
    table is at least as big as DEFAULT_COMPOSITIONS, even if what's padding the count is
    unrelated user-saved compositions rather than the defaults themselves. Eleven extras
    and one missing default, measured live, is the shape that produced two false all-clears
    in a row."""
    have = {"a"} | {f"user-saved-{i}" for i in range(11)}
    notes = composition_gap_notes(have, {"a", "b"})
    assert len(notes) == 1
    assert "'b'" in notes[0]


def test_composition_room_gap_notes_names_each_unassigned_composition() -> None:
    notes = composition_room_gap_notes(["orphan-a", "orphan-b"])
    assert len(notes) == 2
    joined = " ".join(notes)
    assert "'orphan-a'" in joined and "'orphan-b'" in joined


def test_composition_room_gap_notes_silent_when_none_unassigned() -> None:
    assert composition_room_gap_notes([]) == []


# --- composition_drift_notes: MISSING-OR-DIFFERENT, not just missing (e4612853/38c71544) -------

def test_composition_drift_notes_flags_a_differing_spec_by_name() -> None:
    live = {"a": {"op": "select", "object_type": "Commit"}}
    expected = {"a": {"op": "select", "object_type": "Decision"}}
    notes = composition_drift_notes(live, expected)
    assert len(notes) == 1
    assert "'a'" in notes[0]
    assert "DIFFERS" in notes[0]


def test_composition_drift_notes_silent_when_specs_match() -> None:
    live = {"a": {"op": "select", "object_type": "Commit"}}
    expected = {"a": {"op": "select", "object_type": "Commit"}}
    assert composition_drift_notes(live, expected) == []


def test_composition_drift_notes_ignores_key_order() -> None:
    """A spec is a dict, not a string — reordering keys during an unrelated refactor must
    never read as drift (json.dumps(sort_keys=True) on both sides)."""
    live = {"a": {"op": "select", "object_type": "Commit"}}
    expected = {"a": {"object_type": "Commit", "op": "select"}}
    assert composition_drift_notes(live, expected) == []


def test_composition_drift_notes_never_reports_a_missing_composition() -> None:
    """A name in `expected` but absent from `live_specs` is composition_gap_notes' own job
    (a missing default) — this function must stay silent about it, never double-report."""
    live: dict[str, Any] = {}
    expected = {"a": {"op": "select"}}
    assert composition_drift_notes(live, expected) == []


def test_composition_drift_notes_multiple_drifts_all_named() -> None:
    live = {"a": {"op": "select"}, "b": {"op": "select"}, "c": {"op": "select"}}
    expected = {"a": {"op": "select"}, "b": {"op": "different"}, "c": {"op": "also-different"}}
    notes = composition_drift_notes(live, expected)
    assert len(notes) == 2
    joined = " ".join(notes)
    assert "'b'" in joined and "'c'" in joined and "'a'" not in joined


async def test_composition_gaps_names_a_real_drifted_composition(actions: Actions) -> None:
    """End-to-end against a real seeded pool (this repo's own never-mock-the-DB rule): seed
    the real defaults, hand-mutate ONE composition's spec directly (standing in for "source
    edited, DB row never re-pushed" — the exact a346a0d/project-briefing shape), confirm
    _composition_gaps names it while every other default stays silent."""
    assert await cmd_seed(compositions_only=True, pool=actions.pool) == 0
    await actions.pool.execute(
        "UPDATE compositions SET spec=$1 WHERE name='mail'",
        '{"op": "function", "name": "not_the_real_mail_overview_anymore"}')
    gaps = await _composition_gaps(actions.pool)
    joined = " ".join(gaps)
    assert "'mail'" in joined and "DIFFERS" in joined
    assert "'briefing'" not in joined  # an untouched default stays silent
    assert "'project-briefing'" not in joined


def test_alembic_gap_note_flags_a_mismatch() -> None:
    note = alembic_gap_note("0034", "0036")
    assert note is not None
    assert "0034" in note and "0036" in note


def test_alembic_gap_note_silent_when_current() -> None:
    assert alembic_gap_note("0036", "0036") is None


def test_alembic_gap_note_silent_when_head_undeterminable() -> None:
    """A None head means 'couldn't be read locally', never 'assume a mismatch'."""
    assert alembic_gap_note("0034", None) is None


def test_alembic_head_missing_alembic_ini_is_none_not_a_crash(tmp_path: Path) -> None:
    from src.cli import _alembic_head

    assert _alembic_head(tmp_path) is None


def test_alembic_head_reads_this_repos_real_migrations() -> None:
    from src.cli import _alembic_head

    repo_root = Path(__file__).resolve().parent.parent
    assert _alembic_head(repo_root) is not None


# --- _wait_for_smoke: a bounded retry-with-backoff, no real sleeping in tests ------------------

async def test_wait_for_smoke_clean_on_first_try_never_sleeps() -> None:
    calls = []

    async def _probe() -> list[str]:
        calls.append(1)
        return []

    async def _no_sleep(_delay: float) -> None:
        raise AssertionError("must never sleep when the first probe is already clean")

    fails, waited = await _wait_for_smoke(_probe, sleep=_no_sleep)
    assert fails == []
    assert waited == 0.0
    assert len(calls) == 1


async def test_wait_for_smoke_recovers_after_one_retry() -> None:
    """The exact live shape (batch 4's maiden osiris deploy run): all-red immediately,
    all-green shortly after — never reported as a false alarm."""
    attempts = [["chrome /desk: connection refused"], []]
    slept: list[float] = []

    async def _probe() -> list[str]:
        return attempts.pop(0)

    async def _fake_sleep(delay: float) -> None:
        slept.append(delay)

    fails, waited = await _wait_for_smoke(_probe, sleep=_fake_sleep)
    assert fails == []
    assert slept == [2.0]
    assert waited == 2.0


async def test_wait_for_smoke_gives_up_at_the_ceiling_and_reports_honestly() -> None:
    """A genuinely down service is still reported — the bound protects against false alarms
    on a SLOW startup, it must never hide a real, sustained failure."""
    async def _always_fails() -> list[str]:
        return ["osiris-mcp round-trip: error: connection refused"]

    slept: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        slept.append(delay)

    fails, waited = await _wait_for_smoke(_always_fails, ceiling_secs=10.0, sleep=_fake_sleep)
    assert fails == ["osiris-mcp round-trip: error: connection refused"]
    assert waited >= 10.0
    assert slept == [2.0, 4.0, 8.0]  # 2+4=6 (<10, keep going), +8=14 (>=10, stop)


async def test_wait_for_smoke_backoff_is_capped() -> None:
    async def _always_fails() -> list[str]:
        return ["still down"]

    slept: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        slept.append(delay)

    await _wait_for_smoke(_always_fails, ceiling_secs=30.0, sleep=_fake_sleep)
    assert slept == [2.0, 4.0, 8.0, 8.0, 8.0]  # doubles until 8, never exceeds it
    assert max(slept) == 8.0


# --- _wait_for_health: same bounded-retry shape, run BEFORE smoke (Thoth DM 2823) -------------

async def test_wait_for_health_ready_on_first_try_never_sleeps() -> None:
    calls = []

    async def _probe() -> bool:
        calls.append(1)
        return True

    async def _no_sleep(_delay: float) -> None:
        raise AssertionError("must never sleep when the first probe is already ready")

    ready, waited = await _wait_for_health(_probe, sleep=_no_sleep)
    assert ready is True
    assert waited == 0.0
    assert len(calls) == 1


async def test_wait_for_health_recovers_after_startup() -> None:
    """The exact measured shape (Thoth DM 2823): not-ready immediately, ready once the
    console finishes its own cold start — reported as a real elapsed wait, never a guess."""
    attempts = [False, False, True]
    slept: list[float] = []

    async def _probe() -> bool:
        return attempts.pop(0)

    async def _fake_sleep(delay: float) -> None:
        slept.append(delay)

    ready, waited = await _wait_for_health(_probe, sleep=_fake_sleep)
    assert ready is True
    assert slept == [1.0, 2.0]
    assert waited == 3.0


async def test_wait_for_health_gives_up_at_the_ceiling_and_reports_honestly() -> None:
    """A console that never comes up is still reported truthfully — the bound protects
    against false alarms on a slow boot, it must never hide a real, sustained failure."""
    async def _never_ready() -> bool:
        return False

    slept: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        slept.append(delay)

    ready, waited = await _wait_for_health(_never_ready, ceiling_secs=10.0, sleep=_fake_sleep)
    assert ready is False
    assert waited >= 10.0
    assert slept == [1.0, 2.0, 4.0, 8.0]  # 1+2+4=7 (<10, keep going), +8=15 (>=10, stop)


async def test_wait_for_health_backoff_is_capped() -> None:
    async def _never_ready() -> bool:
        return False

    slept: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        slept.append(delay)

    await _wait_for_health(_never_ready, ceiling_secs=31.0, sleep=_fake_sleep)
    assert slept == [1.0, 2.0, 4.0, 8.0, 8.0, 8.0]  # doubles until 8, never exceeds it
    assert max(slept) == 8.0


# --- diff_tool_lists: pure --------------------------------------------------------------------

def test_diff_tool_lists_no_change_is_empty() -> None:
    same = {"smoke": "aaa", "fleet": "bbb"}
    assert diff_tool_lists(same, dict(same)) == []


def test_diff_tool_lists_names_an_addition() -> None:
    before = {"smoke": "aaa"}
    after = {"smoke": "aaa", "retire_assertion": "ccc"}
    assert diff_tool_lists(before, after) == ["+retire_assertion"]


def test_diff_tool_lists_names_a_removal() -> None:
    before = {"smoke": "aaa", "old_tool": "zzz"}
    after = {"smoke": "aaa"}
    assert diff_tool_lists(before, after) == ["-old_tool removed"]


def test_diff_tool_lists_names_a_signature_change() -> None:
    before = {"smoke": "aaa"}
    after = {"smoke": "bbb"}
    assert diff_tool_lists(before, after) == ["~smoke changed"]


def test_diff_tool_lists_composes_all_three_kinds_in_thoths_own_example_shape() -> None:
    before = {"smoke": "aaa", "old_tool": "zzz"}
    after = {"smoke": "bbb", "retire_assertion": "ccc"}
    assert diff_tool_lists(before, after) == [
        "+retire_assertion", "-old_tool removed", "~smoke changed"]


# --- cmd_deploy: fake git_status/restart, a real pool for the seeder/migration comparison ------
# wait_for_health/wait_for_smoke default to REAL bounded pollers (120s/30s ceilings, real
# network round-trips against the live console/MCP) — every test whose restart succeeds and
# falls through to that stage injects these fast fakes instead. cmd_deploy's own control flow
# (order of calls, what it prints, what it returns) is what's under test here; the wait
# LOGIC itself already has its own dedicated, correctly-mocked unit tests below.

async def _fake_wait_for_health() -> tuple[bool, float]:
    return True, 0.0


async def _fake_wait_for_smoke() -> tuple[list[str], float]:
    return [], 0.0


async def _fake_check_whisper_ok() -> tuple[bool, str]:
    return True, "whisper probe: /automount round-tripped clean"


# --- task #179: the deploy refuses to record on a bad whisper probe, same law as the -------
# migration gate --------------------------------------------------------------------------

async def test_cmd_deploy_refuses_to_record_when_the_whisper_probe_fails(
    actions: Actions, tmp_path: Path,
) -> None:
    async def _restart(units: list[str]) -> tuple[int, str]:
        return 0, "done"

    async def _bad_probe() -> tuple[bool, str]:
        return False, "whisper probe: REFUSED — /automount returned 500: boom"

    async def _unreachable(pool: Any, repo_root: Path) -> str | None:
        raise AssertionError("must never be called — the whisper probe refused first")

    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [], restart=_restart,
                               pool=actions.pool, record_deploy=_unreachable,
                               wait_for_health=_fake_wait_for_health,
                               wait_for_smoke=_fake_wait_for_smoke,
                               check_whisper_probe=_bad_probe)
    assert out == 1
    assert "REFUSED" in buf.getvalue()
    assert "NOT recording this deploy" in buf.getvalue()


async def test_cmd_deploy_records_normally_when_the_whisper_probe_succeeds(
    actions: Actions, tmp_path: Path,
) -> None:
    async def _restart(units: list[str]) -> tuple[int, str]:
        return 0, "done"

    calls: list[Any] = []

    async def _record_deploy(pool: Any, repo_root: Path) -> str | None:
        calls.append(repo_root)
        return "cafef00d"

    out = await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [], restart=_restart,
                           pool=actions.pool, record_deploy=_record_deploy,
                           wait_for_health=_fake_wait_for_health,
                           wait_for_smoke=_fake_wait_for_smoke,
                           check_whisper_probe=_fake_check_whisper_ok)
    assert calls == [tmp_path]
    assert out == 0


# --- THE FULL SUITE ON THE MERGED TREE (task #186, Thoth DM 5637, 2026-08-25) ---------------
# OFF by default (every test above ran with it unset). These pin the ONE new step, fully
# injected: `_real_full_suite_gate`'s own subprocess plumbing has no dedicated test here
# (it would mean nesting a real pytest run inside this suite) — this only tests that
# cmd_deploy WIRES the gate correctly and runs it BEFORE the chaos gate.

async def test_cmd_deploy_skips_the_full_suite_gate_by_default(
    actions: Actions, tmp_path: Path,
) -> None:
    """thread be24817b: this asserts the FIELD'S OWN DEFAULT in source, never ambient env —
    an explicitly constructed Settings() is the claim; reading get_settings() here would be
    testing whatever the process's environment happens to hold (which, the moment this very
    gate is armed for a real deploy, is exactly the flag this test exists to disprove — the
    self-refuting loop this fix closes)."""
    async def _restart(units: list[str]) -> tuple[int, str]:
        return 0, "done"

    async def _boom_full_suite_gate(repo_root: Path) -> dict[str, Any]:
        raise AssertionError("the full suite gate must never run when "
                             "osiris_deploy_full_suite_gate is unset (OFF by default, "
                             "the same law as osiris_deploy_chaos_gate)")

    out = await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [], restart=_restart,
                           pool=actions.pool, wait_for_health=_fake_wait_for_health,
                           wait_for_smoke=_fake_wait_for_smoke,
                           check_whisper_probe=_fake_check_whisper_ok,
                           full_suite_gate=_boom_full_suite_gate,
                           deploy_settings=Settings(
                               osiris_deploy_full_suite_gate=False,
                               osiris_deploy_chaos_gate=False))
    assert out == 0


async def test_cmd_deploy_records_normally_when_the_full_suite_gate_holds(
    actions: Actions, tmp_path: Path,
) -> None:
    """thread be24817b: an explicit Settings object arms the flag for THIS call only —
    never ambient env, which a real armed deploy's own subprocess would otherwise inherit
    into every test in the suite it spawns, this one included."""
    async def _restart(units: list[str]) -> tuple[int, str]:
        return 0, "done"

    async def _ok_full_suite_gate(repo_root: Path) -> dict[str, Any]:
        return {"ok": True, "summary": "3891 passed, 1 skipped", "returncode": 0}

    calls: list[Any] = []

    async def _record_deploy(pool: Any, repo_root: Path) -> str | None:
        calls.append(repo_root)
        return "cafef00d"

    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [], restart=_restart,
                               pool=actions.pool, record_deploy=_record_deploy,
                               wait_for_health=_fake_wait_for_health,
                               wait_for_smoke=_fake_wait_for_smoke,
                               check_whisper_probe=_fake_check_whisper_ok,
                               full_suite_gate=_ok_full_suite_gate,
                               deploy_settings=Settings(
                                   osiris_deploy_full_suite_gate=True,
                                   osiris_deploy_chaos_gate=False))
    assert out == 0
    assert calls == [tmp_path]
    assert "full suite: green on the merged tree" in buf.getvalue()


async def test_cmd_deploy_refuses_to_record_when_the_full_suite_gate_finds_a_real_failure(
    actions: Actions, tmp_path: Path,
) -> None:
    async def _restart(units: list[str]) -> tuple[int, str]:
        return 0, "done"

    async def _bad_full_suite_gate(repo_root: Path) -> dict[str, Any]:
        return {"ok": False, "summary": "FAILED tests/test_x.py::test_y - ModuleNotFoundError",
               "returncode": 1}

    async def _unreachable(pool: Any, repo_root: Path) -> str | None:
        raise AssertionError("must never be called — the full suite gate refused first")

    async def _unreachable_chaos(pool: Any) -> dict[str, Any]:
        raise AssertionError("must never be called — the full suite gate refuses BEFORE "
                             "the chaos gate ever runs")

    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [], restart=_restart,
                               pool=actions.pool, record_deploy=_unreachable,
                               wait_for_health=_fake_wait_for_health,
                               wait_for_smoke=_fake_wait_for_smoke,
                               check_whisper_probe=_fake_check_whisper_ok,
                               full_suite_gate=_bad_full_suite_gate,
                               chaos_gate=_unreachable_chaos,
                               deploy_settings=Settings(
                                   osiris_deploy_full_suite_gate=True,
                                   osiris_deploy_chaos_gate=False))
    assert out == 1
    text = buf.getvalue()
    assert "REFUSED" in text and "ModuleNotFoundError" in text
    assert "NOT recording this deploy" in text


# --- CRASH REPLAY AS A GATE (Thoth msg 5338, 2026-08-18) — osiris_deploy_chaos_gate ---------
# OFF by default (every test above ran with it unset — 159 unaffected). These pin the ONE
# new step, fully injected: chaos.py's own control flow has its own dedicated test suite
# (tests/test_chaos.py); this only tests that cmd_deploy WIRES it correctly.

async def test_cmd_deploy_skips_the_chaos_gate_by_default(
    actions: Actions, tmp_path: Path,
) -> None:
    """thread be24817b: an explicitly constructed Settings() pins the claim to the field's
    own default in source, not to whatever the process's ambient environment holds — see
    the full-suite gate's twin test above for the full self-refutation this closes."""
    async def _restart(units: list[str]) -> tuple[int, str]:
        return 0, "done"

    async def _boom_chaos_gate(pool: Any) -> dict[str, Any]:
        raise AssertionError("the chaos gate must never run when osiris_deploy_chaos_gate "
                             "is unset (OFF by default, the same law as osiris_trigger_"
                             "enabled)")

    out = await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [], restart=_restart,
                           pool=actions.pool, wait_for_health=_fake_wait_for_health,
                           wait_for_smoke=_fake_wait_for_smoke,
                           check_whisper_probe=_fake_check_whisper_ok,
                           chaos_gate=_boom_chaos_gate,
                           deploy_settings=Settings(
                               osiris_deploy_chaos_gate=False,
                               osiris_deploy_full_suite_gate=False))
    assert out == 0


async def test_cmd_deploy_records_normally_when_the_chaos_gate_holds(
    actions: Actions, tmp_path: Path,
) -> None:
    """thread be24817b: see the full-suite gate's twin test above — an explicit Settings
    object arms the flag for this call only, never ambient env."""
    async def _restart(units: list[str]) -> tuple[int, str]:
        return 0, "done"

    async def _ok_chaos_gate(pool: Any) -> dict[str, Any]:
        return {"ok": True, "findings": [], "storm_fired": 25, "recovery_elapsed_secs": 3.0}

    calls: list[Any] = []

    async def _record_deploy(pool: Any, repo_root: Path) -> str | None:
        calls.append(repo_root)
        return "cafef00d"

    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [], restart=_restart,
                               pool=actions.pool, record_deploy=_record_deploy,
                               wait_for_health=_fake_wait_for_health,
                               wait_for_smoke=_fake_wait_for_smoke,
                               check_whisper_probe=_fake_check_whisper_ok,
                               chaos_gate=_ok_chaos_gate,
                               deploy_settings=Settings(
                                   osiris_deploy_chaos_gate=True,
                                   osiris_deploy_full_suite_gate=False))
    assert out == 0
    assert calls == [tmp_path]
    assert "chaos replay: all invariants held" in buf.getvalue()

    import json as _json

    cursor = await actions.pool.fetchval(
        "SELECT cursor FROM watermarks WHERE key='chaos-replay:last'")
    assert cursor is not None
    assert _json.loads(cursor)["ok"] is True


async def test_cmd_deploy_refuses_to_record_when_the_chaos_gate_finds_a_real_violation(
    actions: Actions, tmp_path: Path,
) -> None:
    async def _restart(units: list[str]) -> tuple[int, str]:
        return 0, "done"

    async def _bad_chaos_gate(pool: Any) -> dict[str, Any]:
        return {"ok": False, "findings": ["a stranger was minted over a listed body"],
                "storm_fired": 25, "recovery_elapsed_secs": 3.0}

    async def _unreachable(pool: Any, repo_root: Path) -> str | None:
        raise AssertionError("must never be called — the chaos gate refused first")

    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [], restart=_restart,
                               pool=actions.pool, record_deploy=_unreachable,
                               wait_for_health=_fake_wait_for_health,
                               wait_for_smoke=_fake_wait_for_smoke,
                               check_whisper_probe=_fake_check_whisper_ok,
                               chaos_gate=_bad_chaos_gate,
                               deploy_settings=Settings(
                                   osiris_deploy_chaos_gate=True,
                                   osiris_deploy_full_suite_gate=False))
    assert out == 1
    text = buf.getvalue()
    assert "REFUSED" in text and "a stranger was minted over a listed body" in text
    assert "NOT recording this deploy" in text


# --- THE HALCYON GATE (operator ruling 921eabcf, addendum to 6b1efacb, 2026-08-18) ----------
# always-on (no kill switch), runs BEFORE the chaos gate — a cheap read, not a SIGKILL.

async def test_cmd_deploy_refuses_when_a_false_mint_live_specimen_exists(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE HALCYON SPECIMEN ITSELF: a generation carrying false_mint=true with a live
    mount must block a deploy from recording — the deploy-time analogue of graph_lint's
    own `false-mint-live` check, no kill switch (a plain read, always on)."""
    from src.orchestrator import mounts

    victim = await actions.create_or_find_object("Agent", "agent:dh0001", "test")
    await actions.assert_property(victim, "false_mint", True, "test", datetime.now(UTC),
                                  0.9, evidence_class="self_declared")
    await mounts.save_mount(actions.pool, job_dir="/x/jobs/dh0001", agent_id="agent:dh0001",
                            project="demo", cwd="/repo/demo", model=None, session_key=None)

    async def _restart(units: list[str]) -> tuple[int, str]:
        return 0, "done"

    async def _unreachable(pool: Any, repo_root: Path) -> str | None:
        raise AssertionError("must never be called — the false-mint-live gate refused first")

    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [], restart=_restart,
                               pool=actions.pool, record_deploy=_unreachable,
                               wait_for_health=_fake_wait_for_health,
                               wait_for_smoke=_fake_wait_for_smoke,
                               check_whisper_probe=_fake_check_whisper_ok)
    assert out == 1
    text = buf.getvalue()
    assert "REFUSED" in text and "false-mint-live" in text and "agent:dh0001" in text
    assert "NOT recording this deploy" in text


async def test_cmd_deploy_confesses_the_withheld_record_when_head_is_known(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#52's own law (thread 3b34f6c5): a correct refusal to record must not leave the
    ledger silently stale — the halcyon gate above still refuses the RECORD, but this
    proves it also mints the withheld-record confession naming the real HEAD, so a later
    reader of the graph (not just the deploy's own stdout) can tell this HEAD shipped."""
    from src.actions.core import Actions as _Actions
    from src.orchestrator import mounts

    victim = await actions.create_or_find_object("Agent", "agent:dh0002", "test")
    await actions.assert_property(victim, "false_mint", True, "test", datetime.now(UTC),
                                  0.9, evidence_class="self_declared")
    await mounts.save_mount(actions.pool, job_dir="/x/jobs/dh0002", agent_id="agent:dh0002",
                            project="demo", cwd="/repo/demo", model=None, session_key=None)

    import src.orchestrator.deploy_guard as _dg
    monkeypatch.setattr(_dg, "_git_head", lambda root: "deadbeef1234567890")

    async def _restart(units: list[str]) -> tuple[int, str]:
        return 0, "done"

    async def _unreachable(pool: Any, repo_root: Path) -> str | None:
        raise AssertionError("must never be called — the false-mint-live gate refused first")

    out = await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [], restart=_restart,
                           pool=actions.pool, record_deploy=_unreachable,
                           wait_for_health=_fake_wait_for_health,
                           wait_for_smoke=_fake_wait_for_smoke,
                           check_whisper_probe=_fake_check_whisper_ok)
    assert out == 1

    thread = await _Actions(actions.pool).pool.fetchrow(
        "SELECT a.value #>> '{}' AS summary FROM current_assertions a "
        "JOIN objects o ON o.id = a.object_id "
        "WHERE o.type='Thread' AND a.name='summary' "
        "AND a.value #>> '{}' ILIKE 'DEPLOY RECORD WITHHELD%'")
    assert thread is not None
    assert "deadbeef1234567890" in thread["summary"]
    assert "agent:dh0002" in thread["summary"]


# --- cmd_smoke_chaos — the standalone `osiris smoke --chaos` entry point --------------------

async def test_cmd_smoke_chaos_records_the_ledger_and_prints_the_findings(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_real_chaos_gate(pool: Any) -> dict[str, Any]:
        return {"ok": False, "findings": ["1 advisory lock(s) held after recovery"],
                "storm_fired": 10, "recovery_elapsed_secs": 2.0,
                "automount_probes_total": 4}

    import src.cli as cli_mod
    monkeypatch.setattr(cli_mod, "_real_chaos_gate", _fake_real_chaos_gate)

    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_smoke_chaos(pool=actions.pool)
    assert out == 1
    assert "1 advisory lock(s) held after recovery" in buf.getvalue()

    cursor = await actions.pool.fetchval(
        "SELECT cursor FROM watermarks WHERE key='chaos-replay:last'")
    assert cursor is not None
    import json as _json
    assert _json.loads(cursor)["ok"] is False


# --- _synthetic_automount_probe: the pure verdict logic, mocked transport ------------------

async def test_synthetic_automount_probe_ok_on_a_clean_200() -> None:
    import httpx

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"mounted": True})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(_handler), base_url="http://test",
    ) as client:
        ok, note = await _synthetic_automount_probe(client)
    assert ok is True
    assert "round-tripped clean" in note


async def test_synthetic_automount_probe_refuses_on_a_non_200() -> None:
    import httpx

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/automount":
            return httpx.Response(500, text="internal error")
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(_handler), base_url="http://test",
    ) as client:
        ok, note = await _synthetic_automount_probe(client)
    assert ok is False
    assert "REFUSED" in note and "500" in note


async def test_synthetic_automount_probe_refuses_on_a_200_with_an_error_body(
) -> None:
    """The exact silent-failure shape 33a3573 fixed once already (task #179's own headline):
    a route that degrades to 200-with-error must never read as success here."""
    import httpx

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/automount":
            return httpx.Response(200, json={"error": "something went wrong"})
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(_handler), base_url="http://test",
    ) as client:
        ok, note = await _synthetic_automount_probe(client)
    assert ok is False
    assert "something went wrong" in note


async def test_synthetic_automount_probe_refuses_on_a_network_failure() -> None:
    import httpx

    def _handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Connection refused")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(_handler), base_url="http://test",
    ) as client:
        ok, note = await _synthetic_automount_probe(client)
    assert ok is False
    assert "REFUSED" in note


async def test_synthetic_automount_probe_still_refuses_when_cleanup_itself_fails() -> None:
    """The /session-end cleanup call is best-effort — its own failure must never flip a
    genuinely successful /automount into a false refusal, and must never crash the probe."""
    import httpx

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/automount":
            return httpx.Response(200, json={"mounted": True})
        raise httpx.ConnectError("session-end unreachable")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(_handler), base_url="http://test",
    ) as client:
        ok, note = await _synthetic_automount_probe(client)
    assert ok is True
    assert "round-tripped clean" in note


async def test_cmd_deploy_refuses_on_a_dirty_src_tree(actions: Actions, tmp_path: Path) -> None:
    def _dirty(root: Path) -> list[tuple[str, str]]:
        return [(" M", "src/orchestrator/handshake.py")]

    async def _unreachable(units: list[str]) -> tuple[int, str]:
        raise AssertionError("must never be called — the dirty guard refuses first")

    out = await cmd_deploy(repo_root=tmp_path, git_status=_dirty, restart=_unreachable,
                           pool=actions.pool)
    assert out == 1


def test_find_repo_root_outside_any_repo_is_none(tmp_path: Path) -> None:
    assert _find_repo_root(tmp_path) is None


def test_find_repo_root_finds_this_repo_from_a_subdirectory() -> None:
    here = Path(__file__).resolve().parent  # tests/, a subdirectory of the repo root
    root = _find_repo_root(here)
    assert root is not None
    assert (root / "pyproject.toml").is_file()


async def test_cmd_deploy_restarts_and_reports_smoke_and_gaps(
    actions: Actions, tmp_path: Path,
) -> None:
    calls: list[list[str]] = []
    tool_snapshots = iter([{"smoke": "aaa"}, {"smoke": "bbb", "retire_assertion": "ccc"}])

    async def _restart(units: list[str]) -> tuple[int, str]:
        calls.append(units)
        return 0, "done"

    async def _list_tools() -> dict[str, str]:
        return next(tool_snapshots)

    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [], restart=_restart,
                               pool=actions.pool, list_tools=_list_tools,
                               wait_for_health=_fake_wait_for_health,
                               wait_for_smoke=_fake_wait_for_smoke,
                         check_whisper_probe=_fake_check_whisper_ok)
    assert calls == [list(DEPLOY_UNITS)]
    # a blank test DB has no compositions/alembic_version rows seeded — this exercises the
    # gap-reporting path without asserting exact names (that's composition_gap_notes' own
    # unit test's job); only that cmd_deploy runs the comparison and returns cleanly either way.
    assert out in (0, 1)
    assert "TOOL LIST CHANGED: +retire_assertion, ~smoke changed" in buf.getvalue()


async def test_cmd_deploy_restart_failure_is_honest(actions: Actions, tmp_path: Path) -> None:
    async def _failing_restart(units: list[str]) -> tuple[int, str]:
        return 1, "Unit osiris-mcp.service not found."

    out = await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [],
                           restart=_failing_restart, pool=actions.pool)
    assert out == 1


# --- osiris deploy records the ledger a boot-time reboot guard confesses against (489a39d0) ----

async def test_cmd_deploy_records_the_deployed_head_on_a_successful_restart(
    actions: Actions, tmp_path: Path,
) -> None:
    calls: list[tuple[Any, Path]] = []

    async def _restart(units: list[str]) -> tuple[int, str]:
        return 0, "done"

    async def _record_deploy(pool: Any, repo_root: Path) -> str | None:
        calls.append((pool, repo_root))
        return "deadbeef"

    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [], restart=_restart,
                         pool=actions.pool, record_deploy=_record_deploy,
                         wait_for_health=_fake_wait_for_health,
                         wait_for_smoke=_fake_wait_for_smoke,
                         check_whisper_probe=_fake_check_whisper_ok)
    assert calls == [(actions.pool, tmp_path)]
    assert "deploy ledger: recorded deadbeef" in buf.getvalue()
    # #189 adoption meter (Thoth msg 5825) prints on every successful deploy, unconditionally
    assert "adoption189:" in buf.getvalue()


async def test_cmd_deploy_never_records_when_the_restart_fails(
    actions: Actions, tmp_path: Path,
) -> None:
    async def _failing_restart(units: list[str]) -> tuple[int, str]:
        return 1, "Unit osiris-mcp.service not found."

    async def _unreachable(pool: Any, repo_root: Path) -> str | None:
        raise AssertionError("must never be called — the restart never succeeded")

    out = await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [],
                           restart=_failing_restart, pool=actions.pool,
                           record_deploy=_unreachable)
    assert out == 1


async def test_cmd_deploy_reports_head_unknown_off_a_non_git_root(
    actions: Actions, tmp_path: Path,
) -> None:
    """The default `_real_record_deploy` against a `tmp_path` (never a git checkout) — a
    real end-to-end exercise of the fail-open write side, not a fake."""
    async def _restart(units: list[str]) -> tuple[int, str]:
        return 0, "done"

    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [], restart=_restart,
                         pool=actions.pool, wait_for_health=_fake_wait_for_health,
                         wait_for_smoke=_fake_wait_for_smoke,
                         check_whisper_probe=_fake_check_whisper_ok)
    assert "deploy ledger: HEAD unknown — not recorded" in buf.getvalue()


# --- osiris migrate + osiris deploy's migration gate (thread c4681c38) ---------------------------
# a fake `state`/`run_migrations` pair instead of a real alembic.ini or a real DB revision —
# never risk running a real upgrade, or mutating the shared test DB's alembic_version row.

async def test_cmd_migrate_undeterminable_head_is_honest(actions: Actions, tmp_path: Path) -> None:
    async def _state(pool: Any, root: Path) -> tuple[str | None, str | None]:
        return None, None  # no alembic.ini under this repo_root

    out = await cmd_migrate(repo_root=tmp_path, pool=actions.pool, state=_state)
    assert out == 1


async def test_cmd_migrate_up_to_date_never_runs_anything(
    actions: Actions, tmp_path: Path,
) -> None:
    async def _state(pool: Any, root: Path) -> tuple[str | None, str | None]:
        return "0038", "0038"

    async def _unreachable(root: Path) -> None:
        raise AssertionError("must never be called — nothing is pending")

    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_migrate(repo_root=tmp_path, pool=actions.pool, state=_state,
                                run_migrations=_unreachable)
    assert out == 0
    assert "up to date" in buf.getvalue()


async def test_cmd_migrate_check_reports_without_applying(
    actions: Actions, tmp_path: Path,
) -> None:
    async def _state(pool: Any, root: Path) -> tuple[str | None, str | None]:
        return "0034", "0038"

    async def _unreachable(root: Path) -> None:
        raise AssertionError("--check must never apply anything")

    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_migrate(repo_root=tmp_path, pool=actions.pool, state=_state,
                                run_migrations=_unreachable, check=True)
    assert out == 1
    assert "PENDING" in buf.getvalue()
    assert "0034" in buf.getvalue() and "0038" in buf.getvalue()


async def test_cmd_migrate_applies_when_pending(actions: Actions, tmp_path: Path) -> None:
    async def _state(pool: Any, root: Path) -> tuple[str | None, str | None]:
        return "0034", "0038"

    calls: list[Path] = []

    async def _run(root: Path) -> None:
        calls.append(root)

    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_migrate(repo_root=tmp_path, pool=actions.pool, state=_state,
                                run_migrations=_run)
    assert out == 0
    assert calls == [tmp_path]
    assert "applied" in buf.getvalue()


async def test_cmd_migrate_upgrade_failure_is_honest(actions: Actions, tmp_path: Path) -> None:
    async def _state(pool: Any, root: Path) -> tuple[str | None, str | None]:
        return "0034", "0038"

    async def _run(root: Path) -> None:
        raise RuntimeError("could not connect to server")

    out = await cmd_migrate(repo_root=tmp_path, pool=actions.pool, state=_state,
                            run_migrations=_run)
    assert out == 1


async def test_apply_pending_migrations_names_an_unrecognized_revision(
    actions: Actions,
) -> None:
    """Decision 8d3f5e2d, task #142 follow-up: a DB carrying a revision this tree's own
    alembic chain has never heard of used to be refused only by ACCIDENT — alembic's own
    `command.upgrade` errors on it, and the generic except around `run_migrations` reported
    whatever opaque text that exception happened to carry. This checks it explicitly first,
    with the real reason named, and never even attempts the upgrade. Real repo_root (this
    checkout has a real alembic.ini) with a synthetic revision no script defines — never a
    real DB or a real upgrade."""
    async def _state(pool: Any, root: Path) -> tuple[str | None, str | None]:
        return "0099_unmerged_branch_revision", "0045"

    async def _unreachable(root: Path) -> None:
        raise AssertionError(
            "must never attempt the upgrade once the revision is unrecognized")

    repo_root = Path(__file__).resolve().parent.parent
    ok, note = await _apply_pending_migrations(
        actions.pool, repo_root, state=_state, run_migrations=_unreachable)
    assert ok is False
    assert "8d3f5e2d" in note
    assert "0099_unmerged_branch_revision" in note
    assert "do not recognize" in note


async def test_apply_pending_migrations_still_falls_through_when_undeterminable(
    actions: Actions, tmp_path: Path,
) -> None:
    """`known is None` (no alembic.ini under this repo_root, e.g. a bare tmp_path fixture)
    must fall through to the EXISTING try/run_migrations path unchanged — this is the
    backward-compatibility guarantee for every pre-existing caller of this function."""
    async def _state(pool: Any, root: Path) -> tuple[str | None, str | None]:
        return "0037", "0038"

    calls: list[Path] = []

    async def _run(root: Path) -> None:
        calls.append(root)

    ok, note = await _apply_pending_migrations(
        actions.pool, tmp_path, state=_state, run_migrations=_run)
    assert ok is True
    assert calls == [tmp_path]
    assert "applied" in note


async def test_cmd_deploy_applies_pending_migrations_before_restarting(
    actions: Actions, tmp_path: Path,
) -> None:
    """The whole point of leg 2: a deploy is atomic from the schema's point of view — the
    migration must run and land BEFORE anything restarts, never after."""
    order: list[str] = []

    async def _state(pool: Any, root: Path) -> tuple[str | None, str | None]:
        return "0037", "0038"

    async def _run(root: Path) -> None:
        order.append("migrate")

    async def _restart(units: list[str]) -> tuple[int, str]:
        order.append("restart")
        return 0, "done"

    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [], restart=_restart,
                         pool=actions.pool, migration_state=_state, run_migrations=_run,
                         wait_for_health=_fake_wait_for_health,
                         wait_for_smoke=_fake_wait_for_smoke,
                         check_whisper_probe=_fake_check_whisper_ok)
    assert order == ["migrate", "restart"]
    assert "0037" in buf.getvalue() and "0038" in buf.getvalue() and "applied" in buf.getvalue()


async def test_cmd_deploy_refuses_when_migration_fails_and_never_restarts(
    actions: Actions, tmp_path: Path,
) -> None:
    async def _state(pool: Any, root: Path) -> tuple[str | None, str | None]:
        return "0037", "0038"

    async def _failing_run(root: Path) -> None:
        raise RuntimeError("could not connect to server")

    async def _unreachable(units: list[str]) -> tuple[int, str]:
        raise AssertionError("must never be called — the migration gate refuses first")

    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [],
                               restart=_unreachable, pool=actions.pool,
                               migration_state=_state, run_migrations=_failing_run)
    assert out == 1
    assert "REFUSED" in buf.getvalue()


async def _casefold_twin(actions: Actions, upper: str, lower: str) -> None:
    """A minimal casefold-twin pair: `upper` populated (a real live link), `lower` the
    phantom — the exact shape casefold_auto_merge_candidates keys on."""
    now = datetime.now(UTC)
    populated = await actions.create_or_find_object("SoftwareProject", f"repo:{upper}", "test")
    await actions.assert_property(populated, "name", upper, "test", now, 0.9)
    await actions.create_or_find_object("SoftwareProject", f"repo:{lower}", "test")
    t = await actions.create_or_find_object("Thread", f"thread:deploy-casefold-{upper}", "test")
    await actions.create_link(t, populated, "in_repo", "agent:test", now, 0.9)


async def test_cmd_deploy_casefold_automerge_executes_by_default(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#108 piece 2 wiring, default flipped (operator's word: "automatic, not
    bottlenecked by me" — `osiris deploy` is hand-invoked with no wrapper/cron to hang a
    deploy-env-only flag on, so the flip IS the default): a run with no
    OSIRIS_CASEFOLD_AUTOMERGE set must EXECUTE, not just survey."""
    monkeypatch.delenv("OSIRIS_CASEFOLD_AUTOMERGE", raising=False)
    await _casefold_twin(actions, "DeployTwinA", "deploytwina")

    import io
    from contextlib import redirect_stdout

    async def _restart(units: list[str]) -> tuple[int, str]:
        return 0, "done"

    buf = io.StringIO()
    with redirect_stdout(buf):
        await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [], restart=_restart,
                         pool=actions.pool, wait_for_health=_fake_wait_for_health,
                         wait_for_smoke=_fake_wait_for_smoke,
                         check_whisper_probe=_fake_check_whisper_ok)
    text = buf.getvalue()
    assert "casefold auto-merge: EXECUTED — 1 candidate(s)" in text
    assert "deploytwina -> repo:DeployTwinA" in text

    row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical='repo:deploytwina'")
    assert row["status"] == "merged"


async def test_cmd_deploy_casefold_automerge_opts_out_under_the_env_flag(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OSIRIS_CASEFOLD_AUTOMERGE=0 is the escape hatch — a dry-run-only deploy, the
    inverse of the old opt-IN flag now that execute is the default."""
    monkeypatch.setenv("OSIRIS_CASEFOLD_AUTOMERGE", "0")
    await _casefold_twin(actions, "DeployTwinB", "deploytwinb")

    import io
    from contextlib import redirect_stdout

    async def _restart(units: list[str]) -> tuple[int, str]:
        return 0, "done"

    buf = io.StringIO()
    with redirect_stdout(buf):
        await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [], restart=_restart,
                         pool=actions.pool, wait_for_health=_fake_wait_for_health,
                         wait_for_smoke=_fake_wait_for_smoke,
                         check_whisper_probe=_fake_check_whisper_ok)
    assert "casefold auto-merge: dry-run — 1 candidate(s)" in buf.getvalue()

    row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical='repo:deploytwinb'")
    assert row["status"] == "active", "OSIRIS_CASEFOLD_AUTOMERGE=0 must never fold anything"


async def _remote_url_dupe(actions: Actions, basename: str) -> None:
    """A minimal remote_url-matched pair sharing a basename but NOT a case-fold twin —
    the exact #107 shape (path-shaped vs bare) remote_url_duplicate_candidates keys on."""
    now = datetime.now(UTC)
    path_id = await actions.create_or_find_object(
        "SoftwareProject", f"repo:/home/x/code/REPOS/{basename}", "test")
    bare_id = await actions.create_or_find_object("SoftwareProject", f"repo:{basename}", "test")
    url = f"https://github.com/x/{basename}.git"
    await actions.assert_property(path_id, "remote_url", url, "test", now, 0.9)
    await actions.assert_property(bare_id, "remote_url", url, "test", now, 0.9)
    t = await actions.create_or_find_object("Thread", f"thread:deploy-remoteurl-{basename}", "test")
    await actions.create_link(t, bare_id, "in_repo", "agent:test", now, 0.9)


async def test_cmd_deploy_remote_url_automerge_executes_by_default(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#108 piece 3 wiring (decision 2ee34a9d): the SAME env flag as piece 2 gates both
    steps — no OSIRIS_CASEFOLD_AUTOMERGE set must EXECUTE this step too, not just casefold's."""
    monkeypatch.delenv("OSIRIS_CASEFOLD_AUTOMERGE", raising=False)
    await _remote_url_dupe(actions, "deployremotea")

    import io
    from contextlib import redirect_stdout

    async def _restart(units: list[str]) -> tuple[int, str]:
        return 0, "done"

    buf = io.StringIO()
    with redirect_stdout(buf):
        await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [], restart=_restart,
                         pool=actions.pool, wait_for_health=_fake_wait_for_health,
                         wait_for_smoke=_fake_wait_for_smoke,
                         check_whisper_probe=_fake_check_whisper_ok)
    text = buf.getvalue()
    assert "remote_url auto-merge: EXECUTED — 1 candidate(s)" in text
    assert "/home/x/code/REPOS/deployremotea -> repo:deployremotea" in text

    row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical='repo:/home/x/code/REPOS/deployremotea'")
    assert row["status"] == "merged"


async def test_cmd_deploy_remote_url_automerge_opts_out_under_the_env_flag(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_CASEFOLD_AUTOMERGE", "0")
    await _remote_url_dupe(actions, "deployremoteb")

    import io
    from contextlib import redirect_stdout

    async def _restart(units: list[str]) -> tuple[int, str]:
        return 0, "done"

    buf = io.StringIO()
    with redirect_stdout(buf):
        await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [], restart=_restart,
                         pool=actions.pool, wait_for_health=_fake_wait_for_health,
                         wait_for_smoke=_fake_wait_for_smoke,
                         check_whisper_probe=_fake_check_whisper_ok)
    assert "remote_url auto-merge: dry-run — 1 candidate(s)" in buf.getvalue()

    row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical='repo:/home/x/code/REPOS/deployremoteb'")
    assert row["status"] == "active", "OSIRIS_CASEFOLD_AUTOMERGE=0 must never fold anything"


# --- dev-box systemd USER units are repo-managed (thread e6fd3772 piece 3-infra) ---------------
# `osiris deploy` installs deploy/user/*.service over ~/.config/systemd/user/ before restarting
# — these units were previously five hand-installed files this box's own operator diverged from
# the /opt SYSTEM templates by hand; nothing in git was ever the box's actual running config.

_REQUIRED_UNIT_ENV = {
    "osiris-mcp.service": [
        "DATABASE_URL=", "application_name=osiris-mcp", "REDIS_URL=",
        "OOMScoreAdjust=-500",  # thread #175: the 19:08:32 CDT system-wide OOM took this
        # daemon down alongside osiris-worker even though neither was the actual hog.
    ],
    "osiris-worker.service": [
        "DATABASE_URL=", "application_name=osiris-worker", "REDIS_URL=",
        "OSIRIS_WORKER_ROLE=primary", "OSIRIS_PHANTOM_HEAL_ENABLED=1",
        "OOMScoreAdjust=-500",  # thread #175, systemd's own "Failed with result 'oom-kill'"
        "HF_HUB_OFFLINE=1",  # thread 5cd49217: cached model, skip the HF CDN round-trip
    ],
    "osiris-console.service": ["DATABASE_URL=", "application_name=osiris-console", "REDIS_URL="],
    "osiris-pulse.service": ["DATABASE_URL=", "application_name=osiris-pulse", "REDIS_URL="],
}


def test_deploy_user_units_contract_every_required_env_line_present() -> None:
    """Reads the REAL deploy/user/*.service files this repo ships (not a fixture standing in
    for them) — a contract test in the literal sense: if a future edit drops
    OSIRIS_WORKER_ROLE=primary or an application_name tag, this fails at the source of truth,
    not at a copy of it."""
    repo_root = _find_repo_root(Path(__file__).resolve().parent)
    assert repo_root is not None
    unit_dir = repo_root / "deploy" / "user"
    present = {p.name for p in unit_dir.glob("*.service")}
    assert present == set(_REQUIRED_UNIT_ENV), "a new/removed unit must update this contract too"
    for name, required_substrings in _REQUIRED_UNIT_ENV.items():
        text = (unit_dir / name).read_text()
        for needle in required_substrings:
            assert needle in text, f"{name} is missing {needle!r}"


def test_user_unit_sources_discovers_the_real_deploy_user_dir() -> None:
    repo_root = _find_repo_root(Path(__file__).resolve().parent)
    assert repo_root is not None
    names = {p.name for p in user_unit_sources(repo_root)}
    assert names == set(_REQUIRED_UNIT_ENV)


def test_user_unit_sources_missing_dir_is_empty(tmp_path: Path) -> None:
    assert user_unit_sources(tmp_path) == []


async def test_real_install_user_units_no_deploy_user_dir_touches_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default installer must never mkdir into a real caller's home when the repo it was
    handed carries no deploy/user/ — this is what every OTHER cmd_deploy test in this file
    relies on implicitly (none of them pass install_units, all of them pass repo_root=tmp_path)."""
    fake_home = tmp_path / "not-a-real-home"
    monkeypatch.setattr(Path, "home", lambda: fake_home)

    notes = await _real_install_user_units(tmp_path)

    assert notes == ["unit files: no deploy/user/ found — nothing to install"]
    assert not fake_home.exists()


async def test_real_install_user_units_installs_new_files_and_reloads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_home = tmp_path / "home"
    src_dir = tmp_path / "repo" / "deploy" / "user"
    src_dir.mkdir(parents=True)
    (src_dir / "osiris-mcp.service").write_text("[Service]\nExecStart=/bin/true\n")
    monkeypatch.setattr(Path, "home", lambda: fake_home)

    reload_calls: list[list[str]] = []

    async def _fake_exec(*args: str, **kwargs: Any) -> Any:
        reload_calls.append(list(args))

        class _Proc:
            returncode = 0

            async def communicate(self) -> tuple[bytes, bytes]:
                return b"", b""

        return _Proc()

    monkeypatch.setattr("asyncio.create_subprocess_exec", _fake_exec)

    notes = await _real_install_user_units(tmp_path / "repo")

    dest = fake_home / ".config" / "systemd" / "user" / "osiris-mcp.service"
    assert dest.read_text() == "[Service]\nExecStart=/bin/true\n"
    assert any("installed (new) osiris-mcp.service" in n for n in notes)
    assert reload_calls == [["systemctl", "--user", "daemon-reload"]]


async def test_real_install_user_units_unchanged_content_skips_daemon_reload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_home = tmp_path / "home"
    src_dir = tmp_path / "repo" / "deploy" / "user"
    src_dir.mkdir(parents=True)
    (src_dir / "osiris-mcp.service").write_text("same\n")
    dest_dir = fake_home / ".config" / "systemd" / "user"
    dest_dir.mkdir(parents=True)
    (dest_dir / "osiris-mcp.service").write_text("same\n")
    monkeypatch.setattr(Path, "home", lambda: fake_home)

    async def _unreachable(*args: str, **kwargs: Any) -> Any:
        raise AssertionError("daemon-reload must never fire when nothing changed")

    monkeypatch.setattr("asyncio.create_subprocess_exec", _unreachable)

    notes = await _real_install_user_units(tmp_path / "repo")
    assert notes == ["unit files: unchanged, no daemon-reload needed"]


async def test_cmd_deploy_calls_install_units_before_restarting(
    actions: Actions, tmp_path: Path,
) -> None:
    order: list[str] = []

    async def _install(repo_root: Path) -> list[str]:
        order.append("install")
        return ["unit: installed (new) osiris-mcp.service"]

    async def _restart(units: list[str]) -> tuple[int, str]:
        order.append("restart")
        return 0, "done"

    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [], restart=_restart,
                         pool=actions.pool, install_units=_install,
                         wait_for_health=_fake_wait_for_health,
                         wait_for_smoke=_fake_wait_for_smoke,
                         check_whisper_probe=_fake_check_whisper_ok)
    assert order == ["install", "restart"]
    assert "unit: installed (new) osiris-mcp.service" in buf.getvalue()


async def test_cmd_deploy_refuses_on_a_silent_install_units_no_op(
    actions: Actions, tmp_path: Path,
) -> None:
    """The live specimen this guards against: a real deploy restarted onto a stale unit set
    while install_units printed ZERO `unit:` lines and the deploy still exited 0 (Thoth's
    dispatch msg 5161, gate24's captured log). deploy/user/ carrying real files but
    install_units reporting nothing must REFUSE before the restart, not restart blind."""
    (tmp_path / "deploy" / "user").mkdir(parents=True)
    (tmp_path / "deploy" / "user" / "osiris-mcp.service").write_text("[Service]\n")

    async def _silent_no_op(repo_root: Path) -> list[str]:
        return []

    async def _unreachable(units: list[str]) -> tuple[int, str]:
        raise AssertionError("must never be called — the silent no-op guard refuses first")

    import io
    from contextlib import redirect_stderr

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_deploy(repo_root=tmp_path, git_status=lambda root: [],
                               restart=_unreachable, pool=actions.pool,
                               install_units=_silent_no_op)
    assert out == 1
    assert "silent no-op" in buf.getvalue()


# --- boot-status -------------------------------------------------------------------------------

async def test_cmd_boot_status_clean_on_a_blank_db(actions: Actions) -> None:
    """A blank test DB has no active Seats at all — no seats means no gaps, same "silent
    when there is nothing to name" contract as composition_gap_notes on a caught-up DB."""
    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_boot_status(pool=actions.pool)
    assert out == 0
    assert "every active seat carries a compiled managed section" in buf.getvalue()


async def test_cmd_boot_status_names_a_gap_and_exits_nonzero(
    actions: Actions, tmp_path: Path,
) -> None:
    await ensure_seat(actions, house="clihouse", handle="CliGapSeat",
                      anchor_cwd=str(tmp_path / "cligap"), source="test")
    (tmp_path / "cligap").mkdir()
    (tmp_path / "cligap" / "CLAUDE.md").write_text("# never compiled\n")

    import io
    from contextlib import redirect_stdout

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_boot_status(pool=actions.pool)
    assert out == 1
    assert "CliGapSeat" in buf.getvalue()
    assert "reissue_office(adopt=True)" in buf.getvalue()


# --- fold-project: the sanctioned second door (thread 2446) — calls the SAME
# projects.fold_project the MCP wrapper calls, gate untouched -------------------------------

async def _cli_stub_project(actions: Actions, canon: str, name: str) -> None:
    from datetime import UTC, datetime

    pid = await actions.create_or_find_object("SoftwareProject", canon, "test")
    await actions.assert_property(pid, "name", name, "test", datetime.now(UTC), 0.9)


async def test_cmd_fold_project_folds_and_reports(actions: Actions) -> None:
    import io
    from contextlib import redirect_stdout

    await _cli_stub_project(actions, "repo:clidupe1", "clidupe1")
    await _cli_stub_project(actions, "repo:cliinto1", "cliinto1")

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_fold_project("clidupe1", "cliinto1", "both mint the same repo",
                                     actor="operator", pool=actions.pool)
    assert out == 0
    assert "folded repo:clidupe1 into repo:cliinto1" in buf.getvalue()
    row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical='repo:clidupe1'")
    assert row["status"] == "merged"
    # THE RECEIPT DIVERGENCE (thread 2474): two doors onto one function must return the
    # same evidence — the merge event and same_as link the MCP wrapper surfaces must
    # print here too, not just live in the graph unreported.
    dupe_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:clidupe1'")
    into_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:cliinto1'")
    event_id = await actions.pool.fetchval(
        "SELECT id FROM object_events WHERE object_id=$1 AND related_id=$2 "
        "AND event_type='merge'", into_oid, dupe_oid)
    link_id = await actions.pool.fetchval(
        "SELECT id FROM links WHERE from_id=$1 AND to_id=$2 AND type='same_as'",
        dupe_oid, into_oid)
    assert f"merge event: {event_id}" in buf.getvalue()
    assert f"same_as link: {link_id}" in buf.getvalue()


async def test_cmd_fold_project_does_not_soften_the_contradiction_gate(
    actions: Actions,
) -> None:
    """The console-script door must not become a second, weaker path to the same act —
    same test shape as the MCP wrapper's own negative control."""
    import io
    from contextlib import redirect_stderr
    from datetime import UTC, datetime

    await _cli_stub_project(actions, "repo:clicd1", "clicd1")
    await _cli_stub_project(actions, "repo:clicd2", "clicd2")
    cd1 = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:clicd1'")
    cd2 = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:clicd2'")
    now = datetime.now(UTC)
    await actions.assert_property(cd1, "language", "python", "agent:alice", now, 0.9)
    await actions.assert_property(cd2, "language", "go", "agent:bob", now, 0.9)

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_fold_project("clicd1", "clicd2", "looks like a twin",
                                     actor="operator", pool=actions.pool)
    assert out == 1
    assert "contradicting values" in buf.getvalue() and "language" in buf.getvalue()
    row = await actions.pool.fetchrow("SELECT status FROM objects WHERE canonical='repo:clicd1'")
    assert row["status"] == "active"


async def test_cmd_fold_project_refuses_blank_evidence(actions: Actions) -> None:
    import io
    from contextlib import redirect_stderr

    await _cli_stub_project(actions, "repo:clibe1", "clibe1")
    await _cli_stub_project(actions, "repo:clibe2", "clibe2")

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_fold_project("clibe1", "clibe2", "  ", actor="operator",
                                     pool=actions.pool)
    assert out == 1
    assert "auto-merge wearing a signature" in buf.getvalue()


async def test_cli_parser_accepts_fold_project(actions: Actions) -> None:
    """argparse wiring: dupe/into positional, --evidence/--actor required."""
    from src.cli import _build_parser

    args = _build_parser().parse_args(
        ["fold-project", "a", "b", "--evidence", "e", "--actor", "operator"])
    assert args.command == "fold-project"
    assert (args.dupe, args.into, args.evidence, args.actor) == ("a", "b", "e", "operator")


# --- charter-for: the sanctioned second door (thread 2474) — calls the SAME
# charter.charter_for the MCP wrapper calls, guard untouched -------------------------------

async def _cli_repo(actions: Actions, name: str) -> None:
    await actions.create_or_find_object("SoftwareProject", f"repo:{name}", "test")


async def test_cmd_charter_for_the_manager_declares_and_reports(actions: Actions) -> None:
    import io
    from contextlib import redirect_stdout

    from src.orchestrator.seats import attach_seat, bind_holder, ensure_seat

    await _cli_repo(actions, "osiris")
    manager = await ensure_seat(actions, house="clihouse", handle="CliManager1",
                                source="test")
    worker = await ensure_seat(actions, house="clihouse", handle="CliWorker1", source="test")
    await attach_seat(actions, worker["seat_id"], manager["seat_id"], evidence="org chart",
                      actor="test")
    await bind_holder(actions, seat_id=manager["seat_id"], agent_id="agent:climanager1")

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_charter_for(worker["seat_id"], ["osiris"], "onboarding",
                                    actor="agent:climanager1", pool=actions.pool)
    assert out == 0
    assert f"charter for {worker['seat_id']}" in buf.getvalue() and "osiris" in buf.getvalue()
    assert "because: onboarding  declared by: agent:climanager1" in buf.getvalue()
    from src.orchestrator.charter import charter_of

    assert await charter_of(actions.pool, worker["seat_id"]) == ["osiris"]


async def test_cmd_charter_for_refuses_a_non_manager(actions: Actions) -> None:
    import io
    from contextlib import redirect_stderr

    from src.orchestrator.seats import attach_seat, bind_holder, ensure_seat

    await _cli_repo(actions, "osiris")
    manager = await ensure_seat(actions, house="clihouse", handle="CliManager2",
                                source="test")
    worker = await ensure_seat(actions, house="clihouse", handle="CliWorker2", source="test")
    await attach_seat(actions, worker["seat_id"], manager["seat_id"], evidence="org chart",
                      actor="test")
    stranger = await ensure_seat(actions, house="clihouse", handle="CliStranger2",
                                 source="test")
    await bind_holder(actions, seat_id=stranger["seat_id"], agent_id="agent:clistranger2")

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_charter_for(worker["seat_id"], ["osiris"], "unauthorized try",
                                    actor="agent:clistranger2", pool=actions.pool)
    assert out == 1
    assert "not authorized" in buf.getvalue()
    from src.orchestrator.charter import charter_of

    assert await charter_of(actions.pool, worker["seat_id"]) == []


async def test_cmd_charter_for_operator_actor_bypasses_managed_by(actions: Actions) -> None:
    import io
    from contextlib import redirect_stdout

    from src.orchestrator.seats import ensure_seat

    await _cli_repo(actions, "osiris")
    worker = await ensure_seat(actions, house="clihouse", handle="CliWorker3", source="test")

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_charter_for(worker["seat_id"], ["osiris"], "operator backfill",
                                    actor="operator", pool=actions.pool)
    assert out == 0
    from src.orchestrator.charter import charter_of

    assert await charter_of(actions.pool, worker["seat_id"]) == ["osiris"]


async def test_cli_parser_accepts_charter_for(actions: Actions) -> None:
    """argparse wiring: seat positional, --repos/--because/--actor required (comma-split
    happens in main(), not the parser — args.repos stays the raw string here)."""
    from src.cli import _build_parser

    args = _build_parser().parse_args(
        ["charter-for", "seat:abc12345", "--repos", "a,b,c",
         "--because", "onboarding", "--actor", "operator"])
    assert args.command == "charter-for"
    assert args.seat == "seat:abc12345"
    assert args.repos == "a,b,c"
    assert args.because == "onboarding"
    assert args.actor == "operator"


async def test_cmd_amend_practice_amends_and_reports(actions: Actions) -> None:
    import io
    from contextlib import redirect_stdout

    from src.orchestrator.capture import practice_amendments, record_practice

    p = await record_practice(actions, "always vendor the lockfile before a release")

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_amend_practice(str(p), "confirmed live on gestalt, 2026-08-02",
                                       actor="agent:cliamender1", pool=actions.pool)
    assert out == 0
    assert f"amended {p}" in buf.getvalue()
    assert "confirmed live on gestalt, 2026-08-02" in buf.getvalue()

    amendments = await practice_amendments(actions.pool, p)
    assert [a["amendment"] for a in amendments] == ["confirmed live on gestalt, 2026-08-02"]
    assert amendments[0]["source"] == "agent:cliamender1"


async def test_cmd_amend_practice_refuses_a_refuted_practice(actions: Actions) -> None:
    import io
    from contextlib import redirect_stderr

    from src.orchestrator.capture import practice_amendments, record_practice, refute_practice

    p = await record_practice(actions, "always retry twice on any timeout")
    await refute_practice(actions, str(p), killed_by="decision:fix456")

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_amend_practice(str(p), "one more thought on the old lesson",
                                       actor="agent:cliamender2", pool=actions.pool)
    assert out == 1
    assert "refused" in buf.getvalue() and "refuted" in buf.getvalue()
    assert await practice_amendments(actions.pool, p) == []


async def test_cmd_amend_practice_refuses_no_match(actions: Actions) -> None:
    import io
    from contextlib import redirect_stderr

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_amend_practice("no such practice anywhere", "an amendment",
                                       actor="agent:cliamender3", pool=actions.pool)
    assert out == 1
    assert "no practice matches" in buf.getvalue()


async def test_cli_parser_accepts_amend_practice(actions: Actions) -> None:
    """argparse wiring: ref + amendment positionals, --actor required."""
    from src.cli import _build_parser

    args = _build_parser().parse_args(
        ["amend-practice", "practice:abc12345", "narrowed to its residual window",
         "--actor", "operator"])
    assert args.command == "amend-practice"
    assert args.ref == "practice:abc12345"
    assert args.amendment == "narrowed to its residual window"
    assert args.actor == "operator"


# --- annotate-thread: the fourth sanctioned second door (thread 2474) — calls the SAME
# capture.annotate_thread the MCP wrapper calls, guard untouched -------------------------------

async def test_cmd_annotate_thread_annotates_and_reports(actions: Actions) -> None:
    import io
    from contextlib import redirect_stdout

    from src.orchestrator.capture import open_thread, thread_notes

    tid = await open_thread(actions, "the console door needs a second door too")

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_annotate_thread(str(tid), "confirmed live via osiris annotate-thread",
                                        actor="agent:cliannotator1", pool=actions.pool)
    assert out == 0
    assert f"annotated {tid}" in buf.getvalue()
    assert "confirmed live via osiris annotate-thread" in buf.getvalue()

    notes = await thread_notes(actions.pool, tid)
    assert [n["note"] for n in notes] == ["confirmed live via osiris annotate-thread"]
    assert notes[0]["source"] == "agent:cliannotator1"


async def test_cmd_annotate_thread_refuses_no_match(actions: Actions) -> None:
    import io
    from contextlib import redirect_stderr

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_annotate_thread("no such thread anywhere", "a note",
                                        actor="agent:cliannotator2", pool=actions.pool)
    assert out == 1
    assert "no thread matches" in buf.getvalue()


async def test_cli_parser_accepts_annotate_thread(actions: Actions) -> None:
    """argparse wiring: ref + note positionals, --actor required."""
    from src.cli import _build_parser

    args = _build_parser().parse_args(
        ["annotate-thread", "thread:abc12345", "a further observation", "--actor", "operator"])
    assert args.command == "annotate-thread"
    assert args.ref == "thread:abc12345"
    assert args.note == "a further observation"
    assert args.actor == "operator"


# --- rematerialize: the soul store's own second door (task #51 piece 2) — calls the
# SAME SoulStore.rematerialize_to_disk the MCP wrapper calls, guard untouched ------------------

async def test_cmd_rematerialize_writes_and_reports(actions: Actions, tmp_path: Path) -> None:
    import io
    from contextlib import redirect_stdout

    from src.ingest.soul_store import SoulStore

    source = tmp_path / "s" / "clidead01-session.jsonl"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text('{"type": "user", "message": {"content": "hi"}}\n')
    await SoulStore(actions.pool).ingest_path(str(source), "clidead01")

    dest = tmp_path / "d" / "out.jsonl"
    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_rematerialize("clidead01", dest=str(dest), pool=actions.pool)
    assert out == 0
    assert f"wrote {dest}" in buf.getvalue()
    assert dest.read_text() == source.read_text()


async def test_cmd_rematerialize_refuses_no_match(actions: Actions, tmp_path: Path) -> None:
    import io
    from contextlib import redirect_stderr

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_rematerialize("neverseen0", dest=str(tmp_path / "x.jsonl"),
                                      pool=actions.pool)
    assert out == 1
    assert "refused" in buf.getvalue()
    assert not (tmp_path / "x.jsonl").exists()


async def test_cli_parser_accepts_rematerialize(actions: Actions) -> None:
    """argparse wiring: anchor_sid positional, --dest/--force optional."""
    from src.cli import _build_parser

    args = _build_parser().parse_args(
        ["rematerialize", "deadbeef", "--dest", "/tmp/out.jsonl", "--force"])
    assert args.command == "rematerialize"
    assert args.anchor_sid == "deadbeef"
    assert args.dest == "/tmp/out.jsonl"
    assert args.force is True

    bare = _build_parser().parse_args(["rematerialize", "cafef00d"])
    assert bare.dest is None
    assert bare.force is False


# --- amend-decision: the fifth sanctioned second door (thread 2474) — calls the SAME
# capture.amend_decision the MCP wrapper calls, guard untouched --------------------------------

async def test_cmd_amend_decision_amends_and_reports(actions: Actions) -> None:
    import io
    from contextlib import redirect_stdout

    from src.orchestrator.capture import decision_addenda, record_decision

    did = await record_decision(actions, "the CLI is one of exactly two doors")

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_amend_decision(str(did), "reaffirmed after the mint-seat build",
                                       actor="agent:cliamender4", pool=actions.pool)
    assert out == 0
    assert f"amended {did}" in buf.getvalue()
    assert "reaffirmed after the mint-seat build" in buf.getvalue()

    addenda = await decision_addenda(actions.pool, did)
    assert [a["addendum"] for a in addenda] == ["reaffirmed after the mint-seat build"]
    assert addenda[0]["source"] == "agent:cliamender4"


async def test_cmd_amend_decision_refuses_a_superseded_decision(actions: Actions) -> None:
    import io
    from contextlib import redirect_stderr

    from src.orchestrator.capture import decision_addenda, record_decision

    old = await record_decision(actions, "an earlier, now-corrected ruling")
    await record_decision(actions, "the correction", supersedes=str(old))

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_amend_decision(str(old), "one more thought on the dead ruling",
                                       actor="agent:cliamender5", pool=actions.pool)
    assert out == 1
    assert "refused" in buf.getvalue() and "already superseded" in buf.getvalue()
    assert await decision_addenda(actions.pool, old) == []


async def test_cmd_amend_decision_refuses_no_match(actions: Actions) -> None:
    import io
    from contextlib import redirect_stderr

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_amend_decision("no such decision anywhere", "an addendum",
                                       actor="agent:cliamender6", pool=actions.pool)
    assert out == 1
    assert "no decision matches" in buf.getvalue()


async def test_cli_parser_accepts_amend_decision(actions: Actions) -> None:
    """argparse wiring: ref + addendum positionals, --actor required."""
    from src.cli import _build_parser

    args = _build_parser().parse_args(
        ["amend-decision", "decision:abc12345", "reaffirmed", "--actor", "operator"])
    assert args.command == "amend-decision"
    assert args.ref == "decision:abc12345"
    assert args.addendum == "reaffirmed"
    assert args.actor == "operator"


# --- rebind-seat / correct-pin-value: the jesus/chad path from a terminal (thread 6437,
# #199's parity lane) — both call the SAME orchestrator function their MCP twin wraps -----------

async def test_cmd_rebind_seat_moves_the_anchor_and_reports(
    actions: Actions, tmp_path: Path,
) -> None:
    import io
    from contextlib import redirect_stdout

    from src.orchestrator import mounts
    from src.orchestrator.agents import claim_name, house_of, register_agent, resolve_identity

    a_dir = tmp_path / "clirebind-a"
    b_dir = tmp_path / "clirebind-b"
    a_dir.mkdir()
    ident = resolve_identity(cwd=str(a_dir), session="clirebind01", project_label="clirebindhouse")
    await register_agent(actions, ident, actor="analyst:operator")
    await claim_name(actions, ident.agent_id, "CliRebind", source=ident.agent_id)
    await mounts.save_mount(actions.pool, job_dir="/j/clirebind01", agent_id=ident.agent_id,
                            project="clirebindhouse", cwd=str(a_dir), model="claude-fable-5",
                            session_key="k")

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_rebind_seat("CliRebind", str(b_dir), actor="operator",
                                    pool=actions.pool)
    assert out == 0
    assert f"rebound CliRebind -> {b_dir}" in buf.getvalue()
    assert await house_of(actions.pool, ident.agent_id) == "clirebindhouse"
    row = await actions.pool.fetchrow(
        "SELECT cwd FROM agent_mounts WHERE agent_id=$1", ident.agent_id)
    assert row is not None and row["cwd"] == str(b_dir)


async def test_cmd_rebind_seat_refuses_an_unknown_name(actions: Actions, tmp_path: Path) -> None:
    import io
    from contextlib import redirect_stderr

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_rebind_seat("no-such-seat-or-agent-anywhere", str(tmp_path),
                                    actor="operator", pool=actions.pool)
    assert out == 1
    assert "refused" in buf.getvalue()


async def test_cli_parser_accepts_rebind_seat(actions: Actions) -> None:
    """argparse wiring: seat/new_cwd positionals, --extract flag, --actor default."""
    from src.cli import _build_parser

    args = _build_parser().parse_args(["rebind-seat", "Alfred", "/tmp/somewhere"])
    assert args.command == "rebind-seat"
    assert (args.seat, args.new_cwd, args.extract) == ("Alfred", "/tmp/somewhere", False)
    assert args.actor == "console"

    args2 = _build_parser().parse_args(
        ["rebind-seat", "Alfred", "/tmp/elsewhere", "--extract", "--actor", "operator"])
    assert args2.extract is True and args2.actor == "operator"


async def test_cmd_correct_pin_value_corrects_the_named_seats_pin(
    actions: Actions, tmp_path: Path,
) -> None:
    import io
    from contextlib import redirect_stdout

    from src.orchestrator.agents import claim_name

    claimed = await claim_name(actions, "agent:clipin1owner", "CliPinOwner", source="test")
    office = tmp_path / "clipinowner"
    office.mkdir()
    (office / ".osiris").write_text('project = "tony"\n')

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_correct_pin_value(
            "CliPinOwner", "project", "cultural-infrastructure", "task #152",
            pool=actions.pool, office_root=tmp_path)
    assert out == 0
    assert claimed["seat_id"] in buf.getvalue()
    assert "'tony' -> 'cultural-infrastructure'" in buf.getvalue()
    assert 'project = "cultural-infrastructure"' in (office / ".osiris").read_text()


async def test_cmd_correct_pin_value_refuses_an_unclaimed_name(
    actions: Actions, tmp_path: Path,
) -> None:
    import io
    from contextlib import redirect_stderr

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_correct_pin_value(
            "NobodyEverClaimedThis", "project", "x", "y",
            pool=actions.pool, office_root=tmp_path)
    assert out == 1
    assert "no such claimed seat or live agent" in buf.getvalue()


async def test_cmd_correct_pin_value_propagates_the_missing_key_refusal(
    actions: Actions, tmp_path: Path,
) -> None:
    """No parallel guard logic — the underlying correct_own_pin_value's own refusal (an
    EXISTING key only) surfaces through this door unchanged."""
    import io
    from contextlib import redirect_stderr

    from src.orchestrator.agents import claim_name

    await claim_name(actions, "agent:clipin2owner", "CliPinMissingKey", source="test")
    office = tmp_path / "clipinmissingkey"
    office.mkdir()
    (office / ".osiris").write_text('project = "tony"\n')

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_correct_pin_value(
            "CliPinMissingKey", "no_such_key", "x", "y",
            pool=actions.pool, office_root=tmp_path)
    assert out == 1
    assert "is not declared in" in buf.getvalue()


async def test_cli_parser_accepts_correct_pin_value(actions: Actions) -> None:
    """argparse wiring: seat/key/value positionals, --because required."""
    from src.cli import _build_parser

    args = _build_parser().parse_args(
        ["correct-pin-value", "Alfred", "project", "newname", "--because", "reason"])
    assert args.command == "correct-pin-value"
    assert (args.seat, args.key, args.value, args.reason) == (
        "Alfred", "project", "newname", "reason")


# --- mint-seat: a different shape of second door (no stale-tool-index gap — mint_seat's own
# MCP tool infers `manager` from the caller's held seat, which a raw terminal has none of) ------

async def test_cmd_mint_seat_mints_fresh_worker_and_reports(
    actions: Actions, tmp_path: Path,
) -> None:
    import io
    from contextlib import redirect_stdout

    from src.orchestrator.seats import ensure_seat

    manager = await ensure_seat(actions, house="clihouse", handle="CliMintMgr1",
                                source="test")

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_mint_seat(
            "CliMintWorker1", manager=manager["seat_id"], project="cliproj1", house=None,
            model=None, actor="agent:climinter1", pool=actions.pool, office_root=tmp_path)
    assert out == 0
    text = buf.getvalue()
    assert "minted CliMintWorker1" in text and "house=clihouse" in text
    assert "office:" in text
    assert f"manager: {manager['seat_id']} (linked)" in text
    # THE MCP-SYNTAX LEAK, FIXED (thread bc11a2d3/msg 6262 — this assertion used to
    # PROVE the bug, not catch it: a terminal caller was handed `launch(target=...)`,
    # syntax that cannot run in a shell. next_step_cli is the terminal-appropriate twin.
    assert "occupancy: vacant" in text
    assert "launch(target=" not in text
    assert "osiris launch CliMintWorker1" in text

    from src.orchestrator.seats import seat_facts, seats_by_handle

    worker_ids = await seats_by_handle(actions.pool, "CliMintWorker1")
    assert len(worker_ids) == 1
    facts = await seat_facts(actions.pool, worker_ids[0])
    assert facts["anchor_cwd"]


async def test_cmd_mint_seat_refuses_unknown_manager(actions: Actions) -> None:
    import io
    from contextlib import redirect_stderr

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_mint_seat(
            "CliMintWorker2", manager="NoSuchManagerAnywhere", project=None, house=None,
            model=None, actor="agent:climinter2", pool=actions.pool)
    assert out == 1
    assert "no such manager seat" in buf.getvalue()


async def test_cmd_mint_seat_refuses_cross_house_without_operator_actor(
    actions: Actions,
) -> None:
    import io
    from contextlib import redirect_stderr

    from src.orchestrator.seats import ensure_seat

    manager = await ensure_seat(actions, house="clihouseA", handle="CliMintMgr3",
                                source="test")

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_mint_seat(
            "CliMintWorker3", manager=manager["seat_id"], project=None, house="clihouseB",
            model=None, actor="agent:climinter3", pool=actions.pool)
    assert out == 1
    assert "cross-house mint refused" in buf.getvalue()


async def test_cli_parser_accepts_mint_seat(actions: Actions) -> None:
    """argparse wiring: handle positional required, everything else — including
    --manager/--actor, both inferred since dispatch 3678/3681 — optional."""
    from src.cli import _build_parser

    args = _build_parser().parse_args(
        ["mint-seat", "NewWorker", "--manager", "seat:abc12345", "--actor", "operator"])
    assert args.command == "mint-seat"
    assert args.handle == "NewWorker"
    assert args.manager == "seat:abc12345"
    assert args.actor == "operator"
    assert args.project is None and args.house is None and args.model is None
    assert args.adopt is False and args.force is False


async def test_cli_parser_defaults_actor_to_console_everywhere() -> None:
    """dispatch 3678: a human at a raw terminal shouldn't have to type a value that is
    always going to be the same one — every sanctioned-second-door command defaults
    --actor to the console operator sentinel (src.orchestrator.seats._OPERATOR_ACTORS)."""
    from src.cli import _build_parser

    parser = _build_parser()
    cases = [
        ["fold-project", "a", "b", "--evidence", "e"],
        ["merge", "a", "b", "--evidence", "e"],
        ["unmerge", "a", "--because", "b"],
        ["charter-for", "seat:x", "--repos", "r", "--because", "b"],
        ["amend-practice", "ref", "amendment"],
        ["annotate-thread", "ref", "note"],
        ["amend-decision", "ref", "addendum"],
        ["mint-seat", "NewWorker"],
    ]
    for argv in cases:
        args = parser.parse_args(argv)
        assert args.actor == "console", f"{argv[0]!r} did not default --actor to console"


async def test_cli_parser_accepts_merge_and_unmerge() -> None:
    from src.cli import _build_parser

    parser = _build_parser()
    m = parser.parse_args(["merge", "OldLabel", "NewLabel", "--evidence", "same repo"])
    assert m.command == "merge" and m.dupe == "OldLabel" and m.into == "NewLabel"
    u = parser.parse_args(["unmerge", "OldLabel", "--because", "wrong merge"])
    assert u.command == "unmerge" and u.dupe == "OldLabel" and u.execute is False
    u2 = parser.parse_args(["unmerge", "OldLabel", "--because", "wrong merge", "--execute"])
    assert u2.execute is True


async def test_cmd_merge_folds_a_software_project_pair(actions: Actions) -> None:
    """cmd_merge dispatches to the SAME self-typing orchestrator.merge.merge the MCP tool
    wraps — this is the replacement for the old cmd_fold_project, not a narrower rename."""
    import io
    from contextlib import redirect_stdout

    await _cli_stub_project(actions, "repo:mergedupe1", "mergedupe1")
    await _cli_stub_project(actions, "repo:mergeinto1", "mergeinto1")

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_merge("mergedupe1", "mergeinto1", "both mint the same repo",
                              actor="operator", pool=actions.pool)
    assert out == 0
    assert "folded repo:mergedupe1 into repo:mergeinto1" in buf.getvalue()
    row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical='repo:mergedupe1'")
    assert row["status"] == "merged"


async def test_cmd_fold_project_prints_a_deprecation_pointer(actions: Actions) -> None:
    """dispatch 3683: fold-project is a hidden, working, DEPRECATED alias for merge — it
    must say so on every call, not just quietly keep working forever unremarked."""
    import io
    from contextlib import redirect_stderr

    await _cli_stub_project(actions, "repo:depdupe1", "depdupe1")
    await _cli_stub_project(actions, "repo:depinto1", "depinto1")

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_fold_project("depdupe1", "depinto1", "same repo",
                                     actor="operator", pool=actions.pool)
    assert out == 0
    assert "deprecated" in buf.getvalue().lower()
    assert "osiris merge" in buf.getvalue()


async def test_cmd_unmerge_dry_run_by_default(actions: Actions) -> None:
    """Matches the MCP unmerge tool's own convention exactly: no --execute writes nothing."""
    import io
    from contextlib import redirect_stdout

    await _cli_stub_project(actions, "repo:unmdupe1", "unmdupe1")
    await _cli_stub_project(actions, "repo:unminto1", "unminto1")
    await cmd_merge("unmdupe1", "unminto1", "same repo", actor="operator", pool=actions.pool)

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_unmerge("unmdupe1", "reconsidered", actor="operator",
                                pool=actions.pool, as_json=True)
    assert out == 0
    row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical='repo:unmdupe1'")
    assert row["status"] == "merged"  # dry run: still merged, nothing executed
    # Through --json, the machine contract. The old form ORed in a bare "false"
    # substring, which would have passed on almost any output at all — tightened
    # to the actual receipt field while fixing the render.
    assert '"execute":false' in buf.getvalue().lower()


async def test_cmd_retention_dry_run_by_default(actions: Actions) -> None:
    """Cold by default (thread e6fd3772 piece 1): no --execute counts only."""
    import io
    from contextlib import redirect_stdout

    old = datetime.now(UTC) - timedelta(days=200)
    await actions.pool.execute(
        "INSERT INTO audit_log (action, actor, payload, created_at) "
        "VALUES ('osiris_test_cli_retention', 'agent:test', '{}', $1)", old)

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_retention("audit-log", days=90, execute=False, pool=actions.pool,
                                  as_json=True)
    assert out == 0
    # Asserted through --json, the MACHINE contract: the human render is a presentation
    # layer and may be restyled, but this receipt field is a promise to any script.
    assert '"executed":false' in buf.getvalue().lower()

    n = await actions.pool.fetchval(
        "SELECT count(*) FROM audit_log WHERE action='osiris_test_cli_retention'")
    assert n == 1, "a dry run must never delete anything"


async def test_cmd_retention_execute_deletes(actions: Actions) -> None:
    old = datetime.now(UTC) - timedelta(days=200)
    await actions.pool.execute(
        "INSERT INTO audit_log (action, actor, payload, created_at) "
        "VALUES ('osiris_test_cli_retention2', 'agent:test', '{}', $1)", old)

    out = await cmd_retention("audit-log", days=90, execute=True, pool=actions.pool)
    assert out == 0

    n = await actions.pool.fetchval(
        "SELECT count(*) FROM audit_log WHERE action='osiris_test_cli_retention2'")
    assert n == 0


async def test_cmd_retention_refuses_an_unknown_table(actions: Actions) -> None:
    out = await cmd_retention("not-a-real-table", days=30, execute=False, pool=actions.pool)
    assert out == 1


async def test_cmd_mint_seat_infers_manager_from_the_sole_seat_in_house(
    actions: Actions, tmp_path: Path,
) -> None:
    """dispatch 3678: --manager omitted infers the ONE seat in the target --house."""
    import io
    from contextlib import redirect_stdout

    from src.orchestrator.seats import ensure_seat

    manager = await ensure_seat(actions, house="soleseathouse", handle="OnlySeatHere",
                                source="test")

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_mint_seat(
            "InferredWorker1", manager=None, project=None, house="soleseathouse",
            model=None, actor="console", pool=actions.pool, office_root=tmp_path)
    assert out == 0
    text = buf.getvalue()
    assert "inferred --manager='OnlySeatHere'" in text
    assert f"manager: {manager['seat_id']} (linked)" in text


async def test_cmd_mint_seat_refuses_to_infer_manager_with_no_seats_in_house(
    actions: Actions,
) -> None:
    import io
    from contextlib import redirect_stderr

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_mint_seat(
            "InferredWorker2", manager=None, project=None, house="totallyemptyhouse",
            model=None, actor="console", pool=actions.pool)
    assert out == 1
    assert "no seats exist in house 'totallyemptyhouse'" in buf.getvalue()


async def test_cmd_mint_seat_refuses_to_infer_manager_with_several_seats_in_house(
    actions: Actions,
) -> None:
    import io
    from contextlib import redirect_stderr

    from src.orchestrator.seats import ensure_seat

    await ensure_seat(actions, house="crowdedhouse", handle="CrowdedA", source="test")
    await ensure_seat(actions, house="crowdedhouse", handle="CrowdedB", source="test")

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_mint_seat(
            "InferredWorker3", manager=None, project=None, house="crowdedhouse",
            model=None, actor="console", pool=actions.pool)
    assert out == 1
    text = buf.getvalue()
    assert "2 seats in house 'crowdedhouse'" in text
    assert "CrowdedA" in text and "CrowdedB" in text


def test_bare_osiris_shows_help_and_exits_2(capsys: pytest.CaptureFixture[str]) -> None:
    """dispatch 3678/3681: bare `osiris` must HELP, never just error — but the exit code
    (2, a real usage condition) stays exactly what it was before this build."""
    out = main([])
    assert out == 2
    captured = capsys.readouterr()
    assert "THE TWO COMMANDS TO REMEMBER" in captured.out
    assert "COMMANDS, GROUPED BY WHAT YOU'RE TRYING TO DO" in captured.out


async def test_cli_parser_accepts_new(actions: Actions) -> None:
    from src.cli import _build_parser

    parser = _build_parser()
    args = parser.parse_args(["new", "Henry"])
    assert args.command == "new"
    assert args.handle == "Henry" and args.path is None
    assert args.project is None and args.model is None and args.actor == "console"

    with_path = parser.parse_args(["new", "Henry", "/tmp/henry-ws", "--project", "Custom"])
    assert with_path.path == "/tmp/henry-ws" and with_path.project == "Custom"


async def test_cmd_new_founds_a_self_managed_seat_and_prints_the_launch_line(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """cmd_new has no office_root passthrough (matching cmd_mint_seat's own shape) — the
    real default is offices._default_office_root(), redirected here via OSIRIS_OFFICE_ROOT
    (wave 9, msg 6089) so this test never writes into the operator's actual
    ~/.osiris/seats/."""
    import io
    from contextlib import redirect_stdout

    workspace = tmp_path / "henry-ws"
    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(tmp_path / "seats"))

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_new("Henry", str(workspace), project=None, model=None,
                            actor="console", pool=actions.pool)

    assert out == 0
    text = buf.getvalue()
    assert "founded Henry" in text and "self-managed, no manager" in text
    assert "project: Henry" in text
    assert f"workspace: {workspace}" in text
    assert "next: osiris launch Henry" in text
    assert workspace.is_dir()
    assert (workspace / ".osiris").read_text() == 'project = "Henry"\n'


async def test_cmd_new_confesses_before_writing_when_cwd_disagrees_with_the_default(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE OPERATOR'S REAL SPECIMEN (thread bc11a2d3/msg 6262): `mkdir cdking && cd
    cdking && osiris new Chad` silently created ~/code/chad — cdking was left an
    orphan. No path given, standing somewhere that isn't $HOME and isn't the default
    target: confess BEFORE anything writes, name both paths, give the exact remedy.
    Does NOT switch the default to cwd (deriving-by-convention is the trap the earlier
    anchor_cwd bug came from) — cwd_ws below still ends up empty."""
    import io
    from contextlib import redirect_stderr

    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(tmp_path / "seats"))
    cwd_ws = tmp_path / "cdking"
    cwd_ws.mkdir()
    monkeypatch.setattr(Path, "cwd", staticmethod(lambda: cwd_ws))
    default_workspace = Path.home() / "code" / "chad"

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_new("Chad", None, project=None, model=None,
                            actor="console", pool=actions.pool)
    assert out == 0  # the confession is advisory, never a refusal
    err = buf.getvalue()
    assert str(cwd_ws) in err
    assert str(default_workspace) in err
    assert "osiris new Chad ." in err
    assert list(cwd_ws.iterdir()) == []  # cwd itself was never touched


async def test_cmd_new_stays_silent_when_a_path_is_given(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The confession only fires when DEFAULTING — a caller who names a path explicitly
    (even one that differs from cwd) made a deliberate choice, nothing to confess."""
    import io
    from contextlib import redirect_stderr

    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(tmp_path / "seats"))
    workspace = tmp_path / "named-ws"
    monkeypatch.setattr(Path, "cwd", staticmethod(lambda: tmp_path / "elsewhere"))

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_new("Named", str(workspace), project=None, model=None,
                            actor="console", pool=actions.pool)
    assert out == 0
    assert buf.getvalue() == ""


async def test_cmd_new_notes_the_case_drift_when_handle_capitalization_differs(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE OPERATOR'S THIRD DEFECT (msg 6262): he typed 'Chad', got project 'Chad' but
    paths 'chad' — case-insensitive resolution is correct, the silence about it is not.
    Say plainly which spelling is canonical, only when they actually differ."""
    import io
    from contextlib import redirect_stdout

    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(tmp_path / "seats"))
    workspace = tmp_path / "chad-ws"

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_new("Chad", str(workspace), project=None, model=None,
                            actor="console", pool=actions.pool)
    assert out == 0
    text = buf.getvalue()
    assert "note: paths use the lowercase form ('chad')" in text
    assert "keeps your capitalization ('Chad')" in text


async def test_cmd_new_no_case_note_when_handle_is_already_lowercase(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import io
    from contextlib import redirect_stdout

    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(tmp_path / "seats"))
    workspace = tmp_path / "flatname-ws"

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_new("flatname", str(workspace), project=None, model=None,
                            actor="console", pool=actions.pool)
    assert out == 0
    assert "note: paths use the lowercase form" not in buf.getvalue()

async def test_cli_parser_accepts_resume(actions: Actions) -> None:
    """The new verb (operator ruling 60c78788): a real positional handle + --model,
    no --debug (resume has no PTY-broker fallback lane, launch's own concern only)."""
    from src.cli import _build_parser

    parser = _build_parser()
    args = parser.parse_args(["resume", "Chad"])
    assert args.command == "resume"
    assert args.handle == "Chad" and args.model is None

    named = parser.parse_args(["resume", "Chad", "--model", "claude-sonnet-5"])
    assert named.model == "claude-sonnet-5"


async def test_cli_parser_accepts_bootstrap(actions: Actions) -> None:
    from src.cli import _build_parser

    parser = _build_parser()
    args = parser.parse_args(["bootstrap", "/tmp/some-project"])
    assert args.command == "bootstrap"
    assert args.cwd == "/tmp/some-project"
    assert args.project is None and args.actor == "console"

    named = parser.parse_args(
        ["bootstrap", "/tmp/some-project", "--project", "Custom", "--actor", "operator"])
    assert named.project == "Custom" and named.actor == "operator"


async def test_cmd_bootstrap_ingests_memory_and_registers_the_project(
    actions: Actions, tmp_path: Path,
) -> None:
    """#135 deliverable 3's last missing verb — the console-script door onto the SAME
    bootstrap_project the MCP `bootstrap` tool wraps, no duplicated logic."""
    import io
    from contextlib import redirect_stdout

    project_dir = tmp_path / "some-project"
    project_dir.mkdir()
    (project_dir / "CLAUDE.md").write_text(
        "# CLAUDE.md — build log\n\n## 2026-08-16\nFirst entry.\n")

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_bootstrap(str(project_dir), project=None, actor="console",
                                  pool=actions.pool)

    assert out == 0
    text = buf.getvalue()
    assert "project=some-project" in text
    assert "entries=" in text
    assert "CLAUDE.md" in text

    row = await actions.pool.fetchrow(
        "SELECT id FROM objects WHERE type='SoftwareProject' AND canonical=$1",
        "repo:some-project")
    assert row is not None


async def test_cmd_bootstrap_refuses_the_live_db_fallback_without_confirmation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """9b9ba394's own specimen: this exact command wrote a real project into the shared
    fleet graph because apply_dev_fallback()'s "dev" DSN and every deployed service's
    own DATABASE_URL are the SAME database on this box. No `pool=` passed here (the
    real no-pool path this guard sits in front of) — neither DATABASE_URL nor
    OSIRIS_ALLOW_LIVE set, so this must refuse before ever touching create_pool."""
    import io
    from contextlib import redirect_stderr

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("OSIRIS_ALLOW_LIVE", raising=False)

    project_dir = tmp_path / "some-project"
    project_dir.mkdir()

    buf = io.StringIO()
    with redirect_stderr(buf):
        out = await cmd_bootstrap(str(project_dir), project=None, actor="console")

    assert out == 1
    assert "refusing" in buf.getvalue()
    assert "OSIRIS_ALLOW_LIVE" in buf.getvalue()


async def test_cmd_bootstrap_proceeds_when_database_url_is_already_set(
    actions: Actions, pg_dsn: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit DATABASE_URL (a deployed unit's own environment, or a caller who set
    one deliberately) is never blocked — the guard only fires on the SILENT fallback.
    `actions` (unused directly) forces the Type catalog to be seeded on this same
    database before cmd_bootstrap opens its OWN pool against the same DSN."""
    import io
    from contextlib import redirect_stdout

    monkeypatch.setenv("DATABASE_URL", pg_dsn)
    monkeypatch.delenv("OSIRIS_ALLOW_LIVE", raising=False)

    project_dir = tmp_path / "some-project"
    project_dir.mkdir()
    (project_dir / "CLAUDE.md").write_text("# CLAUDE.md\n\n## 2026-08-16\nEntry.\n")

    buf = io.StringIO()
    with redirect_stdout(buf):
        out = await cmd_bootstrap(str(project_dir), project=None, actor="console")

    assert out == 0
    assert "project=some-project" in buf.getvalue()
