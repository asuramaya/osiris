"""osiris — the console-script (task #69, ruling 45b074bf). The pure decision layer
(`match_session`, `resolve_model`, `_house_tag`) is fully covered with no IO at all; the async
commands are tested with a REAL pool (this repo's own "never mock the DB" rule) but a FAKE
`manager` callable — the manager daemon itself is test_manager.py's territory, and actually
spawning a claude process is exactly what these tests must never risk doing by accident.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.cli import (
    DEPLOY_UNITS,
    _composition_gaps,
    _find_repo_root,
    _wait_for_health,
    _wait_for_smoke,
    alembic_gap_note,
    cmd_amend_practice,
    cmd_attach,
    cmd_boot_status,
    cmd_charter_for,
    cmd_deploy,
    cmd_fold_project,
    cmd_launch,
    cmd_migrate,
    cmd_seed,
    commit_deployed_notes,
    composition_drift_notes,
    composition_gap_notes,
    composition_room_gap_notes,
    diff_tool_lists,
    dirty_tracked_src_files,
    match_session,
    oneshot_deployed_scripts,
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
    actions: Actions,
) -> None:
    await ensure_seat(actions, house="osiris", handle="already-live-bg",
                      anchor_cwd="/home/x/.osiris/seats/already-live-bg", source="test")

    async def _spawn(*a: Any, **k: Any) -> None:
        raise AssertionError("should never be called — a live body already holds this seat")

    async def _agents_json(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        return [{"name": "[OS] already-live-bg", "cwd": cwd, "sessionId": "sess-1"}]

    out = await cmd_launch("already-live-bg", model=None, pool=actions.pool,
                           spawn=_spawn, agents_json=_agents_json)
    assert out == 0


async def test_cmd_launch_harness_spawns_and_confirms(actions: Actions) -> None:
    """The full honest path: no live body yet, `claude --bg` fires with the mount+claim_name
    boot prompt, then a bounded poll confirms the body shows up in `claude agents --json` —
    the fake resolves on the FIRST poll iteration, so this needs no real sleep-bound wait."""
    await ensure_seat(actions, house="osiris", handle="freshbg",
                      anchor_cwd="/home/x/.osiris/seats/freshbg", source="test")

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

    out = await cmd_launch("freshbg", model="claude-sonnet-5", pool=actions.pool,
                           spawn=_spawn, agents_json=_agents_json)
    assert out == 0
    assert len(spawn_calls) == 1
    assert spawn_calls[0]["repo"] == "/home/x/.osiris/seats/freshbg"
    assert spawn_calls[0]["model"] == "claude-sonnet-5"
    assert 'mount(cwd="/home/x/.osiris/seats/freshbg"' in spawn_calls[0]["prompt"]
    assert 'claim_name("freshbg")' in spawn_calls[0]["prompt"]


async def test_cmd_launch_harness_confesses_dormant_history_to_stderr(
    actions: Actions, monkeypatch: Any,
) -> None:
    """Thread fc69b9b4: a substantial transcript already sitting at the target office is
    named — not silently spawned into — before `claude --bg` fires. Disclosure only: the
    spawn still happens."""
    import io
    from contextlib import redirect_stderr

    from src.cli import _cmd_launch_harness

    await ensure_seat(actions, house="osiris", handle="ooblek-cli",
                      anchor_cwd="/home/x/.osiris/seats/ooblek-cli", source="test")

    fake_info = {"path": "/whatever/b5f04f84.jsonl", "size_bytes": 20_300_000,
                 "last_touched": "2026-08-02T17:57:18+00:00"}
    monkeypatch.setattr(
        "src.ingest.sessions.dormant_history_confession",
        lambda cwd, **k: fake_info if cwd == "/home/x/.osiris/seats/ooblek-cli" else None,
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
    actions: Actions,
) -> None:
    """The bounded-poll honesty law (same discipline as `_await_launch_confirmation` and
    `_wait_for_smoke`): a body that never shows up in `claude agents --json` gets a plain
    confession, never a false 'launched: true', and the poll itself is bounded — never an
    indefinite wait."""
    from src.cli import _cmd_launch_harness

    await ensure_seat(actions, house="osiris", handle="neverup",
                      anchor_cwd="/home/x/.osiris/seats/neverup", source="test")

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
                               pool=actions.pool, list_tools=_list_tools)
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
                         pool=actions.pool, record_deploy=_record_deploy)
    assert calls == [(actions.pool, tmp_path)]
    assert "deploy ledger: recorded deadbeef" in buf.getvalue()


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
                         pool=actions.pool)
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
                         pool=actions.pool, migration_state=_state, run_migrations=_run)
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
