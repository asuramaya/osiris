"""`osiris launch` and `osiris resume` talk like a person: at most three short lines by
default, one copy-pasteable next step only when one is needed, internals behind --verbose.
Each scenario below is one real operator case; `_DUMP` (env LAUNCH_VOICE_DUMP=<file>)
appends its raw transcript so the same scenarios can be run before and after a change.
"""
from __future__ import annotations

import io
import os
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.cli import _cmd_resume_harness, cmd_launch
from src.orchestrator.seats import ensure_seat

from tests.test_cli import _resumable_seat, _resume_settings

_TRUST = ("Workspace not trusted. Run `claude` in {cwd} once and accept the trust prompt, "
          "then retry.")


async def _quiet_agents(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
    return []


def _dump(name: str, out: str, err: str, code: int) -> None:
    path = os.environ.get("LAUNCH_VOICE_DUMP")
    if path:
        with open(path, "a") as f:
            f.write(f"=== {name} (exit {code})\n--- stdout\n{out}--- stderr\n{err}\n")


async def _run(name: str, coro: Any) -> tuple[int, str, str]:
    so, se = io.StringIO(), io.StringIO()
    with redirect_stdout(so), redirect_stderr(se):
        code = await coro
    _dump(name, so.getvalue(), se.getvalue(), code)
    return code, so.getvalue(), se.getvalue()


def _lines(text: str) -> list[str]:
    return [ln for ln in text.splitlines() if ln.strip()]


def _call_launch(*a: Any, **kw: Any) -> Any:
    """cmd_launch with only the seams this build knows (lets the same scenarios run on the
    build before launch learned to resume)."""
    import inspect
    known = inspect.signature(cmd_launch).parameters
    return cmd_launch(*a, **{k: v for k, v in kw.items() if k in known})


async def _plain_seat(actions: Actions, tmp_path: Path, handle: str) -> Path:
    office = tmp_path / handle
    office.mkdir()
    await ensure_seat(actions, house="osiris", handle=handle, anchor_cwd=str(office),
                      source="test")
    return office


# --- launch ---------------------------------------------------------------------------------

async def scenario_launch_untrusted(actions: Actions, tmp_path: Path) -> tuple[int, str, str]:
    office = await _plain_seat(actions, tmp_path, "handlingtheloop")

    async def _spawn(*a: Any, **k: Any) -> None:
        raise OSError(f"claude --bg exited 1 in {office}: " + _TRUST.format(cwd=office))

    return await _run("A launch: workspace not trusted", cmd_launch(
        "handlingtheloop", model=None, pool=actions.pool, spawn=_spawn,
        agents_json=_quiet_agents))


async def scenario_launch_fresh(actions: Actions, tmp_path: Path) -> tuple[int, str, str]:
    office = await _plain_seat(actions, tmp_path, "freshone")
    polls = 0

    async def _spawn(*a: Any, **k: Any) -> None:
        return None

    async def _agents(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        nonlocal polls
        polls += 1
        return [] if polls < 2 else [{"name": "[OS] freshone", "cwd": str(office),
                                       "sessionId": "sess-fresh-0001"}]

    return await _run("B launch: nothing to resume, fresh", cmd_launch(
        "freshone", model="claude-sonnet-5", pool=actions.pool, spawn=_spawn,
        agents_json=_agents))


async def scenario_launch_already_live(actions: Actions, tmp_path: Path
                                       ) -> tuple[int, str, str]:
    office = await _plain_seat(actions, tmp_path, "livealready")

    async def _agents(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        return [{"name": "[OS] livealready", "cwd": str(office), "sessionId": "sess-live"}]

    async def _spawn(*a: Any, **k: Any) -> None:
        raise AssertionError("a live body must never be twinned")

    return await _run("C launch: already running", cmd_launch(
        "livealready", model=None, pool=actions.pool, spawn=_spawn, agents_json=_agents))


async def scenario_launch_resumable(actions: Actions, tmp_path: Path,
                                    monkeypatch: pytest.MonkeyPatch) -> tuple[int, str, str]:
    """The seat has a resumable earlier session (and a big dormant transcript at its office)."""
    sense = await _resumable_seat(
        actions, tmp_path, handle="metron", agent_id="agent:metron01",
        anchor_cwd=str(tmp_path / "metron-office"))
    monkeypatch.setattr(
        "src.ingest.sessions.dormant_history_confession",
        lambda cwd, *a, **k: {
            "path": "/x/602aab71.jsonl", "size_bytes": 1_500_000,
            "last_touched": "2026-10-30T12:00:00+00:00", "resumable": True,
            "resume_command": "claude --resume 602aab71-0000-4000-8000-000000000000"})
    from src.config.settings import get_settings
    patched = get_settings().model_copy(update={
        "osiris_sense_sessions": str(sense), "osiris_resume_min_tail_bytes": 0})
    monkeypatch.setattr("src.config.settings.get_settings", lambda: patched)
    calls: list[str] = []

    async def _spawn(repo: str, *, name: str | None = None, **k: Any) -> str | None:
        sid = k.get("resume_session")
        calls.append(f"{'resume' if sid else 'fresh'}:{sid}")
        return "7d31784d"

    polls = 0

    async def _agents(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        nonlocal polls
        polls += 1
        return [] if polls < 2 else [{"name": "[OS] metron", "cwd": cwd or "",
                                       "sessionId": "602aab71-0000-4000-8000-000000000000"}]

    async def _no_clear(key: str) -> bool:
        return False

    res = await _run("D launch: seat has a resumable session", _call_launch(
        "metron", model=None, pool=actions.pool, spawn=_spawn, agents_json=_agents,
        resume_spawn=_spawn, clear_stale_record=_no_clear, settings=patched))
    return res[0], res[1] + f"[spawn calls: {calls}]\n", res[2]


# --- resume ---------------------------------------------------------------------------------

async def _spawn_ok(*a: Any, **k: Any) -> str | None:
    return None


async def _no_clear(key: str) -> bool:
    return False


async def scenario_resume_ok(actions: Actions, tmp_path: Path) -> tuple[int, str, str]:
    sense = await _resumable_seat(
        actions, tmp_path, handle="resok", agent_id="agent:resok01",
        anchor_cwd=str(tmp_path / "resok-office"))
    return await _run("E resume: resumed", _cmd_resume_harness(
        "resok", model=None, pool=actions.pool, wake_default=None,
        agents_json=_quiet_agents, resume_spawn=_spawn_ok, settings=_resume_settings(sense),
        clear_stale_record=_no_clear))


async def scenario_resume_nothing(actions: Actions, tmp_path: Path) -> tuple[int, str, str]:
    sense = await _resumable_seat(
        actions, tmp_path, handle="resnone", agent_id="agent:resnone01",
        anchor_cwd=str(tmp_path / "resnone-office"), compacted=True)
    return await _run("F resume: nothing resumable", _cmd_resume_harness(
        "resnone", model=None, pool=actions.pool, wake_default=None,
        agents_json=_quiet_agents, resume_spawn=_spawn_ok,
        settings=_resume_settings(sense, min_tail_bytes=1000)))


async def scenario_resume_unverified(actions: Actions, tmp_path: Path
                                     ) -> tuple[int, str, str]:
    """One hop back, nothing signed in the transcript: osiris cannot confirm the session
    is the seat's own."""
    import os
    import time as _time
    from datetime import UTC, datetime

    from src.orchestrator.seats import bind_holder

    from tests.test_cli import _RESUME_SID

    sense = tmp_path / "projects"
    proj = sense / "-repo-demo"
    proj.mkdir(parents=True, exist_ok=True)
    t = proj / f"{_RESUME_SID}.jsonl"
    t.write_bytes(b'{"type":"assistant","text":"booting"}\n' + b"x" * 16)
    old = _time.time() - 3600
    os.utime(t, (old, old))
    office = tmp_path / "resunk-office"
    office.mkdir()
    seat = await ensure_seat(actions, house="osiris", handle="resunk",
                             anchor_cwd=str(office), source="test")
    pred = await actions.create_or_find_object("Agent", "agent:resunk01", "test")
    now = datetime(2026, 8, 18, tzinfo=UTC)
    await actions.assert_property(pred, "seat_generation", "1", "test", now, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(pred, "session", _RESUME_SID, "test", now, 0.9,
                                  evidence_class="self_declared")
    heir = await actions.create_or_find_object("Agent", "agent:resunk01-ii", "test")
    await actions.assert_property(heir, "succeeded_from", "agent:resunk01", "test", now,
                                  0.9, evidence_class="self_declared")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:resunk01-ii")
    return await _run("G resume: session cannot be verified", _cmd_resume_harness(
        "resunk", model=None, pool=actions.pool, wake_default=None,
        agents_json=_quiet_agents, resume_spawn=_spawn_ok, settings=_resume_settings(sense)))


async def scenario_resume_other_account(actions: Actions, tmp_path: Path,
                                        monkeypatch: pytest.MonkeyPatch
                                        ) -> tuple[int, str, str]:
    sense = await _resumable_seat(
        actions, tmp_path, handle="resacct", agent_id="agent:resacct01",
        anchor_cwd=str(tmp_path / "resacct-office"))

    async def _mismatch(pool: Any, sid8: str) -> dict[str, Any]:
        return {"detail": "that session's history was written under another Claude account "
                          "(work@example.com); the harness would load none of it"}

    async def _brief(pool: Any, **k: Any) -> str:
        return "BRIEFING"

    monkeypatch.setattr("src.orchestrator.resume_account.store_account_mismatch", _mismatch)
    monkeypatch.setattr("src.orchestrator.resume_account.store_briefing", _brief)
    polls = 0

    async def _agents(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        nonlocal polls
        polls += 1
        return [] if polls < 5 else [{"name": "[OS] resacct", "cwd": cwd or "",
                                       "sessionId": "sess-fresh-acct"}]

    return await _run("H resume: history belongs to another account", _cmd_resume_harness(
        "resacct", model=None, pool=actions.pool, wake_default=None, agents_json=_agents,
        resume_spawn=_spawn_ok, launch_spawn=_spawn_ok, settings=_resume_settings(sense),
        clear_stale_record=_no_clear))


async def scenario_resume_spawn_refused(actions: Actions, tmp_path: Path
                                        ) -> tuple[int, str, str]:
    sense = await _resumable_seat(
        actions, tmp_path, handle="resref", agent_id="agent:resref01",
        anchor_cwd=str(tmp_path / "resref-office"))

    async def _refuse(*a: Any, **k: Any) -> None:
        raise OSError("claude --bg exited 1 in /x: " + _TRUST.format(cwd="/x"))

    return await _run("I resume: Claude Code refused to start it", _cmd_resume_harness(
        "resref", model=None, pool=actions.pool, wake_default=None,
        agents_json=_quiet_agents, resume_spawn=_refuse, settings=_resume_settings(sense),
        clear_stale_record=_no_clear))


async def scenario_resume_copy_note(actions: Actions, tmp_path: Path,
                                    monkeypatch: pytest.MonkeyPatch) -> tuple[int, str, str]:
    sense = await _resumable_seat(
        actions, tmp_path, handle="rescopy", agent_id="agent:rescopy01",
        anchor_cwd=str(tmp_path / "rescopy-office"))

    async def _adopt(pool: Any, **k: Any) -> dict[str, Any]:
        return {"copied": True, "session_id": "ffffffff-0000-4000-8000-000000000000"}

    monkeypatch.setattr("src.orchestrator.trigger._adopt_resumed_body", _adopt)

    async def _clear(key: str) -> bool:
        return True

    return await _run("J resume: harness made a copy, stale record cleared",
                      _cmd_resume_harness(
        "rescopy", model="claude-sonnet-5", pool=actions.pool, wake_default=None,
        agents_json=_quiet_agents, resume_spawn=_spawn_ok, settings=_resume_settings(sense),
        clear_stale_record=_clear))


async def scenario_launch_dormant_not_resumable(actions: Actions, tmp_path: Path,
                                                monkeypatch: pytest.MonkeyPatch,
                                                verbose: bool = False
                                                ) -> tuple[int, str, str]:
    """A big old transcript sits at the office but nothing can be resumed: the default
    voice says nothing about it."""
    office = await _plain_seat(actions, tmp_path, "dormantone")
    monkeypatch.setattr(
        "src.ingest.sessions.dormant_history_confession",
        lambda cwd, *a, **k: {
            "path": "/x/1.jsonl", "size_bytes": 20_300_000,
            "last_touched": "2026-08-02T17:57:18+00:00", "resumable": False,
            "not_resumable_reason": "it closed at its own compaction boundary"})
    polls = 0

    async def _agents(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        nonlocal polls
        polls += 1
        return [] if polls < 2 else [{"name": "[OS] dormantone", "cwd": str(office),
                                       "sessionId": "sess-dormant-1"}]

    return await _run("K launch: old transcript, nothing resumable", cmd_launch(
        "dormantone", model=None, pool=actions.pool, spawn=_spawn_ok, agents_json=_agents,
        verbose=verbose))


_ALL = [scenario_launch_untrusted, scenario_launch_fresh, scenario_launch_already_live,
        scenario_launch_resumable, scenario_resume_ok, scenario_resume_nothing,
        scenario_resume_unverified, scenario_resume_other_account,
        scenario_resume_spawn_refused, scenario_resume_copy_note,
        scenario_launch_dormant_not_resumable]


async def _go(scenario: Any, actions: Actions, tmp_path: Path,
              monkeypatch: pytest.MonkeyPatch) -> tuple[int, str, str]:
    import inspect
    kw: dict[str, Any] = {}
    if "monkeypatch" in inspect.signature(scenario).parameters:
        kw["monkeypatch"] = monkeypatch
    return await scenario(actions, tmp_path, **kw)


def _all_lines(out: str, err: str) -> list[str]:
    return _lines(out) + _lines(err)


@pytest.mark.parametrize("scenario", _ALL, ids=[f.__name__ for f in _ALL])
async def test_every_case_is_at_most_three_short_lines(
        scenario: Any, actions: Actions, tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch) -> None:
    code, out, err = await _go(scenario, actions, tmp_path, monkeypatch)
    lines = _all_lines(out, err)
    assert code in (0, 1)
    assert 1 <= len(lines) <= 3, lines
    assert all(len(ln) <= 220 for ln in lines), [len(ln) for ln in lines]


@pytest.mark.parametrize("scenario", _ALL, ids=[f.__name__ for f in _ALL])
async def test_default_output_carries_no_internals(
        scenario: Any, actions: Actions, tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch) -> None:
    """Harness gaps, ledger rows, hop counts, byte sizes, model ids and gate numbers are
    --verbose material."""
    _code, out, err = await _go(scenario, actions, tmp_path, monkeypatch)
    text = out + err
    for banned in ("claude --bg", "silently ignores", "harness gap", "hop", "MB", "bytes",
                   "min_tail", "ceiling", "ledger", "registry", "requested model",
                   "fresh-launch", "claude -p --resume", "claude --resume", "by hand",
                   "agent:", "seat:", "Walked", "generation(s)"):
        assert banned not in text, (banned, text)


async def test_untrusted_launch_is_one_copyable_line(
        actions: Actions, tmp_path: Path) -> None:
    code, out, err = await scenario_launch_untrusted(actions, tmp_path)
    assert code == 1 and out == ""
    (line,) = _lines(err)
    assert line.startswith("osiris launch: handlingtheloop is not trusted by Claude Code yet:")
    assert "run  cd " in line and "handlingtheloop && claude  , accept, then launch again." in line
    assert "NOTHING was spawned" not in line and "Fix (the operator" not in line


async def test_launch_continues_a_resumable_session_itself(
        actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    code, out, err = await scenario_launch_resumable(actions, tmp_path, monkeypatch)
    assert code == 0 and err == ""
    assert "osiris launch: metron resumed (session b5f04f84)." in out
    assert "[spawn calls: ['resume:b5f04f84-0000-4000-8000-000000000000']]" in out
    assert "run `claude --resume" not in out + err  # never hand the person a command to type


async def test_launch_fresh_flag_starts_a_new_session_anyway(
        actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sense = await _resumable_seat(
        actions, tmp_path, handle="freshflag", agent_id="agent:freshflag01",
        anchor_cwd=str(tmp_path / "freshflag-office"))
    from src.config.settings import get_settings
    patched = get_settings().model_copy(update={
        "osiris_sense_sessions": str(sense), "osiris_resume_min_tail_bytes": 0})
    monkeypatch.setattr("src.config.settings.get_settings", lambda: patched)
    calls: list[str | None] = []

    async def _spawn(repo: str, *, name: str | None = None, **k: Any) -> str | None:
        calls.append(k.get("resume_session"))
        return None

    polls = 0

    async def _agents(*, cwd: str | None = None, **k: Any) -> list[dict[str, Any]]:
        nonlocal polls
        polls += 1
        return [] if polls < 2 else [{"name": "[OS] freshflag", "cwd": cwd or "",
                                       "sessionId": "s"}]

    code, out, _err = await _run("L launch --fresh", cmd_launch(
        "freshflag", model=None, pool=actions.pool, spawn=_spawn, agents_json=_agents,
        resume_spawn=_spawn, settings=patched, fresh=True))
    assert code == 0 and calls == [None]
    assert "freshflag is running" in out and "resumed" not in out


async def test_resume_with_nothing_resumable_names_one_next_step(
        actions: Actions, tmp_path: Path) -> None:
    _c, _o, nothing = await scenario_resume_nothing(actions, tmp_path)
    assert _lines(nothing) == [
        "osiris resume: resnone has nothing to resume. Start a new session instead: "
        "osiris launch resnone"]


async def test_resume_of_an_unverifiable_session_names_one_next_step(
        actions: Actions, tmp_path: Path) -> None:
    _c, _o, unverified = await scenario_resume_unverified(actions, tmp_path)
    (line,) = _lines(unverified)
    assert "can't be confirmed as its own" in line and line.endswith("osiris launch resunk")


async def test_resume_of_another_accounts_history_says_so_in_one_line(
        actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    code, out, err = await scenario_resume_other_account(actions, tmp_path, monkeypatch)
    assert code == 0
    (note,) = _lines(err)
    assert "another Claude account" in note and "work@example.com" not in note
    assert "resacct is running" in out


async def test_resume_spawn_refusal_reads_like_launch(
        actions: Actions, tmp_path: Path) -> None:
    code, _out, err = await scenario_resume_spawn_refused(actions, tmp_path)
    assert code == 1
    (line,) = _lines(err)
    assert line.startswith("osiris resume: x is not trusted by Claude Code yet: run  cd /x")
    assert line.endswith("accept, then resume again.")


async def test_verbose_brings_the_internals_back(
        actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # nothing resumable: the gate's numbers
    sense = await _resumable_seat(
        actions, tmp_path, handle="verbn", agent_id="agent:verbn01",
        anchor_cwd=str(tmp_path / "verbn-office"), compacted=True)
    _c, _o, err = await _run("verbose: nothing resumable", _cmd_resume_harness(
        "verbn", model=None, pool=actions.pool, wake_default=None,
        agents_json=_quiet_agents, resume_spawn=_spawn_ok,
        settings=_resume_settings(sense, min_tail_bytes=1000), verbose=True))
    assert "min_tail_bytes=1000" in err and "compaction boundary itself" in err
    # a big old transcript the launch could not resume
    _c, out, err = await scenario_launch_dormant_not_resumable(
        actions, tmp_path, monkeypatch, verbose=True)
    assert "20.3MB" in err and "requested model" in out


async def test_dormant_history_stays_quiet_by_default(
        actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _c, out, err = await scenario_launch_dormant_not_resumable(actions, tmp_path, monkeypatch)
    assert err == "" and _lines(out) == [
        "osiris launch: dormantone is running ('[OS] dormantone' in claude agents)."]


def test_the_flags_exist_and_default_off() -> None:
    from src import cli
    parser = cli._build_parser()
    a = parser.parse_args(["launch", "x"])
    assert a.verbose is False and a.fresh is False
    a = parser.parse_args(["launch", "x", "--verbose", "--fresh"])
    assert a.verbose is True and a.fresh is True
    assert parser.parse_args(["resume", "x", "--verbose"]).verbose is True


def test_the_voice_helpers() -> None:
    from src import launch_voice as v

    home = str(Path.home())
    assert v.home_short(f"{home}/code/handlingtheloop") == "~/code/handlingtheloop"
    assert v.home_short("/srv/x") == "/srv/x"
    assert v.untrusted_line("launch", f"{home}/code/handlingtheloop") == (
        "osiris launch: handlingtheloop is not trusted by Claude Code yet: run  "
        "cd ~/code/handlingtheloop && claude  , accept, then launch again.")
    # the harness's own message names the directory it refused; that one wins
    assert "cd /elsewhere/y && claude" in v.untrusted_line(
        "resume", "/x", "Workspace not trusted. Run `claude` in /elsewhere/y once and accept")
    assert v.is_untrusted("Workspace not trusted. Run ...") and not v.is_untrusted("boom")
    assert v.first_sentence("It broke. And then more detail follows.") == "It broke."
