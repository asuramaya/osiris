"""`osiris at-home`: the jobs that wait only on the operator, with live status and the exact
command, cheapest first; read-only; told to the desk once per state."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from src.orchestrator import at_home

NOW = datetime(2026, 11, 14, 12, 0, 0, tzinfo=UTC)
NAS = {"name": "nas", "kind": "restic", "enabled": True, "path_or_url": "sftp:truenas:/r"}


def _days(n: float) -> str:
    return (NOW - timedelta(days=n)).isoformat()


def _recipient(rid: str, kind: str, proved: str | None) -> dict[str, Any]:
    return {"id": rid, "label": rid, "kind": kind, "last_proved_at": proved,
            "verdict": "current: ok" if proved else "never proved: needs the operator's key"}


GOOD_ENVELOPE = {"configured": True, "ok": True, "recipients": [
    _recipient("k1", "fido2-prf", _days(2)), _recipient("k2", "fido2-prf", _days(3)),
    _recipient("c1", "recovery-code", _days(4))]}


def sources(**over: Any) -> at_home.Sources:
    base: dict[str, Any] = dict(
        envelope=lambda: GOOD_ENVELOPE, nas_target=lambda: NAS,
        offload_receipt=lambda n: {"last_successful_offload": _days(1),
                                   "first_successful_offload": _days(30)},
        drill_receipt=lambda u: {"last_passed_at": _days(1)},
        presence=lambda t: {"present": True, "reason": None, "dev": "enp0s31f6"},
        plaintext_thread_open=lambda: True, stale_days=7)
    base.update(over)
    return at_home.Sources(**base)


def by_key(result: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {i["key"]: i for i in result["items"]}


def test_when_everything_is_done_nothing_is_pending() -> None:
    result = at_home.checklist(sources(plaintext_thread_open=lambda: False), now=NOW)

    assert result["pending"] == 0 and result["next"] is None
    lines = at_home.render(result)
    assert lines[0] == "Nothing is waiting on you."
    assert "Claude rebuild probe" in lines[1] and "optional" in lines[1]


def test_the_nas_waits_with_its_reason_and_whether_you_are_home_right_now() -> None:
    away = sources(offload_receipt=lambda n: {"last_successful_offload": _days(13),
                                              "first_successful_offload": _days(40)},
                   drill_receipt=lambda u: {},
                   presence=lambda t: {"present": False,
                                       "reason": "reachable only over tailscale0"})

    item = by_key(at_home.checklist(away, now=NOW))["nas_offload"]

    assert item["status"] == "waiting"
    assert "stale: no successful offload for 13 days (limit 7)" in item["why"]
    assert "the restore test has not passed against it" in item["why"]
    assert "not on the home network (reachable only over tailscale0)" in item["why"]
    assert item["command"] == "osiris nas-first-offload --name nas"
    assert "--check" in item["then"]

    home = by_key(at_home.checklist(sources(
        offload_receipt=lambda n: {}, drill_receipt=lambda u: {}), now=NOW))["nas_offload"]
    assert "You are on the home network now (enp0s31f6)" in home["why"]


def test_the_nas_is_done_only_with_a_current_offload_and_a_passed_restore_test() -> None:
    done = by_key(at_home.checklist(sources(), now=NOW))["nas_offload"]
    assert done["status"] == "done" and "the restore test has passed" in done["why"]

    no_drill = by_key(at_home.checklist(sources(drill_receipt=lambda u: {}), now=NOW))
    assert no_drill["nas_offload"]["status"] == "waiting"

    old_drill = by_key(at_home.checklist(
        sources(drill_receipt=lambda u: {"last_passed_at": _days(60)}), now=NOW))
    assert old_drill["nas_offload"]["status"] == "waiting"  # passed before the first offload


def test_no_nas_target_is_a_waiting_item_not_a_crash() -> None:
    item = by_key(at_home.checklist(sources(nas_target=lambda: None), now=NOW))["nas_offload"]

    assert item["status"] == "waiting" and "no enabled NAS target" in item["why"]


def test_the_plaintext_removal_is_blocked_until_the_nas_is_done_then_needs_your_word() -> None:
    blocked = by_key(at_home.checklist(sources(drill_receipt=lambda u: {}), now=NOW))
    assert blocked["plaintext_copy"]["status"] == "blocked"
    assert "waits for the NAS offload" in blocked["plaintext_copy"]["why"]

    ready = by_key(at_home.checklist(sources(), now=NOW))["plaintext_copy"]
    assert ready["status"] == "waiting" and "needs your word" in ready["why"]
    assert ready["command"] == "reply on the operator desk: delete the old plaintext NAS copy"

    gone = by_key(at_home.checklist(sources(plaintext_thread_open=lambda: False), now=NOW))
    assert gone["plaintext_copy"]["status"] == "done"
    unknown = by_key(at_home.checklist(sources(plaintext_thread_open=lambda: None), now=NOW))
    assert unknown["plaintext_copy"]["status"] == "waiting"  # unreadable: still shown


def test_the_envelope_jobs_each_read_the_real_recipients() -> None:
    thin = {"configured": True, "ok": False, "recipients": [
        _recipient("k1", "fido2-prf", None)]}

    got = by_key(at_home.checklist(sources(envelope=lambda: thin), now=NOW))

    assert got["envelope_drill"]["status"] == "waiting"
    assert "1 of 1 recipient(s) not proved lately (k1)" in got["envelope_drill"]["why"]
    assert got["envelope_drill"]["command"] == "osiris envelope-drill"
    assert got["envelope_code"]["status"] == "waiting"
    assert got["envelope_code"]["command"] == "osiris soul-key envelope add-code"
    assert got["envelope_key"]["status"] == "waiting"
    assert "only 1 hardware key" in got["envelope_key"]["why"]
    assert got["envelope_key"]["command"] == "osiris soul-key envelope add-key"

    assert all(by_key(at_home.checklist(sources(), now=NOW))[k]["status"] == "done"
               for k in ("envelope_drill", "envelope_code", "envelope_key"))


def test_an_envelope_that_is_not_set_up_or_unreadable_says_so() -> None:
    none = by_key(at_home.checklist(sources(envelope=lambda: {"configured": False}), now=NOW))
    assert "no envelope is set up yet" in none["envelope"]["why"]
    bad = by_key(at_home.checklist(sources(envelope=lambda: {"error": "boom"}), now=NOW))
    assert "could not be read (boom)" in bad["envelope"]["why"]


def test_a_source_that_raises_never_hides_the_other_items() -> None:
    def boom() -> Any:
        raise OSError("disk gone")

    result = at_home.checklist(sources(envelope=boom), now=NOW)

    keys = [i["key"] for i in result["items"]]
    assert "envelope" in keys and "nas_offload" in keys and "plaintext_copy" in keys
    assert "could not be read (OSError)" in by_key(result)["envelope"]["why"]


def test_items_are_ordered_cheapest_and_safest_first() -> None:
    nothing_done = sources(
        envelope=lambda: {"configured": True, "recipients": [_recipient("k1", "fido2-prf", None)]},
        offload_receipt=lambda n: {}, drill_receipt=lambda u: {})

    result = at_home.checklist(nothing_done, now=NOW)

    assert [i["key"] for i in result["items"]] == [
        "envelope_drill", "envelope_code", "envelope_key", "nas_offload", "plaintext_copy",
        "rollback_container", "scratch_dirs", "rebuild_probe"]
    assert result["next"]["key"] == "envelope_drill"  # the first thing to do
    assert result["pending"] == 5 and result["done"] == 2


def test_the_terminal_text_names_each_command_and_marks_blocked_items() -> None:
    result = at_home.checklist(sources(
        offload_receipt=lambda n: {}, drill_receipt=lambda u: {}), now=NOW)

    lines = at_home.render(result)

    assert lines[0] == "1 waiting, 1 blocked on you, cheapest and safest first:"
    text = "\n".join(lines)
    assert "[waiting] The NAS first offload" in text
    assert "run: osiris nas-first-offload --name nas" in text
    assert "or first: osiris nas-first-offload --check" in text
    assert "[blocked] Remove the old plaintext NAS copy" in text
    assert "[done   ] Prove each envelope key opens it" in text


# ---- the desk line --------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(at_home._STATE_ENV, str(tmp_path / "at_home.json"))


async def _desk(actions: Any) -> list[str]:
    rows = await actions.pool.fetch(
        "SELECT body FROM fleet_messages WHERE from_agent = 'cron:at_home' ORDER BY id")
    return [r["body"] for r in rows]


def _pending() -> dict[str, Any]:
    return at_home.checklist(sources(offload_receipt=lambda n: {}, drill_receipt=lambda u: {}),
                             now=NOW)


async def test_something_waiting_is_told_to_the_desk_once_with_the_first_step(actions: Any) -> None:
    told = await at_home.surface(actions.pool, _pending(), now=NOW)

    assert told is not None and "at home: 1 thing(s) wait on you" in told
    assert "osiris nas-first-offload --name nas" in told and "`osiris at-home`" in told
    for _ in range(3):
        assert await at_home.surface(actions.pool, _pending(), now=NOW) is None
    assert len(await _desk(actions)) == 1


async def test_a_new_state_is_told_again_and_so_is_the_same_state_after_a_week(
    actions: Any,
) -> None:
    await at_home.surface(actions.pool, _pending(), now=NOW)
    more = at_home.checklist(sources(
        envelope=lambda: {"configured": True, "recipients": [_recipient("k1", "fido2-prf", None)]},
        offload_receipt=lambda n: {}, drill_receipt=lambda u: {}), now=NOW)

    assert await at_home.surface(actions.pool, more, now=NOW) is not None  # a different set
    assert await at_home.surface(actions.pool, more, now=NOW + timedelta(days=3)) is None
    assert await at_home.surface(actions.pool, more, now=NOW + timedelta(days=8)) is not None


async def test_nothing_pending_clears_the_memory_so_the_next_wait_is_told_afresh(
    actions: Any,
) -> None:
    await at_home.surface(actions.pool, _pending(), now=NOW)
    clear = at_home.checklist(sources(plaintext_thread_open=lambda: False), now=NOW)

    assert await at_home.surface(actions.pool, clear, now=NOW) is None
    assert await at_home.surface(actions.pool, _pending(), now=NOW) is not None


async def test_a_desk_failure_never_fails_the_run(
    actions: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def boom(*a: Any, **k: Any) -> None:
        raise RuntimeError("desk down")

    monkeypatch.setattr("src.orchestrator.mailbox.send_message", boom)

    assert await at_home.surface(actions.pool, _pending(), now=NOW) is None


def test_the_unit_runs_the_notify_command_daily_and_the_installer_knows_it() -> None:
    root = Path(__file__).resolve().parent.parent
    service = (root / "deploy" / "osiris-at-home.service").read_text()
    timer = (root / "deploy" / "osiris-at-home.timer").read_text()
    installer = (root / "scripts" / "install_prune_timers.sh").read_text()

    assert "at-home --notify" in service and "Type=oneshot" in service
    assert "OnCalendar=*-*-*" in timer and "Persistent=true" in timer
    assert installer.count("osiris-at-home") >= 2


def test_the_checklist_is_read_only() -> None:
    source = Path(at_home.__file__).read_text()
    code = source.split('"""', 2)[2]

    # nothing here can run restic, copy a file, open an envelope recipient or start the offload
    for name in ("subprocess", "run_restic", "copy_to_target", "envelope_open", "envelope_verify",
                 "nas_first_offload", "_run_restic_backup", "mark_opened"):
        assert name not in code, name


# ---- the thread-status query, against the database --------------------------------------------


async def test_the_thread_status_query_reads_open_resolved_and_missing(actions: Any) -> None:
    from src.orchestrator.capture import open_thread, resolve_thread

    thread = await open_thread(actions, "STORAGE S6 stand-in: remove the old copy", source="test")
    prefix = str(thread)[:8]

    assert await at_home.thread_status(actions.pool, prefix) == "open"

    await resolve_thread(actions, str(thread), because="done")
    assert await at_home.thread_status(actions.pool, prefix) == "resolved"

    assert await at_home.thread_status(actions.pool, "00000000") is None


async def test_the_real_sources_read_the_live_shapes(
    actions: Any, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.orchestrator.capture import open_thread

    thread = await open_thread(actions, "stand-in for the plaintext copy job", source="test")
    monkeypatch.setattr(at_home, "PLAINTEXT_COPY_THREAD", f"thread:{str(thread)[:8]}")

    from src.orchestrator import at_home_readers

    monkeypatch.setattr(at_home_readers, "read_rollback_container", lambda: None)
    monkeypatch.setattr(at_home_readers, "read_scratch", lambda: [])
    src = await at_home.real_sources(actions.pool, now=NOW)
    result = at_home.checklist(src, now=NOW)

    assert src.plaintext_thread_open() is True  # the thread is open
    assert [i["key"] for i in result["items"]][-4] == "plaintext_copy"
    assert result["items"][-4]["status"] in ("blocked", "waiting")


BOX = {"status": "exited", "image": "postgres:16", "size": 28672}
SCRATCH = [{"path": "~/code/zzscratch-imh", "exists": True, "size": 8192},
           {"path": "~/.claude/projects/-x", "exists": True, "size": 884736}]


def test_the_word_only_items_show_what_is_there_and_never_delete() -> None:
    result = at_home.checklist(sources(plaintext_thread_open=lambda: False,
                                       rollback_container=lambda: BOX, scratch=lambda: SCRATCH),
                               now=NOW)
    items = by_key(result)

    box, scratch = items["rollback_container"], items["scratch_dirs"]
    assert box["status"] == at_home.WAITING and "postgres:16" in box["why"]
    assert "28.0 KB" in box["why"] and "needs your word; nothing deletes it by itself" in box["why"]
    assert "never the volume" in box["why"]
    assert scratch["status"] == at_home.WAITING and "zzscratch-imh (8.0 KB)" in scratch["why"]
    assert result["pending"] == 2
    src_text = Path(at_home.__file__).read_text()
    assert "rmtree" not in src_text and "os.remove" not in src_text and "unlink(" in src_text
    # the only unlink is the state-file temp; nothing in the module runs docker rm or rm
    assert '"rm"' not in src_text and '"rm",' not in src_text


def test_they_are_done_when_the_things_are_absent() -> None:
    result = at_home.checklist(
        sources(plaintext_thread_open=lambda: False,
                scratch=lambda: [{"path": "a", "exists": False, "size": None}]), now=NOW)
    items = by_key(result)

    assert items["rollback_container"]["status"] == at_home.DONE
    assert items["scratch_dirs"]["status"] == at_home.DONE
    assert result["pending"] == 0


def test_the_probe_is_optional_until_its_obligation_closes_and_is_never_counted() -> None:
    open_ = at_home.checklist(sources(plaintext_thread_open=lambda: False), now=NOW)
    closed = at_home.checklist(sources(plaintext_thread_open=lambda: False,
                                       probe_thread_open=lambda: False), now=NOW)

    assert by_key(open_)["rebuild_probe"]["status"] == at_home.OPTIONAL
    assert "probe_rebuild.py A" in by_key(open_)["rebuild_probe"]["command"]
    assert open_["pending"] == 0 and open_["next"] is None
    assert by_key(closed)["rebuild_probe"]["status"] == at_home.DONE


def test_an_unreadable_word_source_shows_unreadable_not_done() -> None:
    def boom() -> Any:
        raise RuntimeError("docker is down")

    result = at_home.checklist(sources(rollback_container=boom, scratch=boom), now=NOW)
    items = by_key(result)

    assert items["rollback_container"]["status"] == at_home.WAITING
    assert "could not be read" in items["scratch_dirs"]["why"]


def test_the_probe_does_not_make_the_desk_line_fire() -> None:
    result = at_home.checklist(sources(plaintext_thread_open=lambda: False), now=NOW)

    assert [i["key"] for i in result["items"] if i["status"] == at_home.WAITING] == []


def test_the_real_readers_say_absent_and_unreadable_apart(monkeypatch: pytest.MonkeyPatch,
                                                          tmp_path: Path) -> None:
    import subprocess as sp

    from src.orchestrator import at_home_readers

    def fake(argv: list[str], **kw: Any) -> sp.CompletedProcess[str]:
        return sp.CompletedProcess(argv, 1, "", "Error: No such object: osiris-pg-pre-repack")

    monkeypatch.setattr(sp, "run", fake)
    assert at_home_readers.read_rollback_container() is None
    monkeypatch.setattr(sp, "run", lambda argv, **kw: sp.CompletedProcess(argv, 1, "", "denied"))
    with pytest.raises(RuntimeError):
        at_home_readers.read_rollback_container()
    monkeypatch.setattr(sp, "run", lambda argv, **kw: sp.CompletedProcess(
        argv, 0, "exited|postgres:16|28672\n", ""))
    assert at_home_readers.read_rollback_container() == {"status": "exited", "image": "postgres:16",
                                                 "size": 28672}
    assert at_home_readers.read_scratch((str(tmp_path / "nope"),)) == [
        {"path": str(tmp_path / "nope"), "exists": False, "size": None}]


def test_every_run_command_is_complete_and_pasteable() -> None:
    result = at_home.checklist(sources(rollback_container=lambda: BOX, scratch=lambda: SCRATCH),
                               now=NOW)
    commands = [i["command"] for i in result["items"] if i["command"]]
    commands += [i["then"] for i in result["items"] if i.get("then")]

    assert commands and all("..." not in c and "\u2026" not in c for c in commands)
    rm = by_key(result)["scratch_dirs"]["command"]
    assert rm == 'rm -r "$HOME/code/zzscratch-imh" "$HOME/.claude/projects/-x"'


def test_the_header_counts_waiting_and_blocked_apart() -> None:
    result = at_home.checklist(sources(rollback_container=lambda: BOX, scratch=lambda: SCRATCH,
                                       offload_receipt=lambda n: {}, drill_receipt=lambda u: {}),
                               now=NOW)

    assert at_home.render(result)[0] == "3 waiting, 1 blocked on you, cheapest and safest first:"
