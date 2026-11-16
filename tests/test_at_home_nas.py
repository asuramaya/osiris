"""The NAS routes behind the console's At home panel: lines become JSON objects, the stream
closes with a verdict, and the restore-test result reads from the receipt."""
from __future__ import annotations

import json

from src.orchestrator.at_home_nas import parse_step_line, restore_test_summary, stream_nas


def test_a_step_line_becomes_an_object_and_a_stop_is_a_failure() -> None:
    assert parse_step_line("[3/6] the offload finished.") == {
        "n": 3, "of": 6, "status": "ok", "text": "the offload finished."}
    stopped = parse_step_line("[1/6] STOPPED: not on the home network.")
    assert stopped["status"] == "failed" and stopped["n"] == 1
    assert stopped["text"] == "not on the home network."
    advice = parse_step_line("        next: run this again at home.")
    assert advice == {"status": "info", "text": "next: run this again at home."}


async def test_the_stream_gives_one_line_per_thing_said_and_a_closing_verdict() -> None:
    async def runner(say) -> int:
        say("NAS first offload to home (sftp://nas/repo):")
        say("[1/6] home network: reachable.")
        say("[2/6] STOPPED: the repository is locked.")
        say("        next: wait for the other run.")
        return 1

    lines = [json.loads(x) async for x in stream_nas(runner)]
    assert [x.get("n") for x in lines[:4]] == [None, 1, 2, None]
    assert lines[2]["status"] == "failed"
    assert lines[-1] == {"done": True, "ok": False, "text": "next: wait for the other run."}


async def test_a_runner_that_breaks_ends_the_stream_with_a_sentence_not_a_traceback() -> None:
    async def runner(say) -> int:
        say("[1/6] starting.")
        raise RuntimeError("secret detail that must not reach the page")

    lines = [json.loads(x) async for x in stream_nas(runner)]
    assert lines[-1]["done"] is True and lines[-1]["ok"] is False
    assert "secret detail" not in json.dumps(lines)


def test_the_restore_test_reads_never_run_passed_and_failed() -> None:
    target = {"name": "home", "path_or_url": "sftp://nas/repo"}
    assert restore_test_summary(None, {})["ok"] is None
    assert restore_test_summary(target, {})["ok"] is None
    passed = restore_test_summary(target, {
        "last_attempt_at": "2026-11-02T01:00:00+00:00",
        "last_passed_at": "2026-11-02T01:00:00+00:00", "last_error": None})
    assert passed["ok"] is True and passed["when"] == "2026-11-02T01:00:00+00:00"
    failed = restore_test_summary(target, {
        "last_attempt_at": "2026-11-03T01:00:00+00:00",
        "last_passed_at": "2026-11-02T01:00:00+00:00", "last_error": "check failed"})
    assert failed["ok"] is False and "check failed" in failed["text"]
