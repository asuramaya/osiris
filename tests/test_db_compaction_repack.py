"""pg_repack, run inside the database container, and inert until it is installed: the choice
needs the extension AND a client that answers in the container. The client is stopped with
SIGINT (cleanup) and a grace period, never a plain kill. Tests use a fake `docker exec`; no
test ever reaches a real container."""
from __future__ import annotations

import asyncio
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator import db_compaction
from src.orchestrator.db_compaction import (
    CLIENT_UNANSWERED,
    REPACK_MIN_SECS,
    REPACK_STOP_GRACE_SECS,
    LockNotAcquired,
    WindowTooShort,
    _docker_exec,
    repack_command,
    repack_ready,
    rewrite_table,
)

SCRATCH = "compaction_scratch_repack"
DSN = "postgresql://osiris:s3cret@127.0.0.1:5601/osiris?application_name=osiris-worker"


class FakeExec:
    def __init__(self, *replies: tuple[int, str]) -> None:
        self.replies = list(replies)
        self.calls: list[tuple[list[str], float]] = []

    async def __call__(self, args: list[str], timeout_secs: float) -> tuple[int, str]:
        self.calls.append((args, timeout_secs))
        return self.replies.pop(0) if len(self.replies) > 1 else self.replies[0]


def _installed(monkeypatch: pytest.MonkeyPatch, present: bool) -> None:
    async def fake(_pool: Any, _name: str) -> bool:
        return present

    monkeypatch.setattr(db_compaction, "_installed", fake)


async def test_it_is_inert_without_the_extension_and_never_runs_docker(
    actions: Actions,
) -> None:
    ran = FakeExec((0, "pg_repack 1.5.3"))
    assert await repack_ready(actions.pool, ran) is False
    assert ran.calls == []                                   # no extension: docker is not asked
    await actions.pool.execute(
        f"CREATE TABLE IF NOT EXISTS {SCRATCH} (id int primary key, v text)")
    await actions.pool.execute(
        f"INSERT INTO {SCRATCH} SELECT g, 'x' FROM generate_series(1, 50) g "
        "ON CONFLICT DO NOTHING")
    engine = await rewrite_table(actions.pool, SCRATCH, seconds_left=300, dsn=DSN, exec_fn=ran)
    assert engine == "VACUUM FULL" and ran.calls == []


async def test_it_needs_a_client_that_answers_in_the_container(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _installed(monkeypatch, True)
    probe = FakeExec((0, "pg_repack 1.5.3"))
    assert await repack_ready(actions.pool, probe) is True
    assert probe.calls == [(["exec", "osiris-pg", "pg_repack", "--version"],
                            db_compaction.REPACK_PROBE_SECS)]
    for code in (1, 125, 127):                               # no client, no container, no docker
        assert await repack_ready(actions.pool, FakeExec((code, "nope"))) is False


async def test_without_a_client_the_rewrite_falls_back_to_vacuum_full(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _installed(monkeypatch, True)
    await actions.pool.execute(
        f"CREATE TABLE IF NOT EXISTS {SCRATCH} (id int primary key, v text)")
    gone = FakeExec((127, "docker: not found"))
    assert await rewrite_table(
        actions.pool, SCRATCH, seconds_left=300, dsn=DSN, exec_fn=gone) == "VACUUM FULL"
    assert [c[0][-1] for c in gone.calls] == ["--version"]   # only the probe, never a repack


async def test_the_command_runs_the_client_in_the_container_and_stops_it_gently(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _installed(monkeypatch, True)
    fake = FakeExec((0, "pg_repack 1.5.3"), (0, "repacked"))
    engine = await rewrite_table(
        actions.pool, "assertions_hot", seconds_left=6900, dsn=DSN, exec_fn=fake)
    assert engine == "pg_repack"
    args, deadline = fake.calls[1]
    assert args[:3] == ["exec", "osiris-pg", "timeout"]
    assert "--signal=INT" in args and f"--kill-after={REPACK_STOP_GRACE_SECS}" in args
    assert "6810s" in args                                   # the window left, minus the grace
    assert args[args.index("pg_repack") + 1:][:4] == ["-U", "osiris", "-d", "osiris"]
    assert "--no-kill-backend" in args and "--wait-timeout=5" in args
    assert args[args.index("--table") + 1] == "assertions_hot"
    assert "s3cret" not in " ".join(args)                    # the password never leaves the box
    assert not any(a in args for a in ("--dbname", DSN, "-9", "--signal=KILL", "--signal=9"))
    assert deadline > 6810 + REPACK_STOP_GRACE_SECS


def test_a_short_allowance_still_gets_a_floor_and_the_user_comes_from_the_dsn() -> None:
    short = repack_command("outbox", stop_after_secs=30, user="osiris", database="osiris")
    assert "30s" in short
    assert db_compaction._user_and_database(DSN) == ("osiris", "osiris")
    assert db_compaction._user_and_database(None) == ("osiris", "osiris")
    assert db_compaction._user_and_database("postgresql://other@h/db2") == ("other", "db2")
    assert db_compaction._user_and_database("postgresql://os%40iris:p%2Fw@h/db%202") == (
        "os@iris", "db 2")


async def test_a_run_stopped_by_the_timeout_says_it_cleaned_up_and_changed_nothing(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _installed(monkeypatch, True)
    fake = FakeExec((0, "pg_repack 1.5.3"), (124, "NOTICE: canceled"))
    with pytest.raises(RuntimeError, match="stopped when the time allowed ran out.*cleaned up"):
        await rewrite_table(actions.pool, "outbox", seconds_left=600, dsn=DSN, exec_fn=fake)


async def test_a_run_that_had_to_be_killed_names_the_cleanup(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _installed(monkeypatch, True)
    fake = FakeExec((0, "pg_repack 1.5.3"), (137, ""))
    with pytest.raises(RuntimeError, match="osiris backfill repack_cleanup"):
        await rewrite_table(actions.pool, "outbox", seconds_left=600, dsn=DSN, exec_fn=fake)


async def test_any_other_failure_is_reported_with_its_tail(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _installed(monkeypatch, True)
    fake = FakeExec((0, "pg_repack 1.5.3"), (1, "ERROR: relation has no primary key"))
    with pytest.raises(RuntimeError, match=r"failed \(exit 1\).*no primary key"):
        await rewrite_table(actions.pool, "outbox", seconds_left=600, dsn=DSN, exec_fn=fake)


async def test_the_docker_client_never_raises_and_is_never_killed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def nowhere(*_a: Any, **_k: Any) -> Any:
        raise FileNotFoundError("docker")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", nowhere)
    code, out = await _docker_exec(["exec", "x"], 5)
    assert code == 127 and "FileNotFoundError" in out

    class Slow:
        returncode = None
        terminated = False
        killed = False

        async def communicate(self) -> tuple[bytes, bytes]:
            await asyncio.sleep(30)
            return b"", b""

        waited = False

        def terminate(self) -> None:
            self.terminated = True

        async def wait(self) -> int:
            self.waited = True
            return -15

        def kill(self) -> None:
            self.killed = True

    slow = Slow()

    async def start(*_a: Any, **_k: Any) -> Slow:
        return slow

    monkeypatch.setattr(asyncio, "create_subprocess_exec", start)
    code, _ = await _docker_exec(["exec", "x"], 0.05)
    assert code == CLIENT_UNANSWERED and code != 124
    assert slow.terminated and slow.waited and not slow.killed


async def test_the_container_name_can_be_overridden(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OSIRIS_PG_CONTAINER", "other-pg")
    assert repack_command("t", stop_after_secs=60, user="u", database="d")[1] == "other-pg"


# what the live extension prints and exits with when a conflicting lock is not released within
# --wait-timeout (checked against pg_repack 1.5.3 with a table lock held by another session)
SKIPPED = ('INFO: repacking table "public.outbox"\n'
           "WARNING: timed out, do not cancel conflicting backends\n"
           "INFO: Skipping repack public.outbox due to timeout")


async def _no_sleep(_secs: float) -> None:
    return None


async def test_a_lock_that_pg_repack_gave_up_on_is_retried_not_counted_as_a_rewrite(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _installed(monkeypatch, True)
    fake = FakeExec((0, "pg_repack 1.5.3"), (0, SKIPPED), (0, SKIPPED), (0, "repacked"))
    engine = await rewrite_table(
        actions.pool, "outbox", seconds_left=3000, dsn=DSN, exec_fn=fake, sleep=_no_sleep)
    assert engine == "pg_repack"
    assert [c[0][c[0].index("pg_repack"):][:1] for c in fake.calls[1:]] == [["pg_repack"]] * 3


async def test_a_lock_that_never_frees_is_a_skip_after_the_same_four_tries(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _installed(monkeypatch, True)
    fake = FakeExec((0, "pg_repack 1.5.3"), (0, SKIPPED))
    with pytest.raises(LockNotAcquired) as caught:
        await rewrite_table(
            actions.pool, "outbox", seconds_left=3000, dsn=DSN, exec_fn=fake, sleep=_no_sleep)
    assert len(fake.calls) == 1 + 4                          # the probe and four tries
    assert "nothing was changed" in caught.value.reason and "5 s" in caught.value.reason


async def test_retries_stop_when_too_little_of_the_window_is_left(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _installed(monkeypatch, True)
    now = [0.0]

    async def spend(_secs: float) -> None:
        now[0] += 200                                        # each wait and try uses up time

    fake = FakeExec((0, "pg_repack 1.5.3"), (0, SKIPPED))
    with pytest.raises(LockNotAcquired):
        await rewrite_table(actions.pool, "outbox", seconds_left=300, dsn=DSN, exec_fn=fake,
                            sleep=spend, clock=lambda: now[0])
    assert len(fake.calls) == 1 + 1                          # one try; no room for another


async def test_a_window_with_too_little_left_does_not_start_a_repack(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _installed(monkeypatch, True)
    fake = FakeExec((0, "pg_repack 1.5.3"))
    with pytest.raises(WindowTooShort) as caught:
        await rewrite_table(actions.pool, "outbox", seconds_left=REPACK_MIN_SECS - 1, dsn=DSN,
                            exec_fn=fake)
    assert len(fake.calls) == 1                              # only the probe
    assert "nothing was changed" in caught.value.reason
    fake2 = FakeExec((0, "pg_repack 1.5.3"), (0, "repacked"))
    assert await rewrite_table(actions.pool, "outbox", seconds_left=REPACK_MIN_SECS, dsn=DSN,
                               exec_fn=fake2) == "pg_repack"


async def test_a_client_that_stopped_answering_is_not_read_as_a_clean_stop(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _installed(monkeypatch, True)
    fake = FakeExec((0, "pg_repack 1.5.3"), (CLIENT_UNANSWERED, "the docker client did not answer"))
    with pytest.raises(RuntimeError, match="state of the repack is unknown.*repack schema"):
        await rewrite_table(actions.pool, "outbox", seconds_left=600, dsn=DSN, exec_fn=fake)


async def test_the_night_run_records_a_short_window_as_a_skip(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def short(*_a: Any, **_k: Any) -> str:
        raise WindowTooShort("assertions_hot", 100)

    monkeypatch.setattr(db_compaction, "rewrite_table", short)
    await actions.pool.execute(
        "CREATE TABLE IF NOT EXISTS compaction_scratch_short (id int primary key, v text)")
    await actions.pool.execute("INSERT INTO compaction_scratch_short SELECT g, repeat('x', 800) "
                               "FROM generate_series(1, 20000) g ON CONFLICT DO NOTHING")
    await actions.pool.execute("DELETE FROM compaction_scratch_short WHERE id % 10 < 9")

    async def fake_reclaim(_pool: Any, _table: str) -> tuple[int, str]:
        return 3 * 1024 ** 3, "test"

    monkeypatch.setattr(db_compaction, "reclaimable_bytes", fake_reclaim)

    async def calm(_pool: Any) -> dict[str, Any]:
        return {"paused": False, "free_gb": 500.0, "floor_gb": 10.0, "reason": ""}

    out = await db_compaction.compact_tonight(
        actions.pool, window_secs=3000, tables=("compaction_scratch_short",), brake=calm)
    receipt = out["compaction_scratch_short"]
    assert receipt["action"] == "skipped" and "only 100 s" in receipt["reason"]
