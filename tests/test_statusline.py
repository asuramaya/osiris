"""Statusline — slow is not down (field-witnessed false-down under load, tonight): the 1.0s
asyncpg connect timeout flapped "graph unreachable" while the graph was very much UP. A
TIMEOUT (and only a timeout — a refusal/DNS/real error is actually down) earns one retry at
a wider budget; the retry's own success renders "graph slow", never a silent all-clear and
never a false "unreachable"."""
from __future__ import annotations

import asyncio
import io
import json
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

import pytest
from scripts import osiris_statusline as sl

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "osiris_statusline.py"


@pytest.fixture(autouse=True)
def _isolated_cache_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Every IN-PROCESS test gets its own cache dir — a shared one would let one test's
    cached fleet-counts answer a different test's query, or leak a stray file into the
    real box's scratch dir this script actually runs against."""
    monkeypatch.setenv("OSIRIS_STATUSLINE_CACHE_DIR", str(tmp_path))


async def test_a_timeout_then_success_reports_slow_not_a_clean_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The retry must use the WIDER budget (2.5s, not another 1.0s knock), and a retry that
    lands must flag `slow=True` — the caller renders "graph slow", not silence."""
    budgets: list[float] = []

    async def fake_counts(project: str, session_id: str, model_id: str = "",
                          model_raw: str = "", window_size: int | None = None,
                          *, connect_timeout: float = 1.0) -> tuple[int, ...]:
        budgets.append(connect_timeout)
        if len(budgets) == 1:
            raise TimeoutError
        return (0, 0, 0, 0, 1, 0, 0, 0, [], (0.0, 10.0, 0))

    monkeypatch.setattr(sl, "_counts", fake_counts)
    counts, slow = await sl._fetch_counts("proj", "deadbeef", "claude-fable-5",
                                          "claude-fable-5", None)
    assert slow is True
    assert counts == (0, 0, 0, 0, 1, 0, 0, 0, [], (0.0, 10.0, 0))
    assert budgets == [1.0, 2.5]   # the retry's own, wider budget — never a repeat of the first


async def test_two_timeouts_still_propagate_so_the_caller_says_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both knocks failing must NOT be swallowed here — main()'s own except-Exception is what
    renders "graph unreachable", and it can only do that if the second failure still raises."""
    async def always_times_out(*a: object, **k: object) -> tuple[int, ...]:
        raise TimeoutError

    monkeypatch.setattr(sl, "_counts", always_times_out)
    with pytest.raises(TimeoutError):
        await sl._fetch_counts("proj", "deadbeef", "claude-fable-5", "claude-fable-5", None)


async def test_a_non_timeout_failure_is_never_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    """A refused connection / real Postgres error is actually DOWN, not slow — retrying it
    wastes the statusline's own hard budget for no chance of a different answer."""
    calls = 0

    async def refused(*a: object, **k: object) -> tuple[int, ...]:
        nonlocal calls
        calls += 1
        raise ConnectionRefusedError

    monkeypatch.setattr(sl, "_counts", refused)
    with pytest.raises(ConnectionRefusedError):
        await sl._fetch_counts("proj", "deadbeef", "claude-fable-5", "claude-fable-5", None)
    assert calls == 1   # no second knock


def test_a_dm_alone_rings_the_doorbell(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """DM-only traffic must light the mail segment BY ITSELF (the Alfred chain,
    2026-07-19: seven DMs waiting, mail 0, flight 0 — and every window in the halted
    chain rendered a dim 'mail 0' because the render condition forgot `dm`)."""
    async def fake_fetch(
        *a: object, **k: object,
    ) -> tuple[tuple[int, int, int, int, int, int, int, int, list[str]], bool]:
        return (0, 0, 7, 0, 16, 0, 25, 0, [], (1.2, 10.0, 0)), False

    monkeypatch.setattr(sl, "_fetch_counts", fake_fetch)
    monkeypatch.setattr(
        sys, "stdin",
        io.StringIO(json.dumps({"workspace": {"current_dir": "/tmp/x"},
                                "session_id": "deadbeef0000",
                                "model": {"id": "claude-fable-5"}})))
    sl.main()
    out = capsys.readouterr().out
    assert "✉7" in out            # the doorbell — a DM waiting must be visible alone
    assert "graph unreachable" not in out


def _strip_for(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
               ceil: tuple[float, float, int], sid: str) -> str:
    """Render main() once with a given (spent, cap, blind) riding the counts. `sid` must be
    UNIQUE per call within a test now that renders are cache-keyed by session — three calls
    sharing one id would have the 2nd and 3rd serve the 1st's cached (and now wrong) ceil."""
    async def fake_fetch(*a: object, **k: object) -> tuple[tuple, bool]:
        return (0, 0, 0, 0, 1, 0, 0, 0, [], ceil), False

    monkeypatch.setattr(sl, "_fetch_counts", fake_fetch)
    monkeypatch.setattr(
        sys, "stdin",
        io.StringIO(json.dumps({"workspace": {"current_dir": "/tmp/x"},
                                "session_id": sid,
                                "model": {"id": "claude-fable-5"}})))
    sl.main()
    return capsys.readouterr().out


def test_the_price_is_dark_until_it_matters(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """Task #26's last mile: a healthy day renders NO spend segment (the pulse carries the
    ambient number); 60% of cap lights it; an unpriced call is loud on its own — never
    silently scored as zero."""
    assert "$" not in _strip_for(monkeypatch, capsys, (1.2, 10.0, 0), "deadbeef0001")
    out = _strip_for(monkeypatch, capsys, (6.5, 10.0, 0), "deadbeef0002")   # 65%: lit
    assert "$6.50/$10" in out
    out2 = _strip_for(monkeypatch, capsys, (0.5, 10.0, 3), "deadbeef0003")  # blind: loud
    assert "3 unpriced" in out2


def test_end_to_end_refused_port_renders_unreachable_not_a_crash(tmp_path: Path) -> None:
    """A real subprocess run against a port nothing listens on: a fast refusal (never a
    timeout) must still degrade to the calm "graph unreachable" line — the chrome must not
    block or break the window it serves."""
    payload = {"workspace": {"current_dir": "/tmp"}, "session_id": "deadbeef0000",
               "model": {"id": "claude-fable-5"}}
    env = {"DATABASE_URL": "postgresql://u:p@127.0.0.1:1/osiris",
           "PATH": "/usr/bin:/bin", "HOME": str(Path.home()),
           "OSIRIS_STATUSLINE_CACHE_DIR": str(tmp_path)}
    out = subprocess.run([sys.executable, str(_SCRIPT)], input=json.dumps(payload),
                         capture_output=True, text=True, check=False, timeout=10, env=env)
    assert out.returncode == 0
    assert "graph unreachable" in out.stdout
    assert "graph slow" not in out.stdout


# ── THE FORK STORM FIX — a shared per-session cache so a re-rendering pane stops forking a
# fresh asyncpg connection on every tick (Ra's diagnosis msg 958: load 20.5/20vcpus, 17
# concurrent osiris_statusline.py forks + 9 PG backends, driven by fleet-wide re-renders).

def test_warm_cache_skips_the_second_fetch_entirely(monkeypatch: pytest.MonkeyPatch) -> None:
    """Acceptance (i): a second render within the TTL must never call _fetch_counts — the
    only place `import asyncpg` lives, so skipping the call structurally proves no connect."""
    calls = 0

    async def fake_fetch(*a: object, **k: object) -> tuple[tuple, bool]:
        nonlocal calls
        calls += 1
        return (1, 2, 3, 0, 4, 5, 6, 7, [], (0.0, 10.0, 0)), False

    monkeypatch.setattr(sl, "_fetch_counts", fake_fetch)
    first = sl._counts_cached("proj", "session-a", "claude-fable-5", "claude-fable-5", None)
    second = sl._counts_cached("proj", "session-a", "claude-fable-5", "claude-fable-5", None)
    assert calls == 1
    assert first == second


def test_two_renders_via_main_share_one_live_fetch(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """The acceptance shape at the real call site: two statusline renders for the same
    pane, back to back — the second must still render correctly (mail/dm intact) with
    `_counts` (not just `_fetch_counts`) called only once end to end."""
    calls = 0

    async def fake_counts(*a: object, **k: object) -> tuple[int, ...]:
        nonlocal calls
        calls += 1
        return (0, 0, 7, 0, 16, 0, 25, 0, [], (1.2, 10.0, 0))

    monkeypatch.setattr(sl, "_counts", fake_counts)
    payload = json.dumps({"workspace": {"current_dir": "/tmp/x"}, "session_id": "session-g",
                          "model": {"id": "claude-fable-5"}})
    monkeypatch.setattr(sys, "stdin", io.StringIO(payload))
    sl.main()
    monkeypatch.setattr(sys, "stdin", io.StringIO(payload))
    sl.main()
    out = capsys.readouterr().out
    assert calls == 1
    assert "✉7" in out


def test_seventeen_concurrent_renders_make_exactly_one_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Acceptance (ii): 17 panes racing the SAME session's cache must collapse to exactly
    one live fetch — every other thread either serves the fresh answer once it lands or
    degrades quietly (no cache yet + lock busy); none of them fetch a second time."""
    calls = 0
    calls_lock = threading.Lock()
    start = threading.Barrier(17)

    async def fake_fetch(*a: object, **k: object) -> tuple[tuple, bool]:
        nonlocal calls
        with calls_lock:
            calls += 1
        await asyncio.sleep(0.1)  # widen the race window so all 17 threads pile up together
        return (0, 0, 0, 0, 1, 0, 0, 0, [], (0.0, 10.0, 0)), False

    monkeypatch.setattr(sl, "_fetch_counts", fake_fetch)
    results: list[Any] = [None] * 17

    def run(i: int) -> None:
        start.wait()
        try:
            results[i] = sl._counts_cached(
                "proj", "session-b", "claude-fable-5", "claude-fable-5", None)
        except sl._StatuslineDegrade:
            results[i] = "degraded"

    threads = [threading.Thread(target=run, args=(i,)) for i in range(17)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)
    assert calls == 1
    assert all(r is not None for r in results)  # no thread crashed or hung


def test_stale_cache_served_when_a_sibling_holds_the_refresh_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cache past its TTL, with another pane's refresh already in flight (the lock held
    elsewhere), must still answer from the stale value — never block, never duplicate the
    query underneath a sibling that's already making one."""
    monkeypatch.setenv("OSIRIS_STATUSLINE_CACHE_TTL", "0")
    stale = (9, 9, 9, 0, 1, 0, 0, 0, [], (0.0, 10.0, 0))
    cache_path = sl._cache_dir() / f"{sl._cache_key('session-c')}.json"
    sl._cache_write(cache_path, stale, False)
    import time
    time.sleep(0.01)  # past the TTL=0 window

    import fcntl
    lock_fh = open(cache_path.with_suffix(".lock"), "a+")
    fcntl.flock(lock_fh, fcntl.LOCK_EX)  # simulate a sibling pane's in-flight refresh
    try:
        async def should_not_be_called(*a: object, **k: object) -> tuple[tuple, bool]:
            raise AssertionError("a stale-but-served cache must never trigger a second fetch")

        monkeypatch.setattr(sl, "_fetch_counts", should_not_be_called)
        result = sl._counts_cached(
            "proj", "session-c", "claude-fable-5", "claude-fable-5", None)
        assert result == (stale, False)
    finally:
        fcntl.flock(lock_fh, fcntl.LOCK_UN)
        lock_fh.close()


def test_no_cache_and_lock_busy_degrades_quietly(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec (c): no cache to fall back on, and a sibling holds the refresh lock — this must
    raise the degrade signal (main()'s except-Exception renders it as the quiet minimal
    line) rather than block on the sibling or fire a second query."""
    cache_path = sl._cache_dir() / f"{sl._cache_key('session-d')}.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    import fcntl
    lock_fh = open(cache_path.with_suffix(".lock"), "a+")
    fcntl.flock(lock_fh, fcntl.LOCK_EX)
    try:
        async def should_not_be_called(*a: object, **k: object) -> tuple[tuple, bool]:
            raise AssertionError("no cache + a busy lock must degrade, never fetch")

        monkeypatch.setattr(sl, "_fetch_counts", should_not_be_called)
        with pytest.raises(sl._StatuslineDegrade):
            sl._counts_cached("proj", "session-d", "claude-fable-5", "claude-fable-5", None)
    finally:
        fcntl.flock(lock_fh, fcntl.LOCK_UN)
        lock_fh.close()


def test_corrupt_cache_file_is_just_a_miss(monkeypatch: pytest.MonkeyPatch) -> None:
    """A half-written or garbled cache file (a torn read racing a rename, disk weirdness)
    must fall through to a live fetch, never raise out of the chrome's own render path."""
    cache_path = sl._cache_dir() / f"{sl._cache_key('session-e')}.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text("not json{{{")

    async def fake_fetch(*a: object, **k: object) -> tuple[tuple, bool]:
        return (1, 1, 1, 0, 1, 0, 0, 0, [], (0.0, 10.0, 0)), False

    monkeypatch.setattr(sl, "_fetch_counts", fake_fetch)
    result = sl._counts_cached("proj", "session-e", "claude-fable-5", "claude-fable-5", None)
    assert result == ((1, 1, 1, 0, 1, 0, 0, 0, [], (0.0, 10.0, 0)), False)


def test_no_session_id_never_touches_the_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without a session id there is nothing safe to key a cache on — every render stays
    live, exactly as it did before this fix (an older harness payload, or garbage input)."""
    calls = 0

    async def fake_fetch(*a: object, **k: object) -> tuple[tuple, bool]:
        nonlocal calls
        calls += 1
        return (0, 0, 0, 0, 0, 0, 0, 0, [], (0.0, 10.0, 0)), False

    monkeypatch.setattr(sl, "_fetch_counts", fake_fetch)
    sl._counts_cached("proj", "", "claude-fable-5", "claude-fable-5", None)
    sl._counts_cached("proj", "", "claude-fable-5", "claude-fable-5", None)
    assert calls == 2
