"""Statusline — slow is not down (field-witnessed false-down under load, tonight): the 1.0s
asyncpg connect timeout flapped "graph unreachable" while the graph was very much UP. A
TIMEOUT (and only a timeout — a refusal/DNS/real error is actually down) earns one retry at
a wider budget; the retry's own success renders "graph slow", never a silent all-clear and
never a false "unreachable"."""
from __future__ import annotations

import io
import json
import subprocess
import sys
from pathlib import Path

import pytest
from scripts import osiris_statusline as sl

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "osiris_statusline.py"


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
               ceil: tuple[float, float, int]) -> str:
    """Render main() once with a given (spent, cap, blind) riding the counts."""
    async def fake_fetch(*a: object, **k: object) -> tuple[tuple, bool]:
        return (0, 0, 0, 0, 1, 0, 0, 0, [], ceil), False

    monkeypatch.setattr(sl, "_fetch_counts", fake_fetch)
    monkeypatch.setattr(
        sys, "stdin",
        io.StringIO(json.dumps({"workspace": {"current_dir": "/tmp/x"},
                                "session_id": "deadbeef0000",
                                "model": {"id": "claude-fable-5"}})))
    sl.main()
    return capsys.readouterr().out


def test_the_price_is_dark_until_it_matters(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """Task #26's last mile: a healthy day renders NO spend segment (the pulse carries the
    ambient number); 60% of cap lights it; an unpriced call is loud on its own — never
    silently scored as zero."""
    assert "$" not in _strip_for(monkeypatch, capsys, (1.2, 10.0, 0))    # healthy: dark
    out = _strip_for(monkeypatch, capsys, (6.5, 10.0, 0))                # 65%: lit
    assert "$6.50/$10" in out
    out2 = _strip_for(monkeypatch, capsys, (0.5, 10.0, 3))               # blind: loud
    assert "3 unpriced" in out2


def test_end_to_end_refused_port_renders_unreachable_not_a_crash() -> None:
    """A real subprocess run against a port nothing listens on: a fast refusal (never a
    timeout) must still degrade to the calm "graph unreachable" line — the chrome must not
    block or break the window it serves."""
    payload = {"workspace": {"current_dir": "/tmp"}, "session_id": "deadbeef0000",
               "model": {"id": "claude-fable-5"}}
    env = {"DATABASE_URL": "postgresql://u:p@127.0.0.1:1/osiris",
           "PATH": "/usr/bin:/bin", "HOME": str(Path.home())}
    out = subprocess.run([sys.executable, str(_SCRIPT)], input=json.dumps(payload),
                         capture_output=True, text=True, check=False, timeout=10, env=env)
    assert out.returncode == 0
    assert "graph unreachable" in out.stdout
    assert "graph slow" not in out.stdout
