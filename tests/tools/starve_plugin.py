"""A pytest plugin that simulates a starved machine, to prove a suspected load flake.

HOW TO USE (one line):
    STARVE_LOOP_STALL=0.2 STARVE_COMMUNICATE=3 uv run pytest -p tests.tools.starve_plugin <files> -q

Never loaded by default, and inert unless an environment variable below is set, so it can sit
in the tree without touching a normal run. Run the suspect tests once without it, once with
it: a test that races real time fails under it and a test that waits for its condition does
not. It injects the starvation instead of burning CPU, which is why it is deterministic
(a 40 s run) and safe to repeat; real CPU pressure alone did not reproduce the flakes it was
built for. Check `systemctl --user list-units 'osiris-chain-*'` before any run that also
loads the machine.

  STARVE_LOOP_STALL=<s>   every asyncio.sleep first blocks the event loop thread for <s>
                          seconds, so a poll loop that ticks every 10 ms really ticks every
                          <s> seconds and a fixed real-time window sees almost nothing
  STARVE_COMMUNICATE=<s>  every subprocess communicate() takes <s> extra seconds, the way
                          `git status` does with the box at load average 30 (a product
                          timeout shorter than <s> then fires)

Specimens it reproduced: a settle test whose 2 s git-status timeout returned None, and a
watchdog test that expected two log records inside a fixed 150 ms window and saw none.
"""
from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Callable, Iterator
from typing import Any

import pytest


def install_loop_stall(seconds: float) -> Callable[[], None]:
    """Make every asyncio.sleep block the event loop thread for `seconds` first. Returns the
    function that restores the real sleep."""
    real_sleep = asyncio.sleep

    async def _stalling_sleep(delay: float, *args: Any, **kwargs: Any) -> Any:
        time.sleep(seconds)  # noqa: ASYNC251, blocking the loop is the injected starvation
        return await real_sleep(delay, *args, **kwargs)

    asyncio.sleep = _stalling_sleep  # type: ignore[assignment]

    def _restore() -> None:
        asyncio.sleep = real_sleep  # type: ignore[assignment]

    return _restore


def _seconds(name: str) -> float:
    try:
        return float(os.environ.get(name, "0") or 0)
    except ValueError:
        return 0.0


_STALL = _seconds("STARVE_LOOP_STALL")
_COMMUNICATE = _seconds("STARVE_COMMUNICATE")

if _STALL:
    install_loop_stall(_STALL)


@pytest.fixture(autouse=True)
def _slow_communicate(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    if _COMMUNICATE:
        process = asyncio.subprocess.Process
        real_communicate = process.communicate
        real_kill = process.kill

        async def _slow(self: Any, *args: Any, **kwargs: Any) -> Any:
            await asyncio.sleep(_COMMUNICATE)
            return await real_communicate(self, *args, **kwargs)

        def _tolerant_kill(self: Any) -> None:
            try:
                real_kill(self)
            except ProcessLookupError:  # the simulated-slow process had already exited
                pass

        monkeypatch.setattr(process, "communicate", _slow)
        monkeypatch.setattr(process, "kill", _tolerant_kill)
    yield
