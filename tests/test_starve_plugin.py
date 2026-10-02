"""The starve plugin (tests/tools/starve_plugin.py) is the instrument for proving a load flake,
so it must be inert unless asked and must restore what it patches."""
from __future__ import annotations

import asyncio
import time

import pytest

from tests.tools import starve_plugin


@pytest.mark.skipif(
    bool(starve_plugin._STALL or starve_plugin._COMMUNICATE),
    reason="the plugin is switched on for this run, which is what this test says it is not")
async def test_importing_the_plugin_changes_nothing_unless_a_variable_is_set() -> None:
    """It sits in the tree, so a normal run (neither variable set) must see the real sleep
    and a zero stall; the suite itself imports it right here."""
    assert starve_plugin._STALL == 0.0 and starve_plugin._COMMUNICATE == 0.0
    assert asyncio.sleep.__name__ == "sleep"


async def test_a_loop_stall_blocks_the_loop_and_is_fully_restored() -> None:
    real = asyncio.sleep
    restore = starve_plugin.install_loop_stall(0.05)
    try:
        started = time.monotonic()
        await asyncio.sleep(0)  # no wait of its own: any delay is the injected stall
        assert time.monotonic() - started >= 0.05  # a lower bound: starvation cannot break it
        assert asyncio.sleep is not real
    finally:
        restore()
    assert asyncio.sleep is real
