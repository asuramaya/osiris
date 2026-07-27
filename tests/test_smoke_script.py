"""scripts/osiris_smoke.py — the judgment layer is pure, so it's tested the same way
tests/test_preflight.py tests scripts/osiris_preflight.py's `evaluate()`: real inputs, no
live network. The script's own IO (the MCP round-trip, the operator brief) is
live-environment-dependent by nature, same precedent as preflight's own untested collectors.
"""
from __future__ import annotations

from scripts.osiris_smoke import _fails_from


def _green_chrome() -> dict:
    return {"/desk": "ok", "/live-desk": "ok", "/mail": "ok", "/fleet": "ok",
            "/roadmap": "ok", "/canon": "ok", "/overhead": "ok", "/membrane": "ok"}


def test_all_green_yields_no_failures() -> None:
    assert _fails_from(_green_chrome(), {"chrome": _green_chrome(), "db": "ok", "ok": True}) == []


def test_a_chrome_route_failure_is_named() -> None:
    chrome = {**_green_chrome(), "/roadmap": "http 500"}
    fails = _fails_from(chrome, {"chrome": _green_chrome(), "db": "ok", "ok": True})
    assert fails == ["chrome /roadmap: http 500"]


def test_mcp_unreachable_is_named_not_silently_dropped() -> None:
    """A bare error STRING (call_mcp_smoke's own return on a failed round-trip) — the exact
    shape thread 1849d800 asked for: osiris-mcp being down must be its OWN loud finding, not
    a blank in the report."""
    fails = _fails_from(_green_chrome(), "error: connection refused")
    assert fails == ["osiris-mcp round-trip: error: connection refused"]


def test_mcps_own_pool_failure_is_named() -> None:
    fails = _fails_from(_green_chrome(), {"chrome": _green_chrome(),
                                          "db": "error: Event loop is closed", "ok": False})
    assert fails == ["osiris-mcp pool: error: Event loop is closed"]


def test_mcps_own_chrome_view_can_disagree_with_the_local_walk() -> None:
    """The two chrome walks are INDEPENDENT (this script's own httpx client vs. osiris-mcp's)
    — a route reachable from one vantage but not the other is a real, distinct finding, not
    a duplicate to be collapsed."""
    mcp_chrome = {**_green_chrome(), "/desk": "error: connection refused"}
    fails = _fails_from(_green_chrome(), {"chrome": mcp_chrome, "db": "ok", "ok": False})
    assert fails == ["osiris-mcp's own chrome view /desk: error: connection refused"]


def test_failures_from_both_sources_all_land() -> None:
    chrome = {**_green_chrome(), "/mail": "http 404"}
    fails = _fails_from(chrome, "error: timed out")
    assert fails == ["chrome /mail: http 404", "osiris-mcp round-trip: error: timed out"]
