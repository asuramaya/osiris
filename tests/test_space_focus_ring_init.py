"""The focus ring must never be touched before it exists.

The frame loop (renderIfDirty -> positionLabels -> positionFocusRing) can fire at any await while
the space is still being set up. The ring was a `const` created far below that code, so a frame
that landed early threw "Cannot access 'focusRingEl' before initialization" at page load. It is
now declared with the render state, before any frame can run, and the position code skips it
until it exists. (A real page-load check needs the graph, the WebGL scene and a browser; this
pins the ordering that caused the error.)"""
from __future__ import annotations

from pathlib import Path

_JS = (Path(__file__).parent.parent / "src" / "ui" / "static" / "space.js").read_text()


def test_the_ring_is_declared_before_the_frame_loop_can_use_it() -> None:
    assert "const focusRingEl" not in _JS
    declared = _JS.index("let focusRingEl = null;")
    assert declared < _JS.index("function renderIfDirty()")
    assert declared < _JS.index("function positionFocusRing()")


def test_the_ring_is_created_once_and_position_code_waits_for_it() -> None:
    assert _JS.count('focusRingEl = document.createElement("div");') == 1
    body = _JS.split("function positionFocusRing() {", 1)[1].split("\n  }\n", 1)[0]
    assert body.index("if (!focusRingEl) return;") < body.index("focusRingEl.hidden")
