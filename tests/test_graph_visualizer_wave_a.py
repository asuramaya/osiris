"""THE GRAPH VISUALIZER, WAVE A (operator dispatch, wave 15, thread 8839): readability of
the cytoscape board (src/ui/static/osiris.js's makeBoard), one numbered item per section
below, mirroring test_console_js_routes.py's own static-source-guard convention -- no
browser test harness exists in this repo, so these are string-presence proofs against the
served JS, not DOM assertions.
"""
from __future__ import annotations

from pathlib import Path

_JS = (Path(__file__).parent.parent / "src" / "ui" / "static" / "osiris.js").read_text()


# --- item 1: labels are titles, 40 chars, hidden below a zoom threshold -------------------

def test_labels_truncate_to_forty_characters() -> None:
    assert "LABEL_MAX = 40" in _JS
    assert "truncateLabel" in _JS


def test_labels_hide_below_a_zoom_threshold() -> None:
    assert "ZOOM_LABEL_THRESHOLD" in _JS
    assert 'cy.on("zoom"' in _JS and "cy.style().update()" in _JS
