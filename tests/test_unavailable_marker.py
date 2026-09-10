"""THE RESERVED UNAVAILABLE MARKER (wave 16 item 2, thread 04c651ce, Thoth dispatch msg
9123): the generic table renderer (osiris.js's table()) recognizes a reserved
{"_unavailable": reason} shape on a cell -- a PARTIAL failure, real data sitting right
beside it in the same row/result -- and strips it to a distinct marker rather than
flattening it as if it were real nested JSON. No browser test harness exists in this
repo (test_console_js_routes.py's own convention) -- these are static-source-guard
proofs against the served JS, mirroring test_graph_visualizer_wave_a.py/wave_b.py.
"""
from __future__ import annotations

from pathlib import Path

_JS = (Path(__file__).parent.parent / "src" / "ui" / "static" / "osiris.js").read_text()


def test_unavailable_key_is_reserved_and_checked_by_key_not_content() -> None:
    assert 'const UNAVAILABLE_KEY = "_unavailable"' in _JS
    assert "const isUnavailable = (v) =>" in _JS
    assert "UNAVAILABLE_KEY in v" in _JS


def test_txt_strips_the_marker_rather_than_flattening_it() -> None:
    txt_start = _JS.index("const _txt = (v) => {")
    txt_end = _JS.index("};", txt_start)
    txt_body = _JS[txt_start:txt_end]
    assert "isUnavailable(v)" in txt_body
    assert 'return "unavailable"' in txt_body


def test_table_cell_renders_a_distinct_dimmed_marker_with_the_reason_on_hover() -> None:
    cell_start = _JS.index("const cell = (v) => {")
    cell_end = _JS.index("};", cell_start)
    cell_body = _JS[cell_start:cell_end]
    assert "isUnavailable(v)" in cell_body
    assert "o-faint" in cell_body
    assert "v[UNAVAILABLE_KEY]" in cell_body
