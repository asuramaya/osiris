"""THE READING LAYER, part C: HARMONY (ruling c5953bb1, Thoth DM 10596, thread 71c4ca0d),
AMENDED by THE LEGIBILITY PASS TIP 1's own amendment (operator via Thoth mail 10726):
select-vs-focus is retired -- "select, inspector, hide, fit, one gesture." A table row
click is now always a real focus, the same one gesture a canvas click is (see
test_legibility_pass_tip1.py for the amendment's own tests). What survives from part C:
the table filters to the reachable set while a real focus is on, and every object
reference in the inspector already walks the focus. Mirrors the existing
static-source-guard convention.
"""
from __future__ import annotations

from pathlib import Path

_STATIC = Path(__file__).parent.parent / "src" / "ui" / "static"
_SPACE_JS = (_STATIC / "space.js").read_text()
_CONSOLE_JS = (_STATIC / "console.js").read_text()


def test_table_filters_to_the_reachable_set_while_a_focus_is_on() -> None:
    body = _CONSOLE_JS.split("function renderEntityExplorerStage()", 1)[1].split(
        "\n}\n", 1)[0]
    assert "space.pathFocusId" in body
    assert "space.pathReachable" in body
    assert "filtered.filter(function(o) { return reachable.has(o.id); });" in body


def test_table_filter_reads_off_the_space_apis_own_live_getters() -> None:
    # pathReachable/pathFocusId are real getters on the api object (not a stale snapshot),
    # so console.js always sees the CURRENT focus state without any extra wiring.
    assert "get pathReachable() { return pathReachable; }" in _SPACE_JS
    assert "get pathFocusId() { return pathFocusId; }" in _SPACE_JS


def test_a_table_row_click_always_focuses() -> None:
    # TIP 1 AMENDMENT (mail 10726): a table row click is the same one-gesture focus a
    # canvas click is now -- no separate select/pan-only branch, no pathFocusId conditional.
    body = _CONSOLE_JS.split("function inspectOnly(id)", 1)[1][:1300]
    assert "if (space) space.focusObject(id);" in body
    assert "selectObject" not in body


def test_a_canvas_click_focuses_the_same_way_a_table_row_does() -> None:
    # window widened for TIP 3 (Thoth mail 10930): a far/mid-tier glyph drill-in branch now
    # runs first; the near-tier focusObject(hit.id) call sits after it.
    click_body = _SPACE_JS.split(
        'renderer.domElement.addEventListener("click", (ev) => {', 1)[1][:500]
    assert "focusObject(hit.id)" in click_body


def test_every_inspector_reference_already_walks_the_focus() -> None:
    # built in part B's own inspect() -- re-confirmed here since it's exactly what part C's
    # "harmony" asks for (upstream_ids, readers, links all route through Osiris.loadRels'
    # own pick callback).
    body = _SPACE_JS.split("async function inspect(id)", 1)[1][:2400]
    assert "Osiris.loadRels(relsEl, id, (pickId) => focusObject(pickId), () => {}, obj);" in body
