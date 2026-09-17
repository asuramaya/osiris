"""WAVE 28, THE TAXONOMY SWEEP, Seshat's UI-strings piece (ruling 70c001ec, thread a47a0c7f,
Thoth mail 12011/12063/12200): the one reader-visible retired-word string in the UI static
files (osiris.js's own bundle-composition summary, "fan out by neighborhood") and the two
identifiers a reader can meet in devtools (the district-label CSS class, the
`district:<name>` synthetic pseudo-node id) -- census's own Surface-1 finding, decision
70c001ec's own NEIGHBORHOOD -> project/focus and district (PROJECT) ruling. Landed first,
its own tip (eab3434d/06108d26); the internal data-model naming and every code comment
(districts/districtByName/buildDistrictModel/landmarks/hub, all bare code identifiers never
shown to a reader) followed as the "second pass" once Imhotep's ratchet fix let it strip
`/* */` block comments too (mail 12200) -- see tests/test_drawing_tip.py and
tests/test_community_regions.py for that pass's own coverage. Mirrors the repo's existing
static-source-guard convention: string/substring proofs against the served JS/HTML, no
browser harness.
"""
from __future__ import annotations

from pathlib import Path

_STATIC = Path(__file__).parent.parent / "src" / "ui" / "static"
_OSIRIS_JS = (_STATIC / "osiris.js").read_text()
_SPACE_JS = (_STATIC / "space.js").read_text()
_INDEX_HTML = (_STATIC / "index.html").read_text()
_SPACE_HTML = (_STATIC / "space.html").read_text()


def test_bundle_composition_summary_fans_out_by_project_not_neighborhood() -> None:
    assert 'case "bundle": return [...inner, `⑂ fan out by ${spec.by || "project"}`];' \
        in _OSIRIS_JS
    assert "neighborhood" not in _OSIRIS_JS.split('case "bundle"', 1)[1][:100]


def test_project_pseudo_node_id_template_no_longer_says_district() -> None:
    body = _SPACE_JS.split("function buildProjectLabelCandidates()", 1)[1][:300]
    assert "__isProjectFill: true, id: `project:${d.name}`, name: d.name," in body
    assert "district:" not in body


def test_project_pseudo_node_css_class_no_longer_says_district() -> None:
    body = _SPACE_JS.split("function pickLabels()", 1)[1][:3400]
    assert 'div.className = nd.__isProjectFill ? "lbl project-label"' in body


def test_project_label_css_rule_is_named_project_not_district() -> None:
    for html in (_INDEX_HTML, _SPACE_HTML):
        assert ".lbl.project-label {" in html
        assert ".lbl.district-label" not in html
