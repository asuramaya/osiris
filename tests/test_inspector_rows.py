"""WAVE 27, THE INSPECTOR (Thoth mail 11754/11874): the right rail shows the newer
relationship types as their own clickable rows. Five of the six are REAL links
(recorded_by, owned_by, admitted_by, vendor_of, and committed_by once Sekhmet's lane
lands) and already flow through the existing 1-hop graph walk (osiris.js's own loadRels)
unchanged -- data-driven by construction, no code needed there, no hardcoded type list to
update when a new type starts being minted. `supersedes`/`superseded_by` is the one
exception: declared as a LinkType at birth but never actually instantiated as a link row
(ruling dd04d7dd, decision 5dea28e5) -- shipped as a PROPERTY PAIR on the Decision objects
themselves instead. loadRels gained a synthetic group for these two property names,
resolved via a second /objects/{id} fetch since the pointer is a bare id with no label.
Mirrors the repo's existing static-source-guard convention for this file
(tests/test_provenance_ui.py).
"""
from __future__ import annotations

from pathlib import Path

_STATIC = Path(__file__).resolve().parent.parent / "src" / "ui" / "static"
_OSIRIS_JS = (_STATIC / "osiris.js").read_text()
_CONSOLE_JS = (_STATIC / "console.js").read_text()
_SPACE_JS = (_STATIC / "space.js").read_text()


# --- five of six are real links: no code needed, verified by absence of special-casing ----

def test_the_one_hop_walk_never_special_cases_any_link_type_by_name() -> None:
    # data-driven by construction: groups are keyed off whatever `e.type` the wire actually
    # carries, never a hardcoded allowlist -- committed_by (or any future type) appears the
    # moment it starts being minted, with zero changes here.
    body = _OSIRIS_JS.split("async function loadRels(el, id, onPick, onOpenSet, obj)", 1)[1][:900]
    assert 'const k = `${dir}|${e.type}`;' in body
    for hardcoded in ("recorded_by", "owned_by", "admitted_by", "vendor_of", "committed_by"):
        assert f'"{hardcoded}"' not in body


# --- supersedes/superseded_by: a property pair, not a link -------------------------------

def test_property_rel_names_names_exactly_the_two_dead_link_types() -> None:
    assert 'const PROPERTY_REL_NAMES = ["supersedes", "superseded_by"];' in _OSIRIS_JS


def test_load_rels_builds_a_synthetic_group_from_the_objects_own_properties() -> None:
    body = _OSIRIS_JS.split("async function loadRels(el, id, onPick, onOpenSet, obj)", 1)[1][:1500]
    assert "for (const p of (obj && obj.properties) || []) {" in body
    assert "if (!PROPERTY_REL_NAMES.includes(p.name) || !p.value || realTypes.has(p.name)) " \
        "continue;" in body
    assert "isProperty: true" in body


def test_synthetic_property_group_never_duplicates_a_real_link_of_the_same_type() -> None:
    # live-caught bug: the LinkType's own "0 rows ever" declaration has since gone stale for
    # at least one real Decision -- a genuine `supersedes` link now coexists with the
    # property pair, both naming the SAME target ("drawn twice"). A type real links already
    # cover for this object is trusted fully instead of adding a duplicate synthetic row.
    body = _OSIRIS_JS.split("async function loadRels(el, id, onPick, onOpenSet, obj)", 1)[1][:1200]
    assert "const realTypes = new Set(Object.values(groups).map((gr) => gr.type));" in body


def test_property_rel_pointer_never_offers_a_dead_open_as_set_click() -> None:
    # a property pointer is never a "set" -- exactly one target, no real link type to query
    # against -- so the control that promotes a real group to a result set is omitted
    # rather than a click that silently returns zero results.
    body = _OSIRIS_JS.split("async function loadRels(el, id, onPick, onOpenSet, obj)", 1)[1][:2400]
    assert 'const openSet = gr.isProperty ? "" :' in body


def test_property_rel_pointer_label_resolves_asynchronously_via_a_second_fetch() -> None:
    # the pointer is a bare object id with no `g.nodes` entry (it was never a link) -- its
    # real label comes from a second /objects/{id} fetch, replacing the placeholder text
    # once it resolves, never blocking the rest of the panel (which already has real labels
    # from the graph walk).
    body = _OSIRIS_JS.split("async function loadRels(el, id, onPick, onOpenSet, obj)", 1)[1]
    assert "if (!gr.isProperty) continue;" in body
    assert 'fetch(`/objects/${m.id}`).then((r) => (r.ok ? r.json() : null))' in body
    assert 'a.textContent = resolved.name ||' in body
    assert '(resolved.type === "SoftwareProject" ? ' \
        "projectDisplayName(resolved.canonical) : resolved.canonical) ||" in body
    assert "m.id.slice(0, 8);" in body


# --- objectDetail: supersedes/superseded_by leave the plain property grid ------------------

def test_object_detail_excludes_supersedes_from_the_plain_property_grid() -> None:
    # these two render as relationship rows now (loadRels), not a bare unclickable uuid
    # string sitting in the ordinary graded-fact grid.
    body = _OSIRIS_JS.split("function objectDetail(o, acts = \"\")", 1)[1][:600]
    assert '!["name", "demo", "tag", "supersedes", "superseded_by"].includes(p.name)' in body


# --- both call sites (space's own inspector, console's own browse) pass obj through --------

def test_space_inspector_passes_the_fetched_object_into_load_rels() -> None:
    body = _SPACE_JS.split("async function inspect(id)", 1)[1][:2200]
    assert "await Osiris.loadRels(relsEl, id, (pickId) => focusObject(pickId), () => {}, obj);" \
        in body


def test_console_browse_inspector_passes_the_fetched_object_into_load_rels() -> None:
    body = _CONSOLE_JS.split("async function inspect(id)", 1)[1][:900]
    assert "await Osiris.loadRels(relsEl, id, inspectOnly, openAsSet, obj);" in body
