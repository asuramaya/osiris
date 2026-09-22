"""THE NAME, NEVER THE SLUG (operator ruling a1cde8a3, Thoth mail 12807): a rename
migrates the canonical too (repo:<new_name>), so the operator's own target -- "no old
name anywhere a human sees it" -- extends to the raw `repo:` scheme itself: a human says
"lotstretcher", never "repo:lotstretcher". Audited every place the console/space frontend
prints a project's canonical where a human expects a name (labels, pseudo-node ids,
tables, tooltips, the dossier headline, the palette) and made the CURRENT NAME primary,
the canonical at most a secondary muted line or title attribute. Mirrors the repo's
existing static-source-guard convention: string/substring proofs against the served JS,
no browser harness.
"""
from __future__ import annotations

from pathlib import Path

_STATIC = Path(__file__).parent.parent / "src" / "ui" / "static"
_OSIRIS_JS = (_STATIC / "osiris.js").read_text()
_SPACE_JS = (_STATIC / "space.js").read_text()
_CONSOLE_JS = (_STATIC / "console.js").read_text()


# --- the shared helpers (osiris.js) ---------------------------------------------------------

def test_project_display_name_strips_the_repo_scheme_only() -> None:
    body = _OSIRIS_JS.split("function projectDisplayName(canonical)", 1)[1][:250]
    assert 'canonical.startsWith("repo:")' in body
    assert "canonical.slice(5)" in body


def test_object_display_label_routes_software_project_through_project_display_name() -> None:
    body = _OSIRIS_JS.split("function objectDisplayLabel(o)", 1)[1][:400]
    assert 'o.type === "SoftwareProject"' in body
    assert "projectDisplayName(o.canonical || o.display_label || o.label || o.id" in body
    assert "return o.display_label || o.label" in body


def test_shared_helpers_are_exported() -> None:
    body = _OSIRIS_JS.split("return { $, esc,", 1)[1][:400]
    assert "projectDisplayName, objectDisplayLabel" in body


# --- the dossier headline (osiris.js objectDetail) --------------------------------------

def test_dossier_title_never_falls_back_to_a_bare_repo_canonical() -> None:
    body = _OSIRIS_JS.split("function objectDetail(o, acts = \"\")", 1)[1][:1400]
    assert 'const title = o.name || (o.type === "SoftwareProject" ' \
        "? projectDisplayName(o.canonical) : o.canonical);" in body
    # the canonical still rides along as the secondary muted line, unchanged
    assert '<div class="o-canon">${esc(o.canonical)}</div>' in body


# --- the generic table/board/timeline surfaces (osiris.js) ------------------------------

def test_objects_table_name_column_uses_the_shared_label_canonical_stays_on_hover() -> None:
    body = _OSIRIS_JS.split("function objectsTable(panel, items, onPick, onCtx)", 1)[1][:900]
    assert "cell(objectDisplayLabel(o), o.canonical || o.label)" in body


def test_board_card_title_uses_the_shared_label() -> None:
    body = _OSIRIS_JS.split("function spatialBoardGrid(panel, items, onPick, onCtx)", 1)[1][:2500]
    assert '<div class="card-title">${esc(objectDisplayLabel(o) || summary.slice(0, 85))}</div>' \
        in body


def test_timeline_item_uses_the_shared_label_canonical_stays_on_hover() -> None:
    body = _OSIRIS_JS.split("function timelineList(panel, items, onPick, onCtx)", 1)[1][:1210]
    assert 'title="${esc(o.canonical || o.label || "")}"' in body
    assert "${esc(objectDisplayLabel(o))}${sum}" in body


# --- loadRels' own relationship rows (osiris.js) -----------------------------------------

def test_load_rels_member_rows_use_the_shared_label() -> None:
    body = _OSIRIS_JS.split("async function loadRels(el, id, onPick, onOpenSet, obj)", 1)[1]
    assert "${esc(objectDisplayLabel(m) || m.label)}" in body


def test_load_rels_async_resolve_strips_the_repo_scheme_too() -> None:
    body = _OSIRIS_JS.split("async function loadRels(el, id, onPick, onOpenSet, obj)", 1)[1]
    assert "a.textContent = resolved.name ||" in body
    assert '(resolved.type === "SoftwareProject" ? projectDisplayName(resolved.canonical) ' \
        ": resolved.canonical) ||" in body


# --- the omnibox / palette (console.js) ---------------------------------------------------

def test_omnibox_graph_hits_use_the_shared_label() -> None:
    body = _CONSOLE_JS.split(
        "const graphHits = hits.filter(h => h && h.id).map(h => ({", 1)[1][:600]
    assert "label: Osiris.objectDisplayLabel(h) || h.name || h.canonical || h.id," in body


# --- the space canvas (space.js): pseudo-node labels, hover card, project stubs ---------

def test_project_fill_pseudo_node_name_is_display_stripped_canonical_kept() -> None:
    body = _SPACE_JS.split("const projectAggregates = (snap.project_aggregates || [])", 1)[1][:250]
    assert "name: Osiris.projectDisplayName(snap.projects[a.project]), " \
        "canonical: snap.projects[a.project]," in body


def test_community_pseudo_node_name_is_display_stripped() -> None:
    body = _SPACE_JS.split("const communityAggregates = (snap.communities || [])", 1)[1][:250]
    assert "projectName: Osiris.projectDisplayName(snap.projects[c.project])," in body


def test_project_label_candidate_carries_its_own_canonical_for_hover() -> None:
    body = _SPACE_JS.split("function buildProjectLabelCandidates()", 1)[1][:400]
    assert "__isProjectFill: true, id: `project:${d.name}`, name: d.name, " \
        "canonical: d.canonical," in body


def test_project_label_div_gets_the_canonical_as_its_title() -> None:
    body = _SPACE_JS.split("function pickLabels()", 1)[1][:3600]
    assert 'if (nd.__isProjectFill) div.title = nd.canonical || "";' in body


def test_hover_card_meta_shows_the_display_name_not_the_canonical() -> None:
    body = _SPACE_JS.split("function updateHoverCard(nd)", 1)[1][:400]
    assert 'title="${nd.project ? Osiris.esc(nd.project) : ""}"' in body
    assert '" · " + Osiris.esc(Osiris.projectDisplayName(nd.project))' in body


def test_project_stub_label_shows_the_display_name_canonical_on_hover() -> None:
    body = _SPACE_JS.split("function buildProjectStubDivs()", 1)[1][:400]
    assert "div.textContent = `+${entry.count} in " \
        "${Osiris.projectDisplayName(entry.hiddenProject)}`;" in body
    assert "div.title = entry.hiddenProject;" in body


# --- the /objects/{id}/graph route (src/api/app.py): canonical now rides along ----------

def test_object_graph_nodes_carry_canonical_for_loadrels_client_resolution() -> None:
    app_py = (Path(__file__).parent.parent / "src" / "api" / "app.py").read_text()
    body = app_py.split('agent_state[r["agent_id"]] = state', 1)[1][:400]
    assert '"id": str(r["id"]), "type": r["type"], "canonical": r["canonical"],' in body
