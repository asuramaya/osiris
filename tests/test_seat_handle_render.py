"""THE CANONICAL-RESOLVE DOOR, client half (Thoth mail 12120/12231): the backlog view's
OLDEST_OWNERS/SEAT cells showed raw seat canonicals (seat:478130b0) where every other
console table shows a reader-facing handle. compositions.py's own by_project.oldest_owners
is deliberately byte-identical to digest._obligation_pressure's raw return -- textrender.py's
render_desk_text still joins it as a flat string list -- so the fix lives entirely at the
render layer: table()'s generic cell() detects a canonical-SHAPED string (a lowercase-
starting prefix, a colon, no whitespace) instead of hardcoding a prefix list that drifts
every time a new ObjectType lands, wraps it in a resolvable span showing the canonical as a
placeholder, and renderResult batches every such span in the panel into ONE POST to the new
/objects/resolve-canonicals door (tests/test_api.py's own coverage), patching each span's
text to the resolved handle once it lands -- the same "starts as a bare id, resolves async,
never blocks the rest of the panel" convention loadRels' own property-pair resolution
already established. Mirrors the repo's existing static-source-guard convention: string/
substring proofs against the served JS, no browser harness (live-verified separately via
claude-in-chrome against a real backlog pull: seat:478130b0 -> "henry", title preserved).
"""
from __future__ import annotations

from pathlib import Path

_OSIRIS_JS = (Path(__file__).parent.parent / "src" / "ui" / "static" / "osiris.js").read_text()


def test_canonical_ref_detection_is_shape_based_not_a_hardcoded_prefix_list() -> None:
    assert 'const CANON_REF_RE = /^[a-z][a-z0-9_-]*:\\S+$/;' in _OSIRIS_JS
    body = _OSIRIS_JS.split("const isCanonicalRef = (v) =>", 1)[1][:150]
    assert 'CANON_REF_RE.test(v)' in body
    assert '!/^https?:/i.test(v)' in body  # a url already reads fine, never an object identity


def test_cell_wraps_a_bare_canonical_string_in_a_resolvable_span() -> None:
    body = _OSIRIS_JS.split("const cell = (v) => {", 1)[1][:600]
    assert 'const canonSpan = (c) => `<span class="o-canon-ref" data-canon="${esc(c)}" ' \
        'title="${esc(c)}">${esc(c)}</span>`;' in body
    assert "if (isCanonicalRef(v))" in body
    assert "return canonSpan(v);" in body


def test_cell_resolves_canonicals_inside_a_flat_owner_style_array_too() -> None:
    # oldest_owners mixes real canonicals ("seat:xxxx") with plain literals ("operator") in
    # one array -- each canonical gets its own resolvable span, a plain literal passes
    # through untouched.
    body = _OSIRIS_JS.split("const cell = (v) => {", 1)[1][:1000]
    assert "Array.isArray(v) && !_hasNestedObject(v) && v.some(isCanonicalRef)" in body
    assert "v.map((x) => (isCanonicalRef(x) ? canonSpan(x) : esc(String(x)))).join(\", \");" \
        in body


def test_resolve_canon_refs_batches_every_span_into_one_post() -> None:
    body = _OSIRIS_JS.split("async function resolveCanonRefs(el)", 1)[1][:900]
    assert 'const spans = [...el.querySelectorAll("[data-canon]")];' in body
    assert "if (!spans.length) return;" in body
    assert 'const canonicals = [...new Set(spans.map((s) => s.dataset.canon))];' in body
    assert 'fetch("/objects/resolve-canonicals"' in body
    assert '"content-type": "application/json"' in body


def test_resolve_canon_refs_patches_text_only_on_a_real_hit_leaves_canonical_otherwise() -> None:
    # a canonical the door couldn't resolve (deleted, mistyped) is left showing itself --
    # already the most honest thing to show, never blanked or replaced with an error string.
    body = _OSIRIS_JS.split("async function resolveCanonRefs(el)", 1)[1][:1200]
    assert "if (hit && hit.handle_or_name) s.textContent = hit.handle_or_name;" in body


def test_render_result_resolves_canon_refs_after_both_generic_render_paths() -> None:
    body = _OSIRIS_JS.split("async function renderResult(result, mounts, view, onPick, "
                            "onDrill, onCtx) {", 1)[1]
    rows_body = body.split('if (kind === "rows") {', 1)[1][:150]
    assert "resolveCanonRefs(panel);" in rows_body
    data_body = body.split("panel.innerHTML = renderData(items);", 1)[1][:100]
    assert "resolveCanonRefs(panel);" in data_body
