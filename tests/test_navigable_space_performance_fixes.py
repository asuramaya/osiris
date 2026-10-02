"""NAVIGABLE SPACE, THE FIX (live measurement on a deployed build): the
deployed space was reported as "super fried" -- slowdowns, crashes, zoom broken, wheel
cooked. Four root causes were measured directly (Chrome,
Intel Iris Xe, commit eac257d) and this closes all four, plus two bugs of the same stale-
hardcoded-constant class caught while fixing them. Mirrors the existing static-source-guard
convention: no browser test harness exists in this repo, so these are string-presence
proofs against the served JS -- correctness of the actual math (cursor-anchored zoom,
clamp bounds, zero instance-buffer writes under a wheel burst) was live-verified via
claude-in-chrome and is reported on the thread, not re-proven here.
"""
from __future__ import annotations

from pathlib import Path

_SPACE_JS = (Path(__file__).parent.parent / "src" / "ui" / "static" / "space.js").read_text()
_CONSOLE_JS = (Path(__file__).parent.parent / "src" / "ui" / "static" / "console.js").read_text()


# --- (1) the wheel clamp was [8, 2000] against a layout whose fitted view can run 259,779
# world units wide -- one tick snapped a 90x jump into a single dense cluster ----------------

def test_zoom_bounds_are_derived_from_the_real_fitted_bbox_not_hardcoded() -> None:
    assert "let minViewSize = 20, maxViewSize = 2000;" in _SPACE_JS  # only the INITIAL default
    body = _SPACE_JS.split("function fitToNodes(list)", 1)[1].split("\n  }\n", 1)[0]
    assert "minViewSize = 20;" in body
    assert "maxViewSize = Math.max(span * 2, 200);" in body


def test_the_old_hardcoded_wheel_clamp_is_gone() -> None:
    assert "Math.max(8, Math.min(2000" not in _SPACE_JS


def test_focus_zoom_to_fit_no_longer_clips_to_a_stale_1300_ceiling() -> None:
    # the same stale-constant bug class as in the wheel clamp -- a wide-spread
    # upstream chain was being clipped back down to a fixed small view.
    # THE READING LAYER, part B later gave focusObject a second `opts`
    # parameter (depth/skipStackPush) -- the signature changed, the ceiling fix didn't.
    body = _SPACE_JS.split("function renderFocusEgoGroups(id, hopsUp, hopsDown)", 1)[1][:2000]
    assert "Math.min(maxViewSize, span * 1.6 + 40)" in body
    assert "Math.min(1300" not in body


# --- zoom is cursor-anchored and coalesced to one update per animation frame ---------------

def test_zoom_is_anchored_at_the_cursor() -> None:
    assert "function zoomAt(clientX, clientY, deltaY)" in _SPACE_JS
    assert "THREE.MathUtils.lerp(camera.left, camera.right, nx)" in _SPACE_JS
    assert "THREE.MathUtils.lerp(camera.top, camera.bottom, ny)" in _SPACE_JS


def test_wheel_events_coalesce_to_one_rendered_zoom_per_frame() -> None:
    assert "let pendingWheelDelta = 0" in _SPACE_JS
    assert "pendingWheelDelta += ev.deltaY;" in _SPACE_JS
    assert "requestAnimationFrame(applyPendingWheel)" in _SPACE_JS
    # the listener itself must never call zoomAt/rescaleForZoom synchronously -- only the
    # coalesced callback may, or every event in a burst pays full cost again.
    listener_body = _SPACE_JS.split('"wheel",', 1)[1].split("{ passive: false }", 1)[0]
    assert "zoomAt(" not in listener_body


# --- (2) rescaleForZoom used to rewrite 49,019 instance matrices on BOTH meshes (getMatrixAt/
# decompose/setMatrixAt), measured at 18.6ms JS for one wheel tick -- moved to a GPU uniform +
# a static per-instance attribute so a zoom step touches two floats, never a buffer -----------

def test_node_size_is_a_shader_uniform_not_a_per_instance_matrix_rewrite() -> None:
    # THE LEGIBILITY PASS, TIP 1(a) moved sizing off a fixed-screen-pixel scheme to a
    # WORLD-unit radius, clamped by a uniform min/max. THE LAST RENDERER moved it BACK to a
    # screen-pixel scheme (per-object degree curve now, not the old flat constant) -- still a shader
    # uniform, still never a per-instance matrix rewrite: aRadiusPx (a per-instance PIXEL
    # target, fixed at build time) times a single live uWorldPerPx uniform, no clamp/cap at all.
    assert "function makeInstancedCircleMaterial(opts)" in _SPACE_JS
    assert "onBeforeCompile" in _SPACE_JS
    assert "attribute float aRadiusPx;" in _SPACE_JS
    assert "uniform float uWorldPerPx;" in _SPACE_JS
    assert "transformed *= aRadiusPx * uWorldPerPx * aVisible;" in _SPACE_JS


def test_rescale_for_zoom_is_o1_uniform_writes_no_matrix_loop() -> None:
    body = _SPACE_JS.split("function rescaleForZoom()", 1)[1].split("\n  }\n", 1)[0]
    assert "meshUniforms.uWorldPerPx.value = wpp" in body
    assert "pickUniforms.uWorldPerPx.value = wpp" in body
    assert "getMatrixAt" not in body
    assert "setMatrixAt" not in body
    assert "for (" not in body  # no per-instance loop at all


def test_build_scene_sets_instance_scale_to_one_not_a_baked_pixel_size() -> None:
    body = _SPACE_JS.split("function buildScene(nodes, edges)", 1)[1][:3000]
    assert "dummy.scale.setScalar(1);" in body
    assert "radiusAttr.setX(i, nd.radiusPx);" in body


# --- (3) the render loop ran forever (rAF + a 50ms setTimeout fallback), even off-surface or
# hidden -- render-on-demand, no fallback timer, pixel ratio capped, antialias conditional ---

def test_render_loop_is_on_demand_not_unconditional() -> None:
    assert "function markDirty()" in _SPACE_JS
    assert "function renderIfDirty()" in _SPACE_JS
    assert "if (!running || !dirty) return;" in _SPACE_JS


def test_the_old_fifty_ms_fallback_timer_is_gone() -> None:
    assert "setTimeout(() => { if (!done)" not in _SPACE_JS
    assert ", 50);" not in _SPACE_JS


def test_pause_and_resume_exist_and_gate_on_document_visibility() -> None:
    assert "function pause() { running = false; }" in _SPACE_JS
    assert 'document.addEventListener("visibilitychange"' in _SPACE_JS
    assert "if (document.hidden) pause(); else resume();" in _SPACE_JS


def test_console_pauses_and_resumes_the_canvas_by_active_surface() -> None:
    assert "window.OsirisSpace.resume();" in _CONSOLE_JS
    assert "window.OsirisSpace.pause();" in _CONSOLE_JS


def test_pixel_ratio_capped_and_antialias_conditional_on_dpr() -> None:
    assert "Math.min(window.devicePixelRatio || 1, 1.5)" in _SPACE_JS
    assert "new THREE.WebGLRenderer({ antialias: dpr <= 1 })" in _SPACE_JS


# --- (4) a WebGL context loss (a real GPU reset) either refused the first load outright or
# vanished a tab later -- preventDefault + rebuild on restore instead of dying -------------

def test_context_loss_is_handled_not_left_to_crash() -> None:
    assert '"webglcontextlost"' in _SPACE_JS
    assert "ev.preventDefault();" in _SPACE_JS
    assert '"webglcontextrestored"' in _SPACE_JS
    # THE LAST RENDERER retired the tier-label/cluster/halo GPU
    # resources this handler used to rebuild alongside the main scene -- context restore
    # is back to its simpler pre-LOD shape.
    body = _SPACE_JS.split('"webglcontextrestored"', 1)[1][:400]
    assert "buildScene(idToNode, edges)" in body
    assert "fitToNodes(idToNode)" in body
    assert "resume();" in body
