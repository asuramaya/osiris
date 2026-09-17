// NAVIGABLE SPACE, THE RENDERER — piece 2, THE VIEW (thread 71c4ca0d, Thoth DM 10436,
// operator rulings f832c3a4/0a3d6719), now INTEGRATED (decision "NAVIGABLE SPACE,
// INTEGRATION SHAPE", mail 10550): mounted inside /ui/'s own browse stage in place of the
// old #cy cytoscape container, not a separate page. The operator's own repeated corrections
// settled the interaction shape, twice: (1) zoom is LOOKING only, never a data-tier switch —
// navigation is a CLICK; (2) a click doesn't change what's loaded either — "more like click
// to highlight" — every positioned object is drawn AT ONCE (matching 0a3d6719's own original
// wording, "an engine that can handle all objects at once"), and a click only lights the
// clicked object's neighborhood, dims everything else, and opens the inspector. There is no
// tier concept in this file.
//
// Labels: the operator caught a real lag bug — DOM label positions were only recomputed on
// a debounce, so they visibly fell behind the WebGL scene during a drag. Fixed by splitting
// "which nodes are labeled" (nearest-N, genuinely expensive, stays debounced) from "where do
// the ALREADY-CHOSEN labels sit on screen" (cheap — one Vector3.project() per label, no
// resort), which now runs every render frame, not just after panning/zooming settles.
//
// Data source: Khnum's GET /graph/stream (thread b6cb1d7c0b36, wire format frozen by DM
// 10439/10449/10451) — one binary snapshot, decoded client-side (decodeSnapshot below),
// no more client-side tiling/pagination. GET /graph/stream/deltas SSE-polls the outbox for
// incremental moves/retirements after the initial snapshot lands (op:'moved'|'retired').
// Positions and collision avoidance (rings) are entirely Khnum's layout heartbeat's own —
// this module reads x/y as given and never relaxes them client-side.
//
// KNOWN GAP (flagged to Khnum, DM 10554/10555, not blocking): the wire header's `types`/
// `projects` arrays resolve node type_code/project_code, but edge_type_code has no matching
// name array yet — edge color-coding below hashes the raw int until that lands, then swaps
// to real relationship names with no shape change on this side.
//
// Mounts via initSpace(container) rather than running as a page-load IIFE, so console.js
// (the live /ui/ shell) can own the container lifecycle; space.html keeps working as a
// standalone dev harness by calling initSpace() against its own fixed ids.

import * as THREE from "./vendor/three.module.js";

// colour-code an edge by its relationship type — a stable hash-to-hue, since no link-type
// palette exists server-side yet (only /schema's own object_types carry colours). Falls back
// to hashing the raw edge_type_code int until Khnum's edge_types name array lands.
const _edgeColorCache = new Map();
function colorForEdgeType(type) {
  const key = String(type);
  if (_edgeColorCache.has(key)) return _edgeColorCache.get(key);
  let h = 0;
  for (let i = 0; i < key.length; i++) h = (h * 31 + key.charCodeAt(i)) >>> 0;
  const hue = h % 360;
  const c = `hsl(${hue}, 55%, 55%)`;
  _edgeColorCache.set(key, c);
  return c;
}

// THE READING LAYER, part A: EDGE CLASSES (ruling c5953bb1, Thoth DM 10596). Structural
// edges are pure containment/membership (an object belongs to a repo, an agent operates in
// a project, a seat holds a mind) — real, but not what a reader is tracing when they ask
// "how did we get here"; their degree dwarfs everything else (repo:osiris alone: 20,352).
// Semantic edges are the actual provenance/evidence trail (possible_upstream, cites,
// derived_from, spawned_by, succeeded_from, supersedes, resolves, grounded_by, and the
// rest) — what "focus really focusing" (the operator's own words) needs to walk and show.
//
// DEFAULT, picked and noted here per Thoth's own instruction not to park on visual choices
// (thread 71c4ca0d carries this note too): every link type in src/ontology/schema.py whose
// own docstring reads as "X belongs to / operates in / is a member or officer of Y" is
// structural; everything else defaults to semantic (the safer default — an edge that's
// actually structural but misclassified just draws a bit more clutter; one that's actually
// meaningful but misclassified as structural would go invisible, the worse failure).
// THE WIRE EDGE CLASSES FIX (Thoth mail 11291): this table is a FALLBACK now, used only
// for a type the header's own `link_type_class` (fetchStreamSnapshot's own edgeClassByType)
// doesn't carry a value for -- not the primary source any more. A stale earlier version of
// this fallback logic read a field (`edge_classes`) the wire never actually sent, so it ran
// unconditionally; kept here in case a future snapshot genuinely omits the header key.
const STRUCTURAL_EDGE_TYPES = new Set([
  "in_repo", "works_in", "governs", "holds", "acts_for", "member_of", "employs",
  "worktree_of", "succeeds_seat", "owns", "owned_by", "subsidiary_of", "ultimate_parent",
  "sent_by", "addressed_to", "broadcast_to", "in_thread",
]);
function classOfEdgeType(type) {
  return STRUCTURAL_EDGE_TYPES.has(type) ? "structural" : "semantic";
}
// WAVE 27, THE LENS PANEL: "container" is its own distinct edgeClass now (see the wire
// classification comment above, in fetchStreamSnapshot), but every pre-existing check that
// used to rely on container reading as "structural" (membership-degree detection: a real
// container-focus walk, the drill's own focus-type gate) still needs to treat the two
// alike -- this is the one place that equivalence lives now, instead of re-normalizing at
// ingest.
function isStructuralLike(edgeClass) {
  return edgeClass === "structural" || edgeClass === "container";
}

// THE READING LAYER, part B (ruling c5953bb1): the curated provenance/evidence edge-type
// allowlist a real FOCUS walks — the actual "long paths leading back and upstream" the
// operator asked to see, as opposed to part A's structural containment edges, which never
// widen a path. Pure, DOM-free module-level functions (not closures inside initSpace) so
// the acceptance test Thoth's own dispatch named — "a synthetic 5-hop chain where focus at
// the tail lights exactly the chain and nothing else" — can exercise the real algorithm
// directly via Node, not a string-presence proof.
// THE LEGIBILITY PASS, TIP 1 AMENDMENT (operator via Thoth mail 10726, ruling amending
// e1cb9e3b): "the lens is the TREE TO SOURCE" -- grounded_by, decided_in, answers added to
// the walk so a decision's own grounding trail is reachable, not just its narrower
// derivation chain.
export const PATH_EDGE_TYPES = new Set([
  "possible_upstream", "cites", "derived_from", "spawned_by",
  "succeeded_from", "supersedes", "resolves", "grounded_by", "decided_in", "answers",
]);
export function buildPathAdjacency(edges) {
  const outAdj = new Map(), inAdj = new Map(); // node id -> [neighbor ids]
  for (const e of edges) {
    if (!PATH_EDGE_TYPES.has(e.type)) continue;
    if (!outAdj.has(e.source)) outAdj.set(e.source, []);
    outAdj.get(e.source).push(e.target);
    if (!inAdj.has(e.target)) inAdj.set(e.target, []);
    inAdj.get(e.target).push(e.source);
  }
  return { outAdj, inAdj };
}
// bidirectional BFS, depth-limited (a widen control raises depth interactively rather than
// a hardcoded ceiling) — Osiris convention: from_id = the dependent/newer fact, to_id =
// what it points at, so "upstream" follows outAdj (X.source -> target) and "downstream...
// over the same reversed" follows inAdj.
export function walkPath(outAdj, inAdj, startId, depth) {
  const seen = new Set([startId]);
  let frontier = [startId];
  for (let d = 0; d < depth && frontier.length; d++) {
    const next = [];
    for (const cur of frontier) {
      for (const t of outAdj.get(cur) || []) { if (!seen.has(t)) { seen.add(t); next.push(t); } }
      for (const t of inAdj.get(cur) || []) { if (!seen.has(t)) { seen.add(t); next.push(t); } }
    }
    frontier = next;
  }
  return seen;
}

// ---- GET /graph/stream wire decode (a JS twin of graph_stream.decode_snapshot) ---------
// 4-byte LE uint32 header length, that many bytes of UTF-8 JSON header, then the raw arrays
// back to back at the byte offsets the header's own `arrays` map names.
const _DTYPE_CTOR = { f: Float32Array, H: Uint16Array, B: Uint8Array, I: Uint32Array };
function decodeSnapshot(buf) {
  const dv = new DataView(buf);
  const headerLen = dv.getUint32(0, true);
  const headerBytes = new Uint8Array(buf, 4, headerLen);
  const header = JSON.parse(new TextDecoder().decode(headerBytes));
  const bodyStart = 4 + headerLen;
  const out = { ...header, arrays: undefined };
  for (const [name, meta] of Object.entries(header.arrays)) {
    const Ctor = _DTYPE_CTOR[meta.dtype];
    // typed-array views need an offset that's a multiple of their own element size — the
    // wire format packs arrays back to back with no padding, so a Float32/Uint32 view at a
    // non-4-aligned offset throws; slice+copy is the safe general case (arrays here are a
    // few hundred KB at most, not worth hand-padding the server's own byte layout for).
    const byteOff = bodyStart + meta.offset;
    const bytes = buf.slice(byteOff, byteOff + meta.length * Ctor.BYTES_PER_ELEMENT);
    out[name] = new Ctor(bytes);
  }
  return out;
}

async function fetchStreamSnapshot() {
  const buf = await fetch("/graph/stream").then((r) => r.arrayBuffer());
  const snap = decodeSnapshot(buf);
  const nodes = [];
  for (let i = 0; i < snap.count; i++) {
    nodes.push({
      id: snap.object_ids[i],
      type: snap.types[snap.type_code[i]],
      project: snap.projects[snap.project_code[i]],
      x: snap.x[i], y: snap.y[i],
      degree: snap.weight[i],
      statusFlag: snap.status_flag[i],
      // WAVE 26, LINEAGES ARE TIME (thread 3683a12a): epoch seconds, float32 on the
      // wire (see graph_stream.py's own docstring item 11) -- fine for a timeline
      // spanning weeks/months/years, not sub-minute precision.
      createdAt: snap.created_at ? snap.created_at[i] : 0,
      // WAVE 26, COMMUNITY REGIONS (mail 11592/11664): index-aligned, 0 = no real
      // community (a small project, or a Leiden cluster too small to be a real
      // district-refinement) -- Khnum's own graph_physics._detect_communities,
      // unchanged, the same partition the compact-arrangement layout is built on.
      communityCode: snap.community_code ? snap.community_code[i] : 0,
      // TIP 1b (Thoth mail 10755): "swap the client label fallback for the header
      // labels" -- Khnum's own labels array (index-aligned to object_ids, tip 2g) is
      // the label now, computed server-side with the exact same per-type rule and
      // 40-char truncation THE LEGIBILITY PASS specified. Falls back to a client-built
      // string only if an older snapshot lacks the field.
      label: snap.labels ? snap.labels[i] : undefined,
    });
  }
  // THE WIRE EDGE CLASSES FIX (Thoth mail 11291): the client's own STRUCTURAL_EDGE_TYPES
  // table was never anything but a fallback, but it ran 100% of the time -- the code read
  // `snap.edge_classes`, a field the wire never actually sends. The real field is
  // `link_type_class`, index-aligned to `edge_types` the same way (values semantic/
  // structural/container), which is exactly why the browser marked authored_by/spawned_by
  // "semantic" and drew them at rest (6,413 authored_by edges over 20k units read as the
  // yellow beam) while the header's own link_type_class says authored_by is structural.
  // "container" (membership/containment, distinct from ordinary structural) used to
  // normalize to "structural" here -- every existing check in this file only ever
  // distinguished "structural" from everything else, so collapsing it avoided special-
  // casing every call site. WAVE 27, THE LENS PANEL (Thoth mail 11754) asks for container
  // as its OWN lens toggle, alongside semantic/structural -- kept distinct now; every call
  // site that relied on container reading as structural (isContainerFocus,
  // containerMembersByType, the pathReachable one-hop widen fallback) goes through
  // isStructuralLike() instead, below, so their own behavior is unchanged. Built once from
  // the type vocabulary itself (edge_types), not per-edge, so a type with zero edges in
  // THIS snapshot still has a real effective class to report on the debug API.
  const edgeClassByType = {};
  if (snap.edge_types) {
    for (let i = 0; i < snap.edge_types.length; i++) {
      const t = snap.edge_types[i];
      const cls = snap.link_type_class && snap.link_type_class[i];
      edgeClassByType[t] = cls || classOfEdgeType(t); // no header value -- the client fallback
    }
  }
  const edges = [];
  for (let i = 0; i < snap.edge_count; i++) {
    const type = (snap.edge_types && snap.edge_types[snap.edge_type_code[i]]) ?? snap.edge_type_code[i];
    const edgeClass = edgeClassByType[type] || classOfEdgeType(type);
    edges.push({
      source: snap.object_ids[snap.edge_src[i]],
      target: snap.object_ids[snap.edge_dst[i]],
      type, edgeClass,
    });
  }
  // THE LAST RENDERER (operator ruling d7d55257, Thoth mail 11066) killed LOD entirely --
  // Khnum's own project_aggregates/type_aggregates/cluster_edges/type_pair_edges (tip
  // 2i/2j/h) still ride the same wire header; type_aggregates/cluster_edges/type_pair_edges
  // remain unread client-side, but THE DRAWING TIP (mail 11408) reuses project_aggregates
  // as exactly the district geometry it needs (a real centroid + exact member-distance-
  // bound radius per project, already computed server-side from the same positions) --
  // resolve its numeric project code back to a name off the same `projects` table
  // node.project already reads, rather than re-deriving anything.
  const districtAggregates = (snap.project_aggregates || []).map((a) => ({
    name: snap.projects[a.project], count: a.count, cx: a.cx, cy: a.cy, radius: a.radius,
  }));
  // WAVE 26, COMMUNITY REGIONS (mail 11592): `communities` is Khnum's own header table,
  // the SAME shape as project_aggregates/type_aggregates -- one row per real (non-zero)
  // community code, project resolved back to a name off the same `projects` table
  // districts already use, so a community's own district membership is a plain string
  // comparison, never a second id space to reconcile.
  const communityAggregates = (snap.communities || []).map((c) => ({
    code: c.community, districtName: snap.projects[c.project],
    count: c.count, cx: c.cx, cy: c.cy, radius: c.radius,
  }));
  return { nodes, edges, edgeClassByType, districtAggregates, communityAggregates };
}

// resolves DOM refs from a passed-in container map, falling back to the same fixed ids
// space.html's own standalone page has always used — lets console.js mount this against
// its own #cy-replacement markup while space.html keeps working unchanged.
function resolveContainer(container) {
  const byId = (id) => document.getElementById(id);
  return {
    wrap: (container && container.wrap) || byId("canvas-wrap"),
    labelsEl: (container && container.labels) || byId("labels"),
    statusEl: (container && container.status) || byId("status-line"),
    levelBadge: (container && container.levelBadge) || byId("graph-level-badge"),
    rightRail: (container && container.rightRail) || byId("right"),
    fitBtn: (container && container.fitBtn) || byId("fit-btn"),
    upBtn: (container && container.upBtn) || byId("up-btn"),
    legendBtn: (container && container.legendBtn) || byId("legend-btn"),
    legendPanel: (container && container.legendPanel) || byId("legend-panel"),
    backBtn: (container && container.backBtn) || byId("back-btn"),
    // TIP 1 AMENDMENT: "Widen" is retired -- depth is unlimited by default now ("until
    // roots"), so raising a capped depth is moot. The same button/id is repurposed as the
    // downstream toggle ("downstream is a toggle, off by default").
    downstreamBtn: (container && container.downstreamBtn) || byId("downstream-btn"),
    onFocus: (container && container.onFocus) || null, // (id) => void, shares selection with the table
  };
}

export async function initSpace(container) {
  const { wrap, labelsEl, statusEl, levelBadge, rightRail, fitBtn, upBtn,
    legendBtn, legendPanel, backBtn, downstreamBtn, onFocus } =
    resolveContainer(container);
  function setStatus(text) { statusEl.textContent = text; }

  const typeColors = await loadTypeColors();

  // ---- renderer / scene / camera -----------------------------------------------------
  // pixel ratio capped at 1.5 and antialias only below/at native DPR (Thoth's own live
  // measurement on an Iris Xe box, mail 10581): AA is a real GPU cost that scales with
  // resolution, and stacking it on top of an already-high device pixel ratio was part of
  // what made a real laptop GPU choke on this scene.
  const dpr = Math.min(window.devicePixelRatio || 1, 1.5);
  const renderer = new THREE.WebGLRenderer({ antialias: dpr <= 1 });
  // three.js's ColorManagement converts every hex colour (THREE.Color.set('#8ab4f8')) from
  // sRGB into LINEAR space internally — without this, the renderer displays those linear
  // values as-is, reading systematically darker than the real colour.
  renderer.outputColorSpace = THREE.SRGBColorSpace;
  renderer.setPixelRatio(dpr);
  renderer.setSize(wrap.clientWidth, wrap.clientHeight);
  wrap.appendChild(renderer.domElement);

  const scene = new THREE.Scene();
  scene.background = new THREE.Color(0x0d1219);
  const pickScene = new THREE.Scene();

  // THE LAST RENDERER (operator ruling d7d55257, Thoth mail 11066): "a per-pixel saturation
  // cap (tone-map the additive pass...) so overlap reads as brightness and never as white."
  // Additive blending alone can sum well past 1.0 per channel and hard-clip to flat white
  // the moment enough points/edges overlap the same pixel -- a genuine HDR render target
  // (HalfFloatType, values free to exceed 1.0) plus a Reinhard tone-map full-screen pass
  // (color / (color + 1), mathematically bounded in [0, 1) for any non-negative input, no
  // matter how many instances overlap) makes "never white" a property of the math, not a
  // heuristic. Falls back to rendering straight to the canvas if the render target can't be
  // created (an old GPU lacking float render-target support) -- the additive brightness cap
  // this buys is a real improvement, never a hard requirement to render at all.
  let sceneTarget = null, toneMapScene = null, toneMapCamera = null;
  try {
    sceneTarget = new THREE.WebGLRenderTarget(1, 1, {
      type: THREE.HalfFloatType, depthBuffer: true, stencilBuffer: false,
    });
    toneMapCamera = new THREE.OrthographicCamera(-1, 1, 1, -1, 0, 1);
    toneMapScene = new THREE.Scene();
    const toneMapMaterial = new THREE.ShaderMaterial({
      uniforms: { tDiffuse: { value: sceneTarget.texture } },
      depthTest: false, depthWrite: false,
      vertexShader: `
        varying vec2 vUv;
        void main() { vUv = uv; gl_Position = vec4(position, 1.0); }
      `,
      fragmentShader: `
        uniform sampler2D tDiffuse;
        varying vec2 vUv;
        void main() {
          vec3 color = texture2D(tDiffuse, vUv).rgb;
          vec3 mapped = color / (color + vec3(1.0)); // Reinhard: bounded in [0,1) always
          gl_FragColor = vec4(mapped, 1.0);
        }
      `,
    });
    toneMapScene.add(new THREE.Mesh(new THREE.PlaneGeometry(2, 2), toneMapMaterial));
  } catch (err) {
    console.error("tone-map render target unavailable, rendering without a saturation cap", err);
    sceneTarget = null;
  }
  function resizeSceneTarget() {
    if (!sceneTarget) return;
    sceneTarget.setSize(
      Math.max(1, Math.round(wrap.clientWidth * dpr)),
      Math.max(1, Math.round(wrap.clientHeight * dpr)));
  }

  // world extent depends entirely on Khnum's own layout heartbeat (deterministic hash
  // placement, piece A) and is NOT a fixed constant — a project-center hash can land
  // anywhere; fitToNodes() (below) frames the camera from the real loaded bbox instead of
  // a guessed number the moment the first snapshot lands, and Fit re-measures live rather
  // than resetting to a stale guess. minViewSize/maxViewSize (Thoth's own live-verified fix,
  // mail 10581) are likewise derived from the real fitted bbox, not the old hardcoded
  // [8, 2000] clamp — that clamp predated the deterministic layout and let one wheel tick
  // snap a 259,779-unit-wide view down to 2,862 (a 90x jump into a single dense cluster,
  // read by the operator as "zoom does not work").
  let viewSize = 1300;
  let minViewSize = 20, maxViewSize = 2000;
  const camera = new THREE.OrthographicCamera(-1, 1, 1, -1, 0.1, 10);
  camera.position.set(0, 0, 5);
  camera.lookAt(0, 0, 0);
  function updateFrustum() {
    const a = wrap.clientWidth / wrap.clientHeight;
    camera.left = (-viewSize * a) / 2;
    camera.right = (viewSize * a) / 2;
    camera.top = viewSize / 2;
    camera.bottom = -viewSize / 2;
    camera.updateProjectionMatrix();
  }
  updateFrustum();
  resizeSceneTarget();
  window.addEventListener("resize", () => {
    renderer.setSize(wrap.clientWidth, wrap.clientHeight);
    updateFrustum();
    resizeSceneTarget();
    edgeFadeUniforms.uViewportPx.value.set(wrap.clientWidth, wrap.clientHeight);
    markDirty();
  });

  // ---- render ON DEMAND (Thoth's own live measurement, mail 10581): the render loop used
  // to run every frame forever (rAF plus a 50ms setTimeout fallback), even when browse
  // isn't the active surface or the tab is hidden — pure waste, and on top of the
  // per-wheel-event instance-buffer rewrite this fix removes below, it compounded into the
  // "super fried" report. Now a frame only renders when something actually changed
  // (camera move, data, focus, label pick); the loop stops scheduling itself entirely once
  // idle rather than polling at 20fps forever.
  let dirty = true, running = true, rafPending = false;
  function markDirty() {
    dirty = true;
    if (!running || rafPending) return;
    rafPending = true;
    requestAnimationFrame(renderIfDirty);
  }
  // shared by the render-on-demand loop and the api's own forceRender debug hook -- one
  // place decides whether the tone-map pass runs, never duplicated.
  function renderScene() {
    if (sceneTarget) {
      if (window.__spaceWheelTiming) {
        const t0 = performance.now();
        renderer.setRenderTarget(sceneTarget);
        renderer.render(scene, camera);
        const t1 = performance.now();
        renderer.setRenderTarget(null);
        renderer.render(toneMapScene, toneMapCamera);
        console.debug("[wheel] renderScene: HDR pass", (t1 - t0).toFixed(2), "ms; tone-map pass", (performance.now() - t1).toFixed(2), "ms");
      } else {
        renderer.setRenderTarget(sceneTarget);
        renderer.render(scene, camera);
        renderer.setRenderTarget(null);
        renderer.render(toneMapScene, toneMapCamera);
      }
    } else {
      renderer.render(scene, camera);
    }
  }
  function renderIfDirty() {
    rafPending = false;
    if (!running || !dirty) return;
    // WHEEL HANG INSTRUMENTATION (Thoth mail 11248): temporary, gated behind
    // window.__spaceWheelTiming -- per-stage performance.now() around the two costs a
    // wheel tick actually pays for (this render call, and positionLabels' own tail of
    // drill/anchor/stub positioning), logged so a single real tick's own cost is visible
    // stage-by-stage rather than guessed at.
    if (window.__spaceWheelTiming) {
      const t0 = performance.now();
      renderScene();
      const t1 = performance.now();
      positionLabels();
      const t2 = performance.now();
      console.debug("[wheel] renderScene", (t1 - t0).toFixed(2), "ms; positionLabels", (t2 - t1).toFixed(2), "ms");
    } else {
      renderScene();
      positionLabels();
    }
    dirty = false;
  }
  function pause() { running = false; }
  function resume() { running = true; markDirty(); }
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) pause(); else resume();
  });

  // WebGL context loss (Thoth's own live report: the first load in her tab was refused
  // outright, "Web page caused context loss and was blocked", and a later tab vanished —
  // a GPU reset Chrome then blocks the page from reusing). preventDefault on loss keeps the
  // browser from tearing the canvas down permanently; rebuild GPU resources on restore
  // instead of leaving a dead black canvas or crashing the tab.
  renderer.domElement.addEventListener("webglcontextlost", (ev) => {
    ev.preventDefault();
    pause();
    setStatus("WebGL context lost — recovering…");
  }, false);
  renderer.domElement.addEventListener("webglcontextrestored", () => {
    setStatus("WebGL context restored — rebuilding…");
    if (idToNode.length) {
      buildScene(idToNode, edges);
      fitToNodes(idToNode);
      setStatus(`${idToNode.length} objects, ${edges.length} edges (recovered)`);
    }
    resume();
  }, false);

  let mesh = null, pickMesh = null, edgeLines = null, ribbonLines = null;
  let meshUniforms = null, pickUniforms = null;
  let visibleAttr = null;
  let idToNode = [];
  // one id->node index, rebuilt only when the node set itself changes (buildScene) --
  // TIP 1 AMENDMENT's own 100ms budget (mail 10726 item 2) made this the fix, not a
  // premature one: rebuilding a 49k-entry Map costs ~15ms each, and focusObject used to
  // build FOUR of them (ego layout, restore, path edges, camera fit) on every single click.
  let idById = new Map();
  // TIP 1(e): header taxonomy-pill type filters hide instances through the same per-instance
  // aVisible flag focus uses (1(d)) — empty means nothing filtered, everything shown.
  let hiddenNodeTypes = new Set();
  // WAVE 27, THE LENS PANEL (Thoth mail 11754): two more reader-opt-OUT toggles alongside
  // the legend's existing node-type/edge-class/edge-type checkboxes — default false (shown),
  // same "a reader's lens, never a default hide" convention. Declared here, not down by
  // their own build functions, for the same TDZ-safety reason as every other early-block
  // state this file already collects (syncCommunityVisibility/buildLandmarkBadges run
  // during initSpace's own synchronous setup, before a `let` declared near those functions'
  // own definitions would have executed yet).
  let communitiesHiddenByLens = false;
  let highDegreeBadgesHiddenByLens = false;
  // WAVE 27, THE LENS PANEL: "state on the URL hash so a view is shareable" -- one JSON
  // blob under its own hash param (never the whole hash, so other hash consumers keep their
  // own room), sorted arrays so two sessions with the same lens produce the SAME hash text,
  // not just an equivalent one. Read once at load (applyLensStateFromHash, before the first
  // buildScene/renderLegend), written after every toggle (renderLegend's own last line) --
  // never a live hashchange listener, since a shared link is opened fresh, not edited by
  // hand mid-session.
  const LENS_HASH_PARAM = "lens";
  function readLensStateFromHash() {
    const params = new URLSearchParams(location.hash.replace(/^#/, ""));
    const raw = params.get(LENS_HASH_PARAM);
    if (!raw) return null;
    try {
      return JSON.parse(raw);
    } catch {
      return null;
    }
  }
  function writeLensStateToHash() {
    const state = {
      hiddenNodeTypes: [...hiddenNodeTypes].sort(),
      hiddenEdgeClasses: [...hiddenEdgeClasses].sort(),
      hiddenEdgeTypes: [...hiddenEdgeTypes].sort(),
      hideCommunities: communitiesHiddenByLens,
      hideHighDegree: highDegreeBadgesHiddenByLens,
    };
    const params = new URLSearchParams(location.hash.replace(/^#/, ""));
    params.set(LENS_HASH_PARAM, JSON.stringify(state));
    history.replaceState(null, "", `#${params.toString()}`);
  }
  function applyLensStateFromHash() {
    const state = readLensStateFromHash();
    if (!state) return;
    hiddenNodeTypes.clear();
    for (const t of state.hiddenNodeTypes || []) hiddenNodeTypes.add(t);
    hiddenEdgeClasses.clear();
    for (const c of state.hiddenEdgeClasses || []) hiddenEdgeClasses.add(c);
    hiddenEdgeTypes.clear();
    for (const t of state.hiddenEdgeTypes || []) hiddenEdgeTypes.add(t);
    communitiesHiddenByLens = !!state.hideCommunities;
    highDegreeBadgesHiddenByLens = !!state.hideHighDegree;
  }
  // CONSOLE CHROME CLEANUP piece 2 (decision 31717ca7, thread 0be2f790's own operator-
  // finding follow-up): the header's repo selector drives the SAME aVisible flag through
  // this sibling set — nd.project (already carried on every node since the snapshot's own
  // project_code lookup, line ~148) is the field it filters on, empty means nothing
  // filtered, everything shown.
  let hiddenProjects = new Set();
  // THE DRILL, item 5 (Thoth mail 11048): ids a project-filter stub click revealed --
  // "without unhiding the project" itself, so this stays a NARROW override, never merged
  // into hiddenProjects. Reset whenever the filter itself changes (setHiddenProjects).
  let revealedStubIds = new Set();
  // THE READING LAYER, part B: FOCUS = PATH LENS (ruling c5953bb1, Thoth DM 10596). SELECT
  // (a plain click) and FOCUS (double-click, Enter, or the inspector's Focus button) are now
  // two different acts — selectedId just shows the inspector; pathFocusId/pathReachable are
  // the real path-lens state (only non-empty while an actual focus is active).
  let selectedId = null;
  let pathFocusId = null;
  let pathReachable = new Set();
  let focusStack = []; // ids, most recent last — back() pops, Escape/Clear focus wipes the overlay
  // THE DRILL: cross-project anchor click targets. Declared here (not next to
  // buildProjectObjectIndex/buildProjectAnchors further down) because buildScene calls
  // buildProjectObjectIndex() on every load -- a `let` declared below buildScene's own
  // call site is still in its temporal dead zone at that point, a real crash-on-every-load
  // bug THE LAST RENDERER's live verification caught (ReferenceError: Cannot access
  // 'projectObjectByName' before initialization).
  let projectObjectByName = new Map(); // "repo:foo" -> that SoftwareProject object's own id
  // shared world->screen scratch vector for every per-frame div-positioning pass (labels,
  // drill entries, project anchors, project stubs) -- same TDZ reasoning as
  // projectObjectByName above: positionDrillDivs/positionProjectAnchors/
  // positionProjectStubs are all reachable during a container focus called well before a
  // declaration placed down near positionLabels would have run. One shared instance is
  // also just correct: it's pure per-call scratch, never carries state between calls.
  const _screenV = new THREE.Vector3();

  function disposeCurrent() {
    for (const m of [mesh, pickMesh, edgeLines, ribbonLines]) {
      if (!m) continue;
      scene.remove(m); pickScene.remove(m);
      m.geometry.dispose();
      if (Array.isArray(m.material)) m.material.forEach((x) => x.dispose());
      else m.material.dispose();
    }
    mesh = pickMesh = edgeLines = ribbonLines = null;
  }

  // THE LAST RENDERER (operator ruling d7d55257, Thoth mail 11066, freezing the renderer):
  // "points at a constant SCREEN size in px on a steep degree curve (~2px leaf, ~8px@100
  // links, ~16px@1000, ~32px@10000; no world-unit sizing, no 48px cap)." A full circle back
  // to this file's own ORIGINAL pre-legibility-pass scheme (aRadiusPx * uWorldPerPx, a
  // constant screen size regardless of zoom) -- what changed since is the CURVE, not the
  // mechanism: px = 2 * degree^log10(2) hits all four of the ruling's own anchors exactly
  // (degree 1 -> 2px, 100 -> 8px, 1,000 -> 16px, 10,000 -> 32px -- verified algebraically:
  // d^log10(2) = 10^(log10(d)*log10(2)) = 2^log10(d), so at d=10^k the curve is exactly
  // 2*2^k), left uncapped past that per the ruling's own words -- no world-unit sizing, no
  // LOD tiers standing in for a floor once zoomed out (kill LOD entirely, same mail).
  const DEGREE_PX_BASE = 2;
  const DEGREE_PX_EXPONENT = Math.log10(2); // ≈0.30103
  function nodeScreenPx(nd) {
    return DEGREE_PX_BASE * Math.pow(Math.max(nd.degree || 0, 1), DEGREE_PX_EXPONENT);
  }
  function worldPerPx() { return viewSize / wrap.clientHeight; }

  // TIP 4 (operator ruling "DENSITY NOT DISCS", mail 11011): "every object draws at every
  // zoom as an additive point sprite... a project far out is a haze whose brightness is its
  // count." `additive` is true for the DRAW mesh only, never the pick mesh -- GPU picking
  // decodes an exact RGB-encoded instance id out of the render target, which additive
  // blending would corrupt the moment two picked instances' colours overlap in that tiny
  // readback; the pick mesh stays fully opaque, same as before this tip. THE LAST RENDERER
  // caps overall SATURATION with a tone-map post-process pass instead (see
  // makeToneMapPass below) rather than a per-instance opacity ceiling, so raw additive
  // brightness here can exceed 1.0 without a hard per-material alpha limiting it.
  const NODE_POINT_OPACITY = 0.85;
  function makeInstancedCircleMaterial(opts) {
    const additive = !!(opts && opts.additive);
    const uniforms = { uWorldPerPx: { value: worldPerPx() } };
    const matOpts = { vertexColors: true };
    if (additive) {
      Object.assign(matOpts, {
        transparent: true, opacity: NODE_POINT_OPACITY,
        depthWrite: false, blending: THREE.AdditiveBlending,
      });
    }
    const mat = new THREE.MeshBasicMaterial(matOpts);
    mat.onBeforeCompile = (shader) => {
      shader.uniforms.uWorldPerPx = uniforms.uWorldPerPx;
      shader.vertexShader =
        "attribute float aRadiusPx;\nattribute float aVisible;\n" +
        "uniform float uWorldPerPx;\n" + shader.vertexShader;
      shader.vertexShader = shader.vertexShader.replace(
        "#include <begin_vertex>",
        "#include <begin_vertex>\n\ttransformed *= aRadiusPx * uWorldPerPx * aVisible;"
      );
    };
    mat.customProgramCacheKey = () => "circleInstancedScreenPx";
    return { material: mat, uniforms };
  }

  // THE READING LAYER, part A: edges fade by SCREEN length, not by zoom level — a long line
  // crossing most of the view (two clusters that happen to be linked) reads as noise; a
  // short local one is the actual signal. Same GPU-uniform discipline as node sizing (mail
  // 10581): each vertex carries the OTHER endpoint's world position too (`otherPosition`),
  // so the vertex shader can project both ends to screen pixels and compute the segment's
  // own on-screen length using nothing but modelViewMatrix/projectionMatrix — already
  // updated by three.js every frame for free. No per-zoom CPU work, no material.opacity
  // scalar to keep in sync (replaces the old viewSize-based updateEdgeStyle entirely).
  // THE LAST RENDERER (Thoth mail 11066): "an edge draws only when both ends are visible,
  // no structural-hop exception, with an alpha floor (~0.06) so any drawn edge is faintly
  // visible." uMinAlpha raised from 0.04 to that floor; the density-scale multiplier TIP 4
  // added (uDensityScale) is gone -- overall saturation is now bounded by the tone-map
  // post-process pass (makeToneMapPass) instead of thinning every edge's own alpha by how
  // many are on screen.
  const edgeFadeUniforms = {
    uViewportPx: { value: new THREE.Vector2(wrap.clientWidth, wrap.clientHeight) },
    uMaxFadePx: { value: 320 },
    uMinAlpha: { value: 0.06 },
    uMaxAlpha: { value: 0.5 },
  };
  function makeEdgeFadeMaterial() {
    return new THREE.ShaderMaterial({
      uniforms: edgeFadeUniforms,
      transparent: true,
      depthWrite: false,
      blending: THREE.AdditiveBlending,
      vertexShader: `
        attribute vec3 color;
        attribute vec3 otherPosition;
        uniform vec2 uViewportPx;
        uniform float uMaxFadePx;
        uniform float uMinAlpha;
        uniform float uMaxAlpha;
        varying vec3 vColor;
        varying float vAlpha;
        void main() {
          vColor = color;
          vec4 clip = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
          vec4 otherClip = projectionMatrix * modelViewMatrix * vec4(otherPosition, 1.0);
          vec2 pxA = (clip.xy / clip.w * 0.5 + 0.5) * uViewportPx;
          vec2 pxB = (otherClip.xy / otherClip.w * 0.5 + 0.5) * uViewportPx;
          float screenLen = distance(pxA, pxB);
          vAlpha = mix(uMaxAlpha, uMinAlpha, clamp(screenLen / uMaxFadePx, 0.0, 1.0));
          gl_Position = clip;
        }
      `,
      fragmentShader: `
        varying vec3 vColor;
        varying float vAlpha;
        void main() { gl_FragColor = vec4(vColor, vAlpha); }
      `,
    });
  }

  // THE DRAWING TIP (Thoth mail 11408, operator ruling 4a51cab1/1178e7d9, thread 325ef660):
  // "nothing hidden, nothing drawn twice" -- caps and hides (ruling c5953bb1's own
  // "structural hidden by default") were the OLD answer to graph density; this tip
  // replaces the answer, not just the renderer. Membership is a REGION (a district fill),
  // the two universal fans are LANDMARKS with a count, every other edge draws at rest
  // (same-district as a line, cross-district aggregated into a per-(district,district,type)
  // ribbon that resolves to individual lines once that specific ribbon's own endpoints are
  // far enough apart on screen). Confirmed by the two numbers-first spikes this tip builds
  // on: Seshat 57992143 (this district/ribbon/landmark model, frame 0.77ms, accounting
  // exact) and Khnum 5f6c4db3 (hubs are already excluded as membership containers, not a
  // separate hub-zone concern for this renderer).
  //
  // THE DISTRICT MODEL: `districts` is project_aggregates (already computed server-side --
  // a real centroid + exact member-distance-bound radius per project, no new query).
  // `DISTRICT_FILL_TYPES` are the five membership link types (item 1: the original four
  // plus Sekhmet's new `owned_by`) -- never drawn as lines at all, the district fill IS the
  // membership claim, drawn once as a filled region instead of once per member as a spoke.
  // `landmarks` are the two universal-fan targets (item 3) -- found DATA-DRIVEN (the single
  // node receiving the most edges of that type), not hardcoded by canonical id. Every edge
  // landing on a landmark's own target, of that landmark's own type, is excluded from
  // line-drawing and folded into that node's own badge count instead.
  const DISTRICT_FILL_TYPES = new Set(["in_repo", "works_in", "holds", "member_of", "owned_by"]);
  const LANDMARK_EDGE_TYPES = ["acts_for", "authored_by"];
  let districts = []; // [{name, count, cx, cy, radius}]
  let districtByName = new Map();
  let landmarks = {}; // {acts_for: {id,count}|null, authored_by: {id,count}|null}
  let ribbons = []; // [{a, b, type, count}] -- a/b are district names, a <= b
  let ribbonsResolvedKeys = new Set(); // "a|b|type" keys currently resolved to individual lines
  let districtLabelCandidates = []; // pseudo-nodes for pickLabels' own shared budget, below
  // WAVE 26, THE STORYLINE (mail 11534): declared here, well before fitToNodes' own initial
  // synchronous call site (below) reads storylineActive via syncStorylineAxis -- the exact
  // TDZ crash class districtLabelCandidates above already hit once; see renderStoryline's
  // own docstring, further down, for what these actually mean.
  let storylineActive = false;
  let storylineChainIds = new Set();
  let storylineSubAgentOf = new Map();
  let storylineTickOf = new Map();
  let storylineMinT = 0, storylineMaxT = 1, storylineAxisWidthWorld = 1;
  let storylineLines = null;
  let storylineAxisEntries = []; // [{t, x, y, div}]
  let storylineMaxSubrowOffsetPx = 0; // deepest sub-agent arc offset (raw px) used this render (THE SPAWN ROW)
  // WAVE 26, COMMUNITY REGIONS (mail 11592/11664): declared here for the same reason as
  // the storyline state just above -- syncCommunityVisibility (further down) is read from
  // fitToNodes' own initial synchronous call site, well before this point in the file
  // would otherwise execute a `let` declared near its own function.
  let communities = []; // [{code, districtName, count, cx, cy, radius}]
  let communityByCode = new Map();
  let communityRegionsVisible = false;
  let communityRibbons = []; // [{a, b, type, count}] -- a/b are community codes, a <= b
  let communityRibbonsResolvedKeys = new Set();
  let communityLabelCandidates = [];
  let communityMeshGroup = null;
  function ribbonKey(r) { return `${r.a}|${r.b}|${r.type}`; }
  function findLandmark(type) {
    const counts = new Map();
    for (const e of edges) { if (e.type === type) counts.set(e.target, (counts.get(e.target) || 0) + 1); }
    let bestId = null, bestN = 0;
    for (const [id, n] of counts) { if (n > bestN) { bestN = n; bestId = id; } }
    return bestId ? { id: bestId, count: bestN } : null;
  }
  function buildDistrictModel(districtAggregates, edgeList) {
    districts = districtAggregates || [];
    districtByName = new Map(districts.map((d) => [d.name, d]));
    edges = edgeList; // findLandmark reads the module-level `edges` closure
    landmarks = {};
    for (const t of LANDMARK_EDGE_TYPES) landmarks[t] = findLandmark(t);
    // the real per-district-pair aggregation (computeRibbons) needs idById, not built yet
    // at this fetch/parse stage -- deferred to buildRibbonLines, called after buildScene.
    ribbonsResolvedKeys = new Set();
    buildDistrictLabelCandidates();
  }
  function computeRibbons() {
    const counts = new Map(); // "a|b|type" -> count
    const meta = new Map();
    for (const e of edges) {
      if (DISTRICT_FILL_TYPES.has(e.type)) continue;
      const lm = landmarks[e.type];
      if (lm && e.target === lm.id) continue;
      const na = idById.get(e.source), nb = idById.get(e.target);
      if (!na || !nb || na.project === nb.project) continue; // same-district: drawn individually
      const [a, b] = na.project <= nb.project ? [na.project, nb.project] : [nb.project, na.project];
      if (!districtByName.has(a) || !districtByName.has(b)) continue;
      const key = `${a}|${b}|${e.type}`;
      counts.set(key, (counts.get(key) || 0) + 1);
      meta.set(key, { a, b, type: e.type });
    }
    ribbons = [...counts.entries()]
      .map(([key, count]) => ({ ...meta.get(key), count }))
      .sort((x, y) => y.count - x.count);
    return ribbons;
  }
  // THE PER-RIBBON RESOLVE (item 2, mail 11408: "resolving per ribbon by its own
  // screen-space centroid distance, not one global viewSize scalar"; refined by mail 11414
  // off the prior-art note's own §4 -- "nothing drawn twice" needs a ribbon never co-drawn
  // beside the lines it summarises, either (a) hierarchy-routed splines that separate on
  // zoom, or (b) a strict LOD swap, one or the other per ribbon, never both. Picked (b): a
  // ribbon in `ribbonsResolvedKeys` is dropped from the ribbon mesh entirely and its own
  // edges draw as individual lines instead; a ribbon NOT in the set draws only in the
  // ribbon mesh -- edgeAccounting()'s own line/ribbon counts are exactly this swap,
  // verified live never double-counting the same edge either way.
  //
  // the spike's own single median-radius-derived threshold flipped EVERY ribbon at once
  // regardless of how far apart its own two districts actually sit -- a ribbon between two
  // ADJACENT small districts resolved at the exact same zoom step as one spanning the whole
  // graph. Each ribbon's own two district centroids are projected to real screen pixels (the
  // same camera.project convention positionLandmarkBadges already uses); a ribbon resolves
  // once its own on-screen centroid distance crosses the threshold. Recomputed on the same
  // deliberate-step cadence buildEdgeLines' other callers already follow (a zoom step or a
  // camera fit, never per pointermove) -- cheap, and consistent with "rebuild on a
  // deliberate step, never per frame."
  const RIBBON_RESOLVE_SCREEN_PX = 900;
  function districtScreenPx(d) {
    _screenV.set(d.cx, d.cy, 0).project(camera);
    return { x: (_screenV.x * 0.5 + 0.5) * wrap.clientWidth, y: (-_screenV.y * 0.5 + 0.5) * wrap.clientHeight };
  }
  function computeResolvedRibbonKeys() {
    const resolved = new Set();
    for (const r of ribbons) {
      const da = districtByName.get(r.a), db = districtByName.get(r.b);
      if (!da || !db) continue;
      const pa = districtScreenPx(da), pb = districtScreenPx(db);
      if (Math.hypot(pa.x - pb.x, pa.y - pb.y) > RIBBON_RESOLVE_SCREEN_PX) resolved.add(ribbonKey(r));
    }
    return resolved;
  }
  function ribbonKeySetsEqual(a, b) {
    if (a.size !== b.size) return false;
    for (const k of a) if (!b.has(k)) return false;
    return true;
  }
  // "accounting exact" (mail 11408's own acceptance line, echoing the spike's 11392):
  // every live edge counted into EXACTLY one of fill/landmark/line/ribbon -- a live-
  // verification receipt hook, not consulted by the renderer itself.
  function edgeAccounting() {
    let fill = 0, landmark = 0, line = 0, ribbon = 0, communityRibbon = 0, other = 0;
    for (const e of edges) {
      if (DISTRICT_FILL_TYPES.has(e.type)) { fill++; continue; }
      const lm = landmarks[e.type];
      // WAVE 27, THE LENS PANEL: a hidden badge still isn't "gone" -- its own edges just
      // draw (and count) as ordinary lines instead, same "nothing hidden" promise the
      // community bucket below already keeps under its own visibility gate.
      if (lm && e.target === lm.id) {
        if (highDegreeBadgesHiddenByLens) { line++; } else { landmark++; }
        continue;
      }
      const na = idById.get(e.source), nb = idById.get(e.target);
      if (na && nb && na.project !== nb.project &&
        districtByName.has(na.project) && districtByName.has(nb.project)) {
        const a = na.project <= nb.project ? na.project : nb.project;
        const b = na.project <= nb.project ? nb.project : na.project;
        if (ribbonsResolvedKeys.has(`${a}|${b}|${e.type}`)) line++; else ribbon++;
        continue;
      }
      // WAVE 26, PIECE 2: the SAME swap one level down, only live once communities are
      // actually visible (mid zoom) -- below that, this bucket stays empty and every
      // same-district edge counts as an ordinary "line", matching what's actually drawn.
      if (communityRegionsVisible && na && nb && na.project === nb.project &&
        na.communityCode && nb.communityCode && na.communityCode !== nb.communityCode) {
        const ca = na.communityCode <= nb.communityCode ? na.communityCode : nb.communityCode;
        const cb = na.communityCode <= nb.communityCode ? nb.communityCode : na.communityCode;
        if (communityRibbonsResolvedKeys.has(`${ca}|${cb}|${e.type}`)) line++; else communityRibbon++;
        continue;
      }
      if (na && nb) { line++; continue; }
      other++; // an endpoint missing from idById -- should never happen, disclosed not hidden
    }
    return { total: edges.length, fill, landmark, line, ribbon, communityRibbon, other,
      accounted: fill + landmark + line + ribbon + communityRibbon + other };
  }
  // recomputes the resolved set and rebuilds ONLY when it actually changed -- same
  // "rebuild on a deliberate step, not per frame" discipline as syncRibbonResolve's own
  // callers (a zoom step, a camera fit), never wired to pointermove/pan.
  function syncRibbonResolve() {
    if (!ribbons.length) return;
    const resolved = computeResolvedRibbonKeys();
    if (ribbonKeySetsEqual(resolved, ribbonsResolvedKeys)) return;
    ribbonsResolvedKeys = resolved;
    buildEdgeLines(idToNode, edges);
    buildRibbonLines();
    markDirty();
  }
  const RIBBON_ALPHA_FLOOR = 0.25;
  function buildRibbonLines() {
    if (ribbonLines) { scene.remove(ribbonLines); ribbonLines.geometry.dispose(); ribbonLines.material.dispose(); ribbonLines = null; }
    computeRibbons();
    // "nothing drawn twice": a ribbon that has resolved to individual lines this frame is
    // redundant geometry -- drop it from the ribbon mesh entirely rather than layering both.
    const unresolved = ribbons.filter((r) => !ribbonsResolvedKeys.has(ribbonKey(r)));
    if (!unresolved.length) return;
    const positions = new Float32Array(unresolved.length * 6);
    const otherPositions = new Float32Array(unresolved.length * 6);
    const colors = new Float32Array(unresolved.length * 6);
    const maxCount = Math.max(...unresolved.map((r) => r.count));
    const ec = new THREE.Color();
    let vi = 0;
    for (const r of unresolved) {
      const da = districtByName.get(r.a), db = districtByName.get(r.b);
      if (!da || !db) continue;
      // Reinhard-shaped brightness by count, same convention as the bundled-curve alpha
      // falloff (BUNDLE_ALPHA_FLOOR) elsewhere in this file -- baked into vertex color
      // since LineBasicMaterial has no per-vertex width/alpha attribute.
      const bright = Math.max(RIBBON_ALPHA_FLOOR, Math.log1p(r.count) / Math.log1p(maxCount));
      ec.set(colorForEdgeType(r.type)).multiplyScalar(bright);
      positions[vi] = da.cx; positions[vi + 1] = da.cy; positions[vi + 2] = -0.2;
      otherPositions[vi] = db.cx; otherPositions[vi + 1] = db.cy; otherPositions[vi + 2] = -0.2;
      vi += 3;
      positions[vi] = db.cx; positions[vi + 1] = db.cy; positions[vi + 2] = -0.2;
      otherPositions[vi] = da.cx; otherPositions[vi + 1] = da.cy; otherPositions[vi + 2] = -0.2;
      vi += 3;
      colors[vi - 6] = ec.r; colors[vi - 5] = ec.g; colors[vi - 4] = ec.b;
      colors[vi - 3] = ec.r; colors[vi - 2] = ec.g; colors[vi - 1] = ec.b;
    }
    const geo = new THREE.BufferGeometry();
    geo.setAttribute("position", new THREE.BufferAttribute(positions.subarray(0, vi), 3));
    geo.setAttribute("otherPosition", new THREE.BufferAttribute(otherPositions.subarray(0, vi), 3));
    geo.setAttribute("color", new THREE.BufferAttribute(colors.subarray(0, vi), 3));
    ribbonLines = new THREE.LineSegments(geo, makeEdgeFadeMaterial());
    scene.add(ribbonLines);
  }
  // THE DISTRICT LABEL BUDGET (item 4, mail 11408: "district labels earn their place by
  // size -- one shared label budget with object labels, declutter with the same
  // overlapsPlaced, small districts under a threshold unlabelled at rest and folded into an
  // 'other' wash"). District labels no longer own a permanent div per district (the spike's
  // own always-on districtLabelEntries) -- they compete for the SAME N_LABELS slots and the
  // SAME overlapsPlaced declutter pass pickLabels/positionLabels already run for object
  // labels, entered as label-pool CANDIDATES (see pickLabels' own DISTRICT_LABEL_MIN_COUNT
  // gate and positionLabels' own district-label pass). `districtMeshGroup` (the fill
  // geometry itself) is unaffected -- every district still fills, labelled or not; only the
  // TEXT is budget-gated, and an unlabelled small district is what "folded into an 'other'
  // wash" means here -- its fill alone, unlabeled, reads as background texture rather than
  // a named place.
  const DISTRICT_LABEL_MIN_COUNT = 8; // below this member count, a district never labels at rest
  let districtMeshGroup = null;
  function buildDistrictFills() {
    if (districtMeshGroup) { scene.remove(districtMeshGroup); districtMeshGroup = null; }
    if (!districts.length) return;
    districtMeshGroup = new THREE.Group();
    const dc = new THREE.Color("#2a3f5f");
    for (const d of districts) {
      const geo = new THREE.CircleGeometry(Math.max(d.radius, 1), 32);
      const mat = new THREE.MeshBasicMaterial({ color: dc, transparent: true, opacity: 0.14, depthWrite: false });
      const mesh = new THREE.Mesh(geo, mat);
      mesh.position.set(d.cx, d.cy, -0.5); // behind both edges and nodes
      districtMeshGroup.add(mesh);
    }
    scene.add(districtMeshGroup);
  }
  let landmarkBadgeEntries = []; // [{id, type, div}]
  function buildLandmarkBadges() {
    for (const e of landmarkBadgeEntries) e.div.remove();
    landmarkBadgeEntries = [];
    // WAVE 27, THE LENS PANEL: the lens's own hide -- no divs at all, same "rare, deliberate
    // rebuild" convention every other legend checkbox already uses (not a per-frame CSS
    // hide). buildEdgeLines/edgeAccounting's own highDegreeBadgesHiddenByLens checks are
    // what keep those edges drawn as ordinary lines instead of vanishing outright.
    if (highDegreeBadgesHiddenByLens) return;
    for (const t of LANDMARK_EDGE_TYPES) {
      const lm = landmarks[t];
      if (!lm) continue;
      const div = document.createElement("div");
      div.className = "lod-glyph-label";
      const nd = idById.get(lm.id);
      div.textContent = `${nd ? labelTextFor(nd) : lm.id} — ${lm.count} ${t}`;
      labelsEl.appendChild(div);
      landmarkBadgeEntries.push({ id: lm.id, type: t, div });
    }
  }
  function positionLandmarkBadges() {
    for (const e of landmarkBadgeEntries) {
      const nd = idById.get(e.id);
      if (!nd) continue;
      _screenV.set(nd.x || 0, nd.y || 0, 0).project(camera);
      e.div.style.left = `${(_screenV.x * 0.5 + 0.5) * wrap.clientWidth}px`;
      e.div.style.top = `${(-_screenV.y * 0.5 + 0.5) * wrap.clientHeight}px`;
    }
  }

  // WAVE 26, PIECE 2: COMMUNITY REGIONS (Thoth mail 11592/11664, thread 3683a12a): "at mid
  // zoom inside a district, each community is a labelled region refined from the district
  // fill, never replacing it." Khnum's own `communities` header table is the SAME Leiden
  // partition his compact-arrangement layout is already built on -- reused, never
  // re-derived. Nested inside the district model, not a peer of it: a community only ever
  // exists WITHIN one district (a small project never has one at all, community_code stays
  // 0 for every one of its members), so every community-level check below runs on top of
  // an edge/node that already passed its own district-level check first.
  const COMMUNITY_LABEL_MIN_COUNT = 20; // below this member count, a community never labels
  function buildCommunityModel(communityAggregates) {
    communities = communityAggregates || [];
    communityByCode = new Map(communities.map((c) => [c.code, c]));
    communityRibbonsResolvedKeys = new Set();
    communityLabelCandidates = communities
      .filter((c) => c.count >= COMMUNITY_LABEL_MIN_COUNT)
      .map((c) => ({
        __isCommunity: true, id: `community:${c.code}`, name: `${c.districtName} · community ${c.code}`,
        x: c.cx, y: c.cy, degree: c.count,
      }));
    computeCommunityZoomViewSize();
  }
  // "AT MID ZOOM": a single global viewSize gate (not per-community -- the ask is "zoomed
  // into roughly a district's own scale," a whole-view state, not a per-region one) --
  // the median community radius is the same "one outlier district dominates the extent"
  // defence THE DRAWING TIP's own spike used for its first (later replaced) ribbon
  // threshold, reused here because visibility genuinely IS a single yes/no at this zoom,
  // unlike ribbon resolution (which stays per-ribbon, computeResolvedCommunityRibbonKeys
  // below).
  let communityZoomViewSize = 0;
  function computeCommunityZoomViewSize() {
    if (!communities.length) { communityZoomViewSize = 0; return; }
    const radii = communities.map((c) => c.radius).sort((a, b) => a - b);
    communityZoomViewSize = radii[Math.floor(radii.length / 2)] * 3;
  }
  function buildCommunityFills() {
    if (communityMeshGroup) { scene.remove(communityMeshGroup); communityMeshGroup = null; }
    if (!communities.length) return;
    communityMeshGroup = new THREE.Group();
    const cc = new THREE.Color("#3a2f5f"); // a distinct, warmer tone from the district fill's
    for (const c of communities) {          // #2a3f5f -- nesting must read visually, not just logically
      const geo = new THREE.CircleGeometry(Math.max(c.radius, 1), 24);
      const mat = new THREE.MeshBasicMaterial({ color: cc, transparent: true, opacity: 0.22, depthWrite: false });
      const mesh = new THREE.Mesh(geo, mat);
      mesh.position.set(c.cx, c.cy, -0.45); // between the district fill (-0.5) and edges (-0.1)
      communityMeshGroup.add(mesh);
    }
    communityMeshGroup.visible = communityRegionsVisible;
    scene.add(communityMeshGroup);
  }
  function computeCommunityRibbons() {
    const counts = new Map(); // "a|b|type" -> count
    const meta = new Map();
    for (const e of edges) {
      if (DISTRICT_FILL_TYPES.has(e.type)) continue;
      const lm = landmarks[e.type];
      if (lm && e.target === lm.id) continue;
      const na = idById.get(e.source), nb = idById.get(e.target);
      if (!na || !nb || na.project !== nb.project) continue; // community ribbons are SAME-district only
      const ca = na.communityCode, cb = nb.communityCode;
      if (!ca || !cb || ca === cb) continue; // same-community: drawn individually, like same-district
      const [a, b] = ca <= cb ? [ca, cb] : [cb, ca];
      const key = `${a}|${b}|${e.type}`;
      counts.set(key, (counts.get(key) || 0) + 1);
      meta.set(key, { a, b, type: e.type });
    }
    communityRibbons = [...counts.entries()]
      .map(([key, count]) => ({ ...meta.get(key), count }))
      .sort((x, y) => y.count - x.count);
    return communityRibbons;
  }
  // per-ribbon screen-distance resolve, same convention as computeResolvedRibbonKeys --
  // "ribbons between communities resolve the same way district ribbons do" (mail 11664).
  function pointScreenPx(x, y) {
    _screenV.set(x, y, 0).project(camera);
    return { x: (_screenV.x * 0.5 + 0.5) * wrap.clientWidth, y: (-_screenV.y * 0.5 + 0.5) * wrap.clientHeight };
  }
  function computeResolvedCommunityRibbonKeys() {
    const resolved = new Set();
    if (!communityRegionsVisible) return resolved; // hidden entirely below mid zoom
    for (const r of communityRibbons) {
      const ca = communityByCode.get(r.a), cb = communityByCode.get(r.b);
      if (!ca || !cb) continue;
      const pa = pointScreenPx(ca.cx, ca.cy), pb = pointScreenPx(cb.cx, cb.cy);
      if (Math.hypot(pa.x - pb.x, pa.y - pb.y) > RIBBON_RESOLVE_SCREEN_PX) resolved.add(ribbonKey(r));
    }
    return resolved;
  }
  let communityRibbonLines = null;
  function buildCommunityRibbonLines() {
    if (communityRibbonLines) {
      scene.remove(communityRibbonLines);
      communityRibbonLines.geometry.dispose();
      communityRibbonLines.material.dispose();
      communityRibbonLines = null;
    }
    computeCommunityRibbons();
    const unresolved = communityRegionsVisible
      ? communityRibbons.filter((r) => !communityRibbonsResolvedKeys.has(ribbonKey(r)))
      : []; // never drawn at all below mid zoom -- the plain same-district line covers it
    if (!unresolved.length) return;
    const positions = new Float32Array(unresolved.length * 6);
    const otherPositions = new Float32Array(unresolved.length * 6);
    const colors = new Float32Array(unresolved.length * 6);
    const maxCount = Math.max(...unresolved.map((r) => r.count));
    const ec = new THREE.Color();
    let vi = 0;
    for (const r of unresolved) {
      const ca = communityByCode.get(r.a), cb = communityByCode.get(r.b);
      if (!ca || !cb) continue;
      const bright = Math.max(RIBBON_ALPHA_FLOOR, Math.log1p(r.count) / Math.log1p(maxCount));
      ec.set(colorForEdgeType(r.type)).multiplyScalar(bright);
      positions[vi] = ca.cx; positions[vi + 1] = ca.cy; positions[vi + 2] = -0.15;
      otherPositions[vi] = cb.cx; otherPositions[vi + 1] = cb.cy; otherPositions[vi + 2] = -0.15;
      vi += 3;
      positions[vi] = cb.cx; positions[vi + 1] = cb.cy; positions[vi + 2] = -0.15;
      otherPositions[vi] = ca.cx; otherPositions[vi + 1] = ca.cy; otherPositions[vi + 2] = -0.15;
      vi += 3;
      colors[vi - 6] = ec.r; colors[vi - 5] = ec.g; colors[vi - 4] = ec.b;
      colors[vi - 3] = ec.r; colors[vi - 2] = ec.g; colors[vi - 1] = ec.b;
    }
    const geo = new THREE.BufferGeometry();
    geo.setAttribute("position", new THREE.BufferAttribute(positions.subarray(0, vi), 3));
    geo.setAttribute("otherPosition", new THREE.BufferAttribute(otherPositions.subarray(0, vi), 3));
    geo.setAttribute("color", new THREE.BufferAttribute(colors.subarray(0, vi), 3));
    communityRibbonLines = new THREE.LineSegments(geo, makeEdgeFadeMaterial());
    scene.add(communityRibbonLines);
  }
  // recomputes visibility + the resolved set, rebuilding ONLY when either actually changed
  // -- same "deliberate step, never per frame" discipline as syncRibbonResolve.
  function syncCommunityVisibility() {
    if (!communities.length) return;
    const wasVisible = communityRegionsVisible;
    // WAVE 27, THE LENS PANEL: the lens toggle is a hard AND on top of the zoom gate --
    // hidden means hidden regardless of scale, same call site either way (a checkbox
    // change re-invokes this function directly, so the existing wasVisible/resolvedChanged
    // early-return already covers "did anything actually change" for both triggers).
    communityRegionsVisible = !communitiesHiddenByLens &&
      communityZoomViewSize > 0 && viewSize < communityZoomViewSize;
    const resolved = computeResolvedCommunityRibbonKeys();
    const resolvedChanged = !ribbonKeySetsEqual(resolved, communityRibbonsResolvedKeys);
    if (wasVisible === communityRegionsVisible && !resolvedChanged) return;
    communityRibbonsResolvedKeys = resolved;
    if (communityMeshGroup) communityMeshGroup.visible = communityRegionsVisible;
    buildEdgeLines(idToNode, edges);
    buildCommunityRibbonLines();
    if (wasVisible !== communityRegionsVisible) scheduleLabelPick(); // label pool membership changed
    markDirty();
  }

  // legend state: which edge classes/types are hidden from the base render. Structural was
  // hidden by DEFAULT under the old "not drawn at rest" rule (ruling c5953bb1); THE DRAWING
  // TIP's own operator ruling (4a51cab1/1178e7d9) retires that rule outright -- "nothing
  // hidden ... caps and hides are escape hatches" -- structural edges that aren't already
  // folded into a district fill or a landmark badge (the five new types Sekhmet minted:
  // recorded_by, owned_by [also a fill type], admitted_by, vendor_of) now draw
  // at rest same as anything else; the legend remains how a reader opts back OUT.
  const hiddenEdgeClasses = new Set();
  const hiddenEdgeTypes = new Set();
  // TIP 1(d): "focus HIDES unreachable nodes AND EDGES" — an edge whose endpoint is
  // currently invisible (focus-unreachable or type-filtered, same aVisible flag applyDim
  // maintains) is dropped from the base layer too, not just faded; the bright path overlay
  // (updatePathEdges) draws the reachable ones on top regardless.
  function nodeVisible(nd) {
    if (!nd) return false;
    if (hiddenNodeTypes.has(nd.type)) return false;
    // THE DRILL, item 5 (Thoth mail 11048): revealedStubIds overrides a project's own
    // hidden state for the specific nodes a stub click revealed -- "without unhiding the
    // project" itself, only this one path.
    if (hiddenProjects.size > 0 && hiddenProjects.has(nd.project) && !revealedStubIds.has(nd.id)) return false;
    if (pathFocusId && nd.id !== pathFocusId && !pathReachable.has(nd.id)) return false;
    return true;
  }
  // THE LAST RENDERER (Thoth mail 11066): "an edge draws only when both ends are visible,
  // no structural-hop exception." Every prior tier/budget/bundling mechanism (TIP 3's LOD
  // cutoff, TIP 4's density scale, THE DRILL's cross-cluster Bezier bundling) is gone --
  // one universal rule, straight lines, nodeVisible on both ends, same at every zoom.
  function buildEdgeLines(nodes, edgeList) {
    if (edgeLines) { scene.remove(edgeLines); edgeLines.geometry.dispose(); edgeLines.material.dispose(); edgeLines = null; }
    const byId = new Map(nodes.map((nd) => [nd.id, nd]));
    // THE DRAWING TIP: district-fill types never draw as individual lines at all (the fill
    // IS the claim); a landmark's own incoming edges of its own type fold into that node's
    // badge count instead of a spoke; a cross-district edge of any other type is
    // represented once, either as a ribbon (its own key not yet in ribbonsResolvedKeys) or
    // individually (its own ribbon HAS resolved) -- never both, per "nothing drawn twice."
    const visible = edgeList.filter((e) => {
      if (DISTRICT_FILL_TYPES.has(e.type)) return false;
      const lm = landmarks[e.type];
      // WAVE 27, THE LENS PANEL: hiding the badge falls through to an ordinary line, never
      // to the district/community ribbon logic below (a landmark type's edges were never
      // part of any ribbon aggregate -- computeRibbons excludes them unconditionally, badge
      // shown or not -- so routing them through that swap here would misclassify them).
      if (lm && e.target === lm.id) {
        if (!highDegreeBadgesHiddenByLens) return false;
        return !hiddenEdgeClasses.has(e.edgeClass) && !hiddenEdgeTypes.has(e.type) &&
          nodeVisible(byId.get(e.source)) && nodeVisible(byId.get(e.target));
      }
      const na = byId.get(e.source), nb = byId.get(e.target);
      if (na && nb && na.project !== nb.project &&
        districtByName.has(na.project) && districtByName.has(nb.project)) {
        const a = na.project <= nb.project ? na.project : nb.project;
        const b = na.project <= nb.project ? nb.project : na.project;
        if (!ribbonsResolvedKeys.has(`${a}|${b}|${e.type}`)) return false;
      } else if (communityRegionsVisible && na && nb && na.project === nb.project &&
        na.communityCode && nb.communityCode && na.communityCode !== nb.communityCode) {
        // WAVE 26, PIECE 2: a same-district edge refines one level further once the
        // reader is zoomed to community scale -- the same "line unless the ribbon hasn't
        // resolved" swap, one level down, only checked at all when communities are
        // actually showing (never below mid zoom, where the plain same-district line
        // this `else` skips is exactly right).
        const ca = na.communityCode <= nb.communityCode ? na.communityCode : nb.communityCode;
        const cb = na.communityCode <= nb.communityCode ? nb.communityCode : na.communityCode;
        if (!communityRibbonsResolvedKeys.has(`${ca}|${cb}|${e.type}`)) return false;
      }
      return !hiddenEdgeClasses.has(e.edgeClass) && !hiddenEdgeTypes.has(e.type) &&
        nodeVisible(byId.get(e.source)) && nodeVisible(byId.get(e.target));
    });
    const positions = new Float32Array(visible.length * 6);
    const otherPositions = new Float32Array(visible.length * 6);
    const edgeColors = new Float32Array(visible.length * 6);
    const ec = new THREE.Color();
    let vi = 0;
    for (const e of visible) {
      const na = byId.get(e.source), nb = byId.get(e.target);
      if (!na || !nb) continue;
      // colour-coded by relationship type ("that would make a ton of sense" — no link-type
      // palette exists server-side, so a stable hash-to-hue keeps a given edge type the
      // same colour across reloads without inventing new server state).
      ec.set(colorForEdgeType(e.type));
      const ax = na.x || 0, ay = na.y || 0, bx = nb.x || 0, by = nb.y || 0;
      positions[vi] = ax; positions[vi + 1] = ay; positions[vi + 2] = -0.1;
      otherPositions[vi] = bx; otherPositions[vi + 1] = by; otherPositions[vi + 2] = -0.1;
      vi += 3;
      positions[vi] = bx; positions[vi + 1] = by; positions[vi + 2] = -0.1;
      otherPositions[vi] = ax; otherPositions[vi + 1] = ay; otherPositions[vi + 2] = -0.1;
      vi += 3;
      edgeColors[vi - 6] = ec.r; edgeColors[vi - 5] = ec.g; edgeColors[vi - 4] = ec.b;
      edgeColors[vi - 3] = ec.r; edgeColors[vi - 2] = ec.g; edgeColors[vi - 1] = ec.b;
    }
    const edgeGeo = new THREE.BufferGeometry();
    edgeGeo.setAttribute("position", new THREE.BufferAttribute(positions.subarray(0, vi), 3));
    edgeGeo.setAttribute("otherPosition", new THREE.BufferAttribute(otherPositions.subarray(0, vi), 3));
    edgeGeo.setAttribute("color", new THREE.BufferAttribute(edgeColors.subarray(0, vi), 3));
    edgeLines = new THREE.LineSegments(edgeGeo, makeEdgeFadeMaterial());
    scene.add(edgeLines);
    renderLegend(edgeList, nodes);
    markDirty();
  }

  // legend: lists every class + type actually present in the loaded data, checkbox per
  // row, toggling straight into hiddenEdgeClasses/hiddenEdgeTypes and rebuilding the edge
  // geometry — a legend toggle is a rare, deliberate act, never a per-frame cost. TIP 1(e):
  // node types sit alongside edge classes now, driving the same aVisible flag the header
  // taxonomy pills drive (setHiddenTypes) — either control moves the one underlying filter.
  function renderLegend(edgeList, nodeList) {
    if (!legendPanel) return;
    const classOf = new Map();
    for (const e of edgeList) classOf.set(e.type, e.edgeClass);
    const byClass = { semantic: [], structural: [], container: [] };
    for (const [type, cls] of classOf) (byClass[cls] || (byClass[cls] = [])).push(type);
    for (const k of Object.keys(byClass)) byClass[k].sort();
    const nodeTypes = [...new Set((nodeList || []).map((nd) => nd.type))].sort();

    const classRow = (cls) => {
      const checked = hiddenEdgeClasses.has(cls) ? "" : "checked";
      const count = (byClass[cls] || []).length;
      return `<label class="legend-row legend-class"><input type="checkbox" data-legend-class="${cls}" ${checked} /> <strong>${cls}</strong> <span class="o-faint">(${count})</span></label>`;
    };
    const typeRow = (type) => {
      const checked = hiddenEdgeTypes.has(type) ? "" : "checked";
      const esc = String(type).replace(/"/g, "&quot;");
      return `<label class="legend-row legend-type"><input type="checkbox" data-legend-type="${esc}" ${checked} /> <span class="legend-swatch" style="background:${colorForEdgeType(type)}"></span>${esc}</label>`;
    };
    const nodeTypeRow = (type) => {
      const checked = hiddenNodeTypes.has(type) ? "" : "checked";
      const esc = String(type).replace(/"/g, "&quot;");
      return `<label class="legend-row legend-node-type"><input type="checkbox" data-legend-node-type="${esc}" ${checked} /> <span class="legend-swatch" style="background:${typeColors.get(type) || "#6e7681"}"></span>${esc}</label>`;
    };
    // WAVE 27, THE LENS PANEL (Thoth mail 11754): two more opt-OUT rows past the edge/node
    // type checkboxes above -- same convention (checked = shown, the default), same rebuild-
    // on-toggle discipline, just gating a mesh/badge group instead of hiddenEdgeClasses/Types.
    const lensRow = (key, label, checked) =>
      `<label class="legend-row legend-lens"><input type="checkbox" data-legend-lens="${key}" ${checked ? "checked" : ""} /> ${label}</label>`;
    legendPanel.innerHTML =
      `<div class="legend-row legend-class"><strong>node types</strong></div>` +
      nodeTypes.map(nodeTypeRow).join("") +
      classRow("semantic") + (byClass.semantic || []).map(typeRow).join("") +
      classRow("structural") + (byClass.structural || []).map(typeRow).join("") +
      classRow("container") + (byClass.container || []).map(typeRow).join("") +
      `<div class="legend-row legend-class"><strong>lens</strong></div>` +
      lensRow("communities", "communities", !communitiesHiddenByLens) +
      lensRow("highDegree", "high-degree objects", !highDegreeBadgesHiddenByLens);

    legendPanel.querySelectorAll("[data-legend-node-type]").forEach((el) => {
      el.addEventListener("change", () => {
        const type = el.dataset.legendNodeType;
        if (el.checked) hiddenNodeTypes.delete(type); else hiddenNodeTypes.add(type);
        applyDim();
        buildEdgeLines(idToNode, edges);
        scheduleLabelPick(); // review flaw #5
      });
    });
    legendPanel.querySelectorAll("[data-legend-class]").forEach((el) => {
      el.addEventListener("change", () => {
        const cls = el.dataset.legendClass;
        if (el.checked) hiddenEdgeClasses.delete(cls); else hiddenEdgeClasses.add(cls);
        buildEdgeLines(idToNode, edges);
      });
    });
    legendPanel.querySelectorAll("[data-legend-type]").forEach((el) => {
      el.addEventListener("change", () => {
        const type = el.dataset.legendType;
        if (el.checked) hiddenEdgeTypes.delete(type); else hiddenEdgeTypes.add(type);
        buildEdgeLines(idToNode, edges);
      });
    });
    legendPanel.querySelectorAll("[data-legend-lens]").forEach((el) => {
      el.addEventListener("change", () => {
        const key = el.dataset.legendLens;
        if (key === "communities") {
          communitiesHiddenByLens = !el.checked;
          syncCommunityVisibility();
        } else if (key === "highDegree") {
          highDegreeBadgesHiddenByLens = !el.checked;
          buildLandmarkBadges();
          buildEdgeLines(idToNode, edges);
        }
        // syncCommunityVisibility bails out before reaching buildEdgeLines/renderLegend's
        // own writeLensStateToHash call when this graph has zero communities at all
        // (communities.length === 0) -- the toggle's own state must still reach the hash
        // even then, so this doesn't rely on that indirect path alone.
        writeLensStateToHash();
      });
    });
    writeLensStateToHash();
  }
  if (legendBtn && legendPanel) {
    legendBtn.addEventListener("click", () => { legendPanel.hidden = !legendPanel.hidden; });
  }

  function buildScene(nodes, edges) {
    disposeCurrent();
    idToNode = nodes;
    idById = new Map(nodes.map((nd) => [nd.id, nd]));
    buildProjectObjectIndex(); // THE DRILL: project-anchor click targets, kept current

    const n = nodes.length;
    const geo = new THREE.CircleGeometry(1, 10);
    // this three.js build's fragment shader only multiplies by vColor (and so only shows
    // instanceColor) when USE_COLOR/USE_COLOR_ALPHA is defined, which is driven by a
    // GEOMETRY-level `color` attribute, not instanceColor alone — the vertex shader
    // computes the right colour into vColor but the fragment shader silently drops it
    // without this, rendering flat black regardless of instanceColor.
    geo.setAttribute("color", new THREE.Float32BufferAttribute(
      new Float32Array(geo.attributes.position.count * 3).fill(1), 3));
    const radiusAttr = new THREE.InstancedBufferAttribute(new Float32Array(Math.max(n, 1)), 1);
    geo.setAttribute("aRadiusPx", radiusAttr); // shared by mesh + pickMesh, same geometry instance
    visibleAttr = new THREE.InstancedBufferAttribute(new Float32Array(Math.max(n, 1)).fill(1), 1);
    geo.setAttribute("aVisible", visibleAttr); // TIP 1(d)/(e): per-instance hide, updated in place by applyDim

    const built = makeInstancedCircleMaterial({ additive: true });
    const mat = built.material;
    meshUniforms = built.uniforms;
    mesh = new THREE.InstancedMesh(geo, mat, Math.max(n, 1));
    mesh.instanceColor = new THREE.InstancedBufferAttribute(new Float32Array(Math.max(n, 1) * 3), 3);

    const builtPick = makeInstancedCircleMaterial();
    const pickMat = builtPick.material;
    pickUniforms = builtPick.uniforms;
    pickMesh = new THREE.InstancedMesh(geo, pickMat, Math.max(n, 1));
    pickMesh.instanceColor = new THREE.InstancedBufferAttribute(new Float32Array(Math.max(n, 1) * 3), 3);

    const dummy = new THREE.Object3D();
    const color = new THREE.Color();
    const idColor = new THREE.Color();
    for (let i = 0; i < n; i++) {
      const nd = nodes[i];
      nd.radiusPx = nodeScreenPx(nd);
      radiusAttr.setX(i, nd.radiusPx);
      visibleAttr.setX(i, 1);
      // "sizing more intuitive where high-degree nodes stand out without obfuscating
      // smaller nodes" — a bigger circle can still sit BEHIND a smaller one drawn later
      // in the same z-plane; give every node a tiny z bias proportional to its own radius
      // so the important (bigger) ones are always nearer the camera and never occluded.
      // Scale stays 1 here deliberately — the shader (aRadiusPx * uWorldPerPx) owns sizing.
      dummy.position.set(nd.x || 0, nd.y || 0, nd.radiusPx * 0.002);
      dummy.scale.setScalar(1);
      dummy.updateMatrix();
      mesh.setMatrixAt(i, dummy.matrix);
      pickMesh.setMatrixAt(i, dummy.matrix);
      color.set(typeColors.get(nd.type) || "#6e7681");
      mesh.instanceColor.setXYZ(i, color.r, color.g, color.b);
      const id = i + 1;
      idColor.setRGB((id & 0xff) / 255, ((id >> 8) & 0xff) / 255, ((id >> 16) & 0xff) / 255);
      pickMesh.instanceColor.setXYZ(i, idColor.r, idColor.g, idColor.b);
    }
    radiusAttr.needsUpdate = true;
    mesh.instanceMatrix.needsUpdate = true;
    pickMesh.instanceMatrix.needsUpdate = true;
    scene.add(mesh);
    pickScene.add(pickMesh);

    buildDistrictFills();
    buildLandmarkBadges();
    buildRibbonLines(); // needs idById, built above -- first real call after buildDistrictModel
    buildCommunityFills();
    buildCommunityRibbonLines(); // needs idById too, same reason
    buildEdgeLines(nodes, edges);
    applyDim();
    markDirty();
  }

  // TIP 1(d): focus HIDES unreachable nodes outright (per-instance aVisible flag), not a
  // dim — "no dim" per Thoth's own dispatch. Reachable-but-not-focused nodes stay visible at
  // their normal type colour (still legible as part of the path); the focused node alone
  // gets the accent colour. A type hidden via the header/legend filter (hiddenNodeTypes,
  // TIP 1(e)) is invisible regardless of focus state.
  function applyDim() {
    if (!mesh) return;
    const color = new THREE.Color();
    const focused = !!pathFocusId;
    for (let i = 0; i < idToNode.length; i++) {
      const nd = idToNode[i];
      const typeHidden = hiddenNodeTypes.has(nd.type);
      const projectHidden = hiddenProjects.size > 0 && hiddenProjects.has(nd.project) &&
        !revealedStubIds.has(nd.id);
      const focusHidden = focused && nd.id !== pathFocusId && !pathReachable.has(nd.id);
      visibleAttr.setX(i, (typeHidden || projectHidden || focusHidden) ? 0 : 1);
      color.set(typeColors.get(nd.type) || "#6e7681");
      if (focused && nd.id === pathFocusId) color.set("#58a6ff");
      else if (!focused && nd.id === selectedId) color.set("#58a6ff");
      mesh.instanceColor.setXYZ(i, color.r, color.g, color.b);
    }
    mesh.instanceColor.needsUpdate = true;
    visibleAttr.needsUpdate = true;
    markDirty();
  }

  // TIP 1(e): the header taxonomy pills' own type filter (SELECTED_ENTITY_TYPES in
  // console.js) drives this — called with the full set of types that should stay HIDDEN
  // (console.js translates its own allowlist semantics before calling). The legend's own
  // node-type checkboxes (renderLegend, below) call this too, so both controls drive the
  // exact same aVisible flag rather than two independent mechanisms.
  function setHiddenTypes(types) {
    hiddenNodeTypes = new Set(types || []);
    applyDim();
    buildEdgeLines(idToNode, edges);
    scheduleLabelPick(); // review flaw #5: labels never re-picked on a filter change before
    // THE FILTER-FIT FIX (Thoth mail 11241): a filter change refits the camera to what's
    // now actually visible -- the camera used to just sit wherever it was, which reads as
    // a black screen when the old view center falls inside newly-hidden content.
    fitToNodes(visibleNodesForFit());
  }

  // CONSOLE CHROME CLEANUP piece 2 (decision 31717ca7): the header's repo pill's own
  // sibling to setHiddenTypes above — same "caller hands the full HIDDEN set, translated
  // from whatever allowlist/selection semantics that caller owns" convention (console.js's
  // selectRepos()/applyRepoFilter() do the SELECTED-repos-to-hidden-repos translation, same
  // shape toggleEntityType already does for types).
  function setHiddenProjects(projects) {
    hiddenProjects = new Set(projects || []);
    // THE DRILL, item 5: a fresh filter change starts from a clean reveal state -- a stub
    // reveal was scoped to the OLD filter's own boundary, not guaranteed to still make
    // sense against a new one.
    revealedStubIds = new Set();
    applyDim();
    buildEdgeLines(idToNode, edges);
    buildProjectStubs();
    // THE LAST RENDERER (Thoth mail 11066, measured leak): "project filter hides points,
    // edges and labels of hidden projects completely." applyDim/buildEdgeLines already gate
    // points/edges via nodeVisible; labels are a separate pool (pickLabels) that needs its
    // own re-pick to drop a just-hidden project's own labels immediately, not on the next
    // debounced pan/zoom.
    scheduleLabelPick();
    // THE FILTER-FIT FIX (Thoth mail 11241, live review of w299): "after picking osiris
    // the canvas rendered fully black until Fit" -- the camera used to just sit wherever
    // it was before the filter, which reads as a black screen when the old view center
    // falls inside newly-hidden content. Refit to the now-visible set immediately.
    fitToNodes(visibleNodesForFit());
  }

  async function loadTypeColors() {
    const cat = await fetch("/schema").then((r) => r.json());
    const m = new Map();
    for (const t of cat.object_types) m.set(t.name, t.color || "#6e7681");
    return m;
  }

  // frames the camera around the REAL bounding box of whatever's currently loaded —
  // the fixed viewSize=1300 default this used to reset to was measured against the old
  // force-relax layout's small extent and reads as "zoomed into one dense cluster" against
  // Khnum's new deterministic hash-placement layout, whose extent can run tens of
  // thousands of world units wide depending on how far apart two project hashes land.
  // THE FILTER-FIT FIX (Thoth mail 11241, live review of w299): "the canvas rendered fully
  // black until Fit, and Fit must fit the VISIBLE set, not the whole graph." fitToNodes
  // itself stays a pure bbox-over-a-list function (still used for the initial whole-graph
  // load and context-restore, where "the whole graph" IS the visible set); this is what
  // both the Fit button and a filter change now pass it, instead of raw idToNode.
  function visibleNodesForFit() {
    return idToNode.filter(nodeVisible);
  }
  function fitToNodes(list) {
    // THE 98TH-PERCENTILE FIT FIX (Thoth mail 11249): a handful of far outliers in the
    // visible set stretched the exact min/max bbox enough that the actual cluster sat in
    // one corner at a much-too-zoomed-out view (measured live: osiris at 67 wpp). Trim the
    // outermost 1% on each axis before framing -- still the real bbox, just not held
    // hostage by a few stray points.
    const xs = [], ys = [];
    for (const nd of list) {
      if (nd.x == null || nd.y == null) continue;
      xs.push(nd.x); ys.push(nd.y);
    }
    if (!xs.length) return;
    xs.sort((a, b) => a - b);
    ys.sort((a, b) => a - b);
    const lo = Math.floor(xs.length * 0.01);
    const hi = Math.max(lo, Math.ceil(xs.length * 0.99) - 1);
    const minX = xs[lo], maxX = xs[hi], minY = ys[lo], maxY = ys[hi];
    camera.position.x = (minX + maxX) / 2;
    camera.position.y = (minY + maxY) / 2;
    const span = Math.max(maxX - minX, maxY - minY, 0);
    viewSize = Math.max(30, span * 1.1 + 40);
    // the wheel clamp's own bounds (Thoth's fix, mail 10581) — derived from THIS fit's real
    // span, not a guess: a floor small enough to inspect one dense cluster, a ceiling about
    // 2x the whole fitted graph so "zoom out" can't run away past anything meaningful.
    minViewSize = 20;
    maxViewSize = Math.max(span * 2, 200);
    updateFrustum();
    rescaleForZoom();
    syncRibbonResolve();
    syncStorylineAxis();
    syncCommunityVisibility();
    markDirty();
  }

  // ---- THE LAST RENDERER retired the whole LOD/tier/cluster/halo machinery this comment
  // block used to introduce (operator ruling d7d55257, Thoth mail 11066: "kill LOD
  // entirely -- remove zoomLOD, label tiers, cluster_edges at far, cluster rings, the halo
  // texture, per-tier alpha, and every far/mid/near branch; delete their tests"). See
  // positionLabels/pickLabels below for the one label rule that replaces it (viewport
  // top-N by degree, de-overlapped, at every zoom, no separate project-label pass).
  setStatus("loading the whole graph…");
  let { nodes, edges, edgeClassByType, districtAggregates, communityAggregates } =
    await fetchStreamSnapshot();
  applyLensStateFromHash(); // before the first buildScene/fitToNodes so the initial render already reflects a shared link
  buildDistrictModel(districtAggregates, edges);
  buildCommunityModel(communityAggregates);
  buildScene(nodes, edges);
  fitToNodes(nodes);
  setStatus(`${nodes.length} objects, ${edges.length} edges`);
  levelBadge.textContent = "whole graph";

  // ---- deltas: GET /graph/stream/deltas is an SSE poll-diff over the outbox, keyed by
  // object id (not array index — see the module docstring). Applied live so the canvas
  // never needs a full reload after the first snapshot; a 'retired' delta drops the node
  // from the next full rebuild rather than trying to hide a single InstancedMesh instance
  // (there is no per-instance visibility toggle cheaper than a rebuild at this node count).
  let nodesById = new Map(nodes.map((nd) => [nd.id, nd]));
  let pendingRebuild = false;
  function scheduleRebuild() {
    if (pendingRebuild) return;
    pendingRebuild = true;
    setTimeout(() => {
      pendingRebuild = false;
      nodes = Array.from(nodesById.values());
      buildScene(nodes, edges);
      if (pathFocusId || selectedId) applyDim();
      setStatus(`${nodes.length} objects, ${edges.length} edges (live)`);
    }, 250);
  }
  try {
    const es = new EventSource("/graph/stream/deltas");
    es.onmessage = (ev) => {
      let delta;
      try { delta = JSON.parse(ev.data); } catch { return; }
      if (delta.op === "retired") {
        nodesById.delete(delta.id);
        scheduleRebuild();
      } else if (delta.op === "moved") {
        const nd = nodesById.get(delta.id);
        if (nd && delta.x != null && delta.y != null) { nd.x = delta.x; nd.y = delta.y; scheduleRebuild(); }
      }
    };
    es.onerror = () => { /* browser auto-reconnects an EventSource; nothing to do here */ };
  } catch (err) {
    console.error("graph/stream/deltas unavailable", err);
  }

  // THE READING LAYER, part B, AMENDED by TIP 1's own amendment (operator via Thoth mail
  // 10726, ruling amending e1cb9e3b): "the lens is the TREE TO SOURCE" — upstream (X's
  // OUTGOING edges, X.source -> target, Osiris's own from_id->to_id convention) walks by
  // DEFAULT, until roots (no depth cap — focusDepth is Infinity now, not a fixed 4);
  // downstream (INCOMING edges) is a TOGGLE, off by default (includeDownstream). Structural
  // containment (in_repo, works_in, ...) never widens the walk itself, per part A — only the
  // "focus is never empty" one-hop fallback below reaches into it.
  // buildPathAdjacency/walkPath are pure, DOM-free, module-level functions (below the
  // module docstring) precisely so THE ACCEPTANCE TEST Thoth's own dispatch named — "a
  // synthetic 5-hop chain where focus at the tail lights exactly the chain and nothing
  // else" — can exercise the real algorithm directly via Node, not a string-presence proof.
  const FOCUS_DEPTH_DEFAULT = Infinity; // "until roots" — walkPath/bfsHops stop naturally
  let focusDepth = FOCUS_DEPTH_DEFAULT;
  let includeDownstream = false;
  const { outAdj: outAdjPath, inAdj: inAdjPath } = buildPathAdjacency(edges);

  // EGO RELAYOUT (TIP 1's own amendment, mail 10726): while a focus is on, the reachable set
  // is relaid out LOCALLY — focus at centre, ancestors ranked leftward by hop (roots
  // farthest left), siblings spread within their own rank; downstream (when toggled) ranked
  // rightward the same way. Spacing is fixed in SCREEN pixels, converted to world units at
  // the CURRENT zoom so the fan-out reads the same size regardless of viewSize. Temporary:
  // the real stored x/y (Khnum's own layout heartbeat) is saved before the first move and
  // restored by clearFocus or before laying out a new focus — never written back anywhere.
  const EGO_COL_SPACING_PX = 150;
  const EGO_ROW_SPACING_PX = 34;
  // review flaw #2: "until roots" with no cap let a real hub (repo:osiris, degree 20,560)
  // reach 20,266 nodes in 3.1s and light the whole graph -- not a lens any more. Rank-capped
  // now: the walk still goes to genuine roots for an ordinary object, but never surfaces
  // more than this many nodes for one direction, so a hub focus stays a legible tree.
  const MAX_EGO_NODES = 300;
  let egoSaved = null; // Map<id, {x,y}> of positions the active relayout overwrote
  // THE ONE-HOP NEIGHBOURHOOD FIX (operator ruling, grounds 5b37d219, Thoth mail 11272):
  // measured defect -- focus walked PATH_EDGE_TYPES only, so focusing a real agent
  // (Sekhmet, degree 402) reached 43 nodes over succeeded_from/succeeds_seat and nothing
  // else; the operator saw a wall of same-named labels and never what the agent actually
  // did. focusBasePathReachable is the ORIGINAL provenance-path walk's own reachable set
  // (upstream/downstream over PATH_EDGE_TYPES, unchanged); the one-hop-all-types
  // neighbourhood is additive on top of it, grouped per (type, direction) into a paged
  // count node when a bucket exceeds DRILL_PAGE_SIZE, added as real objects otherwise.
  let focusBasePathReachable = new Set();
  let focusHopsUp = new Map(), focusHopsDown = new Map();
  function bfsHops(adj, startId, depth) {
    const hops = new Map([[startId, 0]]);
    let frontier = [startId];
    for (let d = 1; d <= depth && frontier.length && hops.size < MAX_EGO_NODES; d++) {
      const next = [];
      for (const cur of frontier) {
        for (const t of adj.get(cur) || []) {
          if (hops.size >= MAX_EGO_NODES) break;
          if (!hops.has(t)) { hops.set(t, d); next.push(t); }
        }
      }
      frontier = next;
    }
    return hops;
  }
  function restoreEgoLayout() {
    if (!egoSaved) return;
    for (const [id, pos] of egoSaved) {
      const nd = idById.get(id);
      if (nd) { nd.x = pos.x; nd.y = pos.y; }
    }
    egoSaved = null;
  }
  function applyEgoLayout(focusId, hopsUp, hopsDown, extraSeed) {
    restoreEgoLayout(); // a fresh focus always starts from the real stored positions
    const idx = idById;
    const focusNode = idx.get(focusId);
    if (!focusNode) return;
    const cx = focusNode.x || 0, cy = focusNode.y || 0;
    // review flaw #6 (TIP 1c, Thoth mail 10891): using the CURRENT (pre-focus) worldPerPx
    // made the ego layout's own scale track whatever zoom the camera happened to be at --
    // a small reachable set following another tight focus could spiral the fit down to a
    // near-empty viewSize, where the 48px screen CAP then dominates the whole frame.
    // maxViewSize (the whole graph's own fitted scale, stable since fitToNodes) gives a
    // reference wpp that never shrinks just because the camera was already zoomed in.
    const wpp = maxViewSize / wrap.clientHeight;
    const colW = EGO_COL_SPACING_PX * wpp, rowH = EGO_ROW_SPACING_PX * wpp;
    const chainW = CHAIN_SPACING_PX * wpp;
    egoSaved = new Map();
    const byRank = new Map(); // signed hop (-left/+right) -> [ids]
    for (const [id, hop] of hopsUp) {
      if (id === focusId || hop === 0) continue;
      (byRank.get(-hop) || (byRank.set(-hop, []), byRank.get(-hop))).push(id);
    }
    for (const [id, hop] of hopsDown) {
      if (id === focusId || hop === 0) continue;
      (byRank.get(hop) || (byRank.set(hop, []), byRank.get(hop))).push(id);
    }
    // SUCCESSION CHAIN COLUMN COMPRESSION (mail 11272 items 2/4): live-verified root cause
    // of "the camera does not refit to something legible" for a long-lineage agent -- a
    // pure single-file succession run (rank K has exactly one member, connected to rank
    // K-1's own single member by a succession edge) used to pay the FULL EGO_COL_SPACING_PX
    // every hop, same as any unrelated provenance hop. A real 43-generation Sekhmet chain
    // measured a 544,433-world-unit span from that alone. Adjacent succession-only ranks
    // now use the same tight CHAIN_SPACING_PX the one-hop groups use; a branching or
    // mixed-type rank still gets the normal column width.
    const successionAdj = new Map(); // id -> Set of ids reachable by one succession edge
    for (const e of edges) {
      if (!SUCCESSION_EDGE_TYPES.has(e.type)) continue;
      (successionAdj.get(e.source) || (successionAdj.set(e.source, new Set()), successionAdj.get(e.source))).add(e.target);
      (successionAdj.get(e.target) || (successionAdj.set(e.target, new Set()), successionAdj.get(e.target))).add(e.source);
    }
    const colX = new Map([[0, cx]]);
    const chainRankIds = new Set(); // rank members whose column used the tight chain width
    for (const [side, cmp] of [[-1, (a, b) => b - a], [1, (a, b) => a - b]]) {
      const ranks = [...byRank.keys()].filter((r) => Math.sign(r) === side).sort(cmp);
      let x = cx, prevSingle = focusId;
      for (const r of ranks) {
        const ids = byRank.get(r);
        const isChainStep = ids.length === 1 && prevSingle && successionAdj.get(ids[0])?.has(prevSingle);
        x += side * (isChainStep ? chainW : colW);
        colX.set(r, x);
        if (isChainStep) chainRankIds.add(ids[0]);
        prevSingle = ids.length === 1 ? ids[0] : null;
      }
    }
    const seed = new Map([[focusId, { x: cx, y: cy }]]);
    for (const [signedHop, ids] of byRank) {
      const x = colX.get(signedHop);
      ids.sort(); // deterministic, not otherwise meaningful
      ids.forEach((id, i) => {
        const nd = idx.get(id);
        if (!nd) return;
        egoSaved.set(id, { x: nd.x, y: nd.y });
        seed.set(id, { x, y: cy + (i - (ids.length - 1) / 2) * rowH });
      });
    }
    // ONE-HOP NEIGHBOURHOOD (mail 11272 item 1): real neighbour objects a (type, direction)
    // bucket was small enough to place directly, seeded radially around the hub by
    // buildEgoGroups -- merged into the SAME seed/relax pass so real edges between them and
    // the path-ranked members still pull toward each other, not just toward the hub. A node
    // already placed by the path walk keeps its ranked-column seed; the one-hop walk never
    // fights it.
    // pinned ids (SUCCESSION CHAIN LAYOUT, mail 11272 item 4): a chain member's own
    // position is a deliberate, ordered-by-generation placement, not a physics seed --
    // exempted from repulsion/springs entirely, or the SAME O(n^2) spread that unfolds a
    // wide rank into a fan would just as happily unfold a 50-member chain back into the
    // "40-wide row of labels" this layout exists to prevent.
    const fixedIds = new Set([focusId, ...chainRankIds]);
    if (extraSeed) {
      for (const [id, p] of extraSeed) {
        if (seed.has(id)) continue;
        const nd = idx.get(id);
        if (!nd) continue;
        egoSaved.set(id, { x: nd.x, y: nd.y });
        seed.set(id, p);
        if (p.pinned) fixedIds.add(id);
      }
    }
    // THE DRILL, item 6: relax the rank layout's own output -- a wide rank (many siblings
    // at the same hop) used to stack in one straight column, reading as a solid bar/disc
    // once density-not-discs made every one of them a real point; the physics step spreads
    // them apart while real edges among the reachable set still pull related nodes toward
    // each other.
    const springs = [];
    for (const e of edges) {
      if (seed.has(e.source) && seed.has(e.target)) springs.push([e.source, e.target]);
    }
    const relaxed = relaxedOrSeed(seed, relaxPositions(seed, springs, fixedIds));
    for (const [id, p] of relaxed) {
      if (id === focusId) continue;
      const nd = idx.get(id);
      if (nd) { nd.x = p.x; nd.y = p.y; }
    }
  }

  // THE ONE-HOP NEIGHBOURHOOD (mail 11272 item 1): "focus = the clicked object plus its
  // ONE-HOP neighbourhood over ALL link types, both directions ... container-class
  // neighbours appear as one anchor each." Groups every real one-hop neighbour by
  // (edge type, direction relative to id) -- a container-scale neighbour (isContainerFocus
  // of its own) gets pulled out separately, one anchor each, never grouped into a bucket.
  function oneHopByTypeDirection(id) {
    const byKey = new Map(); // "type|direction" -> Map<id, nd>
    const containerNeighbors = new Map(); // id -> nd
    for (const e of edges) {
      let otherId, direction;
      if (e.source === id && e.target !== id) { otherId = e.target; direction = "out"; }
      else if (e.target === id && e.source !== id) { otherId = e.source; direction = "in"; }
      else continue;
      const nd = idById.get(otherId);
      if (!nd) continue;
      if (isContainerFocus(otherId)) { containerNeighbors.set(otherId, nd); continue; }
      const key = `${e.type}|${direction}`;
      let m = byKey.get(key);
      if (!m) { m = new Map(); byKey.set(key, m); }
      m.set(otherId, nd);
    }
    const groups = new Map();
    for (const [key, m] of byKey) groups.set(key, [...m.values()]);
    return { groups, containerNeighbors: [...containerNeighbors.values()] };
  }

  // SUCCESSION CHAIN LAYOUT (mail 11272 item 4): "a succession chain renders as a chain
  // (ordered by generation, spaced by pixels), never overlapping." Live-verified without
  // this: focusing a real 402-degree agent's "spawned_by (in)" bucket spread 381
  // same-named lineage members via generic repulsion into one wide horizontal smear --
  // exactly the "40-wide row of labels" the operator's own report described. A succession
  // edge type gets a real linear order (BFS outward from the focus over ONLY that edge
  // type, among this bucket's own members) instead of a radial fan; distance from focus
  // doubles as generation.
  const SUCCESSION_EDGE_TYPES = new Set(["succeeded_from", "succeeds_seat"]);
  function orderSuccessionChain(focusId, members, edgeType) {
    const memberIds = new Set(members.map((m) => m.id));
    const universe = new Set([focusId, ...memberIds]);
    const adj = new Map();
    for (const e of edges) {
      if (e.type !== edgeType) continue;
      if (!universe.has(e.source) || !universe.has(e.target)) continue;
      (adj.get(e.source) || (adj.set(e.source, []), adj.get(e.source))).push(e.target);
      (adj.get(e.target) || (adj.set(e.target, []), adj.get(e.target))).push(e.source);
    }
    const dist = new Map([[focusId, 0]]);
    const queue = [focusId];
    for (let qi = 0; qi < queue.length; qi++) {
      const cur = queue[qi];
      for (const n of adj.get(cur) || []) {
        if (dist.has(n)) continue;
        dist.set(n, dist.get(cur) + 1);
        queue.push(n);
      }
    }
    // members the chain walk never reached (a disconnected outlier within the same edge
    // type/direction bucket) still need a slot -- appended past the real chain, sorted by
    // degree so at least the ordering stays deterministic.
    const ordered = members.filter((m) => dist.has(m.id))
      .sort((a, b) => dist.get(a.id) - dist.get(b.id));
    const unreached = members.filter((m) => !dist.has(m.id))
      .sort((a, b) => (b.degree || 0) - (a.degree || 0) || (a.id < b.id ? -1 : 1));
    return [...ordered, ...unreached].map((nd) => ({ nd, generation: dist.get(nd.id) ?? null }));
  }

  let egoGroupFocusId = null; // which focus these groups belong to
  let egoGroupExpandedKey = null; // "type|direction" currently paged open, or null
  let egoGroupPageCount = 1;
  let egoGroupEntries = []; // [{key, kind:'group'|'more', type, direction, count, x, y, div}]
  let egoContainerAnchorEntries = []; // [{id, label, x, y, div}]
  function disposeEgoGroupDivs() {
    for (const e of egoGroupEntries) e.div.remove();
    egoGroupEntries = [];
  }
  function disposeEgoContainerAnchors() {
    for (const e of egoContainerAnchorEntries) e.div.remove();
    egoContainerAnchorEntries = [];
  }
  function clearEgoGroupState() {
    disposeEgoGroupDivs();
    disposeEgoContainerAnchors();
    egoGroupFocusId = null;
    egoGroupExpandedKey = null;
    egoGroupPageCount = 1;
  }
  function buildEgoGroupDivs() {
    for (const entry of egoGroupEntries) {
      const div = document.createElement("div");
      div.className = "lod-glyph-label ego-drill-label";
      div.style.cursor = "pointer";
      div.textContent = entry.kind === "more"
        ? `+${entry.count} more ${entry.type} (${entry.direction})`
        : `${entry.type} (${entry.direction}) ${entry.count}`;
      div.addEventListener("click", (ev) => {
        ev.stopPropagation();
        if (entry.kind === "more") { egoGroupPageCount++; } else { egoGroupExpandedKey = entry.key; egoGroupPageCount = 1; }
        renderFocusEgoGroups(pathFocusId, focusHopsUp, focusHopsDown);
      });
      labelsEl.appendChild(div);
      entry.div = div;
    }
  }
  function buildEgoContainerAnchorDivs() {
    for (const entry of egoContainerAnchorEntries) {
      const div = document.createElement("div");
      div.className = "lod-glyph-label ego-drill-label";
      div.style.cursor = "pointer";
      div.textContent = entry.label || entry.id.slice(0, 8);
      div.addEventListener("click", (ev) => { ev.stopPropagation(); focusObject(entry.id); });
      labelsEl.appendChild(div);
      entry.div = div;
    }
  }
  function positionEgoGroups() {
    for (const entry of egoGroupEntries) {
      if (!entry.div) continue;
      _screenV.set(entry.x, entry.y, 0).project(camera);
      entry.div.style.left = `${(_screenV.x * 0.5 + 0.5) * wrap.clientWidth}px`;
      entry.div.style.top = `${(-_screenV.y * 0.5 + 0.5) * wrap.clientHeight}px`;
    }
    for (const entry of egoContainerAnchorEntries) {
      if (!entry.div) continue;
      _screenV.set(entry.x, entry.y, 0).project(camera);
      entry.div.style.left = `${(_screenV.x * 0.5 + 0.5) * wrap.clientWidth}px`;
      entry.div.style.top = `${(-_screenV.y * 0.5 + 0.5) * wrap.clientHeight}px`;
    }
  }
  // computes this focus's own one-hop groups/anchors and returns the real member ids to
  // seed into applyEgoLayout's own relax pass -- small buckets (<= DRILL_PAGE_SIZE, and
  // budget-permitting) place directly; a bucket over the page size (or one that would blow
  // the MAX_EGO_NODES budget) becomes a paged count node instead, same mechanic THE DRILL's
  // own container buckets use.
  const CHAIN_SPACING_PX = 22; // tighter than EGO_ROW_SPACING_PX (34) -- lineage siblings, not the ranked tree
  function buildEgoGroups(id, hub) {
    disposeEgoGroupDivs();
    disposeEgoContainerAnchors();
    const { groups, containerNeighbors } = oneHopByTypeDirection(id);
    const cx = hub.x || 0, cy = hub.y || 0;
    const wpp = maxViewSize / wrap.clientHeight;
    const ringR = EGO_COL_SPACING_PX * wpp * 1.6;
    const chainStep = CHAIN_SPACING_PX * wpp;
    const extraSeed = new Map(); // id -> {x,y,pinned?}
    const keys = [...groups.keys()].sort();
    const slotCount = keys.length + containerNeighbors.length;
    const angleStep = slotCount ? (2 * Math.PI / slotCount) : 0;
    let slot = 0;
    for (const key of keys) {
      // THE NO-OP EXPANSION FIX (Thoth mail 11359): oneHopByTypeDirection walks ALL edges
      // touching id, including ones the ORIGINAL PATH_EDGE_TYPES walk already reached (a
      // real Thread's own "possible_upstream|out" one-hop bucket can be entirely a subset
      // of focusBasePathReachable) -- offering, and letting a reader page open, a group
      // that adds zero new nodes to the reachable set is a dead click. Filter to members
      // not already reachable; a group left with none is never offered at all.
      const members = groups.get(key).filter((nd) => !focusBasePathReachable.has(nd.id));
      if (members.length === 0) continue;
      const [type, direction] = key.split("|");
      const angle = slot * angleStep; slot++;
      const tx = cx + Math.cos(angle) * ringR, ty = cy + Math.sin(angle) * ringR;
      const isChain = SUCCESSION_EDGE_TYPES.has(type);
      const dirX = Math.cos(angle), dirY = Math.sin(angle);
      const budgetLeft = MAX_EGO_NODES - focusBasePathReachable.size - extraSeed.size;
      if (key === egoGroupExpandedKey) {
        const ranked = isChain
          ? orderSuccessionChain(id, members, type).map((c) => c.nd)
          : members.slice().sort((a, b) => (b.degree || 0) - (a.degree || 0) || (a.id < b.id ? -1 : 1));
        const take = Math.min(ranked.length, DRILL_PAGE_SIZE * egoGroupPageCount, Math.max(0, budgetLeft));
        for (let k = 0; k < take; k++) {
          const nd = ranked[k];
          if (isChain) {
            const d = ringR + (k + 1) * chainStep;
            extraSeed.set(nd.id, { x: cx + dirX * d, y: cy + dirY * d, pinned: true });
          } else {
            // THE PHYSICS DIVERGENCE FIX (Thoth mail 11308): a page angle step of 0.08 rad
            // wraps past a full 2*PI revolution once `take` (DRILL_PAGE_SIZE *
            // egoGroupPageCount, unbounded by repeated "more" clicks) exceeds ~79 -- at a
            // CONSTANT radius that puts two genuinely different members at the exact same
            // seed (x,y). A small per-index radius growth (a spiral, not a circle) makes
            // that structurally impossible regardless of how many pages are open.
            const a2 = angle + (k - (take - 1) / 2) * 0.08;
            const r2 = ringR * 1.3 + k * 2;
            extraSeed.set(nd.id, { x: cx + Math.cos(a2) * r2, y: cy + Math.sin(a2) * r2 });
          }
        }
        if (ranked.length > take) {
          egoGroupEntries.push({ key: `more:${key}`, kind: "more", type, direction, count: ranked.length - take, x: tx, y: ty, div: null });
        }
        continue;
      }
      if (members.length <= DRILL_PAGE_SIZE && members.length <= budgetLeft) {
        if (isChain) {
          const chain = orderSuccessionChain(id, members, type);
          chain.forEach(({ nd }, i) => {
            const d = ringR + (i + 1) * chainStep;
            extraSeed.set(nd.id, { x: cx + dirX * d, y: cy + dirY * d, pinned: true });
          });
        } else {
          members.forEach((nd, i) => {
            const a2 = angle + (i - (members.length - 1) / 2) * 0.1;
            const r2 = ringR + i * 2; // same spiral defence as the expanded-page branch
            extraSeed.set(nd.id, { x: cx + Math.cos(a2) * r2, y: cy + Math.sin(a2) * r2 });
          });
        }
        continue;
      }
      egoGroupEntries.push({ key, kind: "group", type, direction, count: members.length, x: tx, y: ty, div: null });
    }
    containerNeighbors.forEach((nd, i) => {
      const angle = (keys.length + i) * angleStep;
      const tx = cx + Math.cos(angle) * ringR, ty = cy + Math.sin(angle) * ringR;
      egoContainerAnchorEntries.push({ id: nd.id, label: nd.label, x: tx, y: ty, div: null });
    });
    buildEgoGroupDivs();
    buildEgoContainerAnchorDivs();
    return extraSeed;
  }
  // the shared render path for BOTH the initial focus and any group/"more" click after it
  // -- never resets egoGroupExpandedKey/egoGroupPageCount itself (the caller, focusObject
  // or a click handler, decides that), the exact bug THE DRILL's own clearDrillState hit
  // (mail 11241) if this had reset unconditionally instead.
  function renderFocusEgoGroups(id, hopsUp, hopsDown) {
    const hub = idById.get(id);
    if (!hub) return;
    const extraSeed = buildEgoGroups(id, hub);
    pathReachable = new Set([...focusBasePathReachable, ...extraSeed.keys()]);
    // THE STALE TABLE FIX (Thoth mail 11308): onFocus (console.js's own onSpaceFocus,
    // wired through to renderEntityExplorerStage/hydrateFocusReachable) used to fire only
    // from focusObject's own INITIAL call -- a group/"more" click re-renders through this
    // shared path directly, never notifying the table that pathReachable just grew, so it
    // stayed at the ORIGINAL row count after an expansion. Every call here re-notifies,
    // same id or not.
    if (onFocus) onFocus(id);
    applyEgoLayout(id, hopsUp, hopsDown, extraSeed);
    syncMovedInstancePositions(egoSaved ? new Set(egoSaved.keys()) : null);
    // zoom-to-fit: frame the camera around exactly the reachable set's own (now relaid-out)
    // bounding box -- every focus (and every group expand within it) refits, per mail
    // 11272 item 2 ("the operator reports it does not").
    let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
    for (const rid of pathReachable) {
      const nd = idById.get(rid);
      if (!nd || nd.x == null || nd.y == null) continue;
      minX = Math.min(minX, nd.x); maxX = Math.max(maxX, nd.x);
      minY = Math.min(minY, nd.y); maxY = Math.max(maxY, nd.y);
    }
    if (Number.isFinite(minX)) {
      camera.position.x = (minX + maxX) / 2;
      camera.position.y = (minY + maxY) / 2;
      const span = Math.max(maxX - minX, maxY - minY, 0);
      const EGO_FIT_MIN_VIEWSIZE = 400;
      viewSize = Math.max(EGO_FIT_MIN_VIEWSIZE, Math.min(maxViewSize, span * 1.6 + 40));
      updateFrustum();
      rescaleForZoom();
    }
    applyDim();
    buildEdgeLines(idToNode, edges); // review flaw #1: base layer must hide too, see clearFocus
    updatePathEdges();
    buildProjectAnchors(id);
    positionEgoGroups();
    setStatus(`focused: ${pathReachable.size} reachable` +
      (includeDownstream ? " (upstream+downstream)" : " (upstream)"));
    scheduleLabelPick();
    markDirty();
  }

  // THE DRILL (ruling d7d55257, Thoth mail 11048, item 6): "physics on the visible set
  // only: a small force step (repulsion + springs, a few hundred iterations, seeded) over
  // the expanded nodes, nothing else moves." `seed` is a Map<id,{x,y}> of STARTING
  // positions (the rank layout's own output, or a simple radial scatter for the drill) --
  // a good seed matters far more than iteration count for this to converge quickly and
  // legibly; `edges` is a list of [aId, bId] pairs to spring together (real reachable-set
  // edges for an ego tree, synthetic hub-to-child pairs for a drill's star topology).
  // `fixedId` (usually the focus/hub) never moves. O(n^2) repulsion is fine at this scale
  // -- the whole point of THE DRILL and the ego cap is that n never exceeds MAX_EGO_NODES.
  const EGO_FORCE_ITERATIONS = 180;
  const EGO_REPULSION = 3200;
  const EGO_SPRING = 0.02;
  // THE PHYSICS DIVERGENCE FIX (Thoth mail 11308, w306 review BLOCKER): live-verified --
  // focusing a real 223-degree Thread (177 reachable, 154 Message) or 125-degree Decision
  // (213 reachable, 211 Message) left camera.position at 3.6e83 / -1.0e85, canvas black.
  // Root cause: buildEgoGroups' own small-bucket seeding fans members by angle alone at a
  // CONSTANT radius (`a2 = angle + i * step`) -- once a bucket's own member count pushes
  // the total angular spread past 2*PI (154 members * a 0.1 rad step ~= 15.4 rad, 2.4 full
  // turns), cos/sin periodicity puts genuinely DIFFERENT members at the EXACT SAME (x,y).
  // The old `d2 = max(d2, 1)` clamp bounds any ONE pair's force, but dozens of exactly-
  // coincident pairs at one point still sum to an enormous single-iteration displacement,
  // and 180 iterations of that compounds into non-finite territory. Three independent
  // guards, not just one, since seeding is only ONE of several places a coincidence or a
  // runaway sum could originate: a real minimum-separation floor (not 1 world unit -- big
  // enough that even a full pile-up sums to a bounded force), a hard per-iteration
  // displacement cap (so a raw force spike can never move a point further than a fraction
  // of the graph's own real scale in one step regardless of how many neighbours pile onto
  // it), and a finite-position assertion at the caller (renderFocusEgoGroups/
  // renderContainerDrill) with a fallback to the pre-relax seed.
  const EGO_MIN_SEP2 = 400; // d2 floor -- max single-pair force EGO_REPULSION/400 = 8
  const EGO_MAX_DISPLACEMENT = 400; // per node, per iteration, in world units
  function relaxPositions(seed, springs, fixedId) {
    const ids = [...seed.keys()];
    if (ids.length < 2) return seed;
    const pos = new Map(ids.map((id) => [id, { x: seed.get(id).x, y: seed.get(id).y }]));
    for (let iter = 0; iter < EGO_FORCE_ITERATIONS; iter++) {
      const force = new Map(ids.map((id) => [id, { x: 0, y: 0 }]));
      for (let i = 0; i < ids.length; i++) {
        const a = pos.get(ids[i]);
        for (let j = i + 1; j < ids.length; j++) {
          const b = pos.get(ids[j]);
          let dx = a.x - b.x, dy = a.y - b.y;
          let d2 = dx * dx + dy * dy;
          if (d2 < EGO_MIN_SEP2) d2 = EGO_MIN_SEP2;
          const d = Math.sqrt(d2);
          const f = EGO_REPULSION / d2;
          const fx = (dx / d) * f, fy = (dy / d) * f;
          const fa = force.get(ids[i]), fb = force.get(ids[j]);
          fa.x += fx; fa.y += fy;
          fb.x -= fx; fb.y -= fy;
        }
      }
      for (const [aId, bId] of springs) {
        const a = pos.get(aId), b = pos.get(bId);
        if (!a || !b) continue;
        const dx = b.x - a.x, dy = b.y - a.y;
        const fa = force.get(aId), fb = force.get(bId);
        if (fa) { fa.x += dx * EGO_SPRING; fa.y += dy * EGO_SPRING; }
        if (fb) { fb.x -= dx * EGO_SPRING; fb.y -= dy * EGO_SPRING; }
      }
      for (const id of ids) {
        if (id === fixedId || (fixedId instanceof Set && fixedId.has(id))) continue;
        const p = pos.get(id), f = force.get(id);
        const mag = Math.hypot(f.x, f.y);
        if (mag > EGO_MAX_DISPLACEMENT) {
          const scale = EGO_MAX_DISPLACEMENT / mag;
          f.x *= scale; f.y *= scale;
        }
        p.x += f.x; p.y += f.y;
      }
    }
    return pos;
  }
  // last-resort safety net: a non-finite position anywhere in the relaxed set means the
  // physics genuinely diverged (a real bug, not something to paper over silently) -- the
  // caller falls back to the pre-relax seed rather than feeding NaN/Infinity into the
  // camera fit, which is what actually produced the black-canvas symptom.
  function relaxedOrSeed(seed, relaxed) {
    for (const p of relaxed.values()) {
      if (!Number.isFinite(p.x) || !Number.isFinite(p.y)) return seed;
    }
    return relaxed;
  }

  // pushes the (few) moved nodes' new positions into the GPU buffers directly — never a
  // full buildScene rebuild, so this stays well inside the 100ms budget below regardless of
  // total graph size (cost is O(moved), not O(49k)).
  function syncMovedInstancePositions(movedIds) {
    if (!mesh || !movedIds || !movedIds.size) return;
    const dummy = new THREE.Object3D();
    let touched = false;
    for (let i = 0; i < idToNode.length; i++) {
      const nd = idToNode[i];
      if (!movedIds.has(nd.id)) continue;
      dummy.position.set(nd.x || 0, nd.y || 0, (nd.radiusPx || 0) * 0.002);
      dummy.scale.setScalar(1);
      dummy.updateMatrix();
      mesh.setMatrixAt(i, dummy.matrix);
      pickMesh.setMatrixAt(i, dummy.matrix);
      touched = true;
    }
    if (touched) { mesh.instanceMatrix.needsUpdate = true; pickMesh.instanceMatrix.needsUpdate = true; }
  }

  // ---- THE DRILL (operator ruling d7d55257, Thoth mail 11048) ----------------------------
  // A CONTAINER is any object whose own structural (containment/membership) degree exceeds
  // MAX_EGO_NODES -- a project, an agent with a huge working set, any hub the ordinary ego
  // walk could never show in full. Khnum's own `container` value in link_type_class landed
  // (Thoth mail 11291) and is normalized into "structural" at decode time (fetchStreamSnapshot's
  // own edgeClassByType) -- the exact set every other container-shaped check in this file
  // already uses. Focusing a container
  // is a DRILL, not the ordinary ego tree: the hub plus one count node per member type,
  // sorted by count, real members hidden until a reader clicks a type open. Acceptance:
  // "focusing repo:osiris opens under 30 nodes" -- confirmed live, see the tip's own commit.
  function containerMembersByType(id) {
    const byType = new Map(); // type -> nd[]
    for (const e of edges) {
      if (!isStructuralLike(e.edgeClass)) continue;
      const other = e.source === id ? e.target : e.target === id ? e.source : null;
      if (other == null) continue;
      const nd = idById.get(other);
      if (!nd) continue;
      (byType.get(nd.type) || (byType.set(nd.type, []), byType.get(nd.type))).push(nd);
    }
    return byType;
  }
  // THE MEMBERSHIP-CLASS FIX (Thoth mail 11359): a high structural degree alone is not a
  // container -- since spawned_by went structural (mail 11291), a busy Agent seat's own
  // structural degree can exceed MAX_EGO_NODES the same way a real project's membership
  // degree does, and the drill wrongly ate the whole focus (three count stubs, "0 shown",
  // none of the agent's own succession/messages visible). Only genuine membership-container
  // types ever take the drill; everything else, however high its structural degree, goes
  // through the ordinary one-hop ego groups (which already page a huge bucket).
  const CONTAINER_FOCUS_TYPES = new Set(["SoftwareProject", "Seat"]);
  function isContainerFocus(id) {
    const nd = idById.get(id);
    if (!nd || !CONTAINER_FOCUS_TYPES.has(nd.type)) return false;
    let n = 0;
    for (const e of edges) {
      if (!isStructuralLike(e.edgeClass)) continue;
      if (e.source === id || e.target === id) { n++; if (n > MAX_EGO_NODES) return true; }
    }
    return false;
  }

  let drillContainerId = null;
  let drillMembersByType = null;
  let drillExpandedType = null; // the one type currently paged open, or null
  let drillPageCount = 1; // pages of DRILL_PAGE_SIZE shown for drillExpandedType
  const DRILL_PAGE_SIZE = 50;
  let drillNodeEntries = []; // [{key, kind:'type'|'more', type, count, x, y, div}]
  function disposeDrillDivs() {
    for (const e of drillNodeEntries) e.div.remove();
    drillNodeEntries = [];
  }
  function clearDrillState() {
    disposeDrillDivs();
    drillContainerId = null;
    drillMembersByType = null;
    drillExpandedType = null;
    drillPageCount = 1;
  }
  function buildDrillDivs() {
    for (const entry of drillNodeEntries) {
      const div = document.createElement("div");
      div.className = "lod-glyph-label ego-drill-label";
      div.style.cursor = "pointer";
      div.textContent = entry.kind === "more"
        ? `+${entry.count} more ${entry.type}`
        : `${entry.type} ${entry.count}`;
      div.addEventListener("click", (ev) => {
        ev.stopPropagation();
        if (entry.kind === "more") { drillPageCount++; } else { drillExpandedType = entry.type; drillPageCount = 1; }
        renderContainerDrill(drillContainerId, { skipStackPush: true });
      });
      labelsEl.appendChild(div);
      entry.div = div;
    }
  }
  function positionDrillDivs() {
    for (const entry of drillNodeEntries) {
      if (!entry.div) continue;
      _screenV.set(entry.x, entry.y, 0).project(camera);
      entry.div.style.left = `${(_screenV.x * 0.5 + 0.5) * wrap.clientWidth}px`;
      entry.div.style.top = `${(-_screenV.y * 0.5 + 0.5) * wrap.clientHeight}px`;
    }
  }
  // "top 50 by degree then recency" -- recency isn't on the wire (fetchStreamSnapshot's own
  // node shape carries no timestamp), so degree desc with a stable id tiebreak stands in
  // until a real recency field exists to sort by; noted rather than faked.
  async function renderContainerDrill(id, opts) {
    const t0 = performance.now();
    const options = opts || {};
    const restored = egoSaved ? new Set(egoSaved.keys()) : null;
    restoreEgoLayout();
    if (restored) syncMovedInstancePositions(restored);
    disposeProjectAnchors(); // a drill replaces the normal focus view entirely
    // THE DRILL EXPANSION FIX (Thoth mail 11241, live review of w299): clearDrillState()
    // used to run unconditionally on every call here, wiping drillExpandedType/
    // drillPageCount the SAME turn a type/"more" click had just set them (buildDrillDivs'
    // own click handler sets one then calls straight back into this function) -- expanding
    // a type or paging "more" always looked like nothing happened, because the state that
    // was supposed to drive the new render was destroyed before this function ever read
    // it. Only reset the expand state when the container itself is actually changing; a
    // same-container re-render (the expand/page click's own path) just needs its stale
    // divs disposed, not its just-set intent wiped.
    if (drillContainerId !== id) clearDrillState();
    else disposeDrillDivs();
    selectedId = id;
    pathFocusId = id;
    drillContainerId = id;
    drillMembersByType = containerMembersByType(id);
    if (!options.skipStackPush) pushFocusStack(id);
    if (onFocus) onFocus(id);

    const hub = idById.get(id);
    const cx = hub.x || 0, cy = hub.y || 0;
    const wpp = maxViewSize / wrap.clientHeight;
    const ringR = EGO_COL_SPACING_PX * wpp;

    pathReachable = new Set([id]);
    egoSaved = new Map();
    const seed = new Map([[id, { x: cx, y: cy }]]);
    const springs = [];
    const typeEntries = [...drillMembersByType.entries()].sort((a, b) => b[1].length - a[1].length);
    const angleStep = typeEntries.length ? (2 * Math.PI / typeEntries.length) : 0;

    typeEntries.forEach(([type, members], i) => {
      const angle = i * angleStep;
      const tx = cx + Math.cos(angle) * ringR, ty = cy + Math.sin(angle) * ringR;
      if (type === drillExpandedType) {
        const ranked = members.slice().sort((a, b) =>
          (b.degree || 0) - (a.degree || 0) || (a.id < b.id ? -1 : 1));
        const budget = MAX_EGO_NODES - pathReachable.size;
        const take = Math.min(ranked.length, DRILL_PAGE_SIZE * drillPageCount, Math.max(0, budget));
        for (let k = 0; k < take; k++) {
          const nd = ranked[k];
          pathReachable.add(nd.id);
          egoSaved.set(nd.id, { x: nd.x, y: nd.y });
          const a2 = angle + (k - (take - 1) / 2) * 0.12;
          seed.set(nd.id, { x: cx + Math.cos(a2) * ringR * 1.8, y: cy + Math.sin(a2) * ringR * 1.8 });
          springs.push([id, nd.id]);
        }
        if (ranked.length > take) {
          const key = `more:${type}`;
          seed.set(key, { x: tx, y: ty });
          springs.push([id, key]);
          drillNodeEntries.push({ key, kind: "more", type, count: ranked.length - take, x: tx, y: ty, div: null });
        }
      } else {
        const key = `type:${type}`;
        seed.set(key, { x: tx, y: ty });
        springs.push([id, key]);
        drillNodeEntries.push({ key, kind: "type", type, count: members.length, x: tx, y: ty, div: null });
      }
    });

    const relaxed = relaxedOrSeed(seed, relaxPositions(seed, springs, id));
    for (const [key, p] of relaxed) {
      if (key === id) continue;
      const nd = idById.get(key);
      if (nd) { nd.x = p.x; nd.y = p.y; continue; }
      const entry = drillNodeEntries.find((e) => e.key === key);
      if (entry) { entry.x = p.x; entry.y = p.y; }
    }
    syncMovedInstancePositions(new Set(egoSaved.keys()));
    buildDrillDivs();

    let minX = cx, maxX = cx, minY = cy, maxY = cy;
    for (const rid of pathReachable) {
      const nd = idById.get(rid);
      if (!nd) continue;
      minX = Math.min(minX, nd.x); maxX = Math.max(maxX, nd.x);
      minY = Math.min(minY, nd.y); maxY = Math.max(maxY, nd.y);
    }
    for (const e of drillNodeEntries) {
      minX = Math.min(minX, e.x); maxX = Math.max(maxX, e.x);
      minY = Math.min(minY, e.y); maxY = Math.max(maxY, e.y);
    }
    camera.position.x = (minX + maxX) / 2;
    camera.position.y = (minY + maxY) / 2;
    const span = Math.max(maxX - minX, maxY - minY, 0);
    const EGO_FIT_MIN_VIEWSIZE = 400;
    viewSize = Math.max(EGO_FIT_MIN_VIEWSIZE, Math.min(maxViewSize, span * 1.6 + 40));
    updateFrustum();
    rescaleForZoom();

    applyDim();
    buildEdgeLines(idToNode, edges);
    updatePathEdges();
    setStatus(`container: ${drillMembersByType.size} member types, ` +
      `${pathReachable.size - 1} shown`);
    scheduleLabelPick();
    markDirty();
    if (window.__spaceDebugTiming) console.debug("renderContainerDrill sync ms:", performance.now() - t0);
    await inspect(id);
  }

  // WAVE 26, THE STORYLINE (Thoth mail 11534, operator's word "keep cooking, everyone
  // gets a lane"; ruling 1178e7d9's fourth principle -- lineages are time; held thread
  // 3683a12a): focusing an Agent lays its own succession chain (succeeded_from/
  // succeeds_seat, both directions from the focus, up to 100+ generations) out on a real
  // horizontal TIME axis instead of the ordinary ranked-column ego tree -- x from each
  // body's own `createdAt` (now on the wire, graph_stream.py item 11), one row for the
  // chain itself, a sub-agent (spawned_by a chain member, but not itself IN the chain --
  // a fork, not a successor) hangs as a short branch off its own parent's row at its own
  // spawn time, and a Decision/Thread `recorded_by` a chain member or sub-agent sits as a
  // tick directly ON that body's own row at its own time -- literally a ruler tick, the
  // node's own ontology color already distinguishing it from an Agent body without any
  // separate styling. "Nothing hidden" (this WAVE's own standing rule, carried over from
  // THE DRAWING TIP): every chain member, every sub-agent, every tick is positioned and
  // drawn, none paged/capped by count -- MAX_STORYLINE_NODES below is a crash-guard
  // against a pathological/cyclic graph, never a designed display budget.
  function isAgentFocus(id) {
    const nd = idById.get(id);
    return !!nd && nd.type === "Agent";
  }
  const MAX_STORYLINE_NODES = 4000;
  const STORYLINE_LINE_TYPES = new Set(["succeeded_from", "succeeds_seat", "spawned_by"]);
  const STORYLINE_AXIS_WIDTH_PX = 3600;
  const STORYLINE_ROW_OFFSET_PX = 90;
  const STORYLINE_SUBROW_STEP_PX = 26;
  const STORYLINE_AXIS_TICK_COUNT = 6;
  // WAVE 27, THE SPAWN ROW (Thoth mail 11754, her w313 review note): a real burst-spawned
  // parent (50 sub-agents minted within the same short window) collapsed onto a HANDFUL of
  // rows once the old tier count wrapped modulo a fixed cap (5) -- two siblings 10 apart in
  // spawn order landed on the exact same (dir, tier), and with near-identical createdAt too,
  // the exact same (x, y). No amount of label decluttering can separate two coincident
  // points. FIRST ATTEMPT (live-caught regression, kept here as a warning): uncapping the
  // tier LINEARLY (tier * STEP_PX) fixes the collision but a real fleet burst (measured
  // live: one parent, 1188 siblings) then explodes the vertical extent to +-12,000 world
  // units, zooming the WHOLE storyline down to a handful of visible pixels -- "0 label
  // overlaps" only because nothing is legible. The offset below grows with sqrt(tier)
  // instead: still strictly monotonic (no two siblings of one parent ever share a
  // position -- sqrt is injective on non-negative integers), but a burst 100x bigger only
  // needs ~10x the height, not 100x. storylineMaxSubrowOffsetPx tracks the deepest offset
  // actually used this render (in raw, pre-wpp pixels) so the axis (below) clears
  // whatever extent really occurred, instead of assuming a fixed constant.
  function buildSuccessionAdjacency() {
    const adj = new Map();
    for (const e of edges) {
      if (!SUCCESSION_EDGE_TYPES.has(e.type)) continue;
      (adj.get(e.source) || (adj.set(e.source, new Set()), adj.get(e.source))).add(e.target);
      (adj.get(e.target) || (adj.set(e.target, new Set()), adj.get(e.target))).add(e.source);
    }
    return adj;
  }
  function buildStorylineChain(focusId, adj) {
    const chain = new Set([focusId]);
    let frontier = [focusId];
    while (frontier.length && chain.size < MAX_STORYLINE_NODES) {
      const next = [];
      for (const id of frontier) {
        for (const other of adj.get(id) || []) {
          if (chain.has(other)) continue;
          chain.add(other);
          next.push(other);
          if (chain.size >= MAX_STORYLINE_NODES) break;
        }
        if (chain.size >= MAX_STORYLINE_NODES) break;
      }
      frontier = next;
    }
    return chain;
  }
  // one pass each for branches (spawned_by INTO a chain member, from a non-chain-member --
  // a fork off that body) and ticks (recorded_by INTO a chain member or a sub-agent) --
  // first-writer-wins on a rare double attribution, same convention as the rest of this
  // file's grouping passes (oneHopByTypeDirection et al).
  function buildStorylineBranchesAndTicks(chainIds) {
    const subAgentOf = new Map(); // subAgentId -> parent chain-member id
    for (const e of edges) {
      if (e.type !== "spawned_by" || !chainIds.has(e.target) || chainIds.has(e.source)) continue;
      if (!subAgentOf.has(e.source)) subAgentOf.set(e.source, e.target);
    }
    const bodyIds = new Set([...chainIds, ...subAgentOf.keys()]);
    const ticksOf = new Map(); // tickId -> body id (chain member or sub-agent) it's recorded_by
    for (const e of edges) {
      if (e.type !== "recorded_by" || !bodyIds.has(e.target)) continue;
      if (!ticksOf.has(e.source)) ticksOf.set(e.source, e.target);
    }
    return { subAgentOf, ticksOf };
  }
  function disposeStorylineAxis() {
    for (const e of storylineAxisEntries) e.div.remove();
    storylineAxisEntries = [];
  }
  function clearStorylineState() {
    if (storylineLines) {
      scene.remove(storylineLines);
      storylineLines.geometry.dispose();
      storylineLines.material.dispose();
      storylineLines = null;
    }
    disposeStorylineAxis();
    storylineActive = false;
    storylineChainIds = new Set();
    storylineSubAgentOf = new Map();
    storylineTickOf = new Map();
    storylineMaxSubrowOffsetPx = 0;
  }
  // a dedicated straight-line overlay -- NOT updatePathEdges (its own cross-cluster bow/
  // bundle logic answers a different question, "how far apart are two projects", which
  // means nothing on a time axis where every body sits in the same single view).
  function buildStorylineLines() {
    if (storylineLines) {
      scene.remove(storylineLines);
      storylineLines.geometry.dispose();
      storylineLines.material.dispose();
      storylineLines = null;
    }
    const segs = [];
    for (const e of edges) {
      if (!STORYLINE_LINE_TYPES.has(e.type)) continue;
      if (!pathReachable.has(e.source) || !pathReachable.has(e.target)) continue;
      const a = idById.get(e.source), b = idById.get(e.target);
      if (!a || !b) continue;
      segs.push(a, b);
    }
    if (!segs.length) return;
    const positions = new Float32Array(segs.length * 3);
    const otherPositions = new Float32Array(segs.length * 3);
    const colors = new Float32Array(segs.length * 3);
    const ec = new THREE.Color("#58a6ff");
    for (let i = 0; i < segs.length; i += 2) {
      const a = segs[i], b = segs[i + 1];
      positions[i * 3] = a.x || 0; positions[i * 3 + 1] = a.y || 0; positions[i * 3 + 2] = -0.1;
      positions[(i + 1) * 3] = b.x || 0; positions[(i + 1) * 3 + 1] = b.y || 0; positions[(i + 1) * 3 + 2] = -0.1;
      otherPositions[i * 3] = b.x || 0; otherPositions[i * 3 + 1] = b.y || 0; otherPositions[i * 3 + 2] = -0.1;
      otherPositions[(i + 1) * 3] = a.x || 0; otherPositions[(i + 1) * 3 + 1] = a.y || 0; otherPositions[(i + 1) * 3 + 2] = -0.1;
      colors[i * 3] = ec.r; colors[i * 3 + 1] = ec.g; colors[i * 3 + 2] = ec.b;
      colors[(i + 1) * 3] = ec.r; colors[(i + 1) * 3 + 1] = ec.g; colors[(i + 1) * 3 + 2] = ec.b;
    }
    const geo = new THREE.BufferGeometry();
    geo.setAttribute("position", new THREE.BufferAttribute(positions, 3));
    geo.setAttribute("otherPosition", new THREE.BufferAttribute(otherPositions, 3));
    geo.setAttribute("color", new THREE.BufferAttribute(colors, 3));
    storylineLines = new THREE.LineSegments(geo, makeEdgeFadeMaterial());
    scene.add(storylineLines);
  }
  // "the axis carries date labels; zoom scrubs time": regenerated (not just repositioned)
  // on every deliberate zoom step (syncStorylineAxis, called from zoomAt/fitToNodes) over
  // whatever time window the camera's own current frustum actually covers -- a reader
  // zoomed into one decade of a long chain sees date labels for that decade, not the
  // whole span. Cheap to fully rebuild: STORYLINE_AXIS_TICK_COUNT+1 divs, never more.
  function buildStorylineAxis(fromT, toT) {
    disposeStorylineAxis();
    if (!Number.isFinite(fromT) || !Number.isFinite(toT) || toT <= fromT) return;
    const wpp = maxViewSize / wrap.clientHeight;
    const axisY = -((storylineMaxSubrowOffsetPx + 40) * wpp);
    for (let i = 0; i <= STORYLINE_AXIS_TICK_COUNT; i++) {
      const frac = i / STORYLINE_AXIS_TICK_COUNT;
      const t = fromT + frac * (toT - fromT);
      const x = ((t - storylineMinT) / (storylineMaxT - storylineMinT || 1)) * storylineAxisWidthWorld;
      const div = document.createElement("div");
      div.className = "lod-glyph-label storyline-axis-label";
      div.textContent = new Date(t * 1000).toISOString().slice(0, 10);
      labelsEl.appendChild(div);
      storylineAxisEntries.push({ t, x, y: axisY, div });
    }
  }
  function positionStorylineAxis() {
    for (const e of storylineAxisEntries) {
      _screenV.set(e.x, e.y, 0).project(camera);
      e.div.style.left = `${(_screenV.x * 0.5 + 0.5) * wrap.clientWidth}px`;
      e.div.style.top = `${(-_screenV.y * 0.5 + 0.5) * wrap.clientHeight}px`;
    }
  }
  function syncStorylineAxis() {
    if (!storylineActive) return;
    const halfW = (camera.right - camera.left) / 2;
    const visLeft = camera.position.x - halfW, visRight = camera.position.x + halfW;
    const clamp = (v) => Math.max(0, Math.min(storylineAxisWidthWorld, v));
    const fromT = storylineMinT +
      (clamp(visLeft) / (storylineAxisWidthWorld || 1)) * (storylineMaxT - storylineMinT);
    const toT = storylineMinT +
      (clamp(visRight) / (storylineAxisWidthWorld || 1)) * (storylineMaxT - storylineMinT);
    buildStorylineAxis(fromT, toT);
  }
  async function renderStoryline(id, opts) {
    const t0 = performance.now();
    const options = opts || {};
    const restored = egoSaved ? new Set(egoSaved.keys()) : null;
    restoreEgoLayout();
    if (restored) syncMovedInstancePositions(restored);
    clearDrillState();
    clearEgoGroupState();
    disposeProjectAnchors();
    clearStorylineState();

    selectedId = id;
    pathFocusId = id;
    if (!options.skipStackPush) pushFocusStack(id);

    const adj = buildSuccessionAdjacency();
    const chainIds = buildStorylineChain(id, adj);
    const { subAgentOf, ticksOf } = buildStorylineBranchesAndTicks(chainIds);
    pathReachable = new Set([...chainIds, ...subAgentOf.keys(), ...ticksOf.keys()]);
    focusBasePathReachable = new Set(pathReachable);

    let minT = Infinity, maxT = -Infinity;
    for (const rid of pathReachable) {
      const nd = idById.get(rid);
      if (!nd) continue;
      const t = nd.createdAt || 0;
      if (t < minT) minT = t;
      if (t > maxT) maxT = t;
    }
    if (!Number.isFinite(minT) || !Number.isFinite(maxT)) { minT = 0; maxT = 1; }
    const timeSpan = Math.max(maxT - minT, 1);
    const wpp = maxViewSize / wrap.clientHeight;
    const axisWidth = STORYLINE_AXIS_WIDTH_PX * wpp;
    const timeToX = (t) => ((t - minT) / timeSpan) * axisWidth;
    storylineMinT = minT; storylineMaxT = maxT; storylineAxisWidthWorld = axisWidth;

    egoSaved = new Map();
    for (const rid of pathReachable) {
      const nd = idById.get(rid);
      if (nd) egoSaved.set(rid, { x: nd.x, y: nd.y });
    }
    for (const rid of chainIds) {
      const nd = idById.get(rid);
      if (!nd) continue;
      nd.x = timeToX(nd.createdAt || minT);
      nd.y = 0;
    }
    const subAgentRow = new Map(); // parentId -> siblings placed so far (staggers them)
    storylineMaxSubrowOffsetPx = STORYLINE_ROW_OFFSET_PX;
    for (const [subId, parentId] of subAgentOf) {
      const nd = idById.get(subId);
      if (!nd) continue;
      const n = subAgentRow.get(parentId) || 0;
      subAgentRow.set(parentId, n + 1);
      const dir = n % 2 === 0 ? 1 : -1;
      const tier = Math.floor(n / 2); // sqrt(tier) is injective on tier -- never repeats
      const offsetPx = STORYLINE_ROW_OFFSET_PX + Math.sqrt(tier) * STORYLINE_SUBROW_STEP_PX;
      if (offsetPx > storylineMaxSubrowOffsetPx) storylineMaxSubrowOffsetPx = offsetPx;
      nd.x = timeToX(nd.createdAt || minT);
      nd.y = dir * offsetPx * wpp;
    }
    for (const [tickId, bodyId] of ticksOf) {
      const nd = idById.get(tickId), body = idById.get(bodyId);
      if (!nd || !body) continue;
      nd.x = timeToX(nd.createdAt || minT);
      nd.y = body.y;
    }
    syncMovedInstancePositions(new Set(egoSaved.keys()));
    storylineChainIds = chainIds;
    storylineSubAgentOf = subAgentOf;
    storylineTickOf = ticksOf;
    storylineActive = true;
    buildStorylineLines();
    buildStorylineAxis(minT, maxT);

    let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
    for (const rid of pathReachable) {
      const nd = idById.get(rid);
      if (!nd || nd.x == null || nd.y == null) continue;
      minX = Math.min(minX, nd.x); maxX = Math.max(maxX, nd.x);
      minY = Math.min(minY, nd.y); maxY = Math.max(maxY, nd.y);
    }
    if (Number.isFinite(minX)) {
      camera.position.x = (minX + maxX) / 2;
      camera.position.y = (minY + maxY) / 2;
      const span = Math.max(maxX - minX, maxY - minY, 0);
      const STORYLINE_FIT_MIN_VIEWSIZE = 400;
      viewSize = Math.max(STORYLINE_FIT_MIN_VIEWSIZE, Math.min(maxViewSize, span * 1.6 + 200));
      updateFrustum();
      rescaleForZoom();
    }

    applyDim();
    buildEdgeLines(idToNode, edges);
    if (onFocus) onFocus(id);
    setStatus(`storyline: ${chainIds.size} chain, ${subAgentOf.size} sub-agents, ` +
      `${ticksOf.size} ticks`);
    scheduleLabelPick();
    markDirty();
    if (window.__spaceDebugTiming) console.debug("renderStoryline sync ms:", performance.now() - t0);
    await inspect(id);
  }

  // THE DRILL, items 2/4: "the walk stops at a container... containers passed through are
  // single anchor nodes, so a thread-to-decision walk across two projects reads as two
  // anchors and the path." The ordinary ego walk (bfsHops over outAdjPath/inAdjPath) only
  // ever follows PATH_EDGE_TYPES, never structural/container edges, so it already never
  // walks INTO a container -- nothing to enforce there. What's new: when the reachable set
  // spans more than one real project, each distinct project gets one small anchor label
  // (not per member) so a cross-project path still reads as "which project is this part of"
  // at a glance; clicking an anchor drills into that project's own container view.
  let projectAnchorEntries = [];
  function disposeProjectAnchors() {
    for (const e of projectAnchorEntries) e.div.remove();
    projectAnchorEntries = [];
  }
  function buildProjectObjectIndex() {
    projectObjectByName = new Map();
    for (const nd of idToNode) {
      if (nd.type === "SoftwareProject" && nd.label) projectObjectByName.set(`repo:${nd.label}`, nd.id);
    }
  }
  function buildProjectAnchors(focusId) {
    disposeProjectAnchors();
    const byProject = new Map(); // project name -> {sumX,sumY,count}
    for (const rid of pathReachable) {
      const nd = idById.get(rid);
      if (!nd || !nd.project || nd.project === "unfiled") continue;
      const agg = byProject.get(nd.project) || { sumX: 0, sumY: 0, count: 0 };
      agg.sumX += nd.x || 0; agg.sumY += nd.y || 0; agg.count++;
      byProject.set(nd.project, agg);
    }
    if (byProject.size < 2) return; // one project (or none) -- nothing to distinguish
    for (const [proj, agg] of byProject) {
      const x = agg.sumX / agg.count, y = agg.sumY / agg.count;
      const targetId = projectObjectByName.get(proj);
      const div = document.createElement("div");
      div.className = "lod-glyph-label ego-drill-label";
      div.style.cursor = targetId ? "pointer" : "default";
      div.textContent = `${proj} (${agg.count})`;
      if (targetId) {
        div.addEventListener("click", (ev) => { ev.stopPropagation(); focusObject(targetId); });
      }
      labelsEl.appendChild(div);
      projectAnchorEntries.push({ x, y, div });
    }
  }
  function positionProjectAnchors() {
    for (const e of projectAnchorEntries) {
      _screenV.set(e.x, e.y, 0).project(camera);
      e.div.style.left = `${(_screenV.x * 0.5 + 0.5) * wrap.clientWidth}px`;
      e.div.style.top = `${(-_screenV.y * 0.5 + 0.5) * wrap.clientHeight}px`;
    }
  }

  // THE DRILL, item 5 (Thoth mail 11048): "under a project filter a visible node with
  // hidden cross-project links shows a small counted stub; clicking the stub reveals that
  // project's part of the path without unhiding the project." Computed once per filter
  // change (setHiddenProjects calls buildProjectStubs), never per-frame -- a full edge
  // scan is a rare, deliberate act's cost, not a render one.
  //
  // MAX_PROJECT_STUBS: THE LAST RENDERER's own live verification (repo:osiris filtered
  // to itself, mail 11222) caught a real freeze here -- osiris's own cross-project fan-out
  // (works_in: 8,363 edges) produced 12,273 distinct (node, hiddenProject) boundary pairs,
  // one real DOM div EACH, repositioned via positionProjectStubs() on EVERY render frame.
  // That's the same declutter problem labels already solve (pickLabels' own top-N-by-
  // degree-in-viewport): keep the biggest, most-informative stubs, drop the rest, same as
  // the doc comment above already promises ("a small counted stub") but the code never
  // actually bounded. THE STUB AGGREGATION FIX (Thoth mail 11241, live review of w299)
  // went further: even capped, 80 divs reading "+N in unfiled" all stacked on the same
  // spot was still noise, not signal -- two problems, not one. "unfiled" is never a real,
  // pickable project (the repo dropdown never lists it), so it should never read as a
  // hidden-project boundary at all. And the per-NODE grouping was the wrong unit -- a
  // visible project can have hundreds of individual boundary nodes into the same one
  // hidden project; the legible fact is "this project has N links into that project," not
  // N separate one-node stubs. Regrouped to (visible node's own project, hidden project),
  // capped tighter now that aggregation already does most of the decluttering.
  const MAX_PROJECT_STUBS = 20;
  let projectStubEntries = []; // [{x, y, visibleProject, hiddenProject, count, div}]
  function disposeProjectStubs() {
    for (const e of projectStubEntries) e.div.remove();
    projectStubEntries = [];
  }
  function buildProjectStubDivs() {
    for (const entry of projectStubEntries) {
      const div = document.createElement("div");
      div.className = "lod-glyph-label ego-drill-label";
      div.style.cursor = "pointer";
      div.textContent = `+${entry.count} in ${entry.hiddenProject}`;
      div.addEventListener("click", (ev) => { ev.stopPropagation(); revealProjectStub(entry); });
      labelsEl.appendChild(div);
      entry.div = div;
    }
  }
  function buildProjectStubs() {
    disposeProjectStubs();
    if (hiddenProjects.size === 0) return;
    // one entry per (visible node's own PROJECT, hidden project) pair -- not per node.
    const groups = new Map(); // key -> {visibleProject, hiddenProject, count}
    for (const e of edges) {
      const na = idById.get(e.source), nb = idById.get(e.target);
      if (!na || !nb) continue;
      const aHidden = hiddenProjects.has(na.project), bHidden = hiddenProjects.has(nb.project);
      if (aHidden === bHidden) continue; // both or neither hidden -- not a filter boundary
      const visible = aHidden ? nb : na, hiddenNd = aHidden ? na : nb;
      if (!nodeVisible(visible)) continue; // the visible side must actually be shown itself
      if (hiddenNd.project === "unfiled") continue; // never a real, pickable project
      const key = `${visible.project}|${hiddenNd.project}`;
      const g = groups.get(key) || { visibleProject: visible.project, hiddenProject: hiddenNd.project, count: 0 };
      g.count++;
      groups.set(key, g);
    }
    if (!groups.size) { projectStubEntries = []; return; }
    // THE STUB PLACEMENT FIX (Thoth mail 11249): "place each at the cluster boundary
    // toward its hidden project's centroid" -- every project's own centroid and radius,
    // in one pass over idToNode (real positions exist regardless of visibility), so a
    // stub for (osiris, projA) and one for (osiris, projB) fan out toward projA's and
    // projB's own real direction instead of both landing on osiris's own centroid and
    // stacking there.
    const sums = new Map(); // project -> {sx, sy, n}
    for (const nd of idToNode) {
      const s = sums.get(nd.project) || { sx: 0, sy: 0, n: 0 };
      s.sx += nd.x || 0; s.sy += nd.y || 0; s.n++;
      sums.set(nd.project, s);
    }
    const centroids = new Map();
    for (const [project, s] of sums) centroids.set(project, { x: s.sx / s.n, y: s.sy / s.n });
    // radius = the SAME 98th-percentile trim fitToNodes' own camera fit uses, not the raw
    // max: osiris's own real max distance runs tens of thousands of units past its own
    // tightly-fit view (a few far outliers), which placed every stub for it off-screen
    // entirely -- worse than the "stacked in one column" this fix set out to cure.
    const visibleProjects = new Set([...groups.values()].map((g) => g.visibleProject));
    const distances = new Map(); // project -> sorted distances, only for projects we need
    for (const p of visibleProjects) distances.set(p, []);
    for (const nd of idToNode) {
      const arr = distances.get(nd.project);
      if (!arr) continue;
      const c = centroids.get(nd.project);
      arr.push(Math.hypot((nd.x || 0) - c.x, (nd.y || 0) - c.y));
    }
    const radii = new Map();
    for (const [project, arr] of distances) {
      if (!arr.length) { radii.set(project, 0); continue; }
      arr.sort((a, b) => a - b);
      radii.set(project, arr[Math.min(arr.length - 1, Math.floor(arr.length * 0.98))]);
    }
    const all = [...groups.values()].map((g) => {
      const vc = centroids.get(g.visibleProject), hc = centroids.get(g.hiddenProject);
      let x = vc ? vc.x : 0, y = vc ? vc.y : 0;
      if (vc && hc) {
        const dx = hc.x - vc.x, dy = hc.y - vc.y;
        const dist = Math.hypot(dx, dy) || 1;
        const r = (radii.get(g.visibleProject) || 0) + 60; // a small margin past the cluster edge
        x = vc.x + (dx / dist) * r;
        y = vc.y + (dy / dist) * r;
      }
      return { x, y, visibleProject: g.visibleProject, hiddenProject: g.hiddenProject, count: g.count, div: null };
    });
    all.sort((a, b) => b.count - a.count);
    projectStubEntries = all.slice(0, MAX_PROJECT_STUBS);
    buildProjectStubDivs();
  }
  function positionProjectStubs() {
    // de-overlap: aggregation alone still leaves every hidden-project stub rooted in the
    // SAME visible project at roughly the same centroid ("80 stubs ... stacked on one
    // spot", Thoth mail 11241) -- nudge a colliding stub straight down past whatever
    // already claimed that screen slot, same greedy idea positionLabels' own overlapsPlaced
    // uses, kept local/independent since stubs are a bounded (<=20), separate pass.
    const placed = [];
    const W = 90, H = 16, GAP = 4;
    for (const entry of projectStubEntries) {
      if (!entry.div) continue;
      _screenV.set(entry.x, entry.y, 0).project(camera);
      let x = (_screenV.x * 0.5 + 0.5) * wrap.clientWidth;
      let y = (-_screenV.y * 0.5 + 0.5) * wrap.clientHeight;
      let tries = 0;
      while (tries < 12 && placed.some((b) => Math.abs(b.x - x) < W && Math.abs(b.y - y) < H + GAP)) {
        y += H + GAP;
        tries++;
      }
      placed.push({ x, y });
      entry.div.style.left = `${x}px`;
      entry.div.style.top = `${y}px`;
    }
  }
  // reveals every hidden-project-side node this stub's own (visibleProject, hiddenProject)
  // pair touches -- adds them to revealedStubIds (nodeVisible/applyDim's own override),
  // never touches hiddenProjects itself, so the rest of that project stays hidden.
  function revealProjectStub(entry) {
    for (const e of edges) {
      const na = idById.get(e.source), nb = idById.get(e.target);
      if (!na || !nb) continue;
      if (na.project === entry.visibleProject && nb.project === entry.hiddenProject) revealedStubIds.add(nb.id);
      if (nb.project === entry.visibleProject && na.project === entry.hiddenProject) revealedStubIds.add(na.id);
    }
    applyDim();
    buildEdgeLines(idToNode, edges);
    buildProjectStubs(); // this stub's own count may now be satisfied and disappear
    markDirty();
  }

  // a second LineSegments drawn OVER the dim base edges: the reachable PATH edges (bright,
  // WITH DIRECTION — a vertex-colour gradient, brighter at the source/dependent end, dimmer
  // at the target/depended-on end, per Osiris's own from_id->to_id convention). Ruling
  // c5953bb1's own "the focused object's structural edges draw on focus only" carve-out is
  // GONE (THE LAST RENDERER, Thoth mail 11066: "an edge draws only when both ends are
  // visible, no structural-hop exception") -- it used to draw every structural edge
  // touching the focus regardless of whether the other end was ever positioned or visible,
  // which for a container-scale focus (repo:osiris, ~20k structural neighbours) meant
  // thousands of lines fanning to scattered original positions, "a solid disc of edges."
  // A structural edge among the reachable set (e.g. a drill's own hub-to-member link) still
  // draws through the ordinary base layer (buildEdgeLines) if the legend's own structural
  // checkbox is opted back in -- no separate exception needed or wanted any more.
  let pathHighlightEdges = null;
  const PATH_EDGE_BRIGHT = new THREE.Color(0x58a6ff);
  const PATH_EDGE_DIM = new THREE.Color(0x58a6ff).multiplyScalar(0.35);
  // CROSS-CLUSTER FOCUS EDGE BUNDLING (operator ruling, grounds 5b37d219, Thoth mail 11272
  // item 5): "cross-cluster edges longer than a threshold in screen pixels draw as bundled
  // quadratic curves with alpha falling with length." Scoped to the FOCUS overlay only
  // (this function) -- THE LAST RENDERER's own "no bundling, straight lines" rule (mail
  // 11066) stays in force for the base dim layer (buildEdgeLines); this reopens rendering
  // for focus/preview specifically, per the new ruling, not a blanket reversal.
  const BUNDLE_SCREEN_PX_THRESHOLD = 220;
  const BUNDLE_CURVE_SEGMENTS = 14;
  const BUNDLE_BOW_PX = 60; // how far the curve's own midpoint bows off the straight line
  const BUNDLE_ALPHA_FLOOR = 0.15; // never fades a real edge to invisible, same spirit as uMinAlpha
  function updatePathEdges() {
    if (pathHighlightEdges) {
      scene.remove(pathHighlightEdges);
      pathHighlightEdges.geometry.dispose();
      pathHighlightEdges.material.dispose();
      pathHighlightEdges = null;
    }
    markDirty();
    if (!pathFocusId) return;
    const wpp = worldPerPx();
    const pos = [], col = [];
    for (const e of edges) {
      const onPath = PATH_EDGE_TYPES.has(e.type) && pathReachable.has(e.source) && pathReachable.has(e.target);
      if (!onPath) continue;
      const a = idById.get(e.source), b = idById.get(e.target);
      if (!a || !b) continue;
      const ax = a.x || 0, ay = a.y || 0, bx = b.x || 0, by = b.y || 0;
      const worldLen = Math.hypot(bx - ax, by - ay);
      const screenLen = worldLen / wpp;
      const crossCluster = a.project && b.project && a.project !== b.project;
      if (!crossCluster || screenLen <= BUNDLE_SCREEN_PX_THRESHOLD) {
        pos.push(ax, ay, -0.05, bx, by, -0.05);
        col.push(PATH_EDGE_BRIGHT.r, PATH_EDGE_BRIGHT.g, PATH_EDGE_BRIGHT.b,
          PATH_EDGE_DIM.r, PATH_EDGE_DIM.g, PATH_EDGE_DIM.b);
        continue;
      }
      // alpha falls with length past the threshold -- baked into the vertex COLOR here
      // (this material has no separate alpha attribute), scaling the bright/dim endpoints
      // toward black so additive blending reads as dimmer without ever hitting true zero.
      const over = (screenLen - BUNDLE_SCREEN_PX_THRESHOLD) / BUNDLE_SCREEN_PX_THRESHOLD;
      const alpha = Math.max(BUNDLE_ALPHA_FLOOR, 1 / (1 + over)); // Reinhard-shaped falloff, bounded
      const midX = (ax + bx) / 2, midY = (ay + by) / 2;
      const nx = -(by - ay) / worldLen, ny = (bx - ax) / worldLen; // unit perpendicular
      const bow = BUNDLE_BOW_PX * wpp;
      const ctrlX = midX + nx * bow, ctrlY = midY + ny * bow;
      let px = ax, py = ay;
      for (let s = 1; s <= BUNDLE_CURVE_SEGMENTS; s++) {
        const t = s / BUNDLE_CURVE_SEGMENTS;
        const omt = 1 - t;
        const x = omt * omt * ax + 2 * omt * t * ctrlX + t * t * bx;
        const y = omt * omt * ay + 2 * omt * t * ctrlY + t * t * by;
        pos.push(px, py, -0.05, x, y, -0.05);
        const t0 = (s - 1) / BUNDLE_CURVE_SEGMENTS;
        const c0 = PATH_EDGE_DIM.clone().lerp(PATH_EDGE_BRIGHT, 1 - t0).multiplyScalar(alpha);
        const c1 = PATH_EDGE_DIM.clone().lerp(PATH_EDGE_BRIGHT, 1 - t).multiplyScalar(alpha);
        col.push(c0.r, c0.g, c0.b, c1.r, c1.g, c1.b);
        px = x; py = y;
      }
    }
    if (!pos.length) return;
    const geo = new THREE.BufferGeometry();
    geo.setAttribute("position", new THREE.Float32BufferAttribute(new Float32Array(pos), 3));
    geo.setAttribute("color", new THREE.Float32BufferAttribute(new Float32Array(col), 3));
    pathHighlightEdges = new THREE.LineSegments(
      geo, new THREE.LineBasicMaterial({ vertexColors: true, transparent: true, opacity: 0.85 }));
    scene.add(pathHighlightEdges);
  }

  // ---- pan + zoom — LOOKING ONLY, never changes what's loaded ------------------------
  // a browser fires a real "click" event at pointerup even after a long drag, as long as
  // it lands back on the same element — the operator caught this exactly ("dragging and
  // releasing... refocuses on a random object"). Track total drag distance and only treat
  // the click handler's pick as genuine below a small pixel threshold; anything past that
  // was a pan, not a click, and the trailing click event is swallowed.
  let dragging = false, lastX = 0, lastY = 0, dragDistance = 0;
  const CLICK_SLOP_PX = 4;
  renderer.domElement.addEventListener("pointerdown", (ev) => {
    dragging = true; lastX = ev.clientX; lastY = ev.clientY; dragDistance = 0;
  });
  window.addEventListener("pointerup", () => { dragging = false; });
  window.addEventListener("pointermove", (ev) => {
    if (!dragging) return;
    const dx = ev.clientX - lastX, dy = ev.clientY - lastY;
    lastX = ev.clientX; lastY = ev.clientY;
    dragDistance += Math.hypot(dx, dy);
    const wpx = (camera.right - camera.left) / wrap.clientWidth;
    const wpy = (camera.top - camera.bottom) / wrap.clientHeight;
    camera.position.x -= dx * wpx;
    camera.position.y += dy * wpy;
    // no scheduleLabelUpdate() here — label POSITIONS are repainted on every render this
    // markDirty() triggers (render-on-demand, see the loop below); only WHICH labels show
    // is still debounced (scheduleLabelPick).
    markDirty();
    scheduleLabelPick();
  });

  // O(1) regardless of node count now — sizing lives in the shader (aRadiusPx * a live
  // uWorldPerPx uniform, see makeInstancedCircleMaterial), so a zoom step only ever writes
  // one float per material (the draw mesh's and pick mesh's own uWorldPerPx) instead of
  // rewriting 49,019 instance matrices.
  function rescaleForZoom() {
    const wpp = worldPerPx();
    if (meshUniforms) meshUniforms.uWorldPerPx.value = wpp;
    if (pickUniforms) pickUniforms.uWorldPerPx.value = wpp;
    // no edge-style call here any more — the edge-fade shader (makeEdgeFadeMaterial) reads
    // screen length straight off projectionMatrix/modelViewMatrix every render, already
    // current every frame with zero extra work on a zoom step.
  }

  // wheel = LOOKING ONLY, cursor-anchored (Thoth's own live fix, mail 10581 item 5: "zoom
  // is not anchored at the cursor"), coalesced to one update per animation frame no matter
  // how many wheel events land in that frame (a real trackpad/mouse burst is 20-60 events —
  // each one used to trigger its own full rescale; now each just accumulates a delta, and
  // ONE zoomAt() runs per frame).
  let pendingWheelDelta = 0, wheelClientX = 0, wheelClientY = 0, wheelRafPending = false;
  let wheelReceivedAt = 0; // wheel-hang instrumentation only (Thoth mail 11248)
  function zoomAt(clientX, clientY, deltaY) {
    const t = window.__spaceWheelTiming ? performance.now() : 0;
    const rect = wrap.getBoundingClientRect();
    const nx = rect.width ? (clientX - rect.left) / rect.width : 0.5;
    const ny = rect.height ? (clientY - rect.top) / rect.height : 0.5;
    const worldX = camera.position.x + THREE.MathUtils.lerp(camera.left, camera.right, nx);
    const worldY = camera.position.y + THREE.MathUtils.lerp(camera.top, camera.bottom, ny);
    viewSize = Math.max(minViewSize, Math.min(maxViewSize, viewSize * Math.exp(deltaY * 0.001)));
    updateFrustum();
    const t1 = window.__spaceWheelTiming ? performance.now() : 0;
    // re-anchor: keep the same world point under the cursor after the frustum resize.
    camera.position.x = worldX - THREE.MathUtils.lerp(camera.left, camera.right, nx);
    camera.position.y = worldY - THREE.MathUtils.lerp(camera.top, camera.bottom, ny);
    rescaleForZoom();
    syncRibbonResolve();
    syncStorylineAxis();
    syncCommunityVisibility();
    const t2 = window.__spaceWheelTiming ? performance.now() : 0;
    scheduleLabelPick();
    markDirty();
    if (window.__spaceWheelTiming) {
      console.debug("[wheel] zoomAt: frustum", (t1 - t).toFixed(2), "ms; rescale", (t2 - t1).toFixed(2), "ms; total", (performance.now() - t).toFixed(2), "ms");
    }
  }
  function applyPendingWheel() {
    wheelRafPending = false;
    if (pendingWheelDelta === 0) return;
    const deltaY = pendingWheelDelta;
    pendingWheelDelta = 0;
    if (window.__spaceWheelTiming) console.debug("[wheel] applyPendingWheel fired, rAF delay from wheel event:", (performance.now() - wheelReceivedAt).toFixed(2), "ms");
    zoomAt(wheelClientX, wheelClientY, deltaY);
  }
  renderer.domElement.addEventListener(
    "wheel",
    (ev) => {
      ev.preventDefault();
      if (window.__spaceWheelTiming && !wheelRafPending) wheelReceivedAt = performance.now();
      pendingWheelDelta += ev.deltaY;
      wheelClientX = ev.clientX; wheelClientY = ev.clientY;
      if (!wheelRafPending) { wheelRafPending = true; requestAnimationFrame(applyPendingWheel); }
    },
    { passive: false }
  );

  // ---- GPU picking (nearest-within-tolerance, review flaw #3/#8) ----------------------
  // review flaw #3: the original 1x1 pick was EXACT-PIXEL, no tolerance -- two real clicks
  // on genuinely visible small nodes (4px, 8px) missed outright (selectedId/pickAt both
  // null). Same root cause made the hover card read empty on a real hover (#8): it calls
  // this same function. Fixed by rendering a small box around the cursor instead of one
  // pixel and picking whichever hit id sits closest to the box's own centre — an exact hit
  // still wins immediately (distance 0), a near-miss within PICK_BOX/2 px now resolves too.
  const PICK_BOX = 33; // device px, odd -- generous tolerance, still a trivial GPU readback
  const PICK_HALF = (PICK_BOX - 1) / 2;
  const pickTarget = new THREE.WebGLRenderTarget(PICK_BOX, PICK_BOX);
  const pickBuf = new Uint8Array(PICK_BOX * PICK_BOX * 4);
  function pickAt(clientX, clientY) {
    const rect = renderer.domElement.getBoundingClientRect();
    // setViewOffset's (x,y) origin is TOP-LEFT (matching a mouse event's own coordinates,
    // per its own docstring's tile-grid example: A/B/C at y=0 sit ABOVE D/E/F at y=h) —
    // NOT WebGL's bottom-left convention. The earlier flip here was the real cause of
    // clicks missing the node visually under the cursor.
    const px = (clientX - rect.left) * (window.devicePixelRatio || 1);
    const py = (clientY - rect.top) * (window.devicePixelRatio || 1);
    camera.setViewOffset(
      renderer.domElement.width, renderer.domElement.height,
      px - PICK_HALF, py - PICK_HALF, PICK_BOX, PICK_BOX);
    renderer.setRenderTarget(pickTarget);
    renderer.render(pickScene, camera);
    renderer.setRenderTarget(null);
    camera.clearViewOffset();
    renderer.readRenderTargetPixels(pickTarget, 0, 0, PICK_BOX, PICK_BOX, pickBuf);
    let bestId = 0, bestDist = Infinity;
    for (let y = 0; y < PICK_BOX; y++) {
      for (let x = 0; x < PICK_BOX; x++) {
        const o = (y * PICK_BOX + x) * 4;
        const id = pickBuf[o] | (pickBuf[o + 1] << 8) | (pickBuf[o + 2] << 16);
        if (id === 0) continue;
        const dx = x - PICK_HALF, dy = y - PICK_HALF;
        const dist = dx * dx + dy * dy;
        if (dist < bestDist) { bestDist = dist; bestId = id; }
      }
    }
    return bestId === 0 ? null : idToNode[bestId - 1];
  }

  // TIP 1 AMENDMENT (operator via Thoth mail 10726): a single CLICK on a node IS focus —
  // select, inspector, hide, fit, one gesture. No double-click, no Enter-to-promote; a click
  // on empty canvas still clears. Back/Escape are the only acts left that navigate history.
  // TIP 4 (operator ruling "DENSITY NOT DISCS", mail 11011) simplifies this back to the
  // pre-TIP-3 shape: every object is drawn (and so pickable, same GPU pick pass) at every
  // zoom now, no separate glyph layer or drill-in branch to special-case any more.
  renderer.domElement.addEventListener("click", (ev) => {
    if (dragDistance > CLICK_SLOP_PX) return; // the trailing click after a real pan/drag
    const hit = pickAt(ev.clientX, ev.clientY);
    if (hit) focusObject(hit.id);
    else clearFocus();
  });

  // TIP 1(c): the hover card shows the label line plus type and project — the inspector
  // (click) has the rest. Debounced like the label pick (not every mousemove — pickAt is a
  // real render-target pass, cheap once, not something to run at full mouse-event rate) and
  // skipped entirely while dragging so it never fights a pan.
  let hoverNode = null;
  let hoverTimer = null;
  const hoverEl = document.createElement("div");
  hoverEl.className = "hover-card";
  hoverEl.hidden = true;
  wrap.appendChild(hoverEl);
  // THE UNMISTAKABLE FOCUS (mail 11272 item 2): "a ring or halo" around the clicked
  // object itself -- a plain HTML overlay (same pattern as hoverEl above), sized off the
  // focused node's own aRadiusPx-equivalent screen size, never the WebGL scene.
  const focusRingEl = document.createElement("div");
  focusRingEl.className = "focus-ring";
  focusRingEl.hidden = true;
  wrap.appendChild(focusRingEl);
  function positionFocusRing() {
    const nd = pathFocusId ? idById.get(pathFocusId) : null;
    if (!nd) { focusRingEl.hidden = true; return; }
    _screenV.set(nd.x || 0, nd.y || 0, 0).project(camera);
    const x = (_screenV.x * 0.5 + 0.5) * wrap.clientWidth;
    const y = (-_screenV.y * 0.5 + 0.5) * wrap.clientHeight;
    const r = Math.max(10, (nd.radiusPx || 6) + 6);
    focusRingEl.style.left = `${x}px`;
    focusRingEl.style.top = `${y}px`;
    focusRingEl.style.width = `${r * 2}px`;
    focusRingEl.style.height = `${r * 2}px`;
    focusRingEl.hidden = false;
  }
  // THE UNMISTAKABLE FOCUS (mail 11272 item 3): "the hover card shows the same identity
  // string as the label plus type, project and generation; label and card never
  // disagree." labelTextFor(nd) is already the SAME call pickLabels' own div.textContent
  // uses -- label and card were already structurally incapable of disagreeing, since both
  // read the identical function on the identical node.
  // THE HONEST GENERATION FIX (Thoth mail 11359): this count is succeeded_from HOP DEPTH,
  // not the seat's own generation numeral the label string carries (Khnum's roman numeral,
  // seat_generation) -- for a seat whose own succession chain has gaps or a different root,
  // the two numbers genuinely differ (sekhmet XLIV: label roman XLIV / seat_generation 44,
  // this count 43). Parsing the label's own roman numeral client-side would silently break
  // on every future label-format change Khnum makes; named for what it actually measures
  // instead of claiming to be the generation.
  let succeededFromNext = null; // built lazily, once: id -> id it succeeded (older)
  let succeededFromMembers = null; // ids appearing anywhere in a succeeded_from edge
  function computeGeneration(nd) {
    if (!succeededFromNext) {
      succeededFromNext = new Map();
      succeededFromMembers = new Set();
      for (const e of edges) {
        if (e.type !== "succeeded_from") continue;
        succeededFromNext.set(e.source, e.target);
        succeededFromMembers.add(e.source);
        succeededFromMembers.add(e.target);
      }
    }
    if (!succeededFromMembers.has(nd.id)) return null;
    let count = 0, cur = nd.id, guard = 0;
    while (succeededFromNext.has(cur) && guard++ < 200) { cur = succeededFromNext.get(cur); count++; }
    return count + 1;
  }
  function updateHoverCard(nd) {
    const generation = computeGeneration(nd);
    hoverEl.innerHTML = `<div class="hover-label">${labelTextFor(nd)}</div>` +
      `<div class="hover-meta">${nd.type}${nd.project ? " · " + nd.project : ""}` +
      `${generation != null ? " · chain depth " + generation : ""}</div>`;
  }
  function positionHoverCard(clientX, clientY) {
    const rect = wrap.getBoundingClientRect();
    hoverEl.style.left = `${clientX - rect.left + 14}px`;
    hoverEl.style.top = `${clientY - rect.top + 14}px`;
  }
  renderer.domElement.addEventListener("mousemove", (ev) => {
    if (dragging) { hoverEl.hidden = true; hoverNode = null; return; }
    positionHoverCard(ev.clientX, ev.clientY);
    clearTimeout(hoverTimer);
    hoverTimer = setTimeout(() => {
      const hit = pickAt(ev.clientX, ev.clientY);
      hoverNode = hit || null;
      if (hit) { updateHoverCard(hit); hoverEl.hidden = false; } else { hoverEl.hidden = true; }
    }, 80);
  });
  renderer.domElement.addEventListener("mouseleave", () => {
    clearTimeout(hoverTimer);
    hoverEl.hidden = true;
    hoverNode = null;
  });

  function clearFocus() {
    selectedId = null;
    pathFocusId = null;
    pathReachable = new Set();
    const restored = egoSaved ? new Set(egoSaved.keys()) : null;
    restoreEgoLayout();
    if (restored) syncMovedInstancePositions(restored);
    clearDrillState();
    clearEgoGroupState();
    clearStorylineState();
    focusBasePathReachable = new Set();
    disposeProjectAnchors();
    applyDim();
    // review flaw #1: the BASE edge layer is its own static geometry (built once from
    // node x/y at buildEdgeLines time) -- hiding a NODE's own instance (aVisible=0) never
    // touched its EDGES, so the whole unreachable graph stayed drawn in faint lines after a
    // focus. Rebuilding here (nodeVisible already gates on pathReachable/hiddenNodeTypes)
    // is what actually drops them; clearing needs it too, to restore the full base layer.
    buildEdgeLines(idToNode, edges);
    updatePathEdges();
    rightRail.className = "rail";
    rightRail.innerHTML =
      '<div class="insp-empty" id="insp"><div style="font-weight:700;font-size:13px;' +
      'letter-spacing:0.5px;text-transform:uppercase;color:var(--text);margin-bottom:8px">' +
      "Provenance Inspector</div>Click any object to inspect its evidence grade and relationships.</div>";
    setStatus(`${idToNode.length} objects, ${edges.length} edges`);
    if (onFocus) onFocus(null); // shares the clear with an embedding table (console.js)
  }

  fitBtn.addEventListener("click", () => {
    fitToNodes(visibleNodesForFit());
    scheduleLabelPick();
  });
  upBtn.addEventListener("click", clearFocus);
  if (backBtn) backBtn.addEventListener("click", goBack);
  // TIP 1 AMENDMENT: the downstream toggle (was "Widen" — depth is unlimited by default
  // now, so raising a cap is moot). Off by default; re-runs the current focus on toggle.
  if (downstreamBtn) {
    downstreamBtn.textContent = "Downstream: off";
    downstreamBtn.addEventListener("click", () => {
      includeDownstream = !includeDownstream;
      downstreamBtn.textContent = `Downstream: ${includeDownstream ? "on" : "off"}`;
      if (pathFocusId) focusObject(pathFocusId, { skipStackPush: true });
    });
  }
  function pushFocusStack(id) {
    if (focusStack[focusStack.length - 1] === id) return;
    focusStack.push(id);
    if (focusStack.length > 50) focusStack.shift();
  }
  function goBack() {
    if (focusStack.length < 2) { clearFocus(); return; }
    focusStack.pop(); // the current focus
    const prev = focusStack[focusStack.length - 1];
    focusObject(prev, { skipStackPush: true });
  }

  // ---- FOCUS = THE TREE TO SOURCE (ruling c5953bb1, amended by mail 10726): a single click
  // is the whole gesture now — select, inspector, hide, fit, all synchronous, all CLIENT-SIDE
  // off the already-loaded edge list (never a network wait; `inspect(id)`'s own fetch is
  // awaited LAST, below, and never gates any of this). Walks upstream by default until
  // roots, downstream only when toggled on, hides everything unreachable (TIP 1(d), no
  // dim), relays out the reachable set locally (TIP 1's own ego-layout amendment), fits the
  // camera, then the inspector fetch fills in after.
  async function focusObject(id, opts) {
    // review flaw #9: focusObject(null) (a stray call with no real hit — the footer's own
    // "focused-badge" onclick, or a failed pick) used to set pathFocusId=null yet still walk
    // and report "focused: 1 reachable" against a degenerate single-null-entry set.
    if (!id) { clearFocus(); return; }
    // THE DRILL (Thoth mail 11048): a container-scale focus never runs the ordinary ego
    // walk at all -- it would either blow straight past MAX_EGO_NODES or (worse, the
    // operator's own observed bug) silently truncate while updatePathEdges still drew every
    // one of the focus's own uncapped structural edges, "a solid disc."
    if (isContainerFocus(id)) { await renderContainerDrill(id, opts); return; }
    // WAVE 26, THE STORYLINE (mail 11534): an Agent focus lays out on a time axis
    // instead of the ordinary ranked-column ego tree -- checked after the container
    // gate (an Agent is never a CONTAINER_FOCUS_TYPES member, so this never races it).
    if (isAgentFocus(id)) { await renderStoryline(id, opts); return; }
    if (storylineActive) clearStorylineState(); // leaving a storyline for an ordinary focus
    clearDrillState(); // leaving a drill (if any) for an ordinary small-object focus
    const t0 = performance.now();
    const options = opts || {};
    selectedId = id;
    pathFocusId = id;
    focusDepth = options.depth || FOCUS_DEPTH_DEFAULT;
    const hopsUp = bfsHops(outAdjPath, id, focusDepth);
    const hopsDown = includeDownstream ? bfsHops(inAdjPath, id, focusDepth) : new Map([[id, 0]]);
    pathReachable = new Set([...hopsUp.keys(), ...hopsDown.keys()]);
    // TIP 1(d)/amendment: "focus is never empty" — Thoth's own live measurement found a
    // degree-8 Decision with no PATH_EDGE_TYPES links reaching only itself and collapsing
    // the camera fit to a point. When the walk finds nothing beyond the focused node itself,
    // widen one hop over its own STRUCTURAL edges instead (ranked as upstream, hop 1, for
    // the ego layout below) — still just this node's real neighbours, never a synthetic
    // minimum. review flaw #2's OWN second half: this loop had no cap at all — a real hub
    // (repo:osiris, structural degree 20k+) has few/no PATH_EDGE_TYPES links of its own, so
    // pathReachable.size<=1 was true and this fallback alone reproduced the exact same
    // whole-graph blowup the rank cap above was built to prevent. Same MAX_EGO_NODES cap.
    if (pathReachable.size <= 1) {
      for (const e of edges) {
        if (pathReachable.size >= MAX_EGO_NODES) break;
        if (!isStructuralLike(e.edgeClass)) continue;
        const other = e.source === id ? e.target : e.target === id ? e.source : null;
        if (other == null || pathReachable.has(other)) continue;
        pathReachable.add(other);
        hopsUp.set(other, 1);
      }
    }
    if (!options.skipStackPush) pushFocusStack(id);
    if (onFocus) onFocus(id); // shares the selection with an embedding table (console.js)

    // ONE-HOP NEIGHBOURHOOD (mail 11272 item 1): the provenance-path walk above stays the
    // BASE reachable set (unchanged semantics); the one-hop-all-types neighbourhood is
    // additive on top of it. A fresh focus onto a DIFFERENT object resets the group/page
    // expand state; re-focusing the SAME one (or a group/"more" click within it) keeps it.
    focusBasePathReachable = new Set(pathReachable);
    focusHopsUp = hopsUp; focusHopsDown = hopsDown;
    if (egoGroupFocusId !== id) { egoGroupExpandedKey = null; egoGroupPageCount = 1; }
    egoGroupFocusId = id;

    // EGO RELAYOUT (mail 10726): focus at centre, ancestors ranked leftward by hop (roots
    // farthest left), downstream (if on) ranked rightward — "distance rational instead of
    // the world-unit spread." The one-hop groups/anchors, camera refit, edge rebuild and
    // status line all live in renderFocusEgoGroups now, shared with every group/"more"
    // click after this one so they behave identically.
    renderFocusEgoGroups(id, hopsUp, hopsDown);
    // TIP 1's own 100ms budget (mail 10726 item 2): everything above is client-side and
    // synchronous; only the inspector's own network fetch happens after, unawaited by the
    // visual. Logged, not asserted, since a live DevTools/CPU throttle can't be simulated
    // in a unit test — the discipline is the guarantee, not this one measurement.
    if (window.__spaceDebugTiming) console.debug("focusObject sync ms:", performance.now() - t0);
    await inspect(id);
  }

  async function inspect(id) {
    // review flaw #1 (TIP 1c, Thoth mail 10891): "the right pane must show the focused
    // object's details... today it stays empty after a click." Root cause: no response
    // check plus objectDetail() throwing synchronously (e.g. reading o.properties.some on
    // a malformed/error body) meant the `rightRail.innerHTML = ...` assignment never
    // happened at all — the rail silently kept whatever it showed BEFORE the click (the
    // "Click any object to inspect..." placeholder on a fresh session, read as "empty").
    // Every path below now writes something real to the rail, success or failure.
    let obj;
    try {
      const res = await fetch(`/objects/${id}`);
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      obj = await res.json();
      rightRail.className = "rail";
      rightRail.innerHTML = Osiris.objectDetail(obj, "");
    } catch (err) {
      console.error("inspect() failed for", id, err);
      rightRail.className = "rail";
      rightRail.innerHTML = `<div class="insp-empty">Could not load ${id.slice(0, 8)}: ` +
        `${(err && err.message) || err}</div>`;
      return;
    }
    // the inspector's own Focus button — a re-focus shortcut, now that a plain click on
    // the canvas already IS focus (TIP 1 amendment retired the old double-click/Enter
    // triggers this button used to sit alongside).
    const focusBtn = document.createElement("button");
    focusBtn.className = "iconbtn";
    focusBtn.textContent = pathFocusId === id ? "Focused" : "Focus";
    focusBtn.style.cssText = "margin-bottom:10px";
    focusBtn.addEventListener("click", () => focusObject(id));
    rightRail.prepend(focusBtn);
    // every object reference in the inspector (upstream_ids, readers, links) walks the
    // focus — ruling c5953bb1's own "harmony" requirement, part C, but the wiring lives
    // here since it's the same click-through this inspector has always used.
    const relsEl = rightRail.querySelector("[data-rels]");
    if (relsEl) {
      try {
        await Osiris.loadRels(relsEl, id, (pickId) => focusObject(pickId), () => {}, obj);
      } catch (err) {
        console.error("loadRels() failed for", id, err);
      }
    }
  }

  // TIP 1 AMENDMENT (mail 10726): "no double-click or Enter" — a click already IS focus,
  // so the old Enter-promotes-selection listener (and the dblclick listener above it) are
  // retired outright, not left as harmless redundancy.

  // TIP 1(e), amended: ONE search, ONE gesture — the in-canvas "Find a node" box is gone;
  // the header omnibox (console.js's own runOmniSearch/execOmniItem) drives the graph
  // directly now, a hit always focuses (click and Enter no longer differ, matching the
  // canvas's own "one gesture" — see mail 10726).

  // TIP 1(c), TIP 1b (Thoth mail 10755): LABELS ARE NAMES — Agent by handle/name,
  // SoftwareProject by repo name, Person by name, everything else type + short title. One
  // line, hard-truncated at 40 chars with an ellipsis. Khnum's own `labels` wire header
  // (tip 2g, fixed live in mail 10892/commit 0496a7d to resolve a real summary/title/
  // subject/name assertion for every type — a Commit's own label now carries its real
  // subject line straight off the wire) computes exactly this rule server-side,
  // index-aligned to object_ids — nd.label is already the final text, synchronous, no
  // per-node network fetch for any type. The earlier Commit-only client-side upgrade
  // (fetchCommitSubject, review flaw #6) is retired outright now that the gap it patched
  // is closed at the source.
  function fallbackLabel(nd) { return `${nd.type} ${nd.id.slice(0, 8)}`; }
  function labelTextFor(nd) { return nd.label || fallbackLabel(nd); }

  // THE LAST RENDERER (operator ruling d7d55257, Thoth mail 11066): "labels for the top-N
  // objects by degree inside the current viewport, de-overlapped, at every zoom." No
  // separate project-label pass any more -- a project's own name just IS whatever object in
  // it has the highest degree in view. Candidacy is nodeVisible(nd) (already the single
  // source of truth for hidden types/hidden projects/focus-reachability everywhere else in
  // this file) intersected with the camera's own world-space frustum bounds -- a node must
  // genuinely be on screen to be a label candidate, not merely "near the camera centre" (the
  // old rule, which could label something off past the edge of the viewport).
  const N_LABELS = 40;
  let labeledNodes = [];
  const labelDivs = new Map(); // node -> div, reused across frames instead of rebuilt
  const labelWidths = new Map(); // node -> real rendered px width, measured once at creation
  let labelPickTimer = null;
  function scheduleLabelPick() {
    if (labelPickTimer) return;
    labelPickTimer = setTimeout(() => { labelPickTimer = null; pickLabels(); }, 150);
  }
  // THE DISTRICT LABEL BUDGET (item 4, mail 11408): a district "earns its place by size" in
  // the SAME N_LABELS pool object labels compete for -- built as a stable pseudo-node per
  // district (own .x/.y/.degree so it drops into the identical sort/slice/declutter path
  // with no special-casing there), never real graph nodes, so `.id` is namespaced
  // (`district:<name>`) and `isLit` naturally never matches one. Rebuilt only when the
  // district model itself changes (buildDistrictModel/buildDistrictFills), not per pick --
  // identity stays stable across picks so labelDivs doesn't churn DOM nodes for a district
  // that stays labeled from one pick to the next. `districtLabelCandidates` itself is
  // declared up in THE DISTRICT MODEL block, not here -- buildDistrictModel calls this
  // function during initSpace's own synchronous setup, well before this point in the file
  // would otherwise execute; declaring the `let` down here hit the exact TDZ crash class
  // THE LAST RENDERER's own commit message already named once (projectObjectByName).
  function buildDistrictLabelCandidates() {
    districtLabelCandidates = districts
      .filter((d) => d.count >= DISTRICT_LABEL_MIN_COUNT)
      .map((d) => ({
        __isDistrict: true, id: `project:${d.name}`, name: d.name,
        x: d.cx, y: d.cy, degree: d.count,
      }));
  }
  function pickLabels() {
    const halfW = (camera.right - camera.left) / 2, halfH = (camera.top - camera.bottom) / 2;
    const minX = camera.position.x - halfW, maxX = camera.position.x + halfW;
    const minY = camera.position.y - halfH, maxY = camera.position.y + halfH;
    const inView = (nd) => nd.x >= minX && nd.x <= maxX && nd.y >= minY && nd.y <= maxY;
    const pool = idToNode.filter((nd) => nodeVisible(nd) && inView(nd));
    const districtPool = districtLabelCandidates.filter(inView);
    // WAVE 26, PIECE 2: community labels only ever compete for a slot once communities
    // are actually visible (mid zoom) -- below that they'd just be noise nobody asked for
    // yet, the exact same reasoning the fill/ribbon gates above already use.
    const communityPool = communityRegionsVisible ? communityLabelCandidates.filter(inView) : [];
    // THE FOCUS LABEL POOL FIX (Thoth mail 11359): a plain top-N-by-degree sort ignores the
    // focus entirely -- a real focus's own reachable set is mostly low-natural-degree nodes
    // (Message, chain members), so the pool filled with whatever happened to have the
    // highest degree elsewhere in the viewport, leaving most of what the reader actually
    // focused on unlabeled. Lit (focused/reachable/selected) nodes fill the pool FIRST,
    // highest-degree-first among themselves; only remaining slots go to the ordinary
    // degree ranking. Acceptance: every reachable node gets a label slot up to N_LABELS.
    const isLit = (nd) => nd.id === pathFocusId || pathReachable.has(nd.id) || nd.id === selectedId;
    // WAVE 26 live-verification finding: a district's own count (thousands) always beat an
    // ordinary node's degree by luck, so it never needed special priority -- a community's
    // own count (order 10s-100s, same order as plenty of individual node degrees) does not
    // have that luck, and the plain degree sort silently crowded every community label out
    // (0 ever won a slot against ordinary high-degree nodes in the same view). A district/
    // community pseudo-node now sits in its own tier, between lit and ordinary -- ranked
    // by its own size within that tier, never competing against unrelated object degree.
    const tier = (nd) => (isLit(nd) ? 2 : (nd.__isDistrict || nd.__isCommunity) ? 1 : 0);
    labeledNodes = pool.concat(districtPool, communityPool)
      .sort((a, b) => tier(b) - tier(a) || (b.degree || 0) - (a.degree || 0))
      .slice(0, N_LABELS);
    // reconcile DOM: remove divs for nodes no longer labeled, add for newly labeled ones —
    // reuses existing elements instead of an innerHTML rebuild every pick.
    const wanted = new Set(labeledNodes);
    for (const [nd, div] of labelDivs) {
      if (!wanted.has(nd)) { div.remove(); labelDivs.delete(nd); labelWidths.delete(nd); }
    }
    for (const nd of labeledNodes) {
      if (labelDivs.has(nd)) continue;
      const div = document.createElement("div");
      div.className = nd.__isDistrict ? "lbl project-label"
        : nd.__isCommunity ? "lbl community-label" : "lbl";
      // fallback text now, swapped for the real name async (real nodes only)
      div.textContent = (nd.__isDistrict || nd.__isCommunity)
        ? `${nd.name} (${nd.degree})` : labelTextFor(nd);
      labelsEl.appendChild(div);
      labelDivs.set(nd, div);
      // THE REAL-WIDTH DECLUTTER FIX (live-verification finding, mail 11471's own "overlap
      // pairs" acceptance line): a long label (a Decision title can run 200px+) was always
      // boxed at the same fixed LABEL_W=90 for overlap purposes, regardless of its own real
      // rendered width -- a genuine visual overlap the fixed-box declutter had no way to
      // catch. Measured once, right here, before the div is ever hidden (offsetWidth reads
      // 0 once `hidden` -- display:none -- applies, so this is the only safe moment).
      labelWidths.set(nd, div.offsetWidth || LABEL_W);
    }
    markDirty(); // newly (un)labeled divs need one more positionLabels() pass to place them
  }
  // runs on every render (render-on-demand now, not an unconditional per-frame loop — see
  // below) — cheap (one project() + style write per already-chosen label, no sort, no DOM
  // create/destroy) so labels track the scene with zero perceptible lag whenever it fires.
  // DECLUTTER: "present text without it looking like garbage" — labeledNodes is already
  // nearest-to-camera-first (from pickLabels' own sort), so a plain greedy pass — show a
  // label unless its screen box would overlap one already placed this frame — keeps the
  // closest/most-relevant labels and silently drops the rest, rather than stacking dozens
  // of overlapping strings into an unreadable wall of text.
  const _placed = []; // [x0,y0,x1,y1] boxes already shown this frame
  const LABEL_W = 90, LABEL_H = 16, LABEL_GAP = 4;
  // `w` defaults to LABEL_W for any caller that doesn't have a real measured width handy
  // (e.g. a synthetic probe) -- every real call site below always passes the label's own
  // cached labelWidths entry.
  function overlapsPlaced(x, y, w = LABEL_W) {
    const x0 = x - w / 2, x1 = x + w / 2, y0 = y - LABEL_H, y1 = y;
    for (const b of _placed) {
      if (x0 < b[2] + LABEL_GAP && x1 > b[0] - LABEL_GAP && y0 < b[3] + LABEL_GAP && y1 > b[1] - LABEL_GAP) return true;
    }
    return false;
  }
  function positionLabels() {
    _placed.length = 0;
    // THE LAST RENDERER: object titles show at every zoom now, no tier gate -- the same
    // top-N-by-degree-in-viewport pool pickLabels() computed applies universally.
    for (const nd of labeledNodes) {
      const div = labelDivs.get(nd);
      if (!div) continue;
      _screenV.set(nd.x || 0, nd.y || 0, 0).project(camera);
      const x = (_screenV.x * 0.5 + 0.5) * wrap.clientWidth;
      const y = (-_screenV.y * 0.5 + 0.5) * wrap.clientHeight;
      const w = labelWidths.get(nd) || LABEL_W;
      // a district (or WAVE 26 community) pseudo-node is never focus-reachable and never
      // the succession-chain declutter's own concern -- it just competes for a slot and
      // yields to overlap like any ordinary (non-lit) label, keeping its own class untouched.
      if (nd.__isDistrict || nd.__isCommunity) {
        if (overlapsPlaced(x, y, w)) { div.hidden = true; continue; }
        div.hidden = false;
        div.style.left = `${x}px`;
        div.style.top = `${y}px`;
        _placed.push([x - w / 2, y - LABEL_H, x + w / 2, y]);
        continue;
      }
      const lit = nd.id === pathFocusId || pathReachable.has(nd.id) || nd.id === selectedId;
      // THE CHAIN LABEL DECLUTTER (Thoth mail 11308): "lit labels always win their spot"
      // is right for an ordinary small reachable set, but a real succession chain (up to
      // 50+ pinned members, ALL lit since they're all in pathReachable) flooded the view
      // into an unbroken wall of identical labels -- lit never used to yield to anything.
      // A chain member keeps that guarantee only at generation 1 (nearest the focus) or a
      // multiple of 5; every other generation declutters like an ordinary label instead.
      const generation = lit ? computeGeneration(nd) : null;
      const chainDeclutters = generation != null && generation !== 1 && generation % 5 !== 0;
      // WAVE 26, THE STORYLINE (live-verification finding, mail 11534's own "label overlaps
      // 0" acceptance line): in a storyline, pathReachable IS the whole chain+sub-agent+tick
      // population -- often thousands -- so EVERY storyline node reads "lit," and the
      // generation-modulo-5 exception above only ever fires for true succeeded_from chain
      // members, not the sub-agents/ticks that make up most of that population. The result
      // was 780 overlap pairs measured live against the real DOM. A storyline never gets the
      // "lit always wins" guarantee at all -- every storyline label declutters like an
      // ordinary one; the focus node still wins its own slot in practice because pickLabels'
      // own isLit-first sort already places it first into an empty _placed.
      const alwaysShown = lit && !storylineActive;
      // lit/focused labels always win their spot (never declutter the thing you asked to
      // see) UNLESS this chain rule (or being in a storyline) says otherwise; ordinary
      // labels yield to anything already placed.
      if ((!alwaysShown || chainDeclutters) && overlapsPlaced(x, y, w)) { div.hidden = true; continue; }
      div.hidden = false;
      div.style.left = `${x}px`;
      div.style.top = `${y}px`;
      // THE UNMISTAKABLE FOCUS (mail 11272 item 2): the clicked object's own label is
      // strictly bigger than a merely-lit one, never just bold-and-bright -- pinned is
      // already true for it via `lit` above, this is the "larger" half of the same ask.
      div.className = "lbl" + (lit ? " lit" : "") + (nd.id === pathFocusId ? " focus-label" : "");
      _placed.push([x - w / 2, y - LABEL_H, x + w / 2, y]);
    }
    positionFocusRing();
    positionLandmarkBadges();
    positionStorylineAxis();
    if (window.__spaceWheelTiming) {
      const t0 = performance.now();
      positionDrillDivs();
      const t1 = performance.now();
      positionProjectAnchors();
      const t2 = performance.now();
      positionProjectStubs();
      const t3 = performance.now();
      positionEgoGroups();
      const t4 = performance.now();
      console.debug("[wheel] positionLabels tail: drillDivs", (t1 - t0).toFixed(2),
        "ms; projectAnchors", (t2 - t1).toFixed(2), "ms; projectStubs", (t3 - t2).toFixed(2),
        "ms; egoGroups", (t4 - t3).toFixed(2), "ms");
    } else {
      positionDrillDivs();
      positionProjectAnchors();
      positionProjectStubs();
      positionEgoGroups();
    }
  }

  // the render loop itself is defined above (markDirty/renderIfDirty, right after the
  // camera/resize setup) — render-on-demand per Thoth's own live measurement (mail 10581):
  // the old unconditional rAF-plus-50ms-fallback loop rendered forever regardless of
  // whether anything changed or the tab/surface was even visible, which is real waste this
  // fix removes rather than papering over.
  pickLabels();
  markDirty();

  const api = {
    focusObject, clearFocus, inspect, pause, resume, goBack, setHiddenTypes,
    setHiddenProjects,
    get idToNode() { return idToNode; },
    get edges() { return edges; },
    // THE WIRE EDGE CLASSES FIX (Thoth mail 11291): the effective per-type classification
    // -- the header's own link_type_class when the wire carries one for that type
    // (container already normalized to structural), the client's classOfEdgeType fallback
    // otherwise. Same live-verification convention as the rest of this debug surface.
    effectiveEdgeClass(type) { return edgeClassByType[type] || classOfEdgeType(type); },
    get edgeClassByType() { return { ...edgeClassByType }; },
    get pathReachable() { return pathReachable; },
    get pathFocusId() { return pathFocusId; },
    get selectedId() { return selectedId; },
    // DRAWING THE WHOLE GRAPH (spike, Thoth mail 11392) -- live-verification/report hooks,
    // same convention as the rest of this debug surface.
    get districts() { return districts.map((d) => ({ ...d })); },
    get landmarks() { return { ...landmarks }; },
    get ribbons() { return ribbons.map((r) => ({ ...r })); },
    get ribbonsResolvedKeys() { return [...ribbonsResolvedKeys]; },
    get districtLabelCandidateCount() { return districtLabelCandidates.length; },
    edgeAccounting,
    // WAVE 26, THE STORYLINE (mail 11534) -- live-verification hooks, same convention.
    isAgentFocus,
    get storylineActive() { return storylineActive; },
    get storylineChainLength() { return storylineChainIds.size; },
    get storylineSubAgentCount() { return storylineSubAgentOf.size; },
    get storylineTickCount() { return storylineTickOf.size; },
    get storylineTimeSpanSeconds() { return storylineMaxT - storylineMinT; },
    get storylineAxisLabelCount() { return storylineAxisEntries.length; },
    get storylineMaxSubrowOffsetPx() { return storylineMaxSubrowOffsetPx; },
    // WAVE 26, PIECE 2: COMMUNITY REGIONS (mail 11592/11664) -- live-verification hooks.
    get communities() { return communities.map((c) => ({ ...c })); },
    get communityRegionsVisible() { return communityRegionsVisible; },
    get communityRibbons() { return communityRibbons.map((r) => ({ ...r })); },
    get communityRibbonsResolvedKeys() { return [...communityRibbonsResolvedKeys]; },
    get communityLabelCandidateCount() { return communityLabelCandidates.length; },
    get communityZoomViewSize() { return communityZoomViewSize; },
    get communityRegionsDrawn() { return communityRegionsVisible ? communities.length : 0; },
    // WAVE 27, THE LENS PANEL (mail 11754) -- live-verification hooks, same convention.
    isStructuralLike,
    get communitiesHiddenByLens() { return communitiesHiddenByLens; },
    get highDegreeBadgesHiddenByLens() { return highDegreeBadgesHiddenByLens; },
    get landmarkBadgeEntryCount() { return landmarkBadgeEntries.length; },
    get lensHashParam() { return new URLSearchParams(location.hash.replace(/^#/, "")).get(LENS_HASH_PARAM); },
    readLensStateFromHash, applyLensStateFromHash,
    get edgeSegmentsDrawn() { return edgeLines ? edgeLines.geometry.attributes.position.count / 2 : 0; },
    get ribbonSegmentsDrawn() { return ribbonLines ? ribbonLines.geometry.attributes.position.count / 2 : 0; },
    camera, pickAt, mesh: () => mesh, worldPerPx, nodeScreenPx, renderer,
    // debug/test hooks only (same convention as window.__space always being exposed) —
    // zoomAt bypasses the rAF-coalesced wheel path for direct exercise; forceRender skips
    // the dirty check for a synchronous frame.
    zoomAt, forceRender: () => { renderScene(); positionLabels(); },
    // same "debug/test hooks only" convention as forceRender above -- skips
    // scheduleLabelPick's own 150ms debounce for direct live-verification exercise.
    pickLabelsNow: () => pickLabels(),
    // live-verification/test hooks for the top-N-by-degree label pool -- same "debug hooks
    // alongside the real api" convention as zoomAt/forceRender above.
    get toneMapActive() { return !!sceneTarget; },
    get labeledNodeCount() { return labeledNodes.length; },
    get visibleLabelCount() { return [...labelDivs.values()].filter((d) => !d.hidden).length; },
    // THE DRILL (Thoth mail 11048): live-verification/test hooks for the container drill,
    // cross-project anchors, and the project-filter stub reveal.
    isContainerFocus, containerMembersByType,
    get drillContainerId() { return drillContainerId; },
    get drillNodeEntries() { return drillNodeEntries.map((e) => ({ key: e.key, kind: e.kind, type: e.type, count: e.count })); },
    get drillExpandedType() { return drillExpandedType; },
    expandDrillType(type) { drillExpandedType = type; drillPageCount = 1; return renderContainerDrill(drillContainerId, { skipStackPush: true }); },
    get projectAnchorCount() { return projectAnchorEntries.length; },
    get projectStubEntries() { return projectStubEntries.map((e) => ({ visibleProject: e.visibleProject, hiddenProject: e.hiddenProject, count: e.count })); },
    revealProjectStub,
    // THE ONE-HOP NEIGHBOURHOOD (mail 11272 item 1): live-verification/test hooks, same
    // convention as the container drill's own hooks above.
    oneHopByTypeDirection,
    get egoGroupEntries() { return egoGroupEntries.map((e) => ({ key: e.key, kind: e.kind, type: e.type, direction: e.direction, count: e.count })); },
    get egoContainerAnchorEntries() { return egoContainerAnchorEntries.map((e) => ({ id: e.id, label: e.label })); },
    get egoGroupExpandedKey() { return egoGroupExpandedKey; },
    expandEgoGroup(key) { egoGroupExpandedKey = key; egoGroupPageCount = 1; renderFocusEgoGroups(pathFocusId, focusHopsUp, focusHopsDown); },
    get focusBasePathReachable() { return focusBasePathReachable; },
  };
  window.__space = api; // kept for existing debugging/test scripts, same shape as before
  return api;
}
