/* Osiris UI library — the shared rendering atoms.
 *
 * P4 of the composer: the surfaces stop owning their own renderers. A composition Result
 * (objects / values / rows / data) renders through ONE function here, reusing the same
 * graph, card, table and provenance atoms. The shell (the composer) composes these; it
 * does not redefine them. The type catalog is the SEMANTIC LAYER, read from /schema —
 * never hardcoded. The UI is an application over the ontology; it reads, never defines.
 */
const Osiris = (() => {
  const $ = (id) => document.getElementById(id);
  const esc = (s) =>
    (s == null ? "" : String(s)).replace(/[<>&]/g, (c) => ({ "<": "&lt;", ">": "&gt;", "&": "&amp;" }[c]));

  // ---- the semantic layer (object/link types, read from /schema) -----------
  let TYPE = {};
  const DEF = { c: "#6e7681", s: "ellipse", category: "Other", description: "An ontology object." };
  const ty = (t) => TYPE[t] || DEF;
  async function loadSchema() {
    const cat = await fetch("/schema").then((r) => r.json());
    TYPE = Object.fromEntries(
      cat.object_types.map((t) => [t.name, { c: t.color, s: t.shape, category: t.category, description: t.description }])
    );
    return cat;
  }

  const pct = (v) => (v != null ? Math.round(v * 100) + "%" : "—");
  const OPSYM = { eq: "=", contains: "~", lt: "<", gt: ">" };

  // an op-tree -> a readable pipeline (innermost → outermost) — the lineage breadcrumb (W4).
  // Makes a composition self-documenting: `select Organization → aggregate by sector → order ↓`.
  function lineage(spec) {
    if (!spec || typeof spec !== "object") return [];
    const inner = spec.from ? lineage(spec.from) : [];
    const where = (w) => (w && w.length ? " where " + w.map((c) => c.property + (OPSYM[c.op] || c.op) + c.value).join(" ∧ ") : "");
    switch (spec.op) {
      case "subject": return ["subject"];
      case "select": return [`select ${spec.object_type || "any"}${spec.canonical_prefix ? " " + spec.canonical_prefix + "*" : ""}${where(spec.where)}`];
      case "traverse": return [...inner, `traverse ${spec.direction || "both"} ${spec.hops || 1}↦`];
      case "collect": return [...inner, `collect ${(spec.properties || []).join(", ")}${spec.transform && spec.transform !== "identity" ? " (" + spec.transform + ")" : ""}`];
      case "subtract": return ["subtract"];
      case "union": return ["union"];
      case "intersect": return ["intersect"];
      case "aggregate": return [...inner, `aggregate by ${(spec.group_by || []).join(", ") || "all"} (${(spec.metric || {}).type || "count"})`];
      case "bundle": return [...inner, `⑂ fan out by ${spec.by || "neighborhood"}`];
      case "order": return [...inner, `order ${spec.dir || "asc"}`];
      case "take": return [...inner, `take ${spec.n}`];
      case "function": return [`${spec.name}()`];
      default: return [spec.op || "?"];
    }
  }

  // the innermost `select` node (where filter chips attach); null if the tree has none
  function innerSelect(spec) {
    let n = spec;
    while (n && typeof n === "object") {
      if (n.op === "select") return n;
      n = n.from;
    }
    return null;
  }

  // ---- atoms ---------------------------------------------------------------
  // a graded property row: value + WHERE IT CAME FROM (source · how · confidence). A long
  // value (a commit rationale, a doc body) is CLAMPED to a few lines — click to expand —
  // so the inspector stays scannable instead of a 400-word wall in a narrow column.
  function propRow(p) {
    const v = String(p.value);
    const cls = v.length > 200 ? "o-v clamp" : "o-v";
    const val = v.length > 200
      ? `<div class="${cls}" onclick="this.classList.toggle('clamp')" title="click to expand">${esc(v)}</div>`
      : `<div class="o-v">${esc(v)}</div>`;
    return `<div class="o-k">${esc(p.name)}</div>${val}
      <div class="o-pv">${esc(p.source_label || p.source_id || "—")} · ${esc(p.how || "—")} · ${pct(p.confidence)}</div>`;
  }

  // the object detail (the one noun) — type chip, title, provenance box, graded facts, slots for rels.
  // `acts` is HTML for action buttons the shell injects (search-around, dossier, …).
  function objectDetail(o, acts = "") {
    const demo = o.properties.some((p) => p.name === "demo" && String(p.value).toLowerCase() === "true");
    const title = o.name || o.canonical;
    const m = ty(o.type);
    const facts = o.properties.filter((p) => !["name", "demo", "tag"].includes(p.name));
    const pv = facts.map(propRow).join("") || `<div class="o-muted" style="grid-column:1/4">No properties.</div>`;
    
    // Extract top provenance fact for the provenance badge
    const topProp = facts.find(p => p.confidence != null) || facts[0] || {};
    const grade = (topProp.evidence_class || "self_declared").toLowerCase();
    const source = topProp.source_label || topProp.source_id || "system";
    const conf = topProp.confidence != null ? topProp.confidence : 1.0;
    
    return `
      <div class="o-top">
        <span class="o-type" style="color:${m.c};background:${m.c}1e;border:1px solid ${m.c}55">${esc(o.type)}</span>
        ${demo ? '<span class="o-demo">DEMO</span>' : ""}
        <div class="o-title">${esc(title)}</div>
        <div class="o-canon">${esc(o.canonical)}</div>
        <div class="prov-box">
          <div class="prov-row">
            <span class="o-faint">Evidence Grade</span>
            <span class="grade-chip grade-${esc(grade)}">${esc(grade.replace(/_/g, ' '))}</span>
          </div>
          <div class="prov-row">
            <span class="o-faint">Attribution</span>
            <span class="o-v" style="font-size:11px">${esc(source)}</span>
          </div>
          <div class="prov-row">
            <span class="o-faint">Confidence</span>
            <span class="o-v" style="font-size:11px">${pct(conf)}</span>
          </div>
          <div class="conf-bar"><div class="conf-fill" style="width:${Math.round(conf * 100)}%"></div></div>
        </div>
        ${acts ? `<div class="o-acts">${acts}</div>` : ""}
      </div>
      <div class="o-sect"><h3>Properties · what &amp; how</h3><div class="o-pvgrid">${pv}</div></div>
      <div class="o-sect"><h3>Relationships (1-Hop)</h3><div data-rels class="o-muted">…</div></div>`;
  }

  // walk the 1-hop neighbourhood, GROUPED by (direction, link type) with counts (W3).
  // The flat 80-row dump becomes `→ authored_by (80) ▸` — collapsed, expand on demand, and
  // "open as set" promotes the group into the center as a result set (a typed pivot).
  // `onOpenSet(type, dir, label)` renders that set; `onPick(id)` inspects one neighbour.
  async function loadRels(el, id, onPick, onOpenSet) {
    const g = await fetch(`/objects/${id}/graph?hops=1`).then((r) => r.json());
    const lab = {}; g.nodes.forEach((n) => (lab[n.id] = n));
    const groups = {};  // key: dir|type -> {dir, type, members:[{id,label,type}]}
    g.edges.filter((e) => e.source === id || e.target === id).forEach((e) => {
      const out = e.source === id, other = out ? e.target : e.source, dir = out ? "out" : "in";
      const k = `${dir}|${e.type}`;
      (groups[k] = groups[k] || { dir, type: e.type, members: [] }).members.push(lab[other] || { id: other, label: other, type: "?" });
    });
    const entries = Object.values(groups);
    if (!entries.length) { el.innerHTML = '<span class="o-faint">No links.</span>'; return; }
    el.innerHTML = entries
      .map((gr, i) => {
        const arrow = gr.dir === "out" ? "→" : "←";
        const rows = gr.members
          .map((m) => `<div class="o-rel" style="padding-left:18px"><a data-pick="${m.id}" style="cursor:pointer">${esc(m.label)}</a>
            <span class="o-faint">${esc(m.id).slice(0,8)}</span></div>`)
          .join("");
        return `<div class="o-relgrp">
            <div class="o-relhdr" data-grp="${i}">
              <span class="o-faint">${arrow}</span>
              <span class="o-reltype">${esc(gr.type)}</span>
              <span class="o-faint">(${gr.members.length})</span>
              <span class="o-disc">▸</span>
              <span class="o-openset" data-open="${i}" title="open these as a set">open as set</span>
            </div>
            <div class="o-relbody" data-body="${i}" style="display:none">${rows}</div>
          </div>`;
      })
      .join("");
    // expand/collapse a group
    el.querySelectorAll("[data-grp]").forEach((h) => (h.onclick = (ev) => {
      if (ev.target.dataset.open != null) return;  // the "open as set" link handles itself
      const i = h.dataset.grp, body = el.querySelector(`[data-body="${i}"]`);
      const open = body.style.display === "none";
      body.style.display = open ? "block" : "none";
      h.querySelector(".o-disc").textContent = open ? "▾" : "▸";
    }));
    el.querySelectorAll("[data-pick]").forEach((a) => (a.onclick = () => onPick && onPick(a.dataset.pick)));
    el.querySelectorAll("[data-open]").forEach((s) => (s.onclick = (ev) => {
      ev.stopPropagation();
      const gr = entries[s.dataset.open];
      onOpenSet && onOpenSet(gr.type, gr.dir, `${gr.type} of this object`);
    }));
  }

  // ---- the cytoscape board (objects render here) ---------------------------
  // WAVE A item 1 (operator dispatch, wave 15, thread 8839): board labels are TITLES, not
  // full text — full text lives only in the side panel's Osiris.objectDetail read (inspect()
  // already fetches the real object, unabbreviated). Truncated here so a long summary never
  // crowds the neighborhood it's rendered in.
  const LABEL_MAX = 40;
  const truncateLabel = (s) => {
    s = s || "";
    return s.length > LABEL_MAX ? s.slice(0, LABEL_MAX - 1) + "…" : s;
  };
  // hidden below a zoom threshold (labels are for orientation once you're close enough to
  // read them, not for a zoomed-out overview where they'd just overlap) — cy.style().update()
  // on every zoom tick is what makes the style FUNCTION below re-run; a static mapper is only
  // ever evaluated once per element otherwise.
  const ZOOM_LABEL_THRESHOLD = 0.45;
  // WAVE A item 4: node size by degree — a floor for isolated nodes, growing with connection
  // count, capped so one true supernode can't dwarf the board.
  const NODE_SIZE_MIN = 26, NODE_SIZE_MAX = 68, NODE_SIZE_PER_DEGREE = 3;
  const nodeSize = (e) => Math.min(NODE_SIZE_MAX, NODE_SIZE_MIN + e.degree() * NODE_SIZE_PER_DEGREE);
  // WHOLE-GRAPH LOD sizing/labels (Thoth dispatch 9563, folded from the retired Atlas —
  // same formulas, unchanged): count -> radius by sqrt scale (area, not radius, should
  // track population — twice the count should not look four times the size), and the
  // orphan/abstention labeling the census tracks at every level.
  const sizeForCount = (n) => Math.min(56, 10 + Math.sqrt(Math.max(n, 1)) * 3.2);
  const labelWithOrphans = (base, orphans) =>
    orphans ? `${base} (${orphans} orphan${orphans === 1 ? "" : "s"})` : base;
  const UNFILED_COLOR = "#c9762c";
  const unfiledLabel = (u) => {
    if (!u.orphans) return "Unfiled";
    const abstainedPart = u.abstained ? `, ${u.abstained} abstained` : "";
    return `Unfiled (${u.orphans} orphan${u.orphans === 1 ? "" : "s"}${abstainedPart})`;
  };

  // onFocus(id, deep, type): tap = select (deep=false), double-tap = primary action (deep=true).
  // onCtx(id, type, mouseEvent): right-click = the object's contextual action menu.
  // onGraphLevel(level): whole-graph mode only — fires "supernodes"|"clusters"|"nodes" after
  // each drill, purely for the shell's own level badge (Thoth dispatch 9563).
  function makeBoard(container, onFocus, onCtx, onGraphLevel) {
    if (window.cytoscapeFcose) cytoscape.use(window.cytoscapeFcose);
    const HAS_FCOSE = !!window.cytoscapeFcose;
    const cy = cytoscape({
      container, wheelSensitivity: 1, minZoom: 0.15, maxZoom: 3,
      style: [
        { selector: "node", style: {
          "background-color": (e) => ty(e.data("type")).c, shape: (e) => ty(e.data("type")).s,
          // WAVE A item 4: size by degree — a hub reads as a hub at a glance, a leaf as a
          // leaf, without opening the inspector. Degree changes as edges are added/removed,
          // so this must be a live style FUNCTION (re-run on cy.style().update()), never a
          // value baked in at add-time.
          width: (e) => nodeSize(e), height: (e) => nodeSize(e),
          "border-width": 2, "border-color": "rgba(255,255,255,0.15)",
          label: (e) => (cy.zoom() < ZOOM_LABEL_THRESHOLD ? "" : truncateLabel(e.data("label"))),
          color: "#f0f6fc", "font-size": 11, "font-weight": 600,
          "text-valign": "bottom", "text-margin-y": 5, "text-wrap": "wrap", "text-max-width": 120,
          "text-background-color": "#0d1219", "text-background-opacity": 0.88, "text-background-padding": 3,
          "text-background-shape": "roundrectangle", "min-zoomed-font-size": 6 } },
        // WAVE A item 4: agents painted with the fleet view's own live/idle/dead states
        // (agent_state, server-supplied — see app.py's object_graph) as a border ring, laid
        // over the type-coloured fill so BOTH facts stay visible: what kind of object, and
        // whether it's a body still breathing. Declared BEFORE node.focus so a SELECTED
        // agent still shows the blue focus ring, not its own liveness color — the
        // interaction state always wins over the ambient one.
        { selector: "node[type='Agent'][agent_state='live']", style: { "border-width": 3, "border-color": "#3fb950" } },
        { selector: "node[type='Agent'][agent_state='idle']", style: { "border-width": 3, "border-color": "#d29922" } },
        { selector: "node[type='Agent'][agent_state='dead']", style: { "border-width": 2, "border-color": "#6e7681" } },
        { selector: "node.focus", style: { "border-width": 3, "border-color": "#58a6ff" } },
        // WAVE A item 2: edge labels OFF by default (a hairball of "spawned_by"/"in_repo"
        // text under every line was the actual readability problem, not the lines
        // themselves) — shown only on hover (.edge-hover) or on the selected node's own
        // incident edges (.edge-focus), both toggled by class, never by re-deriving style.
        { selector: "edge", style: {
          width: 1.5, "line-color": "#2c3744", "target-arrow-color": "#58a6ff", "target-arrow-shape": "triangle",
          "curve-style": "bezier", "arrow-scale": 0.9, label: "", "font-size": 9, color: "#8b949e",
          "text-background-color": "#0d1219", "text-background-opacity": 0.9, "text-background-padding": 2,
          "text-rotation": "autorotate", "min-zoomed-font-size": 6 } },
        { selector: "edge.edge-hover, edge.edge-focus", style: { label: "data(type)", "line-color": "#4a5a6a" } },
        // WAVE A item 3: a bundle node ("38 spawned_by") stands in for a hub's own
        // same-type edge group once it passes HUB_BUNDLE_THRESHOLD — square, dashed, so it
        // reads as a summary rather than a real object.
        { selector: "node[type='bundle']", style: {
          "background-color": "#21262d", shape: "round-rectangle", "border-style": "dashed",
          "border-color": "#8b949e", width: 44, height: 24, "font-size": 10 } },
        // WHOLE-GRAPH LOD (Thoth dispatch 9563, folded from the retired Atlas): supernode/
        // cluster pseudo-nodes size by POPULATION (raw.count), never by cytoscape degree —
        // a project with 900 members and 3 edges to other projects should still read as the
        // bigger circle. Unfiled (raw.unfiled) gets its own color, same as the Atlas always
        // gave it — the one place the eye should always find the last-resort population.
        { selector: "node[type='supernode']", style: {
          "background-color": (e) => (e.data("raw") || {}).unfiled ? UNFILED_COLOR : ty("SoftwareProject").c,
          shape: "ellipse", "border-width": 2, "border-color": "rgba(255,255,255,0.25)",
          width: (e) => sizeForCount((e.data("raw") || {}).count || 1),
          height: (e) => sizeForCount((e.data("raw") || {}).count || 1) } },
        { selector: "node[type='cluster']", style: {
          "background-color": (e) => ty((e.data("raw") || {}).type).c,
          shape: "ellipse", "border-width": 2, "border-color": "rgba(255,255,255,0.25)",
          width: (e) => sizeForCount((e.data("raw") || {}).count || 1),
          height: (e) => sizeForCount((e.data("raw") || {}).count || 1) } },
      ],
    });
    cy.on("zoom", () => cy.style().update());
    cy.on("mouseover", "edge", (e) => e.target.addClass("edge-hover"));
    cy.on("mouseout", "edge", (e) => e.target.removeClass("edge-hover"));
    // degree-based size (item 4) is only correct once every edge for this add batch has
    // landed — one style().update() per batch is enough; cytoscape coalesces same-tick add
    // events, so this never fires once per element.
    cy.on("add remove", "edge", () => cy.style().update());
    const layout = (preserve) => {
      // a DISCONNECTED set (unrelated nodes, no edges — e.g. 5 open threads) force-packs into
      // an overlapping cluster under fcose; a grid spreads them cleanly. Edges → force layout.
      const o = (cy.edges().length === 0 && cy.nodes().length > 1)
        ? { name: "grid", avoidOverlap: true, avoidOverlapPadding: 14, padding: 45, condense: false }
        : HAS_FCOSE
        ? { name: "fcose", animate: false, randomize: !preserve, quality: "proof",
            nodeSeparation: 120, idealEdgeLength: 115, nodeRepulsion: 9000, padding: 45, packComponents: true }
        : { name: "cose", animate: false, padding: 45, nodeRepulsion: 12000, idealEdgeLength: 120 };
      cy.layout(o).run();        // non-animated: positions synchronously, so we can frame NOW
      // ALWAYS resize+frame right after — animated layouts raced other events and left the
      // nodes off-viewport (the recurring "opening leads to nothing" blank board).
      cy.resize();
      cy.fit(undefined, 45);
      // WAVE A item 5: layout is the ONE act allowed to move an already-placed node — every
      // node it just positioned is now "placed", so a later incremental merge treats them as
      // real neighbors to land new nodes near, and this run's own result is saved as the
      // sticky position a future add (or reload) restores.
      cy.nodes().forEach((n) => PLACED.add(n.id()));
      savePositions();
    };
    // sticky positions — a placed node moves ONLY via layout() above (Re-layout, or an
    // empty board's own first population) or a human drag; simply adding more graph must
    // never re-shuffle what's already on screen. Persisted per-browser (localStorage, not
    // just in-memory) so a reload doesn't scramble a board someone spent time arranging.
    const POS_KEY = "osiris.board.positions";
    const loadPositions = () => {
      try { return JSON.parse(localStorage.getItem(POS_KEY) || "{}"); } catch (e) { return {}; }
    };
    const savePositions = () => {
      try {
        const pos = {};
        cy.nodes().forEach((n) => { pos[n.id()] = n.position(); });
        localStorage.setItem(POS_KEY, JSON.stringify(pos));
      } catch (e) {}
    };
    const SAVED_POS = loadPositions();
    const PLACED = new Set();
    cy.on("dragfree", "node", savePositions);
    // a freshly-added node: its own saved position wins; otherwise land it near an already-
    // PLACED neighbor (small jitter so siblings don't stack exactly on top of each other);
    // otherwise (a true isolate on a populated board) leave it near the origin for a human's
    // own explicit Re-layout to spread properly, rather than silently invoking one.
    const settleNewNode = (n) => {
      const saved = SAVED_POS[n.id()];
      if (saved) { n.position(saved); PLACED.add(n.id()); return; }
      const anchor = n.connectedEdges().connectedNodes().filter((m) => m.id() !== n.id() && PLACED.has(m.id()));
      if (anchor.length) {
        const p = anchor[0].position();
        n.position({ x: p.x + (Math.random() - 0.5) * 90, y: p.y + (Math.random() - 0.5) * 90 });
      } else {
        n.position({ x: (Math.random() - 0.5) * 40, y: (Math.random() - 0.5) * 40 });
      }
      PLACED.add(n.id());
    };
    // WAVE A item 4: an Agent node's server-supplied `agent_state` (live/idle/dead, the
    // fleet view's own window) rides along as node data when present — passed through
    // wherever a graph-fetched node becomes a cy node, never fabricated client-side.
    const nodeData = (n) => (n.agent_state
      ? { id: n.id, type: n.type, label: n.label, agent_state: n.agent_state }
      : { id: n.id, type: n.type, label: n.label });
    // WAVE A item 3: hub bundling — more than HUB_BUNDLE_THRESHOLD edges of ONE type off ONE
    // node (the "38 spawned_by" shape) collapse into one bundle node rather than 38 real
    // ones fighting the layout. Groups by (hub, direction, type); a group past threshold is
    // withheld from the normal add pass below and replaced with a single synthetic node
    // whose own data carries what it stands for, so a click can put it all back exactly.
    const HUB_BUNDLE_THRESHOLD = 12;
    const expandBundle = (bundleId) => {
      const node = cy.getElementById(bundleId);
      if (!node.length) return;
      const info = node.data("bundleOf");
      const anchorPos = node.position();
      node.connectedEdges().remove();
      node.remove();
      if (!info) return;
      const newIds = [];
      info.nodes.forEach((n) => {
        if (n && !cy.getElementById(n.id).length) { cy.add({ group: "nodes", data: nodeData(n) }); newIds.push(n.id); }
      });
      info.edges.forEach((e) => {
        const id = `${e.source}-${e.type}-${e.target}`;
        if (!cy.getElementById(id).length && cy.getElementById(e.source).length && cy.getElementById(e.target).length)
          cy.add({ group: "edges", data: { id, source: e.source, target: e.target, type: e.type } });
      });
      // land the restored nodes where the bundle itself sat, jittered apart — an expand is
      // the one case with no better anchor than "where the summary used to be".
      newIds.forEach((id) => {
        const n = cy.getElementById(id);
        if (SAVED_POS[id]) { n.position(SAVED_POS[id]); } else {
          n.position({ x: anchorPos.x + (Math.random() - 0.5) * 90, y: anchorPos.y + (Math.random() - 0.5) * 90 });
        }
        PLACED.add(id);
      });
      savePositions();
    };
    const mergeGraph = (g) => {
      const wasEmpty = cy.nodes().length === 0;
      let added = 0;
      const newIds = [];
      const claimed = new Set();       // edge keys already spoken for by a bundle
      const bundledNodeIds = new Set(); // far-node ids hidden behind a bundle
      const groups = {};
      g.edges.forEach((e) => {
        const ok = `${e.source}|out|${e.type}`, ik = `${e.target}|in|${e.type}`;
        (groups[ok] = groups[ok] || { hub: e.source, dir: "out", type: e.type, edges: [] }).edges.push(e);
        (groups[ik] = groups[ik] || { hub: e.target, dir: "in", type: e.type, edges: [] }).edges.push(e);
      });
      Object.values(groups)
        .filter((gr) => gr.edges.length > HUB_BUNDLE_THRESHOLD)
        .sort((a, b) => b.edges.length - a.edges.length)
        .forEach((gr) => {
          const fresh = gr.edges.filter((e) => !claimed.has(`${e.source}-${e.type}-${e.target}`));
          if (fresh.length <= HUB_BUNDLE_THRESHOLD) return; // an earlier, bigger bundle already ate most of it
          const bundleId = `bundle:${gr.hub}:${gr.dir}:${gr.type}`;
          if (cy.getElementById(bundleId).length) return; // already bundled from an earlier merge
          fresh.forEach((e) => claimed.add(`${e.source}-${e.type}-${e.target}`));
          const far = fresh.map((e) => (gr.dir === "out" ? e.target : e.source));
          far.forEach((id) => bundledNodeIds.add(id));
          const farNodes = far.map((id) => g.nodes.find((n) => n.id === id)).filter(Boolean);
          cy.add({ group: "nodes", data: {
            id: bundleId, type: "bundle", label: `${fresh.length} ${gr.type}`,
            bundleOf: { hub: gr.hub, dir: gr.dir, type: gr.type, nodes: farNodes, edges: fresh },
          } });
          newIds.push(bundleId);
          // the hub itself may not be on the board yet within THIS merge call (e.g. a fresh
          // placeObjects batch) — plant it now so the bundle edge has both ends to attach to.
          if (!cy.getElementById(gr.hub).length) {
            const hubNode = g.nodes.find((n) => n.id === gr.hub);
            if (hubNode) { cy.add({ group: "nodes", data: nodeData(hubNode) }); newIds.push(gr.hub); added++; }
          }
          if (cy.getElementById(gr.hub).length) {
            cy.add({ group: "edges", data: gr.dir === "out"
              ? { id: `${bundleId}-e`, source: gr.hub, target: bundleId, type: gr.type }
              : { id: `${bundleId}-e`, source: bundleId, target: gr.hub, type: gr.type } });
          }
          added++;
        });
      g.nodes.forEach((n) => {
        if (bundledNodeIds.has(n.id)) return; // hidden behind a bundle — expandBundle() adds it back
        if (!cy.getElementById(n.id).length) { cy.add({ group: "nodes", data: nodeData(n) }); newIds.push(n.id); added++; }
      });
      g.edges.forEach((e) => {
        const id = `${e.source}-${e.type}-${e.target}`;
        if (claimed.has(id)) return;
        if (!cy.getElementById(id).length && cy.getElementById(e.source).length && cy.getElementById(e.target).length)
          cy.add({ group: "edges", data: { id, source: e.source, target: e.target, type: e.type } });
      });
      // WAVE A item 5: an empty board's first population still deserves a real layout (no
      // neighbors exist yet to land near); anything added to an ALREADY-populated board
      // lands near its neighbors instead — layout() is never called here past that point,
      // so the explicit Re-layout button stays the only thing that moves a placed node.
      if (wasEmpty) { layout(false); }
      else { newIds.forEach((id) => settleNewNode(cy.getElementById(id))); savePositions(); }
      return added;
    };
    // ---- WHOLE-GRAPH LOD (Thoth dispatch 9563, folded from the retired Atlas) ---------
    // A second mode over this SAME cy instance/canvas — "Cytoscape stays the renderer",
    // Thoth's own words, never a second library or a second element. Unlike the
    // neighborhood mode above (fcose-laid-out, sticky only via drag/Re-layout), whole-
    // graph positions are NEVER computed client-side: every position comes straight from
    // the server (graph_layout.py's heartbeat for individual objects, a live centroid
    // rollup for supernodes/clusters) via cy.add({..., position}) — layout() above is
    // never called on these nodes, so a drill never re-shuffles what the heartbeat placed.
    let wgLevel = null;    // null (neighborhood mode) | "supernodes" | "clusters" | "nodes"
    let wgProject = null;  // {id, label} once drilled into one project's own clusters
    const wgClear = () => cy.elements().remove();

    async function loadSupernodes() {
      wgClear();
      wgLevel = "supernodes"; wgProject = null;
      const g = await fetch("/graph/supernodes").then((r) => r.json());
      g.supernodes.forEach((s) => {
        if (s.x == null || s.y == null) return;  // not yet positioned this heartbeat tick
        cy.add({ group: "nodes", data: {
          id: s.id, type: "supernode", label: labelWithOrphans(s.label, s.orphans), raw: s,
        }, position: { x: s.x, y: s.y } });
      });
      g.project_edges.forEach((e) => {
        const id = `${e.source}-in_repo-${e.target}`;
        if (cy.getElementById(e.source).length && cy.getElementById(e.target).length && !cy.getElementById(id).length)
          cy.add({ group: "edges", data: { id, source: e.source, target: e.target, type: "in_repo" } });
      });
      // unfiled always renders, even before the heartbeat has positioned any of its
      // members — falls back to the origin rather than being dropped, since this is the
      // ONE place the eye should always find the last-resort population.
      if (g.unfiled && g.unfiled.count > 0) {
        cy.add({ group: "nodes", data: {
          id: g.unfiled.id, type: "supernode", label: unfiledLabel(g.unfiled),
          raw: { ...g.unfiled, label: "unfiled", unfiled: true },
        }, position: { x: g.unfiled.x != null ? g.unfiled.x : 0, y: g.unfiled.y != null ? g.unfiled.y : 0 } });
      }
      cy.resize(); cy.fit(undefined, 45);
    }

    async function loadClusters(projectId, projectLabel) {
      wgClear();
      wgLevel = "clusters"; wgProject = { id: projectId, label: projectLabel };
      const g = await fetch(`/graph/clusters?project=${encodeURIComponent(projectLabel)}`).then((r) => r.json());
      g.clusters.forEach((c, i) => {
        // a cluster with no positioned member yet has no centroid — seed it in a small
        // circle around the origin rather than dropping it, so it's still clickable.
        const x = c.x != null ? c.x : Math.cos(i) * 30;
        const y = c.y != null ? c.y : Math.sin(i) * 30;
        cy.add({ group: "nodes", data: {
          id: `cluster:${projectLabel}:${c.type}`, type: "cluster",
          label: labelWithOrphans(`${c.type} (${c.count})`, c.orphans),
          raw: { ...c, project: projectLabel },
        }, position: { x, y } });
      });
      cy.resize(); cy.fit(undefined, 45);
    }

    async function loadViewportNear(x, y, span) {
      wgClear();
      wgLevel = "nodes";
      const g = await fetch("/objects/viewport?" + new URLSearchParams({
        minx: x - span, maxx: x + span, miny: y - span, maxy: y + span, limit: 500,
      })).then((r) => r.json());
      g.nodes.forEach((n) => cy.add({ group: "nodes", data: nodeData(n), position: { x: n.x, y: n.y } }));
      g.edges.forEach((e) => {
        const id = `${e.source}-${e.type}-${e.target}`;
        if (!cy.getElementById(id).length && cy.getElementById(e.source).length && cy.getElementById(e.target).length)
          cy.add({ group: "edges", data: { id, source: e.source, target: e.target, type: e.type } });
      });
      cy.resize(); cy.fit(undefined, 45);
    }

    function exitWholeGraph() { wgLevel = null; wgProject = null; wgClear(); }

    // a bundle node's own click is EXPAND, not the normal select/focus verb — it isn't a
    // real object, so onFocus (which fetches /objects/<id>) would 404 on it. A supernode/
    // cluster click DRILLS one level in (never onFocus — neither is a real object either);
    // a real positioned node at the bottom LOD level behaves exactly like neighborhood
    // mode's own tap (onFocus), the one case where whole-graph and neighborhood converge.
    cy.on("tap", "node", (e) => {
      const type = e.target.data("type");
      if (type === "bundle") { expandBundle(e.target.id()); return; }
      if (type === "supernode") {
        const raw = e.target.data("raw") || {};
        loadClusters(e.target.id(), raw.label != null ? raw.label : e.target.id());
        onGraphLevel && onGraphLevel("clusters");
        return;
      }
      if (type === "cluster") {
        const pos = e.target.position();
        loadViewportNear(pos.x, pos.y, 400);
        onGraphLevel && onGraphLevel("nodes");
        return;
      }
      onFocus && onFocus(e.target.id(), false, type);
    });
    cy.on("dbltap", "node", (e) => {
      if (e.target.data("type") === "bundle") return;
      onFocus && onFocus(e.target.id(), true, e.target.data("type"));
    });
    cy.on("cxttap", "node", (e) => {
      if (e.originalEvent) e.originalEvent.preventDefault();
      if (e.target.data("type") === "bundle") return;
      onCtx && onCtx(e.target.id(), e.target.data("type"), e.originalEvent);
    });
    return {
      cy, layout, mergeGraph,
      fit: () => cy.animate({ fit: { padding: 50 }, duration: 250 }),
      // re-measure after the container becomes visible (Cytoscape can't size a hidden #cy),
      // then frame the graph. Without this a board revealed from a panel paints blank.
      resizeFit: () => { cy.resize(); cy.fit(undefined, 40); },
      clear: () => cy.elements().remove(),
      // WHOLE-GRAPH LOD (Thoth dispatch 9563): enter via loadSupernodes(), climb back a
      // level via zoomOut() (mirrors the retired Atlas's own zoomOut — clusters -> its
      // project's supernode view, supernodes level has nowhere higher to climb), leave via
      // exitWholeGraph() (the shell's own mode toggle, back to neighborhood mode).
      loadSupernodes,
      exitWholeGraph,
      wholeGraphZoomOut() {
        if (wgLevel === "nodes" && wgProject) loadClusters(wgProject.id, wgProject.label);
        else loadSupernodes();
      },
      wholeGraphLevel: () => wgLevel,
      focusNode: (id) => {
        cy.nodes().removeClass("focus"); cy.edges().removeClass("edge-focus");
        const n = cy.getElementById(id);
        n.addClass("focus"); n.connectedEdges().addClass("edge-focus");
      },
      // WAVE A item 6: expand/collapse one hop on the SELECTION — a search-driven verb
      // distinct from focus() (which also recenters/reframes); this just widens or narrows
      // what's on the board around one already-present node.
      async expandOneHop(id) {
        if (!cy.getElementById(id).length) return 0;
        const g = await fetch(`/objects/${id}/graph?hops=1`).then((r) => r.json());
        return mergeGraph(g);
      },
      // removes every neighbor of `id` whose ONLY connection to the board is `id` itself —
      // the undo for expandOneHop's own leaves, never a node that's independently anchored
      // elsewhere (collapsing must not silently delete someone else's context).
      collapseOneHop(id) {
        const center = cy.getElementById(id);
        if (!center.length) return 0;
        const doomed = center.neighborhood("node").filter((n) => n.degree() === 1);
        const n = doomed.length;
        doomed.connectedEdges().remove();
        doomed.remove();
        savePositions();
        return n;
      },
      // place a set of {id,label,type} as nodes + the links AMONG the set only. NOT each
      // node's 1-hop neighborhood — that pulled in strangers and made the hairball. A result
      // SET renders as itself; neighborhood expansion is "search around", a separate verb.
      async placeObjects(items) {
        items.forEach((o) => {
          if (!cy.getElementById(o.id).length)
            cy.add({ group: "nodes", data: { id: o.id, type: o.type, label: o.label } });
        });
        for (const o of items) {
          const g = await fetch(`/objects/${o.id}/graph?hops=1`).then((r) => r.json());
          g.edges.forEach((e) => {  // mergeGraph already drops edges with a missing endpoint
            const id = `${e.source}-${e.type}-${e.target}`;
            if (!cy.getElementById(id).length &&
                cy.getElementById(e.source).length && cy.getElementById(e.target).length)
              cy.add({ group: "edges", data: { id, source: e.source, target: e.target, type: e.type } });
          });
        }
        layout(false);
      },
    };
  }

  // ---- THE ATLAS (Wave B, thread 8839): the FULL-GRAPH renderer ------------
  // sigma.js over graphology, vendored beside cytoscape — cytoscape/fcose stays the
  // NEIGHBOURHOOD board's own renderer (makeBoard, above): a bounded 1-hop client-computed
  // layout is exactly its job. The atlas is the opposite shape: ~41k objects, no client-
  // side layout at all — every position it draws came from the server (wave B item 1's
  // heartbeat for individual nodes, a live centroid rollup for supernodes/clusters), and
  // it never asks for more than the current LOD level needs.
  //
  // THREE LEVELS, ONE RENDERER: `zoomInto(kind, id)` drops one level (supernodes → a
  // project's clusters → that cluster's real positioned nodes via item 2's viewport
  // endpoint); `zoomOut()` climbs back. Orphans (item 3's own `orphans` field) render as a
  // dim count badge on every supernode/cluster label — distinct at every level, never
  // folded into a bare total.
  function makeAtlas(container, onDrillDown) {
    const graph = new graphology.Graph();
    const renderer = new Sigma(graph, container, {
      renderEdgeLabels: false,
      defaultNodeColor: "#6e7681",
      defaultEdgeColor: "#2c3744",
    });
    let level = "supernodes";   // "supernodes" | "clusters" | "nodes"
    let currentProject = null;  // set once we've drilled into a project's own clusters

    function clear() { graph.clear(); }

    // count -> radius: sqrt scale (area, not radius, should track count — a supernode
    // twice the population should not look four times the size).
    const sizeForCount = (n) => Math.min(40, 4 + Math.sqrt(Math.max(n, 1)) * 2.2);

    function labelWithOrphans(base, orphans) {
      return orphans ? `${base} (${orphans} orphan${orphans === 1 ? "" : "s"})` : base;
    }

    // the true orphan population can never live inside a project supernode (membership
    // itself requires an in_repo edge — the structural finding on decision 7175ef92) —
    // unfiled is where it concentrates, so its label carries BOTH counts the census
    // tracks: raw orphans and how many already carry a live derivation_abstained_*
    // record (an acknowledged disconnection, not an unexamined one).
    const UNFILED_COLOR = "#c9762c";
    function unfiledLabel(u) {
      if (!u.orphans) return "Unfiled";
      const abstainedPart = u.abstained ? `, ${u.abstained} abstained` : "";
      return `Unfiled (${u.orphans} orphan${u.orphans === 1 ? "" : "s"}${abstainedPart})`;
    }

    async function loadSupernodes() {
      clear();
      level = "supernodes"; currentProject = null;
      const g = await fetch("/graph/supernodes").then((r) => r.json());
      g.supernodes.forEach((s) => {
        if (s.x == null || s.y == null) return;  // not yet positioned this tick — appears once it is
        graph.addNode(s.id, {
          label: labelWithOrphans(s.label, s.orphans), size: sizeForCount(s.count),
          x: s.x, y: s.y, color: ty("SoftwareProject").c, kind: "project", raw: s,
        });
      });
      g.project_edges.forEach((e) => {
        if (graph.hasNode(e.source) && graph.hasNode(e.target) && !graph.hasEdge(e.source, e.target))
          graph.addEdge(e.source, e.target, { size: Math.min(6, 1 + Math.log2(e.weight + 1)) });
      });
      // unfiled always renders, even before the heartbeat has positioned any of its
      // members — it falls back to the origin rather than being dropped the way an
      // unpositioned project is, since this is the ONE place the eye should always find
      // the last-resort population (never merely absent because nothing landed yet).
      if (g.unfiled && g.unfiled.count > 0) {
        graph.addNode(g.unfiled.id, {
          label: unfiledLabel(g.unfiled), size: sizeForCount(g.unfiled.count),
          x: g.unfiled.x != null ? g.unfiled.x : 0, y: g.unfiled.y != null ? g.unfiled.y : 0,
          color: UNFILED_COLOR, kind: "project", raw: { ...g.unfiled, label: "unfiled" },
        });
      }
      renderer.refresh();
    }

    async function loadClusters(projectId, projectLabel) {
      clear();
      level = "clusters"; currentProject = { id: projectId, label: projectLabel };
      const g = await fetch(`/graph/clusters?project=${encodeURIComponent(projectLabel)}`)
        .then((r) => r.json());
      g.clusters.forEach((c, i) => {
        // a cluster with no positioned member yet has no centroid — seed it in a small
        // circle around the origin rather than dropping it, so it's still clickable.
        const x = c.x != null ? c.x : Math.cos(i) * 30;
        const y = c.y != null ? c.y : Math.sin(i) * 30;
        graph.addNode(`cluster:${projectLabel}:${c.type}`, {
          label: labelWithOrphans(`${c.type} (${c.count})`, c.orphans),
          size: sizeForCount(c.count), x, y, color: ty(c.type).c,
          kind: "cluster", raw: { ...c, project: projectLabel },
        });
      });
      renderer.refresh();
    }

    async function loadNodesNear(x, y, span) {
      clear();
      level = "nodes";
      const g = await fetch("/objects/viewport?" + new URLSearchParams({
        minx: x - span, maxx: x + span, miny: y - span, maxy: y + span, limit: 500,
      })).then((r) => r.json());
      g.nodes.forEach((n) => {
        graph.addNode(n.id, {
          label: truncateLabel(n.label), size: 6, x: n.x, y: n.y,
          color: ty(n.type).c, kind: "object", raw: n,
        });
      });
      g.edges.forEach((e) => {
        const id = `${e.source}-${e.type}-${e.target}`;
        if (graph.hasNode(e.source) && graph.hasNode(e.target) &&
            !graph.hasEdge(id) && !graph.hasEdge(e.source, e.target))
          graph.addEdgeWithKey(id, e.source, e.target, { size: 1 });
      });
      renderer.refresh();
    }

    renderer.on("clickNode", ({ node }) => {
      const attrs = graph.getNodeAttributes(node);
      if (attrs.kind === "project") {
        loadClusters(node, attrs.raw.label);
        onDrillDown && onDrillDown("clusters", attrs.raw);
      } else if (attrs.kind === "cluster") {
        loadNodesNear(attrs.x, attrs.y, 400);
        onDrillDown && onDrillDown("nodes", attrs.raw);
      } else if (onDrillDown) {
        onDrillDown("object", attrs.raw);
      }
    });

    return {
      renderer, graph,
      loadSupernodes,
      zoomOut() {
        if (level === "nodes" && currentProject) loadClusters(currentProject.id, currentProject.label);
        else loadSupernodes();
      },
      level: () => level,
      resize: () => renderer.refresh(),
    };
  }

  // ---- THE GENERIC RENDERER (P4/W1) ----------------------------------------
  // A composition Result -> the right atom, in the chosen VIEW (Notion's switchable views
  // × Palantir's multi-modal object set). `mounts` = {board, panel}. An OBJECTS set renders
  // as a clean Graph OR a Table; values/rows/data render into the panel. `onPick(id)` focuses
  // a clicked row. Returns the mode the center should show ("graph" | "panel").
  // a composition that ranks/sequences (order / take) or rolls up (aggregate) is a LIST, not
  // a graph — rendering it on the board throws away the very ordering it computed. So intent
  // wins over count (this is the Notion lesson: the view follows the data's shape).
  function isRanked(spec) {
    for (let s = spec; s && typeof s === "object"; s = s.from)
      if (s.op === "order" || s.op === "take" || s.op === "aggregate") return true;
    return false;
  }
  // the views an objects result supports — a ranked/sequenced set earns a Timeline (the order
  // it computed is the point); everything keeps Graph + Table. The shell builds the switcher
  // Universal 3-way view engine: [ Table ] [ Board ] [ Graph ]
  function viewsFor(result) {
    if (result.kind !== "objects") return [];
    return ["table", "board", "graph"];
  }
  function defaultView(result) {
    if (result.kind !== "objects") return "panel";
    if (isRanked(result.spec)) return "table";
    return result.items.length > 35 ? "table" : "graph";
  }
  async function renderResult(result, mounts, view, onPick, onDrill, onCtx) {
    const { board, panel } = mounts;
    const kind = result.kind, items = result.items;
    if (kind === "objects") {
      if (view === "board" || view === "cards") { spatialBoardGrid(panel, items, onPick, onCtx); return "panel"; }
      if (view === "table") { objectsTable(panel, items, onPick, onCtx); return "panel"; }
      if (view === "timeline") { timelineList(panel, items, onPick, onCtx); return "panel"; }
      if (board) { board.clear(); await board.placeObjects(items); }  // a CLEAN result board
      return "graph";
    }
    if (kind === "values") {
      panel.innerHTML = `<div class="r-head">${items.length} value${items.length === 1 ? "" : "s"}</div>` +
        (items.length ? `<ul class="r-list">${items.map((v) => `<li>${esc(v)}</li>`).join("")}</ul>`
          : `<div class="o-empty">Empty result.</div>`);
      return "panel";
    }
    if (kind === "rows") { renderRows(panel, items, result.spec, onDrill); return "panel"; }
    panel.innerHTML = renderData(items);  // a Function's native output, by shape
    return "panel";
  }

  // an ORDERED objects set as a Timeline — the concise read the operator asked for: a date +
  // the one salient summary per item, in the order the composition computed. NOT a column dump
  // of every property (which buried 'recent work' under full commit rationale).
  // pick the salient DATE / SUMMARY property for a timeline row — domain-NEUTRAL: a couple of
  // common preferred names, then any date-shaped property by pattern (…_date / …_at / date / time)
  // or an ISO-dated value. No hardcoded domain keys (was a mix of git/real-estate/…). The shell
  // reads the object's shape; it doesn't know what domain it is.
  const _ISO = /^\d{4}-\d{2}-\d{2}/;
  function _pickDate(p) {
    for (const k of ["authored_date", "observed_at", "created_at", "date"]) if (p && p[k]) return p[k];
    for (const k in p) if (p[k] && (/(_date|_at|^date$|time)/i.test(k) || _ISO.test(String(p[k])))) return p[k];
    return null;
  }
  function _pickSummary(p) {
    for (const k of ["summary", "title", "subject", "description", "label"]) if (p && p[k]) return p[k];
    return null;
  }
  function timelineList(panel, items, onPick, onCtx) {
    if (!items.length) { panel.innerHTML = `<div class="o-empty">Empty result.</div>`; return; }
    // NO SILENT CAPS (the same law _capped's own SECTION_CAP/_more enforce for "data" mode,
    // below): "objects" mode used to render every item verbatim — 305 threads, full-paragraph
    // summaries, one unbroken scroll — while "data" mode next to it capped at 12 and SAID what
    // it withheld. Two renderers of the same law reading differently is itself the bug; reusing
    // SECTION_CAP/_more (never a second number, never a second idiom) is what makes them read
    // the same again.
    const shown = items.slice(0, SECTION_CAP);
    const body = shown.map((o, i) => {
      const p = o.props || {};
      const d = _pickDate(p), s = _pickSummary(p);
      const when = d ? esc(String(d).slice(0, 10)) : `#${i + 1}`;
      const sum = s ? `<div class="tl-sum">${esc(String(s).slice(0, 160))}</div>` : "";
      return `<div class="tl-item" data-pick="${o.id}" data-type="${esc(o.type)}" title="${esc(o.label || "")}">
        <span class="tl-when">${when}</span>
        <div class="tl-main"><span class="o-faint">${esc(o.type)}</span> ${esc(o.display_label || o.label)}${sum}</div></div>`;
    }).join("");
    panel.innerHTML = `<div class="r-head">${items.length} item${items.length === 1 ? "" : "s"} · in order</div>
      <div class="tl">${body}</div>` + (items.length > shown.length ? _more(items.length - shown.length) : "");
    _wireRows(panel, onPick, onCtx);
  }

  // an objects set as a TABLE — Type · Name · the most-common property columns. The fix for
  // the 80-node hairball: a scannable set, each row clickable into the inspector.
  // wire a result panel's [data-pick] rows: click = select (inspect), right-click = the
  // object's contextual action menu. onCtx(id, type, mouseEvent) — same menu as the set list.
  function _wireRows(panel, onPick, onCtx) {
    panel.querySelectorAll("[data-pick]").forEach((el) => {
      el.onclick = () => onPick && onPick(el.dataset.pick);
      el.oncontextmenu = (e) => { if (onCtx) { e.preventDefault(); onCtx(el.dataset.pick, el.dataset.type, e); } };
    });
  }
  // CONTEXTUAL COLUMNS (operator, 2026-07-11, screenshotting his own composer: "contextual
  // chrome?"). The old table chose its columns by FREQUENCY — and frequency is exactly
  // backwards: a property that is present on EVERY row with the SAME value scores highest and
  // is worth nothing. Running `open threads` produced six columns of which four were void:
  // TYPE ('Thread' ×974), STATUS ('open' ×974 — it was the FILTER), SOURCE_MODEL (empty on
  // every row), and SUMMARY (byte-identical to NAME). Half the width was a mirror of itself.
  //
  // A column now earns its place by VARYING. Constant-across-every-row → it isn't a column,
  // it's a fact about the whole set: it moves to a chip in the header. Empty everywhere →
  // gone. A mirror of Name → gone. What's left is the part of the result that differs, which
  // is the only part anyone reads.
  function _tableShape(items) {
    const skip = new Set(["name", "demo", "tag"]);
    const seen = {}, filled = {};
    items.forEach((o) => Object.entries(o.props || {}).forEach(([k, v]) => {
      if (skip.has(k)) return;
      const s = v == null ? "" : String(v).trim();
      if (!s) return;                                     // empty is not evidence
      (seen[k] = seen[k] || new Set()).add(s);
      filled[k] = (filled[k] || 0) + 1;
    }));
    const mirrorsName = (k) => items.every((o) => {
      const v = (o.props || {})[k];
      const s = v == null ? "" : String(v).trim();
      return !s || s === String(o.label || "").trim();
    });
    const chips = [];
    const cols = Object.keys(seen).filter((k) => {
      if (mirrorsName(k)) return false;                   // a second copy of Name
      // one value, and every row has it → a property of the SET, not of any row
      if (seen[k].size === 1 && filled[k] === items.length) {
        chips.push(`${k}=${[...seen[k]][0]}`);
        return false;
      }
      return true;                                        // it varies: it earns a column
    }).sort((a, b) => filled[b] - filled[a]).slice(0, 4);
    const types = [...new Set(items.map((o) => o.type))];
    if (types.length === 1 && items.length) chips.unshift(types[0]);
    return { cols, chips, showType: types.length > 1 };
  }

  function spatialBoardGrid(panel, items, onPick, onCtx) {
    if (!items.length) { panel.innerHTML = `<div class="o-empty">Empty result.</div>`; return; }
    const shown = items.slice(0, 200);
    
    // Group items into spatial lanes
    const lanes = [
      { id: 'obligations', name: 'Duties & Tasks', items: [] },
      { id: 'decisions', name: 'Rulings & Decisions', items: [] },
      { id: 'entities', name: 'Entities & Codebases', items: [] },
      { id: 'historical', name: 'Historical & Settled', items: [] }
    ];

    shown.forEach(o => {
      const p = o.props || {};
      const status = (o.status || p.status || 'active').toLowerCase();
      const type = (o.type || '').toLowerCase();
      if (status === 'historical' || status === 'retired' || status === 'resolved') {
        lanes[3].items.push(o);
      } else if (type === 'thread' || type === 'obligation' || type === 'task') {
        lanes[0].items.push(o);
      } else if (type === 'decision' || type === 'practice' || type === 'reference' || type === 'blindspot' || type === 'superstition') {
        lanes[1].items.push(o);
      } else {
        lanes[2].items.push(o);
      }
    });

    const activeLanes = lanes.filter(l => l.items.length > 0);
    const lanesToRender = activeLanes.length ? activeLanes : lanes;

    const lanesHtml = lanesToRender.map(l => {
      const cardsHtml = l.items.map(o => {
        const m = ty(o.type);
        const p = o.props || {};
        const source = p.source_id || p.source_label || p.source || "";
        const grade = (p.evidence_class || p.grade || "self_declared").toLowerCase();
        const summary = p.summary || p.rationale || p.statement || p.description || p.title || "";
        const shortId = o.canonical ? (o.canonical.includes(":") ? o.canonical.split(":")[1] : o.canonical) : o.id.slice(0, 8);
        return `<div class="board-card" data-pick="${esc(o.id)}" data-type="${esc(o.type)}" style="border-top: 2px solid ${m.c}">
          <div class="card-tags-top">
            <span class="card-tag card-tag-type" style="color:${m.c};background:${m.c}18;border-color:${m.c}40">${esc(o.type)}</span>
            <span class="card-tag card-tag-id">${esc(shortId)}</span>
          </div>
          <div class="card-title">${esc(o.display_label || o.label || summary.slice(0, 85))}</div>
          ${summary && summary !== o.label ? `<div class="card-desc">${esc(summary)}</div>` : ''}
          <div class="card-tags-bottom">
            <span class="card-tag card-tag-status status-${esc(o.status || p.status || 'active')}">${esc(o.status || p.status || 'active')}</span>
            ${grade ? `<span class="card-tag card-tag-grade grade-${esc(grade)}">${esc(grade.replace(/_/g, ' '))}</span>` : ''}
            ${source ? `<span class="card-tag card-tag-source">by ${esc(source)}</span>` : ''}
          </div>
        </div>`;
      }).join('');

      return `<div class="board-lane">
        <div class="board-lane-head">
          <span class="lane-name">${esc(l.name)}</span>
          <span class="lane-badge">${l.items.length}</span>
        </div>
        <div class="board-lane-items">
          ${cardsHtml || '<div class="lane-empty">No items</div>'}
        </div>
      </div>`;
    }).join('');

    panel.innerHTML = `<div class="spatial-board">${lanesHtml}</div>` + 
      (items.length > shown.length ? _more(items.length - shown.length) : "");
    _wireRows(panel, onPick, onCtx);
  }

  function cardsGrid(panel, items, onPick, onCtx) {
    spatialBoardGrid(panel, items, onPick, onCtx);
  }

  function objectsTable(panel, items, onPick, onCtx) {
    // same NO-SILENT-CAPS treatment as timelineList above, and the same reused SECTION_CAP/
    // _more — column shape is computed from the SHOWN slice, matching _capped's own
    // table(shown) precedent, not the full set a reader never sees past row 12 anyway.
    const shown = items.slice(0, SECTION_CAP);
    const { cols, chips, showType } = _tableShape(shown);
    const head = (showType ? "<th>Type</th>" : "") + "<th>Name</th>" +
      cols.map((c) => `<th>${esc(c)}</th>`).join("");
    const cell = (v, full) => `<td title="${esc(full != null ? full : v)}"><span class="clamp">${esc(v)}</span></td>`;
    const body = shown
      .map((o) => `<tr data-pick="${o.id}" data-type="${esc(o.type)}" style="cursor:pointer">
        ${showType ? `<td><span class="o-faint">${esc(o.type)}</span></td>` : ""}${cell(o.display_label || o.label || "", o.label)}
        ${cols.map((c) => cell((o.props || {})[c] || "")).join("")}</tr>`)
      .join("");
    const chipbar = chips.map((c) => `<span class="r-chip">${esc(c)}</span>`).join("");
    panel.innerHTML =
      `<div class="r-head">${items.length} object${items.length === 1 ? "" : "s"}` +
      (chips.length ? ` <span class="o-faint">— all share</span> ${chipbar}` : "") + "</div>" +
      (items.length ? `<table class="r-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>` +
        (items.length > shown.length ? _more(items.length - shown.length) : "")
        : `<div class="o-empty">Empty result.</div>`);
    _wireRows(panel, onPick, onCtx);
  }

  // aggregate rows: [{group:{prop:val,...}, metric:N}] -> a ranked table where each row DRILLS
  // INTO the objects it counts (the missing interactive primitive — 'changelog by area' was a
  // dead list; now clicking 'composer · 6' opens those 6 commits). onDrill(group, spec) is the
  // shell's hook back to a select filtered by the group.
  function renderRows(panel, rows, spec, onDrill) {
    if (!rows || !rows.length) { panel.innerHTML = `<div class="o-empty">No rows.</div>`; return; }
    // TWO row shapes share the "rows" kind: an `aggregate` yields {group, metric} (a ranked,
    // drill-able table); a `table` op yields a FLAT column dict per object. Render each as itself
    // — the flat table through the generic column renderer, so any `table` composition just works.
    const agg = Object.prototype.hasOwnProperty.call(rows[0], "group")
      && Object.prototype.hasOwnProperty.call(rows[0], "metric");
    if (!agg) {
      panel.innerHTML = `<div class="r-head">${rows.length} row${rows.length === 1 ? "" : "s"}</div>` +
        table(rows);
      return;
    }
    const dims = Object.keys(rows[0].group || {});
    const head = dims.map((d) => `<th>${esc(d)}</th>`).join("") + "<th>metric</th>";
    const body = rows
      .map((r) => `<tr style="cursor:pointer">${dims.map((d) =>
        `<td>${esc(r.group[d] || "(none)")}</td>`).join("")}<td class="r-num">${esc(r.metric)}</td></tr>`)
      .join("");
    panel.innerHTML = `<div class="r-head">${rows.length} group${rows.length === 1 ? "" : "s"} ·
      click a row to open its objects</div>
      <table class="r-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
    panel.querySelectorAll("tbody tr").forEach((tr, i) =>
      (tr.onclick = () => onDrill && onDrill(rows[i].group, spec)));
  }

  // a Function's output, rendered generically by shape (no per-Function knowledge).
  //
  // TWO BUGS THIS FIXES, both visible in one 4K screenshot of `briefing` (operator, 2026-07-11,
  // "feng sui"):
  //  (1) THE SILENTLY DROPPED SECTION. The old grouper kept only ARRAY-valued keys — so the
  //      wall, whose section is a DICT (totals + projects + top_of_wall), was filtered out and
  //      never drawn. The briefing announced "3 sections" and rendered two, and the one it ate
  //      was the most important one. A renderer must never discard a shape it doesn't expect;
  //      it must render it AS ITSELF (scalars → chips, lists → tables, dicts → recurse).
  //  (2) THE UNBOUNDED DUMP. "Resolved — self-healed by later commits" printed all 794 rows
  //      inline, forever. Same law as his desk and the garden: LAND ON COUNTS, WALK IN. Every
  //      section is capped, and — per the no-silent-caps ruling — it SAYS what it withheld.
  const SECTION_CAP = 12;

  function _more(n) {
    return `<div class="r-more">+${n} more — filter, or ⑂ fan out, to see the rest</div>`;
  }
  function _capped(list) {
    if (!list.length) return `<div class="o-empty">—</div>`;
    const shown = list.slice(0, SECTION_CAP);
    return table(shown) + (list.length > shown.length ? _more(list.length - shown.length) : "");
  }
  function renderData(data, depth = 0) {
    if (data == null) return `<div class="o-empty">Empty.</div>`;
    if (Array.isArray(data)) return data.length ? _capped(data) : `<div class="o-empty">No results.</div>`;
    if (typeof data === "object") {
      const ent = Object.entries(data);
      // a scalar leaf is a FACT ABOUT THE SECTION (974 open · 302 obligations), not a row —
      // it belongs on the header as a chip, the same law the object table now follows.
      const facts = ent.filter(([, v]) => v == null || typeof v !== "object");
      const blocks = ent.filter(([, v]) => v && typeof v === "object");
      const long = facts.filter(([, v]) => typeof v === "string" && String(v).length > 60);
      const chips = facts.filter((e) => !long.includes(e));
      let out = "";
      if (chips.length)
        out += `<div class="r-facts">${chips.map(([k, v]) =>
          `<span class="r-chip"><b>${esc(v)}</b> ${esc(k)}</span>`).join("")}</div>`;
      if (long.length)
        out += long.map(([, v]) => `<div class="r-note">${esc(v)}</div>`).join("");
      out += blocks.map(([k, v]) => {
        const title = esc(k).replace(/_/g, " ");
        const count = Array.isArray(v) ? `<span class="o-faint" style="font-size:11.5px">(${v.length})</span>` : "";
        if (depth === 0) {
          return `<div class="sec-card">
            <div class="sec-card-head">
              <div class="sec-card-title">${title} ${count}</div>
            </div>
            ${renderData(v, depth + 1)}
          </div>`;
        }
        return `<div class="r-group"><h3 style="font-size:12px;margin:10px 0 6px">${title} ${count}</h3>${renderData(v, depth + 1)}</div>`;
      }).join("");
      return out || `<div class="o-empty">Empty.</div>`;
    }
    return `<div class="r-head">${esc(data)}</div>`;
  }

  // a list of dicts -> a table (columns = union of keys; arrays/objects flattened).
  // Same two laws the object table follows, because they are laws and not special cases:
  //   · a column earns its place by VARYING — an empty column is gone, a constant one becomes
  //     a chip above the table (it is a fact about the SET, not about any row);
  //   · width is decided by CONTENT, never position — a column whose longest value is short is
  //     marked .r-tight and shrinks to fit, so the prose column takes the width it needs
  //     instead of splitting a 4K panel evenly with a one-word `scope`.
  // NESTED CELL VALUES (task #109's tail, Thoth DM 2145; compositions.py:2216's own
  // documented gap — "neither render_composition nor osiris.js's table() recurse into a
  // nested list/dict CELL value"): a raw JSON.stringify blob or an "[object Object]"-joined
  // mush is worse than not showing it at all. Flattened into the SAME compact
  // "key=value, key=value" prose _fleet_doors_summary/_fleet_ancestors_summary already
  // hand-roll per-Function server-side (compositions.py) — generalized here so no Function
  // needs its own summarizer just to keep a nested field out of the generic table's way.
  // Capped at 2 levels deep (a 3rd level collapses to a count or "{…}", never recurses
  // forever) and a few items per list — a genuinely deep structure degrades to a number
  // rather than an unreadable wall, same economy _fleet_doors_summary's own 4-item cap
  // already established. Lands inside cell()'s EXISTING clamp+tooltip once it runs long
  // (below) — nothing new needed there, only the text itself had to stop lying.
  const NEST_ITEMS_CAP = 4;
  // THE RESERVED UNAVAILABLE MARKER (thread 04c651ce item 2, Thoth dispatch msg 9123):
  // compositions.py's `_unavailable(reason)` nests {"_unavailable": reason} on a field that
  // genuinely could not be computed (a PARTIAL failure — real data sits right beside it in
  // the same row/result) — a reserved leading-underscore key, checked by KEY same as this
  // file's own `_action`/`_actions` row-control convention, never by sniffing text content
  // for the word "unavailable" (which a real value could legitimately contain). table()'s
  // cell() strips it to a distinct dimmed marker instead of flattening it as if it were
  // real nested JSON; a programmatic reader checks the raw JSON's own `_unavailable` key.
  const UNAVAILABLE_KEY = "_unavailable";
  const isUnavailable = (v) =>
    !!(v && typeof v === "object" && !Array.isArray(v) && UNAVAILABLE_KEY in v);
  function _hasNestedObject(v) {
    return Array.isArray(v) ? v.some((x) => x && typeof x === "object") : !!(v && typeof v === "object");
  }
  function _flatObj(o, depth) {
    const parts = Object.entries(o)
      .filter(([k, val]) => val != null && val !== "" && k !== "_action" && k !== "_actions")
      .map(([k, val]) => `${k}=${_flatVal(val, depth)}`);
    return parts.length ? parts.join(", ") : "(none)";
  }
  function _flatVal(v, depth) {
    if (v == null) return "";
    if (Array.isArray(v)) {
      if (!v.length) return "";
      if (depth >= 2) return `${v.length} item${v.length === 1 ? "" : "s"}`;
      const shown = v.slice(0, NEST_ITEMS_CAP)
        .map((x) => (x && typeof x === "object" ? _flatObj(x, depth + 1) : String(x)));
      return shown.join("; ") + (v.length > NEST_ITEMS_CAP ? `, +${v.length - NEST_ITEMS_CAP} more` : "");
    }
    if (typeof v === "object") return depth >= 2 ? "{…}" : _flatObj(v, depth + 1);
    return String(v);
  }
  const _txt = (v) => {
    if (typeof v === "string" && /^\d{4}-\d{2}-\d{2}T/.test(v)) v = v.slice(0, 10);   // ISO → date
    if (v == null) return "";
    if (isUnavailable(v)) return "unavailable";  // stripped: never flattened as if it were data
    if (Array.isArray(v) && !_hasNestedObject(v)) return v.join(", ");  // unchanged: flat list
    if (Array.isArray(v) || typeof v === "object") return _flatVal(v, 0);
    return String(v);
  };
  const TIGHT = 24;                                // a column whose widest value fits in a glance

  // row_action's CLIENT half (ruling c5b184cd, thread e5d1eb6d) — the server half
  // (compositions._table/`function` op) has attached `_action:{action,args}` to a row since
  // #44/89df464; table() must treat it as a CONTROL, never a column, or it renders as its own
  // JSON.stringify'd blob (the exact bug live-desk shipped with — nobody clicked resolve on
  // /ui, so nobody noticed). Label map mirrors chrome.py's _ACTION_LABELS verbatim — same
  // registry, same cosmetic names, one less thing to keep in sync by hand than it looks: any
  // action.js can't render for whatever reason falls back to its own verb name unlabeled.
  const ACTION_LABELS = { resolve_thread: "resolve", assign_thread: "not mine",
    defer_thread: "later", reclassify_thread: "reclassify", settle: "settle" };
  // esc() alone is not attribute-safe — it never escapes '"', and every value here lands
  // inside a double-quoted data-args attribute carrying raw JSON.stringify output (which is
  // built almost entirely OF '"' characters). The browser decodes &quot; back to '"' when it
  // parses the attribute, so JSON.parse(el.dataset.args) still sees the original string.
  const escAttr = (s) => esc(s).replace(/"/g, "&quot;");
  // A "run:<function>" action is NAVIGATION, not a verb — no fixed cosmetic name belongs in
  // ACTION_LABELS for it (that would mean hardcoding a function name into the frozen module,
  // exactly the per-page special-casing this architecture refuses). Generic instead: strip
  // the prefix, underscores to spaces — "run:mail_threads" reads as "mail threads".
  function _actionLabel(name) {
    if (name.startsWith("run:")) return name.slice(4).replace(/_/g, " ");
    return ACTION_LABELS[name] || name;
  }
  function _actionButton(action, label) {
    const name = action.action || "";
    // `subject` (Thoth dispatch 9676/9690, 588148bb piece 4) — the row's OWN object as a
    // "run:" target's bound subject, mutually exclusive with `args` (an op-tree target has
    // no Function to drill into via run-spec's {"op":"function"} wrapping; see compositions.
    // py's `bind_subject` docstring). Carried as its own data attribute, never folded into
    // data-args, so the click delegate can tell the two navigation modes apart.
    const subjAttr = action.subject ? ` data-subject="${escAttr(action.subject)}"` : "";
    return `<button class="r-act-btn" data-action="${escAttr(name)}" ` +
      `data-args="${escAttr(JSON.stringify(action.args || {}))}"${subjAttr}>` +
      `${esc(label || _actionLabel(name))}</button>`;
  }
  // `_actions` (plural, Thoth msg 1976/2029) — a row that affords MORE than one verb (chrome's
  // /desk: done/not mine/later on one debt). Same click delegate, same POST /act, same button
  // markup as the singular form — this is N of the same control, not a new mechanism, so no
  // second delegate and no DOM event: unlike "run:" (navigation, page-state, had to hand off),
  // a write stays entirely inside what the click delegate already does per button.
  function _actionsButtons(actions) {
    return actions.map((a) => _actionButton(a, a.label)).join("");
  }
  // a lightweight, self-built toast — this library has no host page to ask for one (osiris.js
  // is the frozen surface the composer just calls into), so it mounts its own corner and cleans
  // up after itself rather than assuming index.html has somewhere to put a message.
  function toast(msg, isError) {
    let box = document.getElementById("o-toast");
    if (!box) {
      box = document.createElement("div");
      box.id = "o-toast";
      document.body.appendChild(box);
    }
    const item = document.createElement("div");
    item.className = "o-toast-item" + (isError ? " err" : "");
    item.textContent = msg;
    box.appendChild(item);
    setTimeout(() => item.remove(), 3000);
  }
  // THE CLICK DELEGATE — mirrors chrome.py's _ACTIONS script exactly (same POST /act shape,
  // same disable-on-click guard against a double-fire before the request resolves). Installed
  // ONCE at module load on `document`, never re-wired per render: every composition run
  // replaces `panel.innerHTML` wholesale, and a delegate on `document` survives that for free
  // — no rebind after each render, no risk of a listener stacking on a node about to be thrown
  // away.
  document.addEventListener("click", async (e) => {
    const b = e.target.closest("button[data-action]");
    if (!b) return;
    e.preventDefault();
    let args;
    try { args = JSON.parse(b.dataset.args || "{}"); } catch { args = {}; }
    // NAVIGATION, not a mutation (task #90, Thoth msg 1976/2005) — a "run:<function>" action
    // invokes-and-SHOWS a Result rather than writing through /act: a materially different
    // response shape (a whole new board, not a toast + row removal). This module has no
    // access to RESULT/WORKING/renderCurrent — those are page (index.html) state, same
    // boundary renderResult's own inspectOnly/drillInto callback params already respect — so
    // it only recognizes the prefix and hands off via a DOM event; the shell does the run.
    if (b.dataset.action.startsWith("run:")) {
      document.dispatchEvent(new CustomEvent("osiris:run",
        { detail: { name: b.dataset.action.slice(4), args, subject: b.dataset.subject || null } }));
      return;
    }
    const was = b.textContent;
    b.disabled = true;
    b.textContent = "…";
    try {
      const r = await fetch("/act", {
        method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ action: b.dataset.action, args }),
      }).then((res) => res.json());
      if (r && r.error) { toast(r.error, true); b.disabled = false; b.textContent = was; return; }
      toast(`${_actionLabel(b.dataset.action)} — done`);
      // the row's own fact no longer holds (the graph write already landed, server-confirmed)
      // — remove it now rather than wait on a poll this composition may not even be running.
      b.closest("tr")?.remove();
    } catch (err) {
      toast("action failed — " + err, true);
      b.disabled = false;
      b.textContent = was;
    }
  });

  function table(list) {
    const keys = [...new Set(list.flatMap((o) => (o && typeof o === "object" ? Object.keys(o) : [])))]
      .filter((k) => k !== "_action" && k !== "_actions");
    if (!keys.length) return `<ul class="r-list">${list.map((v) => `<li>${esc(v)}</li>`).join("")}</ul>`;
    const vals = {}, widest = {};
    keys.forEach((k) => {
      const seen = list.map((o) => _txt(o ? o[k] : "")).filter((s) => s !== "");
      vals[k] = new Set(seen);
      widest[k] = seen.reduce((m, s) => Math.max(m, s.length), 0);
    });
    const chips = [];
    const cols = keys.filter((k) => {
      if (!vals[k].size) return false;                                    // empty everywhere
      if (vals[k].size === 1 && vals[k].size === list.length) return true;  // 1 row: keep it
      if (vals[k].size === 1 && list.length > 1) {                        // constant → a chip
        chips.push(`${k}=${[...vals[k]][0]}`);
        return false;
      }
      return true;
    });
    if (!cols.length) return `<div class="o-empty">—</div>`;
    // .r-table td .clamp (osiris.css) is a proper 2-line clamp+ellipsis, word-wrapped — built
    // 2026-07-11 for objectsTable's own cells, but table() never applied it, so a MEDIUM string
    // (short of the >160 "wall of text" bar below) sailed through untouched. In a many-column
    // table, table-layout:auto starves a non-tight column thin, and overflow-wrap:anywhere +
    // word-break:break-word then break it mid-word with nowhere else to go — "1 door (session
    // 82d04858 2s ago)" towering into ten near-single-character lines. TIGHT (24, above) is
    // already this file's own line for "short enough to trust at a glance, never wraps" — a
    // .r-tight COLUMN's cells are by definition all <= TIGHT chars, so they never cross this
    // same bar and the clamp's own `white-space` never fights a tight column's `nowrap`. Reusing
    // it here (rather than a fresh magic number) means anything past "glanceable" gets two real,
    // word-wrapped lines instead of a starved column's only remaining option: mid-word carnage.
    const cell = (v) => {
      if (isUnavailable(v))  // stripped to a distinct dimmed marker, the real reason on hover
        return `<span class="o-faint" title="${esc(v[UNAVAILABLE_KEY])}">unavailable</span>`;
      const s = _txt(v);
      if (s.length > 160)                       // a genuine wall of text: hard-cap the DOM weight
        return `<span class="clamp" title="${esc(s)}">${esc(s.slice(0, 157))}…</span>`;
      if (s.length > TIGHT)                      // medium prose: 2 lines, word-wrapped, not char-by-char
        return `<span class="clamp" title="${esc(s)}">${esc(s)}</span>`;
      return esc(s);
    };
    const cls = (k) => (widest[k] <= TIGHT ? ' class="r-tight"' : "");
    const rowActions = (o) =>
      (o && o._action && o._action.action ? _actionButton(o._action) : "") +
      (o && Array.isArray(o._actions) && o._actions.length ? _actionsButtons(o._actions) : "");
    const hasAction = list.some((o) => rowActions(o) !== "");
    return (chips.length
      ? `<div class="r-facts">${chips.map((c) => `<span class="r-chip">${esc(c)}</span>`).join("")}</div>`
      : "") +
      `<table class="r-table"><thead><tr>${cols.map((c) => `<th${cls(c)}>${esc(c)}</th>`).join("")}` +
      `${hasAction ? "<th></th>" : ""}</tr></thead>
      <tbody>${list.map((o) => `<tr>${cols.map((c) => `<td${cls(c)}>${cell(o ? o[c] : "")}</td>`).join("")}` +
      `${hasAction ? `<td>${rowActions(o)}</td>` : ""}` +
      `</tr>`).join("")}</tbody></table>`;
  }

  return { $, esc, pct, OPSYM, loadSchema, ty, objectDetail, loadRels, makeBoard, makeAtlas,
    renderResult, viewsFor, defaultView, lineage, innerSelect, cardsGrid };
})();
