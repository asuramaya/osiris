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
  // evidence_class -> a plain-language "how obtained" (provenance is the whole point)
  const HOW = {
    authoritative_api: "authoritative", self_declared: "self-declared",
    direct_observation: "observed", co_occurrence: "co-occurrence",
    derived: "inferred", corroborated: "corroborated",
  };

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

  // ---- atoms ---------------------------------------------------------------
  // a graded property row: value + WHERE IT CAME FROM (source · how · confidence)
  function propRow(p) {
    return `<div class="o-k">${esc(p.name)}</div><div class="o-v">${esc(p.value)}</div>
      <div class="o-pv">${esc(p.source_label || p.source_id || "—")} · ${esc(p.how || "—")} · ${pct(p.confidence)}</div>`;
  }

  // the object detail (the one noun) — type chip, title, graded facts, slots for rels.
  // `acts` is HTML for action buttons the shell injects (search-around, dossier, …).
  function objectDetail(o, acts = "") {
    const nameP = o.properties.find((p) => p.name === "name");
    const demo = o.properties.some((p) => p.name === "demo" && String(p.value).toLowerCase() === "true");
    const title = (nameP && nameP.value) || o.canonical;
    const m = ty(o.type);
    const facts = o.properties.filter((p) => !["name", "demo", "tag"].includes(p.name));
    const pv = facts.map(propRow).join("") || `<div class="o-muted" style="grid-column:1/4">No properties.</div>`;
    return `
      <div class="o-top">
        <span class="o-type" style="color:${m.c};background:${m.c}1e;border:1px solid ${m.c}55">${esc(o.type)}</span>
        ${demo ? '<span class="o-demo">DEMO</span>' : ""}
        <div class="o-title">${esc(title)}</div>
        <div class="o-canon">${esc(o.canonical)}</div>
        ${acts ? `<div class="o-acts">${acts}</div>` : ""}
      </div>
      <div class="o-sect"><h3>Properties · what &amp; how</h3><div class="o-pvgrid">${pv}</div></div>
      <div class="o-sect"><h3>Relationships</h3><div data-rels class="o-muted">…</div></div>`;
  }

  // walk the 1-hop neighbourhood into clickable relationship rows
  async function loadRels(el, id, onPick) {
    const g = await fetch(`/objects/${id}/graph?hops=1`).then((r) => r.json());
    const lab = {}; g.nodes.forEach((n) => (lab[n.id] = n));
    el.innerHTML =
      g.edges
        .filter((e) => e.source === id || e.target === id)
        .map((e) => {
          const out = e.source === id, other = out ? e.target : e.source, n = lab[other] || { label: other, type: "?" };
          return `<div class="o-rel"><span class="o-faint">${out ? "→" : "←"}</span>
            <span class="o-reltype">${esc(e.type)}</span>
            <a data-pick="${other}" style="cursor:pointer">${esc(n.label)}</a>
            <span class="o-faint">${esc(n.type)}</span></div>`;
        })
        .join("") || '<span class="o-faint">No links.</span>';
    el.querySelectorAll("[data-pick]").forEach((a) => (a.onclick = () => onPick && onPick(a.dataset.pick)));
  }

  // ---- the cytoscape board (objects render here) ---------------------------
  function makeBoard(container, onFocus) {
    if (window.cytoscapeFcose) cytoscape.use(window.cytoscapeFcose);
    const HAS_FCOSE = !!window.cytoscapeFcose;
    const cy = cytoscape({
      container, wheelSensitivity: 0.2, minZoom: 0.15, maxZoom: 3,
      style: [
        { selector: "node", style: {
          "background-color": (e) => ty(e.data("type")).c, shape: (e) => ty(e.data("type")).s,
          width: 30, height: 30, "border-width": 0, label: "data(label)", color: "#cdd6df", "font-size": 10.5,
          "text-valign": "bottom", "text-margin-y": 4, "text-wrap": "wrap", "text-max-width": 110,
          "text-background-color": "#0b0e13", "text-background-opacity": 0.7, "text-background-padding": 3,
          "text-background-shape": "roundrectangle", "min-zoomed-font-size": 7 } },
        { selector: "node.focus", style: { "border-width": 3, "border-color": "#4493f8" } },
        { selector: "edge", style: {
          width: 1.2, "line-color": "#2c3744", "target-arrow-color": "#2c3744", "target-arrow-shape": "triangle",
          "curve-style": "bezier", "arrow-scale": 0.85, label: "data(type)", "font-size": 8.5, color: "#7d8896",
          "text-background-color": "#0b0e13", "text-background-opacity": 0.85, "text-background-padding": 2,
          "text-rotation": "autorotate", "min-zoomed-font-size": 7 } },
      ],
    });
    const layout = (preserve) => {
      const o = HAS_FCOSE
        ? { name: "fcose", animate: true, animationDuration: 300, randomize: !preserve, quality: "proof",
            nodeSeparation: 120, idealEdgeLength: 115, nodeRepulsion: 9000, padding: 45, packComponents: true }
        : { name: "cose", animate: false, padding: 45, nodeRepulsion: 12000, idealEdgeLength: 120 };
      cy.layout(o).run();
    };
    const mergeGraph = (g) => {
      let added = 0;
      g.nodes.forEach((n) => {
        if (!cy.getElementById(n.id).length) { cy.add({ group: "nodes", data: { id: n.id, type: n.type, label: n.label } }); added++; }
      });
      g.edges.forEach((e) => {
        const id = `${e.source}-${e.type}-${e.target}`;
        if (!cy.getElementById(id).length && cy.getElementById(e.source).length && cy.getElementById(e.target).length)
          cy.add({ group: "edges", data: { id, source: e.source, target: e.target, type: e.type } });
      });
      return added;
    };
    cy.on("tap", "node", (e) => onFocus && onFocus(e.target.id()));
    cy.on("dbltap", "node", (e) => onFocus && onFocus(e.target.id(), true));
    return {
      cy, layout, mergeGraph,
      fit: () => cy.animate({ fit: { padding: 50 }, duration: 250 }),
      clear: () => cy.elements().remove(),
      focusNode: (id) => { cy.nodes().removeClass("focus"); cy.getElementById(id).addClass("focus"); },
      // place a set of {id,label,type} as nodes, then draw whatever edges exist among them
      async placeObjects(items) {
        items.forEach((o) => {
          if (!cy.getElementById(o.id).length)
            cy.add({ group: "nodes", data: { id: o.id, type: o.type, label: o.label } });
        });
        for (const o of items) mergeGraph(await fetch(`/objects/${o.id}/graph?hops=1`).then((r) => r.json()));
        layout(false);
      },
    };
  }

  // ---- THE GENERIC RENDERER (P4) -------------------------------------------
  // A composition Result -> the right atom. `mounts` = {board, panel}. objects go on the
  // board (graph); values/rows/data render into the panel. The shell decides which to show.
  async function renderResult(result, mounts) {
    const { board, panel } = mounts;
    const kind = result.kind, items = result.items;
    if (kind === "objects") {
      if (board) await board.placeObjects(items);
      return "graph";
    }
    if (kind === "values") {
      panel.innerHTML = `<div class="r-head">${items.length} value${items.length === 1 ? "" : "s"}</div>` +
        (items.length ? `<ul class="r-list">${items.map((v) => `<li>${esc(v)}</li>`).join("")}</ul>`
          : `<div class="o-empty">Empty result.</div>`);
      return "panel";
    }
    if (kind === "rows") {
      panel.innerHTML = renderRows(items);
      return "panel";
    }
    // data — a Function's native output (list of dicts / dict of lists / scalar tree)
    panel.innerHTML = renderData(items);
    return "panel";
  }

  // aggregate rows: [{group:{prop:val,...}, metric:N}] -> a ranked table
  function renderRows(rows) {
    if (!rows || !rows.length) return `<div class="o-empty">No groups.</div>`;
    const dims = Object.keys(rows[0].group || {});
    const head = dims.map((d) => `<th>${esc(d)}</th>`).join("") + "<th>metric</th>";
    const body = rows
      .map((r) => `<tr>${dims.map((d) => `<td>${esc(r.group[d])}</td>`).join("")}<td class="r-num">${esc(r.metric)}</td></tr>`)
      .join("");
    return `<div class="r-head">${rows.length} group${rows.length === 1 ? "" : "s"}</div>
      <table class="r-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
  }

  // a Function's output, rendered generically by shape (no per-Function knowledge)
  function renderData(data) {
    if (data == null) return `<div class="o-empty">Empty.</div>`;
    if (Array.isArray(data)) return data.length ? table(data) : `<div class="o-empty">No results.</div>`;
    if (typeof data === "object") {
      // dict whose values are arrays -> grouped sections (e.g. the tiered subject report)
      const groups = Object.entries(data).filter(([, v]) => Array.isArray(v));
      if (groups.length)
        return groups
          .map(([k, v]) => `<div class="r-group"><h3>${esc(k)} <span class="o-faint">${v.length}</span></h3>${
            v.length ? table(v) : '<div class="o-empty">—</div>'}</div>`)
          .join("");
      return `<pre class="r-pre">${esc(JSON.stringify(data, null, 2))}</pre>`;
    }
    return `<div class="r-head">${esc(data)}</div>`;
  }

  // a list of dicts -> a table (columns = union of keys; arrays/objects flattened)
  function table(list) {
    const cols = [...new Set(list.flatMap((o) => (o && typeof o === "object" ? Object.keys(o) : [])))];
    if (!cols.length) return `<ul class="r-list">${list.map((v) => `<li>${esc(v)}</li>`).join("")}</ul>`;
    const cell = (v) => esc(Array.isArray(v) ? v.join(", ") : v && typeof v === "object" ? JSON.stringify(v) : v);
    return `<table class="r-table"><thead><tr>${cols.map((c) => `<th>${esc(c)}</th>`).join("")}</tr></thead>
      <tbody>${list.map((o) => `<tr>${cols.map((c) => `<td>${cell(o ? o[c] : "")}</td>`).join("")}</tr>`).join("")}</tbody></table>`;
  }

  return { $, esc, HOW, pct, loadSchema, ty, objectDetail, loadRels, makeBoard, renderResult };
})();
