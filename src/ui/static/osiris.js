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
            <span class="o-faint">${esc(m.type)}</span></div>`)
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
        ? { name: "fcose", animate: false, randomize: !preserve, quality: "proof",
            nodeSeparation: 120, idealEdgeLength: 115, nodeRepulsion: 9000, padding: 45, packComponents: true }
        : { name: "cose", animate: false, padding: 45, nodeRepulsion: 12000, idealEdgeLength: 120 };
      cy.layout(o).run();        // non-animated: positions synchronously, so we can frame NOW
      // ALWAYS resize+frame right after — animated layouts raced other events and left the
      // nodes off-viewport (the recurring "opening leads to nothing" blank board).
      cy.resize();
      cy.fit(undefined, 45);
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
      // re-measure after the container becomes visible (Cytoscape can't size a hidden #cy),
      // then frame the graph. Without this a board revealed from a panel paints blank.
      resizeFit: () => { cy.resize(); cy.fit(undefined, 40); },
      clear: () => cy.elements().remove(),
      focusNode: (id) => { cy.nodes().removeClass("focus"); cy.getElementById(id).addClass("focus"); },
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

  // ---- THE GENERIC RENDERER (P4/W1) ----------------------------------------
  // A composition Result -> the right atom, in the chosen VIEW (Notion's switchable views
  // × Palantir's multi-modal object set). `mounts` = {board, panel}. An OBJECTS set renders
  // as a clean Graph OR a Table; values/rows/data render into the panel. `onPick(id)` focuses
  // a clicked row. Returns the mode the center should show ("graph" | "panel").
  // VIEWS lists the views an objects set supports (the shell builds the switcher from this).
  const VIEWS = ["graph", "table"];
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
  // from this, so a ranking offers Timeline first and a flat set offers Graph first.
  function viewsFor(result) {
    if (result.kind !== "objects") return [];
    return isRanked(result.spec) ? ["timeline", "table", "graph"] : ["graph", "table"];
  }
  function defaultView(result) {
    if (result.kind !== "objects") return "panel";
    if (isRanked(result.spec)) return "timeline";         // a ranking is a timeline, not a graph
    return result.items.length > 30 ? "table" : "graph";  // else a hairball past ~30 → table
  }
  async function renderResult(result, mounts, view, onPick, onDrill) {
    const { board, panel } = mounts;
    const kind = result.kind, items = result.items;
    if (kind === "objects") {
      if (view === "table") { objectsTable(panel, items, onPick); return "panel"; }
      if (view === "timeline") { timelineList(panel, items, onPick); return "panel"; }
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
  const _DATEKEYS = ["authored_date", "observed_at", "filed_date", "sale_date", "date", "first_seen"];
  const _SUMKEYS = ["summary", "change", "subject", "title", "status", "description", "rationale"];
  function _pick(props, keys) { for (const k of keys) if (props && props[k]) return props[k]; return null; }
  function timelineList(panel, items, onPick) {
    if (!items.length) { panel.innerHTML = `<div class="o-empty">Empty result.</div>`; return; }
    const body = items.map((o, i) => {
      const p = o.props || {};
      const d = _pick(p, _DATEKEYS), s = _pick(p, _SUMKEYS);
      const when = d ? esc(String(d).slice(0, 10)) : `#${i + 1}`;
      const sum = s ? `<div class="tl-sum">${esc(String(s).slice(0, 160))}</div>` : "";
      return `<div class="tl-item" data-pick="${o.id}">
        <span class="tl-when">${when}</span>
        <div class="tl-main"><span class="o-faint">${esc(o.type)}</span> ${esc(o.label)}${sum}</div></div>`;
    }).join("");
    panel.innerHTML = `<div class="r-head">${items.length} item${items.length === 1 ? "" : "s"} · in order</div>
      <div class="tl">${body}</div>`;
    panel.querySelectorAll("[data-pick]").forEach((el) => (el.onclick = () => onPick && onPick(el.dataset.pick)));
  }

  // an objects set as a TABLE — Type · Name · the most-common property columns. The fix for
  // the 80-node hairball: a scannable set, each row clickable into the inspector.
  function objectsTable(panel, items, onPick) {
    const skip = new Set(["name", "demo", "tag"]);
    const freq = {};
    items.forEach((o) => Object.keys(o.props || {}).forEach((k) => { if (!skip.has(k)) freq[k] = (freq[k] || 0) + 1; }));
    const cols = Object.entries(freq).sort((a, b) => b[1] - a[1]).slice(0, 4).map((e) => e[0]);
    const head = `<th>Type</th><th>Name</th>${cols.map((c) => `<th>${esc(c)}</th>`).join("")}`;
    const body = items
      .map((o) => `<tr data-pick="${o.id}" style="cursor:pointer">
        <td><span class="o-faint">${esc(o.type)}</span></td><td>${esc(o.label)}</td>
        ${cols.map((c) => `<td>${esc((o.props || {})[c] || "")}</td>`).join("")}</tr>`)
      .join("");
    panel.innerHTML = `<div class="r-head">${items.length} object${items.length === 1 ? "" : "s"}</div>` +
      (items.length ? `<table class="r-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`
        : `<div class="o-empty">Empty result.</div>`);
    panel.querySelectorAll("[data-pick]").forEach((tr) => (tr.onclick = () => onPick && onPick(tr.dataset.pick)));
  }

  // aggregate rows: [{group:{prop:val,...}, metric:N}] -> a ranked table where each row DRILLS
  // INTO the objects it counts (the missing interactive primitive — 'changelog by area' was a
  // dead list; now clicking 'composer · 6' opens those 6 commits). onDrill(group, spec) is the
  // shell's hook back to a select filtered by the group.
  function renderRows(panel, rows, spec, onDrill) {
    if (!rows || !rows.length) { panel.innerHTML = `<div class="o-empty">No groups.</div>`; return; }
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

  return { $, esc, HOW, pct, OPSYM, loadSchema, ty, objectDetail, loadRels, makeBoard,
    renderResult, VIEWS, viewsFor, defaultView, lineage, innerSelect };
})();
