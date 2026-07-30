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

  // the object detail (the one noun) — type chip, title, graded facts, slots for rels.
  // `acts` is HTML for action buttons the shell injects (search-around, dossier, …).
  function objectDetail(o, acts = "") {
    const demo = o.properties.some((p) => p.name === "demo" && String(p.value).toLowerCase() === "true");
    // the focus pane always shows the FULL resolved label, untruncated (o.name comes
    // from /objects/{id}'s resolve_label — rule/chain/canonical tiers, task #97) — a
    // Practice/BlindSpot/etc with no `name` property used to fall straight to its raw
    // canonical hash here even though every list view already resolved it correctly.
    const title = o.name || o.canonical;
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
  // onFocus(id, deep, type): tap = select (deep=false), double-tap = primary action (deep=true).
  // onCtx(id, type, mouseEvent): right-click = the object's contextual action menu.
  function makeBoard(container, onFocus, onCtx) {
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
    cy.on("tap", "node", (e) => onFocus && onFocus(e.target.id(), false, e.target.data("type")));
    cy.on("dbltap", "node", (e) => onFocus && onFocus(e.target.id(), true, e.target.data("type")));
    cy.on("cxttap", "node", (e) => {
      if (e.originalEvent) e.originalEvent.preventDefault();
      onCtx && onCtx(e.target.id(), e.target.data("type"), e.originalEvent);
    });
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
  async function renderResult(result, mounts, view, onPick, onDrill, onCtx) {
    const { board, panel } = mounts;
    const kind = result.kind, items = result.items;
    if (kind === "objects") {
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
  function renderData(data) {
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
      out += blocks.map(([k, v]) =>
        `<div class="r-group"><h3>${esc(k)}${Array.isArray(v) ? ` <span class="o-faint">${v.length}</span>` : ""}</h3>` +
        `${renderData(v)}</div>`).join("");
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
  const _txt = (v) => {
    if (typeof v === "string" && /^\d{4}-\d{2}-\d{2}T/.test(v)) v = v.slice(0, 10);   // ISO → date
    return v == null ? "" : Array.isArray(v) ? v.join(", ")
      : typeof v === "object" ? JSON.stringify(v) : String(v);
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
    return `<button class="r-act-btn" data-action="${escAttr(name)}" ` +
      `data-args="${escAttr(JSON.stringify(action.args || {}))}">${esc(label || _actionLabel(name))}</button>`;
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
        { detail: { name: b.dataset.action.slice(4), args } }));
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

  return { $, esc, pct, OPSYM, loadSchema, ty, objectDetail, loadRels, makeBoard,
    renderResult, viewsFor, defaultView, lineage, innerSelect };
})();
