/* Osiris Console — three-surface operator interface.
 * Surfaces: Browse (entity explorer), Mailbox (fleet messages), Fleet (live agents).
 * Power tools via Ctrl+K palette. Depends on: osiris.js (Osiris namespace).
 */

const $ = id => document.getElementById(id);
const esc = s => (s == null ? "" : String(s)).replace(/[<>&]/g, c => ({ "<": "&lt;", ">": "&gt;", "&": "&amp;" }[c]));

let FOCUS = null, SET = [], ROOM = '', ROOMS = [], PROJECTS = [], ACTIVE_SURFACE = 'browse';
let SELECTED_ENTITY_TYPES = new Set(), ENTITY_SEARCH_QUERY = '', ENTITY_VIEW_MODE = 'table';
let TABLE_SORT_COL = 'date', TABLE_SORT_DIR = 'desc', EXPANDED_ROWS = new Set();
let BOARD_GROUP_BY = 'auto', SYNCING = false, CONSOLE_REV = 0, SHOW_AGENTS = false, SCOPE_FILTER = '';
let TRUE_COUNTS = null; // uncapped per-type census from /objects/counts (#196) — null until loaded
let OBJECTS_LIMIT = 1500, OBJECTS_HAS_MORE = false, OBJECTS_LOADING_MORE = false;

var board = null;
function ensureBoard() {
  if (!board && typeof Osiris !== "undefined" && Osiris.makeBoard) {
    board = Osiris.makeBoard($("cy"),
      function(id, deep, type) { return deep ? primaryAction(id, type) : inspectOnly(id); },
      function(id, type, ev) { return ev && showActionMenu(ev.clientX, ev.clientY, id, type); });
  }
  return board;
}

function setStatus(s) { $("status").textContent = s; }
function showBoard() { $("stage").classList.remove("panel"); }
function showPanel() { $('stage').classList.add('panel'); }

// ── Surface Switching ────────────────────────────────────────────────────────
async function switchSurface(surface) {
  ACTIVE_SURFACE = surface; postConsole({ surface });
  document.querySelectorAll('.lens-item').forEach(el => el.classList.toggle('sel', el.dataset.surface === surface));
  $('page-title').textContent = surface.charAt(0).toUpperCase() + surface.slice(1);
  if (surface === 'browse') {
    $('entity-taxonomy-bar').style.display = 'flex'; $('viewsw').style.display = '';
    if (!SET.length) await loadObjectSet(); renderEntityExplorer();
  } else {
    $('entity-taxonomy-bar').style.display = 'none'; $('viewsw').style.display = 'none';
    (ensureBoard()).clear(); showBoard();
    if (surface === 'mailbox') renderMailbox();
    if (surface === 'projects') renderProjects();
  }
}

// ── Room / Workspace ─────────────────────────────────────────────────────────
function toggleWorkspaceDropdown(e) {
  if (e) e.stopPropagation();
  const dd = $('workspace-dropdown'), pill = $('workspace-pill');
  if (!dd) return; const isOpen = dd.style.display === 'flex';
  closeAllDropdowns();
  if (!isOpen) { dd.style.display = 'flex'; pill.classList.add('open'); renderWorkspaceDropdown(); }
}
function renderWorkspaceDropdown() {
  const c = $('workspace-dd-items'); if (!c) return;
  const items = [{ id: '', name: 'Global / Fleet' }, ...(ROOMS || [])];
  c.innerHTML = items.map(r => {
    const sel = (ROOM || '') === (r.id || '');
    return '<div class="dd-item' + (sel ? ' sel' : '') + '" onclick="selectWorkspace(\'' + esc(r.id || '') + '\')"><div class="dd-item-main"><span class="dd-item-name">' + esc(r.name) + '</span>' + (r.compositions ? '<span class="dd-item-hint">' + r.compositions + ' lenses</span>' : '') + '</div>' + (sel ? '<span class="dd-item-check">\u2713</span>' : '') + '</div>';
  }).join('');
}
function selectWorkspace(id) { closeAllDropdowns(); switchRoom(id); }
function closeAllDropdowns() {
  var wd = document.getElementById('workspace-dropdown'); if(wd) wd.style.display='none';
  var rd = document.getElementById('repo-dropdown'); if(rd) rd.style.display='none';
  var od = document.getElementById('omni-dropdown'); if(od) od.style.display='none';
  var wp = document.getElementById('workspace-pill'); if(wp) wp.classList.remove('open');
  var rp = document.getElementById('repo-pill'); if(rp) rp.classList.remove('open');
  collapseSearchIfUnfocused();
}
function updateWorkspaceScopeUI() {
  const room = (ROOMS || []).find(r => r.id === ROOM);
  const rName = room ? room.name : (ROOM ? 'Custom' : 'Global / Fleet');
  if ($('workspace-pill-label')) $('workspace-pill-label').textContent = rName;
}
async function loadRooms() {
  ROOMS = await fetch('/rooms').then(r => r.json());
  if ($('room')) { $('room').innerHTML = '<option value="">All Rooms</option>' + ROOMS.map(r => '<option value="' + r.id + '">' + esc(r.name) + '</option>').join(''); $('room').value = ROOM; }
  renderWorkspaceDropdown(); updateWorkspaceScopeUI();
}
async function switchRoom(id) {
  ROOM = id; postConsole({ room_id: id || null }); $('room').value = id;
  renderWorkspaceDropdown(); updateWorkspaceScopeUI();
  const room = ROOMS.find(r => r.id === id);
  const collect = !!(room && room.config && room.config.collect);
  document.querySelectorAll('.collect-only').forEach(el => el.style.display = collect ? '' : 'none');
  if (ACTIVE_SURFACE === 'browse') { SET = []; await loadObjectSet(); renderEntityExplorer(); }
}
async function newRoom() {
  const name = prompt('Name this stance / perspective:'); if (!name) return;
  const r = await fetch('/rooms', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ name }) }).then(r => r.json());
  await loadRooms(); switchRoom(r.id);
}
async function loadProjects() {
  try {
    PROJECTS = await fetch('/objects?type=SoftwareProject').then(function(r){return r.json();});
    var sel = $('scope-select');
    if (sel && PROJECTS) {
      var names = PROJECTS.filter(function(p){return p.status === 'active';}).map(function(p){return (p.canonical || '').replace('repo:', '');}).filter(Boolean).sort();
      sel.innerHTML = '<option value="">All Repos</option>' + names.map(function(n){return '<option value="' + n + '">' + esc(n) + '</option>';}).join('');
    }
  } catch(e) {}
}

// ── Object Set ───────────────────────────────────────────────────────────────
var SELECTED_REPOS = [];

function toggleRepoDropdown(e) {
  if (e) e.stopPropagation();
  var dd = document.getElementById("repo-dropdown");
  var pill = document.getElementById("repo-pill");
  if (!dd) return;
  var isOpen = dd.style.display === "flex";
  closeAllDropdowns();
  if (!isOpen) { dd.style.display = "flex"; pill.classList.add("open"); renderRepoDropdown(); }
}
function renderRepoDropdown() {
  var c = document.getElementById("repo-dd-items");
  if (!c || !PROJECTS) return;
  var names = PROJECTS.filter(function(p){return p.status === "active";}).map(function(p){return (p.canonical || "").replace("repo:", "");}).filter(Boolean).sort();
  var seen = {};
  names = names.filter(function(n){ var l = n.toLowerCase(); if (seen[l]) return false; seen[l] = true; return true; });
  c.innerHTML = names.map(function(n){
    var sel = SELECTED_REPOS.indexOf(n) !== -1;
    // data-repo was written as `data-repo="" + esc(n) + ""` INSIDE a single-quoted JS
    // string — so the `+ esc(n) +` was literal HTML text, never concatenation, and every
    // item rendered data-repo="". toggleRepo('') then pushed an empty string that matched
    // no project, so the scope pill accepted clicks and filtered nothing.
    return '<div class="dd-item' + (sel ? ' sel' : '') + '" data-repo="' + esc(n) + '" onclick="toggleRepo(this.dataset.repo)"><div class="dd-item-main"><span class="dd-item-name">' + esc(n) + '</span></div>' + (sel ? '<span class="dd-item-check">' + String.fromCharCode(10003) + '</span>' : '') + '</div>';
  }).join('');
}
function toggleRepo(name) {
  if (!name) { console.warn('toggleRepo: empty name, ignoring'); return; }
  var idx = SELECTED_REPOS.indexOf(name);
  if (idx === -1) SELECTED_REPOS.push(name);
  else SELECTED_REPOS.splice(idx, 1);
  applyRepoFilter();
  renderRepoDropdown();
  updateRepoPill();
}
function selectRepos(list) {
  SELECTED_REPOS = list || [];
  applyRepoFilter();
  updateRepoPill();
}
function applyRepoFilter() {
  var pill = document.getElementById("repo-pill-label");
  if (pill) pill.textContent = SELECTED_REPOS.length ? SELECTED_REPOS.join(", ") : "All Repos";
  SCOPE_FILTER = SELECTED_REPOS.join(",");
  SET = []; loadObjectSet().then(function(){ renderEntityExplorer(); });
}
function updateRepoPill() {
  var pill = document.getElementById("repo-pill-label");
  if (pill) pill.textContent = SELECTED_REPOS.length ? SELECTED_REPOS.join(", ") : "All Repos";
}

function objectScopeParams() {
  // The scope bits objectSetUrl() and objectCountsUrl() both need — split out so the
  // counts fetch (#196, Thoth msg 5600) reads the exact same scope /objects itself would,
  // never a second, drifting copy of the same three branches.
  var ex = SHOW_AGENTS ? '' : '&exclude_types=Agent';
  var room = ROOMS.find(function(r){return r.id === ROOM;});
  var subject = (room && room.config && room.config.subject) || null;
  if (subject && subject.startsWith('repo:')) return { extra: ex, project: [subject.replace('repo:', '')], case_id: null };
  if (subject && subject.startsWith('case:')) return { extra: ex, project: [], case_id: subject.replace('case:', '') };
  var repos = SCOPE_FILTER ? SCOPE_FILTER.split(',').filter(Boolean) : [];
  return { extra: ex, project: repos, case_id: null };
}
function objectSetUrl(cursor) {
  var s = objectScopeParams();
  var url = '/objects?limit=' + OBJECTS_LIMIT + s.extra;
  if (s.case_id) url += '&case_id=' + encodeURIComponent(s.case_id);
  for (var i = 0; i < s.project.length; i++) url += '&project=' + encodeURIComponent(s.project[i]);
  // KEYSET continuation (#93 step 3, Thoth msg 5668) — the cursor #196 built and proved
  // (before_created_at/before_id, migration 0054's objects_type_created_idx) but never
  // wired to a click. Omitted for the initial load (unchanged behavior, every existing
  // caller); passed here only by loadMoreObjects() below.
  if (cursor) url += '&before_created_at=' + encodeURIComponent(cursor.created_at) + '&before_id=' + encodeURIComponent(cursor.id);
  return url;
}
function objectCountsUrl() {
  var s = objectScopeParams();
  var url = '/objects/counts?' + s.extra.replace(/^&/, '');
  if (s.case_id) url += '&case_id=' + encodeURIComponent(s.case_id);
  for (var i = 0; i < s.project.length; i++) url += '&project=' + encodeURIComponent(s.project[i]);
  return url;
}
async function loadObjectSet() {
  const r = await fetch(objectSetUrl()).then(r => r.json()).catch(() => []);
  SET = Array.isArray(r) ? r : [];
  // A full-length page is the only signal /objects gives that there might be more (no
  // total/has_more field) — a heuristic, not a promise, same honesty rule as everywhere
  // else this reign: never assert a conclusion the data can't support. A short page is
  // certain (fewer than asked for = nothing left); a full page MIGHT mean more, or might
  // land exactly on the boundary — "Load More" staying clickable in that rare case costs
  // one empty click, never a silent truncation.
  OBJECTS_HAS_MORE = SET.length >= OBJECTS_LIMIT;
  // TRUE_COUNTS is the uncapped census (#196) — the taxonomy bar prefers it over
  // SET-derived counts, which silently understate the moment a type/scope exceeds the
  // /objects cap (measured: 31,189 objects fleet-wide, Agent alone 10,617 — the cap is
  // 2000). A failed/slow counts fetch degrades to null, and the toolbar falls back to
  // the old SET-derived count rather than showing nothing.
  TRUE_COUNTS = await fetch(objectCountsUrl()).then(r => r.json()).catch(() => null);
}
async function loadMoreObjects() {
  if (OBJECTS_LOADING_MORE || !OBJECTS_HAS_MORE || !SET.length) return;
  var last = SET[SET.length - 1];  // SET is created_at DESC — the last row is the oldest loaded
  if (!last || !last.created_at) { OBJECTS_HAS_MORE = false; renderEntityExplorerStage(); return; }
  OBJECTS_LOADING_MORE = true;
  renderEntityExplorerStage();  // shows the loading state on the button immediately
  try {
    var page = await fetch(objectSetUrl({ created_at: last.created_at, id: last.id }))
      .then(function(r){return r.json();}).catch(function(){return [];});
    page = Array.isArray(page) ? page : [];
    SET = SET.concat(page);
    OBJECTS_HAS_MORE = page.length >= OBJECTS_LIMIT;
  } finally {
    OBJECTS_LOADING_MORE = false;
    renderEntityExplorerStage();
  }
}

// ── Entity Explorer ──────────────────────────────────────────────────────────
function getFilteredEntities() {
  let q = (ENTITY_SEARCH_QUERY || '').trim();
  const tMatch = q.match(/\btype:([a-zA-Z0-9_-]+)/i), sMatch = q.match(/\bstatus:([a-zA-Z0-9_-]+)/i);
  let fType = tMatch ? tMatch[1].toLowerCase() : null, fStatus = sMatch ? sMatch[1].toLowerCase() : null;
  if (tMatch) q = q.replace(tMatch[0], '').trim(); if (sMatch) q = q.replace(sMatch[0], '').trim();
  const textQ = q.toLowerCase(), isAllOn = SELECTED_ENTITY_TYPES.size === 0;
  return (SET || []).filter(o => {
    const ot = (o.type || '').toLowerCase(), os = (o.status || 'active').toLowerCase();
    if (fType && !ot.includes(fType)) return false;
    if (!isAllOn && !SELECTED_ENTITY_TYPES.has(o.type)) return false;
    if (fStatus && !os.includes(fStatus)) return false;
    if (!textQ) return true;
    const nm = (o.name || o.canonical || o.id || '').toLowerCase(), ps = o.props ? JSON.stringify(o.props).toLowerCase() : '';
    return nm.includes(textQ) || ot.includes(textQ) || ps.includes(textQ);
  });
}
function renderEntityToolbar() {
  $('entity-taxonomy-bar').style.display = 'flex';
  // TRUE_COUNTS (#196) is the uncapped census over the same scope; SET-derived counts
  // are a fallback for when it hasn't loaded (or failed) — never the primary source,
  // since SET is capped at 1500 and silently understates any type/scope past that.
  const trueTotal = TRUE_COUNTS ? TRUE_COUNTS.total : SET.length;
  if ($('entity-total-num')) $('entity-total-num').textContent = trueTotal.toLocaleString();
  const typeCounts = TRUE_COUNTS ? Object.assign({}, TRUE_COUNTS.by_type) : {};
  if (!TRUE_COUNTS) SET.forEach(o => { typeCounts[o.type] = (typeCounts[o.type] || 0) + 1; });
  const total = trueTotal;
  const priorityOrder = ['Decision','Thread','Reference','Commit','File','Practice','Seat','SoftwareProject','Person','Organization'];
  const availableTypes = Object.keys(typeCounts).sort((a,b)=>{const ai=priorityOrder.indexOf(a),bi=priorityOrder.indexOf(b);if(ai!==-1&&bi!==-1)return ai-bi;if(ai!==-1)return-1;if(bi!==-1)return 1;return(typeCounts[b]||0)-(typeCounts[a]||0)});
  const isAllOn = SELECTED_ENTITY_TYPES.size === 0, nav = $('entity-type-pills'); if (!nav) return;
  nav.innerHTML = '<button class="tax-tab' + (isAllOn ? ' on' : '') + '" onclick="toggleEntityType(\'All\')"><span class="tax-title">ALL</span><span class="tax-count">' + total.toLocaleString() + '</span></button>' + availableTypes.map(t => {
    const count = typeCounts[t] || 0, sel = (!isAllOn && SELECTED_ENTITY_TYPES.has(t)) ? ' on' : '';
    const dl = t === 'Thread' ? 'THREADS' : (t === 'Decision' ? 'DECISIONS' : (t === 'Reference' ? 'CANON' : (t === 'Commit' ? 'COMMITS' : (t === 'File' ? 'FILES' : (t === 'SoftwareProject' ? 'REPOS' : (t === 'Practice' ? 'PRACTICES' : (t === 'BlindSpot' ? 'BLINDSPOTS' : (t === 'Superstition' ? 'SUPERSTITIONS' : t.toUpperCase()))))))));
    return '<button class="tax-tab' + sel + '" onclick="toggleEntityType(\'' + t + '\')"><span class="tax-title">' + esc(dl) + '</span><span class="tax-count">' + count.toLocaleString() + '</span></button>';
  }).join('');
}
function toggleEntityType(t) { if (t === 'All') SELECTED_ENTITY_TYPES.clear(); else { if (SELECTED_ENTITY_TYPES.has(t)) SELECTED_ENTITY_TYPES.delete(t); else SELECTED_ENTITY_TYPES.add(t); } renderEntityExplorer(); }
function filterEntitySearch(q) { ENTITY_SEARCH_QUERY = q; renderEntityExplorer(); }
function setEntityView(mode) { ENTITY_VIEW_MODE = mode; renderEntityExplorerStage(); }
function toggleTableSort(col) { TABLE_SORT_DIR = TABLE_SORT_COL === col ? (TABLE_SORT_DIR === 'asc' ? 'desc' : 'asc') : 'asc'; TABLE_SORT_COL = col; renderEntityExplorerStage(); }
function inspectAndToggleRow(id) { inspectOnly(id); EXPANDED_ROWS.has(id) ? EXPANDED_ROWS.delete(id) : EXPANDED_ROWS.add(id); renderEntityExplorerStage(); }
function setBoardGroupBy(mode) { BOARD_GROUP_BY = mode; renderEntityExplorerStage(); }
function sortIcon(col) { return TABLE_SORT_COL === col ? (TABLE_SORT_DIR === 'asc' ? ' \u25b4' : ' \u25be') : ''; }
function clearAll() { (ensureBoard()).clear(); setStatus('Board cleared.'); }

async function renderEntityExplorer() { $('entity-taxonomy-bar').style.display = 'flex'; renderEntityToolbar(); renderSwitcher(); renderEntityExplorerStage(); }
function renderSwitcher() {
  const el = $('viewsw'); if (!el || ACTIVE_SURFACE !== 'browse') { if (el) el.innerHTML = ''; return; }
  el.innerHTML = '<button class="view-tab' + (ENTITY_VIEW_MODE === 'table' ? ' on' : '') + '" onclick="setEntityView(\'table\')">Table</button><button class="view-tab' + (ENTITY_VIEW_MODE === 'board' ? ' on' : '') + '" onclick="setEntityView(\'board\')">Board</button><button class="view-tab' + (ENTITY_VIEW_MODE === 'graph' ? ' on' : '') + '" onclick="setEntityView(\'graph\')">Graph</button>';
}
function renderEntityExplorerStage() {
  const filtered = getFilteredEntities(); setStatus(filtered.length + ' of ' + SET.length + ' entities');
  if (ENTITY_VIEW_MODE === 'graph') { showBoard(); (ensureBoard()).clear(); if (filtered.length) (ensureBoard()).placeObjects(filtered.slice(0, 200).map(o => ({ id: o.id, type: o.type, label: o.display_label || o.name || o.canonical || o.id }))); return; }
  const container = $('result');
  if (ENTITY_VIEW_MODE === 'board') { renderBoardProjection(container, filtered); showPanel(); return; }
  renderTableProjection(container, filtered); showPanel();
}

// ── Key/ID rendering ─────────────────────────────────────────────────────────
// A key is either a NAME (repo:osiris -> "osiris", a handle, a slug) or an OPAQUE
// digest (a uuid, a commit sha, a 32-hex canonical). Names are meaningful to a human
// and must survive intact; digests carry no meaning in the middle, so they abbreviate
// head+tail the way a wallet address does. Truncating a name from the right is the
// worst of both: it destroys the only part that identified the thing.
function keyLabel(o) {
  var raw = o.canonical || '';
  if (raw.indexOf(':') !== -1) raw = raw.slice(raw.indexOf(':') + 1);
  if (!raw) raw = o.id || '';
  return raw;
}
function isOpaqueKey(k) {
  // uuid, bare sha, or any long unbroken hex run — nothing a human reads as a word.
  return /^[0-9a-f-]{12,}$/i.test(k) || /[0-9a-f]{16,}/i.test(k);
}
function abbrevKey(k, head, tail) {
  head = head || 6; tail = tail || 4;
  if (k.length <= head + tail + 1) return k;
  return k.slice(0, head) + '\u2026' + k.slice(-tail);
}
// NAMES up to ~24 chars render whole; longer names and all digests abbreviate.
function renderKey(o) {
  var k = keyLabel(o);
  if (!k) return '';
  var shown = isOpaqueKey(k) ? abbrevKey(k) : (k.length <= 24 ? k : abbrevKey(k, 14, 6));
  var cls = 'ee-canon-mono' + (isOpaqueKey(k) ? '' : ' is-name');
  return '<span class="' + cls + '" title="' + esc(k) + '">' + esc(shown) + '</span>';
}

// ── Table Projection ─────────────────────────────────────────────────────────
// The old footer here claimed "Showing first 300 of X" while actually rendering EVERY
// row in `shown`, unsliced — a granular sharpening (#93 item 2, Thoth msg 5668): a
// footer that asserts a conclusion the render doesn't support is exactly the fact-vs-
// conclusion distinction this whole reign kept landing on for the project index's own
// flag copy. Replaced with what's actually true — how many are loaded, out of the real
// total when known (#196's /objects/counts), plus Load More when the server signaled
// there might be more (loadObjectSet's own heuristic, not a promise).
function objectSetFooterHtml() {
  var total = TRUE_COUNTS ? TRUE_COUNTS.total : null;
  var loadedText = 'Loaded ' + SET.length.toLocaleString() + (total != null ? ' of ' + total.toLocaleString() : '') + ' objects in scope';
  var btn = OBJECTS_HAS_MORE
    ? '<button class="iconbtn" style="margin-left:10px" onclick="loadMoreObjects()"' + (OBJECTS_LOADING_MORE ? ' disabled' : '') + '>' + (OBJECTS_LOADING_MORE ? 'Loading…' : 'Load More') + '</button>'
    : '';
  return '<div class="o-faint" style="padding:12px;text-align:center">' + esc(loadedText) + btn + '</div>';
}
function renderTableProjection(container, items) {
  var sorters = { name: function(o){return (o.display_label || o.name || '').toLowerCase();}, type: function(o){return (o.type || '').toLowerCase();}, status: function(o){return (o.status || 'active').toLowerCase();}, date: function(o){return o.created_at || '';} };
  var shown = [].concat(items).sort(function(a, b) { var va = sorters[TABLE_SORT_COL] ? sorters[TABLE_SORT_COL](a) : (a.created_at || ''); var vb = sorters[TABLE_SORT_COL] ? sorters[TABLE_SORT_COL](b) : (b.created_at || ''); return TABLE_SORT_DIR === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va); });
  if (!shown.length) { container.innerHTML = '<div class="o-empty" style="padding:40px 20px">No matching entities.</div>'; return; }
  container.innerHTML = '<table class="ee-table"><thead><tr><th style="width:105px;cursor:pointer" onclick="toggleTableSort(\'type\')">Type' + sortIcon('type') + '</th><th style="width:150px">Key / ID</th><th style="cursor:pointer" onclick="toggleTableSort(\'name\')">Summary' + sortIcon('name') + '</th><th style="width:95px;cursor:pointer" onclick="toggleTableSort(\'date\')">Date' + sortIcon('date') + '</th><th style="width:75px;text-align:right;cursor:pointer" onclick="toggleTableSort(\'status\')">Status' + sortIcon('status') + '</th></tr></thead><tbody>' +
  shown.map(renderTableRow).join('') + '</tbody></table>' + objectSetFooterHtml();
}
function renderTableRow(o) {
  var isSel = FOCUS === o.id, isExp = EXPANDED_ROWS.has(o.id), p = o.props || {};
  var summary = p.summary || p.rationale || p.statement || p.description || p.title || '';
  var dateStr = o.created_at ? o.created_at.slice(0, 10) : '';
  var grade = (p.evidence_class || p.grade || 'self_declared').toLowerCase(), source = p.source_id || p.source_label || p.source || '';
  var tColor = '#6e7681';
  try { tColor = Osiris.ty(o.type).c || '#6e7681'; } catch(e) {}
  return '<tr class="ee-row' + (isSel ? ' sel' : '') + (isExp ? ' expanded' : '') + '" onclick="inspectAndToggleRow(\'' + o.id + '\')" ondblclick="primaryAction(\'' + o.id + '\', \'' + esc(o.type) + '\')">' +
    '<td><span class="ee-type-pill" style="border-color:' + tColor + '40;color:' + tColor + ';background:' + tColor + '18"><span class="dot" style="background:' + tColor + '"></span> ' + esc(o.type) + '</span></td>' +
    '<td>' + renderKey(o) + '</td>' +
    '<td class="ee-summary-cell"><div class="ee-name">' + esc(o.display_label || o.name || summary || o.id) + '</div>' + (summary && summary !== o.name ? '<div class="ee-summary-preview">' + esc(summary) + '</div>' : '') + '</td>' +
    '<td style="color:var(--muted);font-size:11px;font-family:var(--font-mono)">' + esc(dateStr) + '</td>' +
    '<td style="text-align:right"><span class="ee-status status-' + esc(o.status || 'active') + '">' + esc(o.status || 'active') + '</span></td>' +
    '</tr>' +
    (isExp ? '<tr class="ee-tray-row"><td colspan="5"><div class="ee-inline-tray"><div class="ee-tray-head"><span class="ee-tray-title">STATEMENT / RATIONALE</span><div class="ee-tray-badges">' + (grade ? '<span class="card-tag grade-' + esc(grade) + '">' + esc(grade.replace(/_/g, ' ')) + '</span>' : '') + (source ? '<span class="card-tag">source: ' + esc(source) + '</span>' : '') + '<button class="iconbtn" style="padding:1px 6px;font-size:10px" onclick="event.stopPropagation();focus(\'' + o.id + '\')">Open in Graph &#9658;</button></div></div><div class="ee-tray-body">' + esc(summary || o.name || '') + '</div>' + (Object.keys(p).length ? '<div class="ee-tray-props">' + Object.entries(p).filter(function(e){return ['summary','rationale','statement','title','description'].indexOf(e[0]) === -1;}).slice(0, 8).map(function(e){return '<div class="ee-prop-item"><span class="ee-prop-k">' + esc(e[0]) + ':</span> <span class="ee-prop-v">' + esc(String(e[1])) + '</span></div>';}).join('') + '</div>' : '') + '</div></td></tr>' : '');
}

// ── Board Projection ─────────────────────────────────────────────────────────
function renderBoardProjection(container, items) {
  const shown = items.slice(0, 250);
  if (!shown.length) { container.innerHTML = '<div class="o-empty" style="padding:40px 20px">No matching entities.</div>'; return; }
  const distinctTypes = [...new Set(shown.map(o => o.type || 'Unknown'))];
  let effMode = BOARD_GROUP_BY === 'auto' ? (distinctTypes.length <= 1 ? 'status' : 'type') : BOARD_GROUP_BY;
  let lanes = [];
  if (effMode === 'status') {
    lanes = [{ id: 'active', name: 'Active / Open', items: [] }, { id: 'progress', name: 'In Progress', items: [] }, { id: 'resolved', name: 'Resolved', items: [] }];
    shown.forEach(o => { const p = o.props || {}, st = (o.status || p.status || 'active').toLowerCase(), ow = (p.owner || p.assignee || '').toLowerCase(); if (['historical','retired','resolved','closed','done'].includes(st)) lanes[2].items.push(o); else if (st === 'in_progress' || st === 'leased' || (ow && ow !== 'unassigned' && ow !== 'operator')) lanes[1].items.push(o); else lanes[0].items.push(o); });
  } else {
    const priorityOrder = ['Decision','Thread','Reference','Commit','File','Practice','SoftwareProject','Seat'];
    const sortedTypes = distinctTypes.sort((a,b)=>{const ai=priorityOrder.indexOf(a),bi=priorityOrder.indexOf(b);if(ai!==-1&&bi!==-1)return ai-bi;if(ai!==-1)return-1;if(bi!==-1)return 1;return a.localeCompare(b)});
    lanes = sortedTypes.map(t => ({ id: t, name: t==='Thread'?'Threads':(t==='Decision'?'Decisions':(t==='Reference'?'Canon':(t==='Commit'?'Commits':(t==='File'?'Files':(t==='SoftwareProject'?'Projects':t))))), items: [] }));
    const lm = new Map(lanes.map(l => [l.id, l])); shown.forEach(o => { const t = lm.get(o.type); if (t) t.items.push(o); });
  }
  const activeLanes = lanes.filter(l => l.items.length > 0), lanesToRender = activeLanes.length ? activeLanes : lanes;
  container.innerHTML = '<div style="padding:16px;height:100%"><div class="spatial-board">' + lanesToRender.map(l => renderBoardLane(l)).join('') + '</div>' + (items.length > 250 ? '<div class="o-faint" style="padding:16px 0;text-align:center">Showing first 250 of ' + items.length + '</div>' : '') + '</div>';
}
function renderBoardLane(l) {
  var lc = '#6e7681'; try { lc = Osiris.ty(l.id).c || '#6e7681'; } catch(e) {}
  return '<div class="board-lane"><div class="board-lane-head"><div class="lane-title-wrap"><span class="dot" style="background:' + lc + '"></span><span class="lane-name">' + esc(l.name) + '</span></div><span class="lane-badge">' + l.items.length + '</span></div><div class="board-lane-items">' + l.items.map(renderBoardCard).join('') + (l.items.length ? '' : '<div class="lane-empty">No items</div>') + '</div></div>';
}
function renderBoardCard(o) {
  const p = o.props || {}, summary = p.summary || p.rationale || p.statement || p.description || p.title || '';
  const dateStr = o.created_at ? o.created_at.slice(0, 10) : '';
  const grade = (p.evidence_class || p.grade || 'self_declared').toLowerCase(), source = p.source_id || p.source_label || p.source || '';
  const isDuty = o.type === 'Thread' && (p.kind === 'obligation' || o.status === 'obligation'), statusLabel = isDuty ? 'duty' : (o.status || 'active');
  var cc = '#6e7681'; try { cc = Osiris.ty(o.type).c || '#6e7681'; } catch(e) {}
  return '<div class="board-card' + (FOCUS === o.id ? ' sel' : '') + '" onclick="inspectOnly(\'' + o.id + '\')" ondblclick="primaryAction(\'' + o.id + '\', \'' + esc(o.type) + '\')"><div class="card-tags-top"><span class="card-tag card-tag-type" style="border-color:' + cc + '40;color:' + cc + ';background:' + cc + '18"><span class="dot" style="background:' + cc + '"></span> ' + esc(o.type) + '</span>' + renderKey(o) + '</div><div class="card-main-content"><div class="card-title">' + esc(o.display_label || o.name || summary || o.id) + '</div>' + (summary && summary !== o.name ? '<div class="card-desc">' + esc(summary) + '</div>' : '') + '</div><div class="card-tags-bottom"><span class="card-tag card-tag-status status-' + esc(statusLabel) + '">' + esc(statusLabel) + '</span>' + (dateStr ? '<span class="card-tag card-tag-date">' + esc(dateStr) + '</span>' : '') + (grade ? '<span class="card-tag card-tag-grade grade-' + esc(grade) + '">' + esc(grade.replace(/_/g, ' ')) + '</span>' : '') + (source ? '<span class="card-tag card-tag-source">by ' + esc(source) + '</span>' : '') + '</div></div>';
}

// ── Mailbox ──────────────────────────────────────────────────────────────────
async function renderMailbox() {
  var container = $('result'); showPanel();
  try {
    var p = await fetch('/pulse').then(function(r){return r.json();});
    // /pulse returns {line, live, owed, briefs, wakes, spend} — it has NEVER carried a
    // `messages` array (server route: src/api/app.py's pulse_route). The old code called
    // .json() a SECOND time on `p.messages || []` (an array, which has no .json() method),
    // which threw and was swallowed by the outer catch — always rendering "No messages",
    // even when mail was waiting, with nothing in the console to say why (found live,
    // flagged twice, survived both because the failure was silent). No JSON mail-list route
    // exists yet (a real gap, tracked separately) — until one does, say so LOUDLY instead of
    // rendering a false "No messages in inbox."
    if (!Array.isArray(p.messages)) {
      console.error('renderMailbox: /pulse has no messages array — no JSON mail-list route exists yet', p);
      container.innerHTML = '<div class="o-empty" style="padding:40px">Mailbox feed unavailable — /pulse carries no message data (no JSON mail-list route exists yet; see /mail for the HTML view).</div>';
      return;
    }
    var msgs = p.messages;
    if (!msgs.length) { container.innerHTML = '<div class="o-empty" style="padding:40px">No messages in inbox.</div>'; return; }
    container.innerHTML = '<div style="padding:16px;max-width:900px;margin:0 auto"><h2 style="font-size:13px;text-transform:uppercase;letter-spacing:0.5px;color:var(--muted);margin-bottom:12px">Mailbox (' + msgs.length + ')</h2>' + msgs.map(function(m){ return '<div class="mail-card" style="background:var(--panel);border:1px solid var(--border);border-radius:8px;padding:12px 14px;margin-bottom:8px"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px"><div><span style="font-weight:600;color:var(--blue);font-size:11px">' + esc(m.from_agent || m.from_project || '') + '</span> <span style="color:var(--faint)">' + String.fromCharCode(8594) + '</span> <span style="color:var(--muted);font-size:11px">' + esc(m.to_project || m.to_agent || '') + '</span></div><div style="font-size:10px;color:var(--faint)">' + esc(m.created_at ? m.created_at.slice(0, 10) : '') + '</div></div><div style="font-size:12px;line-height:1.5;color:var(--text);margin-bottom:8px;white-space:pre-wrap;word-break:break-word">' + esc((m.body || '').slice(0, 500)) + '</div></div>'; }).join('') + '</div>';
    setStatus(msgs.length + ' messages');
  } catch(e) { console.error('renderMailbox failed', e); container.innerHTML = '<div class="o-empty" style="padding:40px">Could not load mailbox.</div>'; }
}

// ── Projects (#93, the project dimension — Thoth msg 5631) ────────────────────
var PROJECTS_INDEX_DATA = null, PROJECTS_INDEX_STATUS = 'active';
async function renderProjects() {
  var container = $('result'); showPanel();
  try {
    if (!PROJECTS_INDEX_DATA) PROJECTS_INDEX_DATA = await fetch('/projects').then(function(r){return r.json();});
    var rows = Array.isArray(PROJECTS_INDEX_DATA) ? PROJECTS_INDEX_DATA : [];
    if (!rows.length) { container.innerHTML = '<div class="o-empty" style="padding:40px">No projects.</div>'; return; }
    // NEVER collapse the status dimension to one number (Thoth's own instruction, msg
    // 5631): "what is live" and "what has ever existed" are different questions — a
    // toggle, not a single count, so both stay askable.
    var counts = {};
    rows.forEach(function(r){ counts[r.status] = (counts[r.status] || 0) + 1; });
    var allCount = rows.length, activeCount = counts.active || 0;
    var shown = rows.filter(function(r){ return PROJECTS_INDEX_STATUS === 'all' || r.status === PROJECTS_INDEX_STATUS; });
    shown = shown.slice().sort(function(a, b){ return (b.last_touch || '').localeCompare(a.last_touch || ''); });
    var head = '<div style="padding:16px 16px 8px;max-width:1100px;margin:0 auto"><div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px">' +
      '<h2 style="font-size:13px;text-transform:uppercase;letter-spacing:0.5px;color:var(--muted)">Projects (' + shown.length + ')</h2>' +
      '<div><button class="iconbtn' + (PROJECTS_INDEX_STATUS === 'active' ? ' sel' : '') + '" onclick="setProjectsStatusFilter(\'active\')">Active (' + activeCount + ')</button> ' +
      '<button class="iconbtn' + (PROJECTS_INDEX_STATUS === 'all' ? ' sel' : '') + '" onclick="setProjectsStatusFilter(\'all\')">All (' + allCount + ')</button></div></div>';
    var table = '<table class="ee-table"><thead><tr><th>Project</th><th style="width:90px">Status</th><th style="width:90px;text-align:right">Objects</th><th style="width:130px">Last Activity</th></tr></thead><tbody>' +
      shown.map(projectRow).join('') + '</tbody></table></div>';
    container.innerHTML = head + table;
    setStatus(shown.length + ' of ' + allCount + ' projects');
  } catch(e) { container.innerHTML = '<div class="o-empty" style="padding:40px">Could not load projects.</div>'; }
}
function setProjectsStatusFilter(status) { PROJECTS_INDEX_STATUS = status; renderProjects(); }
function projectRow(r) {
  var lastTouch = r.last_touch ? String(r.last_touch).slice(0, 10) : '—';
  // CORRECTED (Sekhmet's per-project pass, decision 3b72b196, msg 5653): link-count is
  // NOT a phantom test — the one active zero-links project (deepseek-harness) turned out
  // real, a genuine disk-census-found repo simply never worked yet; the actual phantom
  // (liveness-fix) carried ONE link, not zero. So this flag surfaces "worth a glance"
  // ONLY — it must never read as "suspect", which the wording used to imply.
  var flag = '';
  if (r.bucket === 'orphan') flag = '<span class="proj-flag" title="zero live links — often just a repo nobody has worked yet, not necessarily a problem (link-count alone is not a phantom test)">no links</span>';
  else if (r.bucket === 'contradicted') flag = '<span class="proj-flag" title="' + esc((r.contradicted_on || []).join(', ')) + ' disagrees across sources">contradicted</span>';
  return '<tr class="ee-row" style="cursor:pointer" onclick="openProjectInBrowse(\'' + esc(r.name) + '\')"><td>' + esc(r.name) + flag + '</td>' +
    '<td><span class="card-tag-status status-' + esc(r.status) + '">' + esc(r.status) + '</span></td>' +
    '<td style="text-align:right">' + (r.object_count || 0).toLocaleString() + '</td>' +
    '<td>' + esc(lastTouch) + '</td></tr>';
}
function openProjectInBrowse(name) {
  // A drill-in stand-in for the real project PAGE (#93 step 2, blocked on the operator's
  // own read of "surfaces templates/primitives" — msg 5635) — filters Browse to this one
  // project via the SAME server-side scope filter #196 built, not a client-side re-derive.
  switchSurface('browse');
  selectRepos([name]);
}

// ── Fleet ────────────────────────────────────────────────────────────────────


// ── Focus / Inspect ──────────────────────────────────────────────────────────
async function focus(id) {
  FOCUS = id; postConsole({ focused_object_id: id });
  $('entity-taxonomy-bar').style.display = 'none'; $('viewsw').style.display = 'none';
  showBoard(); (ensureBoard()).clear();
  const g = await fetch('/objects/' + id + '/graph?hops=1').then(r => r.json());
  let capped = 0;
  if (g.nodes.length > 29) { capped = g.nodes.length - 1; const keep = new Set([id, ...g.nodes.filter(n => n.id !== id).slice(0, 28).map(n => n.id)]); g.nodes = g.nodes.filter(n => keep.has(n.id)); g.edges = g.edges.filter(e => keep.has(e.source) && keep.has(e.target)); }
  (ensureBoard()).mergeGraph(g); (ensureBoard()).layout((ensureBoard()).cy.nodes().length > 1); (ensureBoard()).focusNode(id);
  inspect(id);
  setStatus(capped ? 'Showing 28 of ' + capped + ' connections.' : (ensureBoard()).cy.nodes().length + ' objects on the board.');
}
async function inspect(id) {
  FOCUS = id;
  var obj = await fetch('/objects/' + id).then(function(r){return r.json();}).catch(function(){return null;});
  if (!obj) return;
  var right = $('right');
  right.className = 'rail';
  right.innerHTML = Osiris.objectDetail(obj, '');
  var relsEl = right.querySelector('[data-rels]');
  if (relsEl) await Osiris.loadRels(relsEl, id, inspectOnly, openAsSet);
}
function inspectOnly(id) {
  FOCUS = id;
  var badge = document.getElementById("focused-badge");
  if (badge && id) {
    badge.style.display = "inline";
    badge.textContent = "Inspect: " + id.slice(0, 8) + "";
  }
  inspect(id);
}
async function openAsSet(oid, type, dir, label) {
  const g = await fetch('/objects/' + oid + '/graph?hops=1').then(r => r.json());
  const lab = {}; g.nodes.forEach(n => lab[n.id] = n);
  const items = g.edges.filter(e => e.type === type && ((dir === 'out' && e.source === oid) || (dir === 'in' && e.target === oid))).map(e => ({ id: e.source === oid ? e.target : e.source, type: lab[e.source === oid ? e.target : e.source]?.type || '?', label: lab[e.source === oid ? e.target : e.source]?.label || '' }));
  SET = items; SELECTED_ENTITY_TYPES.clear(); ENTITY_VIEW_MODE = 'table'; ACTIVE_SURFACE = 'browse';
  document.querySelectorAll('.lens-item').forEach(el => el.classList.toggle('sel', el.dataset.surface === 'browse'));
  $('entity-taxonomy-bar').style.display = 'flex'; renderEntityExplorer();
}

// ── Actions ──────────────────────────────────────────────────────────────────
const _CONTENT_TYPES = new Set(['Commit','Reference','SoftwareProject']);
function actionsFor(type) { const a = []; if (_CONTENT_TYPES.has(type)) a.push({ label: 'Read \u25b8', run: viewContent }); a.push({ label: 'Search around', run: focus }); a.push({ label: 'Tag\u2026', run: tagIt }); return a; }
function primaryAction(id, type) { const a = actionsFor(type)[0]; if (a) a.run(id); }
async function viewContent(id) { inspect(id); }
async function tagIt(id) { const t = prompt('Tag:'); if (!t) return; await fetch('/objects/' + id + '/tag', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ tag: t }) }); }

let ACTIONMENU = null;
function closeActionMenu() { if (ACTIONMENU) { ACTIONMENU.remove(); ACTIONMENU = null; } }
function showActionMenu(x, y, id, type) {
  closeActionMenu(); const acts = actionsFor(type); if (!acts.length) return;
  const pop = document.createElement('div'); pop.className = 'pop'; pop.id = 'actionmenu'; pop.style.left = x + 'px'; pop.style.top = y + 'px'; pop.style.position = 'fixed';
  pop.innerHTML = acts.map(a => '<div class="keyrow" style="cursor:pointer;padding:4px 0" onmousedown="closeActionMenu()" onclick="(' + a.run.toString() + ')(\'' + id + '\')">' + esc(a.label) + '</div>').join('');
  document.body.appendChild(pop); ACTIONMENU = pop;
  setTimeout(() => document.addEventListener('click', function h() { closeActionMenu(); document.removeEventListener('click', h); }), 0);
}

// ── Intake ───────────────────────────────────────────────────────────────────
async function addSeed() { const raw = $('seed')?.value.trim(); if (!raw) return; try { const cases = await fetch('/cases').then(r => r.json()); const cid = cases.length ? cases[0].id : null; if (!cid) return; const r = await fetch('/cases/' + cid + '/intake', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ raw }) }).then(r => r.json()); if (r) { $('seedmsg').textContent = 'Added: ' + r.type; $('seed').value = ''; } } catch(e) {} }

// ── Console Sync ─────────────────────────────────────────────────────────────
function postConsole(fields) { fetch('/console', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(fields) }).then(r => r.json()).then(j => { CONSOLE_REV = j.rev; }).catch(() => {}); }
function setSyncBadge(by) { $('syncbadge').textContent = by === 'claude' ? '\u25cf agent' : ''; }
function watchConsole() { const es = new EventSource('/console/stream'); es.onmessage = async ev => { const s = JSON.parse(ev.data); if (s.rev == null || s.rev <= CONSOLE_REV) return; CONSOLE_REV = s.rev; if (s.updated_by !== 'human') { SYNCING = true; try { setSyncBadge(s.updated_by); if (s.room_id && s.room_id !== ROOM) await switchRoom(s.room_id); if (s.focused_object_id && s.focused_object_id !== FOCUS) inspectOnly(s.focused_object_id); } finally { SYNCING = false; } } }; }

// ── Pulse ────────────────────────────────────────────────────────────────────
async function updatePulse() {
  try {
    var p = await fetch("/pulse").then(function(r){return r.json();});
    var pulseEl = document.getElementById("fleet-pulse");
    if (pulseEl) pulseEl.textContent = p.line || "";
  } catch(e) {}
}

// ── Panes ────────────────────────────────────────────────────────────────────
const PANE = { l: { var: '--lw', min: 180, max: 640, def: 274 }, r: { var: '--rw', min: 220, max: 760, def: 350 } };
function loadPanes() { for (const k of ['l','r']) { const v = localStorage.getItem('osiris.pane.' + k); if (v) $('main').style.setProperty(PANE[k].var, v + 'px'); } }
function wireGrips() { const main = $('main'); document.querySelectorAll('[data-grip]').forEach(g => { g.onmousedown = e => { e.preventDefault(); const k = g.dataset.grip, cfg = PANE[k], rail = $(k === 'l' ? 'left' : 'right'); const start = e.clientX, w0 = rail.getBoundingClientRect().width; g.classList.add('on'); main.classList.add('dragging'); const move = ev => { const dw = (ev.clientX - start) * (k === 'l' ? 1 : -1); main.style.setProperty(cfg.var, Math.min(cfg.max, Math.max(cfg.min, w0 + dw)) + 'px'); }; const up = () => { g.classList.remove('on'); main.classList.remove('dragging'); localStorage.setItem('osiris.pane.' + k, parseFloat(main.style.getPropertyValue(cfg.var)) + ''); document.removeEventListener('mousemove', move); document.removeEventListener('mouseup', up); _afterResize(); }; document.addEventListener('mousemove', move); document.addEventListener('mouseup', up); }; }); }
function _afterResize() { if (board && (ensureBoard()).resizeFit) setTimeout(() => (ensureBoard()).resizeFit(), 180); }
function toggleLeft() { $('main').classList.toggle('lefthidden'); _afterResize(); }
function toggleRight() { $('main').classList.toggle('righthidden'); _afterResize(); }

// ── Omnisearch (Ctrl+K palette) ──────────────────────────────────────────────
let OMNI_SEL = 0, OMNI_ITEMS = [];
const POWER_TOOLS = [
  { label: 'Graph Lint', hint: 'Audit graph integrity', run: () => runTool('graph-lint') },
  { label: 'Who Is This', hint: 'Subject report', run: () => runTool('who-is-this') },
  { label: 'Co-Investment Ties', hint: 'Network analysis', run: () => runTool('co-investment-ties') },
  { label: 'Screen Financing', hint: 'Financing network', run: () => runTool('screen-financing-network') },
  { label: 'Op vs Disclosed Geo', hint: 'Geography discrepancy', run: () => runTool('operational-vs-disclosed-geography') },
  { label: 'Family Consistency', hint: 'Data consistency', run: () => runTool('family-consistency') },
  { label: 'Family Drift', hint: 'Data drift', run: () => runTool('family-drift') },
  { label: 'LAP', hint: 'License Analysis', run: () => runTool('lap') },
  { label: 'Overhead', hint: 'Tool traffic', run: () => runTool('overhead') },
  { label: 'Type Census', hint: 'Triage types', run: () => runTool('type-census') },
  { label: 'Closure Health', hint: 'Thread closures', run: () => runTool('closure-health') },
  { label: 'Echoes', hint: 'Open questions', run: () => runTool('echoes') },
  { label: 'The Wall', hint: 'Obligations wall', run: () => runTool('the-wall') },
  { label: 'Go to Browse', hint: 'Entity explorer', cat: 'Navigation', run: () => switchSurface('browse') },
  { label: 'Go to Mailbox', hint: 'Messages', cat: 'Navigation', run: () => switchSurface('mailbox') },
  
];

async function runTool(name) {
  try { setStatus('Running ' + name + '...'); const res = await fetch('/compositions/' + encodeURIComponent(name) + '/run', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ subject: FOCUS }) }).then(r => r.json()); if (res.error) { setStatus(res.error); return; } const container = $('result'); showPanel(); $('entity-taxonomy-bar').style.display = 'none'; $('viewsw').style.display = 'none'; container.innerHTML = '<div style="padding:8px 16px"><button class="iconbtn" onclick="switchSurface(\'browse\')">\u2190 Back to Browse</button></div><pre style="padding:16px;font-size:12px;line-height:1.6;white-space:pre-wrap;color:var(--text);max-height:70vh;overflow-y:auto">' + esc(JSON.stringify(res, null, 2)) + '</pre>'; setStatus('Ran ' + name + '.'); } catch(e) { setStatus('Could not run: ' + name); }
}

function expandSearchInput() { $('global-search-box').classList.add('expanded'); setTimeout(() => $('search').focus(), 50); }
function collapseSearchIfUnfocused() { if (document.activeElement !== $('search')) $('global-search-box').classList.remove('expanded'); }
function handleOmniSearchInput(val) { const clear = $('search-clear-btn'); if (clear) clear.style.display = val ? 'block' : 'none'; filterEntitySearch(val); runOmniSearch(val); }
function clearOmniSearch(e) { if (e) e.stopPropagation(); $('search').value = ''; handleOmniSearchInput(''); }
let OMNI_SEARCH_TOKEN = 0, OMNI_SEARCH_TIMER = null;
function runOmniSearch(q) {
  const dd = $('omni-dropdown'); if (!dd) return;
  if (!q || q.trim().length < 1) { dd.style.display = 'none'; return; }
  dd.style.display = 'flex';
  const ql = q.toLowerCase();
  const toolHits = POWER_TOOLS.filter(t => t.label.toLowerCase().includes(ql) || (t.hint || '').toLowerCase().includes(ql)).slice(0, 8);
  OMNI_ITEMS = toolHits;
  OMNI_SEL = Math.min(OMNI_SEL, OMNI_ITEMS.length - 1);
  renderOmniList(q);
  // Search is an ADDITION to enumeration, never a replacement (ruling 7a1a5517) — this
  // palette used to match ONLY the hardcoded POWER_TOOLS list, never the graph itself, so
  // "find a known thing by name" had no path here at all (#196, Thoth msg 5600). Debounced
  // (200ms) and token-guarded so a fast typist's stale response never clobbers a newer one.
  const myToken = ++OMNI_SEARCH_TOKEN;
  clearTimeout(OMNI_SEARCH_TIMER);
  OMNI_SEARCH_TIMER = setTimeout(async () => {
    let hits = [];
    try {
      const res = await fetch('/search?q=' + encodeURIComponent(q) + '&limit=8').then(r => r.json());
      hits = Array.isArray(res.hits) ? res.hits : (Array.isArray(res) ? res : []);
    } catch (e) { hits = []; }
    if (myToken !== OMNI_SEARCH_TOKEN) return; // a newer keystroke already superseded this
    const graphHits = hits.filter(h => h && h.id).map(h => ({
      label: h.display_label || h.label || h.name || h.canonical || h.id,
      hint: h.type || '', cat: 'Graph',
      run: () => { switchSurface('browse'); focus(h.id); },
    }));
    OMNI_ITEMS = toolHits.concat(graphHits).slice(0, 16);
    OMNI_SEL = Math.min(OMNI_SEL, OMNI_ITEMS.length - 1);
    renderOmniList(q);
  }, 200);
}
function renderOmniList(q) { const list = $('omni-list'); if (!list) return; if (!OMNI_ITEMS.length) { list.innerHTML = '<div class="dd-empty">No matches for "' + esc(q) + '".</div>'; return; } let html = '', lastCat = null; OMNI_ITEMS.forEach((c, i) => { const cat = c.cat || 'Tools'; if (cat !== lastCat) { html += '<div class="omni-cat">' + esc(cat) + '</div>'; lastCat = cat; } html += '<div class="omni-row' + (i === OMNI_SEL ? ' sel' : '') + '" data-i="' + i + '" onclick="execOmniItem(' + i + ')"><span class="omni-label">' + esc(c.label) + '</span>' + (c.hint ? '<span class="omni-hint">' + esc(c.hint) + '</span>' : '') + '</div>'; }); list.innerHTML = html; list.querySelectorAll('[data-i]').forEach(el => el.onmouseenter = () => { OMNI_SEL = +el.dataset.i; paintOmniSel(); }); paintOmniSel(); }
function paintOmniSel() { document.querySelectorAll('#omni-list .omni-row').forEach(el => el.classList.toggle('sel', +el.dataset.i === OMNI_SEL)); const sel = document.querySelector('#omni-list .sel'); if (sel) sel.scrollIntoView({ block: 'nearest' }); }
function omniKey(e) { const dd = $('omni-dropdown'), isOpen = dd && dd.style.display === 'flex'; if (e.key === 'ArrowDown') { e.preventDefault(); if (!isOpen) { runOmniSearch(e.target.value); return; } OMNI_SEL = Math.min(OMNI_SEL + 1, OMNI_ITEMS.length - 1); paintOmniSel(); } else if (e.key === 'ArrowUp') { e.preventDefault(); if (!isOpen) return; OMNI_SEL = Math.max(OMNI_SEL - 1, 0); paintOmniSel(); } else if (e.key === 'Enter') { if (isOpen && OMNI_ITEMS[OMNI_SEL]) { e.preventDefault(); execOmniItem(OMNI_SEL); } } else if (e.key === 'Escape') { e.preventDefault(); closeAllDropdowns(); } }
function execOmniItem(idx) { const item = OMNI_ITEMS[idx]; if (!item) return; closeAllDropdowns(); item.run(); }
function openPalette() { $('search').focus(); $('global-search-box').classList.add('expanded'); runOmniSearch($('search').value || ' '); }
async function openOmniSearch(val) { runOmniSearch(val); }

// ── Keyboard Shortcuts ───────────────────────────────────────────────────────
document.addEventListener('keydown', e => { const inField = /^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement?.tagName); if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') { e.preventDefault(); openPalette(); } else if (e.key === '/' && !inField) { e.preventDefault(); openPalette(); } else if (e.key === 'Escape') { closeAllDropdowns(); if ($('peek').className.includes('on')) closePeek(); } else if (e.key === '[' && !inField) { e.preventDefault(); toggleLeft(); } else if (e.key === ']' && !inField) { e.preventDefault(); toggleRight(); } });
function closePeek() { const o = $('peek'); o.className = 'peek-overlay'; o.innerHTML = ''; }

// ── Boot ─────────────────────────────────────────────────────────────────────
Osiris.loadSchema().then(async function() {
  ensureBoard();
  await Promise.all([loadProjects(), loadRooms(), loadObjectSet()]);
  var cur = await fetch('/console').then(function(r){return r.ok ? r.json() : null;}).catch(function(){return null;});
  await switchRoom((cur && cur.room_id) || '');
  switchSurface('browse');
  loadPanes(); wireGrips(); watchConsole();
  updatePulse(); setInterval(updatePulse, 8000);
});
