/* Osiris Console — three-surface operator interface.
 * Surfaces: Browse (entity explorer), Mailbox (fleet messages), Fleet (live agents).
 * Power tools via Ctrl+K palette. Depends on: osiris.js (Osiris namespace).
 */

const $ = id => document.getElementById(id);
const esc = s => (s == null ? "" : String(s)).replace(/[<>&]/g, c => ({ "<": "&lt;", ">": "&gt;", "&": "&amp;" }[c]));

// ROOM RETIREMENT (thread 96f09d48, decision 31717ca7, Thoth DM 10792): the operator's
// own word — "scope really died and made itself obsolete... gotta remove that too." ROOM
// is now a plain global constant, never reassigned (switchRoom/newRoom/loadRooms, the
// workspace pill and its dropdown are gone) — every existing reader that still checks it
// (loadCompositions' own room-scoped fetch, authorComposition/forkComposition's own
// room_id, watchConsole's other sync fields) keeps working unchanged, always taking its
// own "no room" branch, rather than needing every call site individually scrubbed.
const ROOM = '';
let FOCUS = null, SET = [], PROJECTS = [], ACTIVE_SURFACE = 'browse';
let SELECTED_ENTITY_TYPES = new Set(), ENTITY_SEARCH_QUERY = '', ENTITY_VIEW_MODE = 'table';
let TABLE_SORT_COL = 'date', TABLE_SORT_DIR = 'desc', EXPANDED_ROWS = new Set();
let SYNCING = false, CONSOLE_REV = 0, SHOW_AGENTS = false, SCOPE_FILTER = '';
let TRUE_COUNTS = null; // uncapped per-type census from /objects/counts (#196) — null until loaded
let OBJECTS_LIMIT = 1500, OBJECTS_HAS_MORE = false, OBJECTS_LOADING_MORE = false;

function setStatus(s) { $("status").textContent = s; }
function showBoard() { $("stage").classList.remove("panel"); }
function showPanel() { $('stage').classList.add('panel'); }

// ── Surface Switching ────────────────────────────────────────────────────────
async function switchSurface(surface) {
  if (surface !== 'pane') closePaneStream();  // never leak an open SSE connection off-pane
  ACTIVE_SURFACE = surface; postConsole({ surface });
  document.querySelectorAll('.lens-item').forEach(el => el.classList.toggle('sel', el.dataset.surface === surface));
  $('page-title').textContent = surface.charAt(0).toUpperCase() + surface.slice(1);
  if (surface === 'browse') {
    $('entity-taxonomy-bar').style.display = 'flex';
    showBoard(); // the space canvas (#cy) + browse's own table drawer, never the shared #result panel
    if (window.OsirisSpace) window.OsirisSpace.resume(); // render-on-demand (mail 10581): don't render an invisible canvas
    if (!SET.length) await loadObjectSet(); renderEntityExplorer();
  } else {
    $('entity-taxonomy-bar').style.display = 'none';
    showPanel();
    if (window.OsirisSpace) window.OsirisSpace.pause();
    if (surface === 'mailbox') renderMailbox();
    if (surface === 'pane') renderPane();
  }
}

// ROOM RETIREMENT (thread 96f09d48, decision 31717ca7, Thoth DM 10792): the workspace
// pill, its dropdown, and the switchRoom/newRoom/loadRooms/selectWorkspace/
// updateWorkspaceScopeUI/toggleWorkspaceDropdown/renderWorkspaceDropdown functions that
// drove it are gone -- "one scope" (the header repo selector, console chrome cleanup
// part 2) replaces the room dimension. closeAllDropdowns() drops its own
// workspace-dropdown/workspace-pill lines since neither element exists anymore.
function closeAllDropdowns() {
  var rd = document.getElementById('repo-dropdown'); if(rd) rd.style.display='none';
  var od = document.getElementById('omni-dropdown'); if(od) od.style.display='none';
  var rp = document.getElementById('repo-pill'); if(rp) rp.classList.remove('open');
  collapseSearchIfUnfocused();
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
  updateRepoPill();
  // THE LAST RENDERER (Thoth mail 11066/11087): "the header dropdown closes on pick" --
  // it used to stay open and re-render itself in place (renderRepoDropdown), which read
  // as a pick doing nothing. A pick is a real commit to the selection now, same as every
  // other single-gesture commit in this console; re-open the dropdown for a second pick.
  closeAllDropdowns();
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
  syncSpaceProjectFilter();
}
// CONSOLE CHROME CLEANUP piece 2 (decision 31717ca7): the header's repo selector drives
// the canvas through the SAME per-instance visibility flag syncSpaceTypeFilter already
// uses for the type pills (space.js's setHiddenProjects, sibling to setHiddenTypes) —
// SELECTED_REPOS is an ALLOWLIST of stripped repo names (empty = show all, "All Repos"),
// translated to a hidden-set of RAW nd.project values (space.js's own project field
// carries the "repo:" canonical prefix, or the literal "unfiled") before handing it to
// the graph, which speaks "hidden" not "allowed" — same translation shape
// syncSpaceTypeFilter already does for types.
function syncSpaceProjectFilter() {
  const space = window.OsirisSpace; if (!space || !space.setHiddenProjects) return;
  if (SELECTED_REPOS.length === 0) { space.setHiddenProjects(new Set()); return; }
  const selected = new Set(SELECTED_REPOS);
  const present = new Set(space.idToNode.map(function(n){ return n.project; }));
  const hidden = new Set([...present].filter(function(p){
    return !selected.has((p || '').replace(/^repo:/, ''));
  }));
  space.setHiddenProjects(hidden);
}
function updateRepoPill() {
  var pill = document.getElementById("repo-pill-label");
  if (pill) pill.textContent = SELECTED_REPOS.length ? SELECTED_REPOS.join(", ") : "All Repos";
}

function objectScopeParams() {
  // The scope bits browseScope() and objectCountsUrl() both need — split out so the
  // counts fetch (#196, Thoth msg 5600) reads the exact same scope the entity set's own
  // load would, never a second, drifting copy of the same three branches.
  //
  // ROOM RETIREMENT (thread 96f09d48): the room-subject leg (a room's own config could
  // bind a repo:/case: subject, narrowing this before the repo pill's own SCOPE_FILTER
  // ever got read) is gone along with the room concept itself — the repo pill is now the
  // ONE scoping lever.
  var ex = SHOW_AGENTS ? '' : '&exclude_types=Agent';
  var repos = SCOPE_FILTER ? SCOPE_FILTER.split(',').filter(Boolean) : [];
  return { extra: ex, project: repos, case_id: null };
}
// THE BROWSE-TAB CUTOVER (Thoth dispatch 9838/9855, 588148bb): the entity set's own load
// used to GET /objects directly; it now runs an EPHEMERAL {"op":"select","scope":{...}}
// op-tree through /compositions/run-spec — the exact same `scope` opt-in (and, since it's
// the SAME extraction, the exact same list_objects_scoped SQL) /objects itself calls, so
// there is truly one definition, not a REST caller and a composition caller drifting apart.
// Type pills/search/sort stay client-side residue (unchanged, over whatever SET holds) —
// per Thoth's own dispatch shape, same discipline the Projects swap used.
// THE TABLE FILTER QUERY SHAPE (thread 0be2f790's own operator-finding follow-up, Thoth DM
// 10711): shared by browseScope() (drives the server query) and getFilteredEntities() (the
// client-side re-check kept for defense in depth — see browseScope's own comment below).
// Only the `status:`/free-text portions are parsed here; a typed `type:` tag stays a
// client-only substring convenience over whatever the pill bar already scoped server-side
// (pills are the exact-match multi-select; the inline tag was always a fuzzy refinement, and
// pushing it server-side as an equality filter would silently break a partial match like
// "type:pers").
function parseEntitySearchQuery(raw) {
  var q = (raw || '').trim();
  var tMatch = q.match(/\btype:([a-zA-Z0-9_-]+)/i), sMatch = q.match(/\bstatus:([a-zA-Z0-9_-]+)/i);
  var type = tMatch ? tMatch[1] : null, status = sMatch ? sMatch[1] : null;
  if (tMatch) q = q.replace(tMatch[0], '').trim();
  if (sMatch) q = q.replace(sMatch[0], '').trim();
  return { type: type, status: status, text: q };
}
function browseScope(cursor) {
  var s = objectScopeParams();
  var scope = { limit: OBJECTS_LIMIT };
  if (!SHOW_AGENTS) scope.exclude_types = ['Agent'];
  if (s.case_id) scope.case_id = s.case_id;
  if (s.project.length) scope.project = s.project;
  // THE TABLE FILTER QUERY SHAPE: the type-filter pill bar and the omnibox's own free-text/
  // status: portion now drive the SERVER-side scope, not just an in-memory re-filter over
  // whatever page happened to already be loaded — the fix for the operator paging through
  // 26,351 of 30,290 rows to find 5 matches. getFilteredEntities() still re-applies the same
  // predicates client-side: a no-op once the server has already scoped SET to them, but
  // still load-bearing for the props/free-text match nuance and for the reachable-set
  // narrowing renderEntityExplorerStage layers on top for a canvas path focus (no server
  // equivalent).
  if (SELECTED_ENTITY_TYPES.size) scope.types = Array.from(SELECTED_ENTITY_TYPES);
  var parsed = parseEntitySearchQuery(ENTITY_SEARCH_QUERY);
  if (parsed.status) scope.status = parsed.status;
  if (parsed.text) scope.q = parsed.text;
  // KEYSET continuation (#93 step 3, Thoth msg 5668) — the cursor #196 built and proved
  // (before_created_at/before_id, migration 0054's objects_type_created_idx). Omitted for
  // the initial load (unchanged behavior); passed here only by loadMoreObjects() below —
  // the composition's own `scope.cursor` is the SAME shape select's scope arg accepts.
  if (cursor) scope.cursor = { before_created_at: cursor.created_at, before_id: cursor.id };
  return scope;
}
async function runBrowseSelect(cursor) {
  var spec = { op: 'select', scope: browseScope(cursor) };
  var res = await fetch('/compositions/run-spec', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ spec: spec, name: 'browse' }),
  }).then(function(r){return r.json();}).catch(function(){return null;});
  var items = (res && Array.isArray(res.items)) ? res.items : [];
  // Bridge the composition's generic packaged shape ({label,display_label} plus the
  // opt-in status/created_at that ride along with `scope`) onto the exact object shape
  // the entity explorer's rendering/search/sort/footer always consumed (name/
  // display_label/status/created_at/props) — a client-side adapter, not a backend
  // change: object_items' own shape stays generic for every OTHER composition.
  return items.map(function(it) {
    return { id: it.id, type: it.type, canonical: it.canonical, status: it.status,
             created_at: it.created_at, props: it.props || {},
             name: it.display_label || it.label, display_label: it.display_label };
  });
}
function objectCountsUrl() {
  var s = objectScopeParams();
  var url = '/objects/counts?' + s.extra.replace(/^&/, '');
  if (s.case_id) url += '&case_id=' + encodeURIComponent(s.case_id);
  for (var i = 0; i < s.project.length; i++) url += '&project=' + encodeURIComponent(s.project[i]);
  return url;
}
async function loadObjectSet() {
  SET = await runBrowseSelect();
  // A full-length page is the only signal the scope gives that there might be more (no
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
  loadEdgeCounts(SET.map(function(o){ return o.id; }));
}
// WAVE A item 7 (thread 8839): Browse tiles carry an edge-count badge — fetched separately
// from the object list itself (a per-row COUNT joined into that already-complex query would
// cost every one of its many callers, not just Browse), batched in chunks small enough to
// stay a sane query-string length, and rendered as soon as each chunk lands rather than
// blocking the table/board's own first paint on it.
var EDGE_COUNTS = {};
async function loadEdgeCounts(ids) {
  var CHUNK = 150;
  for (var i = 0; i < ids.length; i += CHUNK) {
    var chunk = ids.slice(i, i + CHUNK);
    try {
      var counts = await fetch('/objects/edge_counts?ids=' + chunk.join(',')).then(function(r){return r.json();});
      Object.assign(EDGE_COUNTS, counts);
      if (ACTIVE_SURFACE === 'browse') renderEntityExplorerStage();
    } catch(e) {}
  }
}
function edgeCountBadge(id) {
  var n = EDGE_COUNTS[id];
  return n ? '<span class="ee-edge-badge" title="' + n + ' link(s)">' + n + '</span>' : '';
}
async function loadMoreObjects() {
  if (OBJECTS_LOADING_MORE || !OBJECTS_HAS_MORE || !SET.length) return;
  var last = SET[SET.length - 1];  // SET is created_at DESC — the last row is the oldest loaded
  if (!last || !last.created_at) { OBJECTS_HAS_MORE = false; renderEntityExplorerStage(); return; }
  OBJECTS_LOADING_MORE = true;
  renderEntityExplorerStage();  // shows the loading state on the button immediately
  try {
    var page = await runBrowseSelect({ created_at: last.created_at, id: last.id });
    SET = SET.concat(page);
    OBJECTS_HAS_MORE = page.length >= OBJECTS_LIMIT;
  } finally {
    OBJECTS_LOADING_MORE = false;
    renderEntityExplorerStage();
  }
}

// ── Entity Explorer ──────────────────────────────────────────────────────────
function getFilteredEntities() {
  const parsed = parseEntitySearchQuery(ENTITY_SEARCH_QUERY);
  const fType = parsed.type ? parsed.type.toLowerCase() : null, fStatus = parsed.status ? parsed.status.toLowerCase() : null;
  const textQ = (parsed.text || '').toLowerCase(), isAllOn = SELECTED_ENTITY_TYPES.size === 0;
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
  //
  // THE TABLE FILTER QUERY SHAPE (Thoth DM 10711): TRUE_COUNTS itself is never scoped by
  // type/status/q (it stays the honest, uncapped census across ALL types, so every pill's
  // own count keeps showing the full landscape regardless of what's currently selected) —
  // once a type/status/text filter is active, SET *is* the scoped population (browseScope()
  // now fetches exactly that from the server), so the big total badge reads SET.length
  // instead of the unscoped global total. Same honesty caveat as SET-derived counts
  // generally: if the scoped population itself exceeds OBJECTS_LIMIT (OBJECTS_HAS_MORE),
  // this undercounts — no uncapped *scoped* census endpoint exists yet, disclosed rather
  // than silently wrong.
  // review flaw #5 (TIP 1c, Thoth mail 10891): "the count follows the lens or says
  // nothing" -- while a real focus is on, the header's own total tracked an unrelated
  // global/filtered count (measured: 1,076 during a 3-node focus) instead of the actual
  // reachable set the canvas and table were both scoped to.
  const space = window.OsirisSpace;
  const focused = space && space.pathFocusId;
  const filterActive = SELECTED_ENTITY_TYPES.size > 0 || !!(ENTITY_SEARCH_QUERY || '').trim();
  const trueTotal = focused ? space.pathReachable.size
    : filterActive ? SET.length : (TRUE_COUNTS ? TRUE_COUNTS.total : SET.length);
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
// THE TABLE FILTER QUERY SHAPE (Thoth DM 10711) + THE LEGIBILITY PASS TIP 1(e) (ruling
// e1cb9e3b), merged: a pill toggle now does THREE things — re-render locally for
// immediate pill/search-box feedback, sync the canvas's own hidden-type set
// (syncSpaceTypeFilter, landed w268), and re-fetch the scoped row set from the server
// (refetchFilteredObjectSet, this tip) rather than only re-filtering whatever page was
// already loaded. All three read the SAME SELECTED_ENTITY_TYPES — one selection, three
// consumers, never a second notion of "what's currently filtered."
let ENTITY_FILTER_DEBOUNCE = null;
async function refetchFilteredObjectSet() {
  SET = [];
  await loadObjectSet();
  renderEntityExplorer();
}
function toggleEntityType(t) {
  if (t === 'All') SELECTED_ENTITY_TYPES.clear();
  else { if (SELECTED_ENTITY_TYPES.has(t)) SELECTED_ENTITY_TYPES.delete(t); else SELECTED_ENTITY_TYPES.add(t); }
  renderEntityExplorer();
  syncSpaceTypeFilter();
  refetchFilteredObjectSet();
}
// THE LEGIBILITY PASS, TIP 1(e) (ruling e1cb9e3b): the header taxonomy pills drive the
// canvas through the same per-instance visibility flag the legend's own node-type
// checkboxes use (space.js's setHiddenTypes) -- SELECTED_ENTITY_TYPES is an ALLOWLIST
// (empty = show all) so it's translated to a hidden-set (everything present, minus the
// allowlist) before handing it to the graph, which speaks "hidden" not "allowed".
function syncSpaceTypeFilter() {
  const space = window.OsirisSpace; if (!space || !space.setHiddenTypes) return;
  if (SELECTED_ENTITY_TYPES.size === 0) { space.setHiddenTypes(new Set()); return; }
  const present = new Set(space.idToNode.map(n => n.type));
  const hidden = new Set([...present].filter(t => !SELECTED_ENTITY_TYPES.has(t)));
  space.setHiddenTypes(hidden);
}
function filterEntitySearch(q) {
  ENTITY_SEARCH_QUERY = q;
  renderEntityExplorer();
  // Debounced (250ms): the omnibox drives this on every keystroke (handleOmniSearchInput),
  // and status:/free-text now hits list_objects_scoped's real query — a refetch per
  // keystroke would hammer the DB for no benefit while the operator is still typing. Free
  // text/status: don't touch the canvas — only the type-pill allowlist drives
  // syncSpaceTypeFilter, unchanged from w268.
  clearTimeout(ENTITY_FILTER_DEBOUNCE);
  ENTITY_FILTER_DEBOUNCE = setTimeout(refetchFilteredObjectSet, 250);
}
function toggleTableSort(col) { TABLE_SORT_DIR = TABLE_SORT_COL === col ? (TABLE_SORT_DIR === 'asc' ? 'desc' : 'asc') : 'asc'; TABLE_SORT_COL = col; renderEntityExplorerStage(); }
function inspectAndToggleRow(id) { inspectOnly(id); EXPANDED_ROWS.has(id) ? EXPANDED_ROWS.delete(id) : EXPANDED_ROWS.add(id); renderEntityExplorerStage(); }
function sortIcon(col) { return TABLE_SORT_COL === col ? (TABLE_SORT_DIR === 'asc' ? ' \u25b4' : ' \u25be') : ''; }
async function renderEntityExplorer() { $('entity-taxonomy-bar').style.display = 'flex'; renderEntityToolbar(); renderEntityExplorerStage(); }
// NAVIGABLE SPACE, INTEGRATION (mail 10550): "graph and table COEXIST in the middle — your
// default: the graph canvas fills the section with the table as a collapsible drawer beneath
// it, the Table/Graph switch becomes 'show table'". Table/Board/Graph's old three-way
// view-tab switcher (renderSwitcher, ENTITY_VIEW_MODE) is superseded by this one boolean;
// #viewsw and Board itself are gone for real now (piece 3, ruling c5953bb1).
let TABLE_DRAWER_OPEN = false;
function toggleTableDrawer() {
  TABLE_DRAWER_OPEN = !TABLE_DRAWER_OPEN;
  const el = $('browse-drawer'); if (el) el.classList.toggle('open', TABLE_DRAWER_OPEN);
}
// THE TABLE-DRAWER-READS-0 FIX (Thoth mail 11241, live review of w299): SET is a
// paginated, created_at-DESC page of RECENT objects (loadObjectSet/runBrowseSelect) —
// architecturally unrelated to a focus walk's own reachable ids, so a real focus (an
// ordinary ego walk, and especially a container drill's expanded page) almost always lit
// rows the table had never loaded, reading as "No matching entities" even though the
// canvas showed plenty. Hydrated by id from the server instead of requiring SET to already
// carry them — bounded by the walk's own cap (MAX_EGO_NODES = 300), never unbounded.
const FOCUS_HYDRATED_OBJECTS = new Map(); // id -> object, cleared whenever the focus changes
let FOCUS_HYDRATE_FOR = null; // the pathFocusId this cache's own in-flight fetch belongs to
async function hydrateFocusReachable(reachable, focusId) {
  const known = new Set(SET.map(function(o) { return o.id; }));
  const missing = [...reachable].filter(function(id) {
    return !known.has(id) && !FOCUS_HYDRATED_OBJECTS.has(id);
  });
  if (!missing.length) return;
  const fetched = await Promise.all(missing.map(function(id) {
    return fetch('/objects/' + id).then(function(r) { return r.ok ? r.json() : null; }).catch(function() { return null; });
  }));
  for (let i = 0; i < missing.length; i++) {
    if (fetched[i]) FOCUS_HYDRATED_OBJECTS.set(missing[i], fetched[i]);
  }
  // the focus (or the drill's own expanded page within it) may have moved on while this
  // was in flight -- only repaint if it's still the same one asking.
  if (window.OsirisSpace && window.OsirisSpace.pathFocusId === focusId) renderEntityExplorerStage();
}
function renderEntityExplorerStage() {
  let filtered = getFilteredEntities();
  // THE READING LAYER, part C ("harmony", ruling c5953bb1): "the table filters to the
  // reachable set while a focus is on" — a real path focus (not a plain select) narrows
  // the table to exactly what the canvas is showing lit.
  var space = window.OsirisSpace;
  if (space && space.pathFocusId) {
    var reachable = space.pathReachable;
    if (FOCUS_HYDRATE_FOR !== space.pathFocusId) { FOCUS_HYDRATED_OBJECTS.clear(); FOCUS_HYDRATE_FOR = space.pathFocusId; }
    var known = filtered.filter(function(o) { return reachable.has(o.id); });
    var knownIds = new Set(known.map(function(o) { return o.id; }));
    var hydrated = [...reachable].filter(function(id) { return !knownIds.has(id) && FOCUS_HYDRATED_OBJECTS.has(id); })
      .map(function(id) { return FOCUS_HYDRATED_OBJECTS.get(id); });
    filtered = known.concat(hydrated);
    hydrateFocusReachable(reachable, space.pathFocusId); // fire-and-forget; re-renders itself when it lands
  }
  setStatus(filtered.length + ' of ' + SET.length + ' entities');
  const countEl = $('browse-drawer-count'); if (countEl) countEl.textContent = filtered.length.toLocaleString();
  renderTableProjection($('browse-table'), filtered);
}
// shares the selection between the space canvas and the table drawer (mail 10550's own
// "the two share the selection") — space.js calls this via its onFocus hook (index.html's
// module script wires window.onSpaceFocus through), console.js's own inspectOnly/focus
// reach FOCUS directly and call it too so a table click paints the same way.
function onSpaceFocus(id) {
  FOCUS = id;
  if (ACTIVE_SURFACE === 'browse') {
    renderEntityExplorerStage(); // repaints the 'sel' row
    renderEntityToolbar(); // review flaw #5: the header total must track the focus too
  }
}
window.onSpaceFocus = onSpaceFocus;

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
    '<td><span class="ee-type-pill" style="border-color:' + tColor + '40;color:' + tColor + ';background:' + tColor + '18"><span class="dot" style="background:' + tColor + '"></span> ' + esc(o.type) + '</span> ' + edgeCountBadge(o.id) + '</td>' +
    '<td>' + renderKey(o) + '</td>' +
    '<td class="ee-summary-cell"><div class="ee-name">' + esc(o.display_label || o.name || summary || o.id) + '</div>' + (summary && summary !== o.name ? '<div class="ee-summary-preview">' + esc(summary) + '</div>' : '') + '</td>' +
    '<td style="color:var(--muted);font-size:11px;font-family:var(--font-mono)">' + esc(dateStr) + '</td>' +
    '<td style="text-align:right"><span class="ee-status status-' + esc(o.status || 'active') + '">' + esc(o.status || 'active') + '</span></td>' +
    '</tr>' +
    (isExp ? '<tr class="ee-tray-row"><td colspan="5"><div class="ee-inline-tray"><div class="ee-tray-head"><span class="ee-tray-title">STATEMENT / RATIONALE</span><div class="ee-tray-badges">' + (grade ? '<span class="card-tag grade-' + esc(grade) + '">' + esc(grade.replace(/_/g, ' ')) + '</span>' : '') + (source ? '<span class="card-tag">source: ' + esc(source) + '</span>' : '') + '<button class="iconbtn" style="padding:1px 6px;font-size:10px" onclick="event.stopPropagation();focus(\'' + o.id + '\')">Open in Graph &#9658;</button></div></div><div class="ee-tray-body">' + esc(summary || o.name || '') + '</div>' + (Object.keys(p).length ? '<div class="ee-tray-props">' + Object.entries(p).filter(function(e){return ['summary','rationale','statement','title','description'].indexOf(e[0]) === -1;}).slice(0, 8).map(function(e){return '<div class="ee-prop-item"><span class="ee-prop-k">' + esc(e[0]) + ':</span> <span class="ee-prop-v">' + esc(String(e[1])) + '</span></div>';}).join('') + '</div>' : '') + '</div></td></tr>' : '');
}

// ── Mailbox ──────────────────────────────────────────────────────────────────
// Ported off /pulse (which never carried a `messages` array — src/api/app.py's pulse_route
// only ever returned {line, live, owed, briefs, wakes, spend}; the old code silently rendered
// "No messages" even when mail was waiting) onto the REAL mail read: the "mail" saved
// composition (MAIL_OVERVIEW, compositions.py) wraps chrome.mail_overview — the same fold-
// aware room/soul read /mail's own HTML view uses. #92's own drill-in (task #90, Thoth msg
// 1976/2005): each row's `row_action` is "run:mail_threads" — the click delegate in osiris.js
// dispatches an `osiris:run` DOM event rather than POSTing, and this is the page shell that
// event was always meant to be caught by (never wired to any listener until now).
async function renderMailbox() { await runMailboxComposition('mail', {}); }

async function runMailboxComposition(name, args) {
  var container = $('result'); showPanel();
  container.innerHTML = '<div class="o-empty" style="padding:40px">Loading…</div>';
  try {
    var isFunctionDrill = args && Object.keys(args).length > 0;
    var res = isFunctionDrill
      ? await fetch('/compositions/run-spec', {
          method: 'POST', headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ spec: { op: 'function', name: name, args: args }, name: name }),
        }).then(function(r){ return r.json(); })
      : await fetch('/compositions/' + encodeURIComponent(name) + '/run', {
          method: 'POST', headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ subject: null }),
        }).then(function(r){ return r.json(); });
    if (res.error) {
      container.innerHTML = '<div class="o-empty" style="padding:40px">' + esc(res.error) + '</div>';
      return;
    }
    var panel = document.createElement('div');
    panel.style.padding = '16px';
    await Osiris.renderResult(res, { panel: panel }, Osiris.defaultView(res), null, null, null);
    container.innerHTML = '';
    if (isFunctionDrill) {
      var back = document.createElement('div');
      back.style.padding = '0 0 8px';
      back.innerHTML = '<button class="iconbtn" onclick="renderMailbox()">' + String.fromCharCode(8592) + ' Back to Mailbox</button>';
      container.appendChild(back);
    }
    container.appendChild(panel);
    setStatus(res.count + (res.count === 1 ? ' item' : ' items'));
  } catch(e) {
    console.error('runMailboxComposition failed', name, args, e);
    container.innerHTML = '<div class="o-empty" style="padding:40px">Could not load mailbox.</div>';
  }
}

// osiris.js's click delegate dispatches this for any `"run:<function>"` row action (built for
// exactly this case — see its own comment); scoped to the mailbox surface so a future consumer
// of the same event elsewhere in the shell (the composer, piece 2) isn't shadowed by this one.
document.addEventListener('osiris:run', function(e) {
  if (ACTIVE_SURFACE !== 'mailbox') return;
  runMailboxComposition(e.detail.name, e.detail.args || {});
});

// ── Pane (Thoth dispatch 9378, lane B piece 2, thread 9d2aaf4d) ───────────────
// THE READ-ONLY PANE: pick a live seat, watch its transcript stream live — no writes, no
// spawn. PICK is /pane/live (a lean slice of the same live/seated fold /fleet already
// computes); the stream is /pane/{agent_id}/stream (SSE), text already role-tagged
// OPERATOR:/CLAUDE: server-side by sessions.py's own distill() — this surface only appends
// what arrives, never re-derives or re-parses it.
var PANE_SOURCE = null;
function closePaneStream() { if (PANE_SOURCE) { PANE_SOURCE.close(); PANE_SOURCE = null; } }
async function renderPane() {
  closePaneStream();
  var container = $('result'); showPanel();
  container.innerHTML = '<div class="o-empty" style="padding:40px">Loading live seats…</div>';
  try {
    var agents = await fetch('/pane/live').then(function(r){ return r.json(); });
    if (!agents.length) { container.innerHTML = '<div class="o-empty" style="padding:40px">No live seated agents right now.</div>'; return; }
    var picker = '<div style="padding:16px;max-width:900px;margin:0 auto">' +
      '<h2 style="font-size:13px;text-transform:uppercase;letter-spacing:0.5px;color:var(--muted);margin-bottom:12px">Pick a live seat (' + agents.length + ')</h2>' +
      agents.map(function(a){ return '<button class="iconbtn" style="margin:0 8px 8px 0" onclick="openPaneStream(' + JSON.stringify(a.agent_id) + ')">' + esc(a.seat) + ' <span class="o-faint">(' + esc(a.project) + ')</span></button>'; }).join('') +
      '</div><div id="pane-stream" style="padding:0 16px"></div>';
    container.innerHTML = picker;
  } catch(e) {
    console.error('renderPane failed', e);
    container.innerHTML = '<div class="o-empty" style="padding:40px">Could not load live seats.</div>';
  }
}
var PANE_AGENT = null;
function openPaneStream(agentId) {
  closePaneStream();
  PANE_AGENT = agentId;
  var out = $('pane-stream');
  if (!out) return;
  out.innerHTML = '<h2 style="font-size:13px;text-transform:uppercase;letter-spacing:0.5px;color:var(--muted);margin:12px 0">' + esc(agentId) + '</h2><pre id="pane-text" style="white-space:pre-wrap;font-size:12px;line-height:1.6;color:var(--text);max-height:60vh;overflow-y:auto;background:var(--panel);border:1px solid var(--border);border-radius:8px;padding:12px 14px"></pre>' +
    '<div style="display:flex;gap:8px;margin-top:8px"><input id="pane-reply-input" class="filter" style="flex:1" placeholder="Reply into this seat’s own session…" onkeydown="if(event.key===\'Enter\')sendPaneReply()" /><button class="iconbtn" onclick="sendPaneReply()">Send</button></div>';
  var pre = $('pane-text');
  PANE_SOURCE = new EventSource('/pane/' + encodeURIComponent(agentId) + '/stream');
  PANE_SOURCE.onmessage = function(ev) {
    var msg; try { msg = JSON.parse(ev.data); } catch(e) { return; }
    if (msg.error) { pre.textContent += '\n[' + msg.error + ']\n'; closePaneStream(); return; }
    if (msg.text) { pre.textContent += (pre.textContent ? '\n\n' : '') + msg.text; pre.scrollTop = pre.scrollHeight; }
  };
  PANE_SOURCE.onerror = function() { /* EventSource auto-retries; nothing to do here */ };
}
// THE REPLY DOOR (Thoth dispatch 9378, lane B piece 3): posts through the seat's own
// harness-agnostic adapter (/pane/{agent}/reply) — a one-shot turn against the seat's
// ALREADY-RUNNING session, never a new spawn. The reply itself lands in the same
// transcript file the stream above is already tailing, so it needs no separate render
// path — it just shows up.
async function sendPaneReply() {
  var input = $('pane-reply-input');
  if (!input || !PANE_AGENT) return;
  var prompt = input.value.trim();
  if (!prompt) return;
  input.value = ''; input.disabled = true;
  try {
    var res = await fetch('/pane/' + encodeURIComponent(PANE_AGENT) + '/reply', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ prompt: prompt }),
    }).then(function(r){ return r.json(); });
    if (res.error) { setStatus(res.error); }
  } catch(e) {
    setStatus('reply failed — ' + e);
  } finally {
    input.disabled = false; input.focus();
  }
}

// ── Settings Menu (THE SETTINGS MENU, ruling be1b2e47, thread 7eb26f68 pieces 2+3) ────
// One generic view over src/config/settings_registry.py's own SETTINGS tuple, rendered
// from settings(action='list') — a new knob needs a new registry entry, never a new
// renderer, unless it introduces a genuinely new `type` this switch doesn't yet cover.
// Piece 3 folded the old dedicated "Backup settings…" panel's own 3 fields
// (backup.vault_path, one backup.timer_schedule.<unit> per timer, backup.offbox_
// repositories) into this same tuple — they render here, grouped under one "backup"
// section (key-prefix grouping, see renderSettingsPanelHtml below), no separate panel or
// CMD-K entry anymore.
// `live` (thread c5ba8681, Imhotep's own follow-up, not yet built) is read defensively
// here — when a future list_settings response carries a non-null `live` per item, it
// just appears beside `value`; no UI change needed when it arrives. Reached via CMD-K
// ("Settings…") — no standing-lens case for a tab.
function settingsEffectProse(effect) {
  if (effect === 'immediate') return 'takes effect immediately';
  if (effect === 'next_tick') return 'takes effect on the next tick';
  if (effect === 'next_deploy') return 'takes effect on the next osiris deploy';
  if (effect && effect.indexOf('restart:') === 0) {
    return 'takes effect on ' + effect.slice(8) + '’s next restart';
  }
  return effect || '';
}
var SETTINGS_LIST = null;
async function renderSettingsPanel() {
  const container = $('result'); showPanel();
  $('entity-taxonomy-bar').style.display = 'none';
  container.innerHTML = '<div class="o-empty" style="padding:40px">Loading settings…</div>';
  try {
    SETTINGS_LIST = (await fetch('/settings').then(r => r.json())).settings || [];
  } catch(e) {
    container.innerHTML = '<div class="o-empty" style="padding:40px">Could not load settings.</div>';
    return;
  }
  container.innerHTML = renderSettingsPanelHtml(SETTINGS_LIST);
}
function settingsFieldInput(item) {
  var id = 'setting-val-' + item.key;
  var v = item.value;
  if (item.type === 'secret_ref') {
    return '<span class="o-faint">' + (v && v.set ? '(set)' : '(not set)') + '</span>';
  }
  if (item.type === 'bool') {
    return '<input type="checkbox" id="' + id + '"' + (v ? ' checked' : '') + ' />';
  }
  if (item.type === 'enum') {
    var opts = (item.choices || []).map(function(c) {
      return '<option value="' + esc(c) + '"' + (c === v ? ' selected' : '') + '>' + esc(c) + '</option>';
    }).join('');
    return '<select id="' + id + '">' + opts + '</select>';
  }
  if (item.type === 'int' || item.type === 'float') {
    return '<input type="number" id="' + id + '" value="' + esc(v) + '"' +
      (item.type === 'float' ? ' step="any"' : '') + ' style="width:120px" />';
  }
  if (item.type === 'json' || item.type === 'records') {
    return '<textarea id="' + id + '" rows="3" style="width:360px;font-family:monospace">' +
      esc(JSON.stringify(v, null, 2)) + '</textarea>';
  }
  // str / path / schedule — plain text; path/schedule may be null (no override, uses
  // the shipped default) — an empty box round-trips to null on save (settingsFieldValue).
  var ph = (item.type === 'path' || item.type === 'schedule')
    ? ' placeholder="(unset — uses shipped default)"' : '';
  return '<input type="text" id="' + id + '" value="' + esc(v == null ? '' : v) + '"' +
    ph + ' style="width:280px" />';
}
function renderSettingsPanelHtml(items) {
  var groups = {}, order = [];
  items.forEach(function(it) {
    var g = it.key.split('.')[0];
    if (!groups[g]) { groups[g] = []; order.push(g); }
    groups[g].push(it);
  });
  var sections = order.map(function(g) {
    var rows = groups[g].map(function(it) {
      var live = (it.live !== undefined && it.live !== null)
        ? ' <span class="o-faint" title="the value actually running right now">live: ' +
          esc(JSON.stringify(it.live)) + '</span>' : '';
      return '<tr><td style="vertical-align:top"><code>' + esc(it.key) + '</code>' +
        (it.consequence === 'high' ? ' <span title="high consequence" style="color:#e5534b">▲</span>' : '') +
        '</td><td style="vertical-align:top">' + settingsFieldInput(it) + live +
        '<div id="setting-err-' + esc(it.key) + '" class="o-faint" style="color:#e5534b"></div></td>' +
        '<td class="o-faint" style="vertical-align:top">' + esc(settingsEffectProse(it.effect)) + '</td>' +
        '<td style="vertical-align:top">' + (it.type === 'secret_ref' ?
          '<button class="iconbtn" onclick="rotateSecret(\'' + esc(it.key) + '\')">Rotate</button>' :
          '<button class="iconbtn" onclick="saveSetting(\'' + esc(it.key) + '\')">Save</button>') + '</td></tr>';
    }).join('');
    return '<h3 style="font-size:12px;text-transform:uppercase;letter-spacing:0.5px;color:var(--muted);margin:20px 0 8px">' +
      esc(g) + '</h3><table class="ee-table"><thead><tr><th>Key</th><th>Value</th><th>Effect</th><th></th></tr></thead>' +
      '<tbody>' + rows + '</tbody></table>';
  }).join('');
  return '<div style="padding:16px;max-width:900px;margin:0 auto">' +
    '<h2 style="font-size:13px;text-transform:uppercase;letter-spacing:0.5px;color:var(--muted);margin-bottom:4px">Settings</h2>' +
    '<div class="o-faint" style="margin-bottom:8px">Every configuration knob osiris has, one governed door.</div>' +
    sections + '</div>';
}
function settingsFieldValue(item) {
  var id = 'setting-val-' + item.key;
  var el = $(id);
  if (item.type === 'bool') return el.checked;
  if (item.type === 'int') return parseInt(el.value, 10);
  if (item.type === 'float') return parseFloat(el.value);
  if (item.type === 'json' || item.type === 'records') return JSON.parse(el.value);
  if (item.type === 'path' || item.type === 'schedule') {
    return el.value.trim() === '' ? null : el.value.trim();
  }
  return el.value;
}
async function saveSetting(key) {
  var item = (SETTINGS_LIST || []).filter(function(it) { return it.key === key; })[0];
  if (!item) return;
  var errEl = $('setting-err-' + key); if (errEl) errEl.textContent = '';
  var value;
  try { value = settingsFieldValue(item); }
  catch(e) { if (errEl) errEl.textContent = 'invalid value: ' + e; return; }
  if (item.consequence === 'high' &&
      !confirm('This is a high-consequence change to ' + key + '. Proceed?')) return;
  var because = '';
  if (item.requires_because) {
    because = prompt('Why this change? (required)'); if (!because) return;
  }
  var res = await fetch('/settings', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ key: key, value: value, because: because }),
  }).then(function(r){ return r.json(); });
  if (res.error) {
    if (errEl) errEl.textContent = res.error;
    setStatus('Save failed: ' + res.error);
    return;
  }
  setStatus(key + ' saved' + (res.note ? ' — ' + res.note : '') + '.');
  renderSettingsPanel();
}
// THE SECRETS ROTATE ACT's own confirm-then-POST door (thread f4498ab304e4's follow-up,
// Thoth mail 10441) — deliberately NOT saveSetting's own shape: a secret_ref field has
// no visible <input> to read (settingsFieldInput never renders one for this type), so
// the new value comes from its own prompt() here, and the confirm is UNCONDITIONAL
// (never gated on item.consequence — a rotation always replaces the live credential,
// no low-stakes case exists the way an ordinary knob has one). The write door is the
// SAME /settings POST saveSetting already uses (settings_service.write_setting's own
// secret_ref branch IS the rotate — no second endpoint to learn); res.note (which
// daemon must restart) surfaces the identical way.
async function rotateSecret(key) {
  var item = (SETTINGS_LIST || []).filter(function(it) { return it.key === key; })[0];
  if (!item) return;
  var errEl = $('setting-err-' + key); if (errEl) errEl.textContent = '';
  if (!confirm('Rotate ' + key + '? This replaces the live credential; the old value ' +
      'cannot be recovered from this panel. Proceed?')) return;
  var value = prompt('New value for ' + key + ':'); if (!value) return;
  var because = '';
  if (item.requires_because) {
    because = prompt('Why this rotation? (required)'); if (!because) return;
  }
  var res = await fetch('/settings', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ key: key, value: value, because: because }),
  }).then(function(r){ return r.json(); });
  if (res.error) {
    if (errEl) errEl.textContent = res.error;
    setStatus('Rotate failed: ' + res.error);
    return;
  }
  setStatus(key + ' rotated' + (res.note ? ' — ' + res.note : '') + '.');
  renderSettingsPanel();
}

// ── Repairs Panel (thread c89a9873, wave 22, ruling 7be61879) ─────────────────────────
// The seven backfill repair targets, one dry-run/apply door each — all through POST
// /backfill, which itself calls orchestrator.backfill.run_backfill, the SAME function
// the MCP tool and the CLI's `osiris backfill` command call. operator_charter's own
// apply control is DELIBERATELY OMITTED here (thread c89a9873's own scope note: its
// blast radius — fleet-wide operator authority — needs a deliberate terminal act, not a
// browser click; /backfill itself refuses dry_run=False for this target regardless, so
// this is belt-and-suspenders, never the only guard). Reached via CMD-K ("Repairs…").
var REPAIRS_TARGETS = [
  { key: 'bootstrap_orphan_references', hint: 'Link an orphaned ref:osiris Reference to the SoftwareProject its own canonical names.' },
  { key: 'boot_alarm_commit_links', hint: 'Link a zero-link boot-alarm Thread to the Commit its summary cites.' },
  { key: 'task_sync_citation_links', hint: 'Link a zero-link task_sync Thread to the Thread it names.' },
  { key: 'lineage_repo_links', hint: 'Link a zero-link Decision/Thread to its author’s lineage project.' },
  { key: 'agent_project_links', hint: 'Move works_in/governs off an off-head Agent onto its living head.' },
  { key: 'closed_by_real_sources', hint: 'Re-point closed_by edges off placeholder Agents onto the real Person/SystemSource, then retire the placeholder.' },
  { key: 'operator_charter', hint: 'Mint governs from person:operator to every active SoftwareProject it doesn’t already cover — fleet-wide authority scope. Apply is CLI-only: osiris backfill operator_charter --apply --because "..."', cliOnly: true },
];
function renderRepairsPanel() {
  const container = $('result'); showPanel();
  $('entity-taxonomy-bar').style.display = 'none';
  var rows = REPAIRS_TARGETS.map(function(t) {
    var applyBtn = t.cliOnly
      ? '<span class="o-faint" title="' + esc(t.hint) + '">CLI-only</span>'
      : '<button class="iconbtn" onclick="applyRepair(\'' + esc(t.key) + '\')">Apply</button>';
    return '<tr><td style="vertical-align:top"><code>' + esc(t.key) + '</code></td>' +
      '<td class="o-faint" style="vertical-align:top">' + esc(t.hint) + '</td>' +
      '<td style="vertical-align:top"><button class="iconbtn" onclick="dryRunRepair(\'' + esc(t.key) + '\')">Dry run</button></td>' +
      '<td style="vertical-align:top">' + applyBtn + '</td></tr>' +
      '<tr><td colspan="4"><pre id="repair-out-' + esc(t.key) + '" class="o-faint" style="white-space:pre-wrap;margin:0 0 12px"></pre></td></tr>';
  }).join('');
  container.innerHTML = '<div style="padding:16px;max-width:900px;margin:0 auto">' +
    '<h2 style="font-size:13px;text-transform:uppercase;letter-spacing:0.5px;color:var(--muted);margin-bottom:4px">Repairs</h2>' +
    '<div class="o-faint" style="margin-bottom:8px">The seven backfill repair verbs. Dry run always writes nothing.</div>' +
    '<table class="ee-table"><thead><tr><th>Target</th><th>What it does</th><th></th><th></th></tr></thead><tbody>' +
    rows + '</tbody></table></div>';
}
async function dryRunRepair(target) {
  var out = $('repair-out-' + target);
  if (out) out.textContent = 'running…';
  var res = await fetch('/backfill', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ target: target, dry_run: true }),
  }).then(function(r){ return r.json(); });
  if (out) out.textContent = JSON.stringify(res, null, 2);
}
async function applyRepair(target) {
  if (!confirm('This is a high-consequence write (' + target + '). Proceed?')) return;
  var because = prompt('Why this change? (required)'); if (!because) return;
  var out = $('repair-out-' + target);
  if (out) out.textContent = 'applying…';
  var res = await fetch('/backfill', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ target: target, dry_run: false, because: because }),
  }).then(function(r){ return r.json(); });
  if (out) out.textContent = JSON.stringify(res, null, 2);
  if (res.error) { setStatus('Apply failed: ' + res.error); return; }
  setStatus(target + ' applied.');
}

// ── Projects ────────────────────────────────────────────────────────────────
// THE CONSOLE CHROME CLEANUP (thread 0be2f790's own operator-finding follow-up, Thoth DM
// 10731 piece 1): the left-nav "Projects" surface (renderProjects() + its own status
// toggle, formerly wired here) is retired — redundant with the header's own repo
// selector, per the operator's own word. The underlying "projects" saved composition is
// UNTOUCHED and stays reachable exactly as every other saved composition is: the omnibox
// (type "projects" in the header search) or the CLI/MCP composition-run door directly —
// this removes only the bespoke surface/status-toggle chrome around it, never the data
// access itself. loadProjects()/the PROJECTS array below stay — the repo pill still
// depends on them.

// ── Fleet ────────────────────────────────────────────────────────────────────


// ── Focus / Inspect ──────────────────────────────────────────────────────────
// WAVE A item 6 (thread 8839): the breadcrumb trail behind focus()'s own navigation —
// every node a click/search/dbltap brought into focus, in order, deduped only when it
// repeats the CURRENT tail (revisiting an older crumb truncates forward, browser-history
// style, rather than growing a trail that loops on itself).
let BREADCRUMBS = [];
function pushBreadcrumb(id, label) {
  if (BREADCRUMBS.length && BREADCRUMBS[BREADCRUMBS.length - 1].id === id) return;
  BREADCRUMBS.push({ id, label: label || id.slice(0, 8) });
  if (BREADCRUMBS.length > 12) BREADCRUMBS = BREADCRUMBS.slice(-12);
  renderBreadcrumbs();
}
function renderBreadcrumbs() {
  var el = $('graph-breadcrumbs'); if (!el) return;
  el.innerHTML = BREADCRUMBS.map(function(c, i) {
    var cur = i === BREADCRUMBS.length - 1;
    return (i ? '<span class="crumb-sep">/</span>' : '') +
      '<span class="crumb' + (cur ? ' current' : '') + '" title="' + esc(c.label) + '" onclick="jumpToBreadcrumb(' + i + ')">' + esc(c.label) + '</span>';
  }).join('');
}
function jumpToBreadcrumb(i) {
  if (i < 0 || i >= BREADCRUMBS.length) return;
  var target = BREADCRUMBS[i];
  BREADCRUMBS = BREADCRUMBS.slice(0, i + 1);
  focus(target.id, true);
}
// NAVIGABLE SPACE, INTEGRATION (mail 10550): focus() used to fetch a fresh one-hop
// neighborhood and merge it into the cytoscape board — now the whole graph is already
// loaded client-side in space.js, so "focus a node" is exactly space's own focusObject:
// walk upstream, hide the rest, zoom-to-fit, open the inspector. No REST round-trip, no
// board to clear. Falls back to the plain inspector fetch if space hasn't finished
// mounting yet (a cold click right at page load).
async function focus(id, fromBreadcrumb) {
  FOCUS = id; postConsole({ focused_object_id: id });
  if (ACTIVE_SURFACE !== 'browse') await switchSurface('browse');
  const space = window.OsirisSpace || (window.__spaceReady && await window.__spaceReady);
  if (space) await space.focusObject(id); else await inspect(id);
  if (!fromBreadcrumb) pushBreadcrumb(id, id.slice(0, 8));
}
// TIP 1 AMENDMENT (operator via Thoth mail 10726, ruling amending e1cb9e3b): select-vs-focus
// is retired -- "select, inspector, hide, fit, one gesture." The old select-only helper and
// space's own selectObject primitive are both gone; every click-through (omnibox, table
// row, canvas) now calls focus()/focusObject directly. The graph's own in-canvas
// "Find a node" box stays gone (TIP 1(e)) -- the header omnibox is the only search, and a
// hit always focuses regardless of click vs Enter.
async function inspect(id) {
  FOCUS = id;
  var obj = await fetch('/objects/' + id).then(function(r){return r.json();}).catch(function(){return null;});
  if (!obj) return;
  var right = $('right');
  right.className = 'rail';
  right.innerHTML = Osiris.objectDetail(obj, '');
  var relsEl = right.querySelector('[data-rels]');
  if (relsEl) await Osiris.loadRels(relsEl, id, inspectOnly, openAsSet, obj);
  bindUpstreamExpansions(right);
}
// PROVENANCE PIECE 3(b) (thread b4477e9e): "who else read this upstream" — a property
// row's own upstream_ids[0] drives one call to the upstream_readers Function (via the
// existing generic composition door, not a bespoke route) so the reader sees every OTHER
// object whose writer also plausibly traces to the same upstream read, without leaving
// the inspector. Toggles closed on a second click rather than re-fetching.
function bindUpstreamExpansions(scope) {
  scope.querySelectorAll('.o-upstream-link').forEach(function(link) {
    link.onclick = async function() {
      // propRow emits k/val/pv/signals/expansion as flat grid children in order — the
      // expansion div this link controls is always its own parent's next sibling.
      var signalsDiv = link.parentElement;
      var exp = signalsDiv && signalsDiv.nextElementSibling;
      if (!exp || !exp.classList.contains('o-upstream-expansion')) return;
      if (exp.style.display !== 'none') { exp.style.display = 'none'; return; }
      exp.style.display = 'block';
      exp.innerHTML = '<div class="o-faint">loading…</div>';
      try {
        var res = await fetch('/compositions/upstream-readers/run', {
          method: 'POST', headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ subject: link.dataset.upstream }),
        }).then(function(r) { return r.json(); });
        var items = (res && res.items) || [];
        if (!items.length) { exp.innerHTML = '<div class="o-faint">No other reader found.</div>'; return; }
        exp.innerHTML = '<div class="o-faint" style="margin:4px 0">Also traces to this upstream:</div>' +
          items.map(function(g) {
            var facts = (g.facts || []).map(function(f) {
              return esc(f.name) + '=' + esc(String(f.value)) + ' (' + esc(f.source_id) + ')';
            }).join(', ');
            return '<div class="o-rel" style="padding-left:8px"><a data-pick="' + esc(g.id) +
              '" style="cursor:pointer">' + esc(g.name) + '</a> — <span class="o-faint">' +
              facts + '</span></div>';
          }).join('');
        exp.querySelectorAll('[data-pick]').forEach(function(a) {
          a.onclick = function() { inspectOnly(a.dataset.pick); };
        });
      } catch (e) {
        exp.innerHTML = '<div class="o-faint">Could not load.</div>';
      }
    };
  });
}
function inspectOnly(id) {
  FOCUS = id;
  var badge = document.getElementById("focused-badge");
  if (badge && id) {
    badge.style.display = "inline";
    badge.textContent = "Inspect: " + id.slice(0, 8) + "";
  }
  // a table-drawer row click "shares the selection" with the space canvas (mail 10550) —
  // focusObject also opens the inspector, so this replaces the plain inspect(id) call
  // whenever the canvas is actually mounted and visible (browse). TIP 1 AMENDMENT (mail
  // 10726): select-vs-focus is retired -- a row click is the same one gesture a canvas
  // click is now, always a real focus.
  var space = ACTIVE_SURFACE === 'browse' ? window.OsirisSpace : null;
  if (space) space.focusObject(id);
  else inspect(id);
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
async function tagIt(id) { const t = prompt('Tag:'); if (!t) return; await fetch('/objects/' + id + '/tags', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ tag: t }) }); }

// ── Intake ───────────────────────────────────────────────────────────────────
async function addSeed() { const raw = $('seed')?.value.trim(); if (!raw) return; try { const cases = await fetch('/cases').then(r => r.json()); const cid = cases.length ? cases[0].id : null; if (!cid) return; const r = await fetch('/cases/' + cid + '/intake', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ raw }) }).then(r => r.json()); if (r) { $('seedmsg').textContent = 'Added: ' + r.type; $('seed').value = ''; } } catch(e) {} }

// ── Console Sync ─────────────────────────────────────────────────────────────
function postConsole(fields) { fetch('/console', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(fields) }).then(r => r.json()).then(j => { CONSOLE_REV = j.rev; }).catch(() => {}); }
function setSyncBadge(by) { $('syncbadge').textContent = by === 'claude' ? '\u25cf agent' : ''; }
// ROOM RETIREMENT (thread 96f09d48): the room_id leg of cross-client sync is gone
// (switchRoom no longer exists) — every OTHER field (focused_object_id, and surface via
// the caller's own postConsole({surface}) elsewhere) keeps syncing unchanged.
function watchConsole() { const es = new EventSource('/console/stream'); es.onmessage = async ev => { const s = JSON.parse(ev.data); if (s.rev == null || s.rev <= CONSOLE_REV) return; CONSOLE_REV = s.rev; if (s.updated_by !== 'human') { SYNCING = true; try { setSyncBadge(s.updated_by); if (s.focused_object_id && s.focused_object_id !== FOCUS) inspectOnly(s.focused_object_id); } finally { SYNCING = false; } } }; }

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
// was the cytoscape board's own resizeFit — retired with the board itself (piece 3); the
// space canvas already resizes itself via its own window "resize" listener.
function _afterResize() {}
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
  { label: 'Author composition…', hint: 'Save a new lens', cat: 'Compositions', run: () => authorComposition() },
  { label: 'Settings…', hint: 'Every configuration knob, including backup', cat: 'Admin', run: () => renderSettingsPanel() },
  { label: 'Repairs…', hint: 'The seven backfill repair verbs', cat: 'Admin', run: () => renderRepairsPanel() },
];

// THE COMPOSER SHELL (Thoth dispatch 9257 piece 2, thread 588148bb): "run" used to mean
// "POST /compositions/{name}/run and dump the raw JSON in a <pre>" \u2014 every saved composition
// paid for osiris.js's generic renderer (P4, commit 9c5e923) without ever reaching it. Now it
// runs through Osiris.renderResult exactly like piece 1's mailbox did, so a table composition
// gets a real table, an objects composition gets the board, row_action buttons work, and
// "run:<function>" drill-ins dispatch through the same scoped listener pattern.
let LAST_COMPOSITION_RUN = null; // {name, spec} of whatever's on screen \u2014 Fork's own source
async function runTool(name) { await runComposition(name, {}, FOCUS); }

async function runComposition(name, args, subject) {
  const container = $('result'); showPanel();
  $('entity-taxonomy-bar').style.display = 'none';
  try {
    setStatus('Running ' + name + '...');
    const isFunctionDrill = args && Object.keys(args).length > 0;
    const res = isFunctionDrill
      ? await fetch('/compositions/run-spec', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ spec: { op: 'function', name: name, args: args }, name: name }) }).then(r => r.json())
      : await fetch('/compositions/' + encodeURIComponent(name) + '/run', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ subject: subject || null }) }).then(r => r.json());
    if (res.error) { setStatus(res.error); container.innerHTML = '<div class="o-empty" style="padding:40px">' + esc(res.error) + '</div>'; return; }
    LAST_COMPOSITION_RUN = isFunctionDrill ? null : { name: res.composition || name, spec: res.spec };
    const panel = document.createElement('div'); panel.style.padding = '16px';
    await Osiris.renderResult(res, { panel: panel }, Osiris.defaultView(res), null, null, null);
    container.innerHTML = '<div style="padding:8px 16px;display:flex;gap:8px">' +
      '<button class="iconbtn" onclick="switchSurface(\'browse\')">\u2190 Back to Browse</button>' +
      (LAST_COMPOSITION_RUN ? '<button class="iconbtn" onclick="forkComposition()">\u2942 Fork (save as)\u2026</button>' : '') +
      '</div>';
    container.appendChild(panel);
    setStatus('Ran ' + name + ' \u2014 ' + res.count + (res.count === 1 ? ' item' : ' items') + '.');
  } catch(e) { console.error('runComposition failed', name, args, e); setStatus('Could not run: ' + name); container.innerHTML = '<div class="o-empty" style="padding:40px">Could not run: ' + esc(name) + '</div>'; }
}

// osiris.js's click delegate dispatches this for any row's "run:<function>" action (built for
// exactly this navigation, task #90/#91 \u2014 see osiris.js's own comment on the click delegate).
// The mailbox surface has its own narrower-scoped listener (piece 1); this one is the general
// composer-shell catch-all for every OTHER surface, so a "run:" button on any saved
// composition's row (not just mail's) has somewhere to land.
document.addEventListener('osiris:run', function(e) {
  if (ACTIVE_SURFACE === 'mailbox') return; // owned by renderMailbox's own listener
  // e.detail.subject (Thoth dispatch 9676/9690, 588148bb piece 4) — a `bind_subject`
  // row_action's own target: THIS row's object, not whatever the shell was last focused
  // on. Falls back to FOCUS for every other "run:" button (args-drill or no subject at
  // all), unchanged from piece 2's own behavior.
  runComposition(e.detail.name, e.detail.args || {}, e.detail.subject || FOCUS);
});

// AUTHOR (the channel Claude composes over MCP already has; this is the human's own door,
// same "friendly form OR raw spec" split P5's original design called for \u2014 kept to the raw
// spec half, since a friendly builder is its own real UI and not what this piece needs to
// prove: that a human can put a saved composition on the graph at all, room-scoped, from the
// shell, without touching MCP).
async function authorComposition() {
  const name = prompt('Name this composition:'); if (!name) return;
  const specText = prompt('Op-tree spec (JSON) \u2014 e.g. {"op":"function","name":"mail_overview"}:');
  if (!specText) return;
  let spec; try { spec = JSON.parse(specText); } catch(e) { setStatus('Invalid JSON spec.'); return; }
  try {
    const preview = await fetch('/compositions/run-spec', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ spec: spec, subject: FOCUS, name: name }) }).then(r => r.json());
    if (preview.error) { setStatus('Spec failed: ' + preview.error); return; }
  } catch(e) { setStatus('Could not preview spec.'); return; }
  const saved = await fetch('/compositions', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ name: name, spec: spec, room_id: ROOM || null }) }).then(r => r.json());
  await loadCompositions();
  setStatus('Saved composition: ' + (saved.name || name));
  await runComposition(name, {}, FOCUS);
}

// FORK \u2014 save the composition currently on screen under a new name (a real fork: the spec
// copies, the two compositions diverge independently from here on, same as `git branch`).
async function forkComposition() {
  if (!LAST_COMPOSITION_RUN) return;
  const name = prompt('Fork "' + LAST_COMPOSITION_RUN.name + '" as:'); if (!name) return;
  const saved = await fetch('/compositions', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ name: name, spec: LAST_COMPOSITION_RUN.spec, room_id: ROOM || null }) }).then(r => r.json());
  await loadCompositions();
  setStatus('Forked as: ' + (saved.name || name));
  await runComposition(name, {}, FOCUS);
}

// PICK \u2014 every saved composition, room-scoped exactly like the object set already is
// (switchRoom's own filter), loaded once per room switch rather than per keystroke: unlike
// /search (piece #196's addition to the palette, debounced because the graph is too large to
// hold client-side) a room's saved compositions are few and already local, same shape POWER_
// TOOLS has always been.
let SAVED_COMPOSITIONS = [];
async function loadCompositions() {
  try {
    const url = '/compositions' + (ROOM ? ('?room=' + encodeURIComponent(ROOM)) : '');
    SAVED_COMPOSITIONS = await fetch(url).then(r => r.json());
  } catch(e) { SAVED_COMPOSITIONS = []; }
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
  // saved compositions — the room-scoped PICK half of "pick a room, run/author/fork
  // compositions" (Thoth dispatch 9257 piece 2). Already loaded client-side (loadCompositions,
  // called on boot and on every room switch), so this filters synchronously same as POWER_
  // TOOLS rather than round-tripping per keystroke the way /search's graph hits do below.
  const compHits = SAVED_COMPOSITIONS.filter(c => c.name.toLowerCase().includes(ql)).slice(0, 8)
    .map(c => ({ label: c.name, hint: c.description || c.kind, cat: 'Compositions', run: () => runTool(c.name) }));
  OMNI_ITEMS = toolHits.concat(compHits);
  OMNI_SEL = Math.min(OMNI_SEL, OMNI_ITEMS.length - 1);
  renderOmniList(q);
  // Search is an ADDITION to enumeration, never a replacement (ruling 7a1a5517) — this
  // palette used to match ONLY the hardcoded POWER_TOOLS list, never the graph itself, so
  // "find a known thing by name" had no path here at all (#196, Thoth msg 5600). Debounced
  // (200ms) and token-guarded so a fast typist's stale response never clobbers a newer one.
  const myToken = ++OMNI_SEARCH_TOKEN;
  clearTimeout(OMNI_SEARCH_TIMER);
  OMNI_SEARCH_TIMER = setTimeout(async () => {
    // TIP 3b review (Thoth mail 10953): the fallback used to be gated on `hits.length ===
    // 0`, computed only AFTER the /search fetch's own await -- a second, sequential await
    // (window.__spaceReady) then followed, with its own token re-check. Two sequential
    // awaits, two chances for a race, and a gate that skipped the client scan entirely on
    // any response shape that didn't trip that one condition -- the live page kept saying
    // "No matches" regardless, the exact contributing bug never pinned down with certainty.
    // Rather than patch one more edge case onto a fragile gate, the gate is gone: both
    // requests run together (one await, one token check), and the client-side agent scan is
    // UNCONDITIONAL once the graph is loaded, deduped against whatever the server found
    // rather than only stepping in when the server came back empty.
    const [searchResult, space] = await Promise.all([
      fetch('/search?q=' + encodeURIComponent(q) + '&limit=8').then(r => r.json()).catch(() => null),
      window.OsirisSpace ? Promise.resolve(window.OsirisSpace) : (window.__spaceReady || Promise.resolve(null)),
    ]);
    if (myToken !== OMNI_SEARCH_TOKEN) return; // a newer keystroke already superseded this
    const hits = searchResult
      ? (Array.isArray(searchResult.hits) ? searchResult.hits : (Array.isArray(searchResult) ? searchResult : []))
      : [];
    // THE LEGIBILITY PASS, TIP 1(e) (ruling e1cb9e3b): ONE search -- the graph's own
    // in-canvas "Find a node" box is gone, this omnibox drives it directly. TIP 1 AMENDMENT
    // (mail 10726): "one gesture" -- a Graph hit always focuses now, click or Enter, matching
    // the canvas's own single-click-is-focus (select-vs-focus is retired outright).
    const seenIds = new Set(hits.filter(h => h && h.id).map(h => h.id));
    const graphHits = hits.filter(h => h && h.id).map(h => ({
      label: h.display_label || h.label || h.name || h.canonical || h.id,
      hint: h.type || '', cat: 'Graph',
      run: () => { switchSurface('browse'); focus(h.id); },
    }));
    let agentHits = [];
    if (space && space.idToNode) {
      // TIP 3 review carry-over (Thoth mail 10930): read the label the same fallback-safe
      // way space.js's own pickLabels/labelTextFor do (nd.label, else
      // `${type} ${id.slice(0,8)}`), so the scan always has real text to search; skip any
      // id the server already returned so the same agent never appears twice.
      agentHits = space.idToNode
        .filter(n => n.type === 'Agent' && !seenIds.has(n.id))
        .map(n => ({ n, text: n.label || `${n.type} ${n.id.slice(0, 8)}` }))
        .filter(({ text }) => text.toLowerCase().includes(ql))
        .slice(0, 8)
        .map(({ n, text }) => ({
          label: text, hint: 'Agent', cat: 'Graph',
          run: () => { switchSurface('browse'); focus(n.id); },
        }));
    }
    OMNI_ITEMS = toolHits.concat(compHits, graphHits, agentHits).slice(0, 16);
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
document.addEventListener('keydown', e => { const inField = /^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement?.tagName); if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') { e.preventDefault(); openPalette(); } else if (e.key === '/' && !inField) { e.preventDefault(); openPalette(); } else if (e.key === 'Escape') {
  // review flaw #2 (TIP 1c, Thoth mail 10891): "Escape must clear the focus" (the
  // amendment's own ruling, mail 10726/b96fc93e: "Escape clears, Back walks the stack") --
  // this handler used to step back one BREADCRUMB instead (WAVE A item 6, predating the
  // path-lens focus feature entirely), which never actually cleared anything. Escape now
  // clears space's own focus outright when nothing more local already consumed it (a
  // dropdown, the peek overlay, or the search box's own Escape handler above).
  const hadDropdown = !!document.querySelector('.dd-item') && ['workspace-dropdown','repo-dropdown','omni-dropdown'].some(id => { const el = $(id); return el && el.style.display && el.style.display !== 'none'; });
  closeAllDropdowns();
  const hadPeek = $('peek').className.includes('on');
  if (hadPeek) closePeek();
  if (!hadDropdown && !hadPeek && ACTIVE_SURFACE === 'browse' && window.OsirisSpace) window.OsirisSpace.clearFocus();
} else if (e.key === '[' && !inField) { e.preventDefault(); toggleLeft(); } else if (e.key === ']' && !inField) { e.preventDefault(); toggleRight(); } });
function closePeek() { const o = $('peek'); o.className = 'peek-overlay'; o.innerHTML = ''; }

// ── Boot ─────────────────────────────────────────────────────────────────────
// ROOM RETIREMENT (thread 96f09d48): boot no longer fetches /console for a room_id to
// restore (switchRoom, its own loadCompositions() call included, is gone) —
// loadCompositions() runs directly here instead, unscoped (ROOM is always '' now).
Osiris.loadSchema().then(async function() {
  await Promise.all([loadProjects(), loadObjectSet(), loadCompositions()]);
  switchSurface('browse');
  loadPanes(); wireGrips(); watchConsole();
  updatePulse(); setInterval(updatePulse, 8000);
});
