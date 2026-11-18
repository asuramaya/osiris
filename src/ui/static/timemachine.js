// ── The Time Machine screen ────────────────────────────────────────────────────────────
// Settings, Backups, "Browse a past moment": open a past moment of the database in a throwaway
// copy, look through it (read-only), and bring ONE item back into the live graph. It talks to
// the /time-machine/* routes and keeps nothing but what is on screen. Nothing runs by itself:
// the first thing it does on load is read whether a moment is already open (a file read); the
// list of moments is read only when its button is pressed; the restore is written only by the
// Apply button, after a plan the operator has seen.
//
// Loaded after athome.js (which provides athomeCall) and console.js ($, setStatus). The pure
// helpers below also run under node (see tests/test_time_machine_ui.py).

var TM = { moments: null, status: null, timer: null, view: 'items', type: '', q: '', offset: 0,
           types: null, ref: null, plan: null, shown: false, busy: false };
var TM_LIMIT = 50;
var TM_WRAP = 'white-space:normal;overflow-wrap:anywhere;';

function tmText(s) {
  return String(s == null ? '' : s).replace(/[<>&]/g, function (c) {
    return { '<': '&lt;', '>': '&gt;', '&': '&amp;' }[c];
  });
}
function tmAttr(s) {
  return String(s == null ? '' : s).replace(/[<>&"']/g, function (c) {
    return { '<': '&lt;', '>': '&gt;', '&': '&amp;', '"': '&quot;', "'": '&#39;' }[c];
  });
}
function tmWhen(iso) {
  if (!iso) return 'unknown';
  var d = new Date(iso);
  if (isNaN(d.getTime())) return String(iso);
  return d.toLocaleString(undefined, { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
}
function tmSize(bytes) {
  if (bytes == null) return '';
  var gb = bytes / 1e9;
  return gb >= 1 ? gb.toFixed(1) + ' GB' : Math.max(1, Math.round(bytes / 1e6)) + ' MB';
}
function tmLeft(seconds) {
  if (seconds == null) return '';
  var m = Math.max(0, Math.round(seconds / 60));
  return m >= 60 ? Math.floor(m / 60) + ' h ' + (m % 60) + ' min left' : m + ' min left';
}
// A value from the past moment can be any JSON; show it as text.
function tmValue(v) {
  if (v == null) return 'nothing';
  return typeof v === 'string' ? v : JSON.stringify(v);
}
// "2026-11-17T12:00" from a date-time box (the browser's own time zone) to "...Z" in UTC.
function tmUtc(localValue) {
  if (!localValue) return '';
  var d = new Date(localValue);
  if (isNaN(d.getTime())) return '';
  return d.toISOString().replace(/\.\d+Z$/, 'Z');
}
// The same, the other way: a UTC time for a date-time box, which reads local time.
function tmLocalBox(iso) {
  var d = new Date(iso);
  if (isNaN(d.getTime())) return '';
  var p = function (n) { return String(n).padStart(2, '0'); };
  return d.getFullYear() + '-' + p(d.getMonth() + 1) + '-' + p(d.getDate()) + 'T' + p(d.getHours()) + ':' + p(d.getMinutes());
}
var TM_KIND_WORDS = { dump: 'Dump', restic: 'Dump on the NAS', pitr: 'Point in time' };
var TM_EFFECT_WORDS = {
  new_property: 'a new property',
  alongside: 'added next to the live value',
  becomes_displayed: 'would become the value shown',
};
var TM_CONFLICT_WORDS = {
  same_source_changed: 'The live item holds a different value from the same source.',
  tie: 'Another source holds a different value with the same confidence and time.',
};

// ── the list of moments ────────────────────────────────────────────────────────────────
function tmMomentHtml(m, idx) {
  var kind = TM_KIND_WORDS[m.kind] || m.kind;
  var ok = m.available !== false;
  var picker = '';
  if (m.pick_time) {
    picker = '<div style="margin:4px 0"><label class="o-faint">Time to open (your local time), between ' +
      tmText(tmWhen(m.window_from)) + ' and ' + tmText(tmWhen(m.window_to)) + ': ' +
      '<input type="datetime-local" id="tm-at-' + idx + '" value="' + tmAttr(tmLocalBox(m.window_to)) +
      '" min="' + tmAttr(tmLocalBox(m.window_from)) + '" max="' + tmAttr(tmLocalBox(m.window_to)) +
      '" style="max-width:100%"' + (ok ? '' : ' disabled') + ' /></label></div>';
  }
  return '<div style="padding:8px 0;border-top:1px solid var(--border);' + TM_WRAP + '">' +
    '<div>' + tmText(m.label || m.id) + ' <span class="o-faint">' + tmText(kind) + (m.size_bytes != null ? ', ' + tmText(tmSize(m.size_bytes)) : '') + '</span></div>' +
    '<div class="o-faint">' + tmText(tmWhen(m.when)) + '</div>' + picker +
    (ok ? '' : '<div class="o-faint">Not available: ' + tmText(m.reason || 'it cannot be opened right now.') + '</div>') +
    '<div style="margin-top:4px"><button class="iconbtn" ' + (ok ? '' : 'disabled style="opacity:0.45" ') +
    'onclick="tmStart(' + idx + ')">Open this moment</button></div></div>';
}
function tmMomentsHtml(list) {
  if (!list || !list.length) return '<div class="o-empty">No past moments are available yet.</div>';
  return list.map(tmMomentHtml).join('');
}
async function tmShowList() {
  TM.shown = true;
  var root = $('tm-root');
  if (root) root.innerHTML = '<div class="o-faint">Loading the list of moments…</div>';
  var r = await athomeCall('/time-machine/moments');
  if (r.status >= 400 || (r.body && r.body.error)) {
    if (root) root.innerHTML = '<div class="o-empty">' + tmText(athomeErrorText(r.body, 'The list could not be read.')) + '</div>';
    return;
  }
  TM.moments = (r.body && r.body.moments) || [];
  tmRender();
}
async function tmStart(idx) {
  var m = (TM.moments || [])[idx];
  if (!m || TM.busy) return;
  var payload = { moment: m.id };
  if (m.pick_time) {
    var at = tmUtc(($('tm-at-' + idx) || {}).value);
    if (!at) { setStatus('Pick a time inside the window first.'); return; }
    payload.at = at;
  }
  TM.busy = true;
  var r = await athomeCall('/time-machine/start', 'POST', payload);
  TM.busy = false;
  if (r.status >= 400 || (r.body && r.body.error)) {
    var note = $('tm-msg');
    if (note) note.textContent = athomeErrorText(r.body, 'It could not be started.');
    return;
  }
  tmRefreshStatus();
}

// ── the open moment: status, polling, time left ────────────────────────────────────────
async function tmRefreshStatus() {
  var r = await athomeCall('/time-machine/status');
  if (r.status === 404) { TM.status = null; var root = $('tm-root'); if (root) root.innerHTML = '<div class="o-empty">Not available yet on this machine.</div>'; return; }
  TM.status = (r.body && r.body.throwaway) || null;
  tmRender();
  tmSchedulePoll();
}
function tmSchedulePoll() {
  clearTimeout(TM.timer);
  if (!$('tm-root')) return;  // the screen is gone: stop asking
  var st = TM.status && TM.status.state;
  if (st === 'starting' || st === 'restoring') TM.timer = setTimeout(tmRefreshStatus, 4000);
  else if (st === 'ready') TM.timer = setTimeout(tmRefreshStatus, 30000);
}
function tmStatusHtml(t) {
  var head = '<div style="' + TM_WRAP + '"><strong>' + tmText((t.moment && t.moment.label) || 'A past moment') + '</strong></div>';
  if (t.state === 'failed') {
    return head + '<div style="margin:4px 0;' + TM_WRAP + '">' + tmText(t.error || t.state_text || 'It could not be opened.') + '</div>' +
      '<div><button class="iconbtn" onclick="tmTeardown()">Clear it</button></div>';
  }
  if (t.state !== 'ready') {
    return head + '<div style="margin:4px 0;' + TM_WRAP + '">' + tmText(t.state_text || (t.state === 'starting' ? 'Starting…' : 'Restoring…')) + '</div>' +
      '<div class="o-faint">This takes a few minutes. You can leave this page; it keeps going.</div>' +
      '<div style="margin-top:6px"><button class="iconbtn" onclick="tmTeardown()">Cancel</button></div>';
  }
  return head + '<div class="o-faint">Open, read-only. ' + tmText(tmLeft(t.seconds_left)) + '.</div>' +
    '<div style="margin:6px 0">' +
    (t.extensions ? '' : '<button class="iconbtn" onclick="tmExtend()">Add two hours</button> ') +
    '<button class="iconbtn" onclick="tmTeardown()">Close this moment</button></div>';
}
async function tmExtend() {
  var r = await athomeCall('/time-machine/extend', 'POST', {});
  if (r.body && r.body.error) { var n = $('tm-msg'); if (n) n.textContent = r.body.error; return; }
  tmRefreshStatus();
}
async function tmTeardown() {
  var r = await athomeCall('/time-machine/teardown', 'POST', {});
  TM.status = null; TM.view = 'items'; TM.types = null; TM.plan = null; TM.ref = null;
  if (r.body && r.body.error) { var n = $('tm-msg'); if (n) n.textContent = r.body.error; }
  tmRender();
  clearTimeout(TM.timer);
}

// ── browsing the open moment ───────────────────────────────────────────────────────────
function tmRowHtml(r) {
  return '<div style="padding:6px 0;border-top:1px solid var(--border);' + TM_WRAP + '">' +
    '<div><code>' + tmText(r.type) + '</code> ' + tmText(r.canonical || r.title || r.short_id) + '</div>' +
    (r.title && r.title !== r.canonical ? '<div class="o-faint">' + tmText(r.title) + '</div>' : '') +
    '<div class="o-faint">' + tmText(r.short_id) + (r.status ? ', ' + tmText(r.status) : '') +
    (r.created_at ? ', made ' + tmText(tmWhen(r.created_at)) : '') + (r.matched ? ', matched in ' + tmText(r.matched) : '') + '</div>' +
    '<div style="margin-top:2px"><button class="iconbtn" data-ref="' + tmAttr(r.id) + '" onclick="tmOpenItem(this)">Look at it</button></div></div>';
}
function tmBrowseHtml(rows, total, types) {
  var chips = (types || []).map(function (t) {
    return '<button class="iconbtn' + (TM.type === t.type ? ' sel' : '') + '" data-type="' + tmAttr(t.type) +
      '" onclick="tmPickType(this)">' + tmText(t.type) + ' ' + tmText(t.count) + '</button> ';
  }).join('');
  var more = rows && rows.length && TM.offset + rows.length < total
    ? '<div style="margin-top:8px"><button class="iconbtn" onclick="tmMoreRows()">Show more</button></div>' : '';
  return '<div style="margin:8px 0"><button class="iconbtn' + (TM.type === '' ? ' sel' : '') + '" data-type="" onclick="tmPickType(this)">All</button> ' + chips + '</div>' +
    '<div style="margin:8px 0"><input id="tm-q" class="filter" placeholder="Search this moment (exact words)" style="width:100%;margin:0 0 4px" value="' + tmAttr(TM.q) +
    '" onkeydown="if(event.key===\'Enter\')tmSearch()" /> <button class="iconbtn" onclick="tmSearch()">Search</button> ' +
    '<button class="iconbtn" onclick="tmShowSessions()">Stored conversations</button></div>' +
    '<div class="o-faint">' + tmText(total == null ? '' : total + ' item(s)') + '</div>' +
    (rows || []).map(tmRowHtml).join('') + more;
}
async function tmLoadItems(append) {
  var box = $('tm-browse');
  if (!box) return;
  if (!TM.types) {
    var tr = await athomeCall('/time-machine/browse/types');
    TM.types = (tr.body && tr.body.types) || [];
  }
  var path = '/time-machine/browse/objects?limit=' + TM_LIMIT + '&offset=' + TM.offset +
    (TM.type ? '&type=' + encodeURIComponent(TM.type) : '') + (TM.q ? '&q=' + encodeURIComponent(TM.q) : '');
  var r = await athomeCall(path);
  if (r.status >= 400 || (r.body && r.body.error)) {
    box.innerHTML = '<div class="o-empty">' + tmText(athomeErrorText(r.body, 'The items could not be read.')) + '</div>';
    return;
  }
  var rows = (r.body.rows || []);
  TM.rows = append ? (TM.rows || []).concat(rows) : rows;
  box.innerHTML = tmBrowseHtml(TM.rows, r.body.total, TM.types);
  var q = $('tm-q'); if (q) q.value = TM.q;
}
function tmPickType(btn) { TM.type = btn.getAttribute('data-type') || ''; TM.offset = 0; tmLoadItems(false); }
function tmSearch() { var q = $('tm-q'); TM.q = q ? q.value.trim() : ''; TM.offset = 0; tmLoadItems(false); }
function tmMoreRows() { TM.offset = (TM.rows || []).length; tmLoadItems(true); }
function tmBack() { TM.view = 'items'; TM.plan = null; tmRender(); }

function tmDetailHtml(d) {
  if (d.ambiguous) {
    return '<div class="o-faint">That matches more than one item:</div>' + d.ambiguous.map(function (o) {
      return '<div style="' + TM_WRAP + '"><code>' + tmText(o.type) + '</code> ' + tmText(o.canonical) + '</div>';
    }).join('');
  }
  var o = d.object || {};
  var assertions = (d.assertions || []).map(function (a) {
    return '<div style="padding:4px 0;border-top:1px solid var(--border);' + TM_WRAP + (a.current ? '' : 'opacity:0.55;') + '">' +
      '<div><strong>' + tmText(a.name) + '</strong>' + (a.current ? '' : ' <span class="o-faint">(replaced later)</span>') + '</div>' +
      '<div style="white-space:pre-wrap;overflow-wrap:anywhere">' + tmText(tmValue(a.value)) + '</div>' +
      '<div class="o-faint">' + tmText(a.source_id || '') + ', confidence ' + tmText(a.confidence) + ', ' + tmText(tmWhen(a.observed_at)) + '</div></div>';
  }).join('');
  var link = function (l, dir) {
    return '<div style="' + TM_WRAP + '" class="o-faint">' + tmText(l.type) + ' ' + dir + ' <code>' + tmText((l.other || {}).type) + '</code> ' +
      tmText((l.other || {}).canonical) + (l.active === false ? ' (no longer active)' : '') + '</div>';
  };
  return '<div style="' + TM_WRAP + '"><code>' + tmText(o.type) + '</code> <strong>' + tmText(o.canonical) + '</strong></div>' +
    '<div class="o-faint">' + tmText(o.status || '') + (o.merged_into ? ', merged into another item since' : '') + ', made ' + tmText(tmWhen(o.created_at)) + '</div>' +
    '<div style="margin:8px 0"><button class="iconbtn" onclick="tmBack()">Back to the list</button> ' +
    '<button class="iconbtn" onclick="tmPlan()">Restore this item…</button></div>' +
    '<h4 class="o-faint" style="margin:10px 0 2px">Properties then</h4>' + (assertions || '<div class="o-empty">None.</div>') +
    '<h4 class="o-faint" style="margin:10px 0 2px">Links</h4>' +
    ((d.links_out || []).map(function (l) { return link(l, 'to'); }).join('') +
     (d.links_in || []).map(function (l) { return link(l, 'from'); }).join('') || '<div class="o-empty">None.</div>');
}
async function tmOpenItem(btn) {
  TM.ref = btn.getAttribute('data-ref');
  TM.view = 'detail'; TM.plan = null;
  tmRender();
  var box = $('tm-browse');
  var r = await athomeCall('/time-machine/browse/object/' + encodeURIComponent(TM.ref));
  if (!box) return;
  if (r.status >= 400 || !r.body || r.body.error) {
    box.innerHTML = '<div class="o-empty">' + tmText(athomeErrorText(r.body, 'That item could not be read.')) + '</div>' +
      '<button class="iconbtn" onclick="tmBack()">Back to the list</button>';
    return;
  }
  box.innerHTML = tmDetailHtml(r.body);
}

// ── stored conversations (read only, decrypted for the operator while looked at) ───────
async function tmShowSessions() {
  TM.view = 'sessions'; tmRender();
  var box = $('tm-browse');
  var r = await athomeCall('/time-machine/browse/sessions?limit=50');
  if (!box) return;
  if (r.status >= 400 || (r.body && r.body.error)) {
    box.innerHTML = '<div class="o-empty">' + tmText(athomeErrorText(r.body, 'The conversations could not be read.')) + '</div><button class="iconbtn" onclick="tmBack()">Back</button>';
    return;
  }
  box.innerHTML = '<div><button class="iconbtn" onclick="tmBack()">Back to the list</button></div>' +
    ((r.body.rows || []).map(function (s) {
      return '<div style="padding:6px 0;border-top:1px solid var(--border);' + TM_WRAP + '"><div>' + tmText(s.harness) + ' ' + tmText(s.anchor_sid) + '</div>' +
        '<div class="o-faint">' + tmText(s.lines) + ' lines, last read in ' + tmText(tmWhen(s.last_ingested_at)) + '</div>' +
        '<button class="iconbtn" data-sid="' + tmAttr(s.anchor_sid) + '" onclick="tmShowLines(this)">Read the last lines</button></div>';
    }).join('') || '<div class="o-empty">No stored conversations in this moment.</div>');
}
async function tmShowLines(btn) {
  var sid = btn.getAttribute('data-sid');
  var r = await athomeCall('/time-machine/browse/session/' + encodeURIComponent(sid) + '/lines?tail=100');
  var holder = document.createElement('pre');
  holder.style.cssText = TM_WRAP + 'white-space:pre-wrap;max-height:320px;overflow:auto;margin:4px 0';
  holder.textContent = (r.body && r.body.lines) ? r.body.lines.join('\n') : athomeErrorText(r.body, 'Those lines could not be read.');
  btn.parentNode.appendChild(holder);
}

// ── plan and apply: the one write ──────────────────────────────────────────────────────
function tmAssertionRow(a, extra) {
  return '<div style="padding:4px 0;border-top:1px solid var(--border);' + TM_WRAP + '"><strong>' + tmText(a.name) + '</strong> ' +
    '<span class="o-faint">' + tmText(extra || '') + '</span>' +
    '<div style="white-space:pre-wrap;overflow-wrap:anywhere">' + tmText(tmValue(a.value)) + '</div></div>';
}
function tmConflictHtml(c, i) {
  var live = c.live || {};
  return '<div data-conflict="' + tmAttr(c.key) + '" style="padding:6px 0;border-top:1px solid var(--border);' + TM_WRAP + '">' +
    '<div><strong>' + tmText(c.name) + '</strong></div>' +
    '<div class="o-faint">' + tmText(TM_CONFLICT_WORDS[c.kind] || c.kind) + '</div>' +
    '<div class="o-faint">Then:</div><div style="white-space:pre-wrap;overflow-wrap:anywhere">' + tmText(tmValue(c.value)) + '</div>' +
    '<div class="o-faint">Now (live):</div><div style="white-space:pre-wrap;overflow-wrap:anywhere">' + tmText(tmValue(live.value)) + '</div>' +
    '<label style="margin-right:12px"><input type="radio" name="tmc-' + i + '" value="keep" checked /> Keep what is live</label>' +
    '<label><input type="radio" name="tmc-' + i + '" value="restore" /> Restore the old value</label></div>';
}
function tmLinkHtml(l, i) {
  return '<label style="display:block;padding:3px 0;' + TM_WRAP + '"><input type="checkbox" data-link="' + tmAttr(l.key) + '" checked /> ' +
    tmText(l.type) + ' ' + (l.direction === 'out' ? 'to' : 'from') + ' <code>' + tmText((l.other || {}).type) + '</code> ' +
    tmText((l.other || {}).canonical) + (l.note ? ' <span class="o-faint">(' + tmText(l.note) + ')</span>' : '') + '</label>';
}
function tmPlanHtml(p) {
  var o = p.object || {}, as = p.assertions || {}, ln = p.links || {}, c = p.counts || {};
  var copy = (as.copy || []).map(function (a) { return tmAssertionRow(a, TM_EFFECT_WORDS[a.effect] || a.effect); }).join('');
  var skip = (as.skip || []).length;
  var nothing = !(as.copy || []).length && !(as.conflicts || []).length && !(ln.copy || []).length;
  return '<div style="' + TM_WRAP + '"><strong>Restore <code>' + tmText(o.type) + '</code> ' + tmText(o.canonical) + '</strong></div>' +
    '<div class="o-faint">From ' + tmText(p.moment || 'the open moment') + '. ' +
    (o.action === 'create' ? 'It does not exist now, so it would be created.' : 'It exists now, so this adds to it.') + ' Nothing is written until you press Apply.</div>' +
    (o.note ? '<div style="margin:6px 0;' + TM_WRAP + '"><strong>' + tmText(o.note) + '</strong></div>' : '') +
    '<h4 class="o-faint" style="margin:10px 0 2px">Would be added (' + tmText((as.copy || []).length) + ')</h4>' + (copy || '<div class="o-empty">Nothing.</div>') +
    ((as.conflicts || []).length ? '<h4 class="o-faint" style="margin:10px 0 2px">Differs from live: you choose (' + tmText(as.conflicts.length) + ')</h4>' + as.conflicts.map(tmConflictHtml).join('') : '') +
    '<div class="o-faint" style="margin-top:8px">' + tmText(skip) + ' property(ies) already match live and are left alone.' +
    ((p.live_only || []).length ? ' Only live, never touched: ' + tmText(p.live_only.join(', ')) + '.' : '') + '</div>' +
    ((ln.copy || []).length ? '<h4 class="o-faint" style="margin:10px 0 2px">Links to bring back (' + tmText(ln.copy.length) + ')</h4>' + ln.copy.map(tmLinkHtml).join('') : '') +
    ((ln.skip || []).length ? '<div class="o-faint">' + tmText(ln.skip.length) + ' link(s) left out (already there or an end is missing).</div>' : '') +
    (ln.truncated ? '<div class="o-faint">Only the first links are shown.</div>' : '') +
    '<div style="margin:10px 0"><button class="iconbtn" onclick="tmBack()">Back</button> ' +
    '<button class="iconbtn" id="tm-apply"' + (nothing ? ' disabled style="opacity:0.45"' : '') + ' onclick="tmApply()">Apply</button> ' +
    '<span class="o-faint">' + (nothing ? 'There is nothing to add.' : 'Adds only; nothing live is overwritten or deleted.') + '</span></div>' +
    '<div id="tm-apply-msg" class="o-faint" style="' + TM_WRAP + '"></div>';
}
// What the operator chose: each conflict's keep or restore, and the links still ticked.
function tmChoices(rootEl) {
  var resolutions = {}, links = [];
  rootEl.querySelectorAll('[data-conflict]').forEach(function (el) {
    var picked = el.querySelector('input[type=radio]:checked');
    resolutions[el.getAttribute('data-conflict')] = picked ? picked.value : 'keep';
  });
  rootEl.querySelectorAll('input[data-link]').forEach(function (el) {
    if (el.checked) links.push(el.getAttribute('data-link'));
  });
  return { resolutions: resolutions, links: links };
}
async function tmPlan() {
  TM.view = 'plan'; tmRender();
  var box = $('tm-browse');
  var r = await athomeCall('/time-machine/plan', 'POST', { ref: TM.ref });
  if (!box) return;
  if (r.status >= 400 || !r.body || r.body.error) {
    box.innerHTML = '<div class="o-empty">' + tmText(athomeErrorText(r.body, 'No plan could be made.')) + '</div><button class="iconbtn" onclick="tmBack()">Back</button>';
    return;
  }
  TM.plan = r.body;
  box.innerHTML = tmPlanHtml(r.body);
}
async function tmApply() {
  if (!TM.plan || TM.busy) return;
  var box = $('tm-browse'), msg = $('tm-apply-msg'), btn = $('tm-apply');
  var chosen = tmChoices(box);
  TM.busy = true; if (btn) btn.disabled = true;
  if (msg) msg.textContent = 'Adding…';
  var r = await athomeCall('/time-machine/apply', 'POST', {
    ref: TM.ref, digest: TM.plan.digest, resolutions: chosen.resolutions, links: chosen.links, note: '' });
  TM.busy = false;
  if (r.status >= 400 || !r.body || r.body.error) {
    if (msg) msg.innerHTML = tmText(athomeErrorText(r.body, 'It was not applied.')) + ' <button class="iconbtn" onclick="tmPlan()">Make a fresh plan</button>';
    if (btn) btn.disabled = false;
    return;
  }
  var b = r.body;
  TM.plan = null;
  if (msg) msg.textContent = b.changed === false ? 'Nothing needed adding.' :
    'Done. ' + (b.created ? 'The item was created. ' : '') + tmText(b.assertions_copied || 0) + ' propert(ies) and ' + tmText(b.links_copied || 0) +
    ' link(s) were added; ' + tmText((b.conflicts_kept || []).length) + ' conflict(s) kept as they are live.';
  if (btn) btn.style.display = 'none';
}

// ── the slot in Backups ────────────────────────────────────────────────────────────────
function tmSlotHtml() {
  return '<div id="settings-past-moment" style="margin-top:14px"><div id="tm-root"><div class="o-faint">Checking whether a past moment is open…</div></div>' +
    '<div id="tm-msg" class="o-faint" style="' + TM_WRAP + '"></div></div>';
}
function tmRender() {
  var root = $('tm-root');
  if (!root) return;
  var t = TM.status;
  if (t) {
    root.innerHTML = '<h4 class="o-faint" style="margin:0 0 4px">A past moment</h4>' + tmStatusHtml(t) +
      (t.state === 'ready' ? '<div id="tm-browse" style="margin-top:6px"></div>' : '');
    if (t.state === 'ready') {
      if (TM.view === 'items') tmLoadItems(false);
      else if (TM.view === 'detail' || TM.view === 'plan' || TM.view === 'sessions') { /* loaded by its own call */ }
    }
    return;
  }
  if (TM.shown && TM.moments) {
    root.innerHTML = '<h4 class="o-faint" style="margin:0 0 4px">Browse a past moment</h4>' +
      '<div class="o-faint" style="margin-bottom:6px">Opens a throwaway copy you can look through. Only one is open at a time and it closes itself after two hours.</div>' +
      tmMomentsHtml(TM.moments);
    return;
  }
  root.innerHTML = '<button class="iconbtn" onclick="tmShowList()">Browse a past moment</button> ' +
    '<span class="o-faint">Look at an earlier state of the database, and bring one item back.</span>';
}
function tmInit() {
  TM.shown = false; TM.moments = null; TM.view = 'items'; TM.types = null; TM.plan = null;
  clearTimeout(TM.timer);
  tmRefreshStatus();
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { tmMomentHtml, tmMomentsHtml, tmStatusHtml, tmPlanHtml, tmDetailHtml, tmUtc, tmLocalBox,
                     tmLeft, tmSize, tmValue, tmChoices };
}
