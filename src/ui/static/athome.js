// ── The At Home Panel ───────────────────────────────────────────────────────────────
// Everything that waits only on the operator being there, in one place: the checklist (same
// order and words as `osiris at-home`), the keys that open the vault with their proof dates, a
// recovery code shown once, a second security key, the first offload to the home storage with
// its six step lines, and the result of the restore test. It reads and acts through the
// /soul-key/* and /at-home/* routes and keeps nothing: a recovery code lives in the page only while it is on
// screen (never in a variable that outlives it, never in localStorage or sessionStorage).
//
// Loaded after console.js, which provides $, esc, showPanel, setStatus and the base64url
// helpers. The pure helpers below also run under node (see tests/test_athome_panel.py).

var ATHOME_STATUS_WORDS = {
  done: 'Done',
  waiting: 'Waiting on you',
  blocked: 'Waiting for an earlier step',
  optional: 'Only if you want it',
};
var ATHOME_STEP_MARK = { running: '…', ok: 'ok', failed: 'failed', skipped: 'skipped' };
var ATHOME_CODE_SHOWN = false;

function athomeAttr(s) {
  return String(s == null ? '' : s).replace(/[<>&"']/g, function (c) {
    return { '<': '&lt;', '>': '&gt;', '&': '&amp;', '"': '&quot;', "'": '&#39;' }[c];
  });
}
function athomeText(s) {
  return String(s == null ? '' : s).replace(/[<>&]/g, function (c) {
    return { '<': '&lt;', '>': '&gt;', '&': '&amp;' }[c];
  });
}
function athomeStatusLabel(status) {
  return ATHOME_STATUS_WORDS[status] || String(status || '');
}
function athomeDays(n) {
  if (n == null) return 'never';
  if (n < 1) return 'today';
  if (n < 2) return 'yesterday';
  return Math.floor(n) + ' days ago';
}
function athomeWhen(iso) {
  if (!iso) return 'never';
  var d = new Date(iso);
  if (isNaN(d.getTime())) return String(iso);
  return d.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
}
// A recovery code is read aloud or copied by hand: groups of four are easier to copy correctly.
function athomeGroupCode(code) {
  var raw = String(code || '').replace(/\s+/g, '');
  if (raw.indexOf('-') !== -1) return raw;
  var out = [];
  for (var i = 0; i < raw.length; i += 4) out.push(raw.slice(i, i + 4));
  return out.join(' ');
}
// Newline-separated JSON objects arrive in arbitrary chunks: complete lines are returned parsed,
// the unfinished tail is handed back to be joined with the next chunk.
function athomeParseLines(buffer) {
  var parts = buffer.split('\n');
  var rest = parts.pop();
  var lines = [];
  parts.forEach(function (p) {
    if (!p.trim()) return;
    try { lines.push(JSON.parse(p)); } catch (e) { lines.push({ text: p, status: 'running' }); }
  });
  return { lines: lines, rest: rest };
}
function athomeErrorText(body, fallback) {
  if (body && typeof body.error === 'string' && body.error) return body.error;
  if (body && typeof body.detail === 'string' && body.detail) return body.detail;
  return fallback;
}

async function athomeCall(path, method, payload) {
  var opts = { method: method || 'GET', headers: {}, cache: 'no-store' };
  if (payload !== undefined) {
    opts.headers['content-type'] = 'application/json';
    opts.body = JSON.stringify(payload);
  }
  var res;
  try {
    res = await fetch(path, opts);
  } catch (e) {
    return { status: 0, body: { error: 'Could not reach the console. Check that it is running.' } };
  }
  var body = null;
  try { body = await res.json(); } catch (e) { body = null; }
  if (res.status === 403) {
    body = { error: 'These controls only answer on the computer that runs the console. Open the console on that computer.' };
  }
  return { status: res.status, body: body || {} };
}

async function athomeStream(path, onLine, payload) {
  var res = await fetch(path, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload || {}) });
  if (!res.ok || !res.body) {
    var body = null;
    try { body = await res.json(); } catch (e) { body = null; }
    onLine({ done: true, ok: false, text: res.status === 404
      ? 'This is not available yet on this machine.'
      : (res.status === 403 ? 'These controls only answer on the computer that runs the console. Open the console on that computer.'
         : athomeErrorText(body, 'This could not be started (error ' + res.status + ').')) });
    return;
  }
  var reader = res.body.getReader();
  var decoder = new TextDecoder();
  var buffer = '';
  for (;;) {
    var chunk = await reader.read();
    if (chunk.done) break;
    buffer += decoder.decode(chunk.value, { stream: true });
    var parsed = athomeParseLines(buffer);
    buffer = parsed.rest;
    parsed.lines.forEach(onLine);
  }
  buffer += decoder.decode();
  athomeParseLines(buffer + '\n').lines.forEach(onLine);
}

// ── rendering ────────────────────────────────────────────────────────────────────────
// The console's tables cut long text off with an ellipsis; a checklist is read in full, so the
// lists here are plain blocks that wrap.
var ATHOME_WRAP = 'white-space:normal;overflow-wrap:anywhere;';
var ATHOME_RECIPIENTS = [];

// What each item lets you do from here. The page does the work; the terminal command is only a
// small detail underneath for someone who prefers it.
function athomeActionHtml(it) {
  var k = it.key, open = it.status !== 'done';
  if (!open) return '';
  var btn = function (label, fn) { return '<button class="iconbtn" onclick="' + fn + '">' + athomeText(label) + '</button> '; };
  if (k === 'envelope_drill') return btn('Prove a key now', 'athomeActProve()');
  if (k === 'envelope_code') return btn('Add a recovery code', 'athomeActCode()');
  if (k === 'envelope_key') return btn('Add a second key', 'athomeActKey()');
  if (k === 'nas_offload') {
    return btn('Check', "athomeNas('check')") +
      '<label style="font-size:11px;margin:0 6px"><input type="checkbox" id="athome-nas-confirm" onchange="athomeNasButton()" /> I am on the home network</label>' +
      '<button class="iconbtn" id="athome-nas-run" disabled style="opacity:0.45" onclick="athomeNas(\'run\')">Make the first copy</button>' +
      '<ol id="athome-nas-steps" style="margin:8px 0 0 18px"></ol>';
  }
  if (k === 'plaintext_copy') return '<div id="athome-plain" class="o-faint">Loading…</div>';
  return '';
}
function athomeChecklistHtml(data) {
  var items = (data && data.items) || [];
  if (!items.length) return '<div class="o-empty">Nothing is waiting on you.</div>';
  return items.map(function (it) {
    var cmd = it.command && it.key === 'rebuild_probe'
      ? '<div class="o-faint" style="margin-top:6px">This one runs from a terminal because it uses a model call. Run: ' +
        '<code style="' + ATHOME_WRAP + '">' + athomeText(it.command) + '</code> ' +
        '<button class="iconbtn" data-copy="' + athomeAttr(it.command) + '" onclick="athomeCopy(this)">Copy</button></div>'
      : it.command
      ? '<details style="margin-top:6px"><summary class="o-faint" style="cursor:pointer">or run in a terminal</summary>' +
        '<code style="' + ATHOME_WRAP + '">' + athomeText(it.command) + '</code> ' +
        '<button class="iconbtn" data-copy="' + athomeAttr(it.command) + '" onclick="athomeCopy(this)">Copy</button></details>'
      : '';
    var then = it.then ? '<div class="o-faint" style="margin-top:4px;' + ATHOME_WRAP + '">Then: ' + athomeText(it.then) + '</div>' : '';
    var act = athomeActionHtml(it);
    return '<div style="display:flex;gap:10px;padding:8px 0;border-bottom:1px solid var(--border,#222)">' +
      '<div style="width:20px;flex:none" class="o-faint">' + athomeText(it.order) + '</div>' +
      '<div style="min-width:0;flex:1;' + ATHOME_WRAP + '"><strong>' + athomeText(it.title) + '</strong> ' +
      '<span class="o-faint">' + athomeText(athomeStatusLabel(it.status)) + '</span>' +
      '<div class="o-faint" style="margin-top:2px;' + ATHOME_WRAP + '">' + athomeText(it.why) + '</div>' +
      (act ? '<div style="margin-top:8px">' + act + '</div>' : '') + then + cmd + '</div></div>';
  }).join('');
}

function athomeKindLabel(kind) {
  if (kind === 'fido2-prf') return 'Security key';
  if (kind === 'recovery-code') return 'Recovery code';
  return String(kind || 'Key');
}
var ATHOME_VERDICT_WORDS = {
  proved: 'Proved', unproven: 'Never proved', stale: 'Not proved in a while', failed: 'Last test failed',
};
function athomeVerdictLabel(v) { return ATHOME_VERDICT_WORDS[v] || (v ? String(v) : ''); }
function athomeKeysHtml(env) {
  if (env && env.error) return '<div class="o-empty">The vault keys could not be read: ' + athomeText(env.error) + '</div>';
  var recipients = (env && env.recipients) || [];
  if (!recipients.length) {
    return '<div class="o-empty">No key is set up yet. Add a recovery code or a security key below.</div>';
  }
  var ids = recipients.map(function (r) { return String(r.id); });
  var rows = recipients.map(function (r) {
    var isKey = r.kind === 'fido2-prf';
    var verdict = athomeVerdictLabel(r.verdict);
    var button = isKey
      ? '<button class="iconbtn" data-id="' + athomeAttr(r.id) + '" data-rp="' + athomeAttr(r.rp_id || '') +
        '" onclick="athomeProveKey(this)">Prove this key</button>'
      : '<button class="iconbtn" data-id="' + athomeAttr(r.id) + '" onclick="athomeShowCodeProof(this)">Prove this code</button>';
    var opensAt = isKey && r.rp_id
      ? '<div class="o-faint">Opens only when the console is at <code>' + athomeText(r.rp_id) + '</code></div>' : '';
    return '<div style="padding:8px 0;border-bottom:1px solid var(--border,#222);' + ATHOME_WRAP + '">' +
      '<div><strong>' + athomeText(athomeKindLabel(r.kind)) + '</strong> ' + athomeText(r.label || '') +
      ' <span class="o-faint">(' + athomeText(r.id) + ')</span></div>' +
      '<div class="o-faint">Last proved: ' + athomeText(athomeWhen(r.last_proved_at)) +
      (verdict ? ' (' + athomeText(verdict) + ')' : '') + '. Last opened: ' + athomeText(athomeWhen(r.last_opened_at)) + '.</div>' +
      opensAt + '<div style="margin-top:6px">' + button + '</div>' +
      '<div id="athome-proof-' + athomeAttr(r.id) + '" class="o-faint" style="margin-top:4px"></div></div>';
  }).join('');
  var check = (env && env.check) || {};
  // a note that names one key repeats that key's own row above: keep only the ones about the whole set
  var notes = [].concat(check.problems || [], check.warnings || []).filter(function (w) {
    return !ids.some(function (id) { return String(w).indexOf(id) !== -1; });
  }).map(function (w) {
    return '<div style="margin:6px 0;' + ATHOME_WRAP + '">' + athomeText(w) + '</div>';
  }).join('');
  return notes + rows;
}

function athomeRestoreHtml(r) {
  if (!r || r.ok == null) {
    return '<div class="o-empty">The restore test has not been run yet. It runs after the first offload.</div>';
  }
  var steps = ((r.steps || []).map(function (s) {
    return '<li>' + athomeText(s.text) + ' ' + (s.ok ? '' : '<strong>(failed)</strong>') + '</li>';
  }).join(''));
  return '<div><strong>' + (r.ok ? 'Passed' : 'Failed') + '</strong> ' +
    '<span class="o-faint">' + athomeText(athomeWhen(r.when)) +
    (r.files_checked != null ? ', ' + athomeText(r.files_checked) + ' files checked' : '') + '</span></div>' +
    (r.text ? '<div class="o-faint" style="margin-top:2px">' + athomeText(r.text) + '</div>' : '') +
    (steps ? '<ul style="margin:6px 0 0 18px">' + steps + '</ul>' : '');
}

function athomeSection(title, hint, innerId, bodyHtml) {
  return '<section style="margin:0 0 22px">' +
    '<h3 style="font-size:12px;text-transform:uppercase;letter-spacing:0.5px;color:var(--muted);margin:0 0 2px">' +
    athomeText(title) + '</h3>' +
    '<div class="o-faint" style="margin-bottom:8px">' + athomeText(hint) + '</div>' +
    '<div id="' + innerId + '">' + (bodyHtml || '') + '</div></section>';
}

async function renderAtHomePanel() {
  var container = $('result'); showPanel();
  $('entity-taxonomy-bar').style.display = 'none';
  athomeClearCode();
  container.innerHTML = '<div style="padding:16px;max-width:900px;margin:0 auto">' +
    '<div style="display:flex;align-items:baseline;justify-content:space-between">' +
    '<h2 style="font-size:13px;text-transform:uppercase;letter-spacing:0.5px;color:var(--muted);margin:0 0 4px">At home</h2>' +
    '<button class="iconbtn" onclick="renderAtHomePanel()">Refresh</button></div>' +
    '<div class="o-faint" style="margin-bottom:14px">What waits only on you being there. Nothing here changes anything until you press its button.</div>' +
    athomeSection('What is waiting', 'In the order to do it. The same list as osiris at-home in a terminal.', 'athome-list', '<div class="o-empty">Loading…</div>') +
    athomeSection('Vault keys', 'Each key that can open the vault. Prove a key to show it still works; it only updates its "last opened" date.', 'athome-keys', '<div class="o-empty">Loading…</div>') +
    '<div id="athome-key-msg" class="o-faint" style="margin:-8px 0 10px"></div>' +
    '<div id="athome-code"></div>' +
    '<div id="athome-key-actions" style="margin:0 0 22px">' +
      '<button class="iconbtn" onclick="athomeAddCode()">Add a recovery code</button> ' +
      '<button class="iconbtn" onclick="athomeShowAddKey()">Add a second key</button>' +
      '<div id="athome-addkey" style="margin-top:8px"></div></div>' +
    athomeSection('Restore test', 'The last time a copy was restored and checked.', 'athome-restore', '<div class="o-empty">Loading…</div>') +
    '<div style="margin:-14px 0 22px"><button class="iconbtn" onclick="athomeRestoreRun()">Run the restore test now</button>' +
    '<ol id="athome-restore-steps" style="margin:8px 0 0 18px"></ol></div>' +
    '</div>';
  athomeLoadAll();
}

async function athomeLoadAll() {
  var results = await Promise.all([
    athomeCall('/soul-key/at-home'), athomeCall('/soul-key/envelope'), athomeCall('/at-home/restore-test'),
  ]);
  var unavailable = '<div class="o-empty">This is not available yet on this machine.</div>';
  var list = $('athome-list'), keys = $('athome-keys'), restore = $('athome-restore');
  if (list) list.innerHTML = results[0].status === 404 ? unavailable
    : (results[0].status >= 400 ? '<div class="o-empty">' + athomeText(athomeErrorText(results[0].body, 'The list could not be read.')) + '</div>'
       : athomeChecklistHtml(results[0].body));
  var plainItem = ((results[0].body && results[0].body.items) || []).find(function (i) { return i.key === 'plaintext_copy'; });
  ATHOME_PLAIN.item = plainItem || null;
  if ($('athome-plain')) athomePlainLoad();
  ATHOME_RECIPIENTS = (results[1].body && results[1].body.recipients) || [];
  if (keys) keys.innerHTML = results[1].status === 404 ? unavailable
    : (results[1].status >= 400 ? '<div class="o-empty">' + athomeText(athomeErrorText(results[1].body, 'The keys could not be read.')) + '</div>'
       : athomeKeysHtml(results[1].body));
  if (restore) restore.innerHTML = results[2].status === 404 ? unavailable
    : (results[2].status >= 400 ? '<div class="o-empty">' + athomeText(athomeErrorText(results[2].body, 'The result could not be read.')) + '</div>'
       : athomeRestoreHtml(results[2].body));
}

function athomeCopy(btn) {
  var text = btn.getAttribute('data-copy') || '';
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text).then(function () { btn.textContent = 'Copied'; });
  }
}
function athomeMsg(text) { var el = $('athome-key-msg'); if (el) el.textContent = text || ''; }

// ── prove a key or a code ────────────────────────────────────────────────────────────
// A key only works from a page whose address matches the key's own site name; say so plainly
// instead of letting the browser fail with a generic error. The secret the key returns is used
// for one request and dropped: it is never logged, never put in an address, never kept.
function athomeHostOk(rpId) {
  var host = (typeof document !== 'undefined' && document.location) ? document.location.hostname : '';
  return !!rpId && (host === rpId || host.endsWith('.' + rpId));
}
function athomeWrongHost(rpId) {
  return 'Open the console at ' + rpId + ' to use this key. The browser will not use a security key from a different address.';
}
function athomeProofMsg(id, text) {
  var el = $('athome-proof-' + id);
  if (el) el.textContent = text || '';
  else athomeMsg(text);
}
function athomeDrillText(r) {
  var b = r.body || {};
  if (r.status >= 400 || b.error) return { ok: false, text: athomeErrorText(b, 'That did not work (error ' + r.status + ').') };
  if (b.status === 'proved' || b.ok) return { ok: true, text: 'Proved. It opened the vault just now.' };
  if (b.status === 'needs_operator_key') {
    return { ok: false, text: 'Nothing was tested. ' + (b.reason || 'This needs the operator key first.') };
  }
  return { ok: false, text: b.reason || 'The test failed.' };
}
function athomeProveKey(btn) {
  return athomeProveKeyFor(btn.getAttribute('data-id'), btn.getAttribute('data-rp'), btn);
}
async function athomeProveKeyFor(id, rpId, btn) {
  btn = btn || { disabled: false };
  if (!(window.PublicKeyCredential && navigator.credentials)) {
    athomeProofMsg(id, 'This browser cannot talk to a security key.'); return;
  }
  if (!athomeHostOk(rpId)) { athomeProofMsg(id, athomeWrongHost(rpId)); return; }
  btn.disabled = true;
  athomeProofMsg(id, 'Touch your security key when it blinks.');
  var ch = await athomeCall('/soul-key/envelope/' + encodeURIComponent(id) + '/challenge');
  if (ch.status >= 400 || !ch.body.salt) {
    btn.disabled = false;
    athomeProofMsg(id, athomeErrorText(ch.body, 'The test could not be started.')); return;
  }
  var c = ch.body, prfBytes = null, assertion;
  try {
    assertion = await navigator.credentials.get({ publicKey: {
      challenge: crypto.getRandomValues(new Uint8Array(32)), rpId: c.rp_id || rpId,
      allowCredentials: [{ type: 'public-key', id: b64urlDecodeToBytes(c.credential_id) }],
      userVerification: 'required',
      extensions: { prf: { eval: { first: b64urlDecodeToBytes(c.salt) } } },
    } });
  } catch (e) {
    btn.disabled = false;
    athomeProofMsg(id, 'The key was not touched in time, or the browser refused it.'); return;
  }
  var results = assertion.getClientExtensionResults().prf;
  if (!results || !results.results || !results.results.first) {
    btn.disabled = false;
    athomeProofMsg(id, 'This key did not return the secret the vault needs. It may not support this feature.'); return;
  }
  prfBytes = new Uint8Array(results.results.first);
  var body = { nonce: c.nonce, prf: b64urlEncodeBytes(prfBytes) };
  prfBytes.fill(0);
  var r = await athomeCall('/soul-key/envelope/' + encodeURIComponent(id) + '/drill', 'POST', body);
  body.prf = null;
  btn.disabled = false;
  var out = athomeDrillText(r);
  athomeProofMsg(id, out.text);
  if (out.ok) athomeReloadKeys();
}
function athomeShowCodeProof(btn) {
  var id = btn.getAttribute('data-id'), el = $('athome-proof-' + id);
  if (!el) return;
  el.innerHTML = '<input type="password" autocomplete="off" id="athome-code-input-' + athomeAttr(id) +
    '" class="filter" style="width:220px;margin:0" placeholder="Type the recovery code" /> ' +
    '<button class="iconbtn" data-id="' + athomeAttr(id) + '" onclick="athomeProveCode(this)">Test it</button>' +
    '<div class="o-faint" id="athome-code-msg-' + athomeAttr(id) + '"></div>';
}
async function athomeProveCode(btn) {
  var id = btn.getAttribute('data-id');
  var input = $('athome-code-input-' + id), msg = $('athome-code-msg-' + id);
  var typed = input ? input.value.replace(/\s+/g, '') : '';
  if (!typed) { if (msg) msg.textContent = 'Type the code first.'; return; }
  btn.disabled = true;
  var ch = await athomeCall('/soul-key/envelope/' + encodeURIComponent(id) + '/challenge');
  if (ch.status >= 400 || !ch.body.nonce) {
    btn.disabled = false;
    if (msg) msg.textContent = athomeErrorText(ch.body, 'The test could not be started.'); return;
  }
  var r = await athomeCall('/soul-key/envelope/' + encodeURIComponent(id) + '/drill', 'POST', { nonce: ch.body.nonce, code: typed });
  if (input) input.value = '';
  btn.disabled = false;
  var out = athomeDrillText(r);
  if (msg) msg.textContent = out.text;
  if (out.ok) athomeReloadKeys();
}
async function athomeReloadKeys() {
  var r = await athomeCall('/soul-key/envelope');
  var keys = $('athome-keys');
  if (keys && r.status < 400) {
    ATHOME_RECIPIENTS = r.body.recipients || [];
    keys.innerHTML = athomeKeysHtml(r.body);
  }
}
function athomeJump(id) {
  var el = $(id);
  if (el && el.scrollIntoView) el.scrollIntoView({ behavior: 'smooth', block: 'center' });
}
function athomeActProve() {
  var keys = ATHOME_RECIPIENTS.filter(function (r) { return r.kind === 'fido2-prf'; });
  var next = keys.find(function (r) { return !r.last_proved_at; }) || keys[0];
  athomeJump('athome-keys');
  if (!next) { athomeMsg('There is no security key to prove.'); return; }
  athomeProveKeyFor(next.id, next.rp_id, null);
}
function athomeActCode() { athomeJump('athome-code'); athomeAddCode(); }
function athomeActKey() { athomeShowAddKey(); athomeJump('athome-addkey'); }

// ── a recovery code, shown once ──────────────────────────────────────────────────────
function athomeClearCode() {
  ATHOME_CODE_SHOWN = false;
  window.removeEventListener('beforeunload', athomeWarnLeave);
  var el = $('athome-code');
  if (el) el.innerHTML = '';
}
function athomeWarnLeave(e) { e.preventDefault(); e.returnValue = ''; }

async function athomeAddCode() {
  if (ATHOME_CODE_SHOWN) { athomeMsg('Finish the code on screen first.'); return; }
  athomeMsg('Making a recovery code…');
  var r = await athomeCall('/soul-key/envelope/add-code', 'POST', { label: 'Recovery code ' + new Date().toISOString().slice(0, 10) });
  if (r.status >= 400 || !r.body.code) {
    athomeMsg(athomeErrorText(r.body, 'A recovery code could not be made (error ' + r.status + ').'));
    return;
  }
  athomeMsg('');
  ATHOME_CODE_SHOWN = true;
  window.addEventListener('beforeunload', athomeWarnLeave);
  var id = r.body.id;
  var el = $('athome-code');
  if (!el) return;
  el.innerHTML = '<div style="border:1px solid var(--border2);border-radius:8px;padding:16px;margin:0 0 14px">' +
    '<div style="margin-bottom:8px"><strong>Write this code down now.</strong> It is shown once and cannot be shown again.</div>' +
    '<div id="athome-code-value" style="font-family:var(--mono,monospace);font-size:26px;letter-spacing:2px;line-height:1.5;word-break:break-all">' +
    athomeText(athomeGroupCode(r.body.code)) + '</div>' +
    '<div class="o-faint" style="margin:8px 0">Keep it somewhere safe, away from this computer. Anyone with this code and the vault files can open the vault.</div>' +
    '<button class="iconbtn" onclick="athomeConfirmCode()">I wrote it down</button> ' +
    '<span style="margin-left:12px"><label style="font-size:11px"><input type="checkbox" id="athome-cancel-confirm" /> ' +
    'I do not want this code</label> ' +
    '<button class="iconbtn" data-id="' + athomeAttr(id) + '" onclick="athomeCancelCode(this)">Remove this code</button></span></div>';
}
function athomeConfirmCode() {
  // the server already holds the recipient; the only thing left is to make the code disappear
  athomeClearCode();
  athomeMsg('Recovery code added. It is gone from this page.');
  athomeReloadKeys();
}
async function athomeCancelCode(btn) {
  var box = $('athome-cancel-confirm');
  if (!box || !box.checked) { athomeMsg('Tick "I do not want this code" first.'); return; }
  btn.disabled = true;
  var r = await athomeCall('/soul-key/envelope/remove', 'POST', { recipient: btn.getAttribute('data-id') });
  if (r.status >= 400 || (r.body && r.body.error)) {
    btn.disabled = false;
    athomeMsg(athomeErrorText(r.body, 'The code could not be removed. It is still on screen.'));
    return;
  }
  athomeClearCode();
  athomeMsg('The code was removed. Nothing was added.');
  athomeReloadKeys();
}

// ── a second security key ────────────────────────────────────────────────────────────
function athomeShowAddKey() {
  var el = $('athome-addkey');
  if (!el) return;
  el.innerHTML = '<label style="font-size:11px"><input type="checkbox" id="athome-addkey-confirm" /> ' +
    'I have the second security key in my hand</label> ' +
    '<input id="athome-addkey-label" class="filter" style="width:180px;margin:0 6px" placeholder="A name for it" /> ' +
    '<button class="iconbtn" onclick="athomeAddKey()">Add it</button>';
}
async function athomeAddKey() {
  var box = $('athome-addkey-confirm');
  if (!box || !box.checked) { athomeMsg('Tick that you have the second key first.'); return; }
  if (!(window.PublicKeyCredential && navigator.credentials)) { athomeMsg('This browser cannot talk to a security key.'); return; }
  var labelEl = $('athome-addkey-label');
  var label = (labelEl && labelEl.value.trim()) || 'Second security key';
  athomeMsg('Starting…');
  var begin = await athomeCall('/soul-key/envelope/add-key/begin', 'POST', { label: label });
  if (begin.status >= 400 || !begin.body.token) {
    athomeMsg(athomeErrorText(begin.body, 'Adding a key could not be started.')); return;
  }
  var b = begin.body;
  if (!athomeHostOk(b.rp_id)) { athomeMsg(athomeWrongHost(b.rp_id)); return; }
  athomeMsg('Touch the new key when it blinks.');
  var salt = b64urlDecodeToBytes(b.salt), created, prfBytes = null;
  try {
    created = await navigator.credentials.create({ publicKey: {
      rp: { id: b.rp_id, name: b.rp_name }, user: { id: b64urlDecodeToBytes(b.user.id), name: b.user.name, displayName: b.user.displayName },
      challenge: b64urlDecodeToBytes(b.challenge), pubKeyCredParams: b.pub_key_cred_params, timeout: b.timeout,
      authenticatorSelection: { userVerification: 'required' },
      extensions: { prf: { eval: { first: salt } } },
    } });
  } catch (e) {
    athomeMsg('The key was not touched in time, or the browser refused it.'); return;
  }
  var ext = created.getClientExtensionResults().prf;
  if (ext && ext.results && ext.results.first) {
    prfBytes = new Uint8Array(ext.results.first);
  } else {
    // some keys only produce the secret when asked, not when made: ask once with the same salt
    athomeMsg('Touch the key once more.');
    try {
      var asked = await navigator.credentials.get({ publicKey: {
        challenge: crypto.getRandomValues(new Uint8Array(32)), rpId: b.rp_id,
        allowCredentials: [{ type: 'public-key', id: new Uint8Array(created.rawId) }],
        userVerification: 'required', extensions: { prf: { eval: { first: salt } } },
      } });
      var again = asked.getClientExtensionResults().prf;
      if (again && again.results && again.results.first) prfBytes = new Uint8Array(again.results.first);
    } catch (e) { prfBytes = null; }
  }
  if (!prfBytes) {
    athomeMsg('This key did not return the secret the vault needs. It may not support this feature, so it was not added.'); return;
  }
  var body = { token: b.token, credential_id: b64urlEncodeBytes(new Uint8Array(created.rawId)), prf: b64urlEncodeBytes(prfBytes) };
  prfBytes.fill(0);
  var fin = await athomeCall('/soul-key/envelope/add-key/finish', 'POST', body);
  body.prf = null;
  if (fin.status >= 400 || (fin.body && fin.body.error)) {
    athomeMsg(athomeErrorText(fin.body, 'The key could not be added (error ' + fin.status + ').')); return;
  }
  athomeMsg('Second key added.');
  var el = $('athome-addkey'); if (el) el.innerHTML = '';
  athomeReloadKeys();
}

// ── the first copy to the home storage ───────────────────────────────────────────────
function athomeNasButton() {
  var box = $('athome-nas-confirm'), btn = $('athome-nas-run');
  if (btn) { btn.disabled = !(box && box.checked); btn.style.opacity = btn.disabled ? '0.45' : '1'; }
}
function athomeStepHtml(line) {
  var mark = ATHOME_STEP_MARK[line.status] || '';
  var n = line.n != null ? '<span class="o-faint">' + athomeText(line.n) + (line.of ? ' of ' + athomeText(line.of) : '') + '</span> ' : '';
  return n + athomeText(line.text || line.step || '') + (mark ? ' <strong>' + athomeText(mark) + '</strong>' : '');
}
async function athomeNas(which) {
  var list = $('athome-nas-steps');
  if (!list) return;
  list.innerHTML = '<li class="o-faint">Starting…</li>';
  var seen = {};
  var finished = false;
  await athomeStream('/at-home/nas/' + which, function (line) {
    if (line.done) {
      finished = true;
      var li = document.createElement('li');
      li.innerHTML = '<strong>' + (line.ok ? 'Finished.' : 'Stopped.') + '</strong> ' + athomeClosing(list, line);
      list.appendChild(li);
      return;
    }
    if (list.firstChild && list.firstChild.className === 'o-faint') list.removeChild(list.firstChild);
    var key = line.n != null ? 'n' + line.n : null;
    var li = key && seen[key] ? seen[key] : document.createElement('li');
    li.innerHTML = athomeStepHtml(line);
    if (key && !seen[key]) { seen[key] = li; list.appendChild(li); }
    else if (!key) list.appendChild(li);
  });
  if (!finished) {
    var tail = document.createElement('li');
    tail.className = 'o-faint';
    tail.textContent = 'The connection ended before it said it was done. Run Check again to see where it stands.';
    list.appendChild(tail);
  }
  // the checklist is not redrawn here: its rows hold these lines. Refresh shows the new status.
}


// ── one stream runner for every long job ────────────────────────────────────────────────
// Lines arrive one by one; a job that reports file counts also gets a progress line. Nothing
// here starts by itself: each runner is called from a button.
// The closing line often repeats the last sentence a step already said: say it only once.
function athomeClosing(list, line) {
  var last = list.lastChild ? list.lastChild.textContent : '';
  var text = String(line.text || '');
  return text && last.indexOf(text) !== -1 ? '' : athomeText(text);
}
function athomeLineHtml(line) {
  var count = (line.total_files != null && line.done_files != null)
    ? ' <span class="o-faint">(' + athomeText(line.done_files) + ' of ' + athomeText(line.total_files) + ' files)</span>' : '';
  return athomeStepHtml(line) + count;
}
async function athomeRunPost(path, payload, listId) { return athomeRunInto(path, listId, payload); }
async function athomeRunInto(path, listId, payload) {
  var list = $(listId);
  if (!list) return false;
  list.innerHTML = '<li class="o-faint">Starting…</li>';
  var finished = false, ok = false;
  await athomeStream(path, function (line) {
    var first = list.firstChild;
    if (first && first.className === 'o-faint') list.removeChild(first);
    var li = document.createElement('li');
    if (line.done) {
      finished = true; ok = !!line.ok;
      li.innerHTML = '<strong>' + (line.ok ? 'Finished.' : 'Stopped.') + '</strong> ' + athomeClosing(list, line);
    } else {
      li.innerHTML = athomeLineHtml(line);
    }
    list.appendChild(li);
  }, payload);
  if (!finished) {
    var tail = document.createElement('li');
    tail.className = 'o-faint';
    tail.textContent = 'The connection ended before it said it was done. Look again in a moment: it may still be running.';
    list.appendChild(tail);
  }
  return ok;
}

// ── the rebuild probe and the restore test: one button each ─────────────────────────────
async function athomeRestoreRun() {
  await athomeRunInto('/at-home/restore-test/run', 'athome-restore-steps');
  var r = await athomeCall('/at-home/restore-test');
  var el = $('athome-restore');
  if (el && r.status < 400) el.innerHTML = athomeRestoreHtml(r.body);
}

// ── the old plaintext copy: explain, offer three choices, do what is chosen ─────────────
// Keep records the choice only. Back it up starts the copy into the encrypted backup (home network
// only, can be stopped and pressed again) and only afterwards offers to remove the plaintext, which
// needs the word "remove" typed. Delete loses history: it needs the exact sentence the server
// defines, typed by hand, before anything is recorded or removed.
function athomeGB(bytes) { return bytes == null ? '?' : (bytes / 1e9).toFixed(bytes >= 1e11 ? 0 : 1); }
var ATHOME_CHOICE_WORDS = { keep: 'keep it', encrypt_then_remove: 'back it up, then remove it', delete: 'delete it' };
function athomeTypedBox(id, phrase, label, fn) {
  return '<div style="margin-top:6px"><div class="o-faint" style="margin-bottom:4px">To confirm, type: <code>' + athomeText(phrase) + '</code></div>' +
    '<input id="' + id + '" class="filter" autocomplete="off" style="width:260px;margin:0 6px 0 0" data-phrase="' + athomeAttr(phrase) +
    '" oninput="athomePlainPhrase(this, \'' + id + '-btn\')" /> ' +
    '<button class="iconbtn" id="' + id + '-btn" disabled style="opacity:0.45" onclick="' + fn + '">' + athomeText(label) + '</button></div>';
}
function athomePlainHtml(d, item) {
  d = d || {};
  var cov = d.coverage;
  var head = '<div style="margin-bottom:6px">' + athomeText((item && item.why) || 'The old unencrypted copy is still on the NAS.') + '</div>' +
    (cov && cov.only_files != null ? '<div class="o-faint" style="margin-bottom:6px">' + athomeText(cov.only_files) +
      ' file(s) (' + athomeText(athomeGB(cov.only_bytes)) + ' GB) are only in the old copy and not yet in the backup.</div>' : '');
  var choice = d.choice && d.choice.choice;
  var confirm = d.confirm || {};
  var chosen = choice ? '<div class="o-faint" style="margin-bottom:6px">Your choice so far: ' +
    athomeText(ATHOME_CHOICE_WORDS[choice] || choice) + '.</div>' : '';
  var buttons =
    '<div><button class="iconbtn" onclick="athomePlainChoose(\'keep\')">Keep it</button> ' +
      '<span class="o-faint">Nothing changes. It stays on the list.</span></div>' +
    '<div><button class="iconbtn" onclick="athomePlainChoose(\'encrypt_then_remove\')">Back it up, then remove the plaintext</button>' +
      '<div class="o-faint">Home network only. It can be stopped and started again. Nothing is removed until every file is shown to be in the backup.</div></div>' +
    '<div><button class="iconbtn" onclick="athomePlainDeleteStart()">Delete it</button> ' +
      '<span class="o-faint">Loses that history. Asks you to type a sentence first.</span><div id="athome-plain-delete"></div></div>';
  var after = '';
  if (choice === 'encrypt_then_remove') {
    after = '<div style="margin:8px 0"><button class="iconbtn" onclick="athomePlainEncrypt()">Back it up now</button> ' +
      '<span class="o-faint">If it stops, press it again to carry on.</span></div>' +
      athomeTypedBox('athome-plain-remove', confirm.encrypt_then_remove || 'remove', 'Remove the plaintext copy', 'athomePlainRemove()') +
      '<div class="o-faint">Removing first checks again that nothing is only in the old copy.</div>';
  }
  return head + chosen + (choice === 'encrypt_then_remove' ? '' : '<div style="display:flex;flex-direction:column;gap:10px">' + buttons + '</div>') + after +
    '<div id="athome-plain-msg" class="o-faint" style="margin-top:6px"></div>' +
    '<ol id="athome-plain-steps" style="margin:8px 0 0 18px"></ol>';
}
function athomePlainPhrase(input, btnId) {
  var btn = $(btnId);
  if (!btn) return;
  var ok = input.value.trim() === input.getAttribute('data-phrase');
  btn.disabled = !ok; btn.style.opacity = ok ? '1' : '0.45';
}
var ATHOME_PLAIN = { item: null, data: null };
async function athomePlainLoad() {
  var el = $('athome-plain');
  if (!el) return;
  var r = await athomeCall('/at-home/plaintext-copy');
  if (r.status === 404) { el.textContent = 'Choosing what to do with it is not available yet on this machine.'; return; }
  if (r.status >= 400) { el.textContent = athomeErrorText(r.body, 'Its state could not be read.'); return; }
  ATHOME_PLAIN.data = r.body;
  el.innerHTML = athomePlainHtml(r.body, ATHOME_PLAIN.item);
}
function athomePlainMsg(t) { var el = $('athome-plain-msg'); if (el) el.textContent = t || ''; }
async function athomePlainChoose(choice) {
  athomePlainMsg('Saving your choice…');
  var r = await athomeCall('/at-home/plaintext-copy/choice', 'POST', { choice: choice });
  if (r.status >= 400 || (r.body && r.body.error)) {
    athomePlainMsg(athomeErrorText(r.body, 'That was not saved (error ' + r.status + ').')); return false;
  }
  await athomePlainLoad();
  athomePlainMsg('Saved: ' + (ATHOME_CHOICE_WORDS[choice] || choice) + '.');
  if (choice === 'encrypt_then_remove') athomePlainEncrypt();
  return true;
}
function athomePlainDeleteStart() {
  var el = $('athome-plain-delete');
  if (!el) return;
  var phrase = (ATHOME_PLAIN.data && ATHOME_PLAIN.data.confirm && ATHOME_PLAIN.data.confirm.delete) || 'delete the old plaintext copy';
  el.innerHTML = athomeTypedBox('athome-plain-del', phrase, 'Delete it for good', 'athomePlainDelete()');
}
async function athomePlainDelete() {
  var box = $('athome-plain-del');
  if (!box || box.value.trim() !== box.getAttribute('data-phrase')) return;
  var phrase = box.value.trim();
  if (!(await athomePlainChoose('delete'))) return;
  await athomeRunPost('/at-home/plaintext-copy/remove', { confirm: phrase }, 'athome-plain-steps');
}
async function athomePlainEncrypt() {
  await athomeRunPost('/at-home/plaintext-copy/encrypt', {}, 'athome-plain-steps');
}
async function athomePlainRemove() {
  var box = $('athome-plain-remove');
  if (!box || box.value.trim() !== box.getAttribute('data-phrase')) return;
  await athomeRunPost('/at-home/plaintext-copy/remove', { confirm: box.value.trim() }, 'athome-plain-steps');
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { athomeStatusLabel, athomeDays, athomeGroupCode, athomeParseLines, athomeErrorText,
                     athomeChecklistHtml, athomeKeysHtml, athomeRestoreHtml, athomeStepHtml, athomeVerdictLabel,
                     athomeText, athomeAttr, athomeKindLabel, athomePlainHtml, athomeActionHtml, athomeGB };
}
