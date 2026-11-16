// ── The At Home Panel ───────────────────────────────────────────────────────────────
// Everything that waits only on the operator being there, in one place: the checklist (same
// order and words as `osiris at-home`), the keys that open the vault with their proof dates, a
// recovery code shown once, a second security key, the first offload to the home storage with
// its six step lines, and the result of the restore test. It reads and acts through the
// /at-home/* routes and keeps nothing: a recovery code lives in the page only while it is on
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
  var opts = { method: method || 'GET', headers: {} };
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
  return { status: res.status, body: body || {} };
}

async function athomeStream(path, onLine) {
  var res = await fetch(path, { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' });
  if (!res.ok || !res.body) {
    var body = null;
    try { body = await res.json(); } catch (e) { body = null; }
    onLine({ done: true, ok: false, text: athomeErrorText(body, 'This could not be started (error ' + res.status + ').') });
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
function athomeChecklistHtml(data) {
  var items = (data && data.items) || [];
  if (!items.length) return '<div class="o-empty">Nothing is waiting on you.</div>';
  return '<table class="ee-table"><tbody>' + items.map(function (it) {
    var cmd = it.command
      ? '<div style="margin-top:4px"><code>' + athomeText(it.command) + '</code> ' +
        '<button class="iconbtn" data-copy="' + athomeAttr(it.command) + '" onclick="athomeCopy(this)">Copy</button></div>'
      : '';
    var then = it.then ? '<div class="o-faint" style="margin-top:4px">Then: ' + athomeText(it.then) + '</div>' : '';
    return '<tr><td style="vertical-align:top;width:28px">' + athomeText(it.order) + '</td>' +
      '<td style="vertical-align:top"><strong>' + athomeText(it.title) + '</strong> ' +
      '<span class="o-faint">' + athomeText(athomeStatusLabel(it.status)) + '</span>' +
      '<div class="o-faint" style="margin-top:2px">' + athomeText(it.why) + '</div>' + cmd + then + '</td></tr>';
  }).join('') + '</tbody></table>';
}

function athomeKindLabel(kind) {
  if (kind === 'fido2-prf') return 'Security key';
  if (kind === 'recovery-code') return 'Recovery code';
  return String(kind || 'Key');
}
function athomeKeysHtml(env) {
  if (env && env.error) return '<div class="o-empty">The vault keys could not be read: ' + athomeText(env.error) + '</div>';
  var recipients = (env && env.recipients) || [];
  if (!recipients.length) {
    return '<div class="o-empty">No key is set up yet. Add a recovery code or a security key below.</div>';
  }
  var rows = recipients.map(function (r) {
    var stale = r.stale ? ' <span class="o-faint">(not tested in a while)</span>' : '';
    var prove = r.can_prove === 'no' || !r.can_prove
      ? '<span class="o-faint" title="' + athomeAttr(r.hint || '') + '">' + athomeText(r.hint || 'Test it from the terminal') + '</span>'
      : '<button class="iconbtn" data-id="' + athomeAttr(r.id) + '" data-mode="' + athomeAttr(r.can_prove) +
        '" onclick="athomeProve(this)">Prove this key</button>';
    return '<tr><td>' + athomeText(athomeKindLabel(r.kind)) + '</td>' +
      '<td>' + athomeText(r.label || '') + '</td>' +
      '<td>' + athomeText(athomeWhen(r.proved_at)) + stale + '</td>' +
      '<td>' + athomeText(athomeWhen(r.opened_at)) + '</td>' +
      '<td>' + prove + '</td></tr>';
  }).join('');
  var warnings = ((env && env.warnings) || []).map(function (w) {
    return '<div style="margin:6px 0">' + athomeText(w) + '</div>';
  }).join('');
  return warnings + '<table class="ee-table"><thead><tr><th>Kind</th><th>Name</th><th>Last proved</th>' +
    '<th>Last opened</th><th></th></tr></thead><tbody>' + rows + '</tbody></table>';
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
    athomeSection('First copy to the home storage', 'Check is read-only. Run makes the first copy and then tests that it can be restored.', 'athome-nas',
      '<div><button class="iconbtn" onclick="athomeNas(\'check\')">Check</button> ' +
      '<label style="font-size:11px;margin-left:8px"><input type="checkbox" id="athome-nas-confirm" onchange="athomeNasButton()" /> I am on the home network</label> ' +
      '<button class="iconbtn" id="athome-nas-run" disabled onclick="athomeNas(\'run\')">Run</button></div>' +
      '<ol id="athome-nas-steps" style="margin:8px 0 0 18px"></ol>') +
    athomeSection('Restore test', 'The last time a copy was restored and checked.', 'athome-restore', '<div class="o-empty">Loading…</div>') +
    '</div>';
  athomeLoadAll();
}

async function athomeLoadAll() {
  var results = await Promise.all([
    athomeCall('/at-home'), athomeCall('/at-home/envelope'), athomeCall('/at-home/restore-test'),
  ]);
  var unavailable = '<div class="o-empty">This is not available yet on this machine.</div>';
  var list = $('athome-list'), keys = $('athome-keys'), restore = $('athome-restore');
  if (list) list.innerHTML = results[0].status === 404 ? unavailable
    : (results[0].status >= 400 ? '<div class="o-empty">' + athomeText(athomeErrorText(results[0].body, 'The list could not be read.')) + '</div>'
       : athomeChecklistHtml(results[0].body));
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

// ── prove a key ──────────────────────────────────────────────────────────────────────
async function athomeProve(btn) {
  var id = btn.getAttribute('data-id'), mode = btn.getAttribute('data-mode');
  btn.disabled = true;
  var done = function (r) {
    btn.disabled = false;
    if (r.status >= 400 || (r.body && r.body.error)) {
      athomeMsg(athomeErrorText(r.body, 'That did not work (error ' + r.status + ').'));
      return;
    }
    athomeMsg('Proved. It opened the vault just now.');
    athomeReloadKeys();
  };
  if (mode === 'browser') {
    athomeMsg('Touch your security key when it blinks.');
    var proof = await athomeBrowserProof(id);
    if (proof.error) { btn.disabled = false; athomeMsg(proof.error); return; }
    done(await athomeCall('/at-home/envelope/prove-browser', 'POST', { recipient_id: id, credential: proof.credential }));
    return;
  }
  athomeMsg('Touch your security key now. This waits up to a minute.');
  done(await athomeCall('/at-home/envelope/prove', 'POST', { recipient_id: id }));
}
async function athomeReloadKeys() {
  var r = await athomeCall('/at-home/envelope');
  var keys = $('athome-keys');
  if (keys && r.status < 400) keys.innerHTML = athomeKeysHtml(r.body);
}
async function athomeBrowserProof(id) {
  if (!(window.PublicKeyCredential && navigator.credentials)) {
    return { error: 'This browser cannot talk to a security key. Use the terminal instead.' };
  }
  var ch = await athomeCall('/at-home/envelope/challenge?recipient_id=' + encodeURIComponent(id));
  if (ch.status >= 400 || !ch.body.publicKey) {
    return { error: athomeErrorText(ch.body, 'The browser check could not be started.') };
  }
  var pk = ch.body.publicKey;
  var options = {
    challenge: b64urlDecodeToBytes(pk.challenge),
    rpId: pk.rpId, timeout: pk.timeout || 60000, userVerification: pk.userVerification || 'discouraged',
    allowCredentials: (pk.allowCredentials || []).map(function (c) { return { type: 'public-key', id: b64urlDecodeToBytes(c.id) }; }),
    extensions: pk.prfEval ? { prf: { eval: { first: b64urlDecodeToBytes(pk.prfEval) } } } : {},
  };
  var cred;
  try {
    cred = await navigator.credentials.get({ publicKey: options });
  } catch (e) {
    return { error: 'The key was not touched in time, or the browser refused it.' };
  }
  var ext = cred.getClientExtensionResults ? cred.getClientExtensionResults() : {};
  var prf = ext && ext.prf && ext.prf.results && ext.prf.results.first;
  return { credential: {
    id: cred.id, rawId: b64urlEncodeBytes(new Uint8Array(cred.rawId)),
    response: {
      clientDataJSON: b64urlEncodeBytes(new Uint8Array(cred.response.clientDataJSON)),
      authenticatorData: b64urlEncodeBytes(new Uint8Array(cred.response.authenticatorData)),
      signature: b64urlEncodeBytes(new Uint8Array(cred.response.signature)),
    },
    prf: prf ? b64urlEncodeBytes(new Uint8Array(prf)) : null,
  } };
}

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
  var r = await athomeCall('/at-home/envelope/add-code', 'POST', {});
  if (r.status >= 400 || !r.body.code) {
    athomeMsg(athomeErrorText(r.body, 'A recovery code could not be made (error ' + r.status + ').'));
    return;
  }
  athomeMsg('');
  ATHOME_CODE_SHOWN = true;
  window.addEventListener('beforeunload', athomeWarnLeave);
  var id = r.body.recipient_id;
  var el = $('athome-code');
  if (!el) return;
  el.innerHTML = '<div style="border:1px solid var(--border2);border-radius:8px;padding:16px;margin:0 0 14px">' +
    '<div style="margin-bottom:8px"><strong>Write this code down now.</strong> It is shown once and cannot be shown again.</div>' +
    '<div id="athome-code-value" style="font-family:var(--mono,monospace);font-size:26px;letter-spacing:2px;line-height:1.5;user-select:all;word-break:break-all">' +
    athomeText(athomeGroupCode(r.body.code)) + '</div>' +
    '<div class="o-faint" style="margin:8px 0">Keep it somewhere safe, away from this computer. Anyone with this code and the vault files can open the vault.</div>' +
    '<button class="iconbtn" data-id="' + athomeAttr(id) + '" onclick="athomeConfirmCode(this)">I wrote it down</button> ' +
    '<span style="margin-left:12px"><label style="font-size:11px"><input type="checkbox" id="athome-cancel-confirm" /> ' +
    'I do not want this code</label> ' +
    '<button class="iconbtn" data-id="' + athomeAttr(id) + '" onclick="athomeCancelCode(this)">Remove this code</button></span></div>';
}
async function athomeConfirmCode(btn) {
  btn.disabled = true;
  var r = await athomeCall('/at-home/envelope/confirm-code', 'POST', { recipient_id: btn.getAttribute('data-id') });
  if (r.status >= 400 || (r.body && r.body.error)) {
    btn.disabled = false;
    athomeMsg(athomeErrorText(r.body, 'The code could not be saved as confirmed. It is still on screen.'));
    return;
  }
  athomeClearCode();
  athomeMsg('Recovery code added. It is gone from this page.');
  athomeReloadKeys();
}
async function athomeCancelCode(btn) {
  var box = $('athome-cancel-confirm');
  if (!box || !box.checked) { athomeMsg('Tick "I do not want this code" first.'); return; }
  btn.disabled = true;
  var r = await athomeCall('/at-home/envelope/cancel-code', 'POST', { recipient_id: btn.getAttribute('data-id') });
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
    '<button class="iconbtn" onclick="athomeAddKey()">Add it</button>';
}
async function athomeAddKey() {
  var box = $('athome-addkey-confirm');
  if (!box || !box.checked) { athomeMsg('Tick that you have the second key first.'); return; }
  athomeMsg('Touch the new key, then touch it again when it blinks. This waits up to a minute.');
  var r = await athomeCall('/at-home/envelope/add-key', 'POST', {});
  if (r.status >= 400 || (r.body && r.body.error)) {
    athomeMsg(athomeErrorText(r.body, 'The key could not be added (error ' + r.status + ').'));
    return;
  }
  athomeMsg('Second key added.');
  var el = $('athome-addkey'); if (el) el.innerHTML = '';
  athomeReloadKeys();
}

// ── the first copy to the home storage ───────────────────────────────────────────────
function athomeNasButton() {
  var box = $('athome-nas-confirm'), btn = $('athome-nas-run');
  if (btn) btn.disabled = !(box && box.checked);
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
      li.innerHTML = '<strong>' + (line.ok ? 'Finished.' : 'Stopped.') + '</strong> ' + athomeText(line.text || '');
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
  athomeLoadAll();
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { athomeStatusLabel, athomeDays, athomeGroupCode, athomeParseLines, athomeErrorText,
                     athomeChecklistHtml, athomeKeysHtml, athomeRestoreHtml, athomeStepHtml,
                     athomeText, athomeAttr, athomeKindLabel };
}
