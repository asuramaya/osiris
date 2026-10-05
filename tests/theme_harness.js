const fs = require('fs');
const src = fs.readFileSync(process.argv[2], 'utf8');
function world(initial, osLight) {
  const store = Object.assign({}, initial);
  const attrs = {};
  const listeners = {};
  const events = [];
  const win = {
    localStorage: {
      getItem: k => (k in store ? store[k] : null),
      setItem: (k, v) => { store[k] = String(v); },
      removeItem: k => { delete store[k]; },
    },
    matchMedia: () => ({ matches: osLight, addEventListener: (t, f) => { listeners.mq = f; } }),
    addEventListener: () => {},
    dispatchEvent: e => { events.push(e.detail); return true; },
    CustomEvent: function (n, o) { this.type = n; this.detail = o && o.detail; },
  };
  const doc = {
    documentElement: {
      setAttribute: (k, v) => { attrs[k] = v; },
      removeAttribute: k => { delete attrs[k]; },
      getAttribute: k => (k in attrs ? attrs[k] : null),
    },
    querySelector: () => null,
  };
  new Function('window', 'document', 'CustomEvent', src)(win, doc, win.CustomEvent);
  return { win, attrs, store, events, listeners, setOs: v => { osLight = v; } };
}
const out = {};
let w = world({}, false);
out.fresh = { attr: w.attrs['data-theme'] || null, mode: w.win.OsirisTheme.get(), eff: w.win.OsirisTheme.effective() };
w = world({}, true);
out.freshLightOs = { attr: w.attrs['data-theme'] || null, eff: w.win.OsirisTheme.effective() };
w.win.OsirisTheme.set('dark');
out.pinDark = { attr: w.attrs['data-theme'], stored: w.store['osiris.theme'], eff: w.win.OsirisTheme.effective() };
w = world({ 'osiris.theme': 'light' }, false);
out.restored = { attr: w.attrs['data-theme'], eff: w.win.OsirisTheme.effective(), mode: w.win.OsirisTheme.get() };
w.win.OsirisTheme.set('system');
out.backToSystem = { attr: w.attrs['data-theme'] || null, stored: w.store['osiris.theme'] || null, mode: w.win.OsirisTheme.get() };
w.win.OsirisTheme.set('purple');
out.junkIgnored = { attr: w.attrs['data-theme'] || null, mode: w.win.OsirisTheme.get() };
w = world({ 'osiris.theme': 'bogus' }, false);
out.bogusStored = { mode: w.win.OsirisTheme.get(), attr: w.attrs['data-theme'] || null };
w = world({}, false);
w.win.OsirisTheme.set('light');
out.event = w.events[w.events.length - 1];
// storage that throws must not break the page
const broken = {
  localStorage: { getItem() { throw new Error('x'); }, setItem() { throw new Error('x'); },
                  removeItem() { throw new Error('x'); } },
  matchMedia: () => ({ matches: false, addEventListener() {} }), addEventListener() {},
  dispatchEvent() { return true; },
};
const attrs2 = {};
const doc2 = { documentElement: { setAttribute: (k, v) => { attrs2[k] = v; },
  removeAttribute: k => { delete attrs2[k]; }, getAttribute: () => null }, querySelector: () => null };
new Function('window', 'document', 'CustomEvent', src)(broken, doc2, function () {});
broken.OsirisTheme.set('light');
out.blockedStorage = { mode: broken.OsirisTheme.get(), attr: attrs2['data-theme'] || null };
console.log(JSON.stringify(out));
