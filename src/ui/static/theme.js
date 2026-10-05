/* Light / dark / system, applied before first paint.
   Loaded as a plain blocking script in <head> so the attribute is on <html> before the first
   frame: no flash of the wrong theme. "system" sets no attribute and lets the stylesheet's
   prefers-color-scheme rule decide; "light" and "dark" pin the choice. The choice is kept in
   localStorage (this browser only, which is the right scope for a display preference). */
(function () {
  var KEY = "osiris.theme";
  var root = document.documentElement;
  var mq = window.matchMedia ? window.matchMedia("(prefers-color-scheme: light)") : null;

  var held = null; // the choice for this page when storage is blocked
  function stored() {
    try {
      var v = window.localStorage.getItem(KEY);
      return v === "light" || v === "dark" ? v : "system";
    } catch (e) { return held || "system"; }
  }
  function effective(mode) {
    mode = mode || stored();
    if (mode === "system") return mq && mq.matches ? "light" : "dark";
    return mode;
  }
  function apply() {
    var mode = stored();
    if (mode === "system") root.removeAttribute("data-theme");
    else root.setAttribute("data-theme", mode);
    var eff = effective(mode);
    // phone browsers tint their own chrome from this tag
    var meta = document.querySelector('meta[name="theme-color"]');
    if (meta) meta.setAttribute("content", eff === "light" ? "#f6f8fa" : "#090d13");
    try {
      window.dispatchEvent(new CustomEvent("osiris-theme", { detail: { mode: mode, effective: eff } }));
    } catch (e) { /* an old engine without CustomEvent just skips the broadcast */ }
  }
  function set(mode) {
    held = mode === "light" || mode === "dark" ? mode : null;
    try {
      if (mode === "light" || mode === "dark") window.localStorage.setItem(KEY, mode);
      else window.localStorage.removeItem(KEY);
    } catch (e) { /* storage blocked: the choice holds for this page only */ }
    apply();
  }

  window.OsirisTheme = { get: stored, effective: effective, set: set, KEY: KEY };
  apply();
  if (mq && mq.addEventListener) mq.addEventListener("change", apply);
  window.addEventListener("storage", function (e) { if (e.key === KEY) apply(); });
})();
