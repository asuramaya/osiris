"""THE LEGIBILITY PASS, TIP 3b (Thoth mail 10953): four flaws from her own live-Chrome
review of TIP 3 (w282, deployed as e81af83).

Three of the four flaws (glyph label de-overlap, no per-object edges at mid, unfiled
rendering) concerned the LOD glyph-disc mechanism itself -- TIP 4 (operator ruling
"DENSITY NOT DISCS," Thoth mail 11011) retires that mechanism wholesale, so those three
fixes are retired along with it; test_legibility_pass_tip3.py (their own home) is deleted
outright and test_legibility_pass_tip4.py re-proves the surviving underlying concerns
(label de-overlap, unfiled exclusion, no-per-object-edges-outside-near) against TIP 4's
own new shape.

The fourth flaw -- the omnibox agent-handle fallback -- is a genuinely separate concern
(search, not the LOD renderer) untouched by TIP 4; its tests stay here, unchanged:

  3. The omnibox agent-handle fallback still said "No matches" on the deployed page despite
     idToNode carrying thousands of matching Agent labels -- the `hits.length === 0` gate
     (plus a second sequential await) was fragile enough that the exact failure mode
     couldn't be pinned down with certainty, so the gate is gone outright: one Promise.all,
     one token check, the client-side scan runs unconditionally once the graph is loaded,
     deduped against whatever the server found.

Same static-source-guard convention as every prior legibility tip file.
"""
from __future__ import annotations

from pathlib import Path

_CONSOLE_JS = (Path(__file__).parent.parent / "src" / "ui" / "static" / "console.js").read_text()


# --- flaw 3: omnibox fallback runs unconditionally, not gated on empty hits -------------

def test_omnibox_agent_scan_runs_unconditionally_not_gated_on_empty_hits() -> None:
    body = _CONSOLE_JS.split("OMNI_SEARCH_TIMER = setTimeout(async () => {", 1)[1][:2000]
    assert "Promise.all([" in body
    assert "if (myToken !== OMNI_SEARCH_TOKEN) return;" in body
    # only ONE token check now -- the old sequential second await/check is gone
    assert body.count("if (myToken !== OMNI_SEARCH_TOKEN) return;") == 1
    assert "if (hits.length === 0) {" not in body


def test_omnibox_agent_scan_dedupes_against_server_hits() -> None:
    body = _CONSOLE_JS.split("OMNI_SEARCH_TIMER = setTimeout(async () => {", 1)[1][:3100]
    assert "const seenIds = new Set(hits.filter(h => h && h.id).map(h => h.id));" in body
    assert "n.type === 'Agent' && !seenIds.has(n.id)" in body


def test_omnibox_agent_scan_still_reads_the_fallback_safe_label() -> None:
    body = _CONSOLE_JS.split("OMNI_SEARCH_TIMER = setTimeout(async () => {", 1)[1][:3100]
    assert "n.label || `${n.type} ${n.id.slice(0, 8)}`" in body
