"""WAVE 27, THE BACKLOG VIEW (Thoth mail 11754, a parity census gap: the console had a lens
for the fleet's own LIVE bodies (/fleet, and its own composition-engine port `fleet-strip`)
but none for its OPEN OBLIGATIONS). Ported the render-hygiene-ratchet way (tests/
test_render_hygiene.py's own THE LAW: a new hand-rolled Python/JS renderer is refused; the
answer is a composition op or a Function) -- `obligation_backlog`, already registered and
SUBJECT_FREE, wrapped as its own saved composition (`BACKLOG`/`DEFAULT_COMPOSITIONS["backlog"]`
in compositions.py, tested end to end in tests/test_compositions.py) and surfaced through the
Ctrl+K power-tools palette, the same discoverability convention every other Function-only tool
(Graph Lint, The Wall, Overhead, ...) already uses -- no dedicated nav pill, matching
fleet-strip's own precedent (no bespoke UI code needed at all).
"""
from __future__ import annotations

from pathlib import Path

_CONSOLE_JS = (Path(__file__).parent.parent / "src" / "ui" / "static" / "console.js").read_text()


def test_backlog_power_tool_is_registered_and_runs_the_saved_composition() -> None:
    body = _CONSOLE_JS.split("const POWER_TOOLS = [", 1)[1][:2000]
    assert "{ label: 'Backlog', hint: 'Open obligations by project and by seat', " \
        "run: () => runTool('backlog') }," in body


def test_backlog_power_tool_never_declares_its_own_category() -> None:
    # no `cat:` — same as every other bare Function-only tool (Graph Lint, The Wall,
    # Overhead, ...); only Navigation/Compositions/Admin entries carry one.
    body = _CONSOLE_JS.split("{ label: 'Backlog',", 1)[1].split("},", 1)[0]
    assert "cat:" not in body
