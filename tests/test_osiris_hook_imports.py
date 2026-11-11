"""The unified hook runs before every osiris tool call, at every turn end and on every status
line refresh, so what it imports is a cost every agent pays on every turn. The PreToolUse
anchor is a pure stdin filter with no network, so it must not load urllib, subprocess, socket or
asyncpg; the Stop hook past the self-compact threshold must not import asyncpg either (it used
to, through settle.py, for a two-line pure function: about 100 ms a Stop)."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HOOK = ROOT / "scripts" / "osiris_hook.py"
HEAVY = ("urllib.request", "urllib.error", "subprocess", "socket", "asyncpg", "http.client",
         "src.orchestrator.settle")


def _loaded_after(subcommand: str, payload: dict[str, object], extra: str = "") -> set[str]:
    """Run the hook's own main() in a child and report which heavy modules got imported."""
    code = (
        "import json, runpy, sys\n"
        f"sys.argv = [{str(HOOK)!r}, {subcommand!r}]\n"
        "try:\n"
        f"    runpy.run_path({str(HOOK)!r}, run_name='__main__')\n"
        "except SystemExit:\n"
        "    pass\n"
        f"{extra}"
        f"print(json.dumps(sorted(m for m in {list(HEAVY)!r} if m in sys.modules)))\n")
    out = subprocess.run([sys.executable, "-c", code], input=json.dumps(payload).encode(),
                         capture_output=True, timeout=60, check=True)
    return set(json.loads(out.stdout.decode().splitlines()[-1]))


def test_the_anchor_path_loads_no_network_process_or_database_module() -> None:
    loaded = _loaded_after("anchor", {
        "session_id": "0b3e0000-0000-4000-8000-0000000000bb", "cwd": str(ROOT),
        "tool_name": "mcp__osiris__inbox", "tool_input": {}})
    assert loaded == set(), f"the anchor filter imported {sorted(loaded)}"


def test_the_stop_hooks_box_verdict_needs_no_database_driver() -> None:
    """_missing_boxes is what the Stop hook calls past the self-compact threshold."""
    extra = (
        "import importlib.util\n"
        f"spec = importlib.util.spec_from_file_location('osiris_hook_mod', {str(HOOK)!r})\n"
        "mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)\n"
        "assert mod._missing_boxes({'a': True, 'b': False, 'c': None}) == ['b']\n")
    loaded = _loaded_after("nosuchcmd", {}, extra)
    assert "asyncpg" not in loaded and "src.orchestrator.settle" not in loaded, sorted(loaded)


def test_settle_still_exports_the_two_verdicts_it_used_to_define() -> None:
    from src.orchestrator import settle, settle_boxes

    assert settle.missing_boxes is settle_boxes.missing_boxes
    assert settle.unevaluated_boxes is settle_boxes.unevaluated_boxes
    assert settle.missing_boxes({"x": False, "y": None, "z": True}) == ["x"]
    assert settle.unevaluated_boxes({"x": False, "y": None, "z": True}) == ["y"]
