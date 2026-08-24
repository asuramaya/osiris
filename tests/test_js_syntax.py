"""Validate JS syntax for all static UI files."""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

_STATIC = Path(__file__).resolve().parent.parent / "src" / "ui" / "static"
_JS_FILES = sorted(f for f in _STATIC.glob("*.js")
                   if "vendor" not in str(f) and not f.is_dir())


@pytest.mark.parametrize("path", _JS_FILES, ids=[f.name for f in _JS_FILES])
def test_js_syntax(path: Path) -> None:
    result = subprocess.run(
        ["node", "--check", str(path)],
        capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 0, (
        f"{path.name}: JS syntax error: {result.stderr}"
    )
