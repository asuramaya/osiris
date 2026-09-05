"""obligation a867ae37 (the fleet-wide ENOSPC incident): tests/conftest.py's own
pytest_configure used to hardcode `/tmp/pt-<pid>` as pytest's basetemp — a literal path,
never derived from `$TMPDIR` — so the house's own documented gate convention
("TMPDIR=/var/tmp/osiris-scratch .venv/bin/pytest ...") never actually protected /tmp's
inode budget for pytest's OWN tmp_path fixture output, only for raw tempfile.mkdtemp()
calls elsewhere. Confirmed live: with TMPDIR correctly set, a real run still created
/tmp/pt-<pid> and NOT a directory under /var/tmp/osiris-scratch, before this fix.

_default_basetemp() is proven directly here (extracted from pytest_configure so this
test never has to spawn a nested pytest or trigger conftest's own PostgresContainer
startup path) — the same "prove the mechanism against synthetic inputs" discipline this
house's other gates already run on.
"""
from __future__ import annotations

import os

import pytest

from tests.conftest import _default_basetemp


def test_default_basetemp_honors_an_explicit_tmpdir(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TMPDIR", "/var/tmp/osiris-scratch")
    base = _default_basetemp()
    assert base.startswith("/var/tmp/osiris-scratch/pt-")
    assert base.endswith(str(os.getpid()))


def test_default_basetemp_defaults_to_var_tmp_not_tmp(monkeypatch: pytest.MonkeyPatch) -> None:
    """THE ROOT-CAUSE FIX: absent any TMPDIR at all, this must land on real disk
    (/var/tmp, no fixed inode ceiling) rather than the constrained tmpfs (/tmp,
    1,048,576 inodes fleet-wide) — the incident's own exposure, closed by default rather
    than left to depend on every caller remembering to set TMPDIR."""
    monkeypatch.delenv("TMPDIR", raising=False)
    base = _default_basetemp()
    assert base.startswith("/var/tmp/pt-")
    assert not base.startswith("/tmp/")


def test_default_basetemp_is_pid_keyed() -> None:
    """The AF_UNIX sun_path-length fix (msg 2261) this function inherits unchanged: a
    short, PID-keyed segment, never pytest's own ever-growing counter shared fleet-wide."""
    base = _default_basetemp()
    assert base.endswith(f"pt-{os.getpid()}")
