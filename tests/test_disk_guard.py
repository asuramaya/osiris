"""The disk guard (vault lane, ruling 39384a87/c53a5fc0 item 5) — `has_room` is pure logic;
the CLI is a thin scan-and-decide shell exercised separately, against a real scratch
directory (no filesystem mocking — `shutil.disk_usage` reads the real one `tmp_path` sits
on, which is exactly what the real script does at runtime)."""
from __future__ import annotations

from pathlib import Path

import pytest
from scripts.osiris_disk_guard import has_room, main


def test_ample_free_space_has_room() -> None:
    assert has_room(free_bytes=10_000, last_size_bytes=1_000) is True


def test_free_space_under_the_last_dump_plus_margin_has_no_room() -> None:
    # last dump is 1000 bytes, default margin 20% -> needs 1200; 1100 free is short
    assert has_room(free_bytes=1_100, last_size_bytes=1_000) is False


def test_free_space_exactly_at_the_margin_boundary_has_room() -> None:
    assert has_room(free_bytes=1_200, last_size_bytes=1_000, margin_frac=0.20) is True


def test_custom_margin_is_honored() -> None:
    # 1000 * 1.10 = 1100 -> 1150 free clears a 10% margin but not a 20% one (1200)
    assert has_room(free_bytes=1_150, last_size_bytes=1_000, margin_frac=0.10) is True
    assert has_room(free_bytes=1_150, last_size_bytes=1_000, margin_frac=0.20) is False


def test_cli_with_no_prior_dump_never_refuses(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    """A brand-new, empty backup directory has nothing to compare against — refusing a
    FIRST dump on an empty disk would be exactly backwards."""
    rc = main([str(tmp_path)])
    assert rc == 0
    assert "nothing to compare" in capsys.readouterr().out


def test_cli_refuses_when_a_dump_this_size_would_not_fit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    (tmp_path / "osiris-20260101-000000.dump").write_bytes(b"x" * 1_000)
    import shutil as shutil_mod

    class _Usage:
        free = 1_100  # short of 1000 * 1.2 = 1200

    monkeypatch.setattr(shutil_mod, "disk_usage", lambda _p: _Usage())
    rc = main([str(tmp_path)])
    assert rc == 1
    assert "REFUSING" in capsys.readouterr().err


def test_cli_proceeds_when_there_is_room(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    (tmp_path / "osiris-20260101-000000.dump").write_bytes(b"x" * 1_000)
    import shutil as shutil_mod

    class _Usage:
        free = 10_000

    monkeypatch.setattr(shutil_mod, "disk_usage", lambda _p: _Usage())
    rc = main([str(tmp_path)])
    assert rc == 0
    assert "OK" in capsys.readouterr().out
