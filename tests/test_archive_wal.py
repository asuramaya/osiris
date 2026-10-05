"""The WAL archive step compresses segments at the source and recovery can read either form.

`osiris_archive_wal.sh` runs inside the database container as Postgres' archive_command. A
segment is 16 MB on disk however little of it holds data, and a short archive_timeout closes
segments often, so an uncompressed archive grows by a full segment per timeout. These tests run
the real script against a temporary directory.
"""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "osiris_archive_wal.sh"
SEGMENT = "000000010000002C00000025"

needs_zstd = pytest.mark.skipif(shutil.which("zstd") is None, reason="zstd not installed")


def _run(
    src: Path, name: str, dest: Path, *, path: str | None = None,
) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "OSIRIS_WAL_ARCHIVE_DIR": str(dest)}
    if path is not None:
        env["PATH"] = path
    return subprocess.run(["bash", str(SCRIPT), str(src), name],
                          capture_output=True, text=True, env=env, timeout=60)


def _segment(tmp_path: Path, *, payload: bytes = b"wal record " * 50) -> Path:
    src = tmp_path / "pg_wal_segment"
    src.write_bytes(payload + b"\0" * (16 * 1024 * 1024 - len(payload)))
    return src


@needs_zstd
def test_a_segment_is_archived_compressed_and_decompresses_to_the_source(tmp_path: Path) -> None:
    src, dest = _segment(tmp_path), tmp_path / "archive"
    out = _run(src, SEGMENT, dest)
    assert out.returncode == 0, out.stderr
    assert sorted(p.name for p in dest.iterdir()) == [f"{SEGMENT}.zst"]
    assert (dest / f"{SEGMENT}.zst").stat().st_size < 200_000   # a mostly empty 16 MB segment
    restored = subprocess.run(["zstd", "-dcq", str(dest / f"{SEGMENT}.zst")],
                              capture_output=True, check=True, timeout=30).stdout
    assert restored == src.read_bytes()


@needs_zstd
def test_archiving_the_same_segment_twice_is_a_noop(tmp_path: Path) -> None:
    src, dest = _segment(tmp_path), tmp_path / "archive"
    assert _run(src, SEGMENT, dest).returncode == 0
    before = (dest / f"{SEGMENT}.zst").stat().st_mtime_ns
    assert _run(src, SEGMENT, dest).returncode == 0
    assert (dest / f"{SEGMENT}.zst").stat().st_mtime_ns == before


@needs_zstd
def test_a_different_segment_under_an_archived_name_is_refused(tmp_path: Path) -> None:
    dest = tmp_path / "archive"
    assert _run(_segment(tmp_path), SEGMENT, dest).returncode == 0
    other = _segment(tmp_path, payload=b"a different segment " * 40)
    out = _run(other, SEGMENT, dest)
    assert out.returncode == 1 and "refusing to overwrite" in out.stderr


@needs_zstd
def test_a_segment_already_archived_raw_counts_as_archived(tmp_path: Path) -> None:
    """Segments archived before compression existed are raw; a retry must not add a second
    compressed copy beside one."""
    src, dest = _segment(tmp_path), tmp_path / "archive"
    dest.mkdir()
    shutil.copy(src, dest / SEGMENT)
    assert _run(src, SEGMENT, dest).returncode == 0
    assert sorted(p.name for p in dest.iterdir()) == [SEGMENT]


@needs_zstd
def test_history_and_backup_files_stay_raw(tmp_path: Path) -> None:
    dest = tmp_path / "archive"
    for name in ("00000002.history", f"{SEGMENT}.00000028.backup", f"{SEGMENT}.partial"):
        small = tmp_path / "small"
        small.write_text("timeline history\n")
        assert _run(small, name, dest).returncode == 0
    assert sorted(p.name for p in dest.iterdir()) == sorted(
        ["00000002.history", f"{SEGMENT}.00000028.backup", f"{SEGMENT}.partial"])


def test_without_zstd_the_segment_is_archived_raw_not_failed(tmp_path: Path) -> None:
    """Archiving must never fail because compression is unavailable."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for tool in ("mkdir", "stat", "cp", "mv", "rm", "cmp", "bash", "env"):
        found = shutil.which(tool)
        if found:
            (bin_dir / tool).symlink_to(found)
    src, dest = _segment(tmp_path), tmp_path / "archive"
    out = _run(src, SEGMENT, dest, path=str(bin_dir))
    assert out.returncode == 0, out.stderr
    assert sorted(p.name for p in dest.iterdir()) == [SEGMENT]
    assert (dest / SEGMENT).read_bytes() == src.read_bytes()
