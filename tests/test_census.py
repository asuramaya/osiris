"""The process census — OS truth beside the graph's beliefs (heinrich's ghost-seat filing,
thread 1fe6811c). Pure: every OS read is a fake here, never a real pgrep or /proc.
"""
from __future__ import annotations

from pathlib import Path

from src.orchestrator.census import live_bodies


def _versions_exe(ver: str = "2.1.210") -> str:
    return f"/home/x/.local/share/claude/versions/{ver}"


def test_maps_bodies_to_projects_by_the_shared_cwd_fold(tmp_path: Path) -> None:
    """The SAME cwd→project fold `resolve_identity` uses: a `.osiris` label wins over the
    folder's basename. Two real claude bodies, two distinct projects."""
    osiris_dir = tmp_path / "osiris"
    osiris_dir.mkdir()
    xxit_dir = tmp_path / "code" / "xxit"
    xxit_dir.mkdir(parents=True)
    cwds = {11: str(osiris_dir), 22: str(xxit_dir)}
    exes = {11: _versions_exe(), 22: _versions_exe()}

    out = live_bodies(pgrep=lambda: [11, 22], read_cwd=cwds.get, read_exe=exes.get)

    assert out == {"osiris": [11], "xxit": [22]}


def test_an_osiris_file_label_overrides_the_folder_basename(tmp_path: Path) -> None:
    """resolve_identity's own precedence: `.osiris`'s `project =` beats the cwd basename — a
    census label must line up with a mount's, not invent its own second mapping."""
    d = tmp_path / "renamed-folder"
    d.mkdir()
    (d / ".osiris").write_text('project = "bytebye"\n')
    (d / ".git").mkdir()  # the repo-root stop the .osiris walk climbs to

    out = live_bodies(pgrep=lambda: [7], read_cwd={7: str(d)}.get,
                      read_exe={7: _versions_exe()}.get)

    assert out == {"bytebye": [7]}


def test_two_bodies_in_one_project_both_count(tmp_path: Path) -> None:
    d = tmp_path / "shared"
    d.mkdir()
    out = live_bodies(pgrep=lambda: [1, 2],
                      read_cwd={1: str(d), 2: str(d)}.get,
                      read_exe={1: _versions_exe(), 2: _versions_exe("2.1.209")}.get)
    assert out == {"shared": [1, 2]}


def test_an_exe_that_is_not_the_claude_binary_is_refused(tmp_path: Path) -> None:
    """The second witness: `pgrep -x claude` matches on a truncated 15-char `comm` field alone,
    which is not proof — an unrelated process (an mcp child, a coincidence) sharing that name
    must not be counted as a body just because its comm string collided."""
    d = tmp_path / "osiris"
    d.mkdir()
    out = live_bodies(pgrep=lambda: [1, 2, 3],
                      read_cwd={1: str(d), 2: str(d), 3: str(d)}.get,
                      read_exe={1: _versions_exe(),          # the real thing
                               2: "/usr/bin/node",           # an mcp child, wrong exe entirely
                               3: "/home/x/.local/share/claude/other/2.1.210"}.get)  # wrong shape
    assert out == {"osiris": [1]}


def test_a_vanished_process_is_skipped_not_counted(tmp_path: Path) -> None:
    """pgrep's snapshot and the /proc reads are not atomic — a pid that exited in between reads
    back None from cwd or exe. Skipped, never crashed on, never miscounted as a ghost."""
    d = tmp_path / "osiris"
    d.mkdir()
    out = live_bodies(pgrep=lambda: [1, 2, 3],
                      read_cwd={1: str(d), 2: None, 3: str(d)}.get,
                      read_exe={1: _versions_exe(), 2: _versions_exe(), 3: None}.get)
    assert out == {"osiris": [1]}


def test_no_claude_bodies_on_the_box_is_an_empty_census() -> None:
    assert live_bodies(pgrep=lambda: [], read_cwd=lambda _: None, read_exe=lambda _: None) == {}


def test_pgrep_failure_degrades_to_empty_never_raises() -> None:
    """A missing `pgrep` binary or any OS-level failure is best-effort, not load-bearing:
    census is a cross-check, never a dependency the rest of the fleet correctness needs."""

    def _boom() -> list[int]:
        raise FileNotFoundError("no pgrep on this box")

    out = live_bodies(pgrep=_boom, read_cwd=lambda _: None, read_exe=lambda _: None)
    assert out == {}


def test_the_real_os_facing_default_never_crashes() -> None:
    """A smoke test of the production seam itself (no fakes): whatever this box's real
    process table looks like, live_bodies() must return a plain dict, never raise."""
    out = live_bodies()
    assert isinstance(out, dict)
