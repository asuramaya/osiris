"""The process census — OS truth beside the graph's beliefs (heinrich's ghost-seat filing,
thread 1fe6811c). Pure: every OS read is a fake here, never a real pgrep or /proc.
"""
from __future__ import annotations

from pathlib import Path

import pytest
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


def test_the_bare_office_root_is_dropped_never_a_phantom_project(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A body sitting at the bare seat-office CONTAINER (~/.osiris/seats itself, never a
    project) must never be tallied under the literal string "seats" (Thoth's live specimen,
    msg 1888: fleet() reported "seats — 0 live · 3 sessions · 2 os bodies"). This census has
    no agent_id to resolve the real project through the seat — it drops the pid instead of
    mis-tallying it, same honesty resolve_identity already keeps for the exact same cwd."""
    import src.orchestrator.census as census_mod

    fake_root = tmp_path / ".osiris" / "seats"
    fake_root.mkdir(parents=True)
    monkeypatch.setenv("OSIRIS_OFFICE_ROOT", str(fake_root))

    real_office = fake_root / "someseat"
    real_office.mkdir()
    cwds = {11: str(fake_root), 22: str(real_office)}
    exes = {11: _versions_exe(), 22: _versions_exe()}

    out = census_mod.live_bodies(pgrep=lambda: [11, 22], read_cwd=cwds.get, read_exe=exes.get)

    assert out == {"someseat": [22]}  # the container's own body is dropped, not "seats": [11]


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


def test_blindness_is_none_never_an_empty_box(tmp_path: Path) -> None:
    """BLIND IS NOT EMPTY: pgrep failing (None, or raising) must stay distinguishable from
    an honest zero — the door sweep deletes on the strength of 'nobody is home', which only
    an honest census may say. `live_bodies` (a pure cross-check) degrades to {} instead."""
    from src.orchestrator.census import live_bodies_by_cwd

    def _boom() -> list[int] | None:
        raise OSError("pgrep exploded")

    assert live_bodies_by_cwd(
        pgrep=lambda: None, read_cwd=lambda _: None, read_exe=lambda _: None) is None
    assert live_bodies_by_cwd(
        pgrep=_boom, read_cwd=lambda _: None, read_exe=lambda _: None) is None
    assert live_bodies(
        pgrep=lambda: None, read_cwd=lambda _: None, read_exe=lambda _: None) == {}


def test_live_bodies_by_cwd_is_directory_grained(tmp_path: Path) -> None:
    """The sweep's witness: same project label, two directories — an office and its governed
    repo — stay distinct doors; the exe check still refuses the impostor."""
    from src.orchestrator.census import live_bodies_by_cwd

    a = tmp_path / "office"
    a.mkdir()
    b = tmp_path / "repo"
    b.mkdir()
    out = live_bodies_by_cwd(
        pgrep=lambda: [1, 2, 3, 4],
        read_cwd={1: str(a), 2: str(a), 3: str(b), 4: str(b)}.get,
        read_exe={1: _versions_exe(), 2: _versions_exe(), 3: _versions_exe(),
                  4: "/usr/bin/node"}.get)
    assert out == {str(a.resolve()): [1, 2], str(b.resolve()): [3]}


# ═══ THE LIVENESS CONVERGENCE FIX, PIECE B2 (Nebbercracker's monsterhouse report,
# DM 11817/11821): a `claude bg-spare` pre-warmed body is a real, exe-verified claude
# process sitting at a real cwd — cmdline is the only signal that tells it apart from
# an actual occupant. Live specimen: pid 3750764, 14h old, cwd = jenny's own seat
# directory, no turns ever — misread as the occupant by both census functions.

def test_is_bg_spare_matches_the_hook_s_own_probe() -> None:
    from src.orchestrator.census import _is_bg_spare

    assert _is_bg_spare(b"claude\x00bg-spare\x00--bg-spare\x00/tmp/cc-daemon/spare.sock")
    assert not _is_bg_spare(b"claude\x00--bg\x00--session-id\x00abc123")
    assert not _is_bg_spare(b"")


def test_live_bodies_excludes_a_bg_spare_process(tmp_path: Path) -> None:
    d = tmp_path / "jenny-office"
    d.mkdir()
    out = live_bodies(
        pgrep=lambda: [1, 2],
        read_cwd={1: str(d), 2: str(d)}.get,
        read_exe={1: _versions_exe(), 2: _versions_exe()}.get,
        read_cmdline={1: b"claude\x00--bg\x00--session-id\x00abc",
                     2: b"claude\x00bg-spare\x00--bg-spare\x00/tmp/x.sock"}.get)
    assert out == {"jenny-office": [1]}


def test_live_bodies_by_cwd_excludes_a_bg_spare_process(tmp_path: Path) -> None:
    """The exact live shape: a spare sitting in a seat's own office directory must never
    read as the occupant `_resume_occupancy_gate`'s own 'foreign' signal trusts."""
    from src.orchestrator.census import live_bodies_by_cwd

    office = tmp_path / "seats" / "jenny"
    office.mkdir(parents=True)
    out = live_bodies_by_cwd(
        pgrep=lambda: [3750764],
        read_cwd={3750764: str(office)}.get,
        read_exe={3750764: _versions_exe()}.get,
        read_cmdline={3750764: b"claude\x00bg-spare\x00--bg-spare\x00"
                               b"/tmp/cc-daemon-1000/x/spare/1.claim.sock"}.get)
    assert out == {}


def test_live_bodies_by_cwd_still_counts_a_real_occupant_beside_a_spare(
    tmp_path: Path,
) -> None:
    from src.orchestrator.census import live_bodies_by_cwd

    office = tmp_path / "seats" / "jenny"
    office.mkdir(parents=True)
    out = live_bodies_by_cwd(
        pgrep=lambda: [1, 2],
        read_cwd={1: str(office), 2: str(office)}.get,
        read_exe={1: _versions_exe(), 2: _versions_exe()}.get,
        read_cmdline={1: b"claude\x00bg-spare\x00--bg-spare\x00/tmp/x.sock",
                     2: b"claude\x00--bg\x00--session-id\x00real"}.get)
    assert out == {str(office.resolve()): [2]}


def test_live_bodies_by_cwd_cmdline_default_is_empty_bytes_never_a_false_spare(
    tmp_path: Path,
) -> None:
    """The real `_proc_cmdline` default reads empty bytes for a fake/vanished pid (caught
    OSError) — never mistaken for a spare match, same vanished-process race every sibling
    probe here absorbs."""
    d = tmp_path / "osiris"
    d.mkdir()
    out = live_bodies(pgrep=lambda: [999999999],
                      read_cwd={999999999: str(d)}.get,
                      read_exe={999999999: _versions_exe()}.get)
    assert out == {"osiris": [999999999]}
