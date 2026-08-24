"""DshSessionAdapter's discovery walk — the bug found live 2026-08-24 (Thoth msg 5467,
Imhotep's own find while working the provenance reconciliation): the on-disk DSH layout
moved out from under `_session_file_in`'s one-level-only assumption TWICE over, and
neither `discover()` nor `enumerate()` noticed. This was the FIRST test file for this
adapter — a gap the adapter contract note in src/ingest/harness/__init__.py now names
explicitly rather than leaving implicit."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

from src.ingest.harness.dsh import DshSessionAdapter, _cwd_to_slug

_ZSTD = "zstd"


def _write_session(path: Path, *, session_id: str, cwd: str) -> None:
    """A minimal DSH session.jsonl, zstd-compressed, matching what _decompress expects."""
    path.parent.mkdir(parents=True, exist_ok=True)
    jsonl_path = path.with_suffix("")  # strip .zstd for the intermediate plain file
    lines = [json.dumps({"type": "session", "id": session_id, "cwd": cwd})]
    jsonl_path.write_text("\n".join(lines) + "\n")
    subprocess.run([_ZSTD, "-f", "-q", str(jsonl_path), "-o", str(path)], check=True)
    jsonl_path.unlink()


def test_cwd_to_slug_carries_the_real_terminator(tmp_path: Path) -> None:
    # The trailing '--' is a load-bearing terminator, not decoration — see the function's
    # own docstring for the live incident this specimen guards against regressing.
    assert _cwd_to_slug("/home/user/code/project") == "--home-user-code-project--"
    assert _cwd_to_slug("/home/user/code/dsh-deepseek-harness") == (
        "--home-user-code-dsh-deepseek-harness--")


def test_enumerate_finds_a_session_nested_one_level_under_its_slug(tmp_path: Path) -> None:
    root = tmp_path / "sessions"
    _write_session(
        root / "--home-user-code-proj--" / "session-aaaaaaaa-0000-0000-0000-000000000000"
        / "session.jsonl.zstd",
        session_id="session-aaaaaaaa-0000-0000-0000-000000000000",
        cwd="/home/user/code/proj")
    locs = list(DshSessionAdapter().enumerate(root=root))
    assert len(locs) == 1
    assert locs[0].anchor_sid == "aaaaaaaa"
    assert locs[0].project == "proj"


def test_enumerate_finds_the_old_flat_layout_too(tmp_path: Path) -> None:
    # Backward compat: a slug dir that STILL holds its .zstd directly (pre-nesting
    # sessions, if any survive on a real box) must not go dark just because the new
    # nested shape is now also handled.
    root = tmp_path / "sessions"
    _write_session(
        root / "--home-user-code-proj--" / "session.jsonl.zstd",
        session_id="session-bbbbbbbb-0000-0000-0000-000000000000",
        cwd="/home/user/code/proj")
    locs = list(DshSessionAdapter().enumerate(root=root))
    assert len(locs) == 1
    assert locs[0].anchor_sid == "bbbbbbbb"


def test_enumerate_finds_every_session_under_a_slug_with_more_than_one(tmp_path: Path) -> None:
    # The exact shape found live: a single project slug carrying TWO nested sessions —
    # the bug this file exists to pin returned only the first, or none at all.
    root = tmp_path / "sessions"
    slug = root / "--home-user-code-proj--"
    _write_session(
        slug / "session-aaaaaaaa-0000-0000-0000-000000000000" / "session.jsonl.zstd",
        session_id="session-aaaaaaaa-0000-0000-0000-000000000000",
        cwd="/home/user/code/proj")
    _write_session(
        slug / "cccccccc-0000-0000-0000-000000000000" / "session.jsonl.zstd",
        session_id="cccccccc-0000-0000-0000-000000000000",
        cwd="/home/user/code/proj")
    locs = list(DshSessionAdapter().enumerate(root=root))
    assert {loc.anchor_sid for loc in locs} == {"aaaaaaaa", "cccccccc"}


def test_discover_finds_a_nested_session_for_a_matching_cwd(tmp_path: Path) -> None:
    root = tmp_path / "sessions"
    _write_session(
        root / "--home-user-code-proj--" / "session-dddddddd-0000-0000-0000-000000000000"
        / "session.jsonl.zstd",
        session_id="session-dddddddd-0000-0000-0000-000000000000",
        cwd="/home/user/code/proj")
    # discover() doesn't take `root` for the sessions dir (it always resolves via the
    # module-level _DSH_SESSIONS) — patch that constant for this one call.
    import src.ingest.harness.dsh as dsh_mod
    original = dsh_mod._DSH_SESSIONS
    dsh_mod._DSH_SESSIONS = root
    try:
        loc = DshSessionAdapter().discover(cwd="/home/user/code/proj", job_dir=None)
    finally:
        dsh_mod._DSH_SESSIONS = original
    assert loc is not None
    assert loc.anchor_sid == "dddddddd"


def test_discover_picks_the_most_recently_modified_session_when_a_slug_has_several(
    tmp_path: Path,
) -> None:
    import os
    import time

    root = tmp_path / "sessions"
    slug = root / "--home-user-code-proj--"
    older = slug / "session-eeeeeeee-0000-0000-0000-000000000000" / "session.jsonl.zstd"
    newer = slug / "session-ffffffff-0000-0000-0000-000000000000" / "session.jsonl.zstd"
    _write_session(older, session_id="session-eeeeeeee-0000-0000-0000-000000000000",
                   cwd="/home/user/code/proj")
    time.sleep(0.05)
    _write_session(newer, session_id="session-ffffffff-0000-0000-0000-000000000000",
                   cwd="/home/user/code/proj")
    assert os.stat(newer).st_mtime >= os.stat(older).st_mtime

    import src.ingest.harness.dsh as dsh_mod
    original = dsh_mod._DSH_SESSIONS
    dsh_mod._DSH_SESSIONS = root
    try:
        loc = DshSessionAdapter().discover(cwd="/home/user/code/proj", job_dir=None)
    finally:
        dsh_mod._DSH_SESSIONS = original
    assert loc is not None
    assert loc.anchor_sid == "ffffffff"
