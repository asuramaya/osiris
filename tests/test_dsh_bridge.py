"""The DSH seam, end to end: the adapter against the REAL on-disk layout, the anchor
grammar `session-<uuid>` teaches identity, and the whisper's explicit-anchor door.

Why this file exists (2026-08-23): the adapter shipped assuming a flat
`<slug>/session.jsonl.zstd` layout while the harness actually writes
`<slug>/session-<uuid>/session.jsonl.zstd` — so it had NEVER discovered a single
real session (the soul store carried zero harness='dsh' rows), and a real DSH
session's mount() refused as UNRESOLVABLE because every identity door parsed the
anchor as a Claude jobs path. These tests pin the real layout so it cannot
silently regress again.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from src.actions.core import Actions
from src.ingest.harness import dsh
from src.ingest.harness.dsh import (
    DshSessionAdapter,
    _cwd_to_slug,
    _session_dirs_in,
    _session_file_in,
)
from src.ingest.sessions import _job_id
from src.orchestrator import mounts as mounts_mod
from src.orchestrator.handshake import _derive_job_dir, _sid8, automount, session_end

UUID_A = "d4db540c-6746-4d84-b4a5-04e10106810e"
UUID_B = "11bf3b2e-0000-4000-8000-000000000000"
SESSION_LINES_A = [
    json.dumps({"type": "session", "version": 0, "id": f"session-{UUID_A}",
                "createdAt": 1787284004948, "cwd": "/home/u/code/osiris"}),
    json.dumps({"type": "request/header", "seq": 2, "time": 1787284006000,
                "data": {"header": {"config": {"model": "z-ai/glm-5.3"}}}}),
    json.dumps({"type": "assistant/message", "seq": 3, "time": 1787284007000,
                "data": {"role": "assistant",
                         "message": {"role": "assistant", "model": "z-ai/glm-5.3",
                                     "content": [{"type": "text", "text": "hi"}]}}}),
]
SESSION_LINES_B = [
    json.dumps({"type": "session", "version": 0, "id": f"session-{UUID_B}",
                "createdAt": 1787284100000, "cwd": "/home/u/code/osiris"}),
]


def _make_session(slug_dir: Path, uuid: str, lines: list[str]) -> Path:
    """Lay out ONE real DSH session: <slug>/session-<uuid>/session.jsonl.zstd."""
    sdir = slug_dir / f"session-{uuid}"
    sdir.mkdir(parents=True, exist_ok=True)
    (sdir / "session.jsonl.zstd").write_bytes(b"placeholder")
    return sdir


@pytest.fixture
def dsh_tree(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A ~/.dsh/sessions tree with two sessions in one workspace, decompress patched.

    The zstd BINARY compresses for real in production; tests patch the module's
    _decompress so no binary dependency rides the suite — the LAYOUT (the thing
    that broke) is what these tests pin, not the codec.
    """
    sessions = tmp_path / ".dsh" / "sessions"
    slug = sessions / "--home-u-code-osiris--"
    _make_session(slug, UUID_A, SESSION_LINES_A)
    _make_session(slug, UUID_B, SESSION_LINES_B)
    by_path = {
        str(slug / f"session-{UUID_A}" / "session.jsonl.zstd"): SESSION_LINES_A,
        str(slug / f"session-{UUID_B}" / "session.jsonl.zstd"): SESSION_LINES_B,
    }
    monkeypatch.setattr(dsh, "_decompress",
                        lambda p: by_path.get(str(Path(p).resolve()), []))
    monkeypatch.setenv("HOME", str(tmp_path))
    return slug


# ---- the slug grammar: mirror of the harness's own projectKey ----


@pytest.mark.parametrize(("cwd", "expected"), [
    ("/home/u/code/osiris", "--home-u-code-osiris--"),
    ("/home/u/code/dealer/to/fb", "--home-u-code-dealer-to-fb--"),
    ("/", "--root--"),
    ("/home/u/a b", "--home-u-a~0020b--"),
])
def test_slug_mirrors_the_harness_projectkey(cwd: str, expected: str) -> None:
    assert _cwd_to_slug(cwd) == expected


# ---- the adapter: nested layout + the anchored job_dir lane ----


def test_session_file_in_finds_the_nested_zstd(dsh_tree: Path) -> None:
    session_dir = dsh_tree / f"session-{UUID_A}"
    zst = _session_file_in(session_dir)
    assert zst is not None and zst.name == "session.jsonl.zstd"
    assert _session_file_in(dsh_tree) is not None  # slug dir sees its nested session


def test_session_dirs_in_lists_all_sessions_in_a_workspace(dsh_tree: Path) -> None:
    dirs = [d.name for d, _f in _session_dirs_in(dsh_tree)]
    assert sorted(dirs) == [f"session-{UUID_B}", f"session-{UUID_A}"]


def test_anchored_job_dir_discovery_is_anchored(dsh_tree: Path) -> None:
    job_dir = dsh_tree / f"session-{UUID_A}"
    loc = DshSessionAdapter().discover(cwd="/home/u/code/osiris", job_dir=str(job_dir))
    assert loc is not None
    assert loc.anchored is True                 # its OWN record, not a cwd guess
    assert loc.anchor_sid == UUID_A[:8]         # the uuid, never 'session-'
    assert loc.session_id == f"session-{UUID_A}"
    assert loc.source_path == str(job_dir / "session.jsonl.zstd")


def test_cwd_discovery_picks_a_session_unanchored(dsh_tree: Path) -> None:
    loc = DshSessionAdapter().discover(cwd="/home/u/code/osiris", job_dir=None)
    assert loc is not None
    assert loc.anchored is False                # hottest-guess across the workspace
    assert loc.anchor_sid in (UUID_A[:8], UUID_B[:8])


def test_discover_at_reads_the_session_header(dsh_tree: Path) -> None:
    loc = DshSessionAdapter().discover_at(dsh_tree / f"session-{UUID_A}" / "session.jsonl.zstd")
    assert loc is not None and loc.anchored is True and loc.anchor_sid == UUID_A[:8]


def test_enumerate_walks_the_nested_layout(dsh_tree: Path) -> None:
    locs = list(DshSessionAdapter().enumerate(root=dsh_tree.parent))
    assert {loc.anchor_sid for loc in locs} == {UUID_A[:8], UUID_B[:8]}


def test_a_foreign_job_dir_is_ignored(dsh_tree: Path) -> None:
    # a Claude jobs dir must never be claimed by the DSH adapter
    loc = DshSessionAdapter().discover(cwd="/home/u/code/osiris",
                                       job_dir="/home/u/.claude/jobs/d4db540c")
    assert loc is None or loc.source_path != "/home/u/.claude/jobs/d4db540c"


# ---- the anchor grammar through the identity doors ----


def test_job_id_extracts_the_uuid_not_the_slug() -> None:
    job = f"/home/u/.dsh/sessions/--home-u-code-osiris--/session-{UUID_A}"
    assert _job_id(job) == UUID_A[:8]
    # claude grammar unchanged
    assert _job_id("/home/u/.claude/jobs/c7540517") == "c7540517"


def test_derive_job_dir_globs_the_dsh_session_dir(dsh_tree: Path) -> None:
    out = _derive_job_dir(f"session-{UUID_A}")
    assert out is not None and out.endswith(f"session-{UUID_A}")


def test_derive_job_dir_never_folds_a_dsh_id_into_a_claude_path(
        dsh_tree: Path) -> None:
    # an id with no dir on disk must NOT fall back to ~/.claude/jobs/session-
    assert _derive_job_dir("session-99999999-0000-4000-8000-000000000000") is None
    # claude grammar unchanged
    assert _derive_job_dir("c7540517-abcd").endswith("/c7540517")


def test_sid8_folds_the_session_prefix() -> None:
    assert _sid8(f"session-{UUID_A}") == UUID_A[:8]
    assert _sid8(UUID_A) == UUID_A[:8]           # a bare uuid keeps its own first 8
    assert _sid8("c7540517-abcd") == "c7540517"  # claude shape unchanged


# ---- the whisper's explicit-anchor door (what the DSH bridge plugin calls) ----


@pytest.fixture(autouse=True)
def _fresh_greet_ledger() -> object:
    mounts_mod._GREETS.clear()
    yield
    mounts_mod._GREETS.clear()


async def test_automount_with_an_explicit_dsh_anchor(
        actions: Actions, dsh_tree: Path) -> None:
    job_dir = str(dsh_tree / f"session-{UUID_A}")
    out = await automount(actions, session_id=f"session-{UUID_A}",
                          cwd="/home/u/code/osiris", actor="analyst:operator",
                          job_dir=job_dir)
    # identity anchors on the uuid's first 8, exactly like every other harness
    assert out["agent"] == f"agent:{UUID_A[:8]}"
    assert out["resolved"] is True
    assert out["model"] == "glm-5.3"            # observed off its own transcript
    assert out["job_dir"] == job_dir
    row = await actions.pool.fetchrow(
        "SELECT job_dir, agent_id, model FROM agent_mounts WHERE job_dir=$1", job_dir)
    assert row is not None
    assert row["agent_id"] == f"agent:{UUID_A[:8]}"
    assert row["model"] == "glm-5.3"


async def test_session_end_releases_the_explicit_dsh_anchor(
        actions: Actions, dsh_tree: Path) -> None:
    job_dir = str(dsh_tree / f"session-{UUID_A}")
    await automount(actions, session_id=f"session-{UUID_A}",
                    cwd="/home/u/code/osiris", actor="analyst:operator",
                    job_dir=job_dir)
    # the resume-race grace yields an end that races its own greeting; a real close
    # happens later — age the greeting out by clearing the in-memory ledger
    mounts_mod._GREETS.clear()
    out = await session_end(actions, session_id=f"session-{UUID_A}", job_dir=job_dir)
    assert out.get("released", 0) >= 1


# ---- the rendered whisper: the bridge asks the server for the paragraph ----


class _RouteRequest:
    """The starlette-shaped minimum the /automount route reads: one json body."""

    def __init__(self, body: dict[str, object]) -> None:
        self._body = body

    async def json(self) -> dict[str, object]:
        return self._body


async def test_automount_route_renders_the_whisper_for_the_bridge(
        actions: Actions, dsh_tree: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """THE RENDER DOOR (the DSH bridge's own): a harness plugin cannot run the python
    hook script, so it POSTs render=true and reads `whisper_text` — ONE renderer
    (scripts.osiris_hook.render_whisper), never a TS twin to drift. The honesty
    gate keys on env_job: the bridge passes the job_dir it is ABOUT to bind with."""
    from src import mcp_server

    monkeypatch.setattr(mcp_server, "_pool", actions.pool)
    job_dir = str(dsh_tree / f"session-{UUID_A}")
    resp = await mcp_server.automount_route(_RouteRequest({
        "session_id": f"session-{UUID_A}", "cwd": "/home/u/code/osiris",
        "job_dir": job_dir, "source": "startup",
        "render": True, "env_job": job_dir,
    }))
    import json

    out = json.loads(resp.body)
    assert out["agent"] == f"agent:{UUID_A[:8]}"
    text = out.get("whisper_text")
    assert isinstance(text, str) and text
    # the honesty gate PASSED (env_job == the seated anchor): the bridge-bound
    # session is told it is already mounted, not ordered to mount first
    assert "ALREADY MOUNTED" in text
    assert f"agent:{UUID_A[:8]}" in text
    # the durable anchor travels, so a bounce is recoverable by hand
    assert job_dir in text


async def test_automount_route_without_render_leaves_the_whisper_out(
        actions: Actions, dsh_tree: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Claude's own whisper hook still prints its own text (env_job from the client
    env, not a request field) — the door only opens when a caller asks."""
    from src import mcp_server

    monkeypatch.setattr(mcp_server, "_pool", actions.pool)
    resp = await mcp_server.automount_route(_RouteRequest({
        "session_id": f"session-{UUID_A}", "cwd": "/home/u/code/osiris",
        "job_dir": str(dsh_tree / f"session-{UUID_A}"), "source": "startup",
    }))
    import json

    out = json.loads(resp.body)
    assert "whisper_text" not in out
