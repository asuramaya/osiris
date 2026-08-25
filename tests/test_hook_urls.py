"""Every session-lifecycle hook that posts to the MCP server over loopback must honor an
env override (thread from Sekhmet's #40 finding, decision 66318e03): the hardcoded
127.0.0.1:8790 default only ever reaches a worker on the SAME box, so an off-box Claude
Code session (hyper-docker, the operator's laptop) rang a doorbell nobody heard,
invisibly.

RETARGETED at the #187 retirement (2026-08-25): these cases used to reload seven
per-purpose scripts that no longer exist. osiris_hook.py now owns every lifecycle event
and reads all of them ONCE into a module-level `_URLS` dict at import, so the property
under test is unchanged — a one-time env read whose override must land and whose loopback
default must survive when unset — but there is one module to reload instead of seven."""
from __future__ import annotations

import importlib

import pytest


@pytest.mark.parametrize(
    ("key", "env_var", "default_path"),
    [
        ("statusline", "OSIRIS_HEARTBEAT_URL", "/heartbeat"),
        ("stop", "OSIRIS_STOP_URL", "/stop"),
        ("whisper", "OSIRIS_AUTOMOUNT_URL", "/automount"),
        ("session-end", "OSIRIS_SESSION_END_URL", "/session-end"),
        ("precompact", "OSIRIS_SWEEP_URL", "/sweep"),
        ("spawn", "OSIRIS_SPAWN_URL", "/spawn"),
        ("succession", "OSIRIS_SUCCESSION_URL", "/succession"),
    ],
)
def test_hook_url_honors_its_env_override(
    key: str, env_var: str, default_path: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.osiris_hook as hook

    monkeypatch.delenv(env_var, raising=False)
    hook = importlib.reload(hook)
    assert hook._URLS[key] == f"http://127.0.0.1:8790{default_path}"

    monkeypatch.setenv(env_var, "http://worker.example:9000/x")
    hook = importlib.reload(hook)
    try:
        assert hook._URLS[key] == "http://worker.example:9000/x"
    finally:
        monkeypatch.delenv(env_var, raising=False)
        importlib.reload(hook)


def test_every_url_is_overridable_none_hardcoded() -> None:
    """The parametrize above can only prove the cases it lists. This proves the LIST is
    complete — a new entry added to _URLS without an env override, or without a test case,
    fails here rather than shipping as a silently unreachable off-box hook."""
    import scripts.osiris_hook as hook

    covered = {
        "statusline", "stop", "whisper", "session-end",
        "precompact", "spawn", "succession",
    }
    assert set(hook._URLS) == covered, (
        "scripts/osiris_hook.py's _URLS changed — add the new key to this test AND give it "
        "an os.environ.get() override, or an off-box session cannot reach it.")


def test_precompact_url_actually_used_not_just_the_constant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The module-level constant is read once at import; guard against a future refactor
    that reads os.environ again at call time and silently drops the reload-time value."""
    import scripts.osiris_hook as hook

    monkeypatch.setenv("OSIRIS_SWEEP_URL", "http://worker.example:9000/sweep")
    hook = importlib.reload(hook)
    posted: dict[str, str] = {}

    class _FakeResp:
        def __enter__(self) -> _FakeResp:
            return self

        def __exit__(self, *a: object) -> None:
            return None

        def read(self) -> bytes:
            return b"{}"

    def _fake_urlopen(req, timeout: float = 0) -> _FakeResp:  # noqa: ANN001
        posted["url"] = req.full_url
        return _FakeResp()

    monkeypatch.setattr(hook.urllib.request, "urlopen", _fake_urlopen)
    monkeypatch.setattr(
        hook.sys, "stdin",
        __import__("io").StringIO(
            '{"transcript_path": "/tmp/x.jsonl", "session_id": "s", "trigger": "manual"}'))
    # main() dispatches on sys.argv[1], not a parameter — drive it the way the harness does.
    monkeypatch.setattr(hook.sys, "argv", ["osiris_hook.py", "precompact"])
    hook.main()
    assert posted["url"] == "http://worker.example:9000/sweep", (
        "main() fails OPEN on any handler exception (returns 0), so a missing 'url' here "
        "means the precompact path never reached _post at all.")

    monkeypatch.delenv("OSIRIS_SWEEP_URL", raising=False)
    importlib.reload(hook)
