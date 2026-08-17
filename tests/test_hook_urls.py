"""Every session-lifecycle hook that posts to the MCP server over loopback must honor an
env override (thread from Sekhmet's #40 finding, decision 66318e03): the hardcoded
127.0.0.1:8790 default only ever reaches a worker on the SAME box, so an off-box Claude
Code session (hyper-docker, the operator's laptop) rang a doorbell nobody heard,
invisibly. Each hook's module-level URL constant is a one-time env read at import — these
tests reload the module under a patched environ to prove the override actually lands and
the loopback default survives when unset."""
from __future__ import annotations

import importlib

import pytest


@pytest.mark.parametrize(
    ("module_name", "env_var", "attr", "default_path"),
    [
        ("scripts.osiris_precompact", "OSIRIS_SWEEP_URL", "SWEEP", "/sweep"),
        ("scripts.osiris_whisper", "OSIRIS_AUTOMOUNT_URL", "AUTOMOUNT", "/automount"),
        ("scripts.osiris_spawn", "OSIRIS_SPAWN_URL", "SPAWN", "/spawn"),
        ("scripts.osiris_sessionend", "OSIRIS_SESSION_END_URL", "SESSION_END",
         "/session-end"),
    ],
)
def test_hook_url_honors_its_env_override(
    module_name: str, env_var: str, attr: str, default_path: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(env_var, raising=False)
    mod = importlib.import_module(module_name)
    mod = importlib.reload(mod)
    assert getattr(mod, attr) == f"http://127.0.0.1:8790{default_path}"

    monkeypatch.setenv(env_var, "http://worker.example:9000/x")
    mod = importlib.reload(mod)
    try:
        assert getattr(mod, attr) == "http://worker.example:9000/x"
    finally:
        monkeypatch.delenv(env_var, raising=False)
        importlib.reload(mod)


def test_precompact_url_actually_used_not_just_the_constant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The module-level constant is read once at import; guard against a future refactor
    that reads os.environ again at call time and silently drops the reload-time value."""
    import scripts.osiris_precompact as precompact

    monkeypatch.setenv("OSIRIS_SWEEP_URL", "http://worker.example:9000/sweep")
    precompact = importlib.reload(precompact)
    posted: dict[str, str] = {}

    class _FakeResp:
        def __enter__(self) -> _FakeResp:
            return self

        def __exit__(self, *a: object) -> None:
            return None

    def _fake_urlopen(req, timeout: float = 0) -> _FakeResp:  # noqa: ANN001
        posted["url"] = req.full_url
        return _FakeResp()

    monkeypatch.setattr(precompact.urllib.request, "urlopen", _fake_urlopen)
    monkeypatch.setattr(
        precompact.sys, "stdin",
        __import__("io").StringIO(
            '{"transcript_path": "/tmp/x.jsonl", "session_id": "s", "trigger": "manual"}'))
    precompact.main()
    assert posted["url"] == "http://worker.example:9000/sweep"

    monkeypatch.delenv("OSIRIS_SWEEP_URL", raising=False)
    importlib.reload(precompact)
