"""src.config.dev_env — the canonical dev-box fallback (task #69, ruling 45b074bf): the ONE
place the fallback values every dev-facing systemd user unit already inlines by hand live,
so a bare `osiris` invocation targets the same instance those units do instead of silently
falling to Settings' prod-shaped 5432/6379 default.
"""
from __future__ import annotations

import pytest
from src.config.dev_env import (
    DEV_DATABASE_URL,
    DEV_REDIS_URL,
    apply_dev_fallback,
    refuse_silent_live_db,
)


def test_fills_both_when_neither_is_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)
    apply_dev_fallback()
    import os

    assert os.environ["DATABASE_URL"] == DEV_DATABASE_URL
    assert os.environ["REDIS_URL"] == DEV_REDIS_URL


def test_never_overrides_an_explicit_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://real:prod@10.0.0.1:5432/osiris")
    monkeypatch.delenv("REDIS_URL", raising=False)
    apply_dev_fallback()
    import os

    assert os.environ["DATABASE_URL"] == "postgresql://real:prod@10.0.0.1:5432/osiris"
    assert os.environ["REDIS_URL"] == DEV_REDIS_URL  # the untouched one still fills


def test_never_overrides_an_explicit_redis_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("REDIS_URL", "redis://real-prod:6379/0")
    apply_dev_fallback()
    import os

    assert os.environ["DATABASE_URL"] == DEV_DATABASE_URL
    assert os.environ["REDIS_URL"] == "redis://real-prod:6379/0"


# ═══ refuse_silent_live_db — thread 86d562e0, the CLASS fix cmd_bootstrap's own guard
# (commit 0f99d49) was scoped away from: the shared check every one-off script now reuses.


def test_refuse_silent_live_db_refuses_with_neither_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("OSIRIS_ALLOW_LIVE", raising=False)
    msg = refuse_silent_live_db("some_script")
    assert msg is not None
    assert "some_script" in msg
    assert "DATABASE_URL" in msg and "OSIRIS_ALLOW_LIVE" in msg


def test_refuse_silent_live_db_allows_an_explicit_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://real:prod@10.0.0.1:5432/osiris")
    monkeypatch.delenv("OSIRIS_ALLOW_LIVE", raising=False)
    assert refuse_silent_live_db("some_script") is None


def test_refuse_silent_live_db_allows_the_explicit_confirmation_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("OSIRIS_ALLOW_LIVE", "1")
    assert refuse_silent_live_db("some_script") is None


def test_refuse_silent_live_db_never_prints_or_exits_itself(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """The caller decides its own exit convention (a CLI return code vs. a bare script's
    SystemExit) — this function only ever returns a message or None."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("OSIRIS_ALLOW_LIVE", raising=False)
    msg = refuse_silent_live_db("some_script")
    assert msg is not None
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == ""
