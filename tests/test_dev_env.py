"""src.config.dev_env — the canonical dev-box fallback (task #69, ruling 45b074bf): the ONE
place the fallback values every dev-facing systemd user unit already inlines by hand live,
so a bare `osiris` invocation targets the same instance those units do instead of silently
falling to Settings' prod-shaped 5432/6379 default.
"""
from __future__ import annotations

import pytest
from src.config.dev_env import DEV_DATABASE_URL, DEV_REDIS_URL, apply_dev_fallback


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
