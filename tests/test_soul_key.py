"""THE KEY DOOR's own orchestration layer (src.orchestrator.soul_key, Thoth mail
12810/12830): direct coverage of the three pool-backed functions BOTH `osiris
soul-key <action>` and the `/soul-key/*` REST routes call — the exhaustive
key-management/rewrap-mechanics coverage lives in test_soul_crypto.py and
test_soul_store.py; this file only proves the composition itself is wired right.
"""
from __future__ import annotations

import pytest
from cryptography.fernet import Fernet
from src.actions.core import Actions
from src.orchestrator import soul_key


async def test_soul_key_status_absent(tmp_path, actions: Actions) -> None:
    out = await soul_key.soul_key_status(actions.pool, path=str(tmp_path / "no-such-file"))
    assert out["present"] is False
    assert out["legacy_plaintext_rows"] is None


async def test_soul_key_status_present_runs_the_live_census(tmp_path, actions: Actions) -> None:
    key_file = tmp_path / "soul.key"
    key_file.write_bytes(Fernet.generate_key())
    out = await soul_key.soul_key_status(actions.pool, path=str(key_file))
    assert out["present"] is True
    assert isinstance(out["legacy_plaintext_rows"], int)


async def test_soul_key_rotate_finish_refuses_with_nothing_in_flight(
    tmp_path, actions: Actions,
) -> None:
    key_file = tmp_path / "soul.key"
    key_file.write_bytes(Fernet.generate_key())
    out = await soul_key.soul_key_rotate(actions.pool, path=str(key_file), finish=True)
    assert "error" in out
    assert "nothing to finish" in out["error"]


async def test_soul_key_rotate_begin_then_finish_round_trips(
    tmp_path, actions: Actions,
) -> None:
    key_file = tmp_path / "soul.key"
    key_file.write_bytes(Fernet.generate_key())

    begin = await soul_key.soul_key_rotate(actions.pool, path=str(key_file))
    assert "error" not in begin
    legacy_path = tmp_path / "soul.key.legacy"
    assert legacy_path.exists()

    finish = await soul_key.soul_key_rotate(actions.pool, path=str(key_file), finish=True)
    assert "error" not in finish
    assert not legacy_path.exists()


async def test_soul_key_restore_drill_refuses_with_no_repo_configured(actions: Actions) -> None:
    out = await soul_key.soul_key_restore_drill(actions.pool)
    assert "error" in out
    assert "no offbox repository configured" in out["error"]


async def test_soul_key_restore_drill_calls_run_drill_for_an_explicit_url(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def _fake_run_drill(repo_url: str, *, scratch=None) -> str | None:
        calls.append(repo_url)
        return None

    import scripts.osiris_offbox_restore_drill as drill_module
    monkeypatch.setattr(drill_module, "run_drill", _fake_run_drill)

    out = await soul_key.soul_key_restore_drill(actions.pool, repo_url="repo-good")
    assert "error" not in out
    assert out["all_ok"] is True
    assert calls == ["repo-good"]
