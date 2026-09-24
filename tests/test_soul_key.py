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


@pytest.fixture(autouse=True)
def _redirect_restore_drill_receipts(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """soul_key_restore_drill now writes a receipt as a side effect (the readiness
    stepper's own "restore test passed" step needs to read one back). Caught live:
    running this file's own test_soul_key_restore_drill_calls_run_drill_for_an_
    explicit_url once left a real ~/.local/state/osiris/restore_drill_receipts.json
    on the machine actually running the tests, contaminating any later live check
    of the readiness route (a stale "repo-good" receipt reading as a real passed
    drill). Every test in this file gets its own scratch file instead, autouse, no
    per-test opt-in to remember."""
    monkeypatch.setenv(
        soul_key._RESTORE_DRILL_RECEIPTS_ENV, str(tmp_path / "restore_drill_receipts.json"))


async def test_soul_key_status_absent(tmp_path, actions: Actions) -> None:
    out = await soul_key.soul_key_status(actions.pool, path=str(tmp_path / "no-such-file"))
    assert out["present"] is False
    assert out["legacy_plaintext_rows"] is None
    # Thoth mail 13006: the live soul_key.rp_id setting, off the registry's own
    # default — surfaced so Seshat's console reads it here instead of hard-coding
    # a second copy that could drift from what a real enrollment used.
    assert out["rp_id"] == "localhost"


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


# --- THE RESTORE-DRILL RECEIPT (the readiness stepper's own "restore test passed"
# step): run_drill itself writes nothing, a bare pass/fail returned to the caller and
# never seen again before this -----------------------------------------------------


async def test_restore_drill_writes_a_passing_receipt(
    actions: Actions, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(soul_key._RESTORE_DRILL_RECEIPTS_ENV, str(tmp_path / "r.json"))
    import scripts.osiris_offbox_restore_drill as drill_module
    monkeypatch.setattr(drill_module, "run_drill", lambda repo_url, **kw: None)

    await soul_key.soul_key_restore_drill(actions.pool, repo_url="repo-a")

    receipts = soul_key.restore_drill_receipts()
    assert "repo-a" in receipts
    assert receipts["repo-a"]["last_passed_at"] is not None
    assert receipts["repo-a"]["last_error"] is None


async def test_restore_drill_writes_a_failing_receipt_never_clobbers_a_prior_pass(
    actions: Actions, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(soul_key._RESTORE_DRILL_RECEIPTS_ENV, str(tmp_path / "r.json"))
    import scripts.osiris_offbox_restore_drill as drill_module
    monkeypatch.setattr(drill_module, "run_drill", lambda repo_url, **kw: None)
    await soul_key.soul_key_restore_drill(actions.pool, repo_url="repo-b")
    first_pass = soul_key.restore_drill_receipts()["repo-b"]["last_passed_at"]

    monkeypatch.setattr(drill_module, "run_drill", lambda repo_url, **kw: "unreachable")
    await soul_key.soul_key_restore_drill(actions.pool, repo_url="repo-b")

    receipt = soul_key.restore_drill_receipts()["repo-b"]
    assert receipt["last_error"] == "unreachable"
    assert receipt["last_passed_at"] == first_pass  # the earlier real pass survives


async def test_soul_key_encrypt_existing_refuses_with_no_key(
    tmp_path, actions: Actions,
) -> None:
    out = await soul_key.soul_key_encrypt_existing(
        actions.pool, path=str(tmp_path / "no-such-file"))
    assert "error" in out
    assert "no encryption key" in out["error"]


async def test_soul_key_encrypt_existing_runs_for_real_not_a_dry_run(
    tmp_path, actions: Actions,
) -> None:
    key_file = tmp_path / "soul.key"
    key_file.write_bytes(Fernet.generate_key())
    out = await soul_key.soul_key_encrypt_existing(actions.pool, path=str(key_file))
    assert "error" not in out
    assert out["dry_run"] is False
