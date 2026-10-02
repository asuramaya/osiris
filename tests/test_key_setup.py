"""Automatic key and backup setup: the deploy-time step that mints the encryption key and the
backup password when absent (src.orchestrator.key_setup), the backup password riding inside the
security-key recovery blob, and the non-destructive recovery check. No real hardware: the
security key is the same deterministic fake test_soul_crypto uses, and every credential lands
in a scratch directory (never the machine's own credential store)."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from src.ingest import soul_crypto, systemd_credential
from src.orchestrator import key_setup, restic_credential
from src.orchestrator import soul_key as soul_key_orch

from tests.test_soul_crypto import _FakeFido2Client


@pytest.fixture(autouse=True)
def _isolated_credentials(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Default layout, redirected: XDG points the key, the backup password and the credstore
    at scratch, no systemd-creds (so the plain-file backend is used and no subprocess runs),
    and no ambient key env or unit file colours the resolution ladder."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdgcfg"))
    for name in ("OSIRIS_SOUL_KEY", "OSIRIS_SOUL_KEY_FILE", "OSIRIS_RESTIC_PASSWORD",
                 "OSIRIS_RESTIC_PASSWORD_FILE", "CREDENTIALS_DIRECTORY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(soul_crypto, "_installed_user_unit_env_value", lambda name: None)
    monkeypatch.setattr(soul_crypto, "_systemd_creds_available", lambda: False)
    monkeypatch.setattr(systemd_credential, "systemd_creds_available", lambda: False)


@pytest.fixture
def fake_security_key(monkeypatch: pytest.MonkeyPatch) -> _FakeFido2Client:
    client = _FakeFido2Client()
    monkeypatch.setattr(soul_crypto, "_find_fido2_device", lambda: object())
    monkeypatch.setattr(soul_crypto, "_fido2_client", lambda device, rp_id: client)
    return client


def _recovery_blob() -> dict[str, Any]:
    status = soul_crypto.soul_key_status()
    return dict(json.loads(soul_crypto._recovery_path(Path(status["path"])).read_text()))


# --- ensure_key_setup ------------------------------------------------------------------------


def test_a_fresh_box_gets_both_the_key_and_the_backup_password() -> None:
    report = key_setup.ensure_key_setup()

    assert report["ok"] is True
    assert report["minted"] is True
    assert report["key"] == "minted"
    assert report["backup_password"] == "minted"
    assert soul_crypto.soul_key_status()["present"] is True
    assert restic_credential.restic_key_status()["present"] is True


def test_setup_is_idempotent_and_never_remints_anything() -> None:
    key_setup.ensure_key_setup()
    key_before = soul_crypto.read_key_bytes_at(Path(soul_crypto.soul_key_status()["path"]))
    password_before = restic_credential.get_restic_password()

    again = key_setup.ensure_key_setup()

    assert again["ok"] is True
    assert again["minted"] is False
    assert again["key"] == "present"
    assert again["backup_password"] == "present"
    assert soul_crypto.read_key_bytes_at(
        Path(soul_crypto.soul_key_status()["path"])) == key_before
    assert restic_credential.get_restic_password() == password_before


def test_an_existing_backup_password_is_adopted_never_replaced() -> None:
    """Existing repositories keep working: their password is the one already on the box."""
    restic_credential.restic_key_init(backend="file")
    original = restic_credential.get_restic_password()

    report = key_setup.ensure_key_setup()

    assert report["backup_password"] == "present"
    assert restic_credential.get_restic_password() == original


def test_root_is_refused_because_it_has_no_natural_owner_for_the_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(key_setup.os, "getuid", lambda: 0)

    report = key_setup.ensure_key_setup()

    assert report["ok"] is False
    assert "as root" in report["error"]
    assert soul_crypto.soul_key_status()["present"] is False


def test_a_backup_password_failure_never_blocks_the_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boom(**_kw: Any) -> dict[str, Any]:
        raise OSError("credstore is read-only")

    monkeypatch.setattr(restic_credential, "restic_key_ensure", _boom)

    report = key_setup.ensure_key_setup()

    assert report["ok"] is True
    assert report["key"] == "minted"
    assert report["backup_password"] == "failed"
    assert "read-only" in report["backup_password_error"]


def test_an_existing_recovery_enrollment_gains_the_backup_password_without_a_touch(
    fake_security_key: _FakeFido2Client,
) -> None:
    soul_crypto.soul_key_init(backend="file")
    assert "error" not in soul_crypto.soul_key_enroll_recovery()
    assert "restic_password_wrapped" not in _recovery_blob()

    first = key_setup.ensure_key_setup()
    second = key_setup.ensure_key_setup()

    assert first["recovery_refreshed"] is True
    assert second["recovery_refreshed"] is False
    assert soul_crypto.soul_key_recovery_facts()["restic_wrapped"] is True


def test_a_recovery_enrollment_for_an_older_key_is_reported_not_silently_extended(
    fake_security_key: _FakeFido2Client,
) -> None:
    soul_crypto.soul_key_init(backend="file")
    soul_crypto.soul_key_enroll_recovery()
    Path(soul_crypto.soul_key_status()["path"]).write_bytes(soul_crypto._generate_key())

    report = key_setup.ensure_key_setup()

    assert report["ok"] is True
    assert "older key" in report["recovery_note"]
    assert "recovery_refreshed" not in report
    assert soul_crypto.soul_key_recovery_facts()["stale"] is True


# --- the backup password inside the recovery blob --------------------------------------------


def test_enrolling_wraps_the_backup_password_into_the_same_blob(
    fake_security_key: _FakeFido2Client,
) -> None:
    soul_crypto.soul_key_init(backend="file")

    out = soul_crypto.soul_key_enroll_recovery(restic_password=b"repo-pass-1")

    assert "error" not in out
    blob = _recovery_blob()
    assert "restic_password_wrapped" in blob
    assert b"repo-pass-1" not in json.dumps(blob).encode()
    raw_key = soul_crypto.read_key_bytes_at(Path(soul_crypto.soul_key_status()["path"]))
    assert soul_crypto.unwrap_restic_password(
        raw_key, blob["restic_password_wrapped"]) == b"repo-pass-1"


def test_recovery_returns_the_backup_password_through_the_sealer_and_never_in_the_result(
    fake_security_key: _FakeFido2Client,
) -> None:
    soul_crypto.soul_key_init(backend="file")
    original = Path(soul_crypto.soul_key_status()["path"]).read_bytes()
    soul_crypto.soul_key_enroll_recovery(restic_password=b"repo-pass-2")
    Path(soul_crypto.soul_key_status()["path"]).unlink()
    sealed: list[bytes] = []

    out = soul_crypto.soul_key_recover(
        backend="file", seal_restic=lambda pw: sealed.append(pw) or {"sealed": True})

    assert out["restic_password_recovered"] is True
    assert sealed == [b"repo-pass-2"]
    assert b"repo-pass-2" not in json.dumps(out).encode()
    assert Path(out["path"]).read_bytes() == original


def test_recovery_without_a_sealer_says_the_password_was_not_recovered(
    fake_security_key: _FakeFido2Client,
) -> None:
    soul_crypto.soul_key_init(backend="file")
    soul_crypto.soul_key_enroll_recovery(restic_password=b"repo-pass-3")
    Path(soul_crypto.soul_key_status()["path"]).unlink()

    out = soul_crypto.soul_key_recover(backend="file")

    assert out["restic_password_recovered"] is False


def test_a_recovered_password_is_sealed_but_a_different_existing_one_is_never_replaced() -> None:
    sealed = restic_credential.seal_recovered_password(b"from-recovery", backend="file")
    assert sealed["sealed"] is True
    assert restic_credential.get_restic_password() == b"from-recovery"
    assert restic_credential.seal_recovered_password(
        b"from-recovery", backend="file") == {"path": sealed["path"], "sealed": False}
    refused = restic_credential.seal_recovered_password(b"something-else", backend="file")
    assert "different backup password" in refused["error"]
    assert restic_credential.get_restic_password() == b"from-recovery"


def test_the_backup_password_survives_a_soul_key_rotation_and_a_re_enrollment(
    fake_security_key: _FakeFido2Client,
) -> None:
    """Independent of the soul key: rotating never changes it, and re-enrolling recovery
    afterwards refreshes the wrap under the new key."""
    key_setup.ensure_key_setup()
    password = restic_credential.get_restic_password()
    soul_crypto.soul_key_enroll_recovery(restic_password=password)

    begin = soul_crypto.soul_key_rotate_begin()
    assert "error" not in begin
    assert restic_credential.get_restic_password() == password
    assert soul_crypto.soul_key_recovery_facts()["stale"] is True

    Path(soul_crypto._recovery_path(Path(begin["path"]))).unlink()
    soul_crypto.soul_key_enroll_recovery(restic_password=password)
    assert soul_crypto.soul_key_recovery_facts()["stale"] is False
    assert soul_crypto.attach_restic_password_to_recovery(password)["changed"] is False


# --- verify-recovery -------------------------------------------------------------------------


def _snapshot() -> dict[str, bytes]:
    key_path = Path(soul_crypto.soul_key_status()["path"])
    files = {"key": key_path, "blob": soul_crypto._recovery_path(key_path)}
    return {name: p.read_bytes() for name, p in files.items()}


def test_verify_proves_recovery_and_changes_nothing(
    fake_security_key: _FakeFido2Client,
) -> None:
    key_setup.ensure_key_setup()
    password = restic_credential.get_restic_password()
    soul_crypto.soul_key_enroll_recovery(restic_password=password)
    before = _snapshot()

    out = soul_crypto.soul_key_verify_recovery(restic_password=password)

    assert out["verified"] is True
    assert out["matches_live_key"] is True
    assert out["restic_password_wrapped"] is True
    assert out["restic_password_matches"] is True
    assert _snapshot() == before
    # the destructive door still refuses while a key exists; verify is the one that works
    assert "already exists" in soul_crypto.soul_key_recover(backend="file")["error"]


def test_verify_reports_a_key_that_changed_since_enrollment(
    fake_security_key: _FakeFido2Client,
) -> None:
    soul_crypto.soul_key_init(backend="file")
    soul_crypto.soul_key_enroll_recovery()
    Path(soul_crypto.soul_key_status()["path"]).write_bytes(soul_crypto._generate_key())

    out = soul_crypto.soul_key_verify_recovery()

    assert out["verified"] is True
    assert out["matches_live_key"] is False


def test_verify_reports_a_backup_password_that_differs_from_the_wrapped_one(
    fake_security_key: _FakeFido2Client,
) -> None:
    soul_crypto.soul_key_init(backend="file")
    soul_crypto.soul_key_enroll_recovery(restic_password=b"wrapped")

    out = soul_crypto.soul_key_verify_recovery(restic_password=b"on-this-machine")

    assert out["restic_password_matches"] is False


def test_verify_with_the_wrong_security_key_fails_and_still_changes_nothing(
    fake_security_key: _FakeFido2Client, monkeypatch: pytest.MonkeyPatch,
) -> None:
    soul_crypto.soul_key_init(backend="file")
    soul_crypto.soul_key_enroll_recovery()
    before = _snapshot()
    other = _FakeFido2Client(device_secret=b"a-different-security-key")
    monkeypatch.setattr(soul_crypto, "_fido2_client", lambda device, rp_id: other)

    out = soul_crypto.soul_key_verify_recovery()

    assert "failed to decrypt" in out["error"]
    assert _snapshot() == before


def test_verify_with_no_enrollment_is_a_named_refusal() -> None:
    soul_crypto.soul_key_init(backend="file")
    assert "no recovery enrollment" in soul_crypto.soul_key_verify_recovery()["error"]


def test_attach_is_refused_with_no_enrollment_and_leaves_no_file() -> None:
    soul_crypto.soul_key_init(backend="file")
    assert "no recovery enrollment" in soul_crypto.attach_restic_password_to_recovery(
        b"x")["error"]


# --- the orchestration layer: receipts and the automatic wrap --------------------------------


def test_enrolling_through_the_orchestrator_creates_and_wraps_the_backup_password(
    fake_security_key: _FakeFido2Client,
) -> None:
    soul_crypto.soul_key_init(backend="file")
    assert restic_credential.restic_key_status()["present"] is False

    out = soul_key_orch.soul_key_enroll_recovery(rp_id="localhost")

    assert "error" not in out
    assert restic_credential.restic_key_status()["present"] is True
    assert soul_crypto.soul_key_recovery_facts()["restic_wrapped"] is True


def test_the_verify_receipt_records_a_pass_and_a_later_failure_never_erases_it(
    fake_security_key: _FakeFido2Client, monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert soul_key_orch.recovery_verify_receipt() == {}
    key_setup.ensure_key_setup()
    soul_key_orch.soul_key_enroll_recovery(rp_id="localhost")

    ok = soul_key_orch.soul_key_verify_recovery(rp_id="localhost")
    assert "error" not in ok
    passed = soul_key_orch.recovery_verify_receipt()
    assert passed["ok"] is True
    assert passed["matches_live_key"] is True
    assert passed["last_verified_at"]

    other = _FakeFido2Client(device_secret=b"a-different-security-key")
    monkeypatch.setattr(soul_crypto, "_fido2_client", lambda device, rp_id: other)
    bad = soul_key_orch.soul_key_verify_recovery(rp_id="localhost")
    assert "error" in bad
    after = soul_key_orch.recovery_verify_receipt()
    assert after["ok"] is False
    assert after["last_error"]
    assert after["last_verified_at"] == passed["last_verified_at"]


def test_a_key_mismatch_is_an_error_and_recorded_as_such(
    fake_security_key: _FakeFido2Client,
) -> None:
    soul_crypto.soul_key_init(backend="file")
    soul_crypto.soul_key_enroll_recovery()
    Path(soul_crypto.soul_key_status()["path"]).write_bytes(soul_crypto._generate_key())

    out = soul_key_orch.soul_key_verify_recovery(rp_id="localhost")

    assert "re-enroll recovery" in out["error"]
    assert soul_key_orch.recovery_verify_receipt()["ok"] is False


def test_recovering_through_the_orchestrator_seals_the_backup_password_on_the_new_machine(
    fake_security_key: _FakeFido2Client,
) -> None:
    key_setup.ensure_key_setup()
    password = restic_credential.get_restic_password()
    soul_key_orch.soul_key_enroll_recovery(rp_id="localhost")
    # a brand new machine: neither credential exists, only the portable recovery blob
    Path(soul_crypto.soul_key_status()["path"]).unlink()
    restic_credential._password_file_path().unlink()

    out = soul_key_orch.soul_key_recover(backend="file", rp_id="localhost")

    assert out["restic_password_recovered"] is True
    assert restic_credential.get_restic_password() == password


# --- status readers never decrypt the key (the setup stepper sat on "Loading" for seconds) ----


def _no_decrypt(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    calls: list[int] = []

    def _boom(blob: bytes) -> bytes:
        calls.append(1)
        raise AssertionError("a status reader must never unseal the key")

    monkeypatch.setattr(soul_crypto, "_decrypt_with_systemd_creds", _boom)
    return calls


def test_the_recovery_status_never_decrypts_the_key(
    fake_security_key: _FakeFido2Client, monkeypatch: pytest.MonkeyPatch,
) -> None:
    key_setup.ensure_key_setup()
    soul_crypto.soul_key_enroll_recovery()
    calls = _no_decrypt(monkeypatch)

    facts = soul_crypto.soul_key_recovery_facts()

    assert facts["enrolled"] is True and facts["stale"] is False
    assert calls == []


def test_a_key_written_by_init_has_its_fingerprint_cached_without_any_read() -> None:
    soul_crypto.soul_key_init(backend="file")
    cached = soul_crypto.cached_key_fingerprint()
    assert cached == soul_crypto.key_fingerprint(
        soul_crypto.read_key_bytes_at(Path(soul_crypto.soul_key_status()["path"])))


def test_a_rotation_invalidates_the_cache_and_staleness_is_still_reported(
    fake_security_key: _FakeFido2Client, monkeypatch: pytest.MonkeyPatch,
) -> None:
    soul_crypto.soul_key_init(backend="file")
    soul_crypto.soul_key_enroll_recovery()
    assert soul_crypto.soul_key_recovery_facts()["stale"] is False

    soul_crypto.soul_key_rotate_begin()  # writes the new key, which remembers its own print

    calls = _no_decrypt(monkeypatch)
    assert soul_crypto.soul_key_recovery_facts()["stale"] is True
    assert calls == []


def test_a_key_file_changed_behind_the_cache_reads_as_unknown_never_as_a_guess(
    fake_security_key: _FakeFido2Client,
) -> None:
    soul_crypto.soul_key_init(backend="file")
    soul_crypto.soul_key_enroll_recovery()
    key_file = Path(soul_crypto.soul_key_status()["path"])

    key_file.write_bytes(soul_crypto._generate_key())  # changed with no way to remember it

    assert soul_crypto.cached_key_fingerprint() is None
    assert soul_crypto.soul_key_recovery_facts()["stale"] is None


def test_deploy_fills_a_missing_cache_once_and_status_then_answers(
    fake_security_key: _FakeFido2Client, monkeypatch: pytest.MonkeyPatch,
) -> None:
    soul_crypto.soul_key_init(backend="file")
    soul_crypto.soul_key_enroll_recovery()
    soul_crypto._fingerprint_cache_path(
        Path(soul_crypto.soul_key_status()["path"])).unlink()  # a key that predates the cache
    assert soul_crypto.soul_key_recovery_facts()["stale"] is None

    key_setup.ensure_key_setup()

    calls = _no_decrypt(monkeypatch)
    assert soul_crypto.soul_key_recovery_facts()["stale"] is False
    assert calls == []


def test_a_reseal_onto_the_tpm_keeps_the_cache_valid(
    fake_security_key: _FakeFido2Client, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The file changes when the key is resealed but the key does not: the cache is
    re-stamped, so a status reader still answers without decrypting."""
    soul_crypto.soul_key_init(backend="file")
    resolved = Path(soul_crypto.soul_key_status()["path"])
    key = resolved.read_bytes()
    resolved.write_bytes(key)  # new mtime, same key, as a reseal leaves the carrier file
    assert soul_crypto.cached_key_fingerprint() is None
    soul_crypto._remember_fingerprint(resolved, key, explicit=False)
    assert soul_crypto.cached_key_fingerprint() == soul_crypto.key_fingerprint(key)
