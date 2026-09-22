"""THE SYSTEMD-CREDS PRIMITIVES, SHARED (src/ingest/systemd_credential.py) — extracted
from soul_crypto.py's own first build (KEY CUSTODY REWRITTEN, ruling e0b98ff2) so the
restic offload password (src/orchestrator/restic_credential.py) reuses the identical
subprocess boundary. Real round trips against the box's own genuine systemd-creds —
no fakes, same discipline test_soul_crypto.py's own systemd-creds tests already hold."""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from src.ingest import systemd_credential


def test_systemd_creds_available_is_true_on_this_box() -> None:
    assert systemd_credential.systemd_creds_available() is True


def test_user_credstore_encrypted_dir_matches_systemd_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE FIRST KEY MUST COME FROM THE NORMAL CLI (Thoth mail 13065) — proved
    against the box's own real `systemd-path`, not just re-deriving the same
    `$XDG_CONFIG_HOME`-or-`~/.config` logic a second time by hand."""
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    live = subprocess.run(
        ["systemd-path", "user-credential-store-encrypted"],
        capture_output=True, text=True, timeout=10, check=True).stdout.strip()
    assert str(systemd_credential.user_credstore_encrypted_dir()) == live


def test_user_credstore_encrypted_dir_respects_xdg_config_home(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    assert systemd_credential.user_credstore_encrypted_dir() == tmp_path / "credstore.encrypted"


def test_encrypt_decrypt_round_trips_with_host_key() -> None:
    plaintext = b"a test secret, never printed"
    blob = systemd_credential.encrypt_with_systemd_creds(
        plaintext, name="test.cred", with_key="host")
    assert blob != plaintext
    assert systemd_credential.decrypt_with_systemd_creds(blob, name="test.cred") == plaintext


def test_decrypt_refuses_a_blob_minted_under_a_different_name() -> None:
    """systemd-creds binds the credential's own identity to `--name=` at mint time —
    decrypting under a DIFFERENT name must refuse, not silently succeed. This is
    exactly why `restic_credential`'s own `_CRED_NAME` must match `soul_crypto`'s
    `_CRED_NAME` NEVER — two independently-named credentials, never a shared name
    that could let one accidentally decrypt as the other."""
    blob = systemd_credential.encrypt_with_systemd_creds(
        b"secret", name="name-a.cred", with_key="host")
    with pytest.raises(RuntimeError):
        systemd_credential.decrypt_with_systemd_creds(blob, name="name-b.cred")


def test_run_systemd_creds_raises_runtime_error_naming_real_stderr() -> None:
    with pytest.raises(RuntimeError, match="systemd-creds"):
        systemd_credential.run_systemd_creds(["not-a-real-subcommand"], input_bytes=b"")


def test_is_tss_member_never_raises_on_a_box_with_a_tss_group() -> None:
    # this box genuinely has a tss group (soul_crypto's own tests establish this) —
    # just proving the call succeeds and returns a bool, not asserting membership
    # either way (the operator's own tss membership is a live, changeable fact).
    assert isinstance(systemd_credential.is_tss_member(), bool)
