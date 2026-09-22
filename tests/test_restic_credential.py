"""THE RESTIC REPOSITORY PASSWORD (src/orchestrator/restic_credential.py) — KEY
CUSTODY REWRITTEN's own follow-on (ruling e0b98ff2, "same shape for the restic
repository password"). Real systemd-creds round trips (this box genuinely has it,
same discipline test_soul_crypto.py already holds), no fakes."""
from __future__ import annotations

from pathlib import Path

import pytest
from src.orchestrator import restic_credential


@pytest.fixture(autouse=True)
def _redirect_credstore_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Defense in depth, same reasoning as test_soul_crypto.py's own fixture of this
    name: every test below passes an explicit `path=`, so the DEFAULT credstore
    location is never actually exercised here today — but a future test that omits
    `path=` must never be able to silently read/write this developer's own real
    `~/.config/credstore.encrypted/`."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdgcfg"))


def test_restic_key_init_default_no_path_writes_into_the_credstore(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE FIRST KEY MUST COME FROM THE NORMAL CLI (Thoth mail 13065): with NO
    explicit `--path`, the credential lands in the per-user encrypted credstore
    (redirected by the autouse fixture above to `tmp_path/xdgcfg/credstore.
    encrypted/` — never the operator's real one), and a bare `get_restic_password()`
    (also no path) reads it straight back — the SAME single-ladder agreement
    `test_soul_crypto.py`'s own equivalent fixed test proves for soul.key."""
    from src.ingest import systemd_credential

    monkeypatch.delenv("OSIRIS_RESTIC_PASSWORD_FILE", raising=False)
    out = restic_credential.restic_key_init()
    assert "error" not in out
    assert out["backend"] == "host-cred"
    assert out["path"] == str(systemd_credential.user_credstore_encrypted_dir()
                              / "restic.password")
    password = restic_credential.get_restic_password()
    assert isinstance(password, bytes) and len(password) > 20


def test_restic_key_init_writes_a_host_cred_credential(tmp_path: Path) -> None:
    path = tmp_path / "restic.password"
    out = restic_credential.restic_key_init(path=str(path))
    assert "error" not in out
    assert out["backend"] == "host-cred"
    assert out["path"] == str(path) + ".cred"
    assert Path(out["path"]).exists()
    assert (tmp_path / "restic.password.meta.json").exists()
    assert out["tss_hint"] is not None and "usermod -aG tss" in out["tss_hint"]


def test_restic_key_init_refuses_when_one_already_exists(tmp_path: Path) -> None:
    path = tmp_path / "restic.password"
    first = restic_credential.restic_key_init(path=str(path))
    assert "error" not in first
    second = restic_credential.restic_key_init(path=str(path))
    assert "error" in second
    assert "already exists" in second["error"]


def test_restic_key_init_file_backend_is_explicit_opt_in(tmp_path: Path) -> None:
    path = tmp_path / "restic.password"
    out = restic_credential.restic_key_init(path=str(path), backend="file")
    assert out["backend"] == "file"
    assert out["path"] == str(path)
    assert path.read_bytes()  # real plaintext bytes, non-empty
    assert out["tss_hint"] is None  # only the host-cred branch ever proposes tss


def test_restic_key_status_reports_facts_never_the_password(tmp_path: Path) -> None:
    path = tmp_path / "restic.password"
    absent = restic_credential.restic_key_status(path=str(path))
    assert absent["present"] is False
    assert absent["backend"] == "missing"

    restic_credential.restic_key_init(path=str(path))
    present = restic_credential.restic_key_status(path=str(path))
    assert present["present"] is True
    assert present["backend"] == "host-cred"
    assert present["created_age_seconds"] is not None
    assert "password" not in present


def test_get_restic_password_round_trips_through_the_real_credential(
    tmp_path: Path,
) -> None:
    path = tmp_path / "restic.password"
    restic_credential.restic_key_init(path=str(path))
    password = restic_credential.get_restic_password(path=str(path))
    assert isinstance(password, bytes)
    assert len(password) > 20  # a real token_urlsafe(32)-shaped secret, not a stub


def test_get_restic_password_raises_when_nothing_resolves(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OSIRIS_RESTIC_PASSWORD", raising=False)
    monkeypatch.delenv("CREDENTIALS_DIRECTORY", raising=False)
    with pytest.raises(restic_credential.ResticPasswordMissing, match="restic-key init"):
        restic_credential.get_restic_password(path=str(tmp_path / "does-not-exist"))


def test_get_restic_password_env_override_always_wins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_RESTIC_PASSWORD", "an-explicit-override")
    assert restic_credential.get_restic_password(
        path=str(tmp_path / "unused")) == b"an-explicit-override"


def test_get_restic_password_reads_credentials_directory_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The DAEMON path (LoadCredentialEncrypted) — systemd has ALREADY decrypted the
    credential into $CREDENTIALS_DIRECTORY before this process starts, so this is a
    plain file read, no systemd-creds subprocess involved at all."""
    monkeypatch.delenv("OSIRIS_RESTIC_PASSWORD", raising=False)
    cred_dir = tmp_path / "creds"
    cred_dir.mkdir()
    (cred_dir / "restic.password").write_bytes(b"daemon-decrypted-password")
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(cred_dir))
    assert restic_credential.get_restic_password() == b"daemon-decrypted-password"


def test_resolve_backend_matches_soul_crypto_own_ladder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.ingest import systemd_credential

    monkeypatch.setattr(systemd_credential, "systemd_creds_available", lambda: False)
    assert restic_credential._resolve_backend(None) == "file"
    monkeypatch.setattr(systemd_credential, "systemd_creds_available", lambda: True)
    monkeypatch.setattr(systemd_credential, "is_tss_member", lambda: True)
    assert restic_credential._resolve_backend(None) == "host+tpm2"
    monkeypatch.setattr(systemd_credential, "is_tss_member", lambda: False)
    assert restic_credential._resolve_backend(None) == "host-cred"
    assert restic_credential._resolve_backend("file") == "file"  # explicit always wins
