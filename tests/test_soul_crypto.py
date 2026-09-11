"""Soul-store key management (Thoth mail 9134/9194/9245, wave 17): NO keyring branch —
env override, else a key file, nothing else; the ONE generator (`soul_key_init`) refuses
to overwrite an existing key and refuses to write without a known-good owner; a missing
key is a named, loud refusal (`SoulKeyMissing`), never a silent auto-generate.
"""
from __future__ import annotations

import getpass

import pytest
from cryptography.fernet import Fernet
from src.ingest import soul_crypto
from src.ingest.soul_crypto import (
    SoulKeyMissing,
    get_soul_fernet,
    get_soul_key,
    is_encrypted,
    soul_key_init,
)


@pytest.fixture(autouse=True)
def _clear_soul_key_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """conftest.py sets a durable `OSIRIS_SOUL_KEY` for every OTHER test's own encrypted
    rows (xdist worker isolation) — this file tests the key-resolution ladder itself, so
    every test here starts from a genuinely clean slate and opts back in explicitly."""
    monkeypatch.delenv("OSIRIS_SOUL_KEY", raising=False)
    monkeypatch.delenv("OSIRIS_SOUL_KEY_LEGACY", raising=False)


def test_no_keyring_import_anywhere_in_the_module() -> None:
    """The amended design (Thoth DM 9245) drops the OS-keyring branch entirely — a
    regression here would silently reintroduce the exact non-determinism (a login
    session's D-Bus still live under systemd) the amendment exists to close. Checks for
    an actual `import keyring` statement, not the word itself — the module's own
    docstring names keyring in prose, explaining why it was dropped."""
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(soul_crypto))
    imported = {
        alias.name for node in ast.walk(tree) if isinstance(node, ast.Import)
        for alias in node.names
    }
    assert "keyring" not in imported


def test_get_soul_key_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    key = Fernet.generate_key().decode()
    monkeypatch.setenv("OSIRIS_SOUL_KEY", key)
    assert get_soul_key() == key.encode()


def test_get_soul_key_reads_an_existing_file(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    key_file = tmp_path / "soul.key"
    key = Fernet.generate_key()
    key_file.write_bytes(key)
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(key_file))
    assert get_soul_key() == key


def test_get_soul_key_missing_raises_naming_the_init_command(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(tmp_path / "no-such-file"))
    with pytest.raises(SoulKeyMissing, match="soul-key-init"):
        get_soul_key()


def test_get_soul_key_never_auto_generates(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    key_file = tmp_path / "soul.key"
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(key_file))
    with pytest.raises(SoulKeyMissing):
        get_soul_key()
    assert not key_file.exists()


def test_get_soul_fernet_scope_param_is_accepted_and_inert(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`scope` (Thoth DM 9379): the seam for per-tenant keys the operator is weighing —
    today it never varies the lookup, so any scope resolves the SAME single-store key."""
    key_file = tmp_path / "soul.key"
    key_file.write_bytes(Fernet.generate_key())
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(key_file))
    a = get_soul_fernet(scope="tenant-a")
    b = get_soul_fernet(scope="tenant-b")
    token = a.encrypt(b"hello")
    assert b.decrypt(token) == b"hello"  # same underlying key regardless of scope


def test_is_encrypted_true_for_a_real_fernet_token() -> None:
    token = Fernet.generate_key()
    assert is_encrypted(Fernet(token).encrypt(b"hello"))


def test_is_encrypted_false_for_plain_bytes() -> None:
    assert is_encrypted(b'{"type": "assistant"}\n') is False


# --- soul_key_init: the one generator --------------------------------------------------------

def test_soul_key_init_refuses_when_a_key_already_exists(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    key_file = tmp_path / "soul.key"
    key_file.write_bytes(Fernet.generate_key())
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(key_file))
    monkeypatch.setattr(getpass, "getuser", lambda: soul_crypto._SERVICE_USER)
    out = soul_key_init()
    assert "error" in out
    assert "already exists" in out["error"]


def test_soul_key_init_refuses_a_non_service_user_with_no_owner(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(tmp_path / "soul.key"))
    monkeypatch.setattr(getpass, "getuser", lambda: "some-random-dev")
    out = soul_key_init()
    assert "error" in out
    assert "refusing" in out["error"]
    assert not (tmp_path / "soul.key").exists()


def test_soul_key_init_writes_when_running_as_the_service_user(
    tmp_path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    key_file = tmp_path / "soul.key"
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(key_file))
    monkeypatch.setattr(getpass, "getuser", lambda: soul_crypto._SERVICE_USER)
    out = soul_key_init()
    assert "error" not in out
    assert key_file.is_file()
    assert out["path"] == str(key_file)
    assert out["chowned"] is False
    # THE MANDATORY DISCLOSURE (Thoth DM 9245): printed exactly once, to stdout, where a
    # human running this in their own terminal actually sees it.
    captured = capsys.readouterr()
    assert "THIS IS THE ONLY TIME THIS KEY PRINTS" in captured.out
    # a fresh key is a real, usable Fernet key — round-trips
    monkeypatch.delenv("OSIRIS_SOUL_KEY", raising=False)
    assert get_soul_fernet().decrypt(get_soul_fernet().encrypt(b"x")) == b"x"


def test_soul_key_init_default_path_note_says_no_change_needed(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    monkeypatch.delenv("OSIRIS_SOUL_KEY_FILE", raising=False)
    monkeypatch.setattr(soul_crypto, "_DEFAULT_KEY_FILE", str(tmp_path / "soul.key"))
    monkeypatch.setattr(getpass, "getuser", lambda: soul_crypto._SERVICE_USER)
    out = soul_key_init()
    assert "error" not in out
    assert "no env change needed" in out["systemd_note"]


def test_soul_key_init_non_default_path_names_the_env_line(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    key_file = tmp_path / "custom" / "soul.key"
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(key_file))
    monkeypatch.setattr(getpass, "getuser", lambda: soul_crypto._SERVICE_USER)
    out = soul_key_init()
    assert "error" not in out
    assert f"OSIRIS_SOUL_KEY_FILE={key_file}" in out["systemd_note"]


def test_soul_key_init_with_owner_chowns_when_a_pwd_entry_exists(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Running as root before the service user can run anything itself — the `--owner`
    path. Never asserts real ownership (this test does not run as root); proves the
    chown call is actually MADE with the right target instead."""
    key_file = tmp_path / "soul.key"
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(key_file))
    monkeypatch.setattr(getpass, "getuser", lambda: "root")  # differs from the --owner target
    calls: list[tuple[str, int, int]] = []

    class _Pw:
        pw_uid = 424242
        pw_gid = 424242

    monkeypatch.setattr(soul_crypto.pwd, "getpwnam", lambda name: _Pw())
    monkeypatch.setattr(
        soul_crypto.os, "chown",
        lambda path, uid, gid: calls.append((str(path), uid, gid)))
    out = soul_key_init(owner=soul_crypto._SERVICE_USER)
    assert "error" not in out
    assert out["chowned"] is True
    assert len(calls) == 2  # the key file AND its parent directory
    assert {c[1] for c in calls} == {424242}
