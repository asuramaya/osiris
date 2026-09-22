"""Soul-store key management (Thoth mail 9134/9194/9245, wave 17): NO keyring branch —
env override, else a key file, nothing else; the ONE generator (`soul_key_init`) refuses
to overwrite an existing key and refuses to write without a known-good owner; a missing
key is a named, loud refusal (`SoulKeyMissing`), never a silent auto-generate.
"""
from __future__ import annotations

import getpass
import json
from pathlib import Path
from typing import Any

import pytest
from cryptography.fernet import Fernet
from src.ingest import soul_crypto, systemd_credential
from src.ingest.soul_crypto import (
    SoulKeyMissing,
    get_soul_fernet,
    get_soul_key,
    is_encrypted,
    soul_key_init,
)

# Captured before the autouse fixture below ever stubs it out — the two
# `test_installed_user_unit_env_value_*` tests restore this real implementation for
# their own duration (everything else in this file wants the stub, see that
# fixture's own docstring for why).
_REAL_INSTALLED_USER_UNIT_ENV_VALUE = soul_crypto._installed_user_unit_env_value


@pytest.fixture(autouse=True)
def _clear_soul_key_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """conftest.py sets a durable `OSIRIS_SOUL_KEY` for every OTHER test's own encrypted
    rows (xdist worker isolation) — this file tests the key-resolution ladder itself, so
    every test here starts from a genuinely clean slate and opts back in explicitly.

    ALSO stubs `_installed_user_unit_env_value` to always return None (THE KEY DOOR,
    defect 1): the real function reads THIS BOX's own actual `~/.config/systemd/user/
    osiris-mcp.service` — a real file, since this exact box is the deployment the
    whole soul-store encryption feature is FOR. Leaving it live would make every test
    below depend on whatever this developer's own machine happens to have installed
    at the moment the suite runs, never a controlled input. Tests that specifically
    exercise the installed-unit branch override this stub explicitly, per test."""
    monkeypatch.delenv("OSIRIS_SOUL_KEY", raising=False)
    monkeypatch.delenv("OSIRIS_SOUL_KEY_LEGACY", raising=False)
    monkeypatch.setattr(soul_crypto, "_installed_user_unit_env_value", lambda _name: None)


@pytest.fixture(autouse=True)
def _redirect_credstore_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """THE FIRST KEY MUST COME FROM THE NORMAL CLI (Thoth mail 13065): the DEFAULT
    (no explicit `--path`) credential location is now the box's own REAL per-user
    credstore (`~/.config/credstore.encrypted/`, `systemd_credential.
    user_credstore_encrypted_dir()`) — a location this file never needed to isolate
    before this ruling (every write used to land at a sibling of `resolved`, which
    every test already redirects via `OSIRIS_SOUL_KEY_FILE`/`_DEFAULT_KEY_FILE`/
    tmp_path). Without this, a test that mints a key with the auto-selected
    host-cred/host+tpm2 backend and NO explicit `--path` (several already do,
    testing owner/note mechanics unrelated to WHERE the credential lands) would
    silently read/write THIS DEVELOPER'S OWN real credential store — caught live
    during this fix's own build (a stray test-written key was found sitting in the
    real `~/.config/credstore.encrypted/soul.key`, cleaned up by hand). Redirecting
    `XDG_CONFIG_HOME` is the SAME env-var isolation `user_credstore_encrypted_dir`
    itself reads, applied here the same way `_key_file_path`'s own XDG fallback is
    already isolated by individual tests below."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdgcfg"))


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


# --- _installed_user_unit_env_value: THE KEY DOOR, defect 1 -----------------------------------

def test_installed_user_unit_env_value_reads_and_expands_h(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The REAL function (not the autouse stub above) reads a static unit file at
    `~/.config/systemd/user/osiris-mcp.service` and expands systemd's own `%h`
    specifier to this process's own home — a plain sync file read, no systemctl."""
    fake_home = tmp_path / "fakehome"
    unit_dir = fake_home / ".config" / "systemd" / "user"
    unit_dir.mkdir(parents=True)
    (unit_dir / "osiris-mcp.service").write_text(
        "[Service]\nEnvironment=FOO=bar\nEnvironment=OSIRIS_SOUL_KEY_FILE=%h/.config/"
        "osiris/soul.key\n")
    monkeypatch.setattr(Path, "home", lambda: fake_home)
    monkeypatch.setattr(
        soul_crypto, "_installed_user_unit_env_value", _REAL_INSTALLED_USER_UNIT_ENV_VALUE)
    assert (soul_crypto._installed_user_unit_env_value("OSIRIS_SOUL_KEY_FILE")
            == str(fake_home / ".config" / "osiris" / "soul.key"))


def test_installed_user_unit_env_value_falls_back_to_worker_unit(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """osiris-mcp.service is checked first; osiris-worker.service is the fallback
    when only that one is installed (both carry the same line by construction, but
    a partial/interrupted deploy could leave only one on disk)."""
    fake_home = tmp_path / "fakehome"
    unit_dir = fake_home / ".config" / "systemd" / "user"
    unit_dir.mkdir(parents=True)
    (unit_dir / "osiris-worker.service").write_text(
        "Environment=OSIRIS_SOUL_KEY_FILE=%h/.config/osiris/soul.key\n")
    monkeypatch.setattr(Path, "home", lambda: fake_home)
    monkeypatch.setattr(
        soul_crypto, "_installed_user_unit_env_value", _REAL_INSTALLED_USER_UNIT_ENV_VALUE)
    assert (soul_crypto._installed_user_unit_env_value("OSIRIS_SOUL_KEY_FILE")
            == str(fake_home / ".config" / "osiris" / "soul.key"))


def test_installed_user_unit_env_value_none_when_nothing_installed(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "fresh-box")
    monkeypatch.setattr(
        soul_crypto, "_installed_user_unit_env_value", _REAL_INSTALLED_USER_UNIT_ENV_VALUE)
    assert soul_crypto._installed_user_unit_env_value("OSIRIS_SOUL_KEY_FILE") is None


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
    with pytest.raises(SoulKeyMissing, match="soul-key init"):
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
    out = soul_key_init()
    assert "error" in out
    assert "already exists" in out["error"]


def test_soul_key_init_refuses_as_root_with_no_owner(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Thoth DM 9435: this box's own live units are systemd --user, no dedicated
    service account at all — refusing every non-root caller (the old behavior) would
    wrongly block the exact shape that deployment needs (the operator running this as
    themselves). Only root, with no --owner to disambiguate, is genuinely ambiguous."""
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(tmp_path / "soul.key"))
    monkeypatch.setattr(soul_crypto.os, "getuid", lambda: 0)
    out = soul_key_init()
    assert "error" in out
    assert "refusing" in out["error"]
    assert not (tmp_path / "soul.key").exists()


def test_soul_key_init_writes_when_running_as_a_normal_user(
    tmp_path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """Thoth DM 9435: no dedicated service account is assumed — any non-root caller
    (the operator's own login user, for a systemd --user deploy) proceeds directly.
    `backend="file", print_recovery=True` here (KEY CUSTODY REWRITTEN, ruling
    e0b98ff2): this test is about the owner/print mechanics, not backend
    selection — the systemd-creds backend gets its OWN dedicated tests below."""
    key_file = tmp_path / "soul.key"
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(key_file))
    monkeypatch.setattr(soul_crypto.os, "getuid", lambda: 1000)
    out = soul_key_init(backend="file", print_recovery=True)
    assert "error" not in out
    assert key_file.is_file()
    assert out["path"] == str(key_file)
    assert out["chowned"] is False
    # THE MANDATORY DISCLOSURE (Thoth DM 9245): printed exactly once, to stdout, where a
    # human running this in their own terminal actually sees it -- now opt-in only
    # (`print_recovery=True` above), see KEY CUSTODY REWRITTEN.
    captured = capsys.readouterr()
    assert "THIS IS THE ONLY TIME THIS KEY PRINTS" in captured.out
    # a fresh key is a real, usable Fernet key — round-trips
    monkeypatch.delenv("OSIRIS_SOUL_KEY", raising=False)
    assert get_soul_fernet().decrypt(get_soul_fernet().encrypt(b"x")) == b"x"


def test_soul_key_init_root_default_path_note_says_no_change_needed(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    """THE KEY DOOR: root always resolves `_DEFAULT_KEY_FILE` (the system-unit
    shape's own default, `deploy/osiris-worker.service`'s `EnvironmentFile=`
    default) — unaffected by the --user-unit/XDG ladder, which is unprivileged-only."""
    monkeypatch.delenv("OSIRIS_SOUL_KEY_FILE", raising=False)
    monkeypatch.setattr(soul_crypto, "_DEFAULT_KEY_FILE", str(tmp_path / "soul.key"))
    monkeypatch.setattr(soul_crypto.os, "getuid", lambda: 0)
    monkeypatch.setattr(getpass, "getuser", lambda: "someuser")
    out = soul_key_init(owner="someuser")  # matches current_user -- no real chown attempted
    assert "error" not in out
    assert "no env change needed" in out["systemd_note"]


def test_soul_key_init_matches_installed_user_unit_note_says_no_change_needed(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    """THE KEY DOOR, defect 1 fixed: when the resolved path already matches what an
    INSTALLED osiris-mcp/osiris-worker --user unit's own `Environment=
    OSIRIS_SOUL_KEY_FILE=` line carries, no env change is needed — just a restart."""
    key_file = tmp_path / "soul.key"
    monkeypatch.delenv("OSIRIS_SOUL_KEY_FILE", raising=False)
    monkeypatch.setattr(soul_crypto.os, "getuid", lambda: 1000)
    monkeypatch.setattr(
        soul_crypto, "_installed_user_unit_env_value", lambda _name: str(key_file))
    out = soul_key_init(backend="file")
    assert "error" not in out
    assert out["path"] == str(key_file)
    assert "already matches the installed" in out["systemd_note"]
    assert "no env change needed" in out["systemd_note"]


def test_key_file_path_unprivileged_no_installed_unit_falls_back_to_xdg(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    """THE KEY DOOR, defect 1: a genuinely fresh box (no --user unit installed yet)
    resolves to `$XDG_CONFIG_HOME/osiris/soul.key` for an unprivileged caller —
    never `/etc/osiris` (a PermissionError waiting to happen for a login user, the
    exact live defect the operator hit by hand)."""
    monkeypatch.delenv("OSIRIS_SOUL_KEY_FILE", raising=False)
    monkeypatch.setattr(soul_crypto.os, "getuid", lambda: 1000)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdgcfg"))
    resolved = soul_crypto._key_file_path()
    assert resolved == tmp_path / "xdgcfg" / "osiris" / "soul.key"


def test_key_file_path_root_never_uses_xdg_or_installed_unit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Root has no natural `~` for a system-unit deploy — falls straight through to
    `_DEFAULT_KEY_FILE`, the same as before THE KEY DOOR ever touched this function."""
    monkeypatch.delenv("OSIRIS_SOUL_KEY_FILE", raising=False)
    monkeypatch.setattr(soul_crypto.os, "getuid", lambda: 0)
    monkeypatch.setattr(
        soul_crypto, "_installed_user_unit_env_value",
        lambda _name: (_ for _ in ()).throw(AssertionError("root must never check this")))
    assert soul_crypto._key_file_path() == Path(soul_crypto._DEFAULT_KEY_FILE)


def test_key_file_path_explicit_always_wins(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(tmp_path / "env-path" / "soul.key"))
    explicit = tmp_path / "explicit" / "soul.key"
    assert soul_crypto._key_file_path(explicit=str(explicit)) == explicit


def test_soul_key_init_non_default_path_names_the_env_line(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    key_file = tmp_path / "custom" / "soul.key"
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(key_file))
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
    out = soul_key_init(owner=soul_crypto._SERVICE_USER, backend="file")
    assert "error" not in out
    assert out["chowned"] is True
    assert len(calls) == 2  # the key file AND its parent directory
    assert {c[1] for c in calls} == {424242}


def test_soul_key_init_explicit_path_overrides_resolution(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE KEY DOOR, `--path`: the escape hatch, bypassing the whole resolution
    ladder including any env var already set."""
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(tmp_path / "env-path" / "soul.key"))
    explicit = tmp_path / "explicit" / "soul.key"
    out = soul_key_init(path=str(explicit), backend="file")
    assert "error" not in out
    assert out["path"] == str(explicit)
    assert explicit.exists()


# --- soul_key_status: filesystem facts, never key bytes ---------------------------------------

def test_soul_key_status_absent(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OSIRIS_SOUL_KEY_FILE", raising=False)
    out = soul_crypto.soul_key_status(path=str(tmp_path / "no-such-file"))
    assert out["present"] is False
    assert out["backend"] == "missing"
    assert out["created_age_seconds"] is None
    assert out["rotation_in_flight"] is False
    assert out["recovery_paths_enrolled"] == []


def test_soul_key_status_present_reports_facts_never_key_bytes(tmp_path) -> None:
    """KEY CUSTODY REWRITTEN (ruling e0b98ff2): a legacy plaintext file at the
    LOGICAL path (no `.cred` sibling) reads as backend `"file"` — the shape
    `soul_key_init(backend="file")` itself would have written."""
    key_file = tmp_path / "soul.key"
    key_bytes = Fernet.generate_key()
    key_file.write_bytes(key_bytes)
    key_file.chmod(0o600)
    out = soul_crypto.soul_key_status(path=str(key_file))
    assert out["present"] is True
    assert out["backend"] == "file"
    assert out["created_age_seconds"] is not None and out["created_age_seconds"] >= 0
    assert out["rotation_in_flight"] is False
    assert key_bytes.decode() not in str(out)


def test_soul_key_status_reports_credential_backend(tmp_path) -> None:
    """KEY CUSTODY REWRITTEN: a systemd-creds credential (`.cred` + `.meta.json`)
    reads its own `backend` back from the meta sidecar, never guesses."""
    key_file = tmp_path / "soul.key"
    out = soul_key_init(path=str(key_file))
    assert "error" not in out
    status = soul_crypto.soul_key_status(path=str(key_file))
    assert status["present"] is True
    assert status["backend"] == out["backend"]  # "host-cred" on this box (no tss membership)


def test_soul_key_status_rotation_in_flight_when_legacy_file_exists(tmp_path) -> None:
    key_file = tmp_path / "soul.key"
    key_file.write_bytes(Fernet.generate_key())
    (tmp_path / "soul.key.legacy").write_bytes(Fernet.generate_key())
    out = soul_crypto.soul_key_status(path=str(key_file))
    assert out["rotation_in_flight"] is True


# --- soul_key_rotate_begin/finish: THE KEY DOOR's two-step rotation ----------------------------

def test_soul_key_rotate_begin_refuses_when_no_key_exists(tmp_path) -> None:
    out = soul_crypto.soul_key_rotate_begin(path=str(tmp_path / "soul.key"))
    assert "error" in out
    assert "soul-key init" in out["error"]


def test_soul_key_rotate_begin_parks_old_key_and_writes_new(
    tmp_path, capsys: pytest.CaptureFixture[str],
) -> None:
    key_file = tmp_path / "soul.key"
    old_key = Fernet.generate_key()
    key_file.write_bytes(old_key)
    out = soul_crypto.soul_key_rotate_begin(path=str(key_file), print_recovery=True)
    assert "error" not in out
    assert out["backend"] == "file"  # preserved from the old key's own shape, plaintext
    assert out["old_key"] == old_key
    assert out["new_key"] != old_key
    assert key_file.read_bytes() == out["new_key"]
    legacy_path = Path(out["legacy_path"])
    assert legacy_path.read_bytes() == old_key
    assert "restart osiris-mcp and osiris-worker" in out["systemd_note"]
    # THE MANDATORY DISCLOSURE holds for rotation too, not just first init (now
    # opt-in via print_recovery=True above, see KEY CUSTODY REWRITTEN)
    captured = capsys.readouterr()
    assert "THIS IS THE ONLY TIME THIS KEY PRINTS" in captured.out


def test_soul_key_rotate_begin_refuses_a_second_rotation_already_in_flight(tmp_path) -> None:
    key_file = tmp_path / "soul.key"
    key_file.write_bytes(Fernet.generate_key())
    first = soul_crypto.soul_key_rotate_begin(path=str(key_file))
    assert "error" not in first
    second = soul_crypto.soul_key_rotate_begin(path=str(key_file))
    assert "error" in second
    assert "already in flight" in second["error"]
    # the first rotation's own new key is untouched by the refused second attempt
    assert key_file.read_bytes() == first["new_key"]


def test_soul_key_rotate_finish_refuses_when_nothing_in_flight(tmp_path) -> None:
    key_file = tmp_path / "soul.key"
    key_file.write_bytes(Fernet.generate_key())
    out = soul_crypto.soul_key_rotate_finish(path=str(key_file))
    assert "error" in out
    assert "nothing to finish" in out["error"]


def test_soul_key_rotate_finish_removes_the_legacy_key(tmp_path) -> None:
    key_file = tmp_path / "soul.key"
    key_file.write_bytes(Fernet.generate_key())
    begin = soul_crypto.soul_key_rotate_begin(path=str(key_file))
    legacy_path = Path(begin["legacy_path"])
    assert legacy_path.exists()
    out = soul_crypto.soul_key_rotate_finish(path=str(key_file))
    assert "error" not in out
    assert not legacy_path.exists()
    # a second finish, with nothing left in flight, refuses cleanly
    second = soul_crypto.soul_key_rotate_finish(path=str(key_file))
    assert "error" in second


# --- KEY CUSTODY REWRITTEN (ruling e0b98ff2): systemd-creds backend selection/rotation ---------

def test_resolve_backend_prefers_host_tpm2_when_tss_member(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(soul_crypto, "_systemd_creds_available", lambda: True)
    monkeypatch.setattr(soul_crypto, "_is_tss_member", lambda: True)
    assert soul_crypto._resolve_backend(None) == "host+tpm2"


def test_resolve_backend_falls_back_to_host_cred_without_tss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(soul_crypto, "_systemd_creds_available", lambda: True)
    monkeypatch.setattr(soul_crypto, "_is_tss_member", lambda: False)
    assert soul_crypto._resolve_backend(None) == "host-cred"


def test_resolve_backend_falls_back_to_file_with_no_systemd_creds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(soul_crypto, "_systemd_creds_available", lambda: False)
    assert soul_crypto._resolve_backend(None) == "file"


def test_resolve_backend_explicit_always_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(soul_crypto, "_systemd_creds_available", lambda: False)
    assert soul_crypto._resolve_backend("host+tpm2") == "host+tpm2"


def test_soul_key_init_host_cred_tss_hint_names_the_group(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real live systemd-creds encrypt/decrypt round trip (no fakes) -- this box
    genuinely has systemd-creds but the operator is genuinely not in `tss`, so
    this exercises the real command, not a mock of it.

    Resolves the key path via `OSIRIS_SOUL_KEY_FILE` for BOTH `init` and the
    later `get_soul_key()` read (never an explicit `--path` for one and the env
    var for the other) — THE FIRST KEY MUST COME FROM THE NORMAL CLI (Thoth mail
    13065): `init`'s DEFAULT credential location is now the real per-user
    credstore, keyed off `_CRED_NAME` alone, never a sibling of the resolved
    path — a caller that writes via an explicit `--path` and reads via `env`
    (two DIFFERENT resolution ladders that only happened to agree by accident
    before this ruling) is a genuine mismatch this door no longer papers over,
    matching production's own real shape (osiris-mcp/osiris-worker always
    resolve via env/installed-unit, never a CLI `--path` flag)."""
    key_file = tmp_path / "soul.key"
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(key_file))
    out = soul_key_init()
    assert "error" not in out
    assert out["backend"] == "host-cred"
    assert out["path"] == str(systemd_credential.user_credstore_encrypted_dir() / "soul.key")
    assert Path(out["path"]).exists()
    assert (tmp_path / "soul.key.meta.json").exists()
    assert out["tss_hint"] is not None and "usermod -aG tss" in out["tss_hint"]
    # the real round trip: get_soul_key decrypts the credential correctly
    key = get_soul_key()
    assert Fernet(key)  # a real, usable Fernet key


def test_get_soul_key_reads_credentials_directory_first(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The DAEMON path (LoadCredentialEncrypted) -- systemd has ALREADY decrypted
    the credential into $CREDENTIALS_DIRECTORY before this process starts, so
    this is a plain file read, no systemd-creds subprocess involved at all."""
    cred_dir = tmp_path / "creds"
    cred_dir.mkdir()
    real_key = Fernet.generate_key()
    (cred_dir / "soul.key").write_bytes(real_key)
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(cred_dir))
    monkeypatch.delenv("OSIRIS_SOUL_KEY_FILE", raising=False)
    assert get_soul_key() == real_key


def test_soul_key_rotate_preserves_host_cred_backend(tmp_path) -> None:
    key_file = tmp_path / "soul.key"
    init_out = soul_key_init(path=str(key_file))
    assert init_out["backend"] == "host-cred"
    begin = soul_crypto.soul_key_rotate_begin(path=str(key_file))
    assert "error" not in begin
    assert begin["backend"] == "host-cred"
    assert Path(begin["path"]).exists()
    assert Path(begin["legacy_path"]).exists()
    # the new credential decrypts correctly under systemd-creds
    new_blob = Path(begin["path"]).read_bytes()
    assert soul_crypto._decrypt_with_systemd_creds(new_blob) == begin["new_key"]
    finish = soul_crypto.soul_key_rotate_finish(path=str(key_file))
    assert "error" not in finish
    assert not Path(begin["legacy_path"]).exists()


# --- FIDO2 hmac-secret ("prf") recovery: fake device/client, no physical hardware ---------------

class _FakeCredentialData:
    def __init__(self, credential_id: bytes) -> None:
        self.credential_id = credential_id


class _FakeAuthData:
    def __init__(self, credential_id: bytes) -> None:
        self.credential_data = _FakeCredentialData(credential_id)


class _FakeAttestationObject:
    def __init__(self, credential_id: bytes) -> None:
        self.auth_data = _FakeAuthData(credential_id)


class _FakeRegistration:
    def __init__(self, credential_id: bytes) -> None:
        self.raw_id = credential_id
        self.attestation_object = _FakeAttestationObject(credential_id)


class _FakeExtensionResults:
    def __init__(self, prf_output: bytes | None) -> None:
        self.prf = {"results": {"first": prf_output}} if prf_output is not None else None


class _FakeAssertion:
    def __init__(self, prf_output: bytes | None) -> None:
        self.client_extension_results = _FakeExtensionResults(prf_output)


class _FakeAssertionSelection:
    def __init__(self, prf_output: bytes | None) -> None:
        self._prf_output = prf_output

    def get_response(self, index: int) -> _FakeAssertion:
        return _FakeAssertion(self._prf_output)


class _FakeFido2Client:
    """A fake standing in for `fido2.client.Fido2Client` — the PRF output is
    DETERMINISTIC per fake credential+salt (an HMAC over the salt, keyed by a
    fixed per-instance secret), matching the real extension's own contract
    (same credential, same salt, same output, every time) closely enough to
    prove `soul_key_enroll_recovery`/`soul_key_recover`'s own plumbing without
    a real Security Key."""

    def __init__(self, device_secret: bytes = b"fake-device-secret") -> None:
        self._device_secret = device_secret
        self._credential_id = b"fake-credential-id-0123456789ab"

    def make_credential(self, options: Any) -> _FakeRegistration:
        return _FakeRegistration(self._credential_id)

    def get_assertion(self, options: Any) -> _FakeAssertionSelection:
        import hashlib
        import hmac

        salt = options.extensions["prf"]["eval"]["first"]
        prf_output = hmac.new(self._device_secret, salt, hashlib.sha256).digest()
        return _FakeAssertionSelection(prf_output)


def test_soul_key_enroll_recovery_refuses_when_no_key_exists(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(soul_crypto, "_find_fido2_device", lambda: object())
    out = soul_crypto.soul_key_enroll_recovery(path=str(tmp_path / "soul.key"))
    assert "error" in out
    assert "soul-key init" in out["error"]


def test_soul_key_enroll_recovery_refuses_with_no_device(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    key_file = tmp_path / "soul.key"
    soul_key_init(path=str(key_file), backend="file")
    monkeypatch.setattr(soul_crypto, "_find_fido2_device", lambda: None)
    out = soul_crypto.soul_key_enroll_recovery(path=str(key_file))
    assert "error" in out
    assert "no FIDO2 security key detected" in out["error"]


def test_soul_key_enroll_recovery_and_recover_round_trip_with_a_fake_device(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE KEY DOOR's own FIDO2 plumbing, proved with a fake device/client — the
    live touch+PIN ceremony against real hardware is NOT exercised here (see
    this module's own docstring: the agent that wrote this has no hands, no
    eyes on a physical Security Key) — this proves the wrap/unwrap/fingerprint
    logic around that ceremony is wired correctly."""
    key_file = tmp_path / "soul.key"
    init_out = soul_key_init(path=str(key_file), backend="file")
    original_key = key_file.read_bytes()

    fake_client = _FakeFido2Client()
    monkeypatch.setattr(soul_crypto, "_find_fido2_device", lambda: object())
    monkeypatch.setattr(soul_crypto, "_fido2_client", lambda device, rp_id: fake_client)

    enrolled = soul_crypto.soul_key_enroll_recovery(path=str(key_file))
    assert "error" not in enrolled
    recovery_path = Path(enrolled["path"])
    assert recovery_path.exists()
    blob = json.loads(recovery_path.read_text())
    assert set(blob) == {"credential_id", "salt", "wrapped_key", "key_fingerprint", "rp_id"}

    status = soul_crypto.soul_key_status(path=str(key_file))
    assert status["recovery_paths_enrolled"] == ["fido2"]
    assert status["recovery_warning"] is not None  # still only 1 path

    # simulate recovering onto a fresh box: remove the live key, keep the blob
    key_file.unlink()
    recovered = soul_crypto.soul_key_recover(path=str(key_file), backend="file")
    assert "error" not in recovered
    assert key_file.read_bytes() == original_key
    assert init_out["backend"] == recovered["backend"] == "file"


def test_soul_key_enroll_recovery_stamps_the_given_rp_id_into_the_blob(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Thoth mail 13006: rp_id is now a caller-supplied param (the CLI reads
    `soul_key.rp_id` off settings and passes it down), never the old hard-coded
    `_RP_ID = "osiris.local"` module constant."""
    key_file = tmp_path / "soul.key"
    soul_key_init(path=str(key_file), backend="file")
    fake_client = _FakeFido2Client()
    monkeypatch.setattr(soul_crypto, "_find_fido2_device", lambda: object())
    seen_rp_ids: list[str] = []
    monkeypatch.setattr(
        soul_crypto, "_fido2_client",
        lambda device, rp_id: (seen_rp_ids.append(rp_id), fake_client)[1])

    enrolled = soul_crypto.soul_key_enroll_recovery(path=str(key_file), rp_id="example.org")
    assert "error" not in enrolled
    blob = json.loads(Path(enrolled["path"]).read_text())
    assert blob["rp_id"] == "example.org"
    assert seen_rp_ids == ["example.org"]


def test_soul_key_recover_prefers_the_blobs_own_rp_id_over_the_callers(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A credential must be addressed by the rp_id it was actually enrolled
    under — if the live `soul_key.rp_id` setting ever changes between enroll and
    recover, recovery must still use the ORIGINAL value, never the new one."""
    key_file = tmp_path / "soul.key"
    soul_key_init(path=str(key_file), backend="file")
    fake_client = _FakeFido2Client()
    monkeypatch.setattr(soul_crypto, "_find_fido2_device", lambda: object())
    monkeypatch.setattr(soul_crypto, "_fido2_client", lambda device, rp_id: fake_client)

    enrolled = soul_crypto.soul_key_enroll_recovery(path=str(key_file), rp_id="original.example")
    assert "error" not in enrolled

    key_file.unlink()
    seen_rp_ids: list[str] = []
    monkeypatch.setattr(
        soul_crypto, "_fido2_client",
        lambda device, rp_id: (seen_rp_ids.append(rp_id), fake_client)[1])
    recovered = soul_crypto.soul_key_recover(
        path=str(key_file), backend="file", rp_id="a-different-live-setting.example")
    assert "error" not in recovered
    assert seen_rp_ids == ["original.example"]  # the blob's own value, never the fallback


def test_soul_key_recover_refuses_when_a_key_already_exists(tmp_path) -> None:
    key_file = tmp_path / "soul.key"
    soul_key_init(path=str(key_file), backend="file")
    out = soul_crypto.soul_key_recover(path=str(key_file))
    assert "error" in out
    assert "already exists" in out["error"]


def test_soul_key_recover_refuses_with_no_recovery_enrollment(tmp_path) -> None:
    out = soul_crypto.soul_key_recover(path=str(tmp_path / "soul.key"))
    assert "error" in out
    assert "no recovery enrollment found" in out["error"]


def test_soul_key_recover_refuses_a_tampered_blob(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    key_file = tmp_path / "soul.key"
    soul_key_init(path=str(key_file), backend="file")
    fake_client = _FakeFido2Client()
    monkeypatch.setattr(soul_crypto, "_find_fido2_device", lambda: object())
    monkeypatch.setattr(soul_crypto, "_fido2_client", lambda device, rp_id: fake_client)
    enrolled = soul_crypto.soul_key_enroll_recovery(path=str(key_file))
    recovery_path = Path(enrolled["path"])
    blob = json.loads(recovery_path.read_text())
    blob["key_fingerprint"] = "0" * 16  # tamper
    recovery_path.write_text(json.dumps(blob))

    key_file.unlink()
    out = soul_crypto.soul_key_recover(path=str(key_file), backend="file")
    assert "error" in out
    assert "fingerprint" in out["error"]
    assert not key_file.exists()  # refused BEFORE writing anything
