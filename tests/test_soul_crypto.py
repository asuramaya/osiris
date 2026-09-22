"""Soul-store key management (Thoth mail 9134/9194/9245, wave 17): NO keyring branch —
env override, else a key file, nothing else; the ONE generator (`soul_key_init`) refuses
to overwrite an existing key and refuses to write without a known-good owner; a missing
key is a named, loud refusal (`SoulKeyMissing`), never a silent auto-generate.
"""
from __future__ import annotations

import getpass
from pathlib import Path

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
    (the operator's own login user, for a systemd --user deploy) proceeds directly."""
    key_file = tmp_path / "soul.key"
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(key_file))
    monkeypatch.setattr(soul_crypto.os, "getuid", lambda: 1000)
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
    out = soul_key_init()
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
    out = soul_key_init(owner=soul_crypto._SERVICE_USER)
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
    out = soul_key_init(path=str(explicit))
    assert "error" not in out
    assert out["path"] == str(explicit)
    assert explicit.exists()


# --- soul_key_status: filesystem facts, never key bytes ---------------------------------------

def test_soul_key_status_absent(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OSIRIS_SOUL_KEY_FILE", raising=False)
    out = soul_crypto.soul_key_status(path=str(tmp_path / "no-such-file"))
    assert out["present"] is False
    assert out["mode"] is None
    assert out["created_age_seconds"] is None
    assert out["rotation_in_flight"] is False


def test_soul_key_status_present_reports_facts_never_key_bytes(tmp_path) -> None:
    key_file = tmp_path / "soul.key"
    key_bytes = Fernet.generate_key()
    key_file.write_bytes(key_bytes)
    key_file.chmod(0o600)
    out = soul_crypto.soul_key_status(path=str(key_file))
    assert out["present"] is True
    assert out["mode"] == "0o600"
    assert out["created_age_seconds"] is not None and out["created_age_seconds"] >= 0
    assert out["rotation_in_flight"] is False
    assert key_bytes.decode() not in str(out)


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
    out = soul_crypto.soul_key_rotate_begin(path=str(key_file))
    assert "error" not in out
    assert out["old_key"] == old_key
    assert out["new_key"] != old_key
    assert key_file.read_bytes() == out["new_key"]
    legacy_path = Path(out["legacy_path"])
    assert legacy_path.read_bytes() == old_key
    assert "restart osiris-mcp and osiris-worker" in out["systemd_note"]
    # THE MANDATORY DISCLOSURE holds for rotation too, not just first init
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
