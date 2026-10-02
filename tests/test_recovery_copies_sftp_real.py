"""The recovery-file copier against a REAL sshd: an unprivileged sshd on a localhost port, with
its own throwaway host key, client key and authorized_keys in a scratch directory, serving real
SFTP into a scratch directory. No operator hands, no existing credentials, nothing outside the
test's own temp directory. Skipped (never faked) on a machine without sshd/sftp."""
from __future__ import annotations

import getpass
import os
import shutil
import socket
import subprocess
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from src.orchestrator import recovery_copies

_SSHD = shutil.which("sshd") or "/usr/sbin/sshd"
_NEEDS = ("ssh-keygen", "sftp", "ssh")


@dataclass
class Sshd:
    host_alias: str
    port: int
    config: Path
    root: Path  # the directory the SFTP session writes into
    known_hosts: Path
    client_key: Path


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture
def sshd(tmp_path: Path) -> Iterator[Sshd]:
    if not Path(_SSHD).exists() or any(shutil.which(t) is None for t in _NEEDS):
        pytest.skip("sshd / ssh-keygen / sftp not available on this machine")
    d = tmp_path / "sshd"
    d.mkdir()
    for name in ("host_key", "client_key"):
        subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(d / name)],
                       check=True, timeout=30)
    (d / "authorized_keys").write_text((d / "client_key.pub").read_text())
    port = _free_port()
    user = getpass.getuser()
    (d / "sshd_config").write_text(
        f"Port {port}\nListenAddress 127.0.0.1\nHostKey {d / 'host_key'}\n"
        f"PidFile {d / 'sshd.pid'}\nAuthorizedKeysFile {d / 'authorized_keys'}\n"
        "PubkeyAuthentication yes\nPasswordAuthentication no\n"
        "KbdInteractiveAuthentication no\nUsePAM no\nStrictModes no\n"
        f"AllowUsers {user}\nSubsystem sftp internal-sftp\nLogLevel ERROR\n")
    proc = subprocess.Popen([_SSHD, "-D", "-e", "-f", str(d / "sshd_config")],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        for _ in range(100):
            if proc.poll() is not None:
                pytest.skip(f"sshd would not start here: {proc.stderr.read().decode()[:300]}")  # type: ignore[union-attr]
            with socket.socket() as probe:
                probe.settimeout(0.2)
                if probe.connect_ex(("127.0.0.1", port)) == 0:
                    break
            time.sleep(0.1)
        else:
            pytest.skip("sshd never started listening")
        host_pub = (d / "host_key.pub").read_text().split()
        known_hosts = d / "known_hosts"
        known_hosts.write_text(f"[127.0.0.1]:{port} {host_pub[0]} {host_pub[1]}\n")
        config = d / "ssh_config"
        config.write_text(
            f"Host osiristest\n  HostName 127.0.0.1\n  Port {port}\n  User {user}\n"
            f"  IdentityFile {d / 'client_key'}\n  IdentitiesOnly yes\n"
            f"  UserKnownHostsFile {known_hosts}\n  StrictHostKeyChecking yes\n")
        root = tmp_path / "nas"
        root.mkdir()
        yield Sshd("osiristest", port, config, root, known_hosts, d / "client_key")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.fixture(autouse=True)
def _use_the_test_ssh_config(sshd: Sshd, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OSIRIS_SSH_CONFIG", str(sshd.config))


@pytest.fixture
def recovery_file(tmp_path: Path) -> Path:
    f = tmp_path / "soul.key.recovery.json"
    f.write_text('{"credential_id": "abc", "wrapped_key": "wrapped-v1"}')
    return f


def _url(sshd: Sshd, name: str = "repo") -> str:
    return f"sftp:{sshd.host_alias}:{sshd.root}/{name}"


def test_a_plain_copy_lands_beside_the_repository_over_real_sftp(
    sshd: Sshd, recovery_file: Path,
) -> None:
    out = recovery_copies._copy_sftp(recovery_file, _url(sshd))

    assert out is None
    landed = sshd.root / "osiris-recovery" / "soul.key.recovery.json"
    assert landed.read_bytes() == recovery_file.read_bytes()
    assert sorted(p.name for p in (sshd.root / "osiris-recovery").iterdir()) == [
        "soul.key.recovery.json"]  # no half-written temporary file left behind


def test_a_second_copy_replaces_the_first_even_though_the_target_exists(
    sshd: Sshd, recovery_file: Path,
) -> None:
    assert recovery_copies._copy_sftp(recovery_file, _url(sshd)) is None
    recovery_file.write_text('{"wrapped_key": "wrapped-v2"}')

    assert recovery_copies._copy_sftp(recovery_file, _url(sshd)) is None

    landed = sshd.root / "osiris-recovery" / "soul.key.recovery.json"
    assert "wrapped-v2" in landed.read_text()


def test_a_directory_name_with_a_space_and_a_quote_survives_the_batch_file(
    sshd: Sshd, recovery_file: Path,
) -> None:
    weird = sshd.root / 'my "backup" drive'
    weird.mkdir()

    out = recovery_copies._copy_sftp(
        recovery_file, f"sftp:{sshd.host_alias}:{weird}/repo")

    assert out is None
    assert (weird / "osiris-recovery" / "soul.key.recovery.json").exists()


def test_a_missing_parent_directory_is_a_named_failure_not_a_silent_success(
    sshd: Sshd, recovery_file: Path,
) -> None:
    out = recovery_copies._copy_sftp(
        recovery_file, f"sftp:{sshd.host_alias}:{sshd.root}/no/such/parent/repo")

    assert out is not None and "sftp failed" in out
    assert not (sshd.root / "no").exists()


def test_an_unknown_host_key_is_refused_never_prompted_for(
    sshd: Sshd, recovery_file: Path,
) -> None:
    sshd.known_hosts.write_text("")  # strict checking, nothing trusted yet

    started = time.monotonic()
    out = recovery_copies._copy_sftp(recovery_file, _url(sshd))

    assert out is not None and "sftp failed" in out
    assert time.monotonic() - started < 20  # BatchMode: it must fail, not wait on a prompt
    assert not (sshd.root / "osiris-recovery").exists()


def test_a_key_the_server_does_not_trust_is_refused(
    sshd: Sshd, recovery_file: Path, tmp_path: Path,
) -> None:
    other = tmp_path / "other_key"
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(other)],
                   check=True, timeout=30)
    sshd.config.write_text(sshd.config.read_text().replace(str(sshd.client_key), str(other)))

    out = recovery_copies._copy_sftp(recovery_file, _url(sshd))

    assert out is not None and "sftp failed" in out
    assert not (sshd.root / "osiris-recovery").exists()


def test_a_closed_port_fails_promptly(sshd: Sshd, recovery_file: Path) -> None:
    sshd.config.write_text(sshd.config.read_text().replace(
        f"Port {sshd.port}", f"Port {_free_port()}"))

    started = time.monotonic()
    out = recovery_copies._copy_sftp(recovery_file, _url(sshd))

    assert out is not None and "sftp failed" in out
    assert time.monotonic() - started < 20


def test_the_whole_target_path_records_a_receipt_and_counts_as_off_box(
    sshd: Sshd, recovery_file: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """copy_to_target end to end through the real copier: receipt written, the destination
    holds the CURRENT file, and a second pass inside a week does not touch the server."""
    monkeypatch.setenv(recovery_copies._RECEIPTS_ENV, str(tmp_path / "copies.json"))
    monkeypatch.setattr(recovery_copies, "source_file", lambda **kw: recovery_file)
    target = {"name": "nas", "kind": "restic", "path_or_url": _url(sshd)}

    first = recovery_copies.copy_to_target(target)
    receipt = recovery_copies.read_copy_receipts()["nas"]

    assert first == {"dest": "nas", "ok": True}
    assert receipt["last_error"] is None and receipt["sha256"]
    assert (sshd.root / "osiris-recovery" / "soul.key.recovery.json").exists()
    shutil.rmtree(sshd.root / "osiris-recovery")  # if it were touched again, it would reappear
    assert recovery_copies.copy_to_target(target) is None
    assert not (sshd.root / "osiris-recovery").exists()
    assert os.environ["OSIRIS_SSH_CONFIG"] == str(sshd.config)
