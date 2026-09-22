"""THE BACKUP TOPOLOGY VALIDATOR (Thoth mail 12809/12812) — src/orchestrator/
backup_validation.py, one shared module for the CLI/MCP/REST doors. Every test mocks the
real filesystem/mount state it depends on (`_findmnt_mountpoint`, `_is_always_present_
mountpoint`) rather than asserting against this host's own actual fstab/mount table —
deterministic regardless of what box the suite runs on."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from src.orchestrator import backup_validation as bv


def test_validate_vault_path_refuses_a_relative_path() -> None:
    out = bv.validate_vault_path("relative/path")
    assert out["ok"] is False
    assert out["checks"]["absolute"] is False
    assert any("absolute" in e for e in out["errors"])


def test_validate_vault_path_refuses_a_missing_path(monkeypatch: Any) -> None:
    monkeypatch.setattr(bv, "_is_always_present_mountpoint", lambda p: True)
    out = bv.validate_vault_path("/no/such/path/at/all/ever")
    assert out["ok"] is False
    assert out["checks"]["exists"] is False
    assert any("does not exist" in e for e in out["errors"])


def test_validate_vault_path_refuses_an_unwritable_path(
    tmp_path: Path, monkeypatch: Any
) -> None:
    monkeypatch.setattr(bv, "_is_always_present_mountpoint", lambda p: True)
    target = tmp_path / "vault"
    target.mkdir(mode=0o500)  # read+execute, no write
    try:
        out = bv.validate_vault_path(str(target))
        assert out["checks"]["exists"] is True
        assert out["checks"]["writable"] is False
        assert any("not writable" in e for e in out["errors"])
    finally:
        target.chmod(0o700)  # restore so tmp_path cleanup can remove it


def test_validate_vault_path_refuses_a_path_under_no_declared_always_present_mount(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """THE HOT-PATH LAW (Thoth mail 12812): a real, writable, on-disk path is STILL
    refused if it doesn't sit under an fstab/systemd-declared always-present mountpoint
    — the exact shape of the docked-drive/NAS mistake this check exists to catch."""
    monkeypatch.setattr(bv, "_is_always_present_mountpoint", lambda p: False)
    out = bv.validate_vault_path(str(tmp_path))
    assert out["ok"] is False
    assert out["checks"]["exists"] is True
    assert out["checks"]["writable"] is True
    assert out["checks"]["always_present_mount"] is False
    assert any("does not sit under a mountpoint" in e for e in out["errors"])


def test_validate_vault_path_ok_when_every_check_passes(
    tmp_path: Path, monkeypatch: Any
) -> None:
    monkeypatch.setattr(bv, "_is_always_present_mountpoint", lambda p: True)
    out = bv.validate_vault_path(str(tmp_path))
    assert out["ok"] is True
    assert out["errors"] == []
    assert out["checks"]["free_bytes"] is not None and out["checks"]["free_bytes"] > 0


def test_validate_vault_path_warns_never_refuses_on_same_device_as_root(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """"warn, never refuse" (Thoth's own exact wording, mail 12812) — same_device_as_root
    lands in `warnings`, never `errors`, and never flips `ok` to False by itself."""
    monkeypatch.setattr(bv, "_is_always_present_mountpoint", lambda p: True)
    monkeypatch.setattr(bv, "_device_of", lambda p: 1)
    out = bv.validate_vault_path(str(tmp_path))
    assert out["checks"]["same_device_as_root"] is True
    assert any("same block device" in w for w in out["warnings"])
    assert out["errors"] == []
    assert out["ok"] is True


def test_is_always_present_mountpoint_checks_the_resolved_real_mountpoint(
    monkeypatch: Any,
) -> None:
    """A vault_path two directories below a real mount still counts — resolved off the
    LIVE mount table (`_real_mountpoint_of`, findmnt --target), not a string prefix
    walk (which cannot tell "same filesystem as a declared parent" from "nothing
    declared at all", the exact false-positive a naive walk up to "/" would hit)."""
    monkeypatch.setattr(bv, "_fstab_mountpoints", lambda: {"/mnt/always"})
    monkeypatch.setattr(bv, "_systemd_mount_mountpoints", lambda: set())
    monkeypatch.setattr(bv, "_real_mountpoint_of", lambda p: "/mnt/always")
    assert bv._is_always_present_mountpoint("/mnt/always/backups/vault") is True
    monkeypatch.setattr(bv, "_real_mountpoint_of", lambda p: "/mnt/elsewhere")
    assert bv._is_always_present_mountpoint("/mnt/elsewhere/vault") is False
    monkeypatch.setattr(bv, "_real_mountpoint_of", lambda p: None)
    assert bv._is_always_present_mountpoint("/no/such/path") is False


def test_check_local_target_presence_absent(monkeypatch: Any) -> None:
    monkeypatch.setattr(bv, "_findmnt_mountpoint", lambda p: False)
    out = bv.check_local_target_presence("/mnt/docked-drive")
    assert out == {"present": False, "writable": None, "free_bytes": None}


def test_check_local_target_presence_present(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(bv, "_findmnt_mountpoint", lambda p: True)
    out = bv.check_local_target_presence(str(tmp_path))
    assert out["present"] is True
    assert out["writable"] is True
    assert out["free_bytes"] is not None and out["free_bytes"] > 0


def test_validate_restic_url_accepts_scheme_prefixed_backends() -> None:
    for url in ("s3:s3.amazonaws.com/bucket", "b2:bucketname:path", "sftp:user@host:/repo",
               "rest:https://host:8000/", "swift:container:/repo", "azure:container:/repo",
               "gs:bucketname:/repo", "rclone:remote:/repo"):
        out = bv.validate_restic_url(url)
        assert out["url_shape_ok"] is True, url
        assert out["error"] is None


def test_validate_restic_url_accepts_a_bare_local_path() -> None:
    out = bv.validate_restic_url("/mnt/nas/restic-repo")
    assert out["url_shape_ok"] is True


def test_validate_restic_url_refuses_empty_and_shapeless_strings() -> None:
    assert bv.validate_restic_url("")["url_shape_ok"] is False
    out = bv.validate_restic_url("justaword")
    assert out["url_shape_ok"] is False
    assert "justaword" in out["error"]


def test_validate_restic_url_never_makes_a_network_call(monkeypatch: Any) -> None:
    """SHAPE ONLY, per Thoth's own repeated instruction — any socket/http attempt fails
    this test outright rather than silently succeeding."""
    import socket

    def _boom(*a: Any, **kw: Any) -> Any:
        raise AssertionError("validate_restic_url made a real network call")

    monkeypatch.setattr(socket, "socket", _boom)
    bv.validate_restic_url("s3:s3.amazonaws.com/bucket")
    bv.validate_restic_url("rest:https://host:8000/")
