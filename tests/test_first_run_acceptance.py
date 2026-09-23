"""THE FIRST-RUN ACCEPTANCE TEST: the
operator runs one sequence by hand on a fresh box. `osiris soul-key init --restart`
-> `soul-key enroll-recovery` -> `osiris restic-key init` -> `osiris backup-settings
write --offload-add` for the NAS (restic, off-box) and the docked drive (local, an
`expected_mountpoint`) -> `osiris offload-runner tick` -> a restore drill proving the
backup is actually recoverable. This test walks that EXACT sequence through the real
async CLI entry points this session already built/gates (`cmd_soul_key`,
`cmd_restic_key`, `cmd_backup_settings`, `cmd_offload_runner`, `cmd_backup_status`),
never a shell subprocess, never the underlying orchestrator functions directly, in an
`XDG_CONFIG_HOME`-isolated credstore so this developer's own real credentials are never
touched (the same isolation `test_soul_crypto.py`/`test_restic_credential.py` already
use).

BACKEND SELECTION STAYS HONEST: `backend=None` (auto) on every mint call below, never
forced; this box genuinely has `systemd-creds` (confirmed: `systemd-creds --version`
reports systemd 259 with the FIDO2/TPM2 feature flags), so the real host-cred/host+tpm2
ladder resolves for real, asserted conditionally on `shutil.which("systemd-creds")` so
this file still means something honest on a box that lacks it (the file-backend branch,
`_resolve_backend`'s own documented "warned escape hatch").

FIDO2 is stubbed at the exact boundary `test_soul_crypto.py` already established
(`soul_crypto._find_fido2_device`/`soul_crypto._fido2_client`), copied here rather than
imported cross-file, matching this suite's own convention of keeping each test file's
fixtures self-contained.

THE NAMED GAP THIS SEQUENCE ONCE HAD IS CLOSED (obligation 8971e4e5): `scripts.
osiris_offbox_restore_drill.run_drill` used to read `RESTIC_PASSWORD` from the ambient
process environment directly, bypassing `restic_credential.get_restic_password()`'s
systemd-creds-aware resolution the offload runner itself uses; this test used to bridge
that gap by hand. run_drill now resolves the password itself through that same shared
ladder, so step 6 below proves the drill reads the SAME credential the tick in step 5
already used, with no manual bridging."""
from __future__ import annotations

import hashlib
import hmac
import json
import shutil
from pathlib import Path
from typing import Any

import pytest
from src import cli
from src.actions.core import Actions
from src.cli import (
    cmd_backup_settings,
    cmd_backup_status,
    cmd_offload_runner,
    cmd_restic_key,
    cmd_soul_key,
)
from src.ingest import soul_crypto

# --- FIDO2 fake, copied verbatim from tests/test_soul_crypto.py's own convention -------

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
    """PIN + touch faked outright; PRF bytes are DETERMINISTIC per credential+salt (an
    HMAC over the salt keyed by a fixed per-instance secret), matching the real
    extension's own contract closely enough to prove the wrap/unwrap plumbing without
    physical hardware, the same fake `test_soul_crypto.py` already established."""

    def __init__(self, device_secret: bytes = b"fake-device-secret") -> None:
        self._device_secret = device_secret
        self._credential_id = b"fake-credential-id-0123456789ab"

    def make_credential(self, options: Any) -> _FakeRegistration:
        return _FakeRegistration(self._credential_id)

    def get_assertion(self, options: Any) -> _FakeAssertionSelection:
        salt = options.extensions["prf"]["eval"]["first"]
        prf_output = hmac.new(self._device_secret, salt, hashlib.sha256).digest()
        return _FakeAssertionSelection(prf_output)


@pytest.fixture(autouse=True)
def _isolate_credential_stores(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """THE FIRST KEY MUST COME FROM THE NORMAL CLI: every mint call
    below uses the DEFAULT (no explicit --path) credstore location, the box's own real
    per-user credstore unless redirected, so `XDG_CONFIG_HOME` isolation is not
    optional here, it is the whole point (the same real-credential-pollution incident
    `test_soul_crypto.py`'s own fixture docstring names)."""
    monkeypatch.delenv("OSIRIS_SOUL_KEY", raising=False)
    monkeypatch.delenv("OSIRIS_SOUL_KEY_LEGACY", raising=False)
    monkeypatch.delenv("OSIRIS_SOUL_KEY_FILE", raising=False)
    monkeypatch.delenv("OSIRIS_RESTIC_PASSWORD", raising=False)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdgcfg"))
    # a real installed osiris-mcp.service on THIS developer's own box must never leak
    # into this test's own backend resolution (test_soul_crypto.py's own fixture, same
    # reasoning: a controlled input, not whatever happens to be installed right now).
    monkeypatch.setattr(soul_crypto, "_installed_user_unit_env_value", lambda _name: None)


def _last_json_line(captured_out: str) -> dict[str, Any]:
    """`cli_render.emit(..., as_json=True)` prints one compact JSON line; reading it
    back through stdout (never the underlying dict directly) is what keeps this test
    honestly exercising the REAL CLI entry point's own output contract, not a shortcut
    around it."""
    lines = [ln for ln in captured_out.strip().splitlines() if ln]
    assert lines, "expected at least one JSON line on stdout"
    return dict(json.loads(lines[-1]))


async def test_first_run_acceptance_sequence(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from src.orchestrator import backup_validation
    from src.orchestrator import offload_runner as offload_runner_module

    # ── 1. `osiris soul-key init --restart` ────────────────────────────────────────
    restart_calls: list[list[str]] = []

    async def _fake_restart(units: list[str]) -> tuple[int, str]:
        restart_calls.append(units)
        return 0, "ok"

    monkeypatch.setattr(cli, "_real_restart_services", _fake_restart)

    rc = await cmd_soul_key("init", restart=True, as_json=True)
    assert rc == 0
    init_out = _last_json_line(capsys.readouterr().out)
    assert restart_calls == [cli._SOUL_KEY_RESTART_UNITS]
    assert init_out["restarted"] is True
    # BACKEND SELECTION STAYS HONEST: never forced, asserted against what this box
    # genuinely has.
    if shutil.which("systemd-creds"):
        assert init_out["backend"] in ("host-cred", "host+tpm2")
    else:
        assert init_out["backend"] == "file"

    # THE NAMED REFUSAL: init twice never overwrites the live key in place.
    rc_twice = await cmd_soul_key("init", as_json=True)
    assert rc_twice == 1
    twice_out = _last_json_line(capsys.readouterr().out)
    assert "already exists" in twice_out["error"]

    # ── 2. `osiris soul-key enroll-recovery` (FIDO2 stubbed at the client boundary) ─
    fake_client = _FakeFido2Client()
    monkeypatch.setattr(soul_crypto, "_find_fido2_device", lambda: object())
    monkeypatch.setattr(soul_crypto, "_fido2_client", lambda device, rp_id: fake_client)

    rc = await cmd_soul_key("enroll-recovery", pool=actions.pool, as_json=True)
    assert rc == 0
    enroll_out = _last_json_line(capsys.readouterr().out)
    assert "error" not in enroll_out

    # ── 3. `osiris restic-key init` ─────────────────────────────────────────────────
    rc = await cmd_restic_key("init", as_json=True)
    assert rc == 0
    restic_init_out = _last_json_line(capsys.readouterr().out)
    if shutil.which("systemd-creds"):
        assert restic_init_out["backend"] in ("host-cred", "host+tpm2")
    else:
        assert restic_init_out["backend"] == "file"

    # THE NAMED REFUSAL: restic-key init twice never overwrites the live credential.
    rc_twice = await cmd_restic_key("init", as_json=True)
    assert rc_twice == 1
    restic_twice_out = _last_json_line(capsys.readouterr().out)
    assert "already exists" in restic_twice_out["error"]

    # ── 4. `osiris backup-settings write --offload-add` (NAS + docked drive) ───────
    # THE DOCKED DRIVE: kind=local, a real `expected_mountpoint` this test controls
    # directly (create/remove the directory to flip presence), backed by a genuine
    # local restic repository under it.
    drive_mount = tmp_path / "drive-mount"
    drive_mount.mkdir()
    drive_repo = drive_mount / "restic-repo"
    rc = await cmd_backup_settings(
        "write", offload_add="docked-drive", offload_kind="local",
        offload_target=f"local:{drive_repo}", offload_mountpoint=str(drive_mount),
        offload_schedule="Sat 03:00:00", because="first-run acceptance test",
        actor="operator", pool=actions.pool, as_json=True)
    assert rc == 0
    capsys.readouterr()

    # THE NAS: kind=restic, off-box, no mountpoint to check. A REAL sftp target has
    # no server in a hermetic test; substituted with a REST-backend URL pointing at a
    # closed local port (127.0.0.1:1, connection refused near-instantly, no DNS
    # lookup, no timeout wait) so the runner's own "reachability failure is treated
    # identically to an absent mountpoint: skip, record, never raise" law (offload_
    # runner.py's own module docstring) is exercised with a REAL, fast subprocess
    # failure rather than a monkeypatched one.
    rc = await cmd_backup_settings(
        "write", offload_add="nas", offload_kind="restic",
        offload_target="rest:http://127.0.0.1:1/repo", offload_schedule="Sun 04:00:00",
        because="first-run acceptance test", actor="operator", pool=actions.pool,
        as_json=True)
    assert rc == 0
    capsys.readouterr()

    # ── 5. `osiris offload-runner tick`: tick #1, the drive's mountpoint present ──
    receipts_file = tmp_path / "receipts.json"
    monkeypatch.setenv(offload_runner_module._RECEIPTS_ENV, str(receipts_file))
    # THE PRESENCE CHECK: real `findmnt` finds no genuine kernel mountpoint under a
    # tmp_path directory (there IS no real docked drive on a CI/dev box); the real
    # branching logic in run_offload_tick is still exercised for real; only the
    # kernel-mount PROBE itself is substituted for a plain existence check, matching
    # what this test can actually control without root.
    monkeypatch.setattr(
        backup_validation, "check_local_target_presence",
        lambda mp: {"present": Path(mp).exists(), "writable": True, "free_bytes": 10**9})

    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "real-file.txt").write_text("real vault content, first-run acceptance test")

    rc = await cmd_offload_runner("tick", vault=str(vault), pool=actions.pool, as_json=True)
    assert rc == 0
    tick1_out = _last_json_line(capsys.readouterr().out)
    by_name = {t["name"]: t for t in tick1_out["targets"]}
    assert by_name["docked-drive"]["ok"] is True
    assert by_name["nas"]["ok"] is False  # a real, fast connection-refused failure
    assert "nas" in by_name and "error" in by_name["nas"]

    receipts = offload_runner_module.offload_receipts()
    assert receipts["docked-drive"]["last_successful_offload"] is not None
    assert receipts["docked-drive"]["last_error"] is None
    # a target that has NEVER once succeeded carries no `last_successful_offload` key
    # at all yet (`_write_receipt`'s own merge: a failed attempt only ever adds
    # `last_attempt_at`/`last_error`, never invents a success key with a None value).
    assert receipts["nas"].get("last_successful_offload") is None
    assert receipts["nas"]["last_error"] is not None

    # ── 6. restore drill against the docked drive's own real local repository ──────
    import scripts.osiris_offbox_restore_drill as drill_module

    # `run_drill` resolves the password itself now (restic_credential.get_restic_
    # password, the SAME systemd-creds-aware ladder the offload runner above already
    # used), reading the SAME credential this drive's own tick just proved works, no
    # manual bridging needed.
    # `run_drill` returns a failure STRING, or None on success (its own docstring),
    # and cleans up its own scratch directory in a `finally` regardless of outcome,
    # so success is proved by the None return alone: internally it already refuses
    # to call a check-clean-but-empty repository a pass (zero restored files is its
    # own named failure string, never silently treated as success).
    scratch = tmp_path / "restore-scratch"
    drill_fail = drill_module.run_drill(f"local:{drive_repo}", scratch=scratch)
    assert drill_fail is None, drill_fail

    # ── 7. `osiris backup-status` reads the SAME receipts the runner wrote ─────────
    rc = await cmd_backup_status(pool=actions.pool, vault=str(vault), as_json=True)
    assert rc == 0
    status_out = _last_json_line(capsys.readouterr().out)
    status_targets = {t["name"]: t for t in status_out["offbox"]["offload_targets"]}
    assert (status_targets["docked-drive"]["last_successful_offload"]
           == receipts["docked-drive"]["last_successful_offload"])
    assert status_targets["docked-drive"]["last_error"] is None
    assert status_targets["nas"]["last_error"] == receipts["nas"]["last_error"]

    # ── 8. tick #2: the drive's mountpoint is now ABSENT: SKIPPED, never an error ─
    shutil.rmtree(drive_mount)
    rc = await cmd_offload_runner("tick", vault=str(vault), pool=actions.pool, as_json=True)
    assert rc == 0
    tick2_out = _last_json_line(capsys.readouterr().out)
    tick2_by_name = {t["name"]: t for t in tick2_out["targets"]}
    assert tick2_by_name["docked-drive"] == {
        "name": "docked-drive", "skipped": "not present (mountpoint absent)"}
    # THE SKIP NEVER CLOBBERS THE PRIOR SUCCESS (offload_runner._write_receipt's own
    # merge law); no _write_receipt call happens at all on a skip, so the receipt
    # from tick #1 survives byte-for-byte.
    receipts_after = offload_runner_module.offload_receipts()
    assert (receipts_after["docked-drive"]["last_successful_offload"]
           == receipts["docked-drive"]["last_successful_offload"])


async def test_offload_tick_with_no_restic_password_names_it(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE NAMED REFUSAL, at the orchestrator layer: a tick with no restic password
    degrades the WHOLE tick with `ResticPasswordMissing` named directly in the
    returned dict's `error` key, rather than a confusing per-target failure repeated
    N times (`run_offload_tick`'s own module docstring). No restic-key init ever ran
    in this test's own isolated XDG_CONFIG_HOME; genuinely no credential exists.
    The sibling test below proves the SAME thing through the real CLI entry point."""
    from src.orchestrator.offload_runner import run_offload_tick
    from src.orchestrator.restic_credential import ResticPasswordMissing

    out = await run_offload_tick(actions.pool, vault=tmp_path)
    assert out["targets"] == []
    assert "error" in out
    # the exception's own str() is what "named" means here, the same message
    # ResticPasswordMissing itself raises, never re-worded in transit.
    try:
        from src.orchestrator.restic_credential import get_restic_password
        get_restic_password()
        pytest.fail("expected ResticPasswordMissing, a real credential was found")
    except ResticPasswordMissing as exc:
        assert out["error"] == str(exc)


async def test_cmd_offload_runner_tick_with_no_restic_password_names_it_in_the_receipt(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """THE REAL CLI-LAYER CONTRACT: `cmd_offload_runner` never raises; it reports
    `out["error"]` naming `ResticPasswordMissing`'s own message and exits 1, because
    nothing here ever ran `osiris restic-key init`, so genuinely no credential exists
    under this test's own isolated XDG_CONFIG_HOME."""
    from src.orchestrator import offload_runner as offload_runner_module

    monkeypatch.setenv(offload_runner_module._RECEIPTS_ENV,
                       str(tmp_path / "receipts.json"))
    rc = await cmd_offload_runner("tick", vault=str(tmp_path), pool=actions.pool,
                                  as_json=True)
    assert rc == 1
    out = _last_json_line(capsys.readouterr().out)
    assert "no restic password" in out["error"].lower() or "restic-key init" in out["error"]
    assert out["targets"] == []


async def test_cmd_offload_runner_tick_with_restic_absent_from_path_names_it(
    actions: Actions, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """THE NAMED REFUSAL: restic absent from PATH surfaces per-target (never a whole-
    tick error; the runner has no separate "is restic installed" probe; the SAME
    subprocess call that would run the backup fails with FileNotFoundError, caught and
    named in the receipt, exactly like any other restic failure, never a special
    case, per run_offload_tick's own module docstring)."""
    from src.orchestrator import backup_validation
    from src.orchestrator import offload_runner as offload_runner_module

    rc = await cmd_restic_key("init", as_json=True)
    assert rc == 0
    capsys.readouterr()

    # THE PASSWORD RESOLVES *BEFORE* PATH GOES DARK: `get_restic_password()`'s own
    # systemd-creds read is a subprocess call too; stripping PATH would blind THAT
    # as well, testing "no systemd-creds" instead of the "no restic binary" this test
    # actually names. Read the real credential now, hand it back via the SAME
    # `OSIRIS_RESTIC_PASSWORD` env override `get_restic_password`'s own ladder already
    # honors, THEN strip PATH so only the restic binary itself goes missing.
    from src.orchestrator.restic_credential import get_restic_password

    monkeypatch.setenv("OSIRIS_RESTIC_PASSWORD", get_restic_password().decode())

    monkeypatch.setenv(offload_runner_module._RECEIPTS_ENV,
                       str(tmp_path / "receipts.json"))
    monkeypatch.setattr(backup_validation, "check_local_target_presence",
                        lambda mp: {"present": True, "writable": True,
                                   "free_bytes": 10**9})
    (tmp_path / "empty-bin").mkdir()
    monkeypatch.setenv("PATH", str(tmp_path / "empty-bin"))  # restic unreachable

    drive = tmp_path / "drive"
    drive.mkdir()
    await cmd_backup_settings(
        "write", offload_add="no-restic-drive", offload_kind="local",
        offload_target=f"local:{tmp_path / 'repo'}", offload_mountpoint=str(drive),
        offload_schedule="Sat 03:00:00", because="restic-absent test",
        actor="operator", pool=actions.pool, as_json=True)
    capsys.readouterr()

    vault = tmp_path / "vault"
    vault.mkdir()
    rc = await cmd_offload_runner("tick", vault=str(vault), pool=actions.pool,
                                  as_json=True)
    assert rc == 0  # a per-target failure, never a whole-tick error
    out = _last_json_line(capsys.readouterr().out)
    target = out["targets"][0]
    assert target["ok"] is False
    assert "FileNotFoundError" in target["error"]
