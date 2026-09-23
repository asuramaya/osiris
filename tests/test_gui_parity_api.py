"""GUI PARITY FOR THE KEY AND OFFLOAD LANES (thread dd11ab34, Thoth mail 13472) — API
tests for the two genuinely new routes: POST /restic-key/init (mirrors `osiris
restic-key init`) and POST /offload-runner/tick (mirrors `osiris offload-runner tick`,
the same tick osiris-offload.timer runs). Neither carries actor/because — the CLI
doors they mirror don't either (verified: cmd_restic_key/cmd_offload_runner take
neither); the write door that DOES require `because` is POST /backup-settings itself,
already covered by tests/test_settings_pane_api.py's own predecessor coverage and the
frontend's own saveOffloadTargets() prompt.

TWO REAL BUGS FOUND AND FIXED WHILE BUILDING THE OFFLOAD-RUNNER TICK TEST (needed a
real, saved target to exercise "skip a disabled one" against): (1) BackupSettingsBody
(src/api/app.py) never had an `offload_targets` field — every POST /backup-settings
the Offload Targets panel's own Save button has ever made silently wrote nothing for
that field and reported success anyway (Pydantic drops an unrecognized body key by
default). (2) settings_service._validate_value's generic per-field "records" check
rejected `None` unconditionally for every "str"-typed field, running BEFORE the
spec's own custom `validate` callback — so `_validate_offload_targets`'s own correct
per-kind rule (`expected_mountpoint` null for kind='restic') never got a chance to
run; every restic-kind offload_targets write has been refused since the field
existed. Both fixed in this tip; test_offload_targets_round_trips_a_restic_target
below is the dedicated regression proof for (2) (the round-trip itself already
exercises (1)).

A new file, not an edit to any existing test file with its own established convention."""
from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app

HELPERS = Path(__file__).parent.parent / "helpers"


@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    from src.orchestrator.manifests import load_manifests

    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = load_manifests(HELPERS)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# --- POST /restic-key/init --------------------------------------------------------------

async def test_restic_key_init_route_writes_a_credential(
    client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pw_file = tmp_path / "restic.password"
    monkeypatch.setenv("OSIRIS_RESTIC_PASSWORD_FILE", str(pw_file))
    r = await client.post("/restic-key/init", json={"backend": "file"})
    assert r.status_code == 200
    body = r.json()
    assert "error" not in body
    assert body["path"] == str(pw_file)
    assert pw_file.is_file()
    assert pw_file.read_bytes()  # a real password, never empty


async def test_restic_key_init_route_never_returns_the_password_bytes(
    client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pw_file = tmp_path / "restic.password"
    monkeypatch.setenv("OSIRIS_RESTIC_PASSWORD_FILE", str(pw_file))
    r = await client.post("/restic-key/init", json={"backend": "file"})
    written = pw_file.read_bytes()
    assert written.decode() not in r.text


async def test_restic_key_init_route_refuses_when_a_credential_already_exists(
    client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pw_file = tmp_path / "restic.password"
    pw_file.write_bytes(b"already-here")
    monkeypatch.setenv("OSIRIS_RESTIC_PASSWORD_FILE", str(pw_file))
    r = await client.post("/restic-key/init", json={})
    body = r.json()
    assert "error" in body
    assert "already exists" in body["error"]
    assert pw_file.read_bytes() == b"already-here"  # untouched


# --- POST /offload-runner/tick ------------------------------------------------------------

async def test_offload_runner_tick_route_degrades_with_no_restic_password(
    client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_RESTIC_PASSWORD_FILE", str(tmp_path / "no-such-file"))
    r = await client.post("/offload-runner/tick", json={})
    assert r.status_code == 200
    body = r.json()
    assert "error" in body
    assert body["targets"] == []


async def test_offload_runner_tick_route_reports_no_targets_configured(
    client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pw_file = tmp_path / "restic.password"
    pw_file.write_bytes(b"a-real-password")
    monkeypatch.setenv("OSIRIS_RESTIC_PASSWORD_FILE", str(pw_file))
    r = await client.post("/offload-runner/tick", json={})
    body = r.json()
    assert "error" not in body
    assert body["targets"] == []


async def test_offload_runner_tick_route_skips_a_disabled_target(
    client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pw_file = tmp_path / "restic.password"
    pw_file.write_bytes(b"a-real-password")
    monkeypatch.setenv("OSIRIS_RESTIC_PASSWORD_FILE", str(pw_file))
    write = await client.post("/backup-settings", json={
        "offload_targets": [{"name": "nas", "kind": "restic", "path_or_url": "sftp:x:/y",
                              "expected_mountpoint": None, "schedule": "daily",
                              "enabled": False}],
        "because": "test setup"})
    assert "error" not in write.json()
    r = await client.post("/offload-runner/tick", json={})
    body = r.json()
    assert body["targets"] == [{"name": "nas", "skipped": "disabled"}]


# --- the two real bugs found while building the tests above, regression-pinned -------

async def test_offload_targets_round_trips_a_restic_target(client: httpx.AsyncClient) -> None:
    # bug (1): BackupSettingsBody had no offload_targets field at all -- this write
    # used to silently do nothing and report success. bug (2): the generic records
    # validator rejected expected_mountpoint=None for EVERY kind, so even a fixed (1)
    # would still refuse every restic-kind row. Both must be fixed for this to pass.
    row = {"name": "nas", "kind": "restic",
           "path_or_url": "sftp:truenas:/mnt/Bunker/Vault/osiris-vault/restic",
           "expected_mountpoint": None, "schedule": "daily", "enabled": True}
    write = await client.post("/backup-settings", json={
        "offload_targets": [row], "because": "regression test"})
    body = write.json()
    assert "error" not in body, body
    assert body["offload_targets"][0]["name"] == "nas"
    assert body["offload_targets"][0]["expected_mountpoint"] is None
    read = await client.get("/backup-settings")
    assert read.json()["offload_targets"][0]["path_or_url"] == row["path_or_url"]


async def test_offload_targets_still_refuses_a_local_target_with_no_mountpoint(
    client: httpx.AsyncClient,
) -> None:
    # the fix must not weaken the custom validator's own real requirement: a
    # kind='local' row genuinely NEEDS a non-null expected_mountpoint.
    row = {"name": "drive", "kind": "local", "path_or_url": "/mnt/vault",
           "expected_mountpoint": None, "schedule": "daily", "enabled": True}
    write = await client.post("/backup-settings", json={
        "offload_targets": [row], "because": "regression test"})
    body = write.json()
    assert "error" in body
    assert "expected_mountpoint" in body["error"]
