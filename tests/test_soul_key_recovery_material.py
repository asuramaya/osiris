"""THE BROWSER RECOVERY MATERIAL DOOR (Thoth mail 13002, THE KEY PANEL piece 2) — API
tests for the four new REST routes (src/orchestrator/soul_key_recovery_material.py):
POST /soul-key/recovery-material (issue), POST /soul-key/recovery-material/complete
(persist the browser-wrapped blob), GET /soul-key/recovery-blob (read for the reverse
direction), POST /soul-key/recover-from-browser (reseal a recovered key). A NEW file,
not an edit to Khnum's own tests/test_api.py — same "new code paths only" law the
routes themselves hold.

Own file rather than mirroring the FIDO2/PRF ceremony itself: that half is genuinely
untestable without real hardware (soul_crypto.py's own tests fake the `fido2` device
object; this module never calls into `fido2` at all — the browser does that part, this
module only ever sees base64 strings). What's tested here is everything on THIS side
of that boundary: token issue/consume/expiry, fingerprint verification, refuse-on-
existing-blob/key, and the exact shape handed to and read from the browser."""
from __future__ import annotations

import base64
import hashlib
import time
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from cryptography.fernet import Fernet
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


def _fp(raw_key: bytes) -> str:
    return hashlib.sha256(raw_key).hexdigest()[:16]


# --- issue_recovery_material / POST /soul-key/recovery-material -----------------------

async def test_issue_material_refuses_when_no_key_exists(
    client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(tmp_path / "no-such-file"))
    r = await client.post("/soul-key/recovery-material", json={})
    body = r.json()
    assert "error" in body
    assert "init" in body["error"]


async def test_issue_material_hands_back_the_raw_key_and_a_token(
    client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    key_file = tmp_path / "soul.key"
    raw = Fernet.generate_key()
    key_file.write_bytes(raw)
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(key_file))
    r = await client.post("/soul-key/recovery-material", json={})
    body = r.json()
    assert "error" not in body
    assert base64.urlsafe_b64decode(body["raw_key"]) == raw
    assert body["token"]
    assert body["expires_in_seconds"] == 60.0


async def test_issue_material_refuses_when_a_recovery_blob_already_exists(
    client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    key_file = tmp_path / "soul.key"
    key_file.write_bytes(Fernet.generate_key())
    (tmp_path / "soul.key.recovery.json").write_text("{}")
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(key_file))
    r = await client.post("/soul-key/recovery-material", json={})
    body = r.json()
    assert "error" in body
    assert "already exists" in body["error"]


# --- write_recovery_blob / POST /soul-key/recovery-material/complete ------------------

async def test_complete_enrollment_writes_the_same_shape_the_cli_writes(
    client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    key_file = tmp_path / "soul.key"
    raw = Fernet.generate_key()
    key_file.write_bytes(raw)
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(key_file))
    issued = (await client.post("/soul-key/recovery-material", json={})).json()
    r = await client.post("/soul-key/recovery-material/complete", json={
        "token": issued["token"], "credential_id": "Y3JlZA", "salt": "c2FsdA",
        "wrapped_key": "d3JhcHBlZA", "key_fingerprint": _fp(raw), "rp_id": "osiris.local"})
    body = r.json()
    assert "error" not in body
    recovery_path = tmp_path / "soul.key.recovery.json"
    assert recovery_path.is_file()
    import json as _json
    on_disk = _json.loads(recovery_path.read_text())
    assert on_disk == {"credential_id": "Y3JlZA", "salt": "c2FsdA",
                        "wrapped_key": "d3JhcHBlZA", "key_fingerprint": _fp(raw),
                        "rp_id": "osiris.local"}


async def test_complete_enrollment_refuses_a_fingerprint_mismatch(
    client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    key_file = tmp_path / "soul.key"
    key_file.write_bytes(Fernet.generate_key())
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(key_file))
    issued = (await client.post("/soul-key/recovery-material", json={})).json()
    r = await client.post("/soul-key/recovery-material/complete", json={
        "token": issued["token"], "credential_id": "Y3JlZA", "salt": "c2FsdA",
        "wrapped_key": "d3JhcHBlZA", "key_fingerprint": "0" * 16, "rp_id": "osiris.local"})
    body = r.json()
    assert "error" in body
    assert "does not match" in body["error"]
    assert not (tmp_path / "soul.key.recovery.json").exists()


async def test_complete_enrollment_token_is_single_use(
    client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    key_file = tmp_path / "soul.key"
    raw = Fernet.generate_key()
    key_file.write_bytes(raw)
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(key_file))
    issued = (await client.post("/soul-key/recovery-material", json={})).json()
    body = {"token": issued["token"], "credential_id": "Y3JlZA", "salt": "c2FsdA",
            "wrapped_key": "d3JhcHBlZA", "key_fingerprint": _fp(raw), "rp_id": "osiris.local"}
    r1 = await client.post("/soul-key/recovery-material/complete", json=body)
    assert "error" not in r1.json()
    (tmp_path / "soul.key.recovery.json").unlink()  # simulate a second attempt's own target
    r2 = await client.post("/soul-key/recovery-material/complete", json=body)
    body2 = r2.json()
    assert "error" in body2
    assert "no matching, unexpired" in body2["error"]


async def test_complete_enrollment_refuses_a_stale_or_unknown_token(
    client: httpx.AsyncClient,
) -> None:
    r = await client.post("/soul-key/recovery-material/complete", json={
        "token": "not-a-real-token", "credential_id": "Y3JlZA", "salt": "c2FsdA",
        "wrapped_key": "d3JhcHBlZA", "key_fingerprint": "0" * 16, "rp_id": "osiris.local"})
    body = r.json()
    assert "error" in body
    assert "no matching, unexpired" in body["error"]


async def test_material_token_expires_after_its_own_ttl(
    client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.orchestrator.soul_key_recovery_material as mod

    key_file = tmp_path / "soul.key"
    raw = Fernet.generate_key()
    key_file.write_bytes(raw)
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(key_file))
    issued = (await client.post("/soul-key/recovery-material", json={})).json()
    # backdate the parked issue time past the 60s window rather than sleeping in a test
    assert mod._material_state is not None
    mod._material_state["issued_at"] = time.monotonic() - mod._MATERIAL_TTL_SECONDS - 1
    r = await client.post("/soul-key/recovery-material/complete", json={
        "token": issued["token"], "credential_id": "Y3JlZA", "salt": "c2FsdA",
        "wrapped_key": "d3JhcHBlZA", "key_fingerprint": _fp(raw), "rp_id": "osiris.local"})
    body = r.json()
    assert "error" in body
    assert "no matching, unexpired" in body["error"]


# --- read_recovery_blob / GET /soul-key/recovery-blob ----------------------------------

async def test_read_recovery_blob_refuses_when_a_key_already_exists(
    client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    key_file = tmp_path / "soul.key"
    key_file.write_bytes(Fernet.generate_key())
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(key_file))
    r = await client.get("/soul-key/recovery-blob")
    body = r.json()
    assert "error" in body
    assert "already exists" in body["error"]


async def test_read_recovery_blob_refuses_when_none_enrolled(
    client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(tmp_path / "no-such-file"))
    r = await client.get("/soul-key/recovery-blob")
    body = r.json()
    assert "error" in body
    assert "no recovery enrollment found" in body["error"]


async def test_read_recovery_blob_returns_the_blob_verbatim(
    client: httpx.AsyncClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    key_file = tmp_path / "soul.key"
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", str(key_file))
    blob = {"credential_id": "Y3JlZA", "salt": "c2FsdA", "wrapped_key": "d3JhcHBlZA",
            "key_fingerprint": "abc123", "rp_id": "osiris.local"}
    import json as _json
    (tmp_path / "soul.key.recovery.json").write_text(_json.dumps(blob))
    r = await client.get("/soul-key/recovery-blob")
    assert r.json() == blob


# --- recover_from_browser / POST /soul-key/recover-from-browser -----------------------

async def test_recover_from_browser_seals_the_key_at_the_target_path(
    client: httpx.AsyncClient, tmp_path: Path,
) -> None:
    resolved = tmp_path / "recovered.key"
    raw = Fernet.generate_key()
    r = await client.post("/soul-key/recover-from-browser", json={
        "raw_key": base64.urlsafe_b64encode(raw).decode(), "key_fingerprint": _fp(raw),
        "resolved_path": str(resolved), "backend": "file"})
    body = r.json()
    assert "error" not in body
    assert body["backend"] == "file"
    assert resolved.read_bytes() == raw


async def test_recover_from_browser_refuses_a_fingerprint_mismatch(
    client: httpx.AsyncClient, tmp_path: Path,
) -> None:
    resolved = tmp_path / "recovered.key"
    raw = Fernet.generate_key()
    r = await client.post("/soul-key/recover-from-browser", json={
        "raw_key": base64.urlsafe_b64encode(raw).decode(), "key_fingerprint": "0" * 16,
        "resolved_path": str(resolved), "backend": "file"})
    body = r.json()
    assert "error" in body
    assert "does not match" in body["error"]
    assert not resolved.exists()


async def test_recover_from_browser_refuses_when_a_key_already_exists(
    client: httpx.AsyncClient, tmp_path: Path,
) -> None:
    resolved = tmp_path / "recovered.key"
    resolved.write_bytes(Fernet.generate_key())
    raw = Fernet.generate_key()
    r = await client.post("/soul-key/recover-from-browser", json={
        "raw_key": base64.urlsafe_b64encode(raw).decode(), "key_fingerprint": _fp(raw),
        "resolved_path": str(resolved), "backend": "file"})
    body = r.json()
    assert "error" in body
    assert "already exists" in body["error"]
