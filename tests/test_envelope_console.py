"""The envelope panel's routes (src/orchestrator/envelope_console.py): who the soul key is sealed
to, the per-recipient drill (a security key's PRF output or a typed recovery code in the body),
adding
a code or a key, and removing one. Everything on THIS side of the browser's WebAuthn call: the
browser's own part (the touch, the PRF) is simulated by deriving a PRF output ourselves.

What these pin beyond the happy path: no secret ever comes back in a response, a log line or the
drill record; a token is single-use; a malformed body is never echoed; nothing answers off this
machine."""
from __future__ import annotations

import base64
import json
import logging
import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_asyncio
from cryptography.fernet import Fernet
from src.actions.core import Actions
from src.api.app import create_app
from src.ingest import soul_crypto, soul_envelope
from src.ingest.soul_crypto import key_fingerprint, recovery_file_path
from src.orchestrator import envelope_console, envelope_drill

KEY = Fernet.generate_key()
PRF = os.urandom(32)
SALT = os.urandom(32)
HELPERS = Path(__file__).parent.parent / "helpers"


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _hardware(rid: str = "k1", prf: bytes = PRF) -> dict[str, Any]:
    token = soul_crypto._hkdf_wrap_key(prf).encrypt(KEY)
    return {"id": rid, "kind": "fido2-prf", "label": "yubikey", "added_at": "2026-11-12T00:00:00Z",
            "last_opened_at": None, "credential_id": base64.urlsafe_b64encode(b"cred" * 8).decode(),
            "salt": base64.urlsafe_b64encode(SALT).decode(),
            "wrapped_key": base64.urlsafe_b64encode(token).decode(), "rp_id": "localhost"}


@pytest.fixture(autouse=True)
def _world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    keyfile = str(tmp_path / "soul.key")
    monkeypatch.setenv("OSIRIS_SOUL_KEY_FILE", keyfile)
    monkeypatch.setenv(envelope_drill._RECORD_ENV, str(tmp_path / "drill.json"))
    monkeypatch.setattr(soul_envelope, "_live_key", lambda p: KEY)
    envelope_console._PENDING.clear()
    return keyfile


def _write_envelope(recipients: list[dict[str, Any]]) -> Path:
    path = recovery_file_path()
    path.write_text(json.dumps({"version": 2, "key_fingerprint": key_fingerprint(KEY),
                                "recipients": recipients, "serial": len(recipients)}))
    return path


def _app_client(actions: Actions, host: str = "127.0.0.1") -> httpx.AsyncClient:
    from src.orchestrator.manifests import load_manifests

    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = load_manifests(HELPERS)
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=(host, 4321)), base_url="http://test")


@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    async with _app_client(actions) as c:
        yield c


@pytest.fixture
def sample(monkeypatch: pytest.MonkeyPatch) -> None:
    """A real sealed line stands in for the store's: the drill decrypts it with the opened key."""
    token = Fernet(KEY).encrypt(b"a real sealed line")

    def decrypts(key: bytes, _t: bytes) -> bool:
        try:
            Fernet(key).decrypt(token)
        except Exception:  # noqa: BLE001
            return False
        return True

    async def fake(pool: Any, **kw: Any) -> Any:
        return envelope_drill.Sample(token=token, decrypts=decrypts)

    monkeypatch.setattr(envelope_drill, "sealed_sample", fake)


async def test_the_panel_lists_who_the_key_is_sealed_to_without_any_secret(
    client: httpx.AsyncClient,
) -> None:
    first = (await client.get("/soul-key/envelope")).json()
    assert first["exists"] is False and first["recipients"] == []

    _write_envelope([_hardware()])
    resp = await client.get("/soul-key/envelope")
    body = resp.json()

    assert resp.headers["cache-control"] == "no-store"
    assert body["exists"] is True and body["stale_days"] == 30
    rec = body["recipients"][0]
    assert rec["id"] == "k1" and rec["rp_id"] == "localhost" and rec["kind"] == "fido2-prf"
    assert "never proved" in rec["verdict"]
    assert body["check"]["unproven"] == ["k1"] and body["check"]["hardware"] == 1
    assert "wrapped_key" not in resp.text and "credential_id" not in resp.text


async def test_a_security_key_drill_proves_the_recipient_and_records_it(
    client: httpx.AsyncClient, sample: None,
) -> None:
    path = _write_envelope([_hardware()])
    ch = (await client.get("/soul-key/envelope/k1/challenge")).json()
    assert ch["kind"] == "fido2-prf" and ch["rp_id"] == "localhost"
    assert ch["user_verification"] == "required" and ch["salt"] and ch["credential_id"]

    resp = await client.post("/soul-key/envelope/k1/drill",
                             json={"nonce": ch["nonce"], "prf": _b64(PRF)})
    body = resp.json()

    assert body["status"] == "proved" and body["ok"] is True
    assert body["fingerprint_matches"] is True and body["artifact_ok"] is True
    assert _b64(PRF) not in resp.text and KEY.decode() not in resp.text
    record = envelope_drill.read_record()["k1"]
    assert record["status"] == "proved" and record["last_proved_at"]
    assert _b64(PRF) not in json.dumps(record) and KEY.decode() not in path.read_text()
    assert json.loads(path.read_text())["recipients"][0]["last_opened_at"]  # the envelope marks it


async def test_a_token_is_single_use_and_a_wrong_answer_is_a_plain_failure(
    client: httpx.AsyncClient, sample: None,
) -> None:
    _write_envelope([_hardware()])
    ch = (await client.get("/soul-key/envelope/k1/challenge")).json()
    wrong = await client.post("/soul-key/envelope/k1/drill",
                              json={"nonce": ch["nonce"], "prf": _b64(os.urandom(32))})
    assert wrong.json()["status"] == "failed" and "wrong key" in wrong.json()["reason"]
    assert envelope_drill.read_record()["k1"].get("last_proved_at") is None

    again = await client.post("/soul-key/envelope/k1/drill",
                              json={"nonce": ch["nonce"], "prf": _b64(PRF)})
    assert again.status_code == 400 and "expired or was already used" in again.json()["error"]

    ch2 = (await client.get("/soul-key/envelope/k1/challenge")).json()
    short = await client.post("/soul-key/envelope/k1/drill",
                              json={"nonce": ch2["nonce"], "prf": _b64(b"short")})
    assert short.status_code == 400 and "expected size" in short.json()["error"]
    elsewhere = await client.post("/soul-key/envelope/k2/drill",
                                  json={"nonce": ch2["nonce"], "prf": _b64(PRF)})
    assert elsewhere.status_code == 400  # a token is for the recipient it was issued for


async def test_a_recovery_code_is_shown_once_stored_nowhere_and_opens_the_key(
    client: httpx.AsyncClient, sample: None, caplog: pytest.LogCaptureFixture,
) -> None:
    path = _write_envelope([_hardware()])
    with caplog.at_level(logging.DEBUG):
        added = await client.post("/soul-key/envelope/add-code", json={"label": "paper"})
        body = added.json()
        code = body["code"]
        assert added.headers["cache-control"] == "no-store" and body["id"] == "c2"
        assert code not in path.read_text() and code not in json.dumps(
            await _state(client))  # not in the listing, not in the file

        ch = (await client.get("/soul-key/envelope/c2/challenge")).json()
        assert ch == {"id": "c2", "kind": "recovery-code", "nonce": ch["nonce"]}
        wrong = await client.post("/soul-key/envelope/c2/drill",
                                  json={"nonce": ch["nonce"], "code": "A" * 32})
        assert wrong.json()["status"] == "failed"
        ch = (await client.get("/soul-key/envelope/c2/challenge")).json()
        ok = await client.post("/soul-key/envelope/c2/drill",
                               json={"nonce": ch["nonce"], "code": code})
    assert ok.json()["status"] == "proved" and code not in ok.text
    assert all(code not in r.getMessage() and "A" * 32 not in r.getMessage()
               for r in caplog.records)  # no log line carried a secret
    assert code not in json.dumps(envelope_drill.read_record())


async def _state(client: httpx.AsyncClient) -> dict[str, Any]:
    out: dict[str, Any] = (await client.get("/soul-key/envelope")).json()
    return out


async def test_adding_a_key_wraps_the_live_key_server_side_and_the_token_is_single_use(
    client: httpx.AsyncClient,
) -> None:
    path = _write_envelope([_hardware()])
    begin = (await client.post("/soul-key/envelope/add-key/begin",
                               json={"label": "spare"})).json()
    assert begin["rp_id"] == "localhost" and begin["user_verification"] == "required"
    assert {p["alg"] for p in begin["pub_key_cred_params"]} == {-7, -257}
    new_prf = os.urandom(32)

    bad = await client.post("/soul-key/envelope/add-key/finish", json={
        "token": begin["token"], "credential_id": _b64(b"new-cred"), "prf": _b64(b"short")})
    assert bad.status_code == 400 and len(json.loads(path.read_text())["recipients"]) == 1

    begin = (await client.post("/soul-key/envelope/add-key/begin", json={"label": "spare"})).json()
    done = await client.post("/soul-key/envelope/add-key/finish", json={
        "token": begin["token"], "credential_id": _b64(b"new-cred"), "prf": _b64(new_prf)})
    assert done.status_code == 200 and done.json() == {
        "id": "k2", "kind": "fido2-prf", "label": "spare"}
    stored = json.loads(path.read_text())["recipients"][1]
    assert stored["rp_id"] == "localhost" and stored["last_opened_at"] is None
    assert soul_envelope._b64_tolerant(begin["salt"]) == base64.urlsafe_b64decode(stored["salt"])
    with soul_envelope.envelope_open("k2", prf=new_prf) as opened:  # the stored copy opens
        assert opened.status == "ok" and opened.fingerprint == key_fingerprint(KEY)
    assert _b64(new_prf) not in path.read_text() and KEY.decode() not in done.text

    again = await client.post("/soul-key/envelope/add-key/finish", json={
        "token": begin["token"], "credential_id": _b64(b"new-cred"), "prf": _b64(new_prf)})
    assert again.status_code == 400  # the token is spent


async def test_adding_a_key_before_the_first_one_is_enrolled_says_so(
    client: httpx.AsyncClient,
) -> None:
    resp = await client.post("/soul-key/envelope/add-key/begin", json={})

    assert resp.status_code == 400 and "enroll the first key" in resp.json()["error"]
    assert (await client.post("/soul-key/envelope/add-code", json={})).status_code == 400


async def test_remove_keeps_the_cli_guards_and_says_it_does_not_revoke(
    client: httpx.AsyncClient,
) -> None:
    code = soul_envelope.envelope_add_code  # a code recipient made the way the route makes one
    path = _write_envelope([_hardware()])
    code(label="paper")

    only_hw = await client.post("/soul-key/envelope/remove", json={"recipient": "k1"})
    assert only_hw.status_code == 400 and "last hardware key" in only_hw.json()["error"]
    unknown = await client.post("/soul-key/envelope/remove", json={"recipient": "zz"})
    assert unknown.status_code == 404
    removed = await client.post("/soul-key/envelope/remove", json={"recipient": "c2"})
    assert removed.status_code == 200 and "does not revoke" in removed.json()["note"]
    assert [r["id"] for r in json.loads(path.read_text())["recipients"]] == ["k1"]
    last = await client.post("/soul-key/envelope/remove", json={"recipient": "k1"})
    assert last.status_code == 400 and "only recipient" in last.json()["error"]


async def test_nothing_answers_off_this_machine(actions: Actions, sample: None) -> None:
    path = _write_envelope([_hardware()])
    before = path.read_text()
    async with _app_client(actions, host="10.0.0.9") as far:
        gets = [await far.get(u) for u in ("/soul-key/envelope", "/soul-key/envelope/k1/challenge",
                                           "/soul-key/at-home")]
        posts = [await far.post(u, json={"recipient": "k1", "label": "x"}) for u in (
            "/soul-key/envelope/k1/drill", "/soul-key/envelope/add-code",
            "/soul-key/envelope/add-key/begin", "/soul-key/envelope/add-key/finish",
            "/soul-key/envelope/remove")]
    assert all(r.status_code == 403 for r in [*gets, *posts])
    assert path.read_text() == before


async def test_a_malformed_body_is_a_plain_sentence_and_never_echoed(
    client: httpx.AsyncClient, sample: None,
) -> None:
    _write_envelope([_hardware()])
    ch = (await client.get("/soul-key/envelope/k1/challenge")).json()
    junk = await client.post("/soul-key/envelope/k1/drill", content=b'{"prf": "SECRETVALUE"',
                             headers={"content-type": "application/json"})
    listy = await client.post("/soul-key/envelope/k1/drill", json=["SECRETVALUE"])
    wrongtype = await client.post("/soul-key/envelope/k1/drill",
                                  json={"nonce": ch["nonce"], "prf": {"SECRETVALUE": 1}})

    for resp in (junk, listy, wrongtype):
        assert resp.status_code == 400 and "SECRETVALUE" not in resp.text
        assert set(resp.json()) == {"error"}


async def test_the_at_home_route_returns_the_checklist_json(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.orchestrator import at_home

    async def sources(pool: Any) -> str:
        return "sources"

    monkeypatch.setattr(at_home, "real_sources", sources)
    monkeypatch.setattr(at_home, "checklist", lambda src: {"items": [{"key": "x"}], "src": src})

    resp = await client.get("/soul-key/at-home")

    assert resp.status_code == 200 and resp.json() == {"items": [{"key": "x"}], "src": "sources"}
