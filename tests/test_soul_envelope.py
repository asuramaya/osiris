"""The soul key's envelope, read-only half: who the key is sealed to, and the unattended check.

These read a recovery file and never open the key. The files here are built the way the real
enrollment builds them (a Fernet token, base64url-wrapped again), so the shape check is
exercised on real tokens, not on hand-made strings."""
from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import pytest
from cryptography.fernet import Fernet
from src.ingest import soul_envelope
from src.ingest.soul_crypto import key_fingerprint, recovery_file_path


def _wrapped(secret: bytes = b"k" * 32) -> str:
    """A wrapped key as `_enroll_and_wrap` writes it: a Fernet token, base64url'd again."""
    token = Fernet(Fernet.generate_key()).encrypt(secret)
    return base64.urlsafe_b64encode(token).decode()


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode()


@pytest.fixture
def keyfile(tmp_path: Path) -> str:
    return str(tmp_path / "soul.key")


def _write(keyfile: str, blob: dict[str, Any]) -> Path:
    path = recovery_file_path(path=keyfile)
    path.write_text(json.dumps(blob))
    return path


def _v1(fingerprint: str = "abcd1234abcd1234") -> dict[str, Any]:
    return {"credential_id": _b64(b"cred"), "salt": _b64(b"s" * 32), "wrapped_key": _wrapped(),
            "key_fingerprint": fingerprint, "rp_id": "localhost"}


PROVEN = "2026-11-12T01:00:00Z"


def _hw(rid: str = "k1", label: str = "yubikey A") -> dict[str, Any]:
    return {"id": rid, "kind": "fido2-prf", "label": label, "added_at": "2026-11-12T00:00:00Z",
            "last_opened_at": PROVEN, "credential_id": _b64(b"cred-" + rid.encode()),
            "salt": _b64(b"s" * 32), "wrapped_key": _wrapped(), "rp_id": "localhost"}


def _code(rid: str = "c1") -> dict[str, Any]:
    return {"id": rid, "kind": "recovery-code", "label": "printed code",
            "added_at": "2026-11-12T00:00:00Z", "last_opened_at": PROVEN,
            "salt": _b64(b"c" * 16), "wrapped_key": _wrapped()}


def test_no_file_means_no_recipients_and_a_plain_not_ok(keyfile: str) -> None:
    assert soul_envelope.envelope_recipients(path=keyfile) == []
    out = soul_envelope.envelope_check(path=keyfile)
    assert out["ok"] is False and out["recipients"] == 0
    assert "no recovery file" in out["problems"][0]


def test_a_version_1_file_reads_as_one_hardware_recipient_called_r0(keyfile: str) -> None:
    _write(keyfile, _v1())
    recs = soul_envelope.envelope_recipients(path=keyfile)
    assert [(r["id"], r["kind"], r["unattended"]) for r in recs] == [("r0", "fido2-prf", False)]
    out = soul_envelope.envelope_check(path=keyfile)
    # structurally fine, but a version 1 file has never been proven through this tool
    assert out["ok"] is False and out["recipients"] == 1 and out["hardware"] == 1
    assert out["unproven"] == ["r0"] and "never been proven" in out["problems"][0]


def test_the_listing_never_carries_a_secret(keyfile: str) -> None:
    blob = {"version": 2, "key_fingerprint": "f" * 16, "recipients": [_hw(), _code()]}
    _write(keyfile, blob)
    recs = soul_envelope.envelope_recipients(path=keyfile)
    assert [r["id"] for r in recs] == ["k1", "c1"]
    for r in recs:
        assert set(r) == {"id", "kind", "label", "added_at", "enrolled_at", "last_opened_at",
                          "unattended", "needs_touch", "needs_human"}
        assert r["unattended"] is False and r["needs_human"] is True
    assert [r["needs_touch"] for r in recs] == [True, False]
    dumped = json.dumps(recs)
    for secret in (blob["recipients"][0]["wrapped_key"], blob["recipients"][0]["salt"],
                   blob["recipients"][0]["credential_id"], blob["recipients"][1]["wrapped_key"]):
        assert secret not in dumped


def test_a_good_two_recipient_envelope_checks_ok(keyfile: str) -> None:
    _write(keyfile, {"version": 2, "key_fingerprint": "f" * 16, "recipients": [_hw(), _code()]})
    out = soul_envelope.envelope_check(path=keyfile)
    assert out == {"ok": True, "problems": [], "by_recipient": {}, "unproven": [],
                   "recipients": 2, "hardware": 1, "stale": None}


@pytest.mark.parametrize("mutate,words", [
    (lambda b: b["recipients"].append(_hw("k1")), "appears more than once"),
    (lambda b: b["recipients"][0].update(kind="gpg"), "unknown kind"),
    (lambda b: b["recipients"][0].pop("salt"), "is missing salt"),
    (lambda b: b["recipients"][0].update(wrapped_key="not-a-token"), "not a well-formed token"),
    (lambda b: b["recipients"][0].update(wrapped_key=_b64(b"\x00" * 90)),
     "not a well-formed token"),
    (lambda b: b["recipients"].__setitem__(slice(None), [_code()]),
     "no recipient is a hardware key"),
    (lambda b: b["recipients"].clear(), "no recipients"),
    (lambda b: b.pop("key_fingerprint"), "no key fingerprint"),
])
def test_each_structural_fault_is_named_in_plain_words(
    keyfile: str, mutate: Any, words: str,
) -> None:
    blob = {"version": 2, "key_fingerprint": "f" * 16, "recipients": [_hw()]}
    mutate(blob)
    _write(keyfile, blob)
    out = soul_envelope.envelope_check(path=keyfile)
    assert out["ok"] is False
    assert any(words in p for p in out["problems"]), out["problems"]


def test_an_unreadable_file_is_reported_not_raised(keyfile: str) -> None:
    recovery_file_path(path=keyfile).write_text("{not json")
    assert soul_envelope.envelope_recipients(path=keyfile) == []
    out = soul_envelope.envelope_check(path=keyfile)
    assert out["ok"] is False and "cannot be read" in out["problems"][0]
    recovery_file_path(path=keyfile).write_text("[1, 2]")
    assert "not a JSON object" in soul_envelope.envelope_check(path=keyfile)["problems"][0]


def test_a_file_for_an_older_key_than_the_live_one_is_stale(
    keyfile: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    live = key_fingerprint(b"the live key bytes")
    _write(keyfile, {"version": 2, "key_fingerprint": live, "recipients": [_hw()]})
    monkeypatch.setattr(soul_envelope, "cached_key_fingerprint", lambda path=None: live)
    assert soul_envelope.envelope_check(path=keyfile)["stale"] is False
    monkeypatch.setattr(soul_envelope, "cached_key_fingerprint",
                        lambda path=None: key_fingerprint(b"a newer key"))
    out = soul_envelope.envelope_check(path=keyfile)
    assert out["stale"] is True and out["ok"] is False
    assert any("older key than the live one" in p for p in out["problems"])


# --- the write half ---------------------------------------------------------------------------

def test_a_problem_with_one_recipient_is_keyed_by_its_id(keyfile: str) -> None:
    bad = _hw("k2")
    bad.pop("salt")
    _write(keyfile, {"version": 2, "key_fingerprint": "f" * 16, "recipients": [_hw("k1"), bad]})
    out = soul_envelope.envelope_check(path=keyfile)
    assert list(out["by_recipient"]) == ["k2"] and "missing salt" in out["by_recipient"]["k2"][0]
    assert out["by_recipient"]["k2"][0] in out["problems"]


RAW = b"0123456789abcdef0123456789abcdef"      # stands in for the live key bytes


def _v2_with_keys(keyfile: str, *recipients: dict[str, Any]) -> Path:
    return _write(keyfile, {"version": 2, "key_fingerprint": key_fingerprint(RAW),
                            "recipients": list(recipients), "serial": len(recipients)})


def _mode(path: Path) -> int:
    import stat

    return stat.S_IMODE(path.stat().st_mode)


def test_adding_a_recovery_code_returns_it_once_and_it_opens_the_key(keyfile: str) -> None:
    path = _write(keyfile, {**_v1(key_fingerprint(RAW))})
    out = soul_envelope.envelope_add_code(path=keyfile, label="paper copy", raw_key=RAW)
    code = out["code"]
    assert len(code.split("-")) == 8 and all(len(g) == 4 for g in code.split("-"))
    assert out["recipient"]["kind"] == "recovery-code" and out["recipient"]["id"] == "c2"
    text = path.read_text()
    assert code not in text and code.replace("-", "") not in text     # stored nowhere
    blob = json.loads(text)
    assert blob["version"] == 2 and [r["id"] for r in blob["recipients"]] == ["r0", "c2"]
    assert _mode(path) == 0o600
    # the version 1 reader still finds the hardware recipient at the top level
    assert blob["credential_id"] == _v1()["credential_id"] and "wrapped_key" in blob
    # the code, typed the way a person types it (lower case, spaces), opens the key
    rec = blob["recipients"][1]
    fernet = soul_envelope.code_wrap_key(
        code.lower().replace("-", " "), base64.urlsafe_b64decode(rec["salt"]), rec["kdf"])
    assert fernet.decrypt(base64.urlsafe_b64decode(rec["wrapped_key"])) == RAW
    # a wrong code does not
    from cryptography.fernet import InvalidToken

    wrong = soul_envelope.code_wrap_key(
        "AAAA-AAAA-AAAA-AAAA-AAAA-AAAA-AAAA-AAAA", base64.urlsafe_b64decode(rec["salt"]),
        rec["kdf"])
    with pytest.raises(InvalidToken):
        wrong.decrypt(base64.urlsafe_b64decode(rec["wrapped_key"]))
    # and the listing and the check see it
    assert [r["id"] for r in soul_envelope.envelope_recipients(path=keyfile)] == ["r0", "c2"]
    assert soul_envelope.envelope_check(path=keyfile)["unproven"] == ["r0", "c2"]


def test_adding_refuses_without_an_envelope_or_for_an_older_key(keyfile: str) -> None:
    assert "enroll a hardware key first" in soul_envelope.envelope_add_code(
        path=keyfile, raw_key=RAW)["error"]
    _write(keyfile, _v1(key_fingerprint(b"an older key")))
    out = soul_envelope.envelope_add_code(path=keyfile, raw_key=RAW)
    assert "older key than the live one" in out["error"]


def test_ids_are_never_reused_after_a_removal(keyfile: str) -> None:
    _v2_with_keys(keyfile, _hw("k1"))
    first = soul_envelope.envelope_add_code(path=keyfile, raw_key=RAW)["recipient"]["id"]
    assert soul_envelope.envelope_remove(first, path=keyfile)["removed"]["id"] == first
    second = soul_envelope.envelope_add_code(path=keyfile, raw_key=RAW)["recipient"]["id"]
    assert first == "c2" and second == "c3"


def test_removal_keeps_one_way_in_and_one_hardware_key(keyfile: str) -> None:
    _v2_with_keys(keyfile, _hw("k1"))
    only = soul_envelope.envelope_remove("k1", path=keyfile)
    assert "only recipient" in only["error"]
    soul_envelope.envelope_add_code(path=keyfile, raw_key=RAW)
    last_hw = soul_envelope.envelope_remove("k1", path=keyfile)
    assert "last hardware key" in last_hw["error"]
    forced = soul_envelope.envelope_remove("k1", path=keyfile, allow_no_hardware=True)
    assert forced["removed"]["id"] == "k1" and forced["remaining"] == ["c2"]
    assert "does not revoke a copy" in forced["note"] and "rotate" in forced["note"]
    assert "no recipient" in soul_envelope.envelope_remove("zz", path=keyfile)["error"]
    assert "no envelope" in soul_envelope.envelope_remove(
        "k1", path=keyfile + "-nope")["error"]


def test_removing_a_second_hardware_key_is_fine_and_the_legacy_mirror_follows(
    keyfile: str,
) -> None:
    path = _v2_with_keys(keyfile, _hw("k1"), _hw("k2"))
    out = soul_envelope.envelope_remove("k1", path=keyfile)
    assert out["remaining"] == ["k2"]
    blob = json.loads(path.read_text())
    assert blob["credential_id"] == _b64(b"cred-k2")      # the version 1 reader follows k2


def test_a_failed_write_leaves_the_old_file_and_no_temp_file(
    keyfile: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import os

    path = _v2_with_keys(keyfile, _hw("k1"), _hw("k2"))
    before = path.read_bytes()

    def boom(src: Any, dst: Any) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError):
        soul_envelope.envelope_remove("k2", path=keyfile)
    assert path.read_bytes() == before
    assert not [p for p in path.parent.iterdir() if p.name.startswith(".envelope-")]


def test_marking_a_recipient_opened_is_the_only_change_and_is_private(keyfile: str) -> None:
    rec = {**_hw("k1"), "last_opened_at": None}
    other = _hw("k2")
    path = _v2_with_keys(keyfile, rec, other)
    assert soul_envelope.envelope_check(path=keyfile)["unproven"] == ["k1"]
    assert soul_envelope.mark_opened("k1", path=keyfile) is True
    after = json.loads(path.read_text())
    assert after["recipients"][0]["last_opened_at"].endswith("Z")
    assert after["recipients"][0]["wrapped_key"] == rec["wrapped_key"]      # nothing else moved
    assert after["recipients"][1] == other
    assert _mode(path) == 0o600
    assert soul_envelope.envelope_check(path=keyfile)["unproven"] == []
    assert soul_envelope.mark_opened("nope", path=keyfile) is False
    assert soul_envelope.mark_opened("k1", path=keyfile + "-nope") is False


# --- adding a hardware key (no real hardware: the same deterministic fake the soul-key tests use)

class _TwoFakeKeys:
    """Two fake security keys; `plug_in` chooses which one the next call talks to."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from src.ingest import soul_crypto

        from tests.test_soul_crypto import _FakeFido2Client

        self.keys = {"A": _FakeFido2Client(b"secret-of-key-A"),
                     "B": _FakeFido2Client(b"secret-of-key-B")}
        self.keys["A"]._credential_id = b"credential-of-key-A-0123456789"
        self.keys["B"]._credential_id = b"credential-of-key-B-0123456789"
        self.present: str | None = "A"
        monkeypatch.setattr(soul_crypto, "_find_fido2_device",
                            lambda: object() if self.present else None)
        monkeypatch.setattr(soul_crypto, "_fido2_client",
                            lambda device, rp_id: self.keys[self.present or "A"])

    def plug_in(self, which: str | None) -> None:
        self.present = which


def test_a_second_hardware_key_is_added_and_opens_the_key_by_itself(
    keyfile: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.ingest import soul_crypto

    fakes = _TwoFakeKeys(monkeypatch)
    # the first key, the way `soul-key enroll-recovery` writes it (a version 1 file)
    first = soul_crypto._enroll_and_wrap(object(), RAW, rp_id="localhost")["blob"]
    path = _write(keyfile, first)
    assert [r["id"] for r in soul_envelope.envelope_recipients(path=keyfile)] == ["r0"]

    fakes.plug_in("B")
    out = soul_envelope.envelope_add_key(path=keyfile, label="spare key", raw_key=RAW)
    rec = out["recipient"]
    assert rec["kind"] == "fido2-prf" and rec["id"] == "k2" and rec["label"] == "spare key"
    blob = json.loads(path.read_text())
    assert blob["version"] == 2 and [r["id"] for r in blob["recipients"]] == ["r0", "k2"]
    assert _mode(path) == 0o600
    # the version 1 mirror still points at the FIRST key, so soul_key_recover is unchanged
    assert blob["credential_id"] == first["credential_id"]
    # the new key, and only it, unwraps the key: derive the wrap key the way an open does
    new = blob["recipients"][1]
    prf = fakes.keys["B"].get_assertion(
        type("O", (), {"extensions": {"prf": {"eval": {
            "first": base64.urlsafe_b64decode(new["salt"])}}}})()
    ).get_response(0).client_extension_results.prf.results.first
    token = base64.urlsafe_b64decode(new["wrapped_key"])
    assert soul_crypto._hkdf_wrap_key(prf).decrypt(token) == RAW
    # the other key's secret does not open it
    from cryptography.fernet import InvalidToken

    other = fakes.keys["A"].get_assertion(
        type("O", (), {"extensions": {"prf": {"eval": {
            "first": base64.urlsafe_b64decode(new["salt"])}}}})()
    ).get_response(0).client_extension_results.prf.results.first
    with pytest.raises(InvalidToken):
        soul_crypto._hkdf_wrap_key(other).decrypt(token)
    assert soul_envelope.envelope_check(path=keyfile)["unproven"] == ["r0", "k2"]


def test_adding_a_key_refuses_cleanly_when_it_cannot(
    keyfile: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    fakes = _TwoFakeKeys(monkeypatch)
    assert "enroll the first key" in soul_envelope.envelope_add_key(
        path=keyfile, raw_key=RAW)["error"]
    _write(keyfile, _v1(key_fingerprint(b"an older key")))
    assert "older key than the live one" in soul_envelope.envelope_add_key(
        path=keyfile, raw_key=RAW)["error"]
    _write(keyfile, _v1(key_fingerprint(RAW)))
    fakes.plug_in(None)
    assert "plug in the NEW key" in soul_envelope.envelope_add_key(
        path=keyfile, raw_key=RAW)["error"]
