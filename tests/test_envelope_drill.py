"""The envelope drill against fake envelope functions: each recipient is proved on its own with the
operator present, an unattended view only reports, and the key never leaves the callback."""
from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from cryptography.fernet import Fernet
from src.orchestrator import envelope_drill as drill

NOW = datetime(2026, 11, 12, 12, 0, 0, tzinfo=UTC)
LIVE_KEY = Fernet.generate_key()
OTHER_KEY = Fernet.generate_key()


def decrypts(key: bytes, token: bytes) -> bool:
    try:
        Fernet(key).decrypt(token)
    except Exception:  # noqa: BLE001
        return False
    return True


SAMPLE = drill.Sample(token=Fernet(LIVE_KEY).encrypt(b"a real sealed line"), decrypts=decrypts)


def _days(n: float) -> str:
    return (NOW - timedelta(days=n)).isoformat()


RECIPIENTS = [
    {"id": "yubi", "kind": "fido2-prf", "label": "yubikey", "last_opened_at": _days(5)},
    {"id": "code", "kind": "recovery-code", "label": "paper", "last_opened_at": _days(40)},
    {"id": "new", "kind": "fido2-prf", "label": "spare", "last_opened_at": None},
]


@pytest.fixture(autouse=True)
def _record(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "envelope_drill.json"
    monkeypatch.setenv(drill._RECORD_ENV, str(path))
    return path


class FakeVerify:
    """envelope_verify: calls on_key(key) while it holds the key, like the real one."""

    def __init__(self, behaviour: dict[str, Any]) -> None:
        self.behaviour = behaviour
        self.asked: list[str] = []

    def __call__(self, recipient_id: str, *, on_key: Any = None) -> dict[str, Any]:
        self.asked.append(recipient_id)
        b = self.behaviour[recipient_id]
        if isinstance(b, Exception):
            raise b
        if "key" in b:
            out = {"ok": True, "fingerprint_matches": b.get("fp", True)}
            out["artifact_ok"] = on_key(b["key"]) if on_key else None
            return out
        return b


def _run(verify: FakeVerify, **kw: Any) -> list[dict[str, Any]]:
    return drill.run_drill(lambda: RECIPIENTS, verify, sample=SAMPLE, now=NOW, **kw)


def test_each_recipient_is_proved_on_its_own_and_recorded() -> None:
    verify = FakeVerify({r["id"]: {"key": LIVE_KEY} for r in RECIPIENTS})

    out = _run(verify)

    assert [(r["id"], r["status"]) for r in out] == [
        ("yubi", "proved"), ("code", "proved"), ("new", "proved")]
    assert verify.asked == ["yubi", "code", "new"]  # one proof per recipient, none shared
    assert drill.read_record()["code"]["last_proved_at"] == NOW.isoformat()


def test_a_recipient_that_needs_the_operators_key_is_reported_not_failed() -> None:
    verify = FakeVerify({"yubi": {"ok": False, "needs_operator_key": True,
                                  "error": "touch the key"}})

    out = drill.run_drill(lambda: RECIPIENTS[:1], verify, sample=SAMPLE, now=NOW)

    assert out[0]["status"] == "needs_operator_key" and out[0]["reason"] == "touch the key"


def test_a_failed_attempt_keeps_the_last_full_proof() -> None:
    ok = FakeVerify({"yubi": {"key": LIVE_KEY}})
    drill.run_drill(lambda: RECIPIENTS[:1], ok, sample=SAMPLE, now=NOW)
    bad = FakeVerify({"yubi": {"ok": False, "error": "wrong PIN"}})

    out = drill.run_drill(lambda: RECIPIENTS[:1], bad, sample=SAMPLE,
                          now=NOW + timedelta(days=1))

    assert out[0]["status"] == "failed" and out[0]["reason"] == "wrong PIN"
    entry = drill.read_record()["yubi"]
    assert entry["last_proved_at"] == NOW.isoformat() and entry["status"] == "failed"


@pytest.mark.parametrize("behaviour,why", [
    ({"ok": False, "error": "no device"}, "no device"),
    ({"key": OTHER_KEY, "fp": False}, "not the live vault key"),
    ({"key": OTHER_KEY}, "does not decrypt a real sealed artifact"),
    ({"ok": True, "fingerprint_matches": True, "artifact_ok": None}, "does not decrypt"),
])
def test_every_way_of_not_proving_is_named(behaviour: dict[str, Any], why: str) -> None:
    out = drill.run_drill(lambda: RECIPIENTS[:1], FakeVerify({"yubi": behaviour}),
                          sample=SAMPLE, now=NOW)

    assert out[0]["status"] == "failed" and why in out[0]["reason"]
    assert "last_proved_at" not in drill.read_record()["yubi"]


def test_with_no_sample_nothing_is_proved() -> None:
    out = drill.run_drill(lambda: RECIPIENTS[:1], FakeVerify({"yubi": {"key": LIVE_KEY}}),
                          sample=None, now=NOW)

    assert out[0]["status"] == "failed"


def test_a_recipient_that_raises_is_its_own_failure_and_the_rest_still_run() -> None:
    verify = FakeVerify({"yubi": RuntimeError("secret " + LIVE_KEY.decode()),
                         "code": {"key": LIVE_KEY}, "new": {"key": LIVE_KEY}})

    out = _run(verify)

    assert [r["status"] for r in out] == ["failed", "proved", "proved"]
    assert out[0]["reason"] == "the proof raised RuntimeError"  # the exception text is not copied
    assert LIVE_KEY.decode() not in repr(out)


def test_a_sample_check_that_raises_cannot_leak_the_key_through_its_message() -> None:
    def boom(key: bytes, token: bytes) -> bool:
        raise ValueError("bad key " + key.decode())

    sample = drill.Sample(token=b"x", decrypts=boom)

    out = drill.run_drill(lambda: RECIPIENTS[:1], FakeVerify({"yubi": {"key": LIVE_KEY}}),
                          sample=sample, now=NOW)

    assert out[0]["status"] == "failed" and LIVE_KEY.decode() not in repr(out)


def test_the_key_never_reaches_the_record_a_log_or_the_output(
    _record: Path, caplog: pytest.LogCaptureFixture, capfd: pytest.CaptureFixture[str],
) -> None:
    caplog.set_level(logging.DEBUG)
    verify = FakeVerify({"yubi": {"key": LIVE_KEY}, "code": {"key": OTHER_KEY},
                         "new": {"key": LIVE_KEY}})

    out = _run(verify)

    seen = _record.read_text() + repr(out) + caplog.text
    captured = capfd.readouterr()
    seen += captured.out + captured.err
    assert LIVE_KEY.decode() not in seen and OTHER_KEY.decode() not in seen
    assert not list(_record.parent.glob("*.tmp"))


def test_only_names_one_recipient() -> None:
    verify = FakeVerify({"code": {"key": LIVE_KEY}})

    out = _run(verify, only="code")

    assert [r["id"] for r in out] == ["code"] and verify.asked == ["code"]


def test_the_unattended_view_gives_days_since_and_a_stale_verdict_and_opens_nothing() -> None:
    view = {v["id"]: v for v in drill.status_view(RECIPIENTS, {}, now=NOW)}

    assert view["yubi"]["verdict"] == "current: last proved 5 days ago"
    assert view["code"]["verdict"].startswith("stale: last proved 40 days ago (limit 30)")
    assert view["new"]["verdict"] == "never proved: needs the operator's key"


def test_a_full_proof_by_the_drill_counts_even_when_the_envelope_has_no_open_time() -> None:
    record = {"new": {"last_proved_at": _days(1), "status": "proved"}}

    view = {v["id"]: v for v in drill.status_view(RECIPIENTS, record, now=NOW)}

    assert view["new"]["verdict"] == "current: last proved 1 days ago"


def test_the_envelope_section_reports_problems_and_never_blanks_on_an_error() -> None:
    ok = drill.envelope_section(
        lambda: RECIPIENTS, lambda: {"ok": False, "problems": ["no hardware recipient"],
                                     "hardware": 0}, now=NOW)
    assert ok["ok"] is False and ok["problems"] == ["no hardware recipient"]
    assert ok["stale"] == ["code"]  # 40 days old; "new" is a warning, not stale

    def boom() -> Any:
        raise OSError("disk gone")

    err = drill.envelope_section(boom, lambda: {}, now=NOW)
    assert err == {"error": "the envelope could not be read: OSError"}


def test_the_section_warns_for_each_unproven_recipient_using_the_envelopes_own_list() -> None:
    sec = drill.envelope_section(
        lambda: RECIPIENTS, lambda: {"ok": True, "problems": [], "hardware": 2,
                                     "unproven": ["new"], "by_recipient": {"new": ["x"]}},
        now=NOW)

    assert sec["warnings"] == ["new: never proved to open the envelope"]
    assert sec["by_recipient"] == {"new": ["x"]} and sec["configured"] is True


def test_with_no_unproven_field_the_section_reads_it_off_the_listing() -> None:
    sec = drill.envelope_section(lambda: RECIPIENTS, lambda: {"ok": True}, now=NOW)

    assert sec["warnings"] == ["new: never proved to open the envelope"]


def test_no_envelope_at_all_says_so_instead_of_reading_as_fine() -> None:
    sec = drill.envelope_section(
        lambda: [], lambda: {"ok": False, "problems": ["no recovery file at /x"]}, now=NOW)

    assert sec["configured"] is False and "not sealed to any recipient" in sec["note"]
    assert sec["problems"] == ["no recovery file at /x"]


def test_an_unreadable_record_reads_as_empty(_record: Path) -> None:
    _record.write_text("{broken")

    assert drill.read_record() == {}


def test_the_section_reads_the_real_envelope_functions_over_a_real_recovery_file(
    tmp_path: Path,
) -> None:
    import base64
    import functools
    import json

    from src.ingest import soul_envelope
    from src.ingest.soul_crypto import recovery_file_path

    def wrapped() -> str:
        token = Fernet(Fernet.generate_key()).encrypt(b"k" * 32)
        return base64.urlsafe_b64encode(token).decode()

    def b64(raw: bytes) -> str:
        return base64.urlsafe_b64encode(raw).decode()

    keyfile = str(tmp_path / "soul.key")
    recovery_file_path(path=keyfile).write_text(json.dumps({
        "version": 2, "key_fingerprint": "abcd1234abcd1234", "recipients": [
            {"id": "k1", "kind": "fido2-prf", "label": "yubikey A", "added_at": _days(30),
             "last_opened_at": _days(3), "credential_id": b64(b"c"), "salt": b64(b"s" * 32),
             "wrapped_key": wrapped(), "rp_id": "localhost"},
            {"id": "c1", "kind": "recovery-code", "label": "printed code",
             "added_at": _days(30), "last_opened_at": None, "salt": b64(b"c" * 16),
             "wrapped_key": wrapped()}]}))

    sec = drill.envelope_section(
        functools.partial(soul_envelope.envelope_recipients, path=keyfile),
        functools.partial(soul_envelope.envelope_check, path=keyfile), now=NOW)

    assert sec["configured"] is True and sec["hardware"] == 1
    by_id = {v["id"]: v for v in sec["recipients"]}
    assert by_id["k1"]["verdict"] == "current: last proved 3 days ago"
    assert by_id["c1"]["verdict"] == "never proved: needs the operator's key"
    assert sec["warnings"] == ["c1: never proved to open the envelope"]
