"""The envelope drill: for each recipient the vault key is sealed to, show when that recipient
last proved it opens the envelope, and run the proof itself when the operator is present.

An envelope is only as good as its least-tested recipient: a hardware key nobody has touched in
a year may no longer open anything, and nothing says so until the day it is needed.

TWO MODES, because opening any recipient needs a person (a touch, a PIN, a typed code):

* UNATTENDED (timer, backup-status): reads the envelope's own listing and its structural check
  (`soul_envelope.envelope_check`), and reports per recipient when it last opened (from the
  envelope's `last_opened_at`), the days since, and a stale verdict. A recipient that is due says
  it needs the operator's key. Nothing is opened, nothing is touched.
* INTERACTIVE (`osiris soul-key envelope drill`): for each recipient, opens the envelope with that
  recipient alone (`envelope_verify`) and, through a callback, decrypts one real sealed artifact
  with the key it yielded. The result is kept in this module's own small record.

KEY HYGIENE. The key never reaches this module except as the argument of the callback the
envelope calls while it holds the key; the callback returns a bool and keeps nothing. No key, no
callback exception text and no device message that could carry one is written to the record, a
log line, a command line or a result.

The envelope itself (`src/ingest/soul_envelope.py`) is built separately; the three functions
this drill calls are passed in, so the drill runs against fakes in tests."""
from __future__ import annotations

import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_RECORD_ENV = "OSIRIS_ENVELOPE_DRILL_RECORD_FILE"
_DEFAULT_RECORD = "~/.local/state/osiris/envelope_drill.json"
DEFAULT_STALE_DAYS = 30

PROVED = "proved"
NEEDS_OPERATOR_KEY = "needs_operator_key"
FAILED = "failed"

Recipients = Callable[[], list[dict[str, Any]]]
Check = Callable[[], dict[str, Any]]
Verify = Callable[..., dict[str, Any]]  # (recipient_id, *, on_key=...) -> {ok, ...}


@dataclass(frozen=True)
class Sample:
    """One real sealed artifact and how to test a key against it."""
    token: bytes
    decrypts: Callable[[bytes, bytes], bool]  # (key, token) -> did the key open the token


def record_path() -> Path:
    return Path(os.environ.get(_RECORD_ENV) or _DEFAULT_RECORD).expanduser()


def read_record() -> dict[str, dict[str, Any]]:
    """The drill's own per-recipient record; empty when there is none or it is unreadable."""
    try:
        body = json.loads(record_path().read_text())
    except (OSError, ValueError):
        return {}
    if not isinstance(body, dict):
        return {}
    return {str(k): v for k, v in body.items() if isinstance(v, dict)}


def _write_record(record: dict[str, dict[str, Any]]) -> None:
    path = record_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_text(json.dumps(record, indent=2))
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _parse(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        when = datetime.fromisoformat(value)
    except ValueError:
        return None
    return when if when.tzinfo else when.replace(tzinfo=UTC)


def _later(a: datetime | None, b: datetime | None) -> datetime | None:
    return b if a is None else a if b is None else max(a, b)


def status_view(
    recipients: list[dict[str, Any]], record: dict[str, dict[str, Any]], *, now: datetime,
    stale_days: int = DEFAULT_STALE_DAYS,
) -> list[dict[str, Any]]:
    """What backup-status shows per recipient: when it last proved itself (the later of the
    envelope's `last_opened_at` and this drill's last full proof), the days since, and a verdict.
    A recipient never proved reads as such, not as fine."""
    out: list[dict[str, Any]] = []
    for r in recipients:
        mine = record.get(str(r.get("id"))) or {}
        proved = _later(_parse(r.get("last_opened_at")), _parse(mine.get("last_proved_at")))
        days = (now - proved).days if proved else None
        if proved is None:
            verdict = "never proved: needs the operator's key"
        elif days is not None and days >= stale_days:
            verdict = f"stale: last proved {days} days ago (limit {stale_days}); needs the " \
                      "operator's key"
        else:
            verdict = f"current: last proved {days} days ago"
        if mine.get("status") == FAILED:
            verdict += f"; the last drill attempt failed: {mine.get('reason')}"
        out.append({
            "id": r.get("id"), "label": r.get("label"), "kind": r.get("kind"),
            "last_proved_at": proved.isoformat() if proved else None, "days_since_proved": days,
            "verdict": verdict, "last_drill_status": mine.get("status"),
            "last_drill_at": mine.get("last_attempt_at")})
    return out


def envelope_section(
    recipients: Recipients, check: Check, *, now: datetime,
    stale_days: int = DEFAULT_STALE_DAYS,
) -> dict[str, Any]:
    """The unattended view for backup-status: the envelope's structural check plus one line per
    recipient. Never opens anything. A broken envelope reads as a problem, never as an error
    that blanks the panel."""
    try:
        listed = recipients()
        checked = check()
    except Exception as exc:  # noqa: BLE001 - one section never blanks the panel
        return {"error": f"the envelope could not be read: {type(exc).__name__}"}
    view = status_view(listed, read_record(), now=now, stale_days=stale_days)
    return {"ok": bool(checked.get("ok")), "problems": list(checked.get("problems") or []),
            "hardware": checked.get("hardware"), "recipients": view,
            "stale": [v["id"] for v in view if str(v["verdict"]).startswith(
                ("stale", "never"))]}


def _on_key(sample: Sample | None) -> Callable[[bytes], bool]:
    """The one place the key is seen: decrypt the sample with it and say whether that worked.
    Nothing about the key or a failure's text leaves this function."""
    def decrypt(key: bytes) -> bool:
        if sample is None:
            return False
        try:
            return bool(sample.decrypts(key, sample.token))
        except Exception:  # noqa: BLE001 - the text of a failure here could echo the key
            return False
    return decrypt


def prove_one(
    verify: Verify, recipient_id: str, *, sample: Sample | None,
) -> tuple[str, str | None]:
    """(status, reason) for one recipient, by the envelope's own interactive proof."""
    try:
        out = verify(recipient_id, on_key=_on_key(sample))
    except Exception as exc:  # noqa: BLE001 - one recipient's failure is its own result
        return FAILED, f"the proof raised {type(exc).__name__}"
    if out.get("needs_operator_key"):
        return NEEDS_OPERATOR_KEY, str(out.get("error") or "needs the operator's key")
    if not out.get("ok"):
        return FAILED, str(out.get("error") or "the envelope did not open for this recipient")
    if out.get("fingerprint_matches") is False:
        return FAILED, "it opened, but the key is not the live vault key"
    if sample is None:
        return FAILED, "no sealed artifact was available to decrypt, so nothing was proved"
    if out.get("artifact_ok") is not True:
        return FAILED, "it opened, but its key does not decrypt a real sealed artifact"
    return PROVED, None


def run_drill(
    recipients: Recipients, verify: Verify, *, sample: Sample | None, only: str | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Prove every recipient on record (or one) with the operator present; record each result
    and return them in order. A failed attempt never erases an earlier full proof."""
    stamp = (now or datetime.now(UTC)).isoformat()
    results: list[dict[str, Any]] = []
    for r in recipients():
        rid = str(r.get("id"))
        if only is not None and rid != only:
            continue
        status, reason = prove_one(verify, rid, sample=sample)
        record = read_record()
        entry = record.get(rid, {})
        entry.update({"label": r.get("label"), "kind": r.get("kind"), "last_attempt_at": stamp,
                      "status": status, "reason": reason})
        if status == PROVED:
            entry["last_proved_at"] = stamp
        record[rid] = entry
        _write_record(record)
        results.append({"id": rid, "label": r.get("label"), "kind": r.get("kind"),
                        "status": status, "reason": reason})
    return results
