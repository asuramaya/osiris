"""The soul key's envelope: one key, sealed to several recipients, any one of which opens it.

The soul key (the Fernet key that encrypts the store) already has a recovery file next to it:
one FIDO2 security key's hmac-secret output wraps the key. This module widens that into an
envelope with a list of recipients, so losing a single hardware key never locks the store, and
a second key or an offline code can be added or removed by re-wrapping the KEY alone: the data
stays encrypted under the same key and is never touched.

Recipient kinds:
  fido2-prf      a FIDO2 security key (hmac-secret / PRF): needs the key, its PIN and a touch
  recovery-code  a long random code written down once and kept offline; typed to open

THE FILE. Version 2 of the recovery file carries `"recipients": [ ... ]`; a version 1 file (one
credential at the top level, written by `soul_key_enroll_recovery`) is READ as one fido2-prf
recipient with the id `r0`, and is only rewritten as version 2 when a recipient is added or
removed. Nothing that works with a version 1 file changes.

THIS HALF IS READ-ONLY: `envelope_recipients` and `envelope_check` read the file and never
open the key, never ask for a touch, never need a secret, so they are safe for status
readers and for an unattended drill. Opening a recipient always needs the person, by design:
there is no way here to open one without them.
"""
from __future__ import annotations

import base64
import json
import os
import secrets
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt

from src.ingest.soul_crypto import (
    _key_file_path,
    cached_key_fingerprint,
    key_fingerprint,
    read_key_bytes_at,
    recovery_file_path,
)

KIND_HARDWARE = "fido2-prf"
KIND_CODE = "recovery-code"
KINDS = (KIND_HARDWARE, KIND_CODE)
ENVELOPE_VERSION = 2

# what each kind's record must carry besides the common fields
_REQUIRED = {
    KIND_HARDWARE: ("credential_id", "salt", "wrapped_key"),
    KIND_CODE: ("salt", "wrapped_key"),
}
# the recovery code: 20 random bytes (160 bits) as 32 base32 characters, hardened by scrypt
_CODE_BYTES = 20
_SCRYPT = {"name": "scrypt", "n": 2 ** 15, "r": 8, "p": 1}
_SCRYPT_SALT_BYTES = 16
# the fields an older reader (soul_key_recover) expects at the top level of the file
_LEGACY_FIELDS = ("credential_id", "salt", "wrapped_key", "rp_id")

# a Fernet token is 1 version byte, 8 timestamp, 16 IV, at least one 16-byte block, 32 HMAC
_MIN_TOKEN_BYTES = 1 + 8 + 16 + 16 + 32


def _read_blob(recovery_path: Path) -> dict[str, Any]:
    """The file as a version 2 view: a version 1 file becomes one fido2-prf recipient `r0`.
    Raises ValueError (never a raw traceback) when the file is not a JSON object."""
    data = json.loads(recovery_path.read_text())
    if not isinstance(data, dict):
        raise ValueError("the recovery file is not a JSON object")
    if isinstance(data.get("recipients"), list):
        return data
    legacy = {k: data[k] for k in ("credential_id", "salt", "wrapped_key", "rp_id")
              if k in data}
    return {**data, "version": 1, "recipients": [{
        "id": "r0", "kind": KIND_HARDWARE, "label": "enrolled security key",
        "added_at": None, "last_opened_at": None, **legacy}]}


def _token_well_formed(wrapped: Any) -> bool:
    """`wrapped_key` is base64url of a Fernet token (which is itself base64url text): both
    layers must decode, and the inner bytes must start with the Fernet version byte and be
    long enough to be a token at all. A shape check only: it proves nothing about the key."""
    if not isinstance(wrapped, str) or not wrapped:
        return False
    try:
        token_text = base64.urlsafe_b64decode(wrapped.encode())
        raw = base64.urlsafe_b64decode(token_text)
    except (ValueError, TypeError):
        return False
    return len(raw) >= _MIN_TOKEN_BYTES and raw[0] == 0x80


def envelope_recipients(*, path: str | None = None) -> list[dict[str, Any]]:
    """Who the key is sealed to: id, kind, label, added_at (also `enrolled_at`),
    last_opened_at, and `unattended` (always False: every real open of a recipient needs a
    person; `needs_touch` is True for a hardware key, `needs_human` for every kind). No
    secret, no touch, no decryption. An empty list when there is no recovery file or it
    cannot be read."""
    recovery_path = recovery_file_path(path=path)
    if not recovery_path.exists():
        return []
    try:
        blob = _read_blob(recovery_path)
    except (OSError, ValueError):
        return []
    return [{"id": str(r.get("id", "")), "kind": r.get("kind"), "label": r.get("label", ""),
             "added_at": r.get("added_at"), "enrolled_at": r.get("added_at"),
             "last_opened_at": r.get("last_opened_at"), "unattended": False,
             "needs_touch": r.get("kind") == KIND_HARDWARE, "needs_human": True}
            for r in blob["recipients"] if isinstance(r, dict)]


def envelope_check(*, path: str | None = None) -> dict[str, Any]:
    """The UNATTENDED structural check: safe from a drill or a cron, it never opens the key.
    Reports `ok`, the `problems` found (each a plain sentence), how many recipients and how
    many are hardware, and `stale` (the file protects an older key than the live one; None
    when that cannot be told without decrypting). `by_recipient` carries the same problems
    keyed by recipient id, and `unproven` lists every recipient never proven to open the key
    (a code nobody tested shows up here, and as a problem). `ok` means the file is well
    formed, every recipient has been proven, and a person COULD open it; only
    `envelope_verify` proves a recipient actually does."""
    recovery_path = recovery_file_path(path=path)
    if not recovery_path.exists():
        return {"ok": False, "problems": [f"no recovery file at {recovery_path}"],
                "by_recipient": {}, "unproven": [], "recipients": 0, "hardware": 0,
                "stale": None}
    try:
        blob = _read_blob(recovery_path)
    except (OSError, ValueError) as exc:
        return {"ok": False, "problems": [f"the recovery file cannot be read: {exc}"],
                "by_recipient": {}, "unproven": [], "recipients": 0, "hardware": 0,
                "stale": None}
    problems: list[str] = []
    by_recipient: dict[str, list[str]] = {}
    unproven: list[str] = []
    seen: set[str] = set()
    hardware = 0
    records = [r for r in blob["recipients"] if isinstance(r, dict)]
    if len(records) != len(blob["recipients"]):
        problems.append("the recipients list holds an entry that is not a record")

    def flag(rid: str, sentence: str) -> None:
        problems.append(sentence)
        by_recipient.setdefault(rid, []).append(sentence)

    for r in records:
        rid = str(r.get("id", ""))
        if not rid:
            problems.append("a recipient has no id")
        elif rid in seen:
            flag(rid, f"recipient id {rid!r} appears more than once")
        seen.add(rid)
        kind = r.get("kind")
        if kind not in KINDS:
            flag(rid, f"recipient {rid!r} has an unknown kind {kind!r}")
            continue
        hardware += kind == KIND_HARDWARE
        for field in _REQUIRED[kind]:
            if not r.get(field):
                flag(rid, f"recipient {rid!r} is missing {field}")
        if r.get("wrapped_key") and not _token_well_formed(r["wrapped_key"]):
            flag(rid, f"recipient {rid!r} has a wrapped key that is not a well-formed token")
        if not r.get("last_opened_at"):
            unproven.append(rid)
            flag(rid, f"recipient {rid!r} has never been proven to open the key; run "
                      f"`osiris soul-key envelope verify --recipient {rid}`")
    if not records:
        problems.append("the envelope has no recipients")
    elif not hardware:
        problems.append("no recipient is a hardware key: one is required, so that a lost "
                        "recovery code alone is never the only way in")
    if not blob.get("key_fingerprint"):
        problems.append("the file records no key fingerprint")
    live = cached_key_fingerprint(path=path)
    stale: bool | None = None
    if live is not None and blob.get("key_fingerprint"):
        stale = live != blob["key_fingerprint"]
        if stale:
            problems.append("the envelope protects an older key than the live one (the key "
                            "was rotated since); re-seal it to the current key")
    return {"ok": not problems, "problems": problems, "by_recipient": by_recipient,
            "unproven": unproven, "recipients": len(records), "hardware": hardware,
            "stale": stale}


# --- the write half --------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_blob_atomic(recovery_path: Path, blob: dict[str, Any]) -> None:
    """Write the file as version 2, atomically and private: a temp file in the same directory
    (mode 0600, flushed to disk), then a rename over the old one, so a crash leaves either the
    old file or the new one, never half of either. The first hardware recipient is mirrored at
    the top level so `soul_key_recover`, which reads the version 1 shape, keeps working."""
    blob = {**blob, "version": ENVELOPE_VERSION}
    for field in _LEGACY_FIELDS:
        blob.pop(field, None)
    first = next((r for r in blob["recipients"] if r.get("kind") == KIND_HARDWARE), None)
    if first is not None:
        for field in _LEGACY_FIELDS:
            if first.get(field):
                blob[field] = first[field]
    fd, tmp = tempfile.mkstemp(prefix=".envelope-", dir=recovery_path.parent)
    try:
        with os.fdopen(fd, "w") as fh:
            os.fchmod(fh.fileno(), 0o600)
            fh.write(json.dumps(blob))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, recovery_path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    dir_fd = os.open(recovery_path.parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _next_id(blob: dict[str, Any], prefix: str) -> str:
    """A recipient id nobody has used before, even one since removed: a counter kept in the
    file (shared by every kind), so a receipt keyed by id never points at a different recipient
    that reused the name."""
    serial = int(blob.get("serial") or len(blob["recipients"])) + 1
    blob["serial"] = serial
    return f"{prefix}{serial}"


def _public(record: dict[str, Any]) -> dict[str, Any]:
    return {"id": record["id"], "kind": record["kind"], "label": record.get("label", ""),
            "added_at": record.get("added_at")}


def generate_recovery_code() -> str:
    """A fresh recovery code: 160 random bits as 32 base32 characters in groups of four, for
    writing down. Shown once by whoever calls this; never stored, logged or put in argv."""
    raw = base64.b32encode(secrets.token_bytes(_CODE_BYTES)).decode().rstrip("=")
    return "-".join(raw[i:i + 4] for i in range(0, len(raw), 4))


def _normalise_code(code: str) -> bytes:
    return "".join(code.split()).replace("-", "").upper().encode()


def code_wrap_key(code: str, salt: bytes, params: dict[str, Any]) -> Fernet:
    """The Fernet key a recovery code derives with scrypt at its recipient's own salt and
    parameters (recorded in the record, so they can change later without breaking old codes)."""
    derived = Scrypt(salt=salt, length=32, n=int(params["n"]), r=int(params["r"]),
                     p=int(params["p"])).derive(_normalise_code(code))
    return Fernet(base64.urlsafe_b64encode(derived))


def _live_key(path: str | None) -> bytes:
    return read_key_bytes_at(_key_file_path(explicit=path), explicit=path is not None)


def envelope_add_code(
    *, path: str | None = None, label: str = "recovery code", raw_key: bytes | None = None,
) -> dict[str, Any]:
    """Seals the key to one more recipient: a recovery code. Returns {"recipient", "code"}: the
    code appears ONCE, in this return value, for the caller to show the person; it is stored
    nowhere. `raw_key` is the live key (read from this box when not given). Adding re-wraps
    the KEY only: the encrypted data is not touched. Refuses when there is no envelope yet (a
    hardware key comes first), or when the file protects an older key than the live one."""
    recovery_path = recovery_file_path(path=path)
    if not recovery_path.exists():
        return {"error": f"no envelope at {recovery_path} yet: enroll a hardware key first "
                         "(`osiris soul-key enroll-recovery`), a recovery code alone is "
                         "never allowed to be the only way in"}
    try:
        blob = _read_blob(recovery_path)
    except (OSError, ValueError) as exc:
        return {"error": f"the recovery file cannot be read: {exc}"}
    key = raw_key if raw_key is not None else _live_key(path)
    if key_fingerprint(key) != blob.get("key_fingerprint"):
        return {"error": "the envelope protects an older key than the live one (the key was "
                         "rotated since); re-seal the envelope to the current key first"}
    code = generate_recovery_code()
    salt = secrets.token_bytes(_SCRYPT_SALT_BYTES)
    token = code_wrap_key(code, salt, _SCRYPT).encrypt(key)
    record = {"id": _next_id(blob, "c"), "kind": KIND_CODE,
              "label": label or "recovery code", "added_at": _now(), "last_opened_at": None,
              "salt": base64.urlsafe_b64encode(salt).decode(), "kdf": dict(_SCRYPT),
              "wrapped_key": base64.urlsafe_b64encode(token).decode()}
    blob["recipients"] = [*blob["recipients"], record]
    _write_blob_atomic(recovery_path, blob)
    return {"recipient": _public(record), "code": code}


def envelope_remove(
    recipient_id: str, *, path: str | None = None, allow_no_hardware: bool = False,
) -> dict[str, Any]:
    """Drops one recipient from the envelope: only the file changes, the data stays as it is.
    Refuses to remove the last recipient (that would lock the store for good) or the last
    hardware recipient (unless `allow_no_hardware`). THIS DOES NOT REVOKE A COPY someone
    already took: a recipient that held a wrapped copy of the key could still have kept it.
    Only rotating the key (`osiris soul-key rotate`) makes an old copy useless; the result
    says so."""
    recovery_path = recovery_file_path(path=path)
    if not recovery_path.exists():
        return {"error": f"no envelope at {recovery_path}"}
    try:
        blob = _read_blob(recovery_path)
    except (OSError, ValueError) as exc:
        return {"error": f"the recovery file cannot be read: {exc}"}
    records = blob["recipients"]
    target = next((r for r in records if r.get("id") == recipient_id), None)
    if target is None:
        known = ", ".join(str(r.get("id")) for r in records) or "none"
        return {"error": f"no recipient {recipient_id!r} (the envelope has: {known})"}
    if len(records) == 1:
        return {"error": f"{recipient_id!r} is the only recipient: removing it would lock the "
                         "store for good. Add another recipient first"}
    left = [r for r in records if r is not target]
    if (target.get("kind") == KIND_HARDWARE and not allow_no_hardware
            and not any(r.get("kind") == KIND_HARDWARE for r in left)):
        return {"error": f"{recipient_id!r} is the last hardware key: without one, a lost "
                         "recovery code is the only way in. Add another hardware key first, "
                         "or pass allow_no_hardware to do it anyway"}
    blob["recipients"] = left
    _write_blob_atomic(recovery_path, blob)
    return {"removed": _public(target), "remaining": [r["id"] for r in left],
            "note": "this re-sealed the envelope only; it does not revoke a copy the removed "
                    "recipient already took. To make an old copy useless, rotate the key "
                    "(`osiris soul-key rotate`)"}


def mark_opened(recipient_id: str, *, path: str | None = None) -> bool:
    """Record, on that recipient, that it was just proven to open the key (`last_opened_at`).
    The only thing a successful verify writes, and it is written atomically like everything
    else here. False when the file or the recipient is not there."""
    recovery_path = recovery_file_path(path=path)
    if not recovery_path.exists():
        return False
    try:
        blob = _read_blob(recovery_path)
    except (OSError, ValueError):
        return False
    for r in blob["recipients"]:
        if r.get("id") == recipient_id:
            r["last_opened_at"] = _now()
            _write_blob_atomic(recovery_path, blob)
            return True
    return False


def envelope_add_key(
    *, path: str | None = None, label: str = "security key", rp_id: str | None = None,
    raw_key: bytes | None = None,
) -> dict[str, Any]:
    """Seals the key to one more hardware recipient: a FIDO2 security key (hmac-secret / PRF).
    Needs the NEW key plugged in, its PIN and a touch (the same enrollment the first key goes
    through), and the live key (read from this box when not given) to wrap. The first key of
    all is enrolled by `osiris soul-key enroll-recovery`, which creates the envelope; this adds
    to it. Re-wraps the KEY only; the data is not touched. The new recipient starts
    UNPROVEN on purpose: its enrollment derives the wrap key once, which is not the same as
    opening the stored copy, so `envelope_verify` (a second touch) is what proves it."""
    from src.ingest import soul_crypto

    recovery_path = recovery_file_path(path=path)
    if not recovery_path.exists():
        return {"error": f"no envelope at {recovery_path} yet: enroll the first key with "
                         "`osiris soul-key enroll-recovery`, then add more here"}
    try:
        blob = _read_blob(recovery_path)
    except (OSError, ValueError) as exc:
        return {"error": f"the recovery file cannot be read: {exc}"}
    key = raw_key if raw_key is not None else _live_key(path)
    if key_fingerprint(key) != blob.get("key_fingerprint"):
        return {"error": "the envelope protects an older key than the live one (the key was "
                         "rotated since); re-seal the envelope to the current key first"}
    use_rp = rp_id or next((str(r["rp_id"]) for r in blob["recipients"]
                            if r.get("kind") == KIND_HARDWARE and r.get("rp_id")),
                           soul_crypto._DEFAULT_RP_ID)
    device = soul_crypto._find_fido2_device()
    if device is None:
        return {"error": "no FIDO2 security key detected: plug in the NEW key and try again"}
    try:
        enrolled = soul_crypto._enroll_and_wrap(device, key, rp_id=use_rp)
    except soul_crypto.SoulKeyRecoveryError as exc:
        return {"error": str(exc)}
    if "error" in enrolled:
        return {"error": str(enrolled["error"])}
    wrapped = enrolled["blob"]
    record = {"id": _next_id(blob, "k"), "kind": KIND_HARDWARE,
              "label": label or "security key", "added_at": _now(), "last_opened_at": None,
              "credential_id": wrapped["credential_id"], "salt": wrapped["salt"],
              "wrapped_key": wrapped["wrapped_key"], "rp_id": wrapped.get("rp_id", use_rp)}
    blob["recipients"] = [*blob["recipients"], record]
    _write_blob_atomic(recovery_path, blob)
    return {"recipient": _public(record)}


# --- opening a recipient and proving it ------------------------------------------------------

STATUS_OK = "ok"
STATUS_NEEDS_OPERATOR = "needs_operator_key"
STATUS_UNAVAILABLE = "unavailable"
STATUS_FAILED = "failed"

# the short plain reasons a caller can match on
ERR_NO_DEVICE = "no device"
ERR_WRONG_PIN = "wrong PIN"
ERR_TOUCH_TIMEOUT = "touch timed out"
ERR_WRONG_KEY = "wrong key or damaged envelope"
ERR_WRONG_CODE = "the code does not match"
ERR_FINGERPRINT = "the fingerprint differs"


class OpenResult:
    """The outcome of opening ONE recipient, and the only holder of the key it yielded.

    Use it as a context manager: `with envelope_open(...) as opened:` and the key is zeroed
    when the block ends. The key lives only in memory here, is never written anywhere, and the
    repr and str show the fingerprint, never the bytes. `status` is one of ok,
    needs_operator_key (a touch, a PIN or a code is needed and nobody gave one, or the person
    did not touch in time), unavailable (no device) and failed (wrong key, wrong code or a
    damaged file); `reason` says which in a few plain words."""

    def __init__(self, status: str, recipient: str, reason: str | None = None,
                 key: bytes | None = None) -> None:
        self.status = status
        self.recipient = recipient
        self.reason = reason
        self._key: bytearray | None = bytearray(key) if key is not None else None

    @property
    def key(self) -> bytes:
        if self._key is None:
            raise ValueError("no key: this recipient did not open, or the block has ended")
        return bytes(self._key)

    @property
    def fingerprint(self) -> str | None:
        return key_fingerprint(bytes(self._key)) if self._key is not None else None

    def fernet(self) -> Fernet:
        """A Fernet over the opened key, for decrypting a real sealed soul line."""
        return Fernet(self.key)

    def close(self) -> None:
        if self._key is not None:
            for i in range(len(self._key)):
                self._key[i] = 0
            self._key = None

    def __enter__(self) -> OpenResult:
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None,
                 tb: TracebackType | None) -> None:
        self.close()

    def __repr__(self) -> str:
        return (f"OpenResult(status={self.status!r}, recipient={self.recipient!r}, "
                f"reason={self.reason!r}, fingerprint={self.fingerprint!r})")

    __str__ = __repr__


def _classify_device_error(exc: Exception) -> tuple[str, str]:
    """(status, reason) for an error out of the security-key library. A touch that timed out or
    was declined is the PERSON not acting (needs_operator_key); a wrong PIN is a failed proof.
    Reads only the error's own text: nothing secret rides on it."""
    text = f"{type(exc).__name__} {exc}".lower()
    if "pin" in text and any(w in text for w in ("invalid", "wrong", "incorrect", "blocked")):
        return STATUS_FAILED, ERR_WRONG_PIN
    if any(w in text for w in ("timeout", "timed out", "cancel", "declin", "no touch", "denied")):
        return STATUS_NEEDS_OPERATOR, ERR_TOUCH_TIMEOUT
    return STATUS_FAILED, f"the security key reported: {type(exc).__name__}"


def envelope_open(
    recipient_id: str, *, path: str | None = None, interactive: bool = False,
    code: str | None = None,
) -> OpenResult:
    """Opens the key using ONLY this recipient's own wrapped copy. It never reads the live key
    file or any carrier of it, so a success here cannot be the host key answering instead: the
    only way in is the recipient's own secret (its security key's touch, or its code).

    `interactive=False` never touches the device and never asks anyone: a hardware recipient
    then returns needs_operator_key at once, and a code recipient does too unless `code` is
    given. `interactive=True` talks to the security key (PIN and touch at the terminal)."""
    recovery_path = recovery_file_path(path=path)
    if not recovery_path.exists():
        return OpenResult(STATUS_FAILED, recipient_id, f"no envelope at {recovery_path}")
    try:
        blob = _read_blob(recovery_path)
    except (OSError, ValueError) as exc:
        return OpenResult(STATUS_FAILED, recipient_id, f"the recovery file cannot be read: {exc}")
    rec = next((r for r in blob["recipients"]
                if isinstance(r, dict) and r.get("id") == recipient_id), None)
    if rec is None:
        known = ", ".join(str(r.get("id")) for r in blob["recipients"]
                          if isinstance(r, dict)) or "none"
        return OpenResult(STATUS_FAILED, recipient_id,
                          f"no recipient {recipient_id!r} (the envelope has: {known})")
    try:
        token = base64.urlsafe_b64decode(str(rec["wrapped_key"]))
        salt = base64.urlsafe_b64decode(str(rec["salt"]))
    except (KeyError, ValueError, TypeError):
        return OpenResult(STATUS_FAILED, recipient_id, ERR_WRONG_KEY)
    kind = rec.get("kind")
    if kind == KIND_CODE:
        if code is None:
            return OpenResult(STATUS_NEEDS_OPERATOR, recipient_id, "the recovery code is needed")
        try:
            raw = code_wrap_key(code, salt, rec.get("kdf") or _SCRYPT).decrypt(token)
        except (InvalidToken, KeyError, ValueError, TypeError):
            return OpenResult(STATUS_FAILED, recipient_id, ERR_WRONG_CODE)
        return OpenResult(STATUS_OK, recipient_id, None, raw)
    if kind != KIND_HARDWARE:
        return OpenResult(STATUS_FAILED, recipient_id, f"unknown recipient kind {kind!r}")
    if not interactive:
        return OpenResult(STATUS_NEEDS_OPERATOR, recipient_id,
                          "the security key's touch and PIN are needed")
    from src.ingest import soul_crypto

    device = soul_crypto._find_fido2_device()
    if device is None:
        return OpenResult(STATUS_UNAVAILABLE, recipient_id, ERR_NO_DEVICE)
    rp_id = str(rec.get("rp_id") or blob.get("rp_id") or soul_crypto._DEFAULT_RP_ID)
    try:
        client = soul_crypto._fido2_client(device, rp_id=rp_id)
        prf = soul_crypto._prf_eval(
            client, base64.urlsafe_b64decode(str(rec["credential_id"])), salt, rp_id=rp_id)
    except soul_crypto.PrfOutputShapeError as exc:
        return OpenResult(STATUS_FAILED, recipient_id, str(exc))
    except Exception as exc:  # noqa: BLE001 - the security-key boundary: name it, never a traceback
        status, reason = _classify_device_error(exc)
        return OpenResult(status, recipient_id, reason)
    if prf is None:
        return OpenResult(STATUS_FAILED, recipient_id, ERR_WRONG_KEY)
    try:
        raw = soul_crypto._hkdf_wrap_key(prf).decrypt(token)
    except InvalidToken:
        return OpenResult(STATUS_FAILED, recipient_id, ERR_WRONG_KEY)
    return OpenResult(STATUS_OK, recipient_id, None, raw)


def envelope_verify(
    recipient_id: str, *, path: str | None = None, interactive: bool = True,
    code: str | None = None, on_key: Callable[[bytes], bool] | None = None,
) -> dict[str, Any]:
    """Proves that one recipient, by itself, opens the key: opens it, compares the key's
    fingerprint with the one the envelope records, and (when `on_key` is given) hands the key to
    that callback ONCE while it is in memory so the caller can decrypt a real sealed artifact
    with it; the key is dropped right after. Returns {ok, status, needs_operator_key,
    fingerprint_matches, artifact_ok, recipient, error}: `artifact_ok` is None without a
    callback, and False when the callback returned false OR raised (its exception text is never
    kept). The key and the callback's errors never appear in the result or in any log.
    `needs_operator_key` is True when the person did not act (no touch in time, no code
    given): a different thing from a failed proof, and the one an unattended drill reports.
    A success writes exactly one thing: `last_opened_at` on that recipient."""
    out: dict[str, Any] = {"ok": False, "status": STATUS_FAILED, "needs_operator_key": False,
                           "fingerprint_matches": None, "artifact_ok": None,
                           "recipient": recipient_id, "error": None}
    with envelope_open(recipient_id, path=path, interactive=interactive, code=code) as opened:
        out["status"] = opened.status
        out["needs_operator_key"] = opened.status == STATUS_NEEDS_OPERATOR
        if opened.status != STATUS_OK:
            out["error"] = opened.reason
            return out
        try:
            expected = _read_blob(recovery_file_path(path=path)).get("key_fingerprint")
        except (OSError, ValueError):
            expected = None
        out["fingerprint_matches"] = expected is not None and opened.fingerprint == expected
        if on_key is not None:
            try:
                out["artifact_ok"] = bool(on_key(opened.key))
            except Exception:  # noqa: BLE001 - a callback's failure is a False, its text is never kept
                out["artifact_ok"] = False
    if not out["fingerprint_matches"]:
        out["error"] = ERR_FINGERPRINT
    elif out["artifact_ok"] is False:
        out["error"] = "the key did not decrypt the sealed artifact"
    else:
        out["ok"] = True
        mark_opened(recipient_id, path=path)
    return out
