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
from pathlib import Path
from typing import Any

from src.ingest.soul_crypto import cached_key_fingerprint, recovery_file_path

KIND_HARDWARE = "fido2-prf"
KIND_CODE = "recovery-code"
KINDS = (KIND_HARDWARE, KIND_CODE)
ENVELOPE_VERSION = 2

# what each kind's record must carry besides the common fields
_REQUIRED = {
    KIND_HARDWARE: ("credential_id", "salt", "wrapped_key"),
    KIND_CODE: ("salt", "wrapped_key"),
}
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
    """Who the key is sealed to: id, kind, label, added_at, last_opened_at, and `unattended`
    (always False: every real open of a recipient needs a person). No secret, no touch, no
    decryption. An empty list when there is no recovery file or it cannot be read."""
    recovery_path = recovery_file_path(path=path)
    if not recovery_path.exists():
        return []
    try:
        blob = _read_blob(recovery_path)
    except (OSError, ValueError):
        return []
    return [{"id": str(r.get("id", "")), "kind": r.get("kind"), "label": r.get("label", ""),
             "added_at": r.get("added_at"), "last_opened_at": r.get("last_opened_at"),
             "unattended": False}
            for r in blob["recipients"] if isinstance(r, dict)]


def envelope_check(*, path: str | None = None) -> dict[str, Any]:
    """The UNATTENDED structural check: safe from a drill or a cron, it never opens the key.
    Reports `ok`, the `problems` found (each a plain sentence), how many recipients and how
    many are hardware, and `stale` (the file protects an older key than the live one; None
    when that cannot be told without decrypting). `ok` means the file is well formed and a
    person COULD open it; only `envelope_verify` proves a recipient actually does."""
    recovery_path = recovery_file_path(path=path)
    if not recovery_path.exists():
        return {"ok": False, "problems": [f"no recovery file at {recovery_path}"],
                "recipients": 0, "hardware": 0, "stale": None}
    try:
        blob = _read_blob(recovery_path)
    except (OSError, ValueError) as exc:
        return {"ok": False, "problems": [f"the recovery file cannot be read: {exc}"],
                "recipients": 0, "hardware": 0, "stale": None}
    problems: list[str] = []
    seen: set[str] = set()
    hardware = 0
    records = [r for r in blob["recipients"] if isinstance(r, dict)]
    if len(records) != len(blob["recipients"]):
        problems.append("the recipients list holds an entry that is not a record")
    for r in records:
        rid = str(r.get("id", ""))
        if not rid:
            problems.append("a recipient has no id")
        elif rid in seen:
            problems.append(f"recipient id {rid!r} appears more than once")
        seen.add(rid)
        kind = r.get("kind")
        if kind not in KINDS:
            problems.append(f"recipient {rid!r} has an unknown kind {kind!r}")
            continue
        hardware += kind == KIND_HARDWARE
        for field in _REQUIRED[kind]:
            if not r.get(field):
                problems.append(f"recipient {rid!r} is missing {field}")
        if r.get("wrapped_key") and not _token_well_formed(r["wrapped_key"]):
            problems.append(f"recipient {rid!r} has a wrapped key that is not a well-formed token")
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
    return {"ok": not problems, "problems": problems, "recipients": len(records),
            "hardware": hardware, "stale": stale}
