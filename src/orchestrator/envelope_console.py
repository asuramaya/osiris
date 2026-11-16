"""THE ENVELOPE IN THE CONSOLE: the logic behind the `/soul-key/envelope*` routes, so a person at
the console can see who the soul key is sealed to, prove each recipient still opens it, and add
or remove one, with what `osiris soul-key envelope` and `osiris envelope-drill` do at a
terminal.

WHERE EACH SECRET LIVES. The live key never leaves this process. A SECURITY KEY is touched in
the BROWSER (WebAuthn PRF, which hashes the salt exactly as python-fido2's `prf` extension does
when the CLI enrolled or opened the same credential, so one credential and one salt give the
same 32 bytes in both): the PIN and the touch stay with the browser and the operating system,
and only the resulting PRF output is POSTed. That output is the recipient's wrapping secret
(with the stored wrapped copy it equals the key), so it is handled like the key: parsed, used
once, never logged, stored or echoed. A RECOVERY CODE is typed into the console and POSTed once
in the body; it is checked and dropped. A new code is generated here, returned ONCE in the
response, and stored nowhere.

SINGLE-USE REQUESTS. A drill and a key enrollment each start by asking for a token (120 s, one
use): the browser cannot reuse an old answer, and a stale tab cannot finish somebody else's
enrollment. The tokens live in this process only.

Nothing here talks to a device, so nothing here can hang waiting for a touch."""
from __future__ import annotations

import asyncio
import base64
import secrets
import time
from datetime import UTC, datetime
from typing import Any

import asyncpg

from src.ingest import soul_envelope
from src.orchestrator import envelope_drill

TOKEN_TTL_SECONDS = 120.0
_PENDING: dict[str, dict[str, Any]] = {}
_PENDING_MAX = 64

_STALE = "that request expired or was already used: start again"
_BAD_PRF = "the key's answer was missing or not the expected size: try the touch again"


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _issue(kind: str, **fields: Any) -> str:
    now = time.monotonic()
    for stale in [t for t, v in _PENDING.items() if v["expires"] <= now]:
        del _PENDING[stale]
    if len(_PENDING) >= _PENDING_MAX:
        _PENDING.clear()  # a flood costs everyone a fresh start, never memory
    token = secrets.token_urlsafe(24)
    _PENDING[token] = {"kind": kind, "expires": now + TOKEN_TTL_SECONDS, **fields}
    return token


def _take(token: object, kind: str) -> dict[str, Any] | None:
    """The pending request for this token, removed so it cannot be used twice; None when it is
    unknown, of another kind or expired."""
    if not isinstance(token, str):
        return None
    pending = _PENDING.pop(token, None)
    if pending is None or pending["kind"] != kind or pending["expires"] <= time.monotonic():
        return None
    return pending


def _prf_bytes(value: object) -> bytes | None:
    if not isinstance(value, str) or not value or len(value) > 200:
        return None
    try:
        raw = soul_envelope._b64_tolerant(value)
    except (ValueError, TypeError):
        return None
    return raw if len(raw) == 32 else None


# --- read-only ---------------------------------------------------------------------------------


def state(*, now: datetime | None = None) -> dict[str, Any]:
    """Who the key is sealed to, when each last proved it opens it, and the envelope's own check.
    Read-only: nothing is opened. `exists` is False only when there is no recovery file at all."""
    when = now or datetime.now(UTC)
    section = envelope_drill.envelope_section(
        soul_envelope.envelope_recipients, soul_envelope.envelope_check, now=when)
    rp_ids: dict[str, Any] = {}
    try:
        rp_ids = {r["id"]: r.get("rp_id") for r in soul_envelope.envelope_recipients()}
    except soul_envelope.EnvelopeUnreadable:
        pass
    recipients = [{**v, "rp_id": rp_ids.get(str(v.get("id")))}
                  for v in section.get("recipients") or []]
    return {
        "exists": section.get("configured") is not False,
        "recipients": recipients,
        "check": {"ok": bool(section.get("ok")), "problems": list(section.get("problems") or []),
                  "warnings": list(section.get("warnings") or []),
                  "stale": list(section.get("stale") or []),
                  "hardware": section.get("hardware"),
                  "unproven": list(section.get("unproven") or [])},
        "stale_days": envelope_drill.DEFAULT_STALE_DAYS,
        **({"error": section["error"]} if section.get("error") else {}),
    }


def challenge(recipient_id: str) -> dict[str, Any]:
    """What the browser needs to evaluate one recipient, plus a single-use token for the drill.
    {"error"} when there is no such recipient."""
    material = soul_envelope.envelope_challenge_material(recipient_id)
    if "error" in material:
        return material
    out: dict[str, Any] = {"id": recipient_id, **material,
                           "nonce": _issue("drill", rid=recipient_id)}
    if material.get("kind") == soul_envelope.KIND_HARDWARE:
        out["user_verification"] = "required"  # a different UV state gives a different secret
    return out


# --- the drill ---------------------------------------------------------------------------------


async def drill(
    pool: asyncpg.Pool, recipient_id: str, *, nonce: object, prf: object = None,
    code: object = None,
) -> dict[str, Any]:
    """Prove one recipient opens the key: unwrap it with the answer the person gave, check the
    fingerprint, decrypt one real sealed line with the key, and zero it; record the result so
    backup-status shows it. The result carries no secret."""
    pending = _take(nonce, "drill")
    if pending is None or pending.get("rid") != recipient_id:
        return {"error": _STALE}
    material = soul_envelope.envelope_challenge_material(recipient_id)
    if "error" in material:
        return material
    prf_raw: bytes | None = None
    code_text: str | None = None
    if material.get("kind") == soul_envelope.KIND_CODE:
        if not isinstance(code, str) or not code.strip() or len(code) > 200:
            return {"error": "type the recovery code"}
        code_text = code
    else:
        prf_raw = _prf_bytes(prf)
        if prf_raw is None:
            return {"error": _BAD_PRF}
    sample = await envelope_drill.sealed_sample(pool)
    seen: dict[str, Any] = {}

    def verify(rid: str, *, on_key: Any = None) -> dict[str, Any]:
        out = soul_envelope.envelope_verify(
            rid, interactive=False, code=code_text, prf=prf_raw, on_key=on_key)
        seen.update(out)
        return out

    status, reason = await asyncio.to_thread(
        envelope_drill.prove_one, verify, recipient_id, sample=sample)
    prf_raw = None
    code_text = None
    label = kind = None
    for r in soul_envelope.envelope_recipients():
        if r["id"] == recipient_id:
            label, kind = r.get("label"), r.get("kind")
    envelope_drill.record_result(recipient_id, label, kind, status, reason,
                                 datetime.now(UTC).isoformat())
    return {"id": recipient_id, "status": status, "ok": status == envelope_drill.PROVED,
            "fingerprint_matches": seen.get("fingerprint_matches"),
            "artifact_ok": seen.get("artifact_ok"), "reason": reason}


# --- adding and removing -----------------------------------------------------------------------


async def add_code(label: object) -> dict[str, Any]:
    """Seal the key to a new recovery code. The code is in THIS return value and nowhere else:
    the caller shows it once and the server keeps no copy."""
    text = label if isinstance(label, str) and label.strip() else "recovery code"
    out = await asyncio.to_thread(soul_envelope.envelope_add_code, label=text[:80])
    if "error" in out:
        return out
    rec = out["recipient"]
    return {"id": rec["id"], "kind": rec["kind"], "label": rec["label"], "code": out["code"]}


def add_key_begin(label: object) -> dict[str, Any]:
    """Options for the browser to create the new credential with the PRF extension, and a single-use
    token that remembers the salt this enrollment will be wrapped at."""
    if not soul_envelope.envelope_recipients():
        return {"error": "enroll the first key with `osiris soul-key enroll-recovery` before "
                         "adding more here"}
    from src.ingest import soul_crypto

    rp_id = next((str(r["rp_id"]) for r in soul_envelope.envelope_recipients()
                  if r.get("rp_id")), soul_crypto._DEFAULT_RP_ID)
    text = label if isinstance(label, str) and label.strip() else "security key"
    salt = secrets.token_bytes(32)
    token = _issue("add-key", salt=salt, rp_id=rp_id, label=text[:80])
    return {"token": token, "rp_id": rp_id, "rp_name": "Osiris soul-store key",
            "user": {"id": _b64(secrets.token_bytes(16)), "name": "osiris-soul-key",
                     "displayName": text[:80]},
            "challenge": _b64(secrets.token_bytes(32)), "salt": _b64(salt),
            "pub_key_cred_params": [{"type": "public-key", "alg": -7},
                                    {"type": "public-key", "alg": -257}],
            "timeout": int(TOKEN_TTL_SECONDS * 1000), "user_verification": "required"}


async def add_key_finish(token: object, credential_id: object, prf: object) -> dict[str, Any]:
    pending = _take(token, "add-key")
    if pending is None:
        return {"error": _STALE}
    prf_raw = _prf_bytes(prf)
    if prf_raw is None or not isinstance(credential_id, str) or not credential_id:
        return {"error": _BAD_PRF}
    out = await asyncio.to_thread(
        soul_envelope.envelope_add_key_from_prf, credential_id=credential_id,
        salt=pending["salt"], rp_id=pending["rp_id"], prf=prf_raw, label=pending["label"])
    if "error" in out:
        return out
    rec = out["recipient"]
    return {"id": rec["id"], "kind": rec["kind"], "label": rec["label"]}


async def remove(recipient_id: object, *, allow_no_hardware: bool = False) -> dict[str, Any]:
    if not isinstance(recipient_id, str) or not recipient_id:
        return {"error": "name the recipient to remove"}
    return await asyncio.to_thread(
        soul_envelope.envelope_remove, recipient_id, allow_no_hardware=allow_no_hardware)
