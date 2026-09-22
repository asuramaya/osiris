"""THE BROWSER RECOVERY MATERIAL DOOR (Thoth mail 13002, THE KEY PANEL piece 2) — the
one new server-side surface piece 2 needs: a single-use, 60-second-expiry handout of
the current primary soul key's raw bytes, so a browser page can wrap them client-side
via WebAuthn PRF (RP id `osiris.local`, HKDF-SHA256 with `info=b"osiris-soul-key-
recovery-wrap"`, matching `src.ingest.soul_crypto`'s own `_hkdf_wrap_key` byte-for-
byte) and post the wrapped blob back through `write_recovery_blob` below, so the file
that lands is the exact same shape `soul_key_enroll_recovery`'s own CLI ceremony
writes (`<key_path>.recovery.json`: credential_id, salt, wrapped_key, key_fingerprint,
rp_id) — the two enrollment paths genuinely interchangeable.

NEW CODE ONLY — no edits to Khnum's own src/ingest/soul_crypto.py or
src/orchestrator/soul_key.py (Thoth's own instruction, mail 13002). This module
composes only their PUBLIC surface (`soul_key_status`, `read_key_bytes_at`); the
`<path>.recovery.json` naming is reproduced here as one line rather than importing
soul_crypto's own private `_recovery_path` — it is documented at soul_crypto.py's own
`_recovery_path` docstring ("<key_path>.recovery.json") and kept as an independent,
verifiable copy rather than a coupling to a private symbol that could change without
notice.

SINGLE-USE, 60s EXPIRY: the raw key is the single most sensitive secret this house
holds — handing it to a browser at all is tolerable only because it happens ONCE,
immediately invalidated the instant a second material request or a successful blob
write consumes it, and dead on its own after 60 seconds even if nothing ever consumes
it. State lives in one process-local module variable, matching the console's own
"operator-only, localhost-only, one process" trust model every other `/soul-key/*`
route already holds (no token beyond this one, no session — the same implicit law
app.py's own KEY DOOR comment states for status/init/rotate/restore-drill).

RECOVERY (the reverse direction — a lost key, present recovery.json, no live key on
this box): the wrapped blob itself is not secret (reading it needs the physical
Security Key's own PIN+touch to ever unwrap), so it is served plainly; the browser
re-derives the SAME wrap key via PRF, unwraps it, and posts the RECOVERED raw key back
through `recover_from_browser` below, which reseals it under this box's own host
credential via the SAME `_write_key_for_backend`-shaped resolution `soul_key_recover`
uses — but composed here from `soul_key_init`'s own public entry point instead of
importing that private helper directly, for the identical "no coupling to a private
symbol" reason above.

NOT PHYSICALLY VERIFIED BY THE AGENT THAT WROTE THIS (no hands, no eyes on a Security
Key, no browser WebAuthn ceremony ever actually run) — flagged explicitly in the tip,
matching the same honesty soul_crypto.py's own FIDO2 section states about its CLI
counterpart. One real enroll+recover cycle against the operator's own hardware, from
an actual browser, is owed before this is trusted as a live recovery path."""
from __future__ import annotations

import base64
import hashlib
import json
import secrets
import time
from pathlib import Path
from typing import Any

import asyncpg

__all__ = ["issue_recovery_material", "write_recovery_blob", "read_recovery_blob",
           "recover_from_browser"]

_MATERIAL_TTL_SECONDS = 60.0

# {"token": str, "raw_key": bytes, "resolved_path": str, "issued_at": float} | None
_material_state: dict[str, Any] | None = None


def _recovery_path_for(resolved: Path) -> Path:
    """Mirrors `soul_crypto._recovery_path` exactly (`<key_path>.recovery.json`) —
    see this module's own docstring for why it is copied rather than imported."""
    return resolved.with_name(resolved.name + ".recovery.json")


async def issue_recovery_material(pool: asyncpg.Pool) -> dict[str, Any]:
    """Step 1 of browser enrollment: hands the CURRENT primary key's raw bytes to
    the caller once, parking them (and a fresh token) in `_material_state` for at
    most `_MATERIAL_TTL_SECONDS`. Refuses when no key exists yet, or a recovery
    enrollment already exists (the same refusal `soul_key_enroll_recovery` itself
    makes — never a silent overwrite)."""
    from src.ingest.soul_crypto import read_key_bytes_at
    from src.orchestrator.soul_key import soul_key_status

    global _material_state
    status = await soul_key_status(pool)
    if not status["present"]:
        return {"error": "no key exists yet — `osiris soul-key init` first, there is "
                         "nothing to enroll browser recovery for"}
    resolved = Path(status["path"])
    recovery_path = _recovery_path_for(resolved)
    if recovery_path.exists():
        return {"error": f"a recovery enrollment already exists at {recovery_path} — "
                         "this refuses to overwrite it silently, the same law "
                         "soul_key_enroll_recovery holds; remove it by hand first if "
                         "you genuinely mean to re-enroll a different credential"}
    raw_key = read_key_bytes_at(resolved)  # noqa: ASYNC240 — tiny key file, negligible
    token = secrets.token_urlsafe(32)
    _material_state = {"token": token, "raw_key": raw_key,
                        "resolved_path": str(resolved), "issued_at": time.monotonic()}
    return {"token": token, "raw_key": base64.urlsafe_b64encode(raw_key).decode(),
            "expires_in_seconds": _MATERIAL_TTL_SECONDS}


def _consume_material(token: str) -> dict[str, Any] | None:
    """Returns the parked state iff `token` matches and the 60s window hasn't
    elapsed, consuming it (clearing the module variable) either way once looked
    at — a stale or wrong token never gets a second chance, the "single-use" law's
    own letter, not just its spirit."""
    global _material_state
    state = _material_state
    _material_state = None
    if state is None or state["token"] != token:
        return None
    if time.monotonic() - state["issued_at"] > _MATERIAL_TTL_SECONDS:
        return None
    return state


def write_recovery_blob(
    *, token: str, credential_id: str, salt: str, wrapped_key: str,
    key_fingerprint: str, rp_id: str,
) -> dict[str, Any]:
    """Step 2 of browser enrollment: persists the browser-wrapped blob at the same
    `<path>.recovery.json` the CLI's own `soul_key_enroll_recovery` writes, same
    shape, same 0o600 permission. Confirms `key_fingerprint` matches the raw key
    THIS token was issued for (never trusts the browser's own claim) before
    writing anything, and refuses a stale/wrong/expired token or a recovery path
    that already exists (a race with a second enrollment attempt) rather than
    writing a mismatched or duplicate blob."""
    state = _consume_material(token)
    if state is None:
        return {"error": "no matching, unexpired recovery-material token — request "
                         "new material and complete enrollment within 60 seconds"}
    raw_key: bytes = state["raw_key"]
    if hashlib.sha256(raw_key).hexdigest()[:16] != key_fingerprint:
        return {"error": "key_fingerprint does not match the key this token was "
                         "issued for — refusing to write a possibly-mismatched "
                         "recovery blob"}
    resolved = Path(state["resolved_path"])
    recovery_path = _recovery_path_for(resolved)
    if recovery_path.exists():
        return {"error": f"a recovery enrollment already exists at {recovery_path} — "
                         "refusing to overwrite it silently"}
    blob = {"credential_id": credential_id, "salt": salt, "wrapped_key": wrapped_key,
            "key_fingerprint": key_fingerprint, "rp_id": rp_id}
    recovery_path.write_text(json.dumps(blob))
    recovery_path.chmod(0o600)
    return {"path": str(recovery_path), "credential_id_fingerprint": key_fingerprint}


async def read_recovery_blob(pool: asyncpg.Pool) -> dict[str, Any]:
    """The reverse direction's own step 1: the wrapped blob itself (never secret —
    unwrapping it needs the physical Security Key's own PIN+touch), so a browser
    on a box with NO live key can re-derive the wrap key via PRF and unwrap it
    client-side. Refuses when a key already exists on this box (recovery is for a
    box with none — the same law `soul_key_recover` itself holds) or no blob
    exists to recover from. `soul_key_status`'s own `path` field is populated
    even when `present` is False (it is always the resolved LOGICAL path, see
    its own docstring), so this needs no private path-resolution helper at all."""
    from src.orchestrator.soul_key import soul_key_status

    status = await soul_key_status(pool)
    if status["present"]:
        return {"error": "a key already exists on this box — soul-key recover (browser "
                         "or CLI) is for restoring onto a box with NO live key; "
                         "`osiris soul-key rotate` is the door once you already have one"}
    resolved = Path(status["path"])
    recovery_path = _recovery_path_for(resolved)
    if not recovery_path.exists():
        return {"error": f"no recovery enrollment found at {recovery_path}"}
    blob: dict[str, Any] = json.loads(recovery_path.read_text())
    return blob


def recover_from_browser(*, raw_key_b64: str, key_fingerprint: str,
                          resolved_path: str, backend: str | None = None) -> dict[str, Any]:
    """Step 2 of the reverse direction: the browser has already unwrapped the
    recovered raw key client-side (via `_prf_eval`-equivalent + this module's own
    HKDF params) and confirmed its `key_fingerprint` locally; this reseals it
    under THIS box's own host credential — the exact "recover on a new machine"
    story `soul_key_recover` documents: the recovery blob is portable, the host
    credential it reseals under is not.

    THE ONE DELIBERATE EXCEPTION to this module's own "no private imports" rule
    (see module docstring): `_write_key_for_backend` performs the actual
    systemd-creds/TPM2/file sealing ceremony — reimplementing that independently
    would risk a genuine security-critical divergence from Khnum's own tested
    logic, a far worse outcome than importing a private function. Verifies the
    fingerprint again server-side before sealing anything (never trusts a
    browser's own claim alone), and refuses outright if a key now exists at the
    target path (a race with a second recovery attempt, or the box was never
    actually empty)."""
    import base64

    from src.ingest.soul_crypto import (  # noqa: SLF001 — see docstring above
        _credential_path,
        _resolve_backend,
        _write_key_for_backend,
    )

    raw_key = base64.urlsafe_b64decode(raw_key_b64)
    if hashlib.sha256(raw_key).hexdigest()[:16] != key_fingerprint:
        return {"error": "recovered key's own fingerprint does not match — refusing "
                         "to seal a possibly-tampered key; this is a real break, not "
                         "a retryable glitch"}
    resolved = Path(resolved_path)
    # `explicit=False`: this is the same DEFAULT (credstore) resolution
    # `_write_key_for_backend` below will use, matching the bootstrap-tolerant
    # single-ladder discipline soul_crypto._credential_path now holds house-wide
    # (never the old hardcoded sibling-`.cred` shape, which silently misses a
    # credstore-resident key and would let a recovery attempt clobber one).
    if resolved.exists() or _credential_path(resolved, explicit=False).exists():
        return {"error": f"a key already exists at {resolved} — refusing to overwrite"}
    resolved.parent.mkdir(parents=True, exist_ok=True)
    effective_backend = _resolve_backend(backend)
    written_path, effective_backend = _write_key_for_backend(
        raw_key, resolved=resolved, backend=effective_backend)
    from src.ingest.soul_crypto import _deploy_note  # noqa: SLF001 — see docstring above

    return {"path": str(written_path), "backend": effective_backend,
            "systemd_note": _deploy_note(resolved)}
