"""THE KEY PANEL, piece 2: browser WebAuthn PRF recovery enrollment + recovery (Thoth
mail 13002), over Seshat's own new routes (src/orchestrator/soul_key_recovery_material.py,
API-half tip, branch seshat-recovery-material-api) rather than Khnum's CLI-only
enroll-recovery/recover, which stay untouched.

The HKDF/Fernet-compatible wrap logic here was cross-verified byte-for-byte against
Python's own cryptography.fernet.Fernet + HKDF via a throwaway Node+Python round-trip
before landing (both directions: a JS-wrapped token decrypts correctly under Python's
Fernet, and a Python-wrapped token decrypts correctly here) — not merely spec-followed.
What could not be verified without real hardware is the actual navigator.credentials
create/get PRF ceremony itself; flagged in the code's own header comment, matching the
same honesty soul_crypto.py's own FIDO2 section states about its CLI counterpart.

Mirrors the repo's existing static-source-guard convention: string/substring proofs
against the served JS, no browser harness."""
from __future__ import annotations

from pathlib import Path

_CONSOLE_JS = (Path(__file__).parent.parent / "src" / "ui" / "static" / "console.js").read_text()


# --- RP id: RULED live off /soul-key/status, never hard-coded (decision ff21aed514bc,
# Thoth mail 13005) -- the console is plain-http localhost:8011, a WebAuthn secure
# context only for the literal hostname "localhost"; "osiris.local" (soul_crypto.py's
# own PRE-ruling constant) could never interoperate with a real browser. rp_id is a
# settings-registry knob both the CLI and this page read; GET /soul-key/status returns
# it, so this page never hard-codes it. ------------------------------------------------

def test_no_hard_coded_rp_id_constant_survives_in_this_file() -> None:
    assert "SOUL_KEY_RP_ID" not in _CONSOLE_JS
    # "osiris.local" may still appear in the header's own historical-note prose (the
    # PRE-ruling constant name), but never as a live code value anywhere.
    assert "id: 'osiris.local'" not in _CONSOLE_JS
    assert "rpId: 'osiris.local'" not in _CONSOLE_JS
    assert "= 'osiris.local'" not in _CONSOLE_JS


def test_rp_origin_check_takes_the_rp_id_as_a_parameter() -> None:
    body = _CONSOLE_JS.split("function _rpOriginOk(rpId) {", 1)[1][:120]
    assert "document.location.hostname === rpId" in body


def test_enroll_reads_rp_id_off_status_before_ever_calling_webauthn() -> None:
    body = _CONSOLE_JS.split("async function enrollRecoveryBrowser() {", 1)[1][:1500]
    status_at = body.index("fetch('/soul-key/status')")
    rp_check_at = body.index("_rpOriginOk(rpId)")
    create_at = body.index("navigator.credentials.create")
    assert status_at < rp_check_at < create_at
    assert "var rpId = status.rp_id;" in body


def test_recover_reads_rp_id_off_the_recovery_blob_before_webauthn() -> None:
    # the credential was enrolled against THIS blob's own rp_id -- not a guess, not a
    # fresh /soul-key/status read (a box mid-recovery has no live key for status to
    # carry an authoritative rp_id off of at all).
    body = _CONSOLE_JS.split("async function recoverKeyBrowser() {", 1)[1][:1300]
    blob_at = body.index("'/soul-key/recovery-blob'")
    rp_check_at = body.index("_rpOriginOk(blob.rp_id)")
    get_at = body.index("navigator.credentials.get")
    assert blob_at < rp_check_at < get_at


# --- HKDF params match soul_crypto.py's own _hkdf_wrap_key exactly --------------------

def test_hkdf_uses_the_same_info_label_and_empty_salt_as_soul_crypto_py() -> None:
    body = _CONSOLE_JS.split(
        "async function hkdfWrapKeyMaterial(prfOutputBytes) {", 1)[1][:800]
    assert "hash: 'SHA-256'" in body
    assert "info: info" in body
    assert "TextEncoder().encode('osiris-soul-key-recovery-wrap')" in body
    assert "salt: new Uint8Array(0)" in body
    assert "256" in body  # 32-byte output, in bits


# --- Fernet-compatible wire format: version/timestamp/iv/ciphertext/hmac --------------

def test_fernet_encrypt_builds_the_real_fernet_wire_format() -> None:
    body = _CONSOLE_JS.split(
        "async function fernetEncryptBytes(derivedKey32, plaintextBytes) {", 1)[1][:1000]
    assert "derivedKey32.slice(0, 16)" in body  # signing key
    assert "derivedKey32.slice(16, 32)" in body  # encryption key
    assert "0x80" in body  # Fernet's fixed version byte
    assert "name: 'AES-CBC'" in body
    assert "name: 'HMAC', hash: 'SHA-256'" in body


def test_fernet_decrypt_verifies_the_hmac_before_decrypting() -> None:
    body = _CONSOLE_JS.split(
        "async function fernetDecryptBytes(derivedKey32, token) {", 1)[1][:800]
    verify_at = body.index("crypto.subtle.verify")
    decrypt_at = body.index("crypto.subtle.decrypt")
    assert verify_at < decrypt_at  # never decrypt an unauthenticated ciphertext
    assert "if (!ok) throw" in body


# --- enrollment: material -> WebAuthn PRF ceremony -> wrap -> complete ----------------

def test_enroll_fetches_material_before_touching_webauthn() -> None:
    body = _CONSOLE_JS.split("async function enrollRecoveryBrowser() {", 1)[1][:2600]
    material_at = body.index("'/soul-key/recovery-material'")
    create_at = body.index("navigator.credentials.create")
    assert material_at < create_at


def test_enroll_creates_a_resident_credential_with_the_prf_extension() -> None:
    body = _CONSOLE_JS.split("async function enrollRecoveryBrowser() {", 1)[1][:2600]
    assert "residentKey: 'required'" in body
    assert "userVerification: 'required'" in body
    assert "extensions: { prf: {} }" in body
    assert "alg: -7" in body  # ES256, matches soul_crypto.py's own registration params


def test_enroll_evaluates_prf_at_a_fresh_random_salt_then_wraps_and_posts_completion() -> None:
    body = _CONSOLE_JS.split("async function enrollRecoveryBrowser() {", 1)[1][:3200]
    assert "crypto.getRandomValues(new Uint8Array(32))" in body  # the salt
    assert "extensions: { prf: { eval: { first: salt } } }" in body
    assert "hkdfWrapKeyMaterial(prfOutput)" in body
    assert "fernetEncryptBytes(wrapKey, rawKey)" in body
    assert "'/soul-key/recovery-material/complete'" in body
    assert "rp_id: rpId" in body


def test_enroll_refuses_a_credential_with_no_prf_output() -> None:
    body = _CONSOLE_JS.split("async function enrollRecoveryBrowser() {", 1)[1][:2600]
    assert "if (!prfResults || !prfResults.results || !prfResults.results.first)" in body


# --- recovery: read blob -> WebAuthn PRF ceremony -> unwrap -> verify -> reseal -------

def test_recover_reads_the_blob_before_touching_webauthn() -> None:
    body = _CONSOLE_JS.split("async function recoverKeyBrowser() {", 1)[1][:2200]
    blob_at = body.index("'/soul-key/recovery-blob'")
    get_at = body.index("navigator.credentials.get")
    assert blob_at < get_at


def test_recover_verifies_the_fingerprint_before_ever_sealing_the_key() -> None:
    body = _CONSOLE_JS.split("async function recoverKeyBrowser() {", 1)[1][:2600]
    fp_check_at = body.index("fingerprint !== blob.key_fingerprint")
    seal_at = body.index("'/soul-key/recover-from-browser'")
    assert fp_check_at < seal_at
    assert "throw new Error" in body[fp_check_at:seal_at]


def test_recover_posts_the_resolved_path_from_soul_key_status_not_a_client_guess() -> None:
    body = _CONSOLE_JS.split("async function recoverKeyBrowser() {", 1)[1][:2600]
    assert "fetch('/soul-key/status')" in body
    assert "resolved_path: status.path" in body


# --- the Key panel offers both buttons at the right times, never a duplicate enroll ----

def test_recover_button_only_offered_when_no_key_is_present() -> None:
    body = _CONSOLE_JS.split("function renderKeyPanelHtml(s) {", 1)[1][:1300]
    assert "!s.present" in body
    assert "recoverKeyBrowser()" in body


def test_enroll_button_only_offered_when_zero_recovery_paths_are_enrolled() -> None:
    body = _CONSOLE_JS.split("function renderKeyPanelHtml(s) {", 1)[1][:1300]
    assert "s.present && (s.recovery_paths_enrolled || []).length === 0" in body
    assert "enrollRecoveryBrowser()" in body


def test_cli_pointer_still_stands_alongside_the_new_browser_button() -> None:
    # piece 2 is an ADDITIONAL path, never a replacement for Khnum's CLI-only door.
    assert "osiris soul-key enroll-recovery" in _CONSOLE_JS
