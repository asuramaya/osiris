"""soul_key: THE KEY API's own orchestration layer, composing
`src.ingest.soul_crypto` (pool-free, filesystem-only key primitives) with
`src.ingest.soul_store` (the DB-touching legacy-row census and rewrap pass) and
`scripts.osiris_offbox_restore_drill` into the three dict-in/dict-out functions BOTH
`osiris soul-key <action>` (src/cli.py's own `cmd_soul_key`) and the `/soul-key/*` REST
routes (src/api/app.py) call, never one wrapping the other, the same split every other
domain in this house already holds (backup_settings.py, settings_service.py).

status/rotate/restore-drill all need Postgres; init stays pool-free (a thin pass-through
to `soul_crypto.soul_key_init`, kept here only so callers have one entry point to import from).

`status` is CHEAP by design: it reads the background encryption pass's own progress record
(`soul_encrypt_progress`) instead of decrypting every row, which took over a minute on a
multi-million-row store. The exact census stays available with `exact=True`.

enroll-recovery, verify-recovery and recover are the security-key actions (PIN and touch).
They live here so the backup password rides along automatically and so the verify receipt is
written in one place; they stay CLI-only, never REST.
"""
from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg

_RESTORE_DRILL_RECEIPTS_ENV = "OSIRIS_RESTORE_DRILL_RECEIPTS_FILE"
_DEFAULT_RESTORE_DRILL_RECEIPTS_FILE = "~/.local/state/osiris/restore_drill_receipts.json"
_VERIFY_RECEIPT_ENV = "OSIRIS_RECOVERY_VERIFY_RECEIPT_FILE"
_DEFAULT_VERIFY_RECEIPT_FILE = "~/.local/state/osiris/recovery_verify_receipt.json"


def _restore_drill_receipts_path() -> Path:
    env = os.environ.get(_RESTORE_DRILL_RECEIPTS_ENV)
    if env:
        return Path(env).expanduser()
    return Path(_DEFAULT_RESTORE_DRILL_RECEIPTS_FILE).expanduser()


def _read_restore_drill_receipts() -> dict[str, Any]:
    path = _restore_drill_receipts_path()
    try:
        return dict(json.loads(path.read_text()))
    except (OSError, ValueError):
        return {}


def _write_restore_drill_receipt(repo_url: str, *, ok: bool, error: str | None) -> None:
    """Same merge discipline as offload_runner._write_receipt: a failed drill never
    clobbers an earlier real `last_passed_at` with None, it only ever adds
    `last_attempt_at`/`last_error` alongside whatever last actually passed. The
    readiness stepper's own "restore test passed" step reads this back (there was
    nowhere to read it back from before this file existed: run_drill itself writes
    nothing, a bare pass/fail returned to the caller and never seen again)."""
    path = _restore_drill_receipts_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    receipts = _read_restore_drill_receipts()
    existing = receipts.get(repo_url, {})
    now = datetime.now(UTC).isoformat()
    existing["last_attempt_at"] = now
    if ok:
        existing["last_passed_at"] = now
        existing["last_error"] = None
    else:
        existing["last_error"] = error
    receipts[repo_url] = existing
    path.write_text(json.dumps(receipts, indent=2))


def record_restore_drill(repo_url: str, *, ok: bool, error: str | None) -> None:
    """Public write entry point for the scheduled drill (`scheduled_drill`), same merge
    discipline as the manual drill's own receipt."""
    _write_restore_drill_receipt(repo_url, ok=ok, error=error)


def restore_drill_receipts() -> dict[str, Any]:
    """Public read entry point, mirrors offload_runner.offload_receipts() under the
    same name shape. Never writes."""
    return _read_restore_drill_receipts()


def _verify_receipt_path() -> Path:
    env = os.environ.get(_VERIFY_RECEIPT_ENV)
    return Path(env).expanduser() if env else Path(_DEFAULT_VERIFY_RECEIPT_FILE).expanduser()


def recovery_verify_receipt() -> dict[str, Any]:
    """The last recovery verification's receipt, or {} when none was ever run. Never
    writes."""
    try:
        return dict(json.loads(_verify_receipt_path().read_text()))
    except (OSError, ValueError):
        return {}


def _write_verify_receipt(*, ok: bool, matches_live_key: bool | None, error: str | None,
                          restic_password_matches: bool | None) -> None:
    """Same merge discipline as the restore-drill receipt: a failed attempt never erases
    an earlier real `last_verified_at`, it only adds the attempt and its error."""
    path = _verify_receipt_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    receipt = recovery_verify_receipt()
    now = datetime.now(UTC).isoformat()
    receipt["last_attempt_at"] = now
    receipt["ok"] = ok
    receipt["last_error"] = error
    if ok:
        receipt["last_verified_at"] = now
        receipt["matches_live_key"] = matches_live_key
        receipt["restic_password_matches"] = restic_password_matches
    path.write_text(json.dumps(receipt, indent=2))


async def soul_key_status(
    pool: asyncpg.Pool, *, path: str | None = None, exact: bool = False,
) -> dict[str, Any]:
    """Filesystem facts (`soul_crypto.soul_key_status`) plus the state of the background
    encryption pass and of the recovery enrollment. NEVER the key bytes.

    FAST BY DEFAULT: `encryption` comes from the worker's own progress record and one
    planner-estimate catalog read, and `legacy_plaintext_rows` (kept under its old name for
    existing readers) is that record's remaining count, None while nothing has counted yet.
    `exact=True` (`osiris soul-key status --exact`) runs the old whole-table census against
    the EXPLICIT resolved key path, the slow path that decrypts every row.

    `recovery`: `enrolled`, `stale` (the enrollment protects an older key generation than
    the live one), `verified` (the last `verify-recovery` receipt, or None), and
    `off_box_copies` (how many backup destinations hold the CURRENT recovery file).

    `rp_id`: the live `soul_key.rp_id` setting, surfaced here so the console reads it off
    this SAME route instead of hard-coding a second copy."""
    from src.ingest import soul_crypto
    from src.orchestrator import soul_encrypt_progress, soul_recompress
    from src.orchestrator.settings_service import get_setting

    out = soul_crypto.soul_key_status(path=path)
    out["rp_id"] = (await get_setting(pool, "soul_key.rp_id"))["value"]
    out["tpm"] = soul_crypto.tpm_facts()
    out["compression"] = soul_recompress.shape_compression(
        soul_recompress.read_progress(), key_present=bool(out["present"]))
    record = soul_encrypt_progress.read_progress()
    total = None
    if out["present"] and not record.get("rows_total"):
        total = await soul_encrypt_progress.estimate_total_rows(pool)
    out["encryption"] = soul_encrypt_progress.shape_encryption(
        record, key_present=out["present"], total_estimate=total)
    out["legacy_plaintext_rows"] = (
        out["encryption"]["rows_remaining"] if out["present"] else None)
    if out["present"] and exact:
        from cryptography.fernet import Fernet, MultiFernet

        from src.ingest.soul_store import encrypt_existing_soul_lines

        # `out["path"]` is the LOGICAL key name (soul_key_status's own contract),
        # never the credential blob path directly -- read_key_bytes_at resolves
        # the real bytes for EITHER backend (systemd-creds or legacy plaintext),
        # unlike get_soul_key()'s own env-first ladder, which ignores this
        # explicit path entirely. noqa: ASYNC240 -- a 44-byte key, negligible.
        key_bytes = soul_crypto.read_key_bytes_at(  # noqa: ASYNC240
            Path(out["path"]), explicit=path is not None)
        fernet = MultiFernet([Fernet(key_bytes)])
        census = await encrypt_existing_soul_lines(pool, dry_run=True, fernet=fernet)
        out["legacy_plaintext_rows"] = census["hot_migrated"] + census["cold_migrated"]
    from src.orchestrator import recovery_copies

    facts = soul_crypto.soul_key_recovery_facts(path=path)
    receipt = recovery_verify_receipt()
    out["recovery"] = {
        "enrolled": facts["enrolled"], "stale": facts["stale"],
        "off_box_copies": recovery_copies.copy_status(path=path),
        "verified": ({
            "last_verified_at": receipt.get("last_verified_at"),
            "ok": receipt.get("ok"),
            "matches_live_key": receipt.get("matches_live_key"),
            "last_error": receipt.get("last_error"),
        } if receipt else None),
    }
    return out


async def soul_key_rotate(
    pool: asyncpg.Pool, *, path: str | None = None, finish: bool = False,
    print_recovery: bool = False,
) -> dict[str, Any]:
    """Two steps. Without `finish`: `soul_crypto.soul_key_rotate_begin` mints a new
    key and parks the old one, then this runs `soul_store.rewrap_soul_lines_key` for
    real (`dry_run=False`) using the two keys `begin` just handed back, so every row
    that exists RIGHT NOW moves onto the new primary in the same call. With
    `finish`: refuses unless a rotation is in flight, refuses unless a FRESH
    dry-run re-wrap census comes back clean (zero rewrapped, zero broken, a
    not-yet-restarted daemon may still be writing under the old key), then calls
    `soul_crypto.soul_key_rotate_finish` to remove the legacy key."""
    from cryptography.fernet import Fernet

    from src.ingest import soul_crypto
    from src.ingest.soul_store import rewrap_soul_lines_key

    status = soul_crypto.soul_key_status(path=path)
    if finish:
        if not status["rotation_in_flight"]:
            return {"error": "no rotation in flight, nothing to finish"}
        resolved = Path(status["path"])
        # read_key_bytes_at/read_legacy_key_bytes decode EITHER backend
        # (systemd-creds or legacy plaintext) -- a raw .read_bytes() here would
        # hand MultiFernet an still-encrypted systemd-creds blob instead of the
        # actual key, the exact defect this census surfaced during this route's
        # own build. noqa: ASYNC240 -- tiny key files/subprocess, negligible.
        new_fernet = Fernet(soul_crypto.read_key_bytes_at(  # noqa: ASYNC240
            resolved, explicit=path is not None))
        old_fernet = Fernet(soul_crypto.read_legacy_key_bytes(resolved))  # noqa: ASYNC240
        census = await rewrap_soul_lines_key(
            pool, new_fernet=new_fernet, old_fernet=old_fernet, dry_run=True)
        remaining = census["hot_rewrapped"] + census["cold_rewrapped"]
        broken = census["hot_broken_count"] + census["cold_broken_count"]
        if remaining or broken:
            return {"error": f"{remaining} row(s) still under the old key and "
                             f"{broken} broken row(s) found, re-run `osiris "
                             "soul-key rotate` (without --finish) to sweep them "
                             "before finishing", "census": census}
        return soul_crypto.soul_key_rotate_finish(path=path)
    begin = soul_crypto.soul_key_rotate_begin(path=path, print_recovery=print_recovery)
    if "error" in begin:
        return begin
    new_fernet = Fernet(begin["new_key"])
    old_fernet = Fernet(begin["old_key"])
    census = await rewrap_soul_lines_key(
        pool, new_fernet=new_fernet, old_fernet=old_fernet, dry_run=False)
    return {
        "path": begin["path"], "legacy_path": begin["legacy_path"],
        "systemd_note": begin["systemd_note"], "census": census,
    }


async def soul_key_restore_drill(
    pool: asyncpg.Pool, *, repo_url: str | None = None, full: bool = False,
) -> dict[str, Any]:
    """Runs the restore drill for `repo_url`, or for every URL in
    `backup.offbox_repositories` (`src.orchestrator.backup_settings.get_backup_settings`)
    when omitted, one drill per configured repository, never guessing which one the operator
    meant. By default the BOUNDED drill (`scripts.osiris_offbox_restore_drill.
    run_bounded_drill`: a repository check with a read-data subset, the newest dump's header,
    a verified restore of a few small files), the same one the scheduled test runs;
    `full=True` is the explicit manual door that restores the WHOLE latest snapshot and can
    move gigabytes. A top-level `error` key is set whenever any drill fails (never only
    per-drill), so a generic caller's own error-key check reports the right exit code / HTTP
    status without re-deriving `all_ok` itself."""
    import asyncio

    from scripts.osiris_offbox_restore_drill import run_bounded_drill, run_drill

    from src.orchestrator.backup_settings import get_backup_settings

    drill = run_drill if full else run_bounded_drill
    urls = [repo_url] if repo_url else None
    if urls is None:
        settings = await get_backup_settings(pool)
        urls = list(settings.get("offbox_repositories") or [])
    if not urls:
        return {"error": "no offbox repository configured (backup.offbox_repositories "
                         "is empty) and no --repo-url given"}
    results = []
    for url in urls:
        fail = await asyncio.to_thread(drill, url)
        ok = fail is None
        results.append({"repo_url": url, "ok": ok, "error": fail})
        _write_restore_drill_receipt(url, ok=ok, error=fail)
    all_ok = all(r["ok"] for r in results)
    out: dict[str, Any] = {
        "drills": results, "all_ok": all_ok, "mode": "full" if full else "bounded"}
    if not all_ok:
        failing = [r["repo_url"] for r in results if not r["ok"]]
        out["error"] = f"{len(failing)} of {len(results)} drill(s) failed: {failing}"
    return out


async def soul_key_encrypt_existing(
    pool: asyncpg.Pool, *, path: str | None = None, batch_size: int = 2000,
) -> dict[str, Any]:
    """The readiness stepper's own "encrypt now" action, real work
    (`dry_run=False`): the same `soul_store.encrypt_existing_soul_lines` the CLI script
    (`scripts/osiris_encrypt_soul_lines.py`) and `soul_key_status`'s own dry-run census
    already call, resolving the key the same explicit-path way `soul_key_status` does
    rather than trusting the calling process's own env/default. Refuses outright when no
    key exists yet: encrypting "existing" rows onto a key that was never minted is not a
    recoverable half-state, it's a caller mistake. The whole migration runs inside this
    one call (a `while` loop over every batch already lives in
    `encrypt_existing_soul_lines` itself); the returned counts ARE the progress readout,
    the same report the CLI script prints, not a separate live stream."""
    from cryptography.fernet import Fernet, MultiFernet

    from src.ingest import soul_crypto
    from src.ingest.soul_store import encrypt_existing_soul_lines

    status = soul_crypto.soul_key_status(path=path)
    if not status["present"]:
        return {"error": "no encryption key set up yet; run soul-key init first"}
    key_bytes = soul_crypto.read_key_bytes_at(  # noqa: ASYNC240 -- a 44-byte key
        Path(status["path"]), explicit=path is not None)
    fernet = MultiFernet([Fernet(key_bytes)])
    return await encrypt_existing_soul_lines(
        pool, batch_size=batch_size, dry_run=False, fernet=fernet)


def _current_restic_password(path: str | None = None) -> bytes | None:
    """The backup password for the DEFAULT key layout only: an explicit `--path` is the
    escape hatch for an unusual layout (a test, a one-off), and the box's real backup
    credential must never ride along into it."""
    from src.orchestrator import restic_credential

    if path is not None:
        return None
    try:
        return restic_credential.get_restic_password()
    except restic_credential.ResticPasswordMissing:
        return None


def soul_key_enroll_recovery(*, path: str | None = None, rp_id: str) -> dict[str, Any]:
    """Enrolls the security key as the recovery method, first making sure a backup password
    exists so the same PIN-and-touch enrollment protects both (the wrap needs no second
    touch, but adding it later to an enrollment that lacks it needs the enrollment's own
    key to be current, so doing it here keeps the whole story in one step)."""
    from src.ingest import soul_crypto
    from src.orchestrator import restic_credential

    if path is None:
        restic_credential.restic_key_ensure()
    return soul_crypto.soul_key_enroll_recovery(
        path=path, rp_id=rp_id, restic_password=_current_restic_password(path))


def soul_key_verify_recovery(*, path: str | None = None, rp_id: str) -> dict[str, Any]:
    """Proves the recovery method works without changing anything (PIN and touch, unwrap,
    compare fingerprints) and records a receipt the setup stepper shows. A failure is
    recorded as an attempt without erasing an earlier real verification, and comes back
    as a top-level `error`, so callers report the right exit code."""
    from src.ingest import soul_crypto

    out = soul_crypto.soul_key_verify_recovery(
        path=path, rp_id=rp_id, restic_password=_current_restic_password(path))
    if "error" in out:
        _write_verify_receipt(ok=False, matches_live_key=None, error=out["error"],
                              restic_password_matches=None)
        return out
    problems = []
    if out["matches_live_key"] is False:
        problems.append("the recovered key is not the live key (the key was rotated after "
                        "recovery was enrolled: re-enroll recovery)")
    if out["restic_password_matches"] is False:
        problems.append("the backup password inside the recovery blob does not match the "
                        "one this machine uses")
    error = "; ".join(problems) or None
    _write_verify_receipt(
        ok=error is None, matches_live_key=out["matches_live_key"], error=error,
        restic_password_matches=out["restic_password_matches"])
    if error:
        out["error"] = error
    return out


def soul_key_recover(
    *, path: str | None = None, backend: str | None = None, rp_id: str,
    recovery_file: str | None = None,
) -> dict[str, Any]:
    """Recovers the key onto a box with none, and the backup password with it when the
    recovery blob carries one (sealed by the same machine-local custody a fresh one gets).

    `recovery_file`: a copy of the recovery file taken from a backup destination (the
    plain `osiris-recovery/` copies the offload runner keeps), for a machine that lost
    everything. It is placed where the recovery file normally lives (never over an
    existing one) and the ordinary recovery then runs, so a new machine needs the file and
    the security key, nothing else."""
    import shutil

    from src.ingest import soul_crypto
    from src.orchestrator import restic_credential

    if recovery_file is not None:
        source = Path(recovery_file).expanduser()
        target = soul_crypto.recovery_file_path(path=path)
        if not source.is_file():
            return {"error": f"recovery file {source} not found"}
        if target.exists():
            return {"error": f"a recovery file already exists at {target}; remove it first "
                             "if you mean to recover from a different copy"}
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        target.chmod(0o600)
    seal = None if path is not None else (
        lambda pw: restic_credential.seal_recovered_password(pw, backend=backend))
    return soul_crypto.soul_key_recover(
        path=path, backend=backend, rp_id=rp_id, seal_restic=seal)
