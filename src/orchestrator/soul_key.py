"""soul_key — THE KEY DOOR's own orchestration layer (Thoth mail 12810/12830, wave 17):
composes `src.ingest.soul_crypto` (pool-free, filesystem-only key primitives) with
`src.ingest.soul_store` (the DB-touching legacy-row census and rewrap pass) and
`scripts.osiris_offbox_restore_drill` into the three dict-in/dict-out functions BOTH
`osiris soul-key <action>` (src/cli.py's own `cmd_soul_key`) and the `/soul-key/*` REST
routes (src/api/app.py) call — never one wrapping the other, the same split every other
domain in this house already holds (backup_settings.py, settings_service.py).

status/rotate/restore-drill all need Postgres; init stays pool-free (a thin pass-through
to `soul_crypto.soul_key_init`, kept here only so callers have one door to import from).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import asyncpg


async def soul_key_status(pool: asyncpg.Pool, *, path: str | None = None) -> dict[str, Any]:
    """Filesystem facts (`soul_crypto.soul_key_status`) plus the live legacy
    (still-plaintext) row count off the store itself, built against the EXPLICIT
    resolved key path rather than whatever the caller process's own env/default
    would resolve to (`soul_store.encrypt_existing_soul_lines`'s own `fernet=`
    seam) — the exact defect a hand-run `--path` census surfaced during this
    door's own build. NEVER the key bytes."""
    from src.ingest import soul_crypto

    out = soul_crypto.soul_key_status(path=path)
    if out["present"]:
        from cryptography.fernet import Fernet, MultiFernet

        from src.ingest.soul_store import encrypt_existing_soul_lines

        # `out["path"]` is the LOGICAL key name (soul_key_status's own contract),
        # never the credential blob path directly -- read_key_bytes_at resolves
        # the real bytes for EITHER backend (systemd-creds or legacy plaintext),
        # unlike get_soul_key()'s own env-first ladder, which ignores this
        # explicit path entirely. noqa: ASYNC240 -- a 44-byte key, negligible.
        key_bytes = soul_crypto.read_key_bytes_at(Path(out["path"]))  # noqa: ASYNC240
        fernet = MultiFernet([Fernet(key_bytes)])
        census = await encrypt_existing_soul_lines(pool, dry_run=True, fernet=fernet)
        out["legacy_plaintext_rows"] = census["hot_migrated"] + census["cold_migrated"]
    else:
        out["legacy_plaintext_rows"] = None
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
    dry-run re-wrap census comes back clean (zero rewrapped, zero broken — a
    not-yet-restarted daemon may still be writing under the old key), then calls
    `soul_crypto.soul_key_rotate_finish` to remove the legacy key."""
    from cryptography.fernet import Fernet

    from src.ingest import soul_crypto
    from src.ingest.soul_store import rewrap_soul_lines_key

    status = soul_crypto.soul_key_status(path=path)
    if finish:
        if not status["rotation_in_flight"]:
            return {"error": "no rotation in flight — nothing to finish"}
        resolved = Path(status["path"])
        # read_key_bytes_at/read_legacy_key_bytes decode EITHER backend
        # (systemd-creds or legacy plaintext) -- a raw .read_bytes() here would
        # hand MultiFernet an still-encrypted systemd-creds blob instead of the
        # actual key, the exact defect this census surfaced during this door's
        # own build. noqa: ASYNC240 -- tiny key files/subprocess, negligible.
        new_fernet = Fernet(soul_crypto.read_key_bytes_at(resolved))  # noqa: ASYNC240
        old_fernet = Fernet(soul_crypto.read_legacy_key_bytes(resolved))  # noqa: ASYNC240
        census = await rewrap_soul_lines_key(
            pool, new_fernet=new_fernet, old_fernet=old_fernet, dry_run=True)
        remaining = census["hot_rewrapped"] + census["cold_rewrapped"]
        broken = census["hot_broken_count"] + census["cold_broken_count"]
        if remaining or broken:
            return {"error": f"{remaining} row(s) still under the old key and "
                             f"{broken} broken row(s) found — re-run `osiris "
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
    pool: asyncpg.Pool, *, repo_url: str | None = None,
) -> dict[str, Any]:
    """Wraps `scripts.osiris_offbox_restore_drill.run_drill` directly (the same
    function that script's own `main()` calls, never a duplicated subprocess
    shell-out). `repo_url` explicit, or every URL in `backup.offbox_repositories`
    (`src.orchestrator.backup_settings.get_backup_settings`) when omitted — one
    drill per configured repository, never guessing which one the operator meant.
    A top-level `error` key is set whenever any drill fails (never only per-drill),
    so a generic caller's own error-key check reports the right exit code / HTTP
    status without re-deriving `all_ok` itself."""
    import asyncio

    from scripts.osiris_offbox_restore_drill import run_drill

    from src.orchestrator.backup_settings import get_backup_settings

    urls = [repo_url] if repo_url else None
    if urls is None:
        settings = await get_backup_settings(pool)
        urls = list(settings.get("offbox_repositories") or [])
    if not urls:
        return {"error": "no offbox repository configured (backup.offbox_repositories "
                         "is empty) and no --repo-url given"}
    results = []
    for url in urls:
        fail = await asyncio.to_thread(run_drill, url)
        results.append({"repo_url": url, "ok": fail is None, "error": fail})
    all_ok = all(r["ok"] for r in results)
    out: dict[str, Any] = {"drills": results, "all_ok": all_ok}
    if not all_ok:
        failing = [r["repo_url"] for r in results if not r["ok"]]
        out["error"] = f"{len(failing)} of {len(results)} drill(s) failed: {failing}"
    return out
