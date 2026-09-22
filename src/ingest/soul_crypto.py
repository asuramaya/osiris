"""Soul-store encryption key management (Thoth mail 9134, operator ruling on thread
773d633a; AMENDED by Thoth DM 9245, wave 17): the LIVE soul store (soul_lines/
soul_lines_cold) is encrypted at rest by osiris itself, one tier above host-disk trust.

`/etc/osiris/soul.key` (overridable via `OSIRIS_SOUL_KEY_FILE`) is the default, matching
the SYSTEM-unit deploy shape (`deploy/osiris-worker.service`/`deploy/osiris-mcp.service`,
`User=osiris`, `EnvironmentFile=/etc/osiris/osiris.env`). THE LIVE DEV BOX RUNS THE OTHER
SHAPE (Thoth DM 9435, confirmed live 2026-09-11): `deploy/user/osiris-worker.service`/
`osiris-mcp.service` are systemd --USER units running as the operator's own login user
(no dedicated service account, no `/opt` tree) — and, critically, carry NO
`EnvironmentFile=` at all; every var is set inline via `Environment=` lines in the unit
file itself, so `OSIRIS_SOUL_KEY_FILE` for THIS shape is set the same way (added directly
to both unit files, not a shared config). `_key_file_path`/`soul_key_init` never assume
which shape is live — the default path is just a default, and the owner-check below
never hardcodes a service account name.

THE KEY DOOR (Thoth mail 12810, operator's word "keys and backup setup configurable
from UI or CLI so a user does not need an agent"), defect 1 fixed the SAME day it was
found live (Thoth mail 12830): `_key_file_path` no longer just falls back to
`_DEFAULT_KEY_FILE` when `OSIRIS_SOUL_KEY_FILE` is unset — an unprivileged caller (the
operator's own interactive shell, which never inherits a unit's inline `Environment=`
lines) resolves the SAME path an INSTALLED --user unit already uses
(`_installed_user_unit_env_value`, a plain sync read of the real file at
`~/.config/systemd/user/osiris-mcp.service`), falling back to `$XDG_CONFIG_HOME/osiris/
soul.key` only on a genuinely fresh box with no deploy yet. `soul_key_init`,
`soul_key_status`, `soul_key_rotate_begin`/`soul_key_rotate_finish` are the four doors
`osiris soul-key <action>` wraps — init mints the first key, status reports filesystem
facts (never key bytes), rotate is a two-step re-wrap (a new key parks the old one at
`<path>.legacy`, re-run to keep sweeping rows written by a not-yet-restarted daemon,
`--finish` removes the legacy key once the receipt is clean).

NO KEYRING BRANCH (amended off the original design posted to 773d633a): the original
env-override -> OS-keyring -> file ladder mirrored `src.connectors.leases.get_lease_key`,
but Thoth's own read of this branch (DM 9245) found it non-deterministic under systemd —
a login session with D-Bus still live would silently mint the key into the OS keyring
instead of the file, invisible to the file-reading worker, and would re-mint a SECOND
key (a second disclosure) the next time something hit the file branch instead. Dropped
entirely: `OSIRIS_SOUL_KEY` env override, else the file, nothing else.

`get_soul_key`/`get_soul_fernet` NEVER GENERATE. A missing key is a hard, named refusal
(`SoulKeyMissing`) — the worker and MCP server both call `get_soul_fernet()` once at
their own boot (not deferred to first write) so a missing key fails loudly at START,
naming the exact fix, rather than failing opaquely on whatever request happens to touch
soul_store first. `soul_key_init`/`soul_key_rotate_begin` are the only two doors that
ever mint a key, both routing through `_generate_and_disclose` — run by a human, in
their own terminal, so the mandatory offline recovery secret lands in front of a
person, never inside a daemon's journal.

`scope` (Thoth DM 9379, no scope growth today): both key functions accept a `scope`
parameter that the operator is weighing widening later (per-tenant keys, one store
becoming several) — a seam at this one lookup point rather than every call site, so that
future change never touches soul_store.py's own encrypt/decrypt call sites. Today there
is exactly one store and `scope` does not vary the lookup at all.
"""
from __future__ import annotations

import getpass
import os
import pwd
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, MultiFernet

_DEFAULT_KEY_FILE = "/etc/osiris/soul.key"
_SERVICE_USER = "osiris"  # deploy/osiris-worker.service's and osiris-mcp.service's own User=


class SoulKeyMissing(RuntimeError):
    """Raised by `get_soul_key` when no key is configured anywhere this module looks —
    never a silent auto-generate. The message IS the fix: the exact `soul-key init`
    invocation to run."""


_FERNET_TOKEN_PREFIX = b"gAAAA"  # base64 of the fixed Fernet version byte (0x80) + start


def is_encrypted(raw: bytes) -> bool:
    """Legacy-plaintext read fallback (Thoth DM 9194/9245): a Fernet token always begins
    with this base64 prefix. A `raw_line`/`content_gzip` blob WITHOUT it predates
    `encrypt_existing_soul_lines` and is legacy plaintext, verified as such by every
    soul_store/handshake reader that checks this first — never decrypted, never a
    spurious InvalidToken. A blob that DOES carry the prefix but fails to decrypt is
    still a real, named break (wrong or rotated-out key, or corruption) — this only
    ever widens what counts as 'not encrypted', never what counts as 'a genuine
    decryption failure'. Retires as a named follow-up once a migration receipt reports
    zero legacy rows fleet-wide."""
    return raw.startswith(_FERNET_TOKEN_PREFIX)


def _installed_user_unit_env_value(env_name: str) -> str | None:
    """THE KEY DOOR, defect 1 (operator's own live hit, Thoth mail 12830): a plain
    synchronous read of an INSTALLED --user unit's own `Environment=` line for
    `env_name`, with systemd's `%h` specifier expanded to THIS process's own home
    directory -- the unit file is real, static text on disk at
    `~/.config/systemd/user/<name>`, the SAME place `_real_install_user_units`
    (cli.py) writes it and `unit_install_drift` reads it back for drift-checking.
    Deliberately no `systemctl --user show` here (settings_service.py's own
    `_restart_unit_env_value` does that, async, for a value that needs the RUNNING
    unit's actual resolved environment) -- this module is pool-free and sync by
    design, and a human running `soul-key init` before either daemon has ever
    started needs the answer from the INSTALLED FILE, not a live process that may
    not exist yet. Checks osiris-mcp.service then osiris-worker.service (either
    carries the same line by construction, one deploy shape); None the moment
    neither is installed or neither carries the line (a fresh box with no deploy
    yet, or the system-unit shape, which installs no --user units at all)."""
    unit_dir = Path.home() / ".config" / "systemd" / "user"
    prefix = f"Environment={env_name}="
    for name in ("osiris-mcp.service", "osiris-worker.service"):
        unit_path = unit_dir / name
        try:
            text = unit_path.read_text()
        except OSError:
            continue
        for line in text.splitlines():
            if line.startswith(prefix):
                return line.removeprefix(prefix).strip().replace("%h", str(Path.home()))
    return None


def _key_file_path(*, explicit: str | None = None) -> Path:
    """Resolution ladder (THE KEY DOOR, defect 1): an explicit `--path` always wins;
    else `OSIRIS_SOUL_KEY_FILE` if the calling process already has it (a running
    daemon's own env, or an operator who exported it by hand); else, for an
    UNPRIVILEGED caller, the path an INSTALLED --user unit actually uses
    (`_installed_user_unit_env_value`) so a human running `soul-key init` in a
    plain login shell lands the key exactly where osiris-mcp/osiris-worker will
    look for it, without exporting anything; else `$XDG_CONFIG_HOME/osiris/
    soul.key` (or `~/.config/osiris/soul.key`) for a genuinely fresh unprivileged
    box with no deploy yet. ROOT never gets the --user-unit or XDG fallback (no
    natural `~` for a system-unit deploy to land the file at) -- root falls
    straight through to `_DEFAULT_KEY_FILE` (/etc/osiris/soul.key), matching the
    system-unit shape's own `EnvironmentFile=/etc/osiris/osiris.env` default."""
    if explicit:
        return Path(explicit).expanduser()
    env = os.environ.get("OSIRIS_SOUL_KEY_FILE")
    if env:
        return Path(env).expanduser()
    if os.getuid() != 0:
        installed = _installed_user_unit_env_value("OSIRIS_SOUL_KEY_FILE")
        if installed:
            return Path(installed).expanduser()
        xdg = os.environ.get("XDG_CONFIG_HOME")
        base = Path(xdg).expanduser() if xdg else Path.home() / ".config"
        return base / "osiris" / "soul.key"
    return Path(_DEFAULT_KEY_FILE)


def _init_command_hint(path: Path) -> str:
    return (
        f"no soul-store encryption key found at {path} (and OSIRIS_SOUL_KEY is unset) — "
        "run `osiris soul-key init` ONCE, in your own terminal, as whichever user the "
        f"worker/MCP service actually runs as (the {_SERVICE_USER!r} account for the "
        "system-unit deploy shape, or your own login user for a systemd --user deploy — "
        "the shape osiris-worker/osiris-mcp use today), before starting either unit; "
        "or, running as root for the system-unit shape, with --owner <that user> to "
        "chown the key into place")


def _deploy_note(path: Path) -> str:
    """THE KEY DOOR, defect 1 (Thoth mail 12830): names the exact next step for
    whichever deploy shape `path` actually resolved against, and NEVER suggests
    `sudo osiris ...` — `sudo` strips PATH down to root's own restricted default,
    which does not include wherever this venv's `osiris` console-script actually
    lives, so that exact command would fail with 'command not found' regardless of
    which shape is live."""
    if os.getuid() == 0:
        if str(path) == _DEFAULT_KEY_FILE:
            return (
                "this is already the default path — no env change needed for a "
                "system-unit deploy (deploy/osiris-worker.service/osiris-mcp.service "
                "already assume it); restart both units to pick up the new key")
        return (
            f"add `OSIRIS_SOUL_KEY_FILE={path}` to /etc/osiris/osiris.env (both "
            "units' own EnvironmentFile=) for a system-unit deploy, then restart "
            "both units")
    if _installed_user_unit_env_value("OSIRIS_SOUL_KEY_FILE") == str(path):
        return (
            "this already matches the installed osiris-mcp/osiris-worker --user "
            "unit(s) own OSIRIS_SOUL_KEY_FILE — no env change needed; restart both "
            "(`systemctl --user restart osiris-mcp osiris-worker`) to pick up the "
            "new key")
    return (
        f"add an explicit `Environment=OSIRIS_SOUL_KEY_FILE={path}` line to BOTH "
        "deploy/user/osiris-worker.service and deploy/user/osiris-mcp.service "
        "directly (a systemd --user deploy carries no shared EnvironmentFile= at "
        "all), then `systemctl --user daemon-reload` and restart both units")


def _generate_and_disclose(where: str) -> bytes:
    """Mint a fresh key and print the MANDATORY offline recovery secret before this
    call's own caller persists it anywhere — a crash between this print and the
    persist step still leaves the operator holding the only copy that matters, never
    the reverse. Every place a key is ever minted (`soul_key_init`, `soul_key_rotate_
    begin`, both below) routes through this, so "print the recovery secret" can
    never be silently skipped."""
    key = Fernet.generate_key()
    print(
        "osiris soul-store encryption key GENERATED — THIS IS THE ONLY TIME THIS KEY "
        f"PRINTS (persisting to {where}). Copy it now to OFFLINE custody — a password "
        "manager entry, a printed copy — kept OUTSIDE this box and OUTSIDE any backup "
        "target (a backed-up copy sitting next to the ciphertext it protects defeats "
        "the whole point). Losing BOTH this printed copy and the live file makes every "
        "stored transcript permanently, irrecoverably unreadable.\n"
        f"  {key.decode()}\n",
        flush=True)
    return key


def get_soul_key(scope: str = "default") -> bytes:  # noqa: ARG001 — the lookup seam, see module docstring
    """Resolve the PRIMARY Fernet key: explicit env override, else the key file. NEVER
    GENERATES — raises `SoulKeyMissing` (naming the exact `soul-key init` command) when
    neither is present. `OSIRIS_SOUL_KEY` overrides the file (tests, emergency operator
    override); an existing file's bytes are read silently, never re-disclosed — only a
    freshly GENERATED key (`soul_key_init` alone) ever prints."""
    env = os.environ.get("OSIRIS_SOUL_KEY")
    if env:
        return env.encode()
    path = _key_file_path()
    if not path.exists():
        raise SoulKeyMissing(_init_command_hint(path))
    return path.read_bytes()


def _legacy_key_file_path(path: Path) -> Path:
    """The sibling file a rotation-in-flight parks the OLD primary key at, alongside
    `path` — `<name>.legacy` in the same directory, so it inherits the same
    permissions/ownership story the primary key file already has. Existence of this
    file IS "a rotation is in flight", read by `soul_key_status`/`soul_key_rotate_*`
    rather than a separate flag anywhere."""
    return path.with_name(path.name + ".legacy")


def get_soul_fernet(scope: str = "default") -> MultiFernet:
    """The encrypt/decrypt object every soul_store write/read site uses. `MultiFernet`
    over the PRIMARY key (`get_soul_key`, always first — new writes encrypt with this
    one only) plus any LEGACY keys named in `OSIRIS_SOUL_KEY_LEGACY` (comma-separated
    Fernet keys, still valid to DECRYPT, never used to encrypt a new write) — the
    rotation window this exists for: old ciphertext keeps reading while a migration
    re-encrypts it onto the new primary, so rotation runs as a background pass rather
    than stop-the-world. `MultiFernet.decrypt` tries each key in order and raises
    `cryptography.fernet.InvalidToken` only once NONE of them work."""
    keys = [Fernet(get_soul_key(scope=scope))]
    legacy = os.environ.get("OSIRIS_SOUL_KEY_LEGACY", "")
    keys.extend(Fernet(k.strip().encode()) for k in legacy.split(",") if k.strip())
    return MultiFernet(keys)


def soul_key_init(*, owner: str | None = None, path: str | None = None) -> dict[str, Any]:
    """THE ONLY GENERATOR (Thoth DM 9245) — meant to be run once, by a human, in their
    own terminal (the CLI door `osiris soul-key init` wraps this unchanged); the worker
    and MCP server never call this, only `get_soul_fernet`/`get_soul_key`.

    REFUSES IF A KEY ALREADY EXISTS at the target path — idempotent refusal, never a
    silent re-mint (`osiris soul-key rotate` is the real door for replacing a live
    key, never overwriting the primary file in place here).

    REFUSES ONLY THE GENUINELY AMBIGUOUS CASE: running as ROOT with no `owner=` given —
    root has no natural owner to land the file as (Thoth DM 9435: this box's live units
    are systemd --USER, no dedicated service account at all, so "the service user" is
    not even a fixed name to assume). Any NON-root caller proceeds directly, no `owner=`
    required — the file lands owned by whoever ran this, which is already correct
    whenever the invoking user is the one the service itself runs as (a `sudo -u osiris`
    shell for the system-unit shape, or simply the operator's own login for the --user
    shape this box actually runs). `owner=` stays available for the explicit root-deploy
    case: the key file AND its parent directory are chown'd to that user after writing.

    `path=` (THE KEY DOOR, defect 1) overrides `_key_file_path`'s own resolution
    ladder entirely — the escape hatch for a genuinely unusual layout; every ordinary
    caller leaves it None and gets the SAME path an installed --user unit already
    uses, resolved without exporting anything (`_key_file_path`'s own docstring).

    Prints the mandatory offline recovery secret exactly once (`_generate_and_disclose`),
    and names the exact next step via `_deploy_note` — never a `sudo osiris ...` example,
    see that function's own docstring for why."""
    resolved = _key_file_path(explicit=path)
    if resolved.exists():
        return {"error": f"{resolved} already exists — soul-key init never overwrites "
                         "an existing key in place; `osiris soul-key rotate` is the "
                         "door for replacing a live key"}
    current_user = getpass.getuser()
    if os.getuid() == 0 and owner is None:
        return {"error": "refusing — running as root with no --owner given. Root has no "
                         "natural owner for the key file: pass --owner <user>, naming "
                         "whichever user the worker/MCP service actually runs as (the "
                         f"{_SERVICE_USER!r} account for the system-unit deploy shape, "
                         "or the operator's own login user for a systemd --user deploy)"}
    target_owner = owner or current_user
    resolved.parent.mkdir(parents=True, exist_ok=True)
    key = _generate_and_disclose(str(resolved))
    resolved.write_bytes(key)
    resolved.chmod(0o600)
    chowned = False
    if owner is not None and current_user != owner:
        pw = pwd.getpwnam(owner)
        os.chown(resolved, pw.pw_uid, pw.pw_gid)
        os.chown(resolved.parent, pw.pw_uid, pw.pw_gid)
        chowned = True
    return {
        "path": str(resolved), "owner": target_owner, "chowned": chowned,
        "systemd_note": _deploy_note(resolved),
    }


def soul_key_status(*, path: str | None = None) -> dict[str, Any]:
    """Filesystem-only facts about the key file `_key_file_path` resolves to (THE KEY
    DOOR) — NEVER the key bytes themselves, that would defeat the whole point of a
    status door existing separately from a debug print. `present`, the resolved
    `path`, octal `mode` (None when absent), `created_age_seconds` (None when
    absent), and `rotation_in_flight` (a `.legacy` sibling file exists — see
    `_legacy_key_file_path`'s own docstring). Callers who also want the live
    soul_lines legacy-row census (a DB read this pool-free module deliberately never
    does) compose this with `soul_store.encrypt_existing_soul_lines(pool,
    dry_run=True)` themselves — `cmd_soul_key`'s own job, not this one's."""
    import time

    resolved = _key_file_path(explicit=path)
    present = resolved.exists()
    mode: str | None = None
    created_age_seconds: float | None = None
    if present:
        st = resolved.stat()
        mode = oct(st.st_mode & 0o777)
        created_age_seconds = max(0.0, time.time() - st.st_ctime)
    return {
        "present": present, "path": str(resolved), "mode": mode,
        "created_age_seconds": created_age_seconds,
        "rotation_in_flight": _legacy_key_file_path(resolved).exists(),
    }


def soul_key_rotate_begin(*, path: str | None = None) -> dict[str, Any]:
    """Step 1 of 2 (THE KEY DOOR): mints a fresh key, parks the CURRENT primary at
    `<path>.legacy` (`_legacy_key_file_path`), and writes the new key to the primary
    path — the SAME disclosure law `soul_key_init` holds (`_generate_and_disclose`,
    prints exactly once). IDEMPOTENT AND SAFE TO RE-RUN mid-rotation: if `.legacy`
    already exists (a rotation is already in flight), this refuses rather than
    minting a SECOND new key and stranding the first rotation's own legacy key —
    `osiris soul-key rotate` re-run with a rotation already in flight is meant to
    re-drive the RE-WRAP pass (the CLI's own job, via `soul_store.
    rewrap_soul_lines_key`), never mint again.

    REFUSES if no primary key exists yet at all — there is nothing to rotate away
    from; `soul-key init` is the door for a genuinely first key.

    Returns `new_key`/`old_key` (raw bytes, for the CALLER to build the two Fernet
    objects the re-wrap pass needs — this module stays pool-free, the DB-touching
    re-wrap itself lives in soul_store.py) plus `path`/`legacy_path` and the same
    `systemd_note` shape `soul_key_init` returns (a rotation changes what's AT the
    path, never the path itself, so no unit env change is ever needed for the
    daemons to find the new key — only a RESTART, named here)."""
    resolved = _key_file_path(explicit=path)
    if not resolved.exists():
        return {"error": f"no key exists at {resolved} yet — `osiris soul-key init` "
                         "first, there is nothing to rotate away from"}
    legacy_path = _legacy_key_file_path(resolved)
    if legacy_path.exists():
        return {"error": f"a rotation is already in flight ({legacy_path} exists) — "
                         "re-run `osiris soul-key rotate` to continue re-wrapping "
                         "rows onto the key already generated, or `--finish` once "
                         "the receipt reports zero rows remain under the old key"}
    old_key = resolved.read_bytes()
    new_key = _generate_and_disclose(str(resolved))
    legacy_path.write_bytes(old_key)
    legacy_path.chmod(0o600)
    resolved.write_bytes(new_key)
    resolved.chmod(0o600)
    return {
        "path": str(resolved), "legacy_path": str(legacy_path),
        "new_key": new_key, "old_key": old_key,
        "systemd_note": (
            "restart osiris-mcp and osiris-worker now to pick up the new primary "
            "key. Any row either daemon writes BEFORE its own restart still "
            "encrypts under the OLD key in its own cached process memory — safe "
            "(the old key stays valid to decrypt until `--finish`), but re-run "
            "`osiris soul-key rotate` (idempotent) after both have restarted to "
            "sweep those rows onto the new key too, before `--finish`"),
    }


def soul_key_rotate_finish(*, path: str | None = None) -> dict[str, Any]:
    """Step 2 of 2 (THE KEY DOOR): removes the `.legacy` key file once the caller has
    already confirmed (via `soul_store.rewrap_soul_lines_key`'s own dry-run receipt)
    that zero rows remain encrypted under it — this function itself does NOT
    re-check the row count (pool-free by design, see module docstring); `cmd_soul_key`
    refuses to call this at all until that receipt is clean. REFUSES if no rotation
    is in flight (nothing to finish)."""
    resolved = _key_file_path(explicit=path)
    legacy_path = _legacy_key_file_path(resolved)
    if not legacy_path.exists():
        return {"error": f"no rotation in flight — {legacy_path} does not exist, "
                         "nothing to finish"}
    legacy_path.unlink()
    return {
        "path": str(resolved),
        "note": "old key removed. OSIRIS_SOUL_KEY_LEGACY (if you had exported it "
                "anywhere) is no longer needed — the old key is gone, and can never "
                "decrypt anything again.",
    }
