"""Soul-store encryption key management: the live soul store (soul_lines/
soul_lines_cold) is encrypted at rest by osiris itself, one tier above host-disk trust.

`/etc/osiris/soul.key` (overridable via `OSIRIS_SOUL_KEY_FILE`) is the default, matching
the system-unit deploy shape (`deploy/osiris-worker.service`/`deploy/osiris-mcp.service`,
`User=osiris`, `EnvironmentFile=/etc/osiris/osiris.env`). The live dev box runs a
different shape, confirmed live on 2026-09-11: `deploy/user/osiris-worker.service`/
`osiris-mcp.service` are systemd --user units running as the operator's own login user
(no dedicated service account, no `/opt` tree), and, critically, carry no
`EnvironmentFile=` at all; every var is set inline via `Environment=` lines in the unit
file itself, so `OSIRIS_SOUL_KEY_FILE` for this shape is set the same way (added directly
to both unit files, not a shared config). `_key_file_path`/`soul_key_init` never assume
which shape is live: the default path is just a default, and the owner check below never
hardcodes a service account name.

Key-path resolution was changed so keys and backup setup can be configured from the UI
or the CLI without needing an agent to do it. The defect this exposed was fixed the same
day it was found live: `_key_file_path` no longer just falls back to `_DEFAULT_KEY_FILE`
when `OSIRIS_SOUL_KEY_FILE` is unset. An unprivileged caller (the operator's own
interactive shell, which never inherits a unit's inline `Environment=` lines) instead
resolves the same path an installed --user unit already uses
(`_installed_user_unit_env_value`, a plain sync read of the real file at
`~/.config/systemd/user/osiris-mcp.service`), falling back to `$XDG_CONFIG_HOME/osiris/
soul.key` only on a genuinely fresh box with no deploy yet.

Key custody was rewritten on 2026-09-22, superseding the original design in which a
printed key was the only recovery path. Measured live on this box (all as the login
user, no root): `systemd-creds encrypt --user --with-key=host` round-trips without root
(a 559-byte blob, bound to this machine's own credential host key); `--with-key=tpm2`
refuses in user scope ("Selected key not available in --uid= scoped mode, refusing");
`/dev/tpmrm0` is `tss`-group-only and the operator is not a member. So:

  (1) The key at rest is now a systemd user credential, never a plaintext file by
      default: `soul_key_init` mints the Fernet key in memory and writes only the
      encrypted blob via `systemd-creds encrypt --user --with-key=host` (upgrading
      automatically to `--with-key=host+tpm2` once `_is_tss_member()` says the
      operator has joined `tss`; this never auto-joins the group, it only ever prints
      the one-line `usermod -aG tss <user>` hint). A `.meta.json` sidecar (never secret,
      chmod 0600 anyway) records which backend was used, read back by `soul_key_status`.
      `backend="file"` stays available as an explicit, warned fallback, the shape this
      whole module used before this change, kept for a box with no systemd-creds at all.
      The first key must still come from the normal CLI, correcting an issue in the
      original design: the default blob location is the per-user encrypted credstore
      (`_credential_path`, `~/.config/credstore.encrypted/soul.key`), and both units get
      `ImportCredential=soul.key` (deploy/user/*.service), not
      `LoadCredentialEncrypted=...:<hard path>`, which failed a unit's own start outright
      until the key existed: a bootstrap deadlock, since minting the key normally means
      running the CLI through that same already-running unit. `ImportCredential=`
      tolerates a missing entry (confirmed live); when present, systemd decrypts it for
      the process into `$CREDENTIALS_DIRECTORY/soul.key` before it ever starts, so
      `get_soul_key` reads that directory first (the daemon path, no `systemd-creds`
      subprocess needed at read time), then the credstore file directly (the CLI path,
      decrypted on demand via `systemd-creds decrypt --user`), then a legacy plaintext
      file (the old `backend="file"` shape), in that order.

  (2) Recovery is now a FIDO2 hmac-secret ("prf") enrollment on the operator's Security
      Key, not a printed key by default: `soul_key_enroll_recovery` makes a discoverable
      credential with the PRF extension (python-fido2, PIN + touch required), derives a
      wrapping key from the PRF output at a random salt, wraps the raw Fernet key with
      it, and writes `<path>.recovery.json` (credential id, salt, wrapped key, a
      fingerprint of the wrapped key's own plaintext, safe to keep on the NAS and in the
      vault, since reading it needs the physical key and its PIN). `soul_key_recover`
      reverses it (PIN + touch) and re-seals the recovered key under the host credential
      on a new machine. The old printed-key banner survives as an explicit
      `print_recovery=True` opt-in on `soul_key_init`/`soul_key_rotate_begin`: no longer
      the default, and no longer the only path. `soul_key_status` reports every recovery
      path actually enrolled and warns when the count is 1 or fewer (losing the sole
      enrolled path loses the data).

There is no OS-keyring branch (amended off the original design): the original
env-override -> OS-keyring -> file ladder mirrored
`src.connectors.leases.get_lease_key`, but a closer read of that branch found it
non-deterministic under systemd: a login session with D-Bus still live would silently
mint the key into the OS keyring instead of the file, invisible to the file-reading
worker, and would re-mint a second key (a second disclosure) the next time something hit
the file branch instead. It was dropped entirely: `OSIRIS_SOUL_KEY` env override always
wins (tests, emergency operator override), then the resolution ladder above, nothing
else.

`get_soul_key`/`get_soul_fernet` never generate. A missing key is a hard, named refusal
(`SoulKeyMissing`); the worker and MCP server both call `get_soul_fernet()` once at their
own boot (not deferred to first write) so a missing key fails loudly at start, naming the
exact fix, rather than failing opaquely on whatever request happens to touch soul_store
first. `soul_key_init`/`soul_key_rotate_begin` are the only two entry points that ever
mint a key, both routing through `_generate_key`; whether either discloses it as a
printed recovery secret is the caller's own `print_recovery` choice, never automatic.

`scope`: both key functions accept a `scope` parameter that the operator is weighing
widening later (per-tenant keys, one store becoming several), a seam at this one lookup
point rather than every call site, so a future change never touches soul_store.py's own
encrypt/decrypt call sites. Today there is exactly one store and `scope` does not vary
the lookup at all.
"""
from __future__ import annotations

import getpass
import json
import os
import pwd
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken, MultiFernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from src.ingest import systemd_credential

_is_tss_member = systemd_credential.is_tss_member
_systemd_creds_available = systemd_credential.systemd_creds_available
_shared_encrypt_with_systemd_creds = systemd_credential.encrypt_with_systemd_creds
_shared_decrypt_with_systemd_creds = systemd_credential.decrypt_with_systemd_creds

_DEFAULT_KEY_FILE = "/etc/osiris/soul.key"
_SERVICE_USER = "osiris"  # deploy/osiris-worker.service's and osiris-mcp.service's own User=
_CRED_NAME = "soul.key"  # the `--name=` systemd-creds is minted/read under, matching
                         # deploy/user/*.service's own `ImportCredential=soul.key`
_TSS_GROUP = "tss"  # owns /dev/tpmrm0 on this box; membership gates --with-key=host+tpm2


class SoulKeyMissing(RuntimeError):
    """Raised by `get_soul_key` when no key is configured anywhere this module looks,
    never a silent auto-generate. The message is the fix: the exact `soul-key init`
    invocation to run."""


class SoulKeyRecoveryError(RuntimeError):
    """Raised by `soul_key_enroll_recovery`/`soul_key_recover` for a FIDO2-layer
    failure (no device, wrong PIN, extension unsupported, credential not found).
    Named separately from `SoulKeyMissing` because the fix is never "run soul-key
    init", it's "plug in your Security Key" or similar, and callers (the CLI) print
    a different hint for each."""


_FERNET_TOKEN_PREFIX = b"gAAAA"  # base64 of the fixed Fernet version byte (0x80) + start


def is_encrypted(raw: bytes) -> bool:
    """Legacy-plaintext read fallback: a Fernet token always begins with this base64
    prefix. A `raw_line`/`content_gzip` blob without it predates
    `encrypt_existing_soul_lines` and is legacy plaintext, verified as such by every
    soul_store/handshake reader that checks this first, never decrypted, never a
    spurious InvalidToken. A blob that does carry the prefix but fails to decrypt is
    still a real, named break (wrong or rotated-out key, or corruption): this only
    ever widens what counts as 'not encrypted', never what counts as 'a genuine
    decryption failure'. This check can retire as a follow-up once a migration
    receipt reports zero legacy rows remaining across every deployment."""
    return raw.startswith(_FERNET_TOKEN_PREFIX)


def _installed_user_unit_env_value(env_name: str) -> str | None:
    """Resolves an installed --user unit's env value: a plain synchronous read of an
    installed --user unit's own `Environment=` line for `env_name`, with systemd's
    `%h` specifier expanded to this process's own home directory. The unit file is
    real, static text on disk at `~/.config/systemd/user/<name>`, the same place
    `_real_install_user_units` (cli.py) writes it and `unit_install_drift` reads it
    back for drift-checking. Deliberately no `systemctl --user show` here
    (settings_service.py's own `_restart_unit_env_value` does that, async, for a
    value that needs the running unit's actual resolved environment): this module is
    pool-free and sync by design, and a human running `soul-key init` before either
    daemon has ever started needs the answer from the installed file, not a live
    process that may not exist yet. Checks osiris-mcp.service then
    osiris-worker.service (either carries the same line by construction, one deploy
    shape); returns None the moment neither is installed or neither carries the line
    (a fresh box with no deploy yet, or the system-unit shape, which installs no
    --user units at all)."""
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
    """Resolution ladder for the key path: an explicit `--path` always wins; else
    `OSIRIS_SOUL_KEY_FILE` if the calling process already has it (a running
    daemon's own env, or an operator who exported it by hand); else, for an
    unprivileged caller, the path an installed --user unit actually uses
    (`_installed_user_unit_env_value`) so a human running `soul-key init` in a
    plain login shell lands the key exactly where osiris-mcp/osiris-worker will
    look for it, without exporting anything; else `$XDG_CONFIG_HOME/osiris/
    soul.key` (or `~/.config/osiris/soul.key`) for a genuinely fresh unprivileged
    box with no deploy yet. Root never gets the --user-unit or XDG fallback (no
    natural `~` for a system-unit deploy to land the file at): root falls
    straight through to `_DEFAULT_KEY_FILE` (/etc/osiris/soul.key), matching the
    system-unit shape's own `EnvironmentFile=/etc/osiris/osiris.env` default.

    This is the logical key name, not necessarily a real file on disk any more
    since key custody was rewritten: the systemd-creds backend stores the actual
    secret at `_credential_path(this)` instead; a plaintext file at this exact
    path only exists for the explicit `backend="file"` fallback."""
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


def _credential_path(key_path: Path, *, explicit: bool = False) -> Path:
    """Where the systemd-creds-encrypted blob actually lives, given that the first
    key must always come from the normal CLI: default (`explicit=False`, every
    ordinary caller with no `--path`/`path=` given) is the per-user encrypted
    credstore directory (`~/.config/credstore.encrypted/soul.key`,
    `systemd_credential.user_credstore_encrypted_dir()`), the location `deploy/
    user/*.service`'s own `ImportCredential=soul.key` searches by default and
    tolerates missing (confirmed live: a unit with no matching credstore entry
    starts and finishes cleanly, unlike the old `LoadCredentialEncrypted=soul.
    key:<hard path>` this replaces, which failed the unit outright if the file
    wasn't there yet). Explicit (`explicit=True`, a caller-given `--path`/
    `path=`) uses the old sibling-of-path shape (`<key_path>.cred`), kept as a
    documented override for a non-standard layout (a test, a scratch
    directory) that was never going through `ImportCredential=` anyway.

    Strictly one or the other, never both checked: the same single-ladder
    discipline `_key_file_path` itself already holds for the logical name.
    Every caller here threads `explicit=path is not None` off its own `path=`
    parameter, so two calls that pass the same `path=` (or both omit it)
    always agree on where the credential lives; mixing an explicit-path init
    with a bare-path status/rotate/get (or the reverse) is a genuine user
    error this function does not paper over."""
    if explicit:
        return key_path.with_name(key_path.name + ".cred")
    return systemd_credential.user_credstore_encrypted_dir() / _CRED_NAME


def _meta_path(key_path: Path) -> Path:
    """A small, never-secret sidecar (chmod 0600 anyway, out of caution, never out
    of necessity) recording which backend a credential/file was minted with:
    `systemd-creds`' own blob format doesn't expose which key type sealed it
    without decrypting, so `soul_key_status` reads this back instead of guessing."""
    return key_path.with_name(key_path.name + ".meta.json")


def _legacy_key_file_path(path: Path) -> Path:
    """The sibling file a rotation-in-flight parks the old primary key at, alongside
    `path`: `<name>.legacy` in the same directory, so it inherits the same
    permissions/ownership story the primary key file already has. Existence of this
    file is "a rotation is in flight", read by `soul_key_status`/`soul_key_rotate_*`
    rather than a separate flag anywhere. Named after the logical key name
    (`_key_file_path`'s own return), never the credential path directly: the
    caller decides whether the parked bytes are plaintext or systemd-creds
    ciphertext by the same backend the primary carried."""
    return path.with_name(path.name + ".legacy")


def _recovery_path(key_path: Path) -> Path:
    """Where a FIDO2 recovery enrollment's own wrapped-key blob lives:
    `<key_path>.recovery.json`. Safe to keep on the NAS and in the vault: reading
    it needs the physical Security Key and its PIN, the same two factors
    `soul_key_recover` demands."""
    return key_path.with_name(key_path.name + ".recovery.json")


# `_is_tss_member`/`_systemd_creds_available` (imported above from
# `src.ingest.systemd_credential`) and the two thin wrappers below are the only
# systemd-creds surface this module needs: the actual subprocess boundary lives
# in that shared module now, a follow-on to the key-custody rewrite that gives
# the restic offload runner the same shape for its own repository password, so
# the soul-store key and the restic password never carry two independently
# maintained copies of the exact same systemd-creds invocation. Kept as
# module-level names (not inlined at each call site) so existing tests that
# monkeypatch `soul_crypto._is_tss_member`/`soul_crypto._systemd_creds_available`
# keep working unchanged: patching a module attribute is agnostic to whether
# that attribute is a `def` or an imported alias.
def _encrypt_with_systemd_creds(plaintext: bytes, *, with_key: str) -> bytes:
    return _shared_encrypt_with_systemd_creds(plaintext, name=_CRED_NAME, with_key=with_key)


def _decrypt_with_systemd_creds(blob: bytes) -> bytes:
    return _shared_decrypt_with_systemd_creds(blob, name=_CRED_NAME)


def read_key_bytes_at(resolved: Path, *, explicit: bool = False) -> bytes:
    """The raw Fernet key bytes actually backing the logical path `resolved`,
    regardless of backend: the credential at `_credential_path(resolved,
    explicit=explicit)`, decrypted, when it exists; the legacy plaintext file at
    `resolved` itself otherwise. For a caller (`src.orchestrator.soul_key`'s own
    status census) that already has an explicit resolved path in hand and needs
    its real bytes, `get_soul_key()`'s own env-first resolution ladder is the
    wrong tool here: it ignores any `path=` a caller resolved by hand. `explicit`
    must match whatever `path=` the caller's own resolution used to arrive at
    `resolved` (`path is not None`), the same single-ladder discipline
    `_credential_path` itself holds, never both locations checked. Raises
    `FileNotFoundError` if neither exists; callers that already called
    `soul_key_status` first (checking `present`) never hit that."""
    cred_path = _credential_path(resolved, explicit=explicit)
    if cred_path.exists():
        return _decrypt_with_systemd_creds(cred_path.read_bytes())
    return resolved.read_bytes()


def read_legacy_key_bytes(resolved: Path) -> bytes:
    """The raw Fernet key bytes for a rotation-in-flight's own parked old key at
    `_legacy_key_file_path(resolved)`. Unlike the primary key, the legacy blob
    is stored directly at that path (never a further `.cred`-suffixed sibling;
    `soul_key_rotate_begin` parks whatever bytes the old carrier held, verbatim),
    so backend is read from `_meta_path` of the legacy path itself, written by
    that same rotate_begin call. A `file`-backend rotation writes no legacy meta
    at all (matching `_write_key_for_backend`'s own contract), so its absence
    means "read the bytes as-is, they're already plaintext."""
    legacy_path = _legacy_key_file_path(resolved)
    if _meta_path(legacy_path).exists():
        return _decrypt_with_systemd_creds(legacy_path.read_bytes())
    return legacy_path.read_bytes()


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
    """Names the exact next step for whichever deploy shape `path` actually
    resolved against, and never suggests `sudo osiris ...`: `sudo` strips PATH
    down to root's own restricted default, which does not include wherever this
    venv's `osiris` console-script actually lives, so that exact command would
    fail with 'command not found' regardless of which shape is live. Unchanged
    by the key-custody rewrite or the requirement that the first key come from
    the normal CLI: this guidance is identical whether the primary is a
    systemd-creds credential (wherever `_credential_path` puts it, the credstore
    by default) or a legacy plaintext file at the logical path itself."""
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


def _generate_key() -> bytes:
    """Mint a fresh Fernet key. Never prints it. Every caller decides separately
    (`print_recovery=`) whether to disclose it as a printed recovery secret; see
    `_disclose_recovery_secret`. Split from that print as part of the key-custody
    rewrite, because the printed banner is no longer automatic: FIDO2 enrollment
    is the default recovery path now, not a printed key nobody asked for
    cluttering a terminal that's about to run `enroll-recovery` right after."""
    return Fernet.generate_key()


def _disclose_recovery_secret(key: bytes, where: str) -> None:
    """Print the mandatory-when-chosen offline recovery secret. Called only when
    a caller's own `print_recovery=True` asks for it (the old default-and-only-
    path banner survives verbatim as this opt-in after the key-custody rewrite).
    A crash between this print and the persist step still leaves the operator
    holding the only copy that matters, never the reverse."""
    print(
        "osiris soul-store encryption key GENERATED — THIS IS THE ONLY TIME THIS KEY "
        f"PRINTS (persisting to {where}). Copy it now to OFFLINE custody — a password "
        "manager entry, a printed copy — kept OUTSIDE this box and OUTSIDE any backup "
        "target (a backed-up copy sitting next to the ciphertext it protects defeats "
        "the whole point). Losing BOTH this printed copy and the live file makes every "
        "stored transcript permanently, irrecoverably unreadable.\n"
        f"  {key.decode()}\n",
        flush=True)


def get_soul_key(scope: str = "default") -> bytes:  # noqa: ARG001 (the lookup seam, see module docstring)
    """Resolve the primary Fernet key. Never generates; raises `SoulKeyMissing`
    (naming the exact `soul-key init` command) when nothing below resolves.

    Order, matching the rewritten key-custody design and the requirement that the
    first key come from the normal CLI: `OSIRIS_SOUL_KEY` env override (tests,
    emergency operator override) always wins; else, when running under a unit that
    declares `ImportCredential=soul.key`, systemd has already decrypted it for this
    process into `$CREDENTIALS_DIRECTORY/soul.key` before the process ever started,
    a plain file read, no `systemd-creds` subprocess at read time, the daemon's own
    fast path; else, for a caller with no `$CREDENTIALS_DIRECTORY` (the CLI, which
    never runs under `ImportCredential=`), the credstore file (`_credential_path`)
    if one exists, decrypted on demand via `systemd-creds decrypt --user`; else a
    legacy plaintext file at the resolved path itself (the explicit `backend="file"`
    shape `soul_key_init` still supports). An existing key's bytes are read
    silently, never re-disclosed; only a freshly generated key, and only when its
    own caller opts into `print_recovery=True`, ever prints."""
    env = os.environ.get("OSIRIS_SOUL_KEY")
    if env:
        return env.encode()
    cred_dir = os.environ.get("CREDENTIALS_DIRECTORY")
    if cred_dir:
        daemon_path = Path(cred_dir) / _CRED_NAME
        if daemon_path.exists():
            return daemon_path.read_bytes()
    path = _key_file_path()
    cred_path = _credential_path(path)
    if cred_path.exists():
        return _decrypt_with_systemd_creds(cred_path.read_bytes())
    if path.exists():
        return path.read_bytes()
    raise SoulKeyMissing(_init_command_hint(path))


def get_soul_fernet(scope: str = "default") -> MultiFernet:
    """The encrypt/decrypt object every soul_store write/read site uses. `MultiFernet`
    over the primary key (`get_soul_key`, always first: new writes encrypt with this
    one only) plus any legacy keys named in `OSIRIS_SOUL_KEY_LEGACY` (comma-separated
    Fernet keys, still valid to decrypt, never used to encrypt a new write). This is
    the rotation window this exists for: old ciphertext keeps reading while a
    migration re-encrypts it onto the new primary, so rotation runs as a background
    pass rather than stop-the-world. `MultiFernet.decrypt` tries each key in order
    and raises `cryptography.fernet.InvalidToken` only once none of them work."""
    keys = [Fernet(get_soul_key(scope=scope))]
    legacy = os.environ.get("OSIRIS_SOUL_KEY_LEGACY", "")
    keys.extend(Fernet(k.strip().encode()) for k in legacy.split(",") if k.strip())
    return MultiFernet(keys)


def _resolve_backend(requested: str | None) -> str:
    """`None` (auto, every ordinary caller): `host+tpm2` when `_is_tss_member()`
    (a stronger binding, offered automatically the moment it's actually usable,
    never auto-joining the group to get there), else `host-cred` when
    `_systemd_creds_available()`, else `file` (a non-systemd host, or one too old
    to carry the binary) as the last-resort explicit fallback. An explicit
    `requested` value is trusted as-is (the CLI's own `--backend` escape hatch)."""
    if requested is not None:
        return requested
    if not _systemd_creds_available():
        return "file"
    return "host+tpm2" if _is_tss_member() else "host-cred"


def _write_key_for_backend(
    key: bytes, *, resolved: Path, backend: str, explicit_path: bool = False,
) -> tuple[Path, str]:
    """Writes `key` under `backend`'s own shape, returning `(written_path,
    effective_backend)`. `file` writes the plaintext directly (0600) at
    `resolved`'s own logical name. `host-cred`/`host+tpm2` encrypt via
    `systemd-creds` into `_credential_path(resolved, explicit=explicit_path)`,
    the default credstore location, or the old sibling-of-path shape when a
    caller-given `--path`/`path=` was in play, and record the backend in
    `_meta_path(resolved)` (never secret, chmod 0600 anyway) so `soul_key_
    status` can report it back without decrypting anything. The credstore
    directory may not exist yet on a fresh box (`mkdir(parents=True)` covers
    both shapes)."""
    if backend == "file":
        resolved.write_bytes(key)
        resolved.chmod(0o600)
        return resolved, "file"
    with_key = "host+tpm2" if backend == "host+tpm2" else "host"
    blob = _encrypt_with_systemd_creds(key, with_key=with_key)
    cred_path = _credential_path(resolved, explicit=explicit_path)
    cred_path.parent.mkdir(parents=True, exist_ok=True)
    cred_path.write_bytes(blob)
    cred_path.chmod(0o600)
    meta_path = _meta_path(resolved)
    meta_path.write_text(json.dumps({"backend": backend}))
    meta_path.chmod(0o600)
    return cred_path, backend


def soul_key_init(
    *, owner: str | None = None, path: str | None = None, backend: str | None = None,
    print_recovery: bool = False,
) -> dict[str, Any]:
    """The only first-key generator: meant to be run once, by a human, in their
    own terminal (the CLI command `osiris soul-key init` wraps this unchanged); the
    worker and MCP server never call this, only `get_soul_fernet`/`get_soul_key`.

    Refuses if a key already exists at the target path, under either shape
    (a systemd-creds `.cred` file or a legacy plaintext file): an idempotent
    refusal, never a silent re-mint (`osiris soul-key rotate` is the real
    command for replacing a live key, never overwriting the primary file in
    place here).

    Refuses only the genuinely ambiguous case: running as root with no `owner=`
    given. Root has no natural owner to land the file as, since this box's live
    units are systemd --user, with no dedicated service account at all, so
    "the service user" is not even a fixed name to assume. Any non-root caller
    proceeds directly, no `owner=` required.

    `backend=`: following the rewritten key-custody design, None (every ordinary
    caller) auto-selects via `_resolve_backend`: `host+tpm2` when the operator
    has already joined `tss`, else `host-cred`, else `file` on a non-systemd
    host. An explicit `"file"` is the warned escape hatch back to a plaintext key
    (the shape this whole module used before that rewrite).

    `print_recovery=`: the old default-and-only banner is now an explicit
    opt-in; the new default recovery path is `soul_key_enroll_recovery`
    (FIDO2), run as a separate, deliberate second step.

    `path=` overrides `_key_file_path`'s own resolution ladder entirely: the
    escape hatch for a genuinely unusual layout. Every ordinary caller leaves
    it None and gets the same path an installed --user unit already uses,
    resolved without exporting anything."""
    resolved = _key_file_path(explicit=path)
    if resolved.exists() or _credential_path(resolved, explicit=path is not None).exists():
        return {"error": f"a key already exists at {resolved} — soul-key init never "
                         "overwrites an existing key in place; `osiris soul-key "
                         "rotate` is the door for replacing a live key"}
    current_user = getpass.getuser()
    if os.getuid() == 0 and owner is None:
        return {"error": "refusing — running as root with no --owner given. Root has no "
                         "natural owner for the key file: pass --owner <user>, naming "
                         "whichever user the worker/MCP service actually runs as (the "
                         f"{_SERVICE_USER!r} account for the system-unit deploy shape, "
                         "or the operator's own login user for a systemd --user deploy)"}
    target_owner = owner or current_user
    resolved.parent.mkdir(parents=True, exist_ok=True)
    key = _generate_key()
    effective_backend = _resolve_backend(backend)
    written_path, effective_backend = _write_key_for_backend(
        key, resolved=resolved, backend=effective_backend, explicit_path=path is not None)
    if print_recovery:
        _disclose_recovery_secret(key, str(written_path))
    chowned = False
    if owner is not None and current_user != owner:
        pw = pwd.getpwnam(owner)
        # `resolved` and `written_path` are the same file for backend="file";
        # a set() dedupes before chowning, never chowning one path twice (the
        # exact defect this test's own call-count assertion caught).
        for p in {resolved, written_path, _meta_path(resolved)}:
            if p.exists():
                os.chown(p, pw.pw_uid, pw.pw_gid)
        os.chown(resolved.parent, pw.pw_uid, pw.pw_gid)
        chowned = True
    tss_hint = None
    if effective_backend == "host-cred" and not _is_tss_member() and _systemd_creds_available():
        tss_hint = (f"a stronger TPM2-bound key is available once you join the "
                    f"'{_TSS_GROUP}' group: `usermod -aG {_TSS_GROUP} {target_owner}` "
                    "(run by root; log out and back in after, then `osiris soul-key "
                    "rotate` to upgrade the existing key onto host+tpm2)")
    return {
        "path": str(written_path), "backend": effective_backend, "owner": target_owner,
        "chowned": chowned, "recovery_disclosed": print_recovery,
        "systemd_note": _deploy_note(resolved), "tss_hint": tss_hint,
    }


def soul_key_status(*, path: str | None = None) -> dict[str, Any]:
    """Facts about the key `_key_file_path` resolves to, reflecting the rewritten
    key-custody design. Never the key bytes themselves: that would defeat the
    whole point of a status function existing separately from a debug print.
    Returns `present`, the resolved logical `path`, `backend` ("host-cred" /
    "host+tpm2" / "file" / "missing", read from `_meta_path` for a credential or
    inferred "file" for a legacy plaintext key), `created_age_seconds` (of
    whichever file actually carries the secret), `rotation_in_flight` (a
    `.legacy` sibling exists), and `recovery_paths_enrolled` (a list, "fido2"
    when `_recovery_path` exists) with `recovery_warning` set whenever that list
    has 1 or fewer entries (losing the sole enrolled path loses the data).
    Callers who also want the live soul_lines legacy-row census (a DB read this
    pool-free module deliberately never does) compose this with `soul_store.
    encrypt_existing_soul_lines(pool, dry_run=True)` themselves; that's
    `cmd_soul_key`'s own job, not this one's."""
    import time

    resolved = _key_file_path(explicit=path)
    cred_path = _credential_path(resolved, explicit=path is not None)
    backend = "missing"
    present = False
    carrier: Path | None = None
    if cred_path.exists():
        present = True
        carrier = cred_path
        meta_path = _meta_path(resolved)
        try:
            backend = json.loads(meta_path.read_text()).get("backend", "host-cred")
        except (OSError, ValueError):
            backend = "host-cred"
    elif resolved.exists():
        present = True
        carrier = resolved
        backend = "file"
    created_age_seconds: float | None = None
    if carrier is not None:
        created_age_seconds = max(0.0, time.time() - carrier.stat().st_ctime)
    recovery_paths: list[str] = []
    if _recovery_path(resolved).exists():
        recovery_paths.append("fido2")
    return {
        "present": present, "path": str(resolved), "backend": backend,
        "created_age_seconds": created_age_seconds,
        "rotation_in_flight": _legacy_key_file_path(resolved).exists(),
        "recovery_paths_enrolled": recovery_paths,
        "recovery_warning": (
            "0 recovery paths enrolled — losing this key's own live file/credential "
            "makes every stored transcript permanently unreadable; run `osiris "
            "soul-key enroll-recovery`" if not recovery_paths else
            "only 1 recovery path enrolled — a second FIDO2 key, or a printed copy "
            "via `soul-key rotate --print-recovery`, is recommended"
            if len(recovery_paths) <= 1 else None),
    }


def soul_key_rotate_begin(
    *, path: str | None = None, print_recovery: bool = False,
) -> dict[str, Any]:
    """Step 1 of 2: mints a fresh key, parks the current primary at
    `<path>.legacy` (`_legacy_key_file_path`, the same shape/backend the old
    primary already carried, plaintext or systemd-creds blob, read back
    unchanged), and writes the new key under the same backend the old one used
    (never silently upgrading or downgrading a rotation: `soul_key_init` is the
    place for choosing a backend). Idempotent and safe to re-run mid-rotation: if
    `.legacy` already exists (a rotation is already in flight), this refuses
    rather than minting a second new key and stranding the first rotation's own
    legacy key. `osiris soul-key rotate` re-run with a rotation already in
    flight is meant to re-drive the re-wrap pass (the CLI's own job, via
    `soul_store.rewrap_soul_lines_key`), never mint again.

    Refuses if no primary key exists yet at all: there is nothing to rotate
    away from; `soul-key init` is the place for a genuinely first key.

    Returns `new_key`/`old_key` (raw bytes, for the caller to build the two
    Fernet objects the re-wrap pass needs; this module stays pool-free, the
    DB-touching re-wrap itself lives in soul_store.py) plus `path`/`legacy_path`/
    `backend` and the same `systemd_note` shape `soul_key_init` returns (a
    rotation changes what's at the path, never the path itself, so no unit env
    change is ever needed for the daemons to find the new key, only a restart,
    named here)."""
    resolved = _key_file_path(explicit=path)
    status = soul_key_status(path=path)
    if not status["present"]:
        return {"error": f"no key exists at {resolved} yet — `osiris soul-key init` "
                         "first, there is nothing to rotate away from"}
    legacy_path = _legacy_key_file_path(resolved)
    if legacy_path.exists():
        return {"error": f"a rotation is already in flight ({legacy_path} exists) — "
                         "re-run `osiris soul-key rotate` to continue re-wrapping "
                         "rows onto the key already generated, or `--finish` once "
                         "the receipt reports zero rows remain under the old key"}
    old_backend = status["backend"]
    old_carrier = (
        resolved if old_backend == "file"
        else _credential_path(resolved, explicit=path is not None))
    if not old_carrier.exists():
        return {"error": f"the primary key at {resolved} is not backed by a file this "
                         "door can rotate (OSIRIS_SOUL_KEY set to a bare value with no "
                         "file behind it?) — rotate that key by hand"}
    # Decoded from `old_carrier_bytes` itself (systemd-creds blob or plaintext,
    # whichever `old_backend` actually is), not get_soul_key(), which ignores
    # this function's own `path=`/`explicit=` entirely and would silently
    # resolve a different key whenever an explicit path is in play (the exact
    # defect this rotation's own live testing surfaced while this was being built).
    # The one narrow case this does not cover, OSIRIS_SOUL_KEY set to an env
    # override that differs from whatever's on disk at `old_carrier`, is
    # already refused above by the `old_carrier.exists()` guard failing to name
    # a real file to rotate in the first place, on a genuinely bare-env-only setup.
    old_carrier_bytes = old_carrier.read_bytes()
    old_key = (
        _decrypt_with_systemd_creds(old_carrier_bytes) if old_backend != "file"
        else old_carrier_bytes)
    new_key = _generate_key()
    legacy_path.write_bytes(old_carrier_bytes)
    legacy_path.chmod(0o600)
    old_meta = _meta_path(resolved)
    legacy_meta_path = _meta_path(legacy_path)
    if old_meta.exists():
        legacy_meta_path.write_text(old_meta.read_text())
        legacy_meta_path.chmod(0o600)
    written_path, effective_backend = _write_key_for_backend(
        new_key, resolved=resolved, backend=old_backend, explicit_path=path is not None)
    if print_recovery:
        _disclose_recovery_secret(new_key, str(written_path))
    return {
        "path": str(written_path), "legacy_path": str(legacy_path),
        "backend": effective_backend, "new_key": new_key, "old_key": old_key,
        "recovery_disclosed": print_recovery,
        "systemd_note": (
            "restart osiris-mcp and osiris-worker now to pick up the new primary "
            "key. Any row either daemon writes BEFORE its own restart still "
            "encrypts under the OLD key in its own cached process memory — safe "
            "(the old key stays valid to decrypt until `--finish`), but re-run "
            "`osiris soul-key rotate` (idempotent) after both have restarted to "
            "sweep those rows onto the new key too, before `--finish`. If you "
            "enrolled FIDO2 recovery, re-run `osiris soul-key enroll-recovery` "
            "too — the old recovery blob still wraps the OLD key."),
    }


def soul_key_rotate_finish(*, path: str | None = None) -> dict[str, Any]:
    """Step 2 of 2: removes the `.legacy` key (and its own meta sidecar, if any)
    once the caller has already confirmed (via `soul_store.
    rewrap_soul_lines_key`'s own dry-run receipt) that zero rows remain
    encrypted under it. This function itself does not re-check the row count
    (pool-free by design, see module docstring); `cmd_soul_key` refuses to call
    this at all until that receipt is clean. Refuses if no rotation is in
    flight (nothing to finish)."""
    resolved = _key_file_path(explicit=path)
    legacy_path = _legacy_key_file_path(resolved)
    if not legacy_path.exists():
        return {"error": f"no rotation in flight — {legacy_path} does not exist, "
                         "nothing to finish"}
    legacy_path.unlink()
    legacy_meta_path = _meta_path(legacy_path)
    if legacy_meta_path.exists():
        legacy_meta_path.unlink()
    return {
        "path": str(resolved),
        "note": "old key removed. OSIRIS_SOUL_KEY_LEGACY (if you had exported it "
                "anywhere) is no longer needed — the old key is gone, and can never "
                "decrypt anything again.",
    }


# --- FIDO2 hmac-secret ("prf") recovery, part of the rewritten key-custody design ---
#
# This has not been physically verified by the agent that wrote it (no hands, no eyes
# on the device): built against python-fido2==2.2.1's own documented Fido2Client/
# PRF-extension API (confirmed live on this box: a YubiKey Security Key NFC fw 5.4.3
# enumerates via `CtapHidDevice.list_devices()`), but the actual touch+PIN interaction
# needs the operator's own hands to run once before this is trusted as the primary
# recovery path, flagged explicitly here, not silently assumed correct.

_DEFAULT_RP_ID = "localhost"  # the rp_id was corrected after an earlier mistake:
    # the original `_RP_ID = "osiris.local"` cannot interoperate with a browser
    # enrollment at all. WebAuthn requires rp_id to equal the page's own origin
    # domain, the console is served on localhost:8011, and plain http is a secure
    # context only for localhost. Now a settings-registry knob (`soul_key.rp_id`,
    # settings_registry.py, default "localhost", matches this same fallback) read by
    # the CLI (`cmd_soul_key` in cli.py) and threaded down as an explicit `rp_id=`
    # param; this module-level constant is only the fallback a caller with no pool in
    # hand (a test, a script) falls back to when it passes no `rp_id=` at all, never
    # read from here directly by `soul_key_enroll_recovery`/`soul_key_recover`
    # themselves, which always receive it as a parameter.
_PRF_SALT_LEN = 32


def _find_fido2_device() -> Any | None:
    """The first USB HID FIDO2 device found, or None. A box with more than one
    plugged in uses the first `CtapHidDevice.list_devices()` yields, a genuinely
    rare case (multi-key households); `--device` isn't exposed today, matching
    the current scope of CLI-only support."""
    from fido2.hid import CtapHidDevice

    devices = list(CtapHidDevice.list_devices())
    return devices[0] if devices else None


def _cli_user_interaction() -> Any:
    """The CLI's own `fido2.client.UserInteraction`: prints what's needed before
    blocking on the physical touch, and reads the PIN from the terminal (never
    logged, never returned in any receipt). Subclasses the real
    `fido2.client.UserInteraction` at call time (never at import time, since this
    module must import with no `fido2` installed at all, matching soul_crypto's
    own pool-free/dependency-light design everywhere else) so `Fido2Client`'s own
    isinstance/structural checks accept it."""
    from fido2.client import UserInteraction

    class _CliUserInteraction(UserInteraction):
        def prompt_up(self) -> None:
            print("Touch your Security Key now…", flush=True)

        def request_pin(self, permissions: Any, rd_id: str | None) -> str:
            return getpass.getpass("Security Key PIN: ")

        def request_uv(self, permissions: Any, rd_id: str | None) -> bool:
            return True

    return _CliUserInteraction()


def _fido2_client(device: Any, *, rp_id: str) -> Any:
    from fido2.client import DefaultClientDataCollector, Fido2Client

    return Fido2Client(
        device, DefaultClientDataCollector(f"https://{rp_id}"),
        user_interaction=_cli_user_interaction())


def _hkdf_wrap_key(prf_output: bytes) -> Fernet:
    """Derives a Fernet-compatible wrapping key from a PRF/hmac-secret output via
    HKDF-SHA256 (RFC 5869): the PRF output itself is 32 raw bytes, not the
    base64url-safe 32-byte key Fernet's own constructor requires, so this is
    never used directly. A fixed, public `info` label (no secret in it) is
    enough: the salt handed to the device is already random per enrollment
    (`_PRF_SALT_LEN`), so HKDF's own job here is only the encoding conversion,
    not adding entropy the PRF output didn't already have."""
    import base64

    derived = HKDF(
        algorithm=hashes.SHA256(), length=32, salt=None,
        info=b"osiris-soul-key-recovery-wrap",
    ).derive(prf_output)
    return Fernet(base64.urlsafe_b64encode(derived))


def soul_key_enroll_recovery(
    *, path: str | None = None, rp_id: str = _DEFAULT_RP_ID,
) -> dict[str, Any]:
    """Enrolls a new discoverable FIDO2 credential on the operator's Security Key
    with the PRF extension, derives a wrapping key from its PRF output at a fresh
    random salt (`_hkdf_wrap_key`), wraps the current primary key with it, and
    writes `_recovery_path(resolved)`: credential id, salt, the wrapped key,
    and a sha256 fingerprint of the plaintext key being wrapped (so `soul_key_
    recover` can confirm it unwrapped the right thing before ever touching
    anything, and `soul_key_status` can report backend consistency without
    decrypting). Refuses if the key itself doesn't exist yet (`soul-key init`
    first), and if a recovery blob already exists at this path (re-enrolling on
    purpose is `--rotate`'s own job below, sharing this function's core, never a
    silent overwrite here).

    `rp_id`: the caller's own resolved `soul_key.rp_id` setting value.
    `cmd_soul_key` (cli.py) reads it live and passes it in; this function's own
    default (`_DEFAULT_RP_ID`, "localhost") is only what a caller with no pool
    in hand (a test, a script) falls back to.

    Requires PIN and touch: blocks on physical interaction via
    `_CliUserInteraction`; never call this from a non-interactive context (the
    REST layer deliberately never exposes this action, see soul_key.py's own
    docstring)."""
    resolved = _key_file_path(explicit=path)
    status = soul_key_status(path=path)
    if not status["present"]:
        return {"error": f"no key exists at {resolved} yet — `osiris soul-key init` "
                         "first, there is nothing to enroll recovery for"}
    recovery_path = _recovery_path(resolved)
    if recovery_path.exists():
        return {"error": f"a recovery enrollment already exists at {recovery_path} — "
                         "this refuses to overwrite it silently; remove it by hand "
                         "first if you genuinely mean to re-enroll a different key"}
    device = _find_fido2_device()
    if device is None:
        return {"error": "no FIDO2 security key detected — plug it in and try again"}
    # read_key_bytes_at, not get_soul_key(): the latter ignores this function's
    # own `path=`/`explicit=` entirely and ignores an explicit path, the same
    # defect class caught in soul_key_rotate_begin while this function was built.
    raw_key = read_key_bytes_at(resolved)
    wrapped = _enroll_and_wrap(device, raw_key, rp_id=rp_id)
    if "error" in wrapped:
        return wrapped
    recovery_path.write_text(json.dumps(wrapped["blob"]))
    recovery_path.chmod(0o600)
    return {"path": str(recovery_path), "credential_id_fingerprint": wrapped["fingerprint"]}


def _enroll_and_wrap(device: Any, raw_key: bytes, *, rp_id: str) -> dict[str, Any]:
    """The actual CTAP2 enrollment flow, split out from `soul_key_enroll_recovery`
    so tests can inject a fake `device`/monkeypatch `_fido2_client` without also
    faking the filesystem side. Returns `{"blob": {...}, "fingerprint": ...}` on
    success or `{"error": ...}` on any `fido2`-layer failure (never lets a raw
    library exception escape past this module's own boundary). `rp_id` is stamped
    into the blob (`"rp_id"`) so a later `soul_key_recover` reads back the same
    value this credential was actually enrolled under, regardless of what the
    live `soul_key.rp_id` setting says by then: the setting can change, but an
    already-enrolled credential's own rp_id can't."""
    import base64
    import hashlib
    import os as _os

    from fido2.webauthn import (
        AuthenticatorSelectionCriteria,
        PublicKeyCredentialCreationOptions,
        PublicKeyCredentialParameters,
        PublicKeyCredentialRpEntity,
        PublicKeyCredentialType,
        PublicKeyCredentialUserEntity,
        ResidentKeyRequirement,
        UserVerificationRequirement,
    )

    client = _fido2_client(device, rp_id=rp_id)
    try:
        registration = client.make_credential(PublicKeyCredentialCreationOptions(
            rp=PublicKeyCredentialRpEntity(id=rp_id, name="Osiris soul-store key"),
            user=PublicKeyCredentialUserEntity(
                id=_os.urandom(16), name="soul-key", display_name="Osiris soul-store key"),
            challenge=_os.urandom(32),
            pub_key_cred_params=[PublicKeyCredentialParameters(
                type=PublicKeyCredentialType.PUBLIC_KEY, alg=-7)],
            authenticator_selection=AuthenticatorSelectionCriteria(
                resident_key=ResidentKeyRequirement.REQUIRED,
                user_verification=UserVerificationRequirement.REQUIRED),
            extensions={"credProps": True, "prf": {}},
        ))
    except Exception as exc:  # noqa: BLE001 - the fido2 boundary: name it, never a raw traceback
        raise SoulKeyRecoveryError(f"FIDO2 enrollment failed: {exc}") from exc
    credential_id = registration.raw_id
    salt = _os.urandom(_PRF_SALT_LEN)
    prf_output = _prf_eval(client, credential_id, salt, rp_id=rp_id)
    if prf_output is None:
        return {"error": "this Security Key did not return a PRF/hmac-secret output — "
                         "it may not support the extension (needs FIDO2, not U2F-only)"}
    wrap_fernet = _hkdf_wrap_key(prf_output)
    wrapped_key = wrap_fernet.encrypt(raw_key)
    fingerprint = hashlib.sha256(raw_key).hexdigest()[:16]
    blob = {
        "credential_id": base64.urlsafe_b64encode(credential_id).decode(),
        "salt": base64.urlsafe_b64encode(salt).decode(),
        "wrapped_key": base64.urlsafe_b64encode(wrapped_key).decode(),
        "key_fingerprint": fingerprint,
        "rp_id": rp_id,
    }
    return {"blob": blob, "fingerprint": fingerprint}


def _prf_eval(client: Any, credential_id: bytes, salt: bytes, *, rp_id: str) -> bytes | None:
    """One `get_assertion` call against `credential_id` with the PRF extension
    evaluated at `salt`, returning the raw PRF output bytes or None if the
    authenticator didn't return one. Shared by enrollment (derive the wrap key
    right after minting the credential) and recovery (re-derive the same wrap
    key from the same credential and salt on a later call: PRF is deterministic,
    same credential, same salt, same output, every time, by the extension's own
    contract)."""
    from fido2.webauthn import (
        PublicKeyCredentialDescriptor,
        PublicKeyCredentialRequestOptions,
        PublicKeyCredentialType,
    )

    assertion = client.get_assertion(PublicKeyCredentialRequestOptions(
        challenge=os.urandom(32), rp_id=rp_id,
        allow_credentials=[PublicKeyCredentialDescriptor(
            type=PublicKeyCredentialType.PUBLIC_KEY, id=credential_id)],
        extensions={"prf": {"eval": {"first": salt}}},
    )).get_response(0)
    results = getattr(assertion.client_extension_results, "prf", None)
    if not results:
        return None
    first = results.get("results", {}).get("first") if isinstance(results, dict) else None
    return bytes(first) if first else None


def soul_key_recover(
    *, path: str | None = None, backend: str | None = None, rp_id: str = _DEFAULT_RP_ID,
) -> dict[str, Any]:
    """Reverses `soul_key_enroll_recovery`: reads `_recovery_path(resolved)`,
    re-derives the same wrapping key via `_prf_eval` on the same credential and
    salt (PIN and touch required again, the physical key is the whole point),
    unwraps the recovered Fernet key, confirms it against the blob's own
    recorded `key_fingerprint` (refuses rather than seals a corrupted/tampered
    recovery), and re-seals it under the host credential on this machine via
    `soul_key_init`'s own `_write_key_for_backend` (`backend=` defaults to the
    same auto-selection `soul_key_init` uses). This is the exact "recover on a
    new machine" story: the recovery blob is portable (safe on the NAS/vault),
    the host credential it re-seals under is not.

    `rp_id`: the caller's own resolved `soul_key.rp_id` setting, used only as a
    fallback. The blob's own recorded `"rp_id"` (stamped in by `_enroll_and_wrap`
    at enrollment time) is preferred whenever present: a credential must be
    addressed by the rp_id it was actually enrolled under, which may differ
    from whatever the live setting says by recovery time if the operator ever
    changes it; this function recovers what was actually minted, never what's
    merely configured today.

    Refuses if a key already exists at the target path (this is a recovery
    function, not a rotation: `soul-key rotate` is the command once you already
    have a live key and just want a new one)."""
    resolved = _key_file_path(explicit=path)
    if resolved.exists() or _credential_path(resolved, explicit=path is not None).exists():
        return {"error": f"a key already exists at {resolved} — soul-key recover is "
                         "for restoring onto a box with NO live key; `osiris soul-key "
                         "rotate` is the door once you already have one"}
    recovery_path = _recovery_path(resolved)
    if not recovery_path.exists():
        return {"error": f"no recovery enrollment found at {recovery_path}"}
    import base64
    import hashlib

    blob = json.loads(recovery_path.read_text())
    effective_rp_id = blob.get("rp_id") or rp_id
    device = _find_fido2_device()
    if device is None:
        return {"error": "no FIDO2 security key detected — plug in the SAME key you "
                         "enrolled recovery with, and try again"}
    client = _fido2_client(device, rp_id=effective_rp_id)
    credential_id = base64.urlsafe_b64decode(blob["credential_id"])
    salt = base64.urlsafe_b64decode(blob["salt"])
    try:
        prf_output = _prf_eval(client, credential_id, salt, rp_id=effective_rp_id)
    except Exception as exc:  # noqa: BLE001 - the fido2 boundary: name it, never a raw traceback
        raise SoulKeyRecoveryError(f"FIDO2 recovery failed: {exc}") from exc
    if prf_output is None:
        return {"error": "this Security Key did not return a PRF/hmac-secret output "
                         "for the enrolled credential — wrong key plugged in?"}
    wrap_fernet = _hkdf_wrap_key(prf_output)
    wrapped_key = base64.urlsafe_b64decode(blob["wrapped_key"])
    try:
        raw_key = wrap_fernet.decrypt(wrapped_key)
    except InvalidToken:
        return {"error": "recovery blob failed to decrypt — wrong Security Key, or the "
                         "blob is corrupted"}
    if hashlib.sha256(raw_key).hexdigest()[:16] != blob["key_fingerprint"]:
        return {"error": "recovered key's own fingerprint does not match the recovery "
                         "blob's recorded one — refusing to seal a possibly-tampered "
                         "key; this is a real break, not a retryable glitch"}
    resolved.parent.mkdir(parents=True, exist_ok=True)
    effective_backend = _resolve_backend(backend)
    written_path, effective_backend = _write_key_for_backend(
        raw_key, resolved=resolved, backend=effective_backend, explicit_path=path is not None)
    return {
        "path": str(written_path), "backend": effective_backend,
        "systemd_note": _deploy_note(resolved),
    }
