"""Soul-store encryption key management (Thoth mail 9134, operator ruling on thread
773d633a; AMENDED by Thoth DM 9245, wave 17): the LIVE soul store (soul_lines/
soul_lines_cold) is encrypted at rest by osiris itself, one tier above host-disk trust.

`/etc/osiris/soul.key` (overridable via `OSIRIS_SOUL_KEY_FILE`) is the production file
path, named via the SAME `EnvironmentFile=` both `deploy/osiris-worker.service` and
`deploy/osiris-mcp.service` already load for `DATABASE_URL` etc — one config file, no
new deploy step.

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
soul_store first. `soul_key_init` below is the ONE door that mints a key — run once, by
a human, in their own terminal, so the mandatory offline recovery secret
(`_generate_and_disclose`) lands in front of a person, never inside a daemon's journal.

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
    never a silent auto-generate. The message IS the fix: the exact `soul-key-init`
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


def _key_file_path() -> Path:
    return Path(os.environ.get("OSIRIS_SOUL_KEY_FILE", _DEFAULT_KEY_FILE)).expanduser()


def _init_command_hint(path: Path) -> str:
    return (
        f"no soul-store encryption key found at {path} (and OSIRIS_SOUL_KEY is unset) — "
        "run `osiris soul-key-init` ONCE, in your own terminal, as the "
        f"{_SERVICE_USER!r} service user (or as root with `--owner {_SERVICE_USER}` to "
        "chown the key into place), before starting osiris-worker/osiris-mcp")


def _generate_and_disclose(where: str) -> bytes:
    """Mint a fresh key and print the MANDATORY offline recovery secret before this
    call's own caller persists it anywhere — a crash between this print and the
    persist step still leaves the operator holding the only copy that matters, never
    the reverse. The ONE place a key is ever minted (`soul_key_init`, below) routes
    through this, so "print the recovery secret" can never be silently skipped."""
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
    GENERATES — raises `SoulKeyMissing` (naming the exact `soul-key-init` command) when
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


def soul_key_init(*, owner: str | None = None) -> dict[str, Any]:
    """THE ONLY GENERATOR (Thoth DM 9245) — meant to be run once, by a human, in their
    own terminal (the CLI door `osiris soul-key-init` wraps this unchanged); the worker
    and MCP server never call this, only `get_soul_fernet`/`get_soul_key`.

    REFUSES IF A KEY ALREADY EXISTS at the target path — idempotent refusal, never a
    silent re-mint (rotation is `OSIRIS_SOUL_KEY_LEGACY` plus a migration pass, never
    overwriting the primary file in place).

    REFUSES UNLESS EITHER: (a) the CURRENT process is already running as the service
    user (`_SERVICE_USER`, matching deploy/osiris-worker.service's own `User=`), so the
    file lands with the right owner by construction with no extra step, or (b) an
    explicit `owner=` is given, in which case the key file AND its parent directory are
    chown'd to that user after writing — the real shape for an operator running this as
    root (or their own login user) before the service user exists to run anything as
    itself. Neither condition met: refuses, naming both options.

    Prints the mandatory offline recovery secret exactly once (`_generate_and_disclose`),
    and names the exact systemd env line the worker/MCP units need if the key file isn't
    already at the default path they assume."""
    path = _key_file_path()
    if path.exists():
        return {"error": f"{path} already exists — soul-key-init never overwrites an "
                         "existing key in place; rotate via OSIRIS_SOUL_KEY_LEGACY plus "
                         "a migration pass instead of regenerating here"}
    current_user = getpass.getuser()
    if current_user != _SERVICE_USER and owner is None:
        return {"error": f"refusing — running as {current_user!r}, not the service user "
                         f"{_SERVICE_USER!r} (deploy/osiris-worker.service's own User=). "
                         f"Either run this as {_SERVICE_USER!r}, or pass "
                         f"owner={_SERVICE_USER!r} (`--owner {_SERVICE_USER}` on the CLI) "
                         "to chown the key file and its directory to that user after "
                         "writing — the shape when this runs as root before the service "
                         "user can run anything itself"}
    target_owner = owner or _SERVICE_USER
    path.parent.mkdir(parents=True, exist_ok=True)
    key = _generate_and_disclose(str(path))
    path.write_bytes(key)
    path.chmod(0o600)
    chowned = False
    if owner is not None and current_user != owner:
        pw = pwd.getpwnam(owner)
        os.chown(path, pw.pw_uid, pw.pw_gid)
        os.chown(path.parent, pw.pw_uid, pw.pw_gid)
        chowned = True
    default_path = str(path) == _DEFAULT_KEY_FILE
    return {
        "path": str(path), "owner": target_owner, "chowned": chowned,
        "systemd_note": (
            "this is already the default path deploy/osiris-worker.service and "
            "deploy/osiris-mcp.service assume — no env change needed" if default_path else
            f"add `OSIRIS_SOUL_KEY_FILE={path}` to /etc/osiris/osiris.env — both units' "
            "own EnvironmentFile= — since this key file is not at the default path"),
    }
