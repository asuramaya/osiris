"""THE GUARD: no test can resolve a credential or state path under the real home. The
suite-wide fixture in conftest.py redirects them; this proves the redirect is complete, by
resolving every default location the readers use and requiring each to land in scratch. A new
reader that resolves a fresh path under `~/.config` or `~/.local/state` and is not redirected
fails here, instead of silently reading (or writing) a developer's real key or password."""
from __future__ import annotations

import os
import pwd
from pathlib import Path

import pytest
from src.ingest import soul_crypto, systemd_credential
from src.orchestrator import (
    graph_stream,
    offload_runner,
    recovery_copies,
    restic_credential,
    soul_encrypt_progress,
    soul_key,
    soul_recompress,
)


def _real_home() -> Path:
    return Path(pwd.getpwuid(os.getuid()).pw_dir).resolve()


def _resolved_paths() -> dict[str, Path]:
    key = soul_crypto._key_file_path()
    pw = restic_credential._password_file_path()
    return {
        "credential store": systemd_credential.user_credstore_encrypted_dir(),
        "soul key (logical)": key,
        "soul key (credential)": soul_crypto._credential_path(key),
        "soul key recovery file": soul_crypto._recovery_path(key),
        "backup password (logical)": pw,
        "backup password (credential)": restic_credential._credential_path(pw),
        "offload receipts": offload_runner._receipts_path(),
        "recovery copies receipts": recovery_copies._receipts_path(),
        "encryption progress": soul_encrypt_progress._progress_path(),
        "recompress progress": soul_recompress._progress_path(),
        "restore drill receipts": soul_key._restore_drill_receipts_path(),
        "recovery verify receipt": soul_key._verify_receipt_path(),
        "graph snapshot": graph_stream.snapshot_file_path(),
    }


_NAMES = [
    "credential store", "soul key (logical)", "soul key (credential)",
    "soul key recovery file", "backup password (logical)", "backup password (credential)",
    "offload receipts", "recovery copies receipts", "encryption progress", "recompress progress",
    "restore drill receipts", "recovery verify receipt", "graph snapshot",
]


@pytest.mark.parametrize("name", _NAMES)
def test_default_locations_never_resolve_under_the_real_home(name: str) -> None:
    resolved = _resolved_paths()[name].expanduser().resolve()
    home = _real_home()
    assert not resolved.is_relative_to(home / ".config"), f"{name}: {resolved}"
    assert not resolved.is_relative_to(home / ".local" / "state"), f"{name}: {resolved}"


def test_the_real_credentials_on_this_box_are_invisible_to_a_test() -> None:
    """Whatever this machine's own deploy minted, the readers see an empty store."""
    assert soul_crypto.soul_key_status()["present"] is False
    assert restic_credential.restic_key_status()["present"] is False
