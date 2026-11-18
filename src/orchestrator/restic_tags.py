"""Restic snapshot tags the backup policy must treat as untouchable.

`history-import` marks the snapshots made by converting the old plaintext NAS copy into backdated
snapshots (src/orchestrator/plaintext_copy.py): the only copies of the August and September
dumps and base backup. Any thinning policy (`restic forget`) MUST pass `forget_protect_args()`:
`--keep-tag` keeps a snapshot with that tag whatever the other keep rules say, so the thinning
can never forget this history. Prune of unreferenced data is not affected."""
from __future__ import annotations

HISTORY_IMPORT_TAG = "history-import"
PROTECTED_TAGS = (HISTORY_IMPORT_TAG,)


def forget_protect_args() -> list[str]:
    """The `restic forget` arguments that keep every protected snapshot."""
    out: list[str] = []
    for tag in PROTECTED_TAGS:
        out += ["--keep-tag", tag]
    return out
