"""A file whose modification time says "mail was sent after this moment". The status bar caches
its rendered line for a few seconds per session so most refreshes cost a stat and a read; a
message sent since the cache was written must show at once, so `send_message` touches this file
and the cache is stale if the file is newer. The hook reads the same path with the standard
library only, so the default below is mirrored in scripts/osiris_hook.py (a test pins them)."""
from __future__ import annotations

import os
from pathlib import Path

_ENV = "OSIRIS_MAIL_BUMP_FILE"
_DEFAULT = "~/.osiris/mail-bump"


def bump_path() -> Path:
    return Path(os.environ.get(_ENV) or _DEFAULT).expanduser()


def touch() -> None:
    """Best effort: a failure here only means the bar may show an old count until its short
    cache expires, never that a message is not sent."""
    try:
        path = bump_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
        os.utime(path, None)
    except OSError:
        pass
