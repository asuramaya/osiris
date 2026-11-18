"""Has the NAS got this file? The local prune ladder asks before it deletes.

The ladder (`scripts/osiris_prune_ladder.py`) thins dumps, base backups and WAL in the local vault.
On 2026-11-15 it removed 3,341 WAL segments and many dumps that the NAS had never received, because
the box had been away from the home network and the offload runs only there. Now a vault file may be
removed only when a successful offload began after the file was last written, because then that
backup saw the file as it is now.

The rule uses `last_backup_start`, the moment the newest successful offload began (recorded in the
offload records), and each file's own modification time: a file older than that start was whole when
the backup read it. It needs no network, so it works at 14:00 while the box is away, which is when
it matters. (Asking restic for path and size would need the LAN at that moment.)

Where no offload target has ever been tracked the ladder behaves as before. Where one is tracked
but no start is recorded (an older record, a record that cannot be read), nothing is confirmed and
everything waits. How much is waiting is written to a small state file that `osiris backup-status`
and `osiris at-home` read; past a ceiling they say so. Nothing here ever deletes anything."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

_RECEIPTS_ENV = "OSIRIS_OFFLOAD_RECEIPTS_FILE"
_DEFAULT_RECEIPTS = "~/.local/state/osiris/offload_receipts.json"
_HELD_ENV = "OSIRIS_PRUNE_HELD_FILE"
_DEFAULT_HELD = "~/.local/state/osiris/prune_held.json"
CEILING_ENV = "OSIRIS_PRUNE_HOLD_CEILING_BYTES"
DEFAULT_CEILING_BYTES = 150 * 10**9


@dataclass(frozen=True)
class Horizon:
    """What the NAS is known to hold: files last written before `start` are confirmed."""
    tracked: bool
    start: datetime | None
    reason: str

    def confirms(self, mtime: float) -> bool:
        if not self.tracked:
            return True
        return self.start is not None and mtime < self.start.timestamp()


OPEN = Horizon(False, None, "no offload target is tracked, so nothing waits on a copy")


def receipts_path() -> Path:
    return Path(os.environ.get(_RECEIPTS_ENV) or _DEFAULT_RECEIPTS).expanduser()


def load_horizon(path: Path | None = None) -> Horizon:
    """The newest recorded backup start over every tracked target. No record file at all: open
    (the old behaviour). A file that cannot be read, or targets with no recorded start: nothing is
    confirmed."""
    path = path or receipts_path()
    try:
        raw = path.read_text()
    except FileNotFoundError:
        return OPEN
    except OSError:
        return Horizon(True, None, "the offload records could not be read")
    try:
        body = json.loads(raw)
    except ValueError:
        return Horizon(True, None, "the offload records could not be read")
    if not isinstance(body, dict) or not body:
        return OPEN if body == {} else Horizon(True, None, "the offload records are not usable")
    starts: list[datetime] = []
    for entry in body.values():
        value = entry.get("last_backup_start") if isinstance(entry, dict) else None
        try:
            parsed = datetime.fromisoformat(str(value)) if value else None
        except ValueError:
            parsed = None
        if parsed is not None:
            starts.append(parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC))
    if not starts:
        return Horizon(True, None, "the last offload did not record when its backup began")
    return Horizon(True, max(starts), "confirmed up to the newest successful offload's start")


@dataclass
class Held:
    """What the ladder wanted to remove but left, because the NAS has not confirmed it."""
    files: int = 0
    bytes: int = 0
    oldest: float | None = None
    kinds: dict[str, int] = field(default_factory=dict)

    def add(self, kind: str, size: int, mtime: float) -> None:
        self.files += 1
        self.bytes += max(0, int(size))
        self.oldest = mtime if self.oldest is None else min(self.oldest, mtime)
        self.kinds[kind] = self.kinds.get(kind, 0) + 1


def ceiling_bytes() -> int:
    try:
        return max(1, int(os.environ.get(CEILING_ENV, DEFAULT_CEILING_BYTES)))
    except ValueError:
        return DEFAULT_CEILING_BYTES


def held_path() -> Path:
    return Path(os.environ.get(_HELD_ENV) or _DEFAULT_HELD).expanduser()


def write_held(held: Held, horizon: Horizon, *, now: datetime | None = None) -> None:
    """Record how much is waiting for the NAS (atomic). Only when offloads are tracked; with no
    target there is nothing to report and no file is touched."""
    if not horizon.tracked:
        return
    path = held_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    state = {
        "at": (now or datetime.now(UTC)).isoformat(), "files": held.files, "bytes": held.bytes,
        "oldest_file_at": (datetime.fromtimestamp(held.oldest, tz=UTC).isoformat()
                           if held.oldest is not None else None),
        "kinds": held.kinds, "ceiling_bytes": ceiling_bytes(),
        "over_ceiling": held.bytes > ceiling_bytes(),
        "confirmed_through": horizon.start.isoformat() if horizon.start else None,
        "reason": horizon.reason}
    try:
        tmp.write_text(json.dumps(state))
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def read_held() -> dict[str, Any] | None:
    """The last recorded hold, or None when there is none (or it cannot be read)."""
    try:
        body = json.loads(held_path().read_text())
    except (OSError, ValueError):
        return None
    return body if isinstance(body, dict) and "bytes" in body else None


def describe_held(state: dict[str, Any] | None) -> str | None:
    """One plain sentence for the status views, or None when nothing is waiting."""
    if not state or not state.get("files"):
        return None
    gb = state["bytes"] / 1e9
    since = str(state.get("oldest_file_at") or "")[:10]
    over = (f" That is over the {state['ceiling_bytes'] / 1e9:.0f} GB limit: nothing is deleted, "
            "but connect to the home network so the NAS can take it."
            if state.get("over_ceiling") else "")
    return (f"{state['files']} backup file(s) ({gb:.1f} GB) are being kept past their normal "
            f"retention because the NAS has not confirmed them" +
            (f" (oldest from {since})" if since else "") + f".{over}")


# ---- fewer dumps while away ----------------------------------------------------------------------

AWAY_AFTER = timedelta(hours=24)


def last_offload_end(path: Path | None = None) -> datetime | None:
    """When the newest successful offload finished, or None when that cannot be told (no record, an
    unreadable one, or none recorded): the caller then behaves as if the NAS were up to date."""
    try:
        body = json.loads((path or receipts_path()).read_text())
    except (OSError, ValueError):
        return None
    ends: list[datetime] = []
    for entry in body.values() if isinstance(body, dict) else []:
        value = (entry.get("last_backup_end") or entry.get("last_successful_offload")) \
            if isinstance(entry, dict) else None
        try:
            parsed = datetime.fromisoformat(str(value)) if value else None
        except ValueError:
            parsed = None
        if parsed is not None:
            ends.append(parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC))
    return max(ends) if ends else None


def take_dump(last_end: datetime | None, newest_dump: datetime | None, now: datetime,
              ) -> tuple[bool, str]:
    """Whether the backup run should take a database dump now. While the NAS is more than 24 hours
    behind, at most one dump a day (an uncompressed dump is about 7.7 GB and every one is held
    locally until the NAS confirms it). WAL and base backups are not affected by this. Anything
    that cannot be told (no offload record, no dump yet) takes the dump: never toward skipping."""
    if last_end is None:
        return True, "no offload record, taking the dump"
    if now - last_end <= AWAY_AFTER:
        return True, "the NAS is up to date"
    if newest_dump is None or now - newest_dump >= AWAY_AFTER:
        return True, "away from the NAS, and no dump in the last 24 hours"
    return False, ("the NAS is more than 24 hours behind and a dump was taken in the last 24 "
                   "hours: skipping this one")
