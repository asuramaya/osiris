"""Boot heal, REBOOT SURVIVAL, the units half: it has to survive and recover reboot by
itself. Run by osiris-boot-heal.service, a oneshot ordered Before= every daemon unit at
every boot: repairs the box's own installed systemd units BEFORE anything depends on
them, so a stub or a drifted unit (today's live specimen: tests/test_cli.py's own
deploy test once wrote 30-byte stubs through the real installer with Path.home
unpatched, and osiris-mcp/osiris-pulse came up dead on the next reboot) never survives
past this unit's own single boot-time pass.

NEVER A SECOND INSTALLER (the explicit boundary set for this script: call the existing
installer, don't fork it): reuses the SAME two sanctioned, already-idempotent install
paths `osiris deploy` itself calls: `src.cli._real_install_user_units` for the five
deploy/user/*.service daemon units, `scripts/install_prune_timers.sh` for the seven
oneshot timer pairs under deploy/. Never a third diff-and-copy mechanism invented here.
Both are already compare-then-copy (a byte-identical target is a silent no-op) and
already refuse to write outside a real `~/.config/systemd/user` unless explicitly
redirected for a test.

BEST-EFFORT, NEVER A BOOT BLOCKER: this unit carries no `Requires=` from any daemon (only
`Before=` ordering). A crash here delays nothing, and every exception is caught and named
rather than raised, so a bug in this SCRIPT can never be the reason the fleet fails to boot
(the exact inversion of the problem it exists to fix). Exit 1 only on a genuine unhandled
failure: `osiris boot-status` reads this unit's own `systemctl --user is-failed` to surface
that, never by re-deriving the same check.

    .venv/bin/python scripts/osiris_boot_heal.py

journald captures this script's stdout automatically (a `Type=oneshot` unit's own contract):
no separate log file, `journalctl --user -u osiris-boot-heal` is the record.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.cli import _find_repo_root, _real_install_user_units, _run_install_script  # noqa: E402


async def _heal(repo_root: Path) -> list[str]:
    notes = await _real_install_user_units(repo_root)
    notes.append(_run_install_script("scripts/install_prune_timers.sh", repo_root))
    return notes


def main() -> int:
    repo_root = _find_repo_root(Path(__file__).resolve().parent) or Path.home() / "code" / "osiris"
    try:
        notes = asyncio.run(_heal(repo_root))
    except Exception as exc:  # noqa: BLE001, this script must never crash the boot it guards
        print(f"osiris-boot-heal: FAILED — {exc!r}", file=sys.stderr)
        return 1
    for note in notes:
        print(f"osiris-boot-heal: {note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
