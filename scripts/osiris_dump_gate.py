#!/usr/bin/env python3
"""Exit 0: take a database dump now. Exit 1: skip this one (the NAS is more than 24 hours behind
and a dump was taken in the last 24 hours). Any trouble reading anything exits 0: never toward
skipping a dump. Used by osiris_backup.sh before pg_dump."""
from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    try:
        from src.orchestrator import offload_confirm

        vault = Path(args[0]) if args else Path.home() / "osiris-vault"
        mtimes = [p.stat().st_mtime for p in vault.glob("osiris-*.dump") if p.is_file()]
        newest = datetime.fromtimestamp(max(mtimes), tz=UTC) if mtimes else None
        take, why = offload_confirm.take_dump(
            offload_confirm.last_offload_end(), newest, datetime.now(UTC))
    except Exception as exc:  # noqa: BLE001 - never skip a dump because the check broke
        print(f"osiris_dump_gate: could not decide ({type(exc).__name__}); taking the dump")
        return 0
    print(f"osiris_dump_gate: {why}")
    return 0 if take else 1


if __name__ == "__main__":
    sys.exit(main())
