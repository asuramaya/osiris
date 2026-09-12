"""RENDERS the backup lane's shipped `.timer` files with any operator-configured schedule
override substituted in (Wave 21, operator's word 2026-09-11, thread f04cce36 piece 3 —
"deploy's own timer-install step regenerates the units from" `backup_settings`).

`deploy/<unit>.timer` stays the shipped DEFAULT, untouched — this writes RENDERED copies
into `--out`, which `install_prune_timers.sh` installs from instead of `deploy/` directly.
A unit with no configured override in `backup_settings.timer_schedules` renders as a
byte-identical copy of the shipped file (install_prune_timers.sh's own compare-then-copy
stays a true no-op for it). Matching `.service` files copy across unchanged — only a
`.timer`'s own `OnCalendar=` line ever carries a schedule.

FAILS OPEN, NEVER BLOCKS DEPLOY: a settings-read hiccup (DB down, table missing on an
old checkout not yet migrated) degrades to the shipped defaults for every unit, printed
to stderr rather than raised — the basic timer install this script feeds must keep
working even when the config layer piece 3 just built can't be reached."""
from __future__ import annotations

import argparse
import asyncio
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


async def _configured_schedules() -> dict[str, str]:
    from src.config.settings import get_settings
    from src.db.pool import create_pool
    from src.orchestrator.backup_settings import get_backup_settings

    settings = get_settings()
    pool = await create_pool(settings.database_url, min_size=1, max_size=1,
                             application_name="osiris-script:render-backup-timers")
    try:
        row = await get_backup_settings(pool)
        result: dict[str, str] = row.get("timer_schedules") or {}
        return result
    finally:
        await pool.close()


def render(deploy_dir: Path, out_dir: Path, overrides: dict[str, str]) -> int:
    """Pure — no DB, no network. Returns how many units carry a configured override."""
    from src.orchestrator.backup_settings import BACKUP_TIMER_UNITS

    out_dir.mkdir(parents=True, exist_ok=True)
    rendered = 0
    for unit in BACKUP_TIMER_UNITS:
        src = deploy_dir / unit
        if not src.is_file():
            continue
        text = src.read_text()
        cal = overrides.get(unit)
        if cal:
            text = "\n".join(
                f"OnCalendar={cal}" if line.startswith("OnCalendar=") else line
                for line in text.splitlines()
            ) + "\n"
            rendered += 1
        (out_dir / unit).write_text(text)
        service_name = unit.removesuffix(".timer") + ".service"
        service = deploy_dir / service_name
        if service.is_file():
            shutil.copy2(service, out_dir / service_name)
    return rendered


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deploy-dir", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)

    try:
        overrides = asyncio.run(_configured_schedules())
    except Exception as exc:  # noqa: BLE001 — see module docstring: fails open
        print(f"render_backup_timers: settings unavailable ({exc}) — using shipped "
              "defaults for every unit", file=sys.stderr)
        overrides = {}

    rendered = render(args.deploy_dir, args.out, overrides)
    print(f"render_backup_timers: {rendered} unit(s) carry a configured override, "
          f"written to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
