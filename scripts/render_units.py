"""render_units: generalizes render_backup_timers.py's own pattern past the 5
backup-lane timers to every unit-backed setting: MemoryMax for the four persistent
dev-box daemons, osiris-pulse's own --watch interval, the console's --host/--port, and
osiris-pg-autotune.timer's own schedule (osiris-preflight.timer was already covered,
since it's one of BACKUP_TIMER_UNITS).

SAME FAILS-OPEN DISCIPLINE render_backup_timers.py established: a settings-read hiccup
degrades every unit to its shipped default, printed to stderr, never raised. A key with
no configured override (its stored value equal to its own spec default) renders
byte-identical to the shipped file. Every substitution function below is written to be a
true no-op when handed the value already baked into the file it touches, so "unset"
falls out of matching defaults rather than a sentinel each caller has to special-case.

ONE SUBSTITUTION FUNCTION PER SHAPE, not a generic templating engine: same "each file
passes through render, never re-templated wholesale" discipline the backup lane already
used. `_sub_oncalendar` (the timer lane, reused verbatim), `_sub_memory_max` (in-place
replace when a MemoryMax= line exists, insert one before [Install] when adding a
genuinely new cap, drop the line when the value is empty), and two CLI-arg regex
substitutions for `deploy/user/osiris-{pulse,console}.service`'s own ExecStart= line.

THE REBOOT-SURVIVAL GUARD: a rendered daemon `.service` is only installed when it
still has a real `[Unit]` `Description=` and a non-empty `ExecStart=`
(`_looks_like_a_real_unit`). A stub or a substitution gone wrong must never install a
unit that starts nothing (or the wrong thing). A unit that fails the check falls back
to the shipped file untouched, logged to stderr, same fails-open discipline as a
settings-read hiccup."""
from __future__ import annotations

import argparse
import asyncio
import re
import shutil
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _sub_oncalendar(text: str, value: Any) -> str:
    if not value:
        return text
    return "\n".join(
        f"OnCalendar={value}" if line.startswith("OnCalendar=") else line
        for line in text.splitlines()
    ) + "\n"


def _sub_memory_max(text: str, value: Any) -> str:
    value = (value or "").strip()
    lines = text.splitlines()
    out: list[str] = []
    replaced = False
    for line in lines:
        if line.startswith("MemoryMax="):
            replaced = True
            if value:
                out.append(f"MemoryMax={value}")
            # else: drop the line entirely, an explicit "no cap".
        else:
            out.append(line)
    if value and not replaced:
        idx = next((i for i, ln in enumerate(out) if ln.strip() == "[Install]"), len(out))
        out.insert(idx, f"MemoryMax={value}")
    return "\n".join(out) + "\n"


def _sub_watch_arg(text: str, value: Any) -> str:
    if value is None:
        return text
    return re.sub(r"--watch \d+", f"--watch {int(value)}", text)


def _sub_console_host(text: str, value: Any) -> str:
    if value is None:
        return text
    return re.sub(r"--host \S+", f"--host {value}", text)


def _sub_console_port(text: str, value: Any) -> str:
    if value is None:
        return text
    return re.sub(r"--port \d+", f"--port {int(value)}", text)


def _sub_console_graceful_timeout(text: str, value: Any) -> str:
    """THE CONSOLE GRACEFUL SHUTDOWN: same shape as `_sub_console_host`/
    `_sub_console_port` above. A plain regex replace against the flag's own literal on
    `ExecStart=`, never an insert-if-missing branch, because the shipped unit already
    carries `--timeout-graceful-shutdown 10` (this substitution's own registered
    default)."""
    if value is None:
        return text
    return re.sub(r"--timeout-graceful-shutdown \d+",
                  f"--timeout-graceful-shutdown {int(value)}", text)


def _sub_env_var(name: str) -> Callable[[str, Any], str]:
    """`Environment=<name>=<value>` substitution, generic: the first caller is
    `ingest.transcripts_root` -> `OSIRIS_TRANSCRIPTS`, but the shape is the same for
    any single-line env override. Replace in place when the line already exists (the
    common case, since every unit this touches ships a real default line), otherwise
    insert one right before `[Install]`, mirroring `_sub_memory_max`'s own "insert a
    genuinely new line" branch. An empty/None `value` means "no configured override":
    the shipped line is left untouched either way."""
    prefix = f"Environment={name}="

    def _sub(text: str, value: Any) -> str:
        value = value.strip() if isinstance(value, str) else value
        if not value:
            return text
        lines = text.splitlines()
        out: list[str] = []
        replaced = False
        for line in lines:
            if line.startswith(prefix):
                replaced = True
                out.append(f"{prefix}{value}")
            else:
                out.append(line)
        if not replaced:
            idx = next((i for i, ln in enumerate(out) if ln.strip() == "[Install]"), len(out))
            out.insert(idx, f"{prefix}{value}")
        return "\n".join(out) + "\n"

    return _sub


def _looks_like_a_real_unit(text: str) -> bool:
    """The reboot-survival guard: a rendered unit is never installed unless it still
    has a real `[Unit]` `Description=` line and a non-empty `ExecStart=`. These are the
    two lines every substitution function above could, in principle, mangle (a bad
    regex match, an unexpected value shape) into a unit that starts the wrong process
    or none at all. Cheap line-scan, not a full systemd-unit parser: this only needs to
    catch "the substitution broke the file," not validate every field."""
    has_description = any(
        line.startswith("Description=") and line.removeprefix("Description=").strip()
        for line in text.splitlines()
    )
    has_execstart = any(
        line.startswith("ExecStart=") and line.removeprefix("ExecStart=").strip()
        for line in text.splitlines()
    )
    return has_description and has_execstart


# timer unit (relative to deploy/) -> the settings key controlling its OnCalendar= line.
# The matching .service copies across byte-for-byte, same as render_backup_timers.py.
_TIMER_SCHEDULE_KEYS: dict[str, str] = {}


def _timer_schedule_keys() -> dict[str, str]:
    if not _TIMER_SCHEDULE_KEYS:
        from src.config.settings_registry import BACKUP_TIMER_UNITS

        for unit in BACKUP_TIMER_UNITS:
            _TIMER_SCHEDULE_KEYS[unit] = f"backup.timer_schedule.{unit}"
        _TIMER_SCHEDULE_KEYS["osiris-pg-autotune.timer"] = "daemon.osiris_pg_autotune.schedule"
    return _TIMER_SCHEDULE_KEYS


# deploy/user/<name>.service -> the (key, substitution) pairs applied to it, in order.
_DAEMON_SERVICE_SUBS: dict[str, list[tuple[str, Callable[[str, Any], str]]]] = {
    "osiris-mcp.service": [
        ("daemon.osiris_mcp.memory_max", _sub_memory_max),
        ("ingest.transcripts_root", _sub_env_var("OSIRIS_TRANSCRIPTS")),
    ],
    "osiris-worker.service": [("daemon.osiris_worker.memory_max", _sub_memory_max)],
    "osiris-pulse.service": [
        ("daemon.osiris_pulse.memory_max", _sub_memory_max),
        ("daemon.osiris_pulse.watch_interval_secs", _sub_watch_arg),
    ],
    "osiris-console.service": [
        ("daemon.osiris_console.memory_max", _sub_memory_max),
        ("daemon.osiris_console.host", _sub_console_host),
        ("daemon.osiris_console.port", _sub_console_port),
        ("daemon.osiris_console.graceful_shutdown_secs", _sub_console_graceful_timeout),
    ],
}


async def _configured_values() -> dict[str, Any]:
    from src.config.settings import get_settings
    from src.db.pool import create_pool
    from src.orchestrator.settings_service import list_settings

    settings = get_settings()
    pool = await create_pool(settings.database_url, min_size=1, max_size=1,
                             application_name="osiris-script:render-units")
    try:
        items = await list_settings(pool)
        return {item["key"]: item["value"] for item in items}
    finally:
        await pool.close()


def render(deploy_dir: Path, out_dir: Path, values: dict[str, Any]) -> int:
    """Pure: no DB, no network. `values` is every registered setting's own current value
    (its spec default when unset, `settings_service.list_settings`'s own shape). Returns
    how many rendered files differ from their own shipped source."""
    out_dir.mkdir(parents=True, exist_ok=True)
    rendered = 0

    for unit, key in _timer_schedule_keys().items():
        src = deploy_dir / unit
        if not src.is_file():
            continue
        text = src.read_text()
        new_text = _sub_oncalendar(text, values.get(key))
        if new_text != text:
            rendered += 1
        (out_dir / unit).write_text(new_text)
        service_name = unit.removesuffix(".timer") + ".service"
        service = deploy_dir / service_name
        if service.is_file():
            shutil.copy2(service, out_dir / service_name)

    user_dir = deploy_dir / "user"
    if user_dir.is_dir():
        out_user_dir = out_dir / "user"
        out_user_dir.mkdir(parents=True, exist_ok=True)
        for name, subs in _DAEMON_SERVICE_SUBS.items():
            src = user_dir / name
            if not src.is_file():
                continue
            text = src.read_text()
            new_text = text
            for key, fn in subs:
                new_text = fn(new_text, values.get(key))
            if not _looks_like_a_real_unit(new_text):
                print(f"render_units: rendered {name} is missing a Description= or a "
                      "real ExecStart= — installing the shipped file untouched instead",
                      file=sys.stderr)
                new_text = text
            if new_text != text:
                rendered += 1
            (out_user_dir / name).write_text(new_text)

    return rendered


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deploy-dir", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)

    try:
        values = asyncio.run(_configured_values())
    except Exception as exc:  # noqa: BLE001, see module docstring: fails open
        print(f"render_units: settings unavailable ({exc}) — using shipped defaults "
              "for every unit", file=sys.stderr)
        values = {}

    rendered = render(args.deploy_dir, args.out, values)
    print(f"render_units: {rendered} unit(s) carry a configured override, written to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
