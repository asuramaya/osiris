"""Run the restore test against the NAS now, for the console's "Run the restore test now" button
(`POST /at-home/restore-test/run`, streamed like `/at-home/nas/run`).

It is the same bounded test the weekly timer runs (`run_bounded_drill`), but it ignores the six
hour wait after a failed attempt, which is the point of a button. Steps, one line each:

  [1/4] home network: the route to the NAS leaves through the home interface (never Tailscale)
  [2/4] nothing else is using the repository (the offload and the scheduled test share a lock)
  [3/4] the test itself, with a line as each of its steps starts and ends, then its result
  [4/4] the result is saved where the panel and `osiris at-home` read it

A test that could not reach the NAS, or found the repository busy, saves nothing: it says nothing
about the backup, so the last real result stays. Never raises; the caller streams the lines."""
from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

TOTAL = 4


@dataclass(frozen=True)
class Deps:
    presence: Callable[[dict[str, Any]], dict[str, Any]]
    # (repository url, on_step) -> a failure sentence, or None when the test passed
    drill: Callable[[str, Callable[[str, float | None], None]], str | None]
    hold_lock: Callable[[], Any]
    record: Callable[..., None]


def real_deps() -> Deps:
    from scripts.osiris_offbox_restore_drill import run_bounded_drill

    from src.orchestrator.network_presence import lan_presence
    from src.orchestrator.offload_runner import hold_tick_lock
    from src.orchestrator.soul_key import record_restore_drill

    return Deps(
        presence=lan_presence,
        drill=lambda url, on_step: run_bounded_drill(url, on_step=on_step),
        hold_lock=hold_tick_lock, record=record_restore_drill)


async def run(target: dict[str, Any], deps: Deps, *, say: Callable[[str], None]) -> int:
    from src.orchestrator import restic_locks
    from src.orchestrator.network_presence import is_unreachable_error

    url = str(target.get("path_or_url") or "")

    def stop(n: int, why: str, what_next: str) -> int:
        say(f"[{n}/{TOTAL}] STOPPED: {why}")
        say(f"        next: {what_next}")
        return 1

    seen = await asyncio.to_thread(deps.presence, target)
    if not seen.get("present"):
        return stop(1, f"not on the home network ({seen.get('reason') or 'route unknown'}).",
                    "run this again when you are home on the LAN, not over Tailscale.")
    if not seen.get("dev"):
        return stop(1, "could not confirm which interface the route to the NAS leaves through; "
                       "not going without that proof.",
                    "check `ip route get` for the NAS address, then run this again.")
    say(f"[1/{TOTAL}] home network: the route to the NAS leaves through {seen['dev']}.")

    with deps.hold_lock() as locked:
        if not locked:
            return stop(2, "an offload or another restore test is running right now.",
                        "wait a few minutes and run this again.")
        say(f"[2/{TOTAL}] nothing else is using the repository.")
        loop = asyncio.get_running_loop()

        def on_step(name: str, seconds: float | None) -> None:
            # called from the test's own thread: hand the line to the loop
            text = (f"        {name}: started" if seconds is None
                    else f"        {name}: done in {seconds:.0f} s")
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(say, text)

        say(f"[3/{TOTAL}] running the restore test (about 5 minutes; it reads a fixed 2 GiB and "
            "restores a few files).")
        try:
            failure = await asyncio.to_thread(deps.drill, url, on_step)
        except Exception as exc:  # noqa: BLE001 - a sentence for the page, never a traceback
            failure = f"{type(exc).__name__}: {exc}"
        notes = restic_locks.pop_notes(url)
        if is_unreachable_error(failure):
            return stop(3, "the NAS cannot be reached from this network; nothing was saved.",
                        "run this again when you are home on the LAN.")
        if restic_locks.is_busy(failure):
            return stop(3, f"the repository is in use ({failure}); nothing was saved.",
                        "wait a few minutes and run this again.")
        note = "; ".join(notes) if notes else None
        deps.record(url, ok=failure is None, error=failure, note=note)
        if note:
            say(f"        {note}")
        if failure:
            return stop(4, f"the restore test failed: {failure}", "the result is saved; see "
                        "`osiris at-home` or run `osiris soul-key restore-drill --repo-url ...`.")
        say(f"[4/{TOTAL}] the restore test passed; the result is saved.")
    return 0


async def command(*, pool: Any, say: Callable[[str], None], deps: Deps | None = None) -> int:
    """Find the NAS target, then `run` it."""
    from src.orchestrator.backup_settings import get_backup_settings
    from src.orchestrator.nas_first_offload import pick_target

    settings = await get_backup_settings(pool)
    target, why = pick_target(settings.get("offload_targets") or [], None)
    if target is None:
        say(f"[1/{TOTAL}] STOPPED: {why}.")
        return 1
    return await run(target, deps or real_deps(), say=say)
