"""`osiris at-home`: what is waiting only on you being there, in the order to do it.

Some jobs cannot be finished by an agent: they need the operator physically present (on the home
network, with a security key in hand) or their word. This lists each one with a live status, read
from the real sources, and the exact command to run for it, the cheapest and safest first:

  1. the envelope drill (a touch per key; changes nothing but a "last opened" time)
  2. a recovery code for the envelope (typed at a hidden prompt)
  3. a second hardware key for the envelope
  4. the NAS first offload (needs the home LAN; the first step is a read-only check)
  5. the old plaintext NAS copy's removal (needs your say-so, after step 4)
  6. the stopped database rollback container (needs your say-so)
  7. scratch folders left by an agent (need your say-so)
  8. the optional Claude rebuild probe (only if you want it)

Read-only: it opens nothing, uploads nothing and changes nothing. Each source is a function passed
in, so the tests run it against fakes; a source that cannot be read says so and never hides the
other items.

Statuses: `done`, `waiting` (something for the operator to do, with why), `blocked` (waits for an
earlier item first), `optional` (can wait for ever; never counted, never told to the desk).

Items 5 to 7 only show what is there: no code here deletes anything."""
from __future__ import annotations

import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DONE, WAITING, BLOCKED, OPTIONAL = "done", "waiting", "blocked", "optional"
ROLLBACK_CONTAINER = "osiris-pg-pre-repack"
ROLLBACK_TITLE = "Remove the stopped database rollback container"
PROBE_COMMAND = ("cd ~/code/zzprobe-rebuild && "
                 "python3 ~/.claude/jobs/aa0277bc/tmp/probe_rebuild.py A")
# the open obligation that holds the old plaintext copy's removal, and the S4 job before it
PLAINTEXT_COPY_THREAD = "thread:5dc7ea2b"
_STATE_ENV = "OSIRIS_AT_HOME_STATE_FILE"
_DEFAULT_STATE = "~/.local/state/osiris/at_home.json"
RETELL_DAYS = 7


@dataclass(frozen=True)
class Sources:
    """Everything the checklist reads, as functions the caller supplies."""
    envelope: Callable[[], dict[str, Any]]                 # envelope_drill.envelope_section
    nas_target: Callable[[], dict[str, Any] | None]        # the enabled NAS offload target
    offload_receipt: Callable[[str], dict[str, Any]]       # per-target offload receipt
    drill_receipt: Callable[[str], dict[str, Any]]         # per-repository restore-test receipt
    presence: Callable[[dict[str, Any]], dict[str, Any]]   # network_presence.lan_presence
    plaintext_thread_open: Callable[[], bool | None]       # None when it cannot be read
    stale_days: int = 7
    # the word-only items; the defaults say "nothing there" so older callers see them done
    rollback_container: Callable[[], dict[str, Any] | None] = lambda: None
    scratch: Callable[[], list[dict[str, Any]]] = lambda: []
    probe_thread_open: Callable[[], bool | None] = lambda: None


def _item(key: str, order: int, title: str, status: str, why: str,
          command: str | None = None, then: str | None = None) -> dict[str, Any]:
    return {"key": key, "order": order, "title": title, "status": status, "why": why,
            "command": command, "then": then}


def envelope_items(section: dict[str, Any]) -> list[dict[str, Any]]:
    """The three envelope jobs from backup-status's envelope block."""
    if section.get("error"):
        return [_item("envelope", 1, "Key envelope", WAITING,
                      f"its state could not be read ({section['error']}).",
                      "osiris backup-status")]
    if not section.get("configured"):
        return [_item("envelope", 1, "Key envelope", WAITING,
                      "no envelope is set up yet: the vault key is not sealed to any recipient.",
                      "osiris soul-key envelope list")]
    recipients = section.get("recipients") or []
    hardware = [r for r in recipients if r.get("kind") == "fido2-prf"]
    codes = [r for r in recipients if r.get("kind") == "recovery-code"]
    unproven = [r for r in recipients if not r.get("last_proved_at")
                or str(r.get("verdict", "")).startswith(("stale", "never"))]
    out = []
    out.append(_item(
        "envelope_drill", 1, "Prove each envelope key opens it",
        DONE if not unproven else WAITING,
        "every recipient has proved itself recently." if not unproven else
        f"{len(unproven)} of {len(recipients)} recipient(s) not proved lately "
        f"({', '.join(str(r.get('label') or r.get('id')) for r in unproven)}).",
        "osiris envelope-drill"))
    out.append(_item(
        "envelope_code", 2, "A recovery code for the envelope", DONE if codes else WAITING,
        "a recovery code is enrolled." if codes else
        "no recovery code yet: if the hardware key is lost, nothing else opens the vault key.",
        "osiris soul-key envelope add-code"))
    out.append(_item(
        "envelope_key", 3, "A second hardware key for the envelope",
        DONE if len(hardware) >= 2 else WAITING,
        f"{len(hardware)} hardware keys are enrolled." if len(hardware) >= 2 else
        f"only {len(hardware)} hardware key is enrolled: a second one means losing one key "
        "does not lock you out.",
        "osiris soul-key envelope add-key"))
    return out


def nas_item(src: Sources, now: datetime) -> dict[str, Any]:
    target = src.nas_target()
    if target is None:
        return _item("nas_offload", 4, "The NAS first offload", WAITING,
                     "no enabled NAS target is configured.", "osiris backup-settings")
    name = str(target.get("name", "nas"))
    from src.orchestrator.offload_staleness import annotate_target

    receipt = src.offload_receipt(name)
    verdict = annotate_target({**target, **receipt}, now=now, stale_days=src.stale_days)
    drill = src.drill_receipt(str(target.get("path_or_url") or ""))
    first = _when(receipt.get("first_successful_offload") or receipt.get("last_successful_offload"))
    passed = _when(drill.get("last_passed_at"))
    current = str(verdict["offload_verdict"]).startswith("current")
    restored = bool(first and passed and passed >= first)
    if current and restored:
        return _item("nas_offload", 4, "The NAS first offload", DONE,
                     f"{verdict['offload_verdict']}; the restore test has passed.")
    problems = []
    if not current:
        problems.append(str(verdict["offload_verdict"]))
    if not restored:
        problems.append("the restore test has not passed against it")
    seen = src.presence(target)
    here = (f"You are on the home network now ({seen.get('dev')}): good time to run it."
            if seen.get("present") and seen.get("dev") else
            f"You are not on the home network ({seen.get('reason') or 'route unknown'}): "
            "run it when you are home on the LAN.")
    return _item("nas_offload", 4, "The NAS first offload", WAITING,
                 "; ".join(problems) + ". " + here,
                 f"osiris nas-first-offload --name {name}",
                 then="osiris nas-first-offload --check  (read-only: only the route and the "
                      "repository, run first if you want a safe look)")


def plaintext_item(src: Sources, nas: dict[str, Any]) -> dict[str, Any]:
    still_open = src.plaintext_thread_open()
    if still_open is False:
        return _item("plaintext_copy", 5, "Remove the old plaintext NAS copy", DONE,
                     "the removal is recorded as done.")
    if nas["status"] != DONE:
        return _item("plaintext_copy", 5, "Remove the old plaintext NAS copy", BLOCKED,
                     "waits for the NAS offload and its restore test (item 4) to pass first.")
    return _item("plaintext_copy", 5, "Remove the old plaintext NAS copy", WAITING,
                 "the NAS now holds a restorable backup, so the old plaintext copy (about "
                 "162 GB) can go. This needs your word; nothing deletes it by itself.",
                 "reply on the operator desk: delete the old plaintext NAS copy")


def _size(n: object) -> str:
    try:
        size = float(n)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "unknown size"
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return "unknown size"


def rollback_item(src: Sources) -> dict[str, Any]:
    box = src.rollback_container()
    title = ROLLBACK_TITLE
    if box is None:
        return _item("rollback_container", 6, title, DONE, f"{ROLLBACK_CONTAINER} is gone.")
    return _item("rollback_container", 6, title, WAITING,
                 f"{ROLLBACK_CONTAINER} ({box.get('image')}, {box.get('status')}) has been kept "
                 f"since the repack swap; it holds {_size(box.get('size'))} of its own, and it "
                 "points at the live database's data volume, so remove only the container, never "
                 "the volume. This needs your word; nothing deletes it by itself.",
                 f"docker rm {ROLLBACK_CONTAINER}")


def scratch_item(src: Sources) -> dict[str, Any]:
    found = [p for p in src.scratch() if p.get("exists")]
    title = "Remove the scratch folders an agent left"
    if not found:
        return _item("scratch_dirs", 7, title, DONE, "both scratch folders are gone.")
    names = "; ".join(f"{p['path']} ({_size(p.get('size'))})" for p in found)
    return _item("scratch_dirs", 7, title, WAITING,
                 f"still there: {names}. This needs your word; nothing deletes them by itself.",
                 "rm -r " + " ".join(str(p["path"]) for p in found))


def probe_item(src: Sources) -> dict[str, Any]:
    title = "The Claude rebuild probe (optional)"
    still_open = src.probe_thread_open()
    if still_open is False:
        return _item("rebuild_probe", 8, title, DONE, "its obligation is closed.")
    return _item("rebuild_probe", 8, title, OPTIONAL,
                 "you chose to do this later. Until it runs, Claude sessions are carried over by "
                 "a brief plus recall instead of a rebuilt transcript. It writes only into a "
                 "throwaway folder.", PROBE_COMMAND)


def _when(value: object) -> datetime | None:
    from src.orchestrator.offload_staleness import _parse

    return _parse(value)


def checklist(src: Sources, *, now: datetime | None = None) -> dict[str, Any]:
    """The items, ordered cheapest and safest first, and the counts."""
    now = now or datetime.now(UTC)
    items: list[dict[str, Any]] = []
    try:
        items += envelope_items(src.envelope())
    except Exception as exc:  # noqa: BLE001 - one source never hides the others
        items.append(_item("envelope", 1, "Key envelope", WAITING,
                           f"its state could not be read ({type(exc).__name__}).",
                           "osiris backup-status"))
    try:
        nas = nas_item(src, now)
    except Exception as exc:  # noqa: BLE001
        nas = _item("nas_offload", 4, "The NAS first offload", WAITING,
                    f"its state could not be read ({type(exc).__name__}).",
                    "osiris backup-status")
    items.append(nas)
    try:
        items.append(plaintext_item(src, nas))
    except Exception as exc:  # noqa: BLE001
        items.append(_item("plaintext_copy", 5, "Remove the old plaintext NAS copy", WAITING,
                           f"its state could not be read ({type(exc).__name__}).", None))
    for fn, key, order, title in (
            (rollback_item, "rollback_container", 6, ROLLBACK_TITLE),
            (scratch_item, "scratch_dirs", 7, "Remove the scratch folders an agent left"),
            (probe_item, "rebuild_probe", 8, "The Claude rebuild probe (optional)")):
        try:
            items.append(fn(src))
        except Exception as exc:  # noqa: BLE001
            items.append(_item(key, order, title, WAITING if key != "rebuild_probe" else OPTIONAL,
                               f"its state could not be read ({type(exc).__name__}).", None))
    items.sort(key=lambda i: i["order"])
    pending = [i for i in items if i["status"] in (WAITING, BLOCKED)]
    done = sum(1 for i in items if i["status"] == DONE)
    return {"items": items, "pending": len(pending), "done": done,
            "next": next((i for i in items if i["status"] == WAITING), None)}


def render(result: dict[str, Any]) -> list[str]:
    """Plain lines for the terminal."""
    if not result["pending"]:
        if any(i["status"] == OPTIONAL for i in result["items"]):
            return ["Nothing is waiting on you.", *_optional_lines(result)]
        return ["Nothing is waiting on you. Everything on the at-home list is done."]
    lines = [f"{result['pending']} thing(s) waiting on you, cheapest and safest first:"]
    for i, item in enumerate(result["items"], 1):
        mark = {"done": "done   ", "waiting": "waiting", "blocked": "blocked",
                "optional": "optional"}[item["status"]]
        lines.append(f"{i}. [{mark}] {item['title']}: {item['why']}")
        if item["status"] in (WAITING, OPTIONAL) and item["command"]:
            lines.append(f"       run: {item['command']}")
            if item.get("then"):
                lines.append(f"       or first: {item['then']}")
    return lines


def _optional_lines(result: dict[str, Any]) -> list[str]:
    out = []
    for item in result["items"]:
        if item["status"] == OPTIONAL:
            out.append(f"[optional] {item['title']}: {item['why']}")
            out.append(f"       run: {item['command']}")
    return out


# ---- the desk line, told once per state ------------------------------------------------------


def _state_path() -> Path:
    return Path(os.environ.get(_STATE_ENV) or _DEFAULT_STATE).expanduser()


def _read_state() -> dict[str, Any]:
    try:
        body = json.loads(_state_path().read_text())
    except (OSError, ValueError):
        return {}
    return body if isinstance(body, dict) else {}


def _write_state(state: dict[str, Any]) -> None:
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_text(json.dumps(state))
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


async def surface(pool: Any, result: dict[str, Any], *, now: datetime | None = None) -> str | None:
    """One operator-desk line when anything is waiting, told once per state (the set of waiting
    items) and again after a week of the same state. Nothing pending clears the memory, so the
    next time something waits it is told afresh. Never fails the caller."""
    import logging

    from src.orchestrator.mailbox import send_message

    now = now or datetime.now(UTC)
    try:
        state = _read_state()
        waiting = [i for i in result["items"] if i["status"] == WAITING]
        if not waiting:
            if state.get("told"):
                _write_state({})
            return None
        key = ",".join(sorted(i["key"] for i in waiting))
        told_at = _when(state.get("at"))
        if state.get("told") == key and told_at and (now - told_at).days < RETELL_DAYS:
            return None
        first = waiting[0]
        line = (f"at home: {len(waiting)} thing(s) wait on you; first, {first['title']}: "
                f"{first['why']} Run: {first['command'] or 'see `osiris at-home`'}. "
                "The whole list: `osiris at-home`.")
        await send_message(pool, from_agent="cron:at_home", from_project="osiris",
                           to_project="operator", body=line, desk_kind="hands",
                           dedup_window_secs=3600)
        _write_state({"told": key, "at": now.isoformat()})
        return line
    except Exception:  # noqa: BLE001 - telling the desk must never fail the run
        logging.getLogger(__name__).warning("could not tell the desk what is waiting",
                                            exc_info=True)
        return None


# ---- the real sources -------------------------------------------------------------------------


async def thread_status(pool: Any, id_prefix: str) -> str | None:
    """The status of the Thread whose id starts with `id_prefix` ("open", "resolved", ...), or None
    when no such thread exists. Read through `winning_props`, the way every thread reader does."""
    value = await pool.fetchval(
        "SELECT (SELECT value #>> '{}' FROM winning_props(ARRAY[o.id]::uuid[]) "
        "        WHERE name='status') FROM objects o "
        "WHERE o.type='Thread' AND o.id::text LIKE $1 || '%' LIMIT 1", id_prefix)
    return None if value is None else str(value)


async def _captured(fn: Callable[[], Any]) -> Callable[[], Any]:
    """Run a blocking read off the loop now; the returned function gives its value, or raises what
    it raised, when the checklist asks for it (so one bad source shows as unreadable)."""
    import asyncio

    try:
        value = await asyncio.to_thread(fn)
    except Exception as exc:  # noqa: BLE001
        error = exc

        def failed() -> Any:
            raise error
        return failed
    return lambda: value


async def real_sources(pool: Any, *, now: datetime | None = None) -> Sources:
    """The checklist's sources, read from the live system once: the envelope block, the enabled NAS
    target and its receipts, and whether the plaintext copy's job is still open."""
    import functools

    from src.ingest import soul_envelope
    from src.orchestrator.backup_settings import get_backup_settings
    from src.orchestrator.envelope_drill import envelope_section
    from src.orchestrator.network_presence import lan_presence, sftp_host
    from src.orchestrator.offload_runner import offload_receipts
    from src.orchestrator.settings_service import settings_with_overlay
    from src.orchestrator.soul_key import restore_drill_receipts

    when = now or datetime.now(UTC)
    settings = await get_backup_settings(pool)
    nas = next((t for t in settings.get("offload_targets") or []
                if isinstance(t, dict) and t.get("enabled") and t.get("kind") == "restic"
                and sftp_host(str(t.get("path_or_url") or ""))), None)
    try:
        stale_days = int((await settings_with_overlay(pool)).osiris_offload_stale_days) or 7
    except Exception:  # noqa: BLE001 - the limit is display only
        stale_days = 7
    status = await thread_status(pool, PLAINTEXT_COPY_THREAD.split(":", 1)[1])
    import asyncio

    from src.orchestrator import at_home_readers as readers

    box, scratch = await asyncio.gather(
        _captured(readers.read_rollback_container), _captured(readers.read_scratch))
    receipts = offload_receipts()
    drills = restore_drill_receipts()
    section = envelope_section(
        functools.partial(soul_envelope.envelope_recipients),
        functools.partial(soul_envelope.envelope_check), now=when)
    return Sources(
        envelope=lambda: section, nas_target=lambda: nas,
        offload_receipt=lambda name: receipts.get(name) or {},
        drill_receipt=lambda url: drills.get(url) or {},
        presence=lan_presence,
        plaintext_thread_open=lambda: (None if status is None else status == "open"),
        stale_days=stale_days,
        rollback_container=lambda: box(), scratch=lambda: scratch(),
        probe_thread_open=lambda: None)
