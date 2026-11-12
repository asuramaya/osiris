"""The envelope drill: for each recipient the vault key is sealed to, show when that recipient
last proved it opens the envelope, and run the proof itself when the operator is present.

An envelope is only as good as its least-tested recipient: a hardware key nobody has touched in
a year may no longer open anything, and nothing says so until the day it is needed.

TWO MODES, because opening any recipient needs a person (a touch, a PIN, a typed code):

* UNATTENDED (timer, backup-status): reads the envelope's own listing and its structural check
  (`soul_envelope.envelope_check`), and reports per recipient when it last opened (from the
  envelope's `last_opened_at`), the days since, and a stale verdict. A recipient that is due says
  it needs the operator's key. Nothing is opened, nothing is touched.
* INTERACTIVE (`osiris soul-key envelope drill`): for each recipient, opens the envelope with that
  recipient alone (`envelope_verify`) and, through a callback, decrypts one real sealed artifact
  with the key it yielded. The result is kept in this module's own small record.

KEY HYGIENE. The key never reaches this module except as the argument of the callback the
envelope calls while it holds the key; the callback returns a bool and keeps nothing. No key, no
callback exception text and no device message that could carry one is written to the record, a
log line, a command line or a result.

The envelope itself (`src/ingest/soul_envelope.py`) is built separately; the three functions
this drill calls are passed in, so the drill runs against fakes in tests."""
from __future__ import annotations

import fcntl
import json
import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.ingest.soul_envelope import EnvelopeUnreadable, atomic_write_private

_RECORD_ENV = "OSIRIS_ENVELOPE_DRILL_RECORD_FILE"
_DEFAULT_RECORD = "~/.local/state/osiris/envelope_drill.json"
DEFAULT_STALE_DAYS = 30
REMIND_DAYS = 7  # how often a still-wanted notice is told again

PROVED = "proved"
NEEDS_OPERATOR_KEY = "needs_operator_key"
FAILED = "failed"

Recipients = Callable[[], list[dict[str, Any]]]
Check = Callable[[], dict[str, Any]]
Verify = Callable[..., dict[str, Any]]  # (recipient_id, *, on_key=...) -> {ok, ...}


@dataclass(frozen=True)
class Sample:
    """One real sealed artifact and how to test a key against it."""
    token: bytes
    decrypts: Callable[[bytes, bytes], bool]  # (key, token) -> did the key open the token


def record_path() -> Path:
    return Path(os.environ.get(_RECORD_ENV) or _DEFAULT_RECORD).expanduser()


def read_record() -> dict[str, dict[str, Any]]:
    """The drill's own per-recipient record; empty when there is none or it is unreadable."""
    try:
        body = json.loads(record_path().read_text())
    except (OSError, ValueError):
        return {}
    if not isinstance(body, dict):
        return {}
    return {str(k): v for k, v in body.items() if isinstance(v, dict)}


def _write_record(record: dict[str, dict[str, Any]]) -> None:
    """Atomic, private (file 0600, directory 0700). Callers that read first then write must hold
    `_locked_record` for the whole read-modify-write."""
    atomic_write_private(record_path(), json.dumps(record, indent=2))


@contextmanager
def _locked_record() -> Iterator[None]:
    """An exclusive lock around a read-modify-write of the record, so the daily timer and an
    interactive drill cannot erase each other's entries. Reads need no lock: a write replaces
    the file whole."""
    path = record_path()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(path.with_name(f"{path.name}.lock"), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)  # closing the descriptor releases the lock


def _parse(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        when = datetime.fromisoformat(value)
    except ValueError:
        return None
    return when if when.tzinfo else when.replace(tzinfo=UTC)


def _later(a: datetime | None, b: datetime | None) -> datetime | None:
    return b if a is None else a if b is None else max(a, b)


def _ago(when: datetime | None, now: datetime) -> int | None:
    return (now - when).days if when else None


def status_view(
    recipients: list[dict[str, Any]], record: dict[str, dict[str, Any]], *, now: datetime,
    stale_days: int = DEFAULT_STALE_DAYS,
) -> list[dict[str, Any]]:
    """What backup-status shows per recipient, with two different things kept apart:

    * PROVED: the drill opened the envelope with that recipient alone AND its key decrypted a real
      sealed line (`last_proved_at`, from this module's record).
    * OPENED: the envelope's own `last_opened_at`, set by a fingerprint check (`envelope verify`)
      that never decrypts a line.

    A recipient is only as fresh as the later of the two (an opened-but-never-proved one is not
    blank), but the verdict names each, so "proved" never reads as more than it was. A recipient
    with neither reads as never proved, not as fine."""
    out: list[dict[str, Any]] = []
    for r in recipients:
        mine = record.get(str(r.get("id"))) or {}
        proved = _parse(mine.get("last_proved_at"))
        opened = _parse(r.get("last_opened_at"))
        latest = _later(proved, opened)
        days = _ago(latest, now)
        opened_days, proved_days = _ago(opened, now), _ago(proved, now)
        detail = (
            (f"opened (fingerprint only) {opened_days} days ago" if opened
             else "never opened") + "; " +
            (f"proved on a real sealed line {proved_days} days ago" if proved
             else "never proved on a real sealed line"))
        if latest is None:
            verdict = "never proved: needs the operator's key"
        elif days is not None and days >= stale_days:
            verdict = f"stale: last activity {days} days ago (limit {stale_days}): {detail}; " \
                      "needs the operator's key"
        else:
            verdict = f"current: {detail}"
        if mine.get("status") == FAILED:
            verdict += f"; the last drill attempt failed: {mine.get('reason')}"
        out.append({
            "id": r.get("id"), "label": r.get("label"), "kind": r.get("kind"),
            "last_proved_at": proved.isoformat() if proved else None,
            "days_since_proved": proved_days,
            "last_opened_at": opened.isoformat() if opened else None,
            "days_since_opened": opened_days,
            "last_activity_at": latest.isoformat() if latest else None,
            "days_since_activity": days,
            "verdict": verdict, "last_drill_status": mine.get("status"),
            "last_drill_at": mine.get("last_attempt_at")})
    return out


def _section_error(exc: Exception) -> dict[str, Any]:
    sentence = f"the envelope could not be read: {type(exc).__name__}"
    return {"error": sentence, "problems": [sentence]}


def envelope_section(
    recipients: Recipients, check: Check, *, now: datetime,
    stale_days: int = DEFAULT_STALE_DAYS,
) -> dict[str, Any]:
    """The unattended view for backup-status: the envelope's structural check plus one line per
    recipient. Never opens anything. A broken envelope reads as a problem, never as an error
    that blanks the panel."""
    problems: list[str] = []
    unreadable = False
    try:
        listed = recipients()
    except EnvelopeUnreadable as exc:
        listed, unreadable = [], True
        problems.append(str(exc))
    except Exception as exc:  # noqa: BLE001 - one section never blanks the panel
        return _section_error(exc)
    try:
        checked = check()
    except Exception as exc:  # noqa: BLE001 - one section never blanks the panel
        return _section_error(exc)
    if not listed and not unreadable and checked.get("absent"):
        # the ONLY quiet case: there is no recovery file at all, a first run
        return {"configured": False, "ok": False, "problems": list(checked.get("problems") or []),
                "recipients": [], "warnings": [],
                "note": "no envelope is set up yet: the key is not sealed to any recipient"}
    if not listed:
        # a file is there but yields no recipients (unreadable, corrupt, or emptied): a problem
        problems += [p for p in (checked.get("problems") or []) if p not in problems]
        if not problems:
            problems.append("the recovery file lists no recipients")
        return {"configured": True, "ok": False, "unreadable": unreadable,
                "problems": problems, "by_recipient": {}, "hardware": checked.get("hardware"),
                "recipients": [], "warnings": [], "stale": []}
    view = status_view(listed, read_record(), now=now, stale_days=stale_days)
    # recipients never proved are a warning in their own right: the envelope's own list when it
    # gives one, else read off the view
    unproven = list(checked.get("unproven") or [
        v["id"] for v in view if v["last_activity_at"] is None])
    warnings = [f"{rid}: never proved to open the envelope" for rid in unproven]
    return {"configured": True, "ok": bool(checked.get("ok")),
            "problems": list(checked.get("problems") or []),
            "by_recipient": dict(checked.get("by_recipient") or {}),
            "hardware": checked.get("hardware"), "recipients": view, "warnings": warnings,
            "stale": [v["id"] for v in view if str(v["verdict"]).startswith("stale")]}


def _on_key(sample: Sample | None) -> Callable[[bytes], bool]:
    """The one place the key is seen: decrypt the sample with it and say whether that worked.
    Nothing about the key or a failure's text leaves this function."""
    def decrypt(key: bytes) -> bool:
        if sample is None:
            return False
        try:
            return bool(sample.decrypts(key, sample.token))
        except Exception:  # noqa: BLE001 - the text of a failure here could echo the key
            return False
    return decrypt


def prove_one(
    verify: Verify, recipient_id: str, *, sample: Sample | None,
) -> tuple[str, str | None]:
    """(status, reason) for one recipient, by the envelope's own interactive proof."""
    try:
        out = verify(recipient_id, on_key=_on_key(sample))
    except Exception as exc:  # noqa: BLE001 - one recipient's failure is its own result
        return FAILED, f"the proof raised {type(exc).__name__}"
    if out.get("needs_operator_key") or out.get("status") == "unavailable":
        # nobody acted, or the key is not plugged in: the operator's to do, not a failed proof
        return NEEDS_OPERATOR_KEY, str(out.get("error") or "needs the operator's key")
    if not out.get("ok"):
        return FAILED, str(out.get("error") or "the envelope did not open for this recipient")
    if out.get("fingerprint_matches") is not True:
        return FAILED, ("it opened, but the key is not the live vault key, "
                        "or that could not be confirmed")
    if sample is None:
        return FAILED, "no sealed artifact was available to decrypt, so nothing was proved"
    if out.get("artifact_ok") is not True:
        return FAILED, "it opened, but its key does not decrypt a real sealed artifact"
    return PROVED, None


def run_drill(
    recipients: Recipients, verify: Verify, *, sample: Sample | None, only: str | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Prove every recipient on record (or one) with the operator present; record each result
    and return them in order. A failed attempt never erases an earlier full proof."""
    stamp = (now or datetime.now(UTC)).isoformat()
    results: list[dict[str, Any]] = []
    for r in recipients():
        rid = str(r.get("id"))
        if only is not None and rid != only:
            continue
        status, reason = prove_one(verify, rid, sample=sample)
        with _locked_record():  # the proof above can take minutes; only the write is locked
            record = read_record()
            entry = record.get(rid, {})
            entry.update({"label": r.get("label"), "kind": r.get("kind"),
                          "last_attempt_at": stamp, "status": status, "reason": reason})
            if status == PROVED:
                entry["last_proved_at"] = stamp
            record[rid] = entry
            _write_record(record)
        results.append({"id": rid, "label": r.get("label"), "kind": r.get("kind"),
                        "status": status, "reason": reason})
    return results


_SAMPLE_TRIES = 8
_FALLBACK_SECONDS = 20.0


async def _sealed_tokens(pool: Any, tries: int) -> list[bytes]:
    """Up to `tries` real sealed lines. A table sample is cheap but the page sample happens
    BEFORE the sealed-line filter, so it can come back empty on a store that has plenty: retry
    once with a far wider sample, then fall back to a bounded read of the last sealed rows in
    key order (deterministic, time-limited), so an empty result means the store really has none."""
    from src.ingest.soul_crypto import FERNET_TOKEN_PREFIX

    reltuples = await pool.fetchval(
        "SELECT reltuples::bigint FROM pg_class WHERE oid = 'soul_lines'::regclass")
    pct = min(100.0, max(0.01, tries * 4 / max(int(reltuples or 0), tries) * 100))
    sampled = ("SELECT raw_line FROM soul_lines TABLESAMPLE SYSTEM ($1) "
               "WHERE substring(raw_line from 1 for 5) = $2 LIMIT $3")
    for percent in (pct, min(100.0, pct * 20)):
        rows = await pool.fetch(sampled, percent, FERNET_TOKEN_PREFIX, tries)
        if rows:
            return [bytes(r["raw_line"]) for r in rows]
    rows = await pool.fetch(
        "SELECT raw_line FROM soul_lines WHERE substring(raw_line from 1 for 5) = $1 "
        "ORDER BY harness DESC, anchor_sid DESC, line_idx DESC LIMIT $2",
        FERNET_TOKEN_PREFIX, tries, timeout=_FALLBACK_SECONDS)
    return [bytes(r["raw_line"]) for r in rows]


async def sealed_sample(pool: Any, *, tries: int = _SAMPLE_TRIES) -> Sample | None:
    """A few REAL sealed soul lines from the store, and a check that passes when a key opens at
    least one of them. Several, because a line sealed before the last key rotation opens only
    with an older key: what is being proved is that the recipient's key opens real data, not that
    every line is current. None only when the store really holds no sealed line to try."""
    from cryptography.fernet import Fernet, InvalidToken

    tokens = await _sealed_tokens(pool, tries)
    if not tokens:
        return None

    def opens_one(key: bytes, _unused: bytes) -> bool:
        fernet = Fernet(key)
        for token in tokens:
            try:
                fernet.decrypt(token)
            except InvalidToken:
                continue
            return True
        return False

    return Sample(token=tokens[0], decrypts=opens_one)


def verify_with_prompts(
    recipients: Recipients, verify: Verify, prompt_code: Callable[[str], str],
) -> Verify:
    """Wrap the envelope's verify so a recovery-code recipient is asked for its typed code (hidden
    input, by `prompt_code`) and a hardware recipient is left to its own touch prompt. The code
    goes straight to the envelope and is not kept."""
    kinds = {str(r.get("id")): r.get("kind") for r in recipients()}

    def wrapped(recipient_id: str, **kw: Any) -> dict[str, Any]:
        code = (prompt_code(f"recovery code for {recipient_id}: ")
                if kinds.get(recipient_id) == "recovery-code" else None)
        return verify(recipient_id, interactive=True, code=code, **kw)

    return wrapped


_NOTIFIED = "_notified"


def _due(entry: object, state: str, now: datetime) -> bool:
    """Is this notice to be told now? Yes when it was never told, its state has changed, or it
    was last told REMIND_DAYS or more ago and is still wanted. `entry` is {state, at}; the older
    plain-string form (state only) is told once more, which stamps it."""
    if not isinstance(entry, dict) or entry.get("state") != state:
        return True
    told_at = _parse(entry.get("at"))
    return told_at is None or (now - told_at).days >= REMIND_DAYS


async def surface_stale(
    pool: Any, recipients: Recipients, check: Check, *, now: datetime | None = None,
    stale_days: int = DEFAULT_STALE_DAYS,
) -> list[str]:
    """Tell the operator's desk about a recipient that has gone stale or was never proved, and
    about a problem the envelope's own check reports. It never opens anything.

    FAILS CLOSED: only a missing recovery file is the quiet first-run case. A file that cannot be
    read, an exception while looking, or a file with no recipients is a problem told to the desk
    (state "open") and logged.

    A notice is told when its state is new and again every REMIND_DAYS while it is still wanted
    (the record keeps `{state, at}` per notice), so a recipient stale for a year is not told once
    and forgotten. A cleared state (proved again, problem fixed) is dropped, so it is told afresh
    if it comes back. Never fails the run."""
    import logging

    from src.orchestrator.mailbox import send_message

    log = logging.getLogger(__name__)
    when = now or datetime.now(UTC)
    told: list[str] = []
    try:
        section = envelope_section(recipients, check, now=when, stale_days=stale_days)
    except Exception as exc:  # noqa: BLE001 - nothing here may fail the run, nor stay silent
        log.warning("could not read the envelope's state", exc_info=True)
        section = _section_error(exc)
    if section.get("error"):
        log.warning("envelope check: %s", section["error"])
    if section.get("configured") is False:
        return told  # the one quiet case: no recovery file yet; backup-status says so
    wanted: dict[str, tuple[str, str]] = {}
    for v in section.get("recipients") or []:
        verdict = str(v["verdict"])
        if verdict.startswith(("stale", "never")):
            state = str(v["last_activity_at"] or "never")
            wanted[f"recipient:{v['id']}"] = (
                state, f"envelope: {v.get('label') or v['id']}: {verdict}")
    for problem in section.get("problems") or []:
        wanted[f"problem:{problem}"] = ("open", f"envelope: {problem}")
    if not wanted and (section.get("error") or section.get("unreadable")):
        wanted["problem:unreadable"] = ("open", "envelope: the envelope could not be read")
    if section.get("error") or section.get("unreadable"):
        log.warning("envelope check: telling the desk the envelope cannot be read")
    try:
        notified: dict[str, Any] = dict(read_record().get(_NOTIFIED) or {})
        sent: dict[str, dict[str, str]] = {}
        for key, (state, sentence) in wanted.items():
            if not _due(notified.get(key), state, when):
                continue
            await send_message(
                pool, from_agent="cron:envelope_check", from_project="osiris",
                to_project="operator", body=sentence, desk_kind="hands",
                dedup_window_secs=3600)
            sent[key] = {"state": state, "at": when.isoformat()}
            told.append(sentence)
        with _locked_record():
            # re-read inside the lock: a drill that finished meanwhile keeps its entry
            record = read_record()
            current: dict[str, Any] = dict(record.get(_NOTIFIED) or {})
            current.update(sent)
            record[_NOTIFIED] = {k: v for k, v in current.items() if k in wanted}
            _write_record(record)
    except Exception:  # noqa: BLE001 - telling the desk must never fail the run
        log.warning("could not surface the envelope's state", exc_info=True)
    return told
