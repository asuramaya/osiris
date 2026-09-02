"""The watch — the kernel as a tripwire, not just a lens.

Three source-agnostic primitives, each riding reliability the kernel already has
(idempotent emit, durable outbox, atomic cursors). No real collection lives here:
a `tick` takes an injected puller; a real connector lands in a later phase.

  * watermarks (`get_cursor`/`set_cursor`) — a generic "last cursor" store. A source
    tick pulls only the delta past its cursor. Re-pulls are already safe (find-or-
    create dedups); the watermark makes them cheap.
  * `tick` — a scheduled, source-agnostic pull: read cursor -> puller(cursor) -> feed
    each item through Actions -> advance cursor. The materialized objects write outbox
    events, which the evaluator below picks up. One clean seam between collection and
    the watch.
  * `evaluate_watches` — drains the durable outbox PAST ITS OWN CLAIM FLAG
    (`evaluated_at`, independent of the cascade's `published_at`), matches each new
    mutation against active WATCHES (kind='watch' compositions, whose `select` spec is
    reduced to match criteria), and emits an `alerts` row to a dumb sink.

A watch and a lens are ONE primitive (a composition): the same `select` spec you run() on
demand (the lens — current members) drives this evaluator (the tripwire — alert on a new
member). The match is prospective: a watch created today fires on tomorrow's events, not
yesterday's (each outbox row is evaluated exactly once) — "tell me when X happens", not
"search what already happened".
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.config.settings import Settings, get_settings
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

logger = logging.getLogger("osiris.monitor")


# --- watermarks: a generic cursor store -------------------------------------

async def get_cursor(pool: asyncpg.Pool, key: str) -> str | None:
    """The last recorded cursor for `key`, or None if never set."""
    cursor: str | None = await pool.fetchval("SELECT cursor FROM watermarks WHERE key=$1", key)
    return cursor


async def set_cursor(pool: asyncpg.Pool, key: str, cursor: str) -> None:
    """Record `cursor` for `key` (upsert)."""
    await pool.execute(
        "INSERT INTO watermarks (key, cursor, updated_at) VALUES ($1,$2,now()) "
        "ON CONFLICT (key) DO UPDATE SET cursor=EXCLUDED.cursor, updated_at=now()",
        key,
        cursor,
    )


# --- worker dead-man's-switch (D3): the heartbeat IS a watermark's freshness ----
WORKER_HEARTBEAT_KEY = "worker:heartbeat"


async def write_heartbeat(pool: asyncpg.Pool) -> None:
    """The worker touches this each cron tick. Its `updated_at` is the liveness signal."""
    await pool.execute(
        "INSERT INTO watermarks (key, cursor, updated_at) VALUES ($1, now()::text, now()) "
        "ON CONFLICT (key) DO UPDATE SET cursor=now()::text, updated_at=now()",
        WORKER_HEARTBEAT_KEY,
    )


async def heartbeat_age_secs(pool: asyncpg.Pool) -> float | None:
    """Seconds since the worker's last heartbeat, or None if it has never beaten."""
    age: float | None = await pool.fetchval(
        "SELECT extract(epoch FROM (now() - updated_at)) FROM watermarks WHERE key=$1",
        WORKER_HEARTBEAT_KEY,
    )
    return age


# --- ORGAN HEALTH: per-JOB vitals, derived at READ time -------------------------
#
# The session-miner died at 08:50 on 2026-07-12 and stayed dead for TEN HOURS. Every ten-minute
# tick failed ("no LLM provider"), the memory stopped forming, and nothing told anyone — the only
# witness was a counter buried inside a fleet_digest too large to open (bug 79e1328c).
#
# TWO THINGS THAT LOOK LIKE FIXES AND ARE NOT:
#
#  1. "Add a cron that checks whether the miner is dead." That cron would live in the SAME worker.
#     A dead process cannot report its own death — the watcher would fail exactly when it mattered
#     (loop pathology, invariant 7). So health is never WRITTEN by a watchdog; it is DERIVED at
#     READ time by whoever asks, all of whom are alive by construction: the statusline (the
#     operator's shell), orient() (the MCP), the console. No new daemon watches the daemons.
#
#  2. "Check the worker's heartbeat." It was GREEN the whole ten hours. The worker was perfectly
#     alive and healthy — it was the JOB INSIDE IT that was failing. A process-level pulse would
#     have reported all-clear while the graph went blind. So vitals are per-JOB, never per-process.
#
# And Osiris HAS NO HANDS: this senses and surfaces. It never restarts anything.
_JOB_PREFIX = "job:"


async def record_job(
    pool: asyncpg.Pool, name: str, *, every: int, secs: float = 0.0, error: str | None = None,
) -> None:
    """A job confesses its own outcome — the one write in this file's health story.

    `every` is the job's PERIOD in seconds, recorded WITH the outcome so the reader never needs
    a table of magic thresholds: a job is late relative to its own cadence, and a job added next
    year brings its own definition of late.

    A failure does NOT clear `last_ok` — the reader needs to know both that it broke and when it
    last worked, which is the difference between "down 4 minutes" and "down ten hours".
    """
    key = f"{_JOB_PREFIX}{name}"
    raw = await get_cursor(pool, key)
    try:
        blob: dict[str, Any] = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        blob = {}
    now = datetime.now(UTC).isoformat()
    blob["every"] = every
    blob["last_run"] = now
    blob["secs"] = round(secs, 1)
    if error is None:
        blob["last_ok"] = now
        blob["fails"] = 0
        blob.pop("last_error", None)
    else:
        blob["last_error"] = error[:300]
        blob["fails"] = int(blob.get("fails", 0)) + 1
    await set_cursor(pool, key, json.dumps(blob))


# THE ONE PLACE THIS THRESHOLD LIVES (Thoth msg 6327): `grep -rn "3 \* every"` used to return
# three hand-copied sites — this file, src/orchestrator/surface.py, and
# scripts/osiris_fleet_glance.py — the exact "same word, same number, same SQL, three
# implementations" bug surface.py's own module docstring names as its founding problem. A fix
# landed at one site (surface.py's floor, commit ac20a0e) left the other two answering a
# different question about the same job at the same instant. Every reader of "is this job
# late" now calls THIS function; none re-derives the arithmetic.
#
# `_SICK_FLOOR_SECS`: a worker restart's boot cost is 0-2s (measured, 8 restarts, 2026-09-01)
# and never explains a false alarm on its own — what does is a cron tick actually IN FLIGHT,
# draining a real backlog, cancelled mid-run by a deploy's SIGTERM (measured worst case: 44s,
# "cron:drain_cascade cancelled" in the worker log). 90s is 2x that. It only LOOSENS
# sub-30s-cadence jobs (max(3*every, 90)); a 24h job still reads down after 3 days, unchanged.
_SICK_FLOOR_SECS = 90


def sick_after_secs(every: int) -> float:
    """The age, in seconds, past which a job's silence stops being a blip and starts being
    down. Three of its own cadences, floored so a fast job survives a deploy's restart+cancel
    cost without a false alarm (see `_SICK_FLOOR_SECS` above)."""
    return max(3 * every, _SICK_FLOOR_SECS)


def _verdict(last_ok: datetime | None, every: int, now: datetime) -> tuple[str, float | None]:
    if last_ok is None:
        return "never", None
    age = (now - last_ok).total_seconds()
    if age > sick_after_secs(every):      # three cadences missed in a row is not a blip
        return "down", age
    if age > 1.5 * every:
        return "stale", age
    return "ok", age


async def reap_decommissioned_jobs(pool: asyncpg.Pool) -> list[str]:
    """DELETE the watermarks of crons that no longer exist. Returns what it reaped.

    A watermark OUTLIVES the job that wrote it. When the session-miner's crawl was removed
    (ceae1604), `job:sense_sessions` stayed behind with its last_ok frozen at the moment it died —
    and THREE separate readers, each with its own copy of the same law, went on reporting "NOT
    SENSING" forever about a capability we had deliberately deleted. The operator saw it on his
    statusline and told me. He was right.

    I had already fixed this — in ONE of the three. organ_health got a filter; the statusline
    re-implements the check inline (it is standalone on purpose, so it imports nothing); preflight
    has its own copy again. THAT IS THE BUG I HAVE COMMITTED ALL WEEK: a correction that lands at
    one site and not at the others that READ.

    So the fix is not a third filter. It is to stop lying in the DB. THE SCHEDULE IS THE SOURCE OF
    TRUTH; the watermark is only residue — and the worker, which knows its own schedule, reconciles
    the two at boot. Every reader is corrected for free, and a reader written next year inherits it
    without knowing this exists.

    It is telemetry, never the graph: deleting it forgets an outage's *timing*, not any fact the
    kernel holds. And it is LOUD — the caller logs what it reaped, because an organ silently
    vanishing is exactly the class of failure this whole module exists to prevent.
    """
    live = scheduled_jobs()
    if not live:                       # cannot ask the schedule → never reap on a guess
        return []
    rows = await pool.fetch(
        "DELETE FROM watermarks WHERE key LIKE $1 "
        "AND substring(key from 5) <> ALL($2::text[]) RETURNING key",
        f"{_JOB_PREFIX}%", list(live))
    return [r["key"][len(_JOB_PREFIX):] for r in rows]


def scheduled_jobs() -> set[str]:
    """The crons that ACTUALLY run right now — imported lazily so the read path never depends on
    the worker being importable. Empty set = "I could not ask", and the caller then trusts the
    watermarks rather than silently hiding a sick organ (fail-loud, not fail-quiet)."""
    try:
        from src.workers.arq_worker import WorkerSettings
        return {n for c in WorkerSettings.cron_jobs
                if (n := getattr(c.coroutine, "__name__", ""))}
    except Exception:  # pragma: no cover — arq missing in a satellite/read-only deploy
        return set()


async def organ_health(
    pool: asyncpg.Pool, *, scheduled: set[str] | None = None,
) -> list[dict[str, Any]]:
    """THE READ SIDE — every job's vitals, computed now, by you, from what it last stamped.

    Returns one row per job, worst first, so a caller can `[o for o in organs if o["down"]]` and
    render nothing at all when the body is well. Silence when healthy is the whole design: an
    alarm that is always on is an alarm nobody reads.
    """
    rows = await pool.fetch(
        "SELECT key, cursor FROM watermarks WHERE key LIKE $1", f"{_JOB_PREFIX}%")
    live = scheduled_jobs() if scheduled is None else scheduled
    now = datetime.now(UTC)
    organs: list[dict[str, Any]] = []
    for r in rows:
        job = r["key"][len(_JOB_PREFIX):]
        # AN ORGAN THAT IS NO LONGER SCHEDULED IS NOT AN ORGAN. A watermark row outlives the job
        # that wrote it, so a DECOMMISSIONED cron (the session-miner's crawl, killed in ceae1604)
        # would sit here reading "down" forever and nag the operator at every prompt about a
        # capability we deliberately removed. THE SCHEDULE IS THE SOURCE OF TRUTH, never the
        # residue. An alarm that is always on is an alarm nobody reads.
        if live and job not in live:
            continue
        try:
            blob = json.loads(r["cursor"] or "{}")
        except json.JSONDecodeError:
            continue
        every = int(blob.get("every") or 600)
        raw_ok = blob.get("last_ok")
        last_ok = datetime.fromisoformat(raw_ok) if raw_ok else None
        verdict, age = _verdict(last_ok, every, now)
        organs.append({
            "job": r["key"][len(_JOB_PREFIX):],
            "verdict": verdict,
            "down": verdict in ("down", "never"),
            "every_secs": every,
            "last_ok": raw_ok,
            "age_secs": round(age) if age is not None else None,
            "fails": int(blob.get("fails") or 0),
            **({"last_error": blob["last_error"]} if blob.get("last_error") else {}),
        })
    rank = {"never": 0, "down": 1, "stale": 2, "ok": 3}
    organs.sort(key=lambda o: (rank[o["verdict"]], -(o["age_secs"] or 0)))
    return organs


def health_banner(organs: list[dict[str, Any]]) -> str | None:
    """One line, or None when the body is well. The line a mind reads at mount, and the operator
    reads at every prompt: WHAT stopped, and HOW LONG AGO it last worked.

    THE BANNER MUST NOT OVERSTATE. It used to end "the graph is not forming memory" — written when
    the only organ that could plausibly die was the session-miner's crawl. That crawl is gone
    (ceae1604) and memory now forms through DELIBERATE CAPTURE, which no cron can break. A banner
    that cries "you have lost your memory" because the semantic index is late is the same crime as
    everything else we killed this week: a claim wearing more authority than the evidence supports.
    So it names WHAT stopped and lets the reader judge the blast radius.
    """
    sick = [o for o in organs if o["down"]]
    if not sick:
        return None
    parts = []
    for o in sick[:3]:
        when = "never ran" if o["verdict"] == "never" else f"last ok {_ago(o['age_secs'])}"
        parts.append(f"{o['job']} ({when})")
    more = f" +{len(sick) - 3} more" if len(sick) > 3 else ""
    return ("⚠ AN OSIRIS ORGAN HAS STOPPED — " + ", ".join(parts) + more
            + ". Deliberate capture (record_decision / open_thread) still works — it does not "
              "ride a cron. Nothing auto-restarts this (Osiris has no hands over your systems). "
              "Check: systemctl --user status osiris-worker")


def _ago(secs: float | None) -> str:
    if secs is None:
        return "never"
    if secs < 3600:
        return f"{int(secs // 60)}m ago"
    if secs < 86400:
        return f"{int(secs // 3600)}h ago"
    return f"{int(secs // 86400)}d ago"


# --- miner tick telemetry: the onboarding-day lesson (decision 3191e0df) --------
# A fail-open cron was down a DAY behind a green heartbeat. The heartbeat says the worker
# breathes; THIS says whether the sensing tick actually finishes, how long it runs, and
# whether it is saturated. One writer by design — the sense_sessions cron (unique=True);
# the digest and preflight only read.
MINER_TICKS_KEY = "miner:ticks"
_MINER_KEEP = 48  # ~8h of 10-min ticks


async def _miner_blob(pool: asyncpg.Pool) -> dict[str, Any]:
    raw = await get_cursor(pool, MINER_TICKS_KEY)
    if not raw:
        return {"starts": 0, "completions": 0, "ticks": []}
    try:
        blob: dict[str, Any] = json.loads(raw)
    except json.JSONDecodeError:  # a corrupt blob is telemetry lost, never a crash
        return {"starts": 0, "completions": 0, "ticks": []}
    blob.setdefault("starts", 0)
    blob.setdefault("completions", 0)
    blob.setdefault("ticks", [])
    return blob


async def miner_tick_started(pool: asyncpg.Pool) -> None:
    """Stamp a tick's beginning. A start with no matching completion is a death the
    coroutine could not confess itself (a timeout cancel that outran the shield)."""
    blob = await _miner_blob(pool)
    blob["starts"] += 1
    blob["last_start"] = datetime.now(UTC).isoformat()
    await set_cursor(pool, MINER_TICKS_KEY, json.dumps(blob))


async def miner_tick_ended(
    pool: asyncpg.Pool, *, secs: float, budget: int,
    report: dict[str, int] | None = None, error: str | None = None,
) -> None:
    """Record a tick's outcome — duration, chunk spend vs budget, yield, or the error."""
    blob = await _miner_blob(pool)
    blob["completions"] += 1
    rec: dict[str, Any] = {"at": datetime.now(UTC).isoformat(),
                           "secs": round(secs, 1), "budget": budget}
    if report is not None:
        rec["chunks"] = report.get("chunks", 0)
        rec["yield"] = sum(report.get(k, 0) for k in
                           ("decisions", "threads", "obligations", "resolved"))
    if error is not None:
        rec["error"] = error[:200]
    blob["ticks"] = ([*blob["ticks"], rec])[-_MINER_KEEP:]
    await set_cursor(pool, MINER_TICKS_KEY, json.dumps(blob))


async def miner_health(pool: asyncpg.Pool) -> dict[str, Any]:
    """The read side: counters + the retained tick records (newest last)."""
    return await _miner_blob(pool)


# --- tick: a source-agnostic scheduled pull ---------------------------------

@dataclass
class WatchItem:
    """One thing a puller found in a delta: an object plus its facts. The tick
    materializes it through Actions (idempotent), so a re-pull is a no-op."""

    type: str
    canonical: str
    properties: dict[str, Any] = field(default_factory=dict)
    evidence_class: EvidenceClass = EvidenceClass.AUTHORITATIVE_API
    observed_at: datetime | None = None


@dataclass
class PullResult:
    """What a puller returns: the items in this delta and the new cursor to persist
    AFTER they are committed (so a crash mid-tick re-pulls the same delta safely)."""

    items: list[WatchItem]
    cursor: str


# A puller is the only collection-specific part: (last cursor) -> a delta. Injected,
# so the watch is fully testable with a canned source and no network.
Puller = Callable[[str | None], Awaitable[PullResult]]


async def tick(actions: Actions, source_id: str, puller: Puller) -> int:
    """Pull the delta past `source:<source_id>`'s cursor, materialize each item
    through Actions, then advance the cursor. Returns the number of items applied.

    The cursor is advanced ONLY after the items commit — if the process dies
    mid-tick, the next tick re-pulls the same delta and find-or-create dedups it."""
    pool = actions.pool
    cursor = await get_cursor(pool, f"source:{source_id}")
    result = await puller(cursor)
    for item in result.items:
        observed = item.observed_at or datetime.now(UTC)
        object_id = await actions.create_or_find_object(item.type, item.canonical, source_id)
        for name, value in item.properties.items():
            await actions.assert_property(
                object_id, name, value, source_id, observed,
                confidence_for(item.evidence_class),
                evidence_class=item.evidence_class.value,
            )
    await set_cursor(pool, f"source:{source_id}", result.cursor)
    return len(result.items)


# --- subscriptions: saved match criteria ------------------------------------

@dataclass(frozen=True)
class GraphEvent:
    """One outbox mutation, enriched with the object's type/canonical for matching."""

    outbox_id: int
    event_type: str
    object_id: uuid.UUID | None
    case_id: uuid.UUID | None
    object_type: str | None
    canonical: str | None
    payload: dict[str, Any]
    value: Any | None  # the new property value (property_added only)
    # the object's current scalar properties — loaded for object_created so a beat can
    # match a whole object at once (e.g. zip AND price); empty for other events.
    props: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Watch:
    """A saved watch — a kind='watch' composition. Its `select` spec is reduced to the
    evaluator's match `criteria`; the same spec is also run() on demand as a lens."""

    id: uuid.UUID
    name: str
    criteria: dict[str, Any]
    webhook_url: str | None


def watch_criteria(spec: dict[str, Any]) -> dict[str, Any]:
    """Derive the evaluator's match criteria from a watch's `select` spec. A watch fires
    on SET-ENTRY: a newly created object whose type+properties satisfy the select (the
    real-beat semantic — new filing/notice/entity). Membership-change on an existing
    object via a later property update is out of scope for v1 (documented)."""
    criteria: dict[str, Any] = {
        "event_types": ["object_created"],
        "object_type": spec.get("object_type"),
        "where": spec.get("where", []) or [],
    }
    if spec.get("canonical_prefix"):
        criteria["canonical_prefix"] = spec["canonical_prefix"]
    return criteria


def matches(criteria: dict[str, Any], event: GraphEvent) -> dict[str, Any] | None:
    """Does `event` satisfy `criteria`? Returns a small "why it matched" dict, or
    None. All present clauses must hold (AND); absent clauses don't constrain.

    Supported clauses (all source-agnostic):
      event_types       : list[str]  — any of these outbox event types
      object_type       : str        — the object's type
      canonical_prefix  : str        — object canonical starts with (e.g. 'cik:')
      canonical_contains: str        — substring, case-insensitive
      property_name     : str        — for property_added: the property's name
      value_contains    : str        — for property_added: substring of the new value
    """
    ets = criteria.get("event_types")
    if ets and event.event_type not in ets:
        return None
    ot = criteria.get("object_type")
    if ot and event.object_type != ot:
        return None
    cp = criteria.get("canonical_prefix")
    if cp and not (event.canonical or "").startswith(cp):
        return None
    cc = criteria.get("canonical_contains")
    if cc and cc.lower() not in (event.canonical or "").lower():
        return None
    pn = criteria.get("property_name")
    if pn and event.payload.get("name") != pn:
        return None
    vc = criteria.get("value_contains")
    if vc and vc.lower() not in str(event.value or "").lower():
        return None
    # object-level conjunction over the object's properties (a "beat"): every condition
    # must hold. Reads event.props (object_created); falls back to the event value when a
    # condition names the property that just changed.
    for cond in criteria.get("where", []) or []:
        prop = cond.get("property")
        actual = event.props.get(prop)
        if actual is None and event.payload.get("name") == prop:
            actual = event.value
        if not match_condition(actual, cond.get("op", "contains"), cond.get("value")):
            return None
    return {
        "event_type": event.event_type,
        "object_type": event.object_type,
        "canonical": event.canonical,
        "property": event.payload.get("name"),
    }


def match_condition(actual: Any, op: str, expected: Any) -> bool:
    """One beat condition. Ops: eq / contains (case-insensitive substring) / matches_all
    (every whitespace-separated token in `expected` present in `actual`, any order —
    word-order-proof) / lt / gt (numeric) / present / absent (the value exists / is
    missing-or-blank — `expected` is ignored). A missing value or an un-parseable number
    fails closed (no false match). Shared by the evaluator (matches), composition `where`
    clauses (select's _eval), and the read-model feed (/matches)."""
    if op == "present":                       # the property exists and isn't blank
        return actual is not None and str(actual).strip() != ""
    if op == "absent":                        # missing or blank — the complement
        return actual is None or str(actual).strip() == ""
    if actual is None:
        return False
    if op == "eq":
        return str(actual).strip().lower() == str(expected).strip().lower()
    if op == "contains":
        return str(expected).lower() in str(actual).lower()
    if op == "matches_all":                   # word-order-proof: all tokens present, any order
        hay = str(actual).lower()
        return all(tok in hay for tok in str(expected).lower().split())
    if op in ("lt", "gt"):
        try:
            a, b = float(actual), float(expected)
        except (TypeError, ValueError):
            return False
        return a < b if op == "lt" else a > b
    return False


async def _active_watches(pool: asyncpg.Pool) -> list[Watch]:
    """The active watches — kind='watch' compositions. Read straight from the table (no
    import of compositions.py, which imports this module) and reduce each select spec to
    the evaluator's criteria."""
    import json
    rows = await pool.fetch(
        "SELECT id, name, spec, webhook_url FROM compositions "
        "WHERE kind='watch' AND active ORDER BY created_at"
    )
    out: list[Watch] = []
    for r in rows:
        spec = r["spec"]
        if isinstance(spec, str):  # asyncpg returns jsonb as text unless a codec is set
            spec = json.loads(spec)
        out.append(Watch(r["id"], r["name"], watch_criteria(spec), r["webhook_url"]))
    return out


# --- the dumb alert sink ----------------------------------------------------

# A sink delivers an alert OUTSIDE the durable `alerts` table (which is always written
# first). The default routes by channel (webhook → email → log; see `default_sink`). NOT a
# CRM — a table row + an optional notification, nothing more. Injectable for tests.
Sink = Callable[["Alert"], Awaitable[bool]]


@dataclass(frozen=True)
class Alert:
    id: uuid.UUID
    watch_id: uuid.UUID
    watch_name: str
    object_id: uuid.UUID | None
    event_type: str
    matched: dict[str, Any]
    webhook_url: str | None


async def _sink_webhook(alert: Alert) -> bool:
    """POST the alert to the watch's webhook. Returns True if it should be marked delivered."""
    if not alert.webhook_url:
        return False
    import httpx
    payload = {
        "alert_id": str(alert.id),
        "watch": alert.watch_name,
        "event_type": alert.event_type,
        "object_id": str(alert.object_id) if alert.object_id else None,
        "matched": alert.matched,
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(alert.webhook_url, json=payload)
        resp.raise_for_status()
    return True


async def _sink_log(alert: Alert) -> bool:
    """The default channel: a structured log line. Always 'delivers' (the record IS the log
    + the durable /alerts row) — the right fallback for a single-operator box with no webhook."""
    logger.info(
        "ALERT watch=%r event=%s object=%s matched=%s",
        alert.watch_name, alert.event_type, alert.object_id, alert.matched,
    )
    return True


async def _sink_email(alert: Alert) -> bool:
    """Email channel (the seam). Requires OSIRIS_SMTP_HOST; absent => recorded-only + warn,
    never crash a run. Sends via stdlib smtplib only when fully configured."""
    s = get_settings()
    if not s.osiris_smtp_host:
        logger.warning(
            "email requested (OSIRIS_ALERT_EMAIL) but OSIRIS_SMTP_HOST unset; alert %s "
            "recorded only", alert.id,
        )
        return False
    import smtplib
    import ssl
    from email.message import EmailMessage
    msg = EmailMessage()
    msg["Subject"] = f"Osiris alert · {alert.watch_name}"
    msg["From"] = s.osiris_smtp_user or "osiris@localhost"
    msg["To"] = s.osiris_alert_email
    msg.set_content(
        f"Watch: {alert.watch_name}\nEvent: {alert.event_type}\n"
        f"Object: {alert.object_id}\nMatched: {alert.matched}\n"
    )
    with smtplib.SMTP(s.osiris_smtp_host, s.osiris_smtp_port, timeout=15) as srv:
        srv.starttls(context=ssl.create_default_context())
        if s.osiris_smtp_user:
            srv.login(s.osiris_smtp_user, s.osiris_smtp_password)
        srv.send_message(msg)
    return True


async def default_sink(alert: Alert) -> bool:
    """Route an alert to its channel: a watch's webhook → webhook; else if an alert email is
    configured → email; else → the log. A pluggable side-channel over the durable record."""
    if alert.webhook_url:
        return await _sink_webhook(alert)
    if get_settings().osiris_alert_email:
        return await _sink_email(alert)
    return await _sink_log(alert)


async def _load_event(pool: asyncpg.Pool, row: asyncpg.Record) -> GraphEvent:
    object_type: str | None = None
    canonical: str | None = None
    value: Any | None = None
    if row["object_id"] is not None:
        obj = await pool.fetchrow(
            "SELECT type, canonical FROM objects WHERE id=$1", row["object_id"]
        )
        if obj is not None:
            object_type, canonical = obj["type"], obj["canonical"]
    payload = row["payload"]
    if isinstance(payload, str):
        import json
        payload = json.loads(payload)
    if row["event_type"] == "property_added" and row["object_id"] is not None:
        name = payload.get("name")
        if name:
            value = await pool.fetchval(
                "SELECT value #>> '{}' FROM current_assertions "
                "WHERE object_id=$1 AND name=$2 ORDER BY observed_at DESC LIMIT 1",
                row["object_id"],
                name,
            )
    props: dict[str, Any] = {}
    if row["event_type"] == "object_created" and row["object_id"] is not None:
        # by evaluation time the object's assertions are committed (the evaluator runs
        # after the tick), so a beat can match the whole object on its creation event.
        for r in await pool.fetch(
            "SELECT DISTINCT ON (name) name, value #>> '{}' AS v FROM current_assertions "
            "WHERE object_id=$1 ORDER BY name, observed_at DESC",
            row["object_id"],
        ):
            props[r["name"]] = r["v"]
    return GraphEvent(
        outbox_id=row["id"],
        event_type=row["event_type"],
        object_id=row["object_id"],
        case_id=row["case_id"],
        object_type=object_type,
        canonical=canonical,
        payload=payload,
        value=value,
        props=props,
    )


async def evaluate_watches(
    pool: asyncpg.Pool, *, sink: Sink | None = None, limit: int = 500
) -> int:
    """Claim a batch of un-evaluated outbox rows, match each against active WATCHES
    (kind='watch' compositions), and emit an `alerts` row per match. Returns the number
    of alerts emitted. Idempotent: the claim flag (`evaluated_at`) makes each row fire at
    most once; the (composition, outbox) unique key makes a re-run a no-op.

    The claim is gap-free (FOR UPDATE SKIP LOCKED on the unevaluated flag), and the
    evaluator never touches `published_at`, so it is fully decoupled from the cascade.
    """
    deliver: Sink = sink if sink is not None else default_sink
    watches = await _active_watches(pool)

    rows = await pool.fetch(
        "UPDATE outbox SET evaluated_at=now() WHERE id IN ("
        "  SELECT id FROM outbox WHERE evaluated_at IS NULL "
        "  ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED"
        ") RETURNING id, event_type, object_id, case_id, payload",
        limit,
    )
    if not rows:
        return 0

    fired: list[Alert] = []
    for row in rows:
        if not watches:
            continue
        event = await _load_event(pool, row)
        for w in watches:
            why = matches(w.criteria, event)
            if why is None:
                continue
            alert_id = await pool.fetchval(
                "INSERT INTO alerts (composition_id, outbox_id, object_id, event_type, matched) "
                "VALUES ($1,$2,$3,$4,$5) ON CONFLICT (composition_id, outbox_id) DO NOTHING "
                "RETURNING id",
                w.id,
                event.outbox_id,
                event.object_id,
                event.event_type,
                why,
            )
            if alert_id is None:  # already alerted (idempotent re-run)
                continue
            fired.append(
                Alert(alert_id, w.id, w.name, event.object_id, event.event_type,
                      why, w.webhook_url)
            )

    # deliver to the side-channel sink AFTER the durable rows are committed. A sink
    # failure must never lose the alert (the table row stands) or abort the batch.
    # DELIVERY is throttled (the 3am-false-alert guard); the durable rows are never.
    settings = get_settings()
    suppressed = 0
    for alert in fired:
        if not await _deliverable(pool, alert, settings):
            suppressed += 1  # row kept, delivery suppressed (cooldown or rate cap)
            continue
        try:
            if await deliver(alert):
                await pool.execute(
                    "UPDATE alerts SET delivered_at=now() WHERE id=$1", alert.id
                )
        except Exception as exc:
            logger.warning("alert sink failed for %s: %r", alert.id, exc)
    if suppressed:
        logger.info("throttled %d alert deliveries (rows kept, read /alerts)", suppressed)
    return len(fired)


async def _deliverable(pool: asyncpg.Pool, alert: Alert, s: Settings) -> bool:
    """Should this alert be DELIVERED (vs. recorded-only)? Suppress a re-alert of the same
    (watch, object) inside the cooldown, and cap deliveries per watch per window — so a
    burst or a flapping object can't flood the operator. The durable row stands regardless."""
    if alert.object_id is not None and s.osiris_alert_cooldown_secs > 0:
        recent = await pool.fetchval(
            "SELECT 1 FROM alerts WHERE composition_id=$1 AND object_id=$2 "
            "AND delivered_at > now() - ($3::int * interval '1 second') AND id<>$4 LIMIT 1",
            alert.watch_id, alert.object_id, s.osiris_alert_cooldown_secs, alert.id,
        )
        if recent is not None:
            return False
    delivered_in_window: int = await pool.fetchval(
        "SELECT count(*) FROM alerts WHERE composition_id=$1 "
        "AND delivered_at > now() - ($2::int * interval '1 second')",
        alert.watch_id, s.osiris_alert_window_secs,
    )
    return delivered_in_window < s.osiris_alert_max_per_window
