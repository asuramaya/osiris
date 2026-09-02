"""THE SURFACE — one segment authority above vitals (ruling e9ef7373, 2026-07-21).

THE PROBLEM, proven by the '$12/$10' bug (decision 5afabc9e): vitals.py already consolidated
the raw NUMBERS (live_souls / operator_debts / wakes_hour) — 'same word, same number, same SQL'
— but the PRESENTATION RULES never were. 'show $X/$10 only when metered, only past 60% of cap,
amber→red at 85%' was written three times (mounts.fleet_pulse, digest._costs + api.membrane,
scripts/osiris_statusline.py), and a one-line rule change took three commits and still missed
one.

THE FIX: this module computes every AMBIENT GLANCE segment — live, owed/owed_here, briefs
(total + this-agent's-own), wakes, spend, sensing health, and mail (broadcasts + in-flight +
DMs) — exactly ONCE, with every threshold/gate/severity rule already applied. A renderer
(fleet_pulse's one-liner, the statusline's ANSI strip, the membrane header's HTML spans) does
ZERO fetching and ZERO rule logic: it picks the segments it wants, in the order it wants, and
formats `value`/`data`/`severity` in its own medium.

SCOPE = the ambient glance segments shared across strip+pulse+console-header. It is explicitly
NOT the console's deep detail sections (costs breakdown, roster, laundering/disputes, activity)
— those stay console-specific (src/orchestrator/digest.py). `context%` is also out of scope on
purpose: it is read straight off the harness's own payload/transcript, session-local, never a
DB fact — there is nothing here for it to share.

WHY SOME SEGMENTS COME IN TWO VARIANTS: the surfaces don't just duplicate the SAME rule for
`owed` and `briefs` — they deliberately compute DIFFERENT SCOPES, per two distinct operator
rulings that predate this module and stay in force here: 'owe' is FLEET-WIDE on the orient
pulse (`owed`) but THIS-PROJECT-ONLY on the statusline (`owed_here`, 2026-07-16: 'a number you
can do nothing about from this directory is wallpaper'); 'briefs' is the WHOLE DESK on the
pulse (`briefs_total`) but THIS AGENT'S OWN on the statusline (`briefs_mine`, 2026-07-16: 'desk
should not be globally scoped... only scope what is for or from the agent'). Both variants are
computed here, once, from the same underlying authority — a renderer picks the one its surface
has always shown.

WHY `show` ISN'T UNIFORM EITHER: some segments are ambient-always (live/owed/owed_here/
briefs_total/wakes/mail — every surface that renders them shows them every time, healthy or
not); others are dark-until-it-matters (sensing, briefs_mine, spend on the alert convention) —
an always-red debt teaches the eye to stop reading it. `severity` is the one TRUE, single-
computed urgency (ok/amber/red/alarm); `show` on those segments follows the dark-until-matters
convention directly. The one deliberate, documented exception: fleet_pulse's `spend` has never
gated on severity (it shows the figure whenever spend is metered, full stop, no threshold) — its
renderer reads `segment.data["metered"]` itself rather than `segment.show` for that one field;
see the migration note in mounts.fleet_pulse.

PERF CONSTRAINT (load-bearing, non-negotiable): the statusline forks a fresh cold-connect
process every ~10s. `fetch()` is the renderer's ONLY database contact — one call, sequential
awaits on whatever connection-like object it's handed (a bare short-lived Connection for the
statusline, a pool for the long-lived servers; both quack fetchrow/fetchval, same contract
vitals.py's `_DB` Protocol already relies on). Concurrent gather is deliberately NOT used here:
it is unsafe on a bare Connection (one in-flight query at a time) and the perf cost this module
exists to cut is the COLD CONNECT itself, not round-trip count.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

Severity = Literal["ok", "amber", "red", "alarm"]


@dataclass(frozen=True)
class Segment:
    """One ambient fact, fully judged. `value` is a medium-neutral default rendering (good
    enough for a consumer that just wants a label); `data` carries the raw numbers a renderer
    needs to reconstruct its OWN historical exact text (fleet_pulse's plain line and the
    statusline's per-substring ANSI coloring are not the same string shape, and were never
    meant to become one — only the rule that PRODUCES their numbers was ever meant to unify)."""

    key: str
    show: bool
    severity: Severity = "ok"
    value: str = ""
    note: str | None = None
    link: str | None = None
    data: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Segments:
    """Every ambient glance segment, computed once. The SET is shared; which keys a given
    surface renders, and in what order, is that surface's own presentation choice."""

    live: Segment
    owed: Segment
    owed_here: Segment
    briefs_total: Segment
    briefs_mine: Segment
    wakes: Segment
    mail: Segment
    sensing: Segment
    spend: Segment


# A worker restart alone never explains a sick reading (measured: 8 restarts on 2026-09-01,
# boot-to-first-tick 0-2s every time). What DOES cost tens of seconds is a cron tick that was
# actually IN FLIGHT — draining a real backlog — when the deploy's SIGTERM cancelled it: two of
# those 8 restarts show a 5s-cadence job cancelled 40-43s into a run ("42.92s cron:drain_cascade
# cancelled" in the worker log), so last_ok stays that old until the next tick lands. 90s is
# 2x that observed worst case (decision, house osiris, 2026-09-02) — a floor, never a tighter
# bound: a 24h job stays sick after 3 days regardless, this only protects sub-30s cadences from
# their own restart/backlog cost.
_SICK_FLOOR_SECS = 90


async def _sensing(conn: Any) -> list[str]:
    """Is Osiris still SENSING? Relocated verbatim from the statusline's own inline copy (the
    only place this rule lived before this module) — a job is sick if it has never confessed
    an ok, or if it has gone three of its own cadences quiet (or `_SICK_FLOOR_SECS`, whichever
    is looser — a fast-cadence job survives a deploy's restart+cancel cost without a false
    alarm). Computed HERE, at read time, in a process that is alive by construction (a watchdog
    cron would live inside the very worker that died, 2026-07-12's ten-hour outage)."""
    jobs = await conn.fetch("SELECT key, cursor FROM watermarks WHERE key LIKE 'job:%'")
    sick: list[str] = []
    for j in jobs:
        try:
            blob = json.loads(j["cursor"] or "{}")
        except ValueError:
            continue
        ok, every = blob.get("last_ok"), int(blob.get("every") or 600)
        if not ok:
            sick.append(j["key"][4:])
            continue
        age = (datetime.now(UTC) - datetime.fromisoformat(ok)).total_seconds()
        if age > max(3 * every, _SICK_FLOOR_SECS):  # three cadences missed is not a blip
            sick.append(j["key"][4:])
    return sick


async def fetch(
    conn: Any,
    *,
    project: str | None = None,
    agent: str | None = None,
    lease_secs: int = 900,
    live_secs: int = 900,
) -> Segments:
    """THE single lean batched fetch. `conn` is a pool or a live connection — anything that
    quacks fetchrow/fetchval/fetch, same contract as vitals.py's `_DB` Protocol. `project` and
    `agent` scope the this-project/this-agent variants (owed_here, briefs_mine, mail, flight);
    omit either and that variant reads as the identity-less/whole-desk default the underlying
    authorities already define for an unscoped caller (mailbox.unread_split, mounts.fleet_pulse
    never pass one at all)."""
    from src.config.settings import get_settings
    from src.ingest.providers import spend_is_metered
    from src.orchestrator import vitals
    from src.orchestrator.ceiling import ceiling
    from src.orchestrator.mailbox import desk_briefs_from, desk_briefs_total, unread_split

    souls = await vitals.live_souls(conn, live_secs=live_secs)
    debts = await vitals.operator_debts(conn, hood=project)
    b_total = await desk_briefs_total(conn)
    b_mine = await desk_briefs_from(conn, agent)
    wakes_n = await vitals.wakes_hour(conn)
    split = await unread_split(conn, project, reader_agent=agent, lease_secs=lease_secs)
    flight_n = 0
    if project:
        flight_n = int(await conn.fetchval(
            "SELECT count(*) FROM fleet_messages m JOIN message_recipients r "
            "  ON r.message_id=m.id WHERE m.to_project=$1 AND m.to_agent IS NULL "
            "  AND r.agent_id <> $3 AND r.read_at IS NULL AND r.delivered_at IS NOT NULL "
            "  AND r.delivered_at >= now() - make_interval(secs => $2)",
            project, lease_secs, agent or "") or 0)
    sick = await _sensing(conn)

    settings = get_settings()
    metered = spend_is_metered(settings)
    spent = cap = 0.0
    blind = 0
    unlimited = False
    if metered:
        c = await ceiling(conn, cap=settings.osiris_daily_usd)
        spent, cap, blind, unlimited = c.spent, c.cap, c.blind, c.unlimited

    live_seg = Segment(
        "live", show=True, value=f"{souls['souls']} live",
        data={"souls": souls["souls"], "visitors": souls["visitors"]}, link="fleet")

    owed_seg = Segment(
        "owed", show=True, severity="red" if debts["owed"] else "ok",
        value=f"owed {debts['owed']}", data={"owed": debts["owed"]}, link="desk")
    owed_here_seg = Segment(
        "owed_here", show=True, severity="red" if debts["owed_here"] else "ok",
        value=f"owe {debts['owed_here']}", data={"owed_here": debts["owed_here"]}, link="desk")

    briefs_total_seg = Segment(
        "briefs_total", show=True, value=f"briefs {b_total}",
        data={"briefs": b_total}, link="desk")
    briefs_mine_seg = Segment(
        "briefs_mine", show=b_mine > 0, value=f"briefs {b_mine}",
        data={"briefs": b_mine}, link="desk")

    wakes_seg = Segment("wakes", show=True, value=f"wakes {wakes_n}/h",
                        data={"wakes": wakes_n}, link="wakes")

    # dm lights the segment BY ITSELF (the Alfred chain, 2026-07-19: seven DMs waiting, mail 0,
    # flight 0 — a render condition that forgot `dm` rendered a dim "mail 0" over live traffic).
    mail_severity: Severity = (
        "alarm" if split["dm"] else "amber" if flight_n else "ok")
    mail_seg = Segment(
        "mail", show=True, severity=mail_severity, value=f"mail {split['mail']}",
        data={"mail": split["mail"], "flight": flight_n, "dm": split["dm"]}, link="conversations")

    sensing_seg = Segment(
        "sensing", show=bool(sick), severity="alarm" if sick else "ok",
        value=f"not sensing: {','.join(sick[:2])}" if sick else "sensing",
        data={"sick": sick}, link="fleet")

    if not metered:
        spend_seg = Segment("spend", show=False, data={"metered": False})
    else:
        severity: Severity = (
            "alarm" if blind else
            "red" if (cap > 0 and spent >= 0.85 * cap) else
            "amber" if (cap > 0 and spent >= 0.6 * cap) else
            "ok")
        spend_value = "$∞" if unlimited else f"${spent:.2f}/${cap:.0f}"
        spend_seg = Segment(
            "spend", show=(severity != "ok"), severity=severity, value=spend_value,
            note=f"{blind} unpriced" if blind else None,
            data={"spent": spent, "cap": cap, "blind": blind, "unlimited": unlimited,
                 "metered": True},
            link="desk")

    return Segments(
        live=live_seg, owed=owed_seg, owed_here=owed_here_seg,
        briefs_total=briefs_total_seg, briefs_mine=briefs_mine_seg,
        wakes=wakes_seg, mail=mail_seg, sensing=sensing_seg, spend=spend_seg)
