"""THE READ TRIANGLE, WAVE 1 (thread 68f1bafa, Thoth DM 7883): server-rendered plain text
for read verbs, so a slash command prints one string verbatim instead of the model
receiving JSON and re-prettifying it at token cost (operator's own complaint, 2026-09-07:
"that cost tokens, it barfed it out into the model, then the model tries to prettify it").

WAVE 1 SCOPE: only `get_status`'s render='text' ships this pass — a single generic
line-per-field renderer, deliberately NOT a hand-tuned layout per verb (that is real
design work per verb: backlog's bands, threads' one-liners, roster's occupancy glyphs,
mail's fyi-folding, team's live/owe/envelope split). Reusing this same generic renderer
for those verbs without designing their own compact shape would just move the JSON-dump
problem one layer down. Scoped deliberately, not an oversight — see decision recorded
alongside this file's own introduction, and follow-up thread for the remaining five verbs
+ slash faces + console faces.
"""
from __future__ import annotations

import json
from typing import Any

_STATUS_FIELD_ORDER = ("you", "project", "model", "seat", "mail")


def render_status_text(result: dict[str, Any]) -> str:
    """One line per field, ordered fields first, then whatever else is present (e.g.
    `fleet_pulse`, a vacant-seats note) — never drops a key silently, same rule
    cli_render.py holds itself to. Nested values render as compact (no-space) JSON on
    their own line rather than being expanded, since this is a glance, not a dump."""
    lines: list[str] = []
    seen: set[str] = set()
    for key in _STATUS_FIELD_ORDER:
        if key in result:
            lines.append(_render_line(key, result[key]))
            seen.add(key)
    for key, value in result.items():
        if key in seen:
            continue
        lines.append(_render_line(key, value))
    return "\n".join(lines)


def _render_line(key: str, value: Any) -> str:
    if isinstance(value, (dict, list)):
        return f"{key}: {json.dumps(value, separators=(',', ':'))}"
    return f"{key}: {value}"


BACKLOG_BAND_CAP = 20


def render_backlog_text(rows: list[dict[str, Any]]) -> str:
    """One line per project, already ordered by the caller (backlog()'s own sort: the
    caller's own project first, then past-window projects, then by open count) — this
    function only CAPS and FORMATS, never reorders. Caps at `BACKLOG_BAND_CAP`, the
    remainder folded into one trailing count line rather than silently dropped."""
    shown, remainder = rows[:BACKLOG_BAND_CAP], max(0, len(rows) - BACKLOG_BAND_CAP)
    if not shown:
        return "backlog: no project carries an open obligation"
    lines = [_render_backlog_row(r) for r in shown]
    if remainder:
        lines.append(f"+{remainder} more project(s)")
    return "\n".join(lines)


def _render_backlog_row(row: dict[str, Any]) -> str:
    target = f"/{row['target']}" if row.get("target") is not None else ""
    past = f" [{row['past_window']} past window]" if row.get("past_window") else ""
    owners = ", ".join(row.get("oldest_owners") or [])
    return f"{row['project']}: {row['open']}{target} open{past} — oldest: {owners}"


def render_obligation_backlog_text(result: dict[str, Any]) -> str:
    """THE BACKLOG BAND, `--fleet` view (thread 8608): one line per carrying seat instead
    of `render_backlog_text`'s per-project rows — `backlog(fleet=True)`'s own shape, so
    the crunch is visible per body without a hand-run query. Same cap/remainder
    convention as `render_backlog_text` (`BACKLOG_BAND_CAP`), plus a trailing line for
    `unowned`/`literal_owner` (both hidden at zero — a clean fleet with nothing to
    reassign should read that way, not carry two zero-lines forever)."""
    rows = result.get("by_seat") or []
    shown, remainder = rows[:BACKLOG_BAND_CAP], max(0, len(rows) - BACKLOG_BAND_CAP)
    lines = [f"fleet total: {result.get('fleet_total', 0)} open"]
    if shown:
        lines.extend(_render_seat_backlog_row(r) for r in shown)
    else:
        lines.append("  no seat carries an open obligation")
    if remainder:
        lines.append(f"+{remainder} more seat(s)")
    tail = []
    if result.get("unowned"):
        tail.append(f"unowned: {result['unowned']}")
    if result.get("literal_owner"):
        tail.append(f"literal owner (no matching seat): {result['literal_owner']}")
    if tail:
        lines.append(" — ".join(tail))
    return "\n".join(lines)


def _render_seat_backlog_row(row: dict[str, Any]) -> str:
    past = f" [{row['past_window']} past window]" if row.get("past_window") else ""
    oldest = ", ".join(o["id"] for o in (row.get("oldest") or []))
    return f"  {row['seat']}: {row['open']} open{past} — oldest: {oldest}"


_OCCUPANCY_GLYPH = {"occupied": "●", "cold": "○", "vacant": "·"}


def render_roster_text(rows: list[dict[str, Any]]) -> str:
    """One line per seat, grouped by house (a blank line between houses), an occupancy
    glyph (thread 68f1bafa's own ask: "roster (house-scoped)") -- ● occupied, ○ cold
    (held, nobody live this instant -- NOT vacant), · vacant (never held). No cap: a
    fleet's seat count is bounded by the fleet itself, not an open-ended query.

    `manager` (operator ruling, thread d575e68c) prints as a trailing `-> <handle>` suffix
    rather than restructuring this house-grouped flat list into a tree indented under each
    manager's own line: this render is already a flat sorted-by-handle list within a house
    block, and a manager can sit in a DIFFERENT house than its worker (a coordinator
    governing across houses is the normal `governed` shape roster() itself documents), so
    grouping by manager would either fight the existing house grouping or require a second
    axis of nesting for a field that's usually absent. A suffix costs one line-format branch
    and is silent (no extra line, no restructuring) when the seat is unmanaged."""
    if not rows:
        return "roster: no active seats"
    by_house: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        by_house.setdefault(r.get("house") or "(no house)", []).append(r)
    blocks = []
    for house in sorted(by_house):
        lines = [f"{house}:"]
        for r in sorted(by_house[house], key=lambda r: r["handle"] or ""):
            glyph = _OCCUPANCY_GLYPH.get(r["occupancy"], "?")
            holder = f" ({r['holder']})" if r.get("holder") else ""
            governs = ", ".join(r.get("chartered_repos_display") or r.get("chartered_repos") or [])
            tail = f" — governs: {governs}" if governs else ""
            manager = f" -> {r['manager']}" if r.get("manager") else ""
            lines.append(f"  {glyph} {r['handle']}{holder}{tail}{manager}")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


def render_mail_text(messages: list[dict[str, Any]]) -> str:
    """One line per ASK message (needs a reply/ack), FYI messages folded into a single
    trailing count line rather than itemized (thread 68f1bafa's own "mail (fyi folded to
    one line)" spec) — an inbox full of fyi noise must never bury the handful of asks
    that actually need a decision."""
    if not messages:
        return "mail: empty"
    asks = [m for m in messages if m.get("grade") == "ask"]
    fyi = [m for m in messages if m.get("grade") != "ask"]
    lines = [_render_mail_row(m) for m in asks]
    if fyi:
        lines.append(f"{len(fyi)} fyi message(s) — ack to settle")
    if not lines:
        return "mail: empty"
    return "\n".join(lines)


def _render_mail_row(m: dict[str, Any]) -> str:
    thread = m.get("thread")
    snippet = (m.get("body") or "")[:100]
    return f"{m.get('id')} ask from:{m.get('from')} thread:{thread} — {snippet}"


def render_desk_text(desk: dict[str, Any], *, backlog_text: str | None = None) -> str:
    """The operator desk (thread 68f1bafa's own "/desk with the backlog band first and
    briefs collapsed to one count line"): the backlog view first (all-projects debt
    pressure, when given), then owed/letters headline, then needs_decision/needs_hands/
    fyi/dimmed/miner_guesses each folded to ONE COUNT LINE (never itemized -- a card's
    real text is only in the structured receipt, since settling by id needs the ids the
    collapsed view deliberately drops), then `your_queue` itemized one line per thread
    (the canonical debt list, not a "brief" -- kept legible, not collapsed)."""
    lines: list[str] = []
    if backlog_text:
        lines.append(backlog_text)
        lines.append("")
    lines.append(f"owed: {desk.get('owed', 0)}  letters: {desk.get('letters', 0)}")
    for key, label in (("needs_decision", "needs decision"), ("needs_hands", "needs hands"),
                       ("fyi", "fyi")):
        n = len(desk.get(key) or [])
        if n:
            lines.append(f"{label}: {n}")
    dimmed = desk.get("dimmed") or []
    if dimmed:
        lines.append(f"dimmed: {len(dimmed)}")
    guesses = (desk.get("miner_guesses") or {}).get("threads") or []
    if guesses:
        lines.append(f"miner_guesses: {len(guesses)} (not counted in owed)")
    proposals = desk.get("proposals") or {}
    if proposals.get("count"):
        lines.append(f"proposals: {proposals['count']} (miners as last resort, "
                     "accept/reject from the owning seat's own tab)")
    queue = (desk.get("your_queue") or {}).get("threads") or []
    if queue:
        lines.append("your_queue:")
        for t in queue:
            lines.append(f"  {t.get('id')} — {t.get('summary')}")
    if len(lines) == 1:  # only the owed/letters headline, nothing else at all
        lines.append("desk clear")
    return "\n".join(lines)


def render_team_text(rows: list[dict[str, Any]]) -> str:
    """One line per managed seat: live glyph, owe (stale flagged separately when nonzero),
    envelope (unread asks for that seat's current holder). No cap: a manager's own team is
    bounded by who they manage, not an open-ended query."""
    if not rows:
        return "team: manages no seats"
    lines = []
    for r in rows:
        glyph = "●" if r.get("live") else "○"
        owe = f"owe {r['owe']}" + (f" ({r['stale']} stale)" if r.get("stale") else "")
        lines.append(f"{glyph} {r['handle']}: {owe}, envelope {r['envelope']}")
    return "\n".join(lines)


THREADS_BAND_CAP = 30


def render_threads_text(rows: list[dict[str, Any]]) -> str:
    """One line per thread, already ordered by the caller (threads()'s own oldest-first
    query) — caps and formats only, never reorders. Caps at `THREADS_BAND_CAP`, the
    remainder folded into one trailing count line."""
    shown, remainder = rows[:THREADS_BAND_CAP], max(0, len(rows) - THREADS_BAND_CAP)
    if not shown:
        return "threads: none open in your name here"
    lines = [f"{r['id']} [{r['kind'] or '?'}] {r['summary']}" for r in shown]
    if remainder:
        lines.append(f"+{remainder} more thread(s)")
    return "\n".join(lines)
