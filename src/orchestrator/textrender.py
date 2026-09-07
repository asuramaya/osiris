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


_OCCUPANCY_GLYPH = {"occupied": "●", "cold": "○", "vacant": "·"}


def render_roster_text(rows: list[dict[str, Any]]) -> str:
    """One line per seat, grouped by house (a blank line between houses), an occupancy
    glyph (thread 68f1bafa's own ask: "roster (house-scoped)") -- ● occupied, ○ cold
    (held, nobody live this instant -- NOT vacant), · vacant (never held). No cap: a
    fleet's seat count is bounded by the fleet itself, not an open-ended query."""
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
            governs = ", ".join(r.get("chartered_repos") or [])
            tail = f" — governs: {governs}" if governs else ""
            lines.append(f"  {glyph} {r['handle']}{holder}{tail}")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


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
