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
