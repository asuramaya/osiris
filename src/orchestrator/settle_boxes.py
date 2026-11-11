"""The two pure box verdicts, in a module that imports nothing: the Stop hook (a stdlib-only
script that runs on every turn end) and /settle both read them, and importing settle.py for a
two-line function dragged asyncpg into the hook (about 100 ms a Stop past the self-compact
threshold). settle.py re-exports both names, so every existing import keeps working."""
from __future__ import annotations


def missing_boxes(boxes: dict[str, bool | None]) -> list[str]:
    """The labels of any box explicitly False, never None (fog-of-war, could not be
    evaluated) or True (satisfied). Pure; the one line both the hook's verdict and
    /settle's own confirm step share instead of each re-deriving it."""
    return [label for label, ok in boxes.items() if ok is False]


def unevaluated_boxes(boxes: dict[str, bool | None]) -> list[str]:
    """The labels of any box that is None, COULD NOT BE EVALUATED, a DIFFERENT thing
    from missing (False) or satisfied (True), and deliberately never folded into either:
    a check that silently never runs and a check that genuinely ran and found nothing
    satisfied must never collapse onto the same signal, the exact class of defect that let
    an earlier standing_orders_touched cwd bug hide behind `complete:true` for eleven days
    of a real, untouched charter.md. Fog-of-war stays non-blocking (a box that is
    structurally inapplicable, e.g. an unseated session with no standing-orders file to
    check, must not permanently red every settle it will ever call), but it must never
    again be invisible: a caller SEES this list, distinctly from `missing_boxes`, whether
    or not it changes what they decide to do about it."""
    return [label for label, ok in boxes.items() if ok is None]
