"""TERMINAL RENDERING FOR THE osiris CLI — the human's half of every read verb.

WHY THIS EXISTS (operator, 2026-08-28): four CLI commands answered a human by printing
`json.dumps(result, indent=2)` — illegible to a person and, at indent=2, needlessly
expensive for an agent reading the same output. Both audiences were being served badly by
the same line. This module splits them: a human gets an aligned, coloured render; a machine
gets `--json`, and that json is now COMPACT (one line, no indent), which is strictly fewer
tokens than what it replaced. Nobody is asked to read the other's format.

THE ONE RULE HERE: this module NEVER decides what is true, only how it looks. It does no
lookups, holds no policy, and must never drop a key a caller handed it — a renderer that
silently omits a field is the same disease as a summary that quotes a stale count. When it
cannot type a value it prints it verbatim rather than guessing.

DEGRADES BY DESIGN, in this order: no TTY (a pipe, a CI log, an agent's subprocess) or
NO_COLOR set or TERM=dumb -> every escape sequence vanishes and the layout stays byte-clean
ASCII-plus-box-drawing. Width comes from the real terminal when there is one and falls back
to 80. So `osiris roster | tee log` is readable and `osiris roster` is pretty, from one path.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
from typing import Any, TextIO

# --- the palette -------------------------------------------------------------------------

_RESET = "\x1b[0m"
_CODES = {
    "dim": "\x1b[2m",
    "bold": "\x1b[1m",
    "red": "\x1b[31m",
    "green": "\x1b[32m",
    "yellow": "\x1b[33m",
    "blue": "\x1b[34m",
    "magenta": "\x1b[35m",
    "cyan": "\x1b[36m",
}

# Box drawing. Kept in one place so a future --ascii switch has a single seam to flip.
_H, _V = "─", "│"
_TL, _TR, _BL, _BR = "╭", "╮", "╰", "╯"


def supports_color(stream: TextIO | None = None) -> bool:
    """NO_COLOR (the informal standard) and TERM=dumb both win over everything else; then a
    real tty is required. FORCE_COLOR overrides the tty test alone — it exists so a test, or
    a human piping into `less -R`, can still get colour on purpose."""
    if os.environ.get("NO_COLOR") is not None:
        return False
    if os.environ.get("TERM") == "dumb":
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    stream = stream or sys.stdout
    try:
        return bool(stream.isatty())
    except (AttributeError, ValueError):
        return False


def width(stream: TextIO | None = None, *, default: int = 80) -> int:
    """Real terminal width, clamped to something a table can actually use. COLUMNS is
    honoured (tests and `watch` both set it) before asking the OS."""
    env = os.environ.get("COLUMNS")
    if env and env.isdigit():
        return max(40, min(200, int(env)))
    if not supports_color(stream):
        return default
    try:
        return max(40, min(200, shutil.get_terminal_size((default, 24)).columns))
    except OSError:
        return default


class Paint:
    """Colour that knows whether it is allowed to speak. Every method is a no-op returning
    the bare string when colour is off, so callers never branch on it themselves."""

    def __init__(self, enabled: bool) -> None:
        self.enabled = enabled

    def _wrap(self, text: str, *codes: str) -> str:
        if not self.enabled or not text:
            return text
        prefix = "".join(_CODES[c] for c in codes if c in _CODES)
        return f"{prefix}{text}{_RESET}" if prefix else text

    def dim(self, t: str) -> str:
        return self._wrap(t, "dim")

    def bold(self, t: str) -> str:
        return self._wrap(t, "bold")

    def good(self, t: str) -> str:
        return self._wrap(t, "green")

    def warn(self, t: str) -> str:
        return self._wrap(t, "yellow")

    def bad(self, t: str) -> str:
        return self._wrap(t, "red")

    def key(self, t: str) -> str:
        return self._wrap(t, "cyan")

    def title(self, t: str) -> str:
        return self._wrap(t, "bold", "magenta")


# --- value formatting --------------------------------------------------------------------

# Words that carry a verdict. Matched on the WHOLE lowercased value, never a substring: an
# id or a path that merely contains "ok" is not a status, and colouring it would be a lie.
_GOOD = {"ok", "true", "yes", "green", "clean", "up", "live", "healthy", "pass", "passed",
         "resolved", "complete", "coherent", "active", "delivered", "nudged", "resumable"}
_BAD = {"error", "false", "fail", "failed", "down", "dead", "refused", "missing", "stale",
        "conflict", "unreachable", "broken"}
_WARN = {"warn", "warning", "unknown", "pending", "queued", "held", "partial", "degraded",
         "undetermined", "skipped", "none"}


def _is_scalar(v: Any) -> bool:
    return v is None or isinstance(v, (str, int, float, bool))


def fmt_value(v: Any, paint: Paint) -> str:
    """One scalar, coloured by VERDICT where the value is unambiguously one. Non-scalars are
    handed back compactly rather than expanded — the caller decides whether a nested thing
    deserves its own block."""
    if v is None:
        return paint.dim("—")
    if isinstance(v, bool):
        return paint.good("yes") if v else paint.dim("no")
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        low = v.strip().lower()
        if low in _GOOD:
            return paint.good(v)
        if low in _BAD:
            return paint.bad(v)
        if low in _WARN:
            return paint.warn(v)
        return v
    if isinstance(v, (list, tuple)):
        if not v:
            return paint.dim("(none)")
        if all(_is_scalar(i) for i in v):
            return ", ".join("—" if i is None else str(i) for i in v)
        return paint.dim(f"({len(v)} items)")
    if isinstance(v, dict):
        return paint.dim("(none)") if not v else paint.dim(f"({len(v)} fields)")
    return str(v)


def shorten_path(p: str, limit: int) -> str:
    """A path's IDENTITY LIVES IN ITS TAIL. Twenty seats under ~/.osiris/seats/ are identical
    for the first 30 characters and differ only in the last segment, so head-truncation —
    what a generic truncator does — turns every one of them into the same useless '/home…'.
    Keep the last segments and elide the middle instead. Not applied to non-paths."""
    if len(p) <= limit or "/" not in p:
        return p
    parts = [x for x in p.split("/") if x]
    tail: list[str] = []
    for seg in reversed(parts):
        candidate = "…/" + "/".join([seg, *tail])
        if len(candidate) > limit and tail:
            break
        tail.insert(0, seg)
    out = "…/" + "/".join(tail)
    return out if len(out) <= limit else "…" + p[-(limit - 1):]


def _looks_like_path(v: Any) -> bool:
    return isinstance(v, str) and v.startswith("/") and "/" in v[1:]


def _visible_len(s: str) -> int:
    """Length as the terminal sees it — escape sequences occupy no columns. Without this
    every coloured cell over-pads and the table shears."""
    out, i = 0, 0
    while i < len(s):
        if s[i] == "\x1b":
            j = s.find("m", i)
            if j == -1:
                return out + len(s) - i
            i = j + 1
            continue
        out += 1
        i += 1
    return out


def _pad(s: str, n: int) -> str:
    return s + " " * max(0, n - _visible_len(s))


def _truncate(s: str, n: int) -> str:
    """Truncate on VISIBLE width, and never leave a dangling escape: if we cut inside a
    coloured run we append a reset so the rest of the terminal is not stained."""
    if n <= 0:
        return ""
    if _visible_len(s) <= n:
        return s
    out, shown, i = [], 0, 0
    while i < len(s) and shown < n - 1:
        if s[i] == "\x1b":
            j = s.find("m", i)
            if j == -1:
                break
            out.append(s[i:j + 1])
            i = j + 1
            continue
        out.append(s[i])
        shown += 1
        i += 1
    return "".join(out) + "…" + (_RESET if "\x1b" in s else "")


# --- blocks ------------------------------------------------------------------------------

def heading(title: str, subtitle: str | None, paint: Paint, cols: int) -> list[str]:
    """A titled rule. The subtitle is the place for the caller's own caveat — the scope of a
    count, the window a measurement covers — so it sits beside the number it qualifies."""
    left = f"{_H}{_H} {title} "
    line = left + _H * max(0, cols - _visible_len(left))
    out = [paint.title(line)]
    if subtitle:
        out.append(paint.dim(_truncate(subtitle, cols)))
    return out


def kv_block(pairs: list[tuple[str, Any]], paint: Paint, cols: int,
             *, indent: str = "  ") -> list[str]:
    """Aligned key/value lines. Keys are rendered as written — a caller who wants
    'anchor_cwd' shown as 'anchor cwd' says so; this module does not rewrite anyone's
    vocabulary, because the key IS the field name a reader will grep for."""
    if not pairs:
        return []
    kw = max(_visible_len(str(k)) for k, _ in pairs)
    room = max(10, cols - len(indent) - kw - 2)
    lines = []
    for k, v in pairs:
        # A path keeps its TAIL (see shorten_path); everything else truncates from the right.
        rendered = (shorten_path(v, room) if _looks_like_path(v) else
                    _truncate(fmt_value(v, paint), room))
        lines.append(f"{indent}{_pad(paint.key(str(k)), kw)}  {rendered}")
    return lines


def table(rows: list[dict[str, Any]], paint: Paint, cols: int,
          *, columns: list[str] | None = None, indent: str = "  ") -> list[str]:
    """A real aligned table. Column set is the UNION of every row's keys in first-seen order
    — never the first row's alone, or a row with an extra field would lose it silently.
    Widths are content-driven, then shrunk proportionally if the terminal is too narrow;
    the leftmost column is protected last because it is almost always the identifier."""
    if not rows:
        return [f"{indent}{paint.dim('(no rows)')}"]
    if columns is None:
        columns = []
        for r in rows:
            for k in r:
                if k not in columns:
                    columns.append(k)
    if not columns:
        return [f"{indent}{paint.dim('(no columns)')}"]

    # MIN_CELL is the width below which a column stops carrying information — "seat:…" tells
    # a reader nothing. If honouring it for every column overflows the terminal, a table is
    # simply the WRONG SHAPE for this data and we render records instead of shredding it.
    # Deciding that here, once, is why no caller has to guess whether its result "fits".
    MIN_CELL = 10
    budget = cols - len(indent) - 2 * (len(columns) - 1)
    if budget < MIN_CELL * len(columns):
        return records(rows, columns, paint, cols, indent=indent)

    cells = [[fmt_value(r.get(c), paint) for c in columns] for r in rows]
    widths = [max(_visible_len(str(c)), *(_visible_len(row[i]) for row in cells))
              for i, c in enumerate(columns)]

    # Shrink the WIDEST column each pass (never below MIN_CELL) so one long path cannot
    # starve eleven short, fully-readable fields — the failure the equal-shrink version had.
    while sum(widths) > budget and max(widths) > MIN_CELL:
        widths[widths.index(max(widths))] -= 1

    head = "  ".join(_pad(paint.bold(str(c)), w) for c, w in zip(columns, widths, strict=True))
    rule = "  ".join(_H * w for w in widths)
    lines = [f"{indent}{head}", f"{indent}{paint.dim(rule)}"]
    for raw, row in zip(rows, cells, strict=True):
        out = []
        for col, cell, w in zip(columns, row, widths, strict=True):
            if _looks_like_path(raw.get(col)):
                cell = shorten_path(str(raw[col]), w)
            out.append(_pad(_truncate(cell, w), w))
        lines.append(f"{indent}" + "  ".join(out))
    return lines


def records(rows: list[dict[str, Any]], columns: list[str], paint: Paint, cols: int,
            *, indent: str = "  ") -> list[str]:
    """ONE BLOCK PER ROW — the shape a table takes when it has more columns than the terminal
    has room for. Nothing is dropped; the same fields simply run down the page instead of
    across it, so every value stays readable at full width. Empty fields are omitted PER
    RECORD (a 12-column result is usually sparse) but the field is never removed from the
    data — `--json` still carries it, and a field that is empty in one record and set in
    another still shows up wherever it is set."""
    lines: list[str] = []
    for i, row in enumerate(rows):
        if i:
            lines.append("")
        label = next((str(row[k]) for k in ("handle", "name", "seat", "id", "canonical")
                      if row.get(k)), f"[{i}]")
        lines.append(f"{indent}{paint.bold(label)}")
        pairs = [(k, row.get(k)) for k in columns
                 if k != "handle" and row.get(k) not in (None, "", [], {})]
        lines += kv_block(pairs, paint, cols, indent=indent + "  ")
    return lines


def _looks_like_table(v: Any) -> bool:
    """A list of dicts that share a shape is a table; anything else is not. Requires >=2 keys
    so a list of single-key wrappers renders as a plain list instead of a one-column table."""
    if not isinstance(v, list) or len(v) == 0:
        return False
    if not all(isinstance(i, dict) for i in v):
        return False
    return max(len(i) for i in v) >= 2


def render(data: Any, paint: Paint, cols: int, *, title: str | None = None,
           depth: int = 0) -> list[str]:
    """THE GENERIC FALLBACK — what replaces json.dumps for an arbitrary verb result.

    Scalars are gathered into one aligned kv block at the top (the summary a human wants
    first), then each nested structure gets its own titled section: a list-of-dicts becomes a
    table, anything deeper recurses. EVERY KEY THE CALLER PASSED IS RENDERED — this never
    filters, so a verb that grows a field gets it on screen without touching this file."""
    lines: list[str] = []
    if title:
        lines += heading(title, None, paint, cols)

    if _is_scalar(data):
        return lines + [f"  {fmt_value(data, paint)}"]

    if isinstance(data, list):
        if _looks_like_table(data):
            return lines + table(data, paint, cols)
        for i, item in enumerate(data):
            if _is_scalar(item):
                lines.append(f"  {fmt_value(item, paint)}")
            else:
                lines += render(item, paint, cols, title=f"[{i}]", depth=depth + 1)
        return lines

    if not isinstance(data, dict):
        return lines + [f"  {data}"]

    # Split by KEY, never by value: two fields holding equal values must not collapse into
    # one bucket. An EMPTY list/dict is summarised inline ("(none)") rather than given its
    # own empty section — an empty section reads as missing data when it means "measured,
    # and it is zero", and those are different facts.
    scalar_keys = [k for k, v in data.items()
                   if _is_scalar(v) or (isinstance(v, (list, dict)) and not v)]
    scalars = [(k, data[k]) for k in scalar_keys]
    nested = [(k, v) for k, v in data.items() if k not in set(scalar_keys)]

    if scalars:
        lines += kv_block(scalars, paint, cols)
    for k, v in nested:
        if isinstance(v, list) and all(_is_scalar(i) for i in v):
            lines.append("")
            lines += kv_block([(k, v)], paint, cols)
            continue
        lines.append("")
        lines += heading(k, None, paint, cols) if depth == 0 else [f"  {paint.key(k)}:"]
        if _looks_like_table(v):
            lines += table(v, paint, cols)
        else:
            lines += render(v, paint, cols, depth=depth + 1)
    return lines


# --- painting the server's OWN text (thread bad45d61, wave 10) ---------------------------
#
# Khnum's fleet render (824de38) re-derives its whole display from the raw `registered`
# rows client-side -- correct for a genuinely structural regrouping, but the live specimen
# (msg 8160) shows the cost: a caller that reads from a capped/sampled field instead of the
# full row set silently under-counts (3 sections where the server tree has 36). The read
# triangle's own text renders (textrender.py: backlog/threads/roster/team) already carry
# their own hand-designed grouping and caps with an explicit remainder count -- there is
# nothing left to re-derive. PAINT INSTEAD: take the server's own already-complete text
# verbatim and add color only, never re-parse it into a different shape. A caller that
# wants machine data still gets the full structured `--json` response, unchanged.

_GLYPH_PAINT = {"●": "good", "○": "dim", "·": "dim"}
_WORD_RE = re.compile(r"[A-Za-z][A-Za-z_-]*")


def _paint_words(text: str, paint: Paint) -> str:
    """Wrap every whole-word match against the SAME verdict vocabulary fmt_value uses
    (_GOOD/_BAD/_WARN), so a status word reads the same color whether it arrived as a
    bare scalar or embedded in the server's own prose line. Matched case-sensitively
    against the lowered word — the word itself is never altered, only wrapped."""
    def _sub(m: re.Match[str]) -> str:
        word = m.group(0)
        low = word.lower()
        if low in _GOOD:
            return paint.good(word)
        if low in _BAD:
            return paint.bad(word)
        if low in _WARN:
            return paint.warn(word)
        return word
    return _WORD_RE.sub(_sub, text)


def paint_text(text: str, paint: Paint) -> str:
    """Colorize the server's own pre-rendered text (a read-triangle verb's `render='text'`
    response) line by line, WITHOUT re-parsing it into rows or re-deriving any grouping —
    the exact discipline this thread exists to name. Three universal shapes, recognized
    across every read-triangle text render:
      - a leading occupancy/liveness glyph (●/○/·) -- colored, never counted or moved
      - a bare 'label:' header line (nothing after the colon) -- bolded, e.g. roster's
        house names
      - everything else -- verdict words painted in place via `_paint_words`
    A line this recognizes none of still prints, verbatim, unstyled — the same "never
    guess" discipline `fmt_value` already holds itself to."""
    if not paint.enabled:
        return text
    out: list[str] = []
    for line in text.split("\n"):
        stripped = line.strip()
        if not stripped:
            out.append(line)
            continue
        indent = line[:len(line) - len(line.lstrip())]
        rest = line[len(indent):]
        if rest[0] in _GLYPH_PAINT and (len(rest) == 1 or rest[1] == " "):
            glyph, tail = rest[0], rest[1:]
            glyph_method = getattr(paint, _GLYPH_PAINT[glyph])
            out.append(f"{indent}{glyph_method(glyph)}{_paint_words(tail, paint)}")
            continue
        if stripped.endswith(":") and stripped != ":":
            out.append(f"{indent}{paint.title(stripped)}")
            continue
        out.append(f"{indent}{_paint_words(rest, paint)}")
    return "\n".join(out)


# --- painting the fleet tree (thread dd937122, wave 11): FOLDED HERE from fleetview.py's own
# paint_fleet_text -- two client-side painters built independently for the same reason
# (color an already-rendered server string, never re-derive it) now live in the one module
# that owns every CLI paint concern. Kept as its OWN function, not merged into paint_text
# above: the fleet tree's line shapes (a "▸ " project header, a ghost-note span, a bare
# "●"/"○" with no glyph-then-space contract) are genuinely different from the read
# triangle's, and forcing one function to recognize both risks the same "guess wrong on an
# edge case" failure paint_text's own docstring explicitly refuses to take. -------------------

# Line shapes this depends on (all produced only by fleetview.render_fleet_tree): a project
# header starts "▸ "; the unfiled summary line starts exactly "▸ unfiled:"; a live node line
# contains "●", any other node/summary line "○"; a ghost note is the parenthesised
# "⚠ N ghost(s) (...)" span inside a header line; a live node MAY carry a trailing "NN%ctx"
# seam reading (added alongside this fold, thread dd937122's own "seam amber and red" ask).
_GHOST_SPAN_RE = re.compile(r"⚠ \d+ ghosts?( \([^)]*\))?")
_GHOST_DETAIL_RE = re.compile(r"\d+ false-live|\d+ unclaimed (?:body|bodies)")
_SEAM_PCT_RE = re.compile(r"\d+%ctx")


def _paint_seam_pct(token: str, paint: Paint) -> str:
    """A live node's own context_pct reading, colored at the SAME two thresholds the stop
    hook itself alarms on -- osiris_seam_whisper_pct (45, the "seam soon" nudge) and
    context_lens.SELF_COMPACT_PCT (70, self-compaction) -- never a third, re-invented
    number. Below the whisper threshold: unstyled, an ordinary reading, not a verdict."""
    from src.config.settings import get_settings
    from src.orchestrator.context_lens import SELF_COMPACT_PCT

    pct = int(token[:-len("%ctx")])
    whisper = get_settings().osiris_seam_whisper_pct
    if pct >= SELF_COMPACT_PCT:
        return paint.bad(token)
    if whisper and pct >= whisper:
        return paint.warn(token)
    return token


def paint_fleet_text(text: str, paint: Paint) -> str:
    """Recolor an ALREADY-RENDERED `render_fleet_tree` string, line by line — project names
    bold, live marks green, the ghost note amber with its false-live/unclaimed-body
    breakdown red inside it, retired/summary marks and the unfiled line dim, a live node's
    own seam percentage (when present) amber/red past the whisper/self-compact thresholds.
    A disabled `paint` (`Paint(enabled=False)`, every one of its methods a no-op) returns
    `text` unchanged — cheap enough to call unconditionally rather than branch around."""
    if not paint.enabled:
        return text
    out: list[str] = []
    for line in text.split("\n"):
        if line.startswith("▸ unfiled:"):
            out.append(paint.dim(line))
            continue
        if line.startswith("▸ "):
            def _detail(m: re.Match[str]) -> str:
                return paint.bad(m.group(0))
            line = _GHOST_SPAN_RE.sub(
                lambda m: paint.warn(_GHOST_DETAIL_RE.sub(_detail, m.group(0))), line)
            name, sep, rest = line[2:].partition(" — ")
            line = f"▸ {paint.bold(name)}{sep}{rest}" if sep else line
        line = _SEAM_PCT_RE.sub(lambda m: _paint_seam_pct(m.group(0), paint), line)
        line = line.replace("●", paint.good("●")).replace("○", paint.dim("○"))
        out.append(line)
    return "\n".join(out)


# --- the one choke point -------------------------------------------------------------------

def emit(data: Any, *, as_json: bool, title: str | None = None,
         stream: TextIO | None = None, text: str | None = None) -> None:
    """EVERY read verb's output goes through here, so the human/machine split is made in
    exactly one place and cannot drift command to command.

    `as_json=True` prints COMPACT json — one line, no indent. That is deliberate and it is
    the agent-facing win: it is strictly fewer tokens than the indent=2 dump this replaces,
    while staying byte-exact data. Human mode never claims to be parseable.

    `text` (thread bad45d61, wave 10): when given AND `as_json` is False, this is a read-
    triangle verb's own SERVER-RENDERED text (`render='text'`) — `paint_text` colorizes it
    IN PLACE, never re-derived from `data` via the generic `render()` reconstruction below.
    This is the fleet-fix's own rule, generalized: a caller that already has a complete,
    hand-designed text shape from the server must paint that, not re-parse `data` (which
    may be capped/sampled for a different purpose) into a second, possibly-inconsistent
    shape. Ignored when `as_json=True` — machine mode is always `data`, byte-exact."""
    stream = stream or sys.stdout
    if as_json:
        print(json.dumps(data, default=str, separators=(",", ":")), file=stream)
        return
    paint = Paint(supports_color(stream))
    if text is not None:
        print(paint_text(text, paint), file=stream)
        return
    cols = width(stream)
    for line in render(data, paint, cols, title=title):
        print(line, file=stream)
