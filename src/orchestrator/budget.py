"""THE WAIST HAS A WIDTH — a bound on what any tool may hand back.

Osiris grew tools that answer honestly and answer HUGE. `fleet()` shipped 1022 flat rows
(166k chars); `fleet_digest(hours=24)` shipped the fleet's entire lifetime (262k); an
un-mounted `orient()` shipped 292k. Every one of them was CORRECT. Every one of them blew
the caller's context before a single line could be read — and a truth that cannot be
received is not a truth that was told.

Three agents hit this from three directions and each invented a private workaround. That is
the third appearance of one shape, so it is not a pattern — it is a missing primitive.

Two layers, and the distinction matters:

  * THE LENS (per tool) decides what is WORTH sending — the live rows, the windowed rows,
    the counts you walk into. That judgment cannot live here; only the tool knows what its
    caller came for.
  * THE BOUND (this module) is the BACKSTOP — it does not know what matters, so it never
    pretends to. It keeps a lens's failure from becoming the caller's death, and it says so
    out loud.

A cap that hides what it dropped is a lie the reader cannot detect: they see a list, they
believe it is THE list, and they reason off a number that was never whole. So every trim
announces itself, in the result, where the reader already is. NO SILENT CAPS.

This is a lens, never the record: it bounds what is SHOWN and never what is stored.
"""

from __future__ import annotations

import copy
import json
from typing import Any

# ~12k tokens. Set from the real ceiling, not a round number: the fleet_digest that provoked
# this shipped 262k chars, and a well-lensed one lands at 35k of genuine content (a day of the
# fleet's decisions, its swaps, its conversations). The bound must be loose enough that an
# honest answer survives INTACT — a backstop that trims real work is doing the lens's job and
# doing it blindly — and tight enough that a runaway never reaches the caller's context.
BUDGET_CHARS = 48_000
MIN_KEEP = 3      # a trimmed list still has to SHOW you its shape, or it teaches nothing
MAX_STR = 8_000   # a single monstrous string (a render, a blob) is a firehose too

_NOTE = ("TRUNCATED to fit the response budget. What you see is a PREFIX, not the whole — "
         "do NOT read these lists as complete or count off them. Narrow the query (a project, "
         "a window, an id) and ask again for the part you actually need.")


def _size(value: Any) -> int:
    try:
        return len(json.dumps(value, default=str))
    except (TypeError, ValueError):
        return len(str(value))


def _lists(node: Any, path: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], list[Any]]]:
    """Every list in the tree, with the path that reaches it.

    We do NOT descend into a list's own items: trimming the parent already drops whatever
    hangs off them, and a nested trim would report a cut inside rows the caller never sees.
    """
    if isinstance(node, dict):
        found: list[tuple[tuple[str, ...], list[Any]]] = []
        for key, value in node.items():
            found.extend(_lists(value, (*path, str(key))))
        return found
    if isinstance(node, list):
        return [(path, node)]
    return []


def _strings(node: Any, path: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], str]]:
    if isinstance(node, dict):
        found: list[tuple[tuple[str, ...], str]] = []
        for key, value in node.items():
            found.extend(_strings(value, (*path, str(key))))
        return found
    if isinstance(node, str):
        return [(path, node)]
    return []


def _set_at(root: dict[str, Any], path: tuple[str, ...], value: Any) -> None:
    node: Any = root
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value


def fit(result: Any, *, tool: str, budget: int = BUDGET_CHARS) -> Any:
    """Trim `result` until it fits the budget — loudly, largest firehose first.

    Returns the result unchanged when it already fits (the overwhelming case; the cost of
    this check is one serialization). When it does not fit, the biggest list is halved,
    then the next biggest, until it does — so a tool with one runaway stream loses that
    stream's tail and keeps everything else intact, rather than every stream losing its
    middle.

    The trims are reported under `_bounded`, naming each path, what survived, and what the
    whole was. A reader who takes the prefix for the whole has been LIED to; a reader who is
    told 'you are holding 11 of 1022' has merely been bounded.
    """
    if not isinstance(result, dict) or _size(result) <= budget:
        return result

    result = copy.deepcopy(result)  # a lens never mutates what the tool actually computed
    dropped: dict[str, dict[str, int]] = {}

    while _size(result) > budget:
        candidates = [(p, lst) for p, lst in _lists(result) if len(lst) > MIN_KEEP]
        if not candidates:
            break
        path, longest = max(candidates, key=lambda pair: _size(pair[1]))
        keep = max(MIN_KEEP, len(longest) // 2)
        name = ".".join(path)
        dropped.setdefault(name, {"shown": 0, "of": len(longest)})
        dropped[name]["shown"] = keep
        _set_at(result, path, longest[:keep])

    # lists exhausted and still over: some single string is the firehose (a giant render).
    if _size(result) > budget:
        for path, text in sorted(_strings(result), key=lambda pair: -len(pair[1])):
            if _size(result) <= budget or len(text) <= MAX_STR:
                break
            _set_at(result, path, text[:MAX_STR] + "\n… [truncated]")
            name = ".".join(path)
            dropped.setdefault(name, {"shown": MAX_STR, "of": len(text)})

    if dropped:
        result["_bounded"] = {"tool": tool, "note": _NOTE, "dropped": dropped}
    return result
