"""Fleet tree render — grouped by project, live expanded, history collapsed.

The roster is event-sourced: every retired session stays a root forever, so the flat tree
grows into a wall of ○ lineage noise (the operator: "most are duplicates in the same dir —
collect by root dir"). The render answers with GROUPING, never merging (an identity merge is
review-gated, always): each project is a section, LIVE agents (and anything holding a live
descendant) render fully, and the retired collapse into one counted line with the freshest
id. `full=True` keeps the collapse off — the wall, but grouped and sorted.

BY THE GRAPH PROJECT, NOT THE RAW SESSION-REGISTRY LABEL (operator ruling f6b758fc, thread
f6b758fc: "not organized by project, color would be huge in the cli"). The raw `project`
label on a node is whatever directory a session happened to launch from — every ad hoc
probe/smoketest/tmp dir stamped its own section, sorted alphabetically, no signal about
which sections held a real body. `resolve_fleet_projects` (agents.py) is the async,
DB-backed half that answers "what does this session's cwd/label actually resolve to as a
graph project" and writes that answer into each node as `resolved_project` (a distinct key,
never a mutation of the raw `project` — the raw label still rides in `registered`/the
unfiled tally). THIS module stays pure and never does that resolution itself: it only
consumes whatever `resolved_project` a caller already computed, falling back to the raw
`project` label (today's grouping, unchanged) for any node that never got one — every
existing caller/test that predates this ruling. A node whose `resolved_project` is
explicitly `None` (present, but false) is UNFILED — collapsed into one trailing
`unfiled: N sessions in M dirs` line, expanded into its own raw-label sections only under
`full=True`.

Ordering (requirement 2 of the ruling): project sections are LIVE-BODIES-FIRST, then by
last activity — never alphabetical. `sorted(groups)` is gone.

Color (requirement 3): an optional `paint` (`cli_render.Paint`) recolors the same text —
project names bold, live green, the ghost note amber, its false-live/unclaimed-body
breakdown red, retired dim. `paint=None` (the MCP `fleet()` tool's own default) renders
byte-identical to today's plain text — `Paint(enabled=False)` makes every call a no-op.

Pure — the MCP fleet() tool feeds it rows; tests feed it fixtures.
"""
from __future__ import annotations

from collections import Counter
from typing import Any

from src.cli_render import Paint

Node = dict[str, Any]  # canonical -> {model, project, parent, live, ts: datetime|None}


def _children_of(nodes: dict[str, Node]) -> dict[str | None, list[str]]:
    out: dict[str | None, list[str]] = {}
    for canon, n in nodes.items():
        parent = n.get("parent")
        out.setdefault(parent if parent in nodes else None, []).append(canon)
    return out


def _subtree(canon: str, kids: dict[str | None, list[str]]) -> list[str]:
    out, stack = [], [canon]
    while stack:
        c = stack.pop()
        out.append(c)
        stack.extend(kids.get(c, []))
    return out


def _any_live(canon: str, nodes: dict[str, Node], kids: dict[str | None, list[str]]) -> bool:
    return any(nodes[c].get("live") for c in _subtree(canon, kids))


def _short(model: str | None) -> str:
    return (model or "?").removeprefix("claude-")


def _id_label(canon: str, nodes: dict[str, Node]) -> str:
    """A canonical id, with its CLAIMED seat beside it — 'agent:c0ffee (Ra V)' — wherever one
    is claimed (dd47c1da: "fleet() must print claimed names"), and its BINDING anchored
    beside that — '(Ra V ⚓seat:ab12cd34)' — wherever the mind actively HOLDS a Seat object
    (Phase B, 5cef856b: the declared identity beside the inferred one). An agent with
    neither renders exactly as before: the id, alone."""
    seat = nodes[canon].get("seat")
    bound = nodes[canon].get("bound")
    tag = " ".join(x for x in (seat, f"⚓{bound}" if bound else None) if x)
    return f"{canon} ({tag})" if tag else canon


def _tally(canons: list[str], nodes: dict[str, Node]) -> str:
    counts = Counter(_short(nodes[c].get("model")) for c in canons)
    return ", ".join(f"{m} ×{n}" for m, n in counts.most_common())


def _latest(canons: list[str], nodes: dict[str, Node]) -> str | None:
    stamped = [(c, nodes[c]["ts"]) for c in canons if nodes[c].get("ts") is not None]
    if not stamped:
        return None
    return max(stamped, key=lambda x: x[1])[0]


def _sort_roots(roots: list[str], nodes: dict[str, Node]) -> list[str]:
    def key(c: str) -> tuple[int, float, str]:
        ts = nodes[c].get("ts")
        # live first, then freshest; datetimes can't compare to None so invert via timestamp
        return (0 if nodes[c].get("live") else 1,
                -(ts.timestamp()) if ts is not None else float("inf"), c)
    return sorted(roots, key=key)


def _group_order_key(
    project: str, roots: list[str], nodes: dict[str, Node], kids: dict[str | None, list[str]],
) -> tuple[int, float, str]:
    """Requirement 2 of ruling f6b758fc: LIVE-BODIES-FIRST, then by last activity — never
    alphabetical. Same live/freshest shape as `_sort_roots`, one level up (over a whole
    project's roots rather than one root's siblings)."""
    live = any(_any_live(r, nodes, kids) for r in roots)
    latest = _latest(roots, nodes)
    ts = nodes[latest]["ts"] if latest else None
    return (0 if live else 1, -(ts.timestamp()) if ts is not None else float("inf"), project)


def _render_expanded(
    canon: str, indent: int, nodes: dict[str, Node], kids: dict[str | None, list[str]],
    lines: list[str], *, full: bool, paint: Paint,
) -> None:
    n = nodes[canon]
    prefix = "  " + "    " * indent + ("└─ " if indent else "")
    live = bool(n.get("live"))
    mark = paint.good("●") if live else (paint.dim("○") if n.get("retired") else "○")
    lines.append(f"{prefix}{mark} {_id_label(canon, nodes)}  {_short(n.get('model'))}".rstrip())
    children = kids.get(canon, [])
    if not children:
        return
    expand = [c for c in children if full or _any_live(c, nodes, kids)]
    fold = [c for c in children if c not in expand]
    for c in _sort_roots(expand, nodes):
        _render_expanded(c, indent + 1, nodes, kids, lines, full=full, paint=paint)
    if fold:
        folded = [d for c in fold for d in _subtree(c, kids)]
        pad = "  " + "    " * (indent + 1) + "└─ "
        lines.append(f"{pad}{paint.dim('○ swarm')}: {len(folded)} retired "
                     f"({_tally(folded, nodes)})")


def render_fleet_tree(
    nodes: dict[str, Node], *, full: bool = False, os_bodies: dict[str, int] | None = None,
    ghost_gap: dict[str, dict[str, list[Any]]] | None = None, paint: Paint | None = None,
) -> str:
    """The glanceable fleet: one section per project, live expanded, retired collapsed.

    `os_bodies` (heinrich's ghost-seat filing, thread 1fe6811c) is ADDITIVE and OPTIONAL: when
    given, a project's line grows the OS-truth count beside its graph-belief `live` count.

    `ghost_gap` (thread #174, 2026-08-18) is the PER-IDENTITY finding fleet() computes — never
    re-derived here as a netted `live_n - bodies` subtraction, which is exactly the bug this
    replaced: a false-LIVE row and a false-DEAD body in the SAME project cancel under
    subtraction (rotten-apple's own specimen — "1 live · 3 bodies" read as clean while carrying
    both). Rendered honestly as however many of each this project actually carries, never a
    net that can hide one behind the other.

    `paint` (`cli_render.Paint`, ruling f6b758fc requirement 3) recolors the SAME text —
    `None` (the MCP `fleet()` tool's own default: "the MCP fleet verb's text render stays
    plain") behaves exactly like `Paint(enabled=False)`, a no-op. The CLI passes a real
    enabled `Paint` for its own client-side render.

    GROUPING is by `resolved_project` (requirement 1) where a caller supplied one — the
    REAL graph project, never the raw session-registry label — falling back to the raw
    `project` label for any node that never got a `resolved_project` (every caller/test
    that predates this ruling: unchanged behavior). A node whose `resolved_project` is
    explicitly `None` is UNFILED: collapsed into one trailing `unfiled: N sessions in M
    dirs` line (M = distinct raw labels/cwds among them), expanded into its own per-label
    sections — same as before this ruling — only under `full=True`."""
    paint = paint or Paint(enabled=False)
    kids = _children_of(nodes)
    roots = kids.get(None, [])
    groups: dict[str, list[str]] = {}
    unfiled: list[str] = []
    for r in roots:
        n = nodes[r]
        if "resolved_project" in n:
            # PRESENT, three-state: a resolved project string groups on it; `None` is an
            # explicit, honest "nothing active claims this" — unfiled.
            resolved = n["resolved_project"]
            if resolved:
                groups.setdefault(str(resolved), []).append(r)
            else:
                unfiled.append(r)
        else:
            # ABSENT: no caller ever resolved this node — fall back to the raw `project`
            # label, exactly today's grouping (every existing caller/test).
            groups.setdefault(n.get("project") or "?", []).append(r)
    if full:
        # "expanded only under --full": an unfiled session still gets its own raw-label
        # section — same grouping this render used before the ruling — rather than the
        # one trailing summary line.
        for r in unfiled:
            groups.setdefault(nodes[r].get("project") or "?", []).append(r)
        unfiled = []
    lines: list[str] = []
    for project in sorted(groups, key=lambda p: _group_order_key(p, groups[p], nodes, kids)):
        proj_roots = _sort_roots(groups[project], nodes)
        live_n = sum(1 for r in proj_roots if _any_live(r, nodes, kids))
        swarm_n = sum(len(_subtree(r, kids)) - 1 for r in proj_roots)
        head = f"▸ {paint.bold(project)} — {live_n} live · {len(proj_roots)} sessions"
        if swarm_n:
            head += f" · swarm {swarm_n}"
        if os_bodies is not None:
            bodies = os_bodies.get(project, 0)
            head += f" · {bodies} os {'body' if bodies == 1 else 'bodies'}"
        if ghost_gap is not None:
            gap = ghost_gap.get(project) or {}
            n_false_live = len(gap.get("false_live") or [])
            n_false_dead = len(gap.get("false_dead") or [])
            total = n_false_live + n_false_dead
            if total:
                bits = []
                if n_false_live:
                    bits.append(paint.bad(f"{n_false_live} false-live"))
                if n_false_dead:
                    noun = "body" if n_false_dead == 1 else "bodies"
                    bits.append(paint.bad(f"{n_false_dead} unclaimed {noun}"))
                ghost_word = paint.warn(f"⚠ {total} ghost{'s' if total != 1 else ''}")
                head += f" · {ghost_word} ({', '.join(bits)})"
        lines.append(head)
        expand = [r for r in proj_roots if full or _any_live(r, nodes, kids)]
        fold = [r for r in proj_roots if r not in expand]
        for r in expand:
            _render_expanded(r, 0, nodes, kids, lines, full=full, paint=paint)
        if fold:
            latest = _latest(fold, nodes)
            note = f" (latest {_id_label(latest, nodes)})" if latest else ""
            # THE GHOSTS (operator, 2026-07-12: "dead agents that were retired or abandoned
            # ungracefully"). This line used to read "N retired sessions" — but `fold` means
            # NOTHING MORE THAN 'not live'. Only 41 of 517 root minds (8%) ever signed a death
            # certificate; the tree was awarding the word to the other 92%, and RETIRED IS NOT A
            # SYNONYM FOR QUIET. It is a deliberate, signed close that the wake trigger is bound
            # to respect — a word with teeth, spent here on minds that merely stopped talking.
            # The trigger reads the real property and was never fooled; only the LENS lied, so
            # only the lens is fixed. A mind cannot reliably confess its own death (the session
            # that dies is the one that cannot write), and nothing here will sign one on its
            # behalf: we say what we observed — it went quiet — and no more.
            signed = sum(1 for r in fold if nodes[r].get("retired"))
            past = f"  {paint.dim('○')} {len(fold)} past session{'s' if len(fold) != 1 else ''}"
            if signed:
                past += paint.dim(f" · {signed} retired")
            lines.append(f"{past}{note}")
    if unfiled:
        n_sessions = len(unfiled)
        # M = distinct raw project labels/cwds among the unresolved sessions — a raw label
        # when the session had one, else its cwd, else the bare `?` when it had neither
        # (never merged together: two DIFFERENT unlabeled dirs are two different dirs).
        dirs = {nodes[r].get("project") or nodes[r].get("cwd") or "?" for r in unfiled}
        m_dirs = len(dirs)
        lines.append(f"▸ {paint.dim('unfiled')}: {n_sessions} "
                     f"session{'s' if n_sessions != 1 else ''} in {m_dirs} "
                     f"dir{'s' if m_dirs != 1 else ''}")
    return "\n".join(lines)
