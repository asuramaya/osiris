"""Fleet tree render — grouped by project, live expanded, history collapsed.

The roster is event-sourced: every retired session stays a root forever, so the flat tree
grows into a wall of ○ lineage noise (the operator: "most are duplicates in the same dir —
collect by root dir"). The render answers with GROUPING, never merging (an identity merge is
review-gated, always): each project is a section, LIVE agents (and anything holding a live
descendant) render fully, and the retired collapse into one counted line with the freshest
id. `full=True` keeps the collapse off — the wall, but grouped and sorted.

Pure — the MCP fleet() tool feeds it rows; tests feed it fixtures.
"""
from __future__ import annotations

from collections import Counter
from typing import Any

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
    is claimed (dd47c1da: "fleet() must print claimed names"). An anonymous agent (no `seat`
    on its node) renders exactly as before: the id, alone."""
    seat = nodes[canon].get("seat")
    return f"{canon} ({seat})" if seat else canon


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


def _render_expanded(
    canon: str, indent: int, nodes: dict[str, Node], kids: dict[str | None, list[str]],
    lines: list[str], *, full: bool,
) -> None:
    n = nodes[canon]
    prefix = "  " + "    " * indent + ("└─ " if indent else "")
    mark = "●" if n.get("live") else "○"
    lines.append(f"{prefix}{mark} {_id_label(canon, nodes)}  {_short(n.get('model'))}".rstrip())
    children = kids.get(canon, [])
    if not children:
        return
    expand = [c for c in children if full or _any_live(c, nodes, kids)]
    fold = [c for c in children if c not in expand]
    for c in _sort_roots(expand, nodes):
        _render_expanded(c, indent + 1, nodes, kids, lines, full=full)
    if fold:
        folded = [d for c in fold for d in _subtree(c, kids)]
        pad = "  " + "    " * (indent + 1) + "└─ "
        lines.append(f"{pad}○ swarm: {len(folded)} retired ({_tally(folded, nodes)})")


def render_fleet_tree(
    nodes: dict[str, Node], *, full: bool = False, os_bodies: dict[str, int] | None = None,
) -> str:
    """The glanceable fleet: one section per project, live expanded, retired collapsed.

    `os_bodies` (heinrich's ghost-seat filing, thread 1fe6811c) is ADDITIVE and OPTIONAL: when
    given, a project's line grows the OS-truth count beside its graph-belief `live` count, and
    a `ghost` note when the graph claims more live than any real process backs — the gap made
    visible, never a change to what `live` itself means (still `_any_live`, untouched)."""
    kids = _children_of(nodes)
    roots = kids.get(None, [])
    groups: dict[str, list[str]] = {}
    for r in roots:
        groups.setdefault(nodes[r].get("project") or "?", []).append(r)
    lines: list[str] = []
    for project in sorted(groups):
        proj_roots = _sort_roots(groups[project], nodes)
        live_n = sum(1 for r in proj_roots if _any_live(r, nodes, kids))
        swarm_n = sum(len(_subtree(r, kids)) - 1 for r in proj_roots)
        head = f"▸ {project} — {live_n} live · {len(proj_roots)} sessions"
        if swarm_n:
            head += f" · swarm {swarm_n}"
        if os_bodies is not None:
            bodies = os_bodies.get(project, 0)
            head += f" · {bodies} os {'body' if bodies == 1 else 'bodies'}"
            gap = live_n - bodies
            if gap > 0:
                head += f" · ⚠ {gap} ghost{'s' if gap != 1 else ''}"
        lines.append(head)
        expand = [r for r in proj_roots if full or _any_live(r, nodes, kids)]
        fold = [r for r in proj_roots if r not in expand]
        for r in expand:
            _render_expanded(r, 0, nodes, kids, lines, full=full)
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
            past = f"  ○ {len(fold)} past session{'s' if len(fold) != 1 else ''}"
            if signed:
                past += f" · {signed} retired"
            lines.append(f"{past}{note}")
    return "\n".join(lines)
