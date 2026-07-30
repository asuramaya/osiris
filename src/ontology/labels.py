"""Label resolution — task #97 workstream 3 (ruling 52daab71): ONE canonical answer to
"what text represents this object," replacing seven independently-written, disagreeing
implementations found across the codebase in one survey (app.py's `_OBJ_LABEL` SQL
macro, compositions.py's `object_items`, app.py's `_object_card`, dossier.py's
`entity_dossier`, osiris.js's `objectDetail`, chrome.py's `_comp_label`, and search's
total absence of one — the root of the reported `practice:b1eb7520e783` bug, a hash
where a sentence belongs).

THREE TIERS, always in this order: (1) RULE — the object's declared ObjectType names a
`label_field`/`subtitle_field` (ontology/schema.py) and this row has that property set;
(2) CHAIN — the universal fallback below, first populated property wins, because a
declared field can be null on any single row and an undeclared type needs SOMETHING;
(3) CANONICAL — the object's own id-shaped string, the last resort everyone already
recognizes as "we don't have a real name for this" (and the signal Seshat's accretion/
gap-surface workstream reads: a `source == "canonical"` result names a real gap).

The chain's own member choice is not arbitrary: `statement` and `surface` are there
because Practice/Superstition and BlindSpot respectively have NO name/title/summary/
subject property at all (confirmed against capture.py's own record_practice/
record_blind_spot) — without those two members, every Practice and BlindSpot object
falls straight to canonical, which IS the reported bug. Do not reorder without
re-opening ruling 52daab71 — Thoth's own text names this exact ordering.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from src.ontology.schema import object_type

LABEL_CHAIN: tuple[str, ...] = ("name", "title", "summary", "statement", "surface", "handle")


@dataclass(frozen=True)
class LabelResult:
    """One object's resolved display identity. `source` names which tier won —
    'rule' (the type's own declared label_field), 'chain' (the universal fallback), or
    'canonical' (nothing else was populated). `field` is the property name that won,
    or None for 'canonical'."""

    label: str
    subtitle: str | None
    source: str  # 'rule' | 'chain' | 'canonical'
    field: str | None


def _first_truthy(props: dict[str, Any], names: tuple[str, ...]) -> tuple[str, str] | None:
    for n in names:
        v = props.get(n)
        if v is not None and str(v).strip():
            return n, str(v)
    return None


def resolve_label(object_type_name: str, props: dict[str, Any], canonical: str) -> LabelResult:
    """The one place that decides what text represents an object. `props` is a flat
    dict of this object's CURRENT winning property values (already resolved by
    confidence/recency — winning_props/current_assertions), not raw assertion rows.
    Never raises — an object with nothing resolvable gets its canonical, same as today,
    just now via a single declared path instead of six accidental ones."""
    t = object_type(object_type_name)
    subtitle: str | None = None
    if t.subtitle_field:
        sub = props.get(t.subtitle_field)
        if sub is not None and str(sub).strip():
            subtitle = str(sub)

    if t.label_field:
        rule_hit = props.get(t.label_field)
        if rule_hit is not None and str(rule_hit).strip():
            return LabelResult(str(rule_hit), subtitle, "rule", t.label_field)

    chain_hit = _first_truthy(props, LABEL_CHAIN)
    if chain_hit:
        field, value = chain_hit
        return LabelResult(value, subtitle, "chain", field)

    return LabelResult(canonical, subtitle, "canonical", None)


def _common_prefix(strings: list[str]) -> str:
    if not strings:
        return ""
    shortest = min(strings, key=len)
    for i, ch in enumerate(shortest):
        if any(s[i] != ch for s in strings):
            return shortest[:i]
    return shortest


_DEFAULT_WIDTH = 40


def _truncate(s: str, width: int) -> str:
    """Word-boundary truncation (Thoth's requirement #1 — length): never cut mid-word
    when a space is available to break on instead."""
    if len(s) <= width:
        return s
    cut = s.rfind(" ", 0, width)
    return (s[:cut] if cut > 0 else s[:width]).rstrip() + "…"


def disambiguate_labels(items: list[tuple[str, str, str]],
                        width: int = _DEFAULT_WIDTH) -> dict[str, str]:
    """Given (id, label, canonical) triples, return {id: display_label} — each already
    truncated to `width` AND disambiguated against its siblings in THIS set. The live
    bug this fixes: three SoftwareProject rows whose full labels differ
    ("…REPOS/coinbase-onchain" vs "-agent" vs "-web") but are textually IDENTICAL for
    their first `width` characters, so a naive truncation collapses all three to
    "/home/asuramaya/code/REPOS/c…" — collision happens at the DISPLAY width, not
    necessarily on the full string, so grouping must key on the truncated prefix, not
    on full-string equality.

    For a colliding group: strip the longest common PREFIX across the group's FULL
    (untruncated) labels and show what's left, prefixed with an ellipsis — the
    differing part is, by construction, whatever remains after the shared prefix.
    Falls back to appending each object's own short canonical suffix only if the
    stripped tails STILL collide (genuine duplicates) — two distinct objects must
    never render as visually identical.

    This is a LIST-level operation (unlike resolve_label, which knows nothing of its
    object's siblings) — call it over whatever set is about to render together in one
    view (a table page, the sidebar's current filter), not the whole graph at once."""
    groups: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    for iid, label, canonical in items:
        groups[label[:width]].append((iid, label, canonical))

    out: dict[str, str] = {}
    for group in groups.values():
        if len(group) == 1:
            iid, lbl, _ = group[0]
            out[iid] = _truncate(lbl, width)
            continue
        prefix = _common_prefix([lbl for _, lbl, _ in group])
        tails = {iid: (f"…{lbl[len(prefix):]}" if len(prefix) < len(lbl) else lbl)
                 for iid, lbl, _ in group}
        tail_counts: dict[str, int] = defaultdict(int)
        for t in tails.values():
            tail_counts[t] += 1
        for iid, _lbl, canonical in group:
            t = _truncate(tails[iid], width)
            out[iid] = t if tail_counts[tails[iid]] == 1 else f"{t} ({canonical[-8:]})"
    return out
