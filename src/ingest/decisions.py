"""Decision mining — the project's WHY, derived from its own commit rationale.

`gitlog` says it outright: *each commit message IS a decision record*. The thread-miner pulls
the forward-looking walls ("NEXT:", "THE WALL"); this pulls the backward-looking DECISIONS —
"we chose X", "X overrides Y", "deliberately NOT Z", "RESET" — into `Decision` objects, so
"why is it this way?" is a QUERY, not a re-read of 158 commit bodies. The why is the
least-recoverable memory: the code shows WHAT, the DAG shows WHEN, only the rationale holds WHY.

Mined => graded DERIVED (an inference over prose, like threads — tight markers keep it
trustworthy). The two harder links — `supersedes` (which earlier decision did this reset
replace?) and `grounded_by` (which canon principle grounds it?) — are a JUDGMENT, not a regex,
so they belong to the keyless AI path, not here. This miner does the clean, deterministic part:
the decision sentence, its kind, and the commit it was decided in.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from datetime import UTC, datetime
from typing import Any

from src.actions.core import Actions
from src.config.settings import get_settings
from src.db.pool import create_pool
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_SOURCE = "git-memory"
_EC = EvidenceClass.DERIVED.value
_CONF = confidence_for(EvidenceClass.DERIVED)

# Decision markers, ordered most-specific-first; the first that matches names the decision's
# KIND. Tight + author-intended (the thread-miner's lesson: broad words mine noise). These are
# the backward-looking "we settled this" flags, complementary to threads' forward-looking ones.
_MARKERS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("reset", re.compile(r"\bRESET\b")),
    ("ruling", re.compile(r"\bruling #?\d")),
    # tight: a decision-shaped supersession ("supersedes the…", "overrides DESIGN/the…",
    # "(open|design) decision #N") — NOT a bare "overrides"/"supersedes" mid-clause, which the
    # live run showed is usually config/mechanism prose ("subject override", "overrides, tests").
    ("override", re.compile(
        r"\b(supersedes? the|overrides? (?:DESIGN|the\b)|(?:open |design )?decision #?\d)", re.I)),
    ("rejection", re.compile(
        r"\bdeliberately (?:NOT|DO NOT|skip\w*|drop\w*|left|avoid\w*|kill\w*)", re.I)),
    ("rejection", re.compile(r"\b(scratched|deprecated)\b", re.I)),
    ("choice", re.compile(r"\b(we chose|chose to|decided to|the decision (?:was|is))\b", re.I)),
    ("decision", re.compile(r"\bDECISION\b")),
)
# META noise: a sentence ABOUT decision-mining itself (a commit that builds this very feature)
# — not a decision. Same guard the thread-miner needs against self-description.
_META = re.compile(r"\bdecision (?:miner|mining|records?|log|object|node|type)\b", re.I)


def extract_decisions(body: str) -> list[tuple[str, str]]:
    """The (statement, kind) decisions in a commit body — deduped, trimmed. Pure."""
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    flat = re.sub(r"\n(?=\S)", " ", body)          # join soft wraps (commit bodies are wrapped)
    for frag in re.split(r"(?:\. |\n|; )", flat):
        s = frag.strip(" .-—\t")
        if not (16 <= len(s) <= 240) or _META.search(s):
            continue
        if s[0].islower():            # a mid-sentence fragment from the split, not a decision
            continue
        kind = next((k for k, rx in _MARKERS if rx.search(s)), None)
        if kind is None:
            continue
        key = re.sub(r"\W+", "", s.lower())
        if key not in seen:
            seen.add(key)
            out.append((s, kind))
    return out


async def mine_decisions(
    actions: Actions, *, source_id: str = _SOURCE, case_id: uuid.UUID | None = None
) -> dict[str, Any]:
    """Scan every Commit's rationale, mint a Decision per decision sentence (idempotent on a
    content hash), record its kind, and link it `decided_in` the commit. Returns counts."""
    pool = actions.pool
    rows = await pool.fetch(
        "SELECT o.id, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='rationale') AS body, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='authored_date') AS date "
        "FROM objects o WHERE o.type='Commit'"
    )
    # create_link is a plain append — dedup the decided_in edge so a re-run is idempotent
    existing = {(r["from_id"], r["to_id"]) for r in
                await pool.fetch("SELECT from_id, to_id FROM links WHERE type='decided_in'")}
    decisions = 0
    for r in rows:
        body = r["body"] or ""
        if not body:
            continue
        observed = datetime.fromisoformat(r["date"]) if r["date"] else datetime.now(UTC)
        for text, kind in extract_decisions(body):
            canon = f"decision:{hashlib.sha1(text.encode()).hexdigest()[:12]}"
            d = await actions.create_or_find_object("Decision", canon, source_id, case_id)
            await actions.assert_property(d, "summary", text, source_id, observed, _CONF,
                                          case_id=case_id, evidence_class=_EC)
            await actions.assert_property(d, "kind", kind, source_id, observed, _CONF,
                                          case_id=case_id, evidence_class=_EC)
            if (d, r["id"]) not in existing:
                await actions.create_link(d, r["id"], "decided_in", source_id, observed, _CONF,
                                          case_id=case_id, evidence_class=_EC)
                existing.add((d, r["id"]))
            decisions += 1
    return {"decisions": decisions, "commits_scanned": len(rows)}


def main() -> None:  # pragma: no cover - CLI
    import asyncio

    async def run() -> None:
        pool = await create_pool(get_settings().database_url)
        try:
            print(await mine_decisions(Actions(pool)))
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":  # pragma: no cover
    main()
