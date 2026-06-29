"""Thread mining — the project's OPEN questions, derived from its own commit rationale.

The self-referential desk's sharpest read: "what am I blocked on / what's next?" The
durable record already holds it — every commit body states its walls and next-steps. This
mines those sentences into `Thread` objects so the answer is a QUERY, not a re-read of 130
commit messages. Mined => graded DERIVED (an inference over prose, not authoritative); the
operator/Claude curates (close a thread, or let a later commit supersede it).
"""

from __future__ import annotations

import hashlib
import re
import uuid
from datetime import UTC, datetime
from typing import Any

from src.actions.core import Actions
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_SOURCE = "git-memory"
_EC = EvidenceClass.DERIVED.value
_CONF = confidence_for(EvidenceClass.DERIVED)

# High-signal markers an author uses to flag a WALL or a next-step in a commit body. Tuned
# from a live run: broad words ("pending", "deferred", "blocked") caught features and old
# resolutions, so we keep only the explicit, author-intended flags. Mining prose is rough by
# nature — this is why the AI extractor exists; deterministic stays tight to stay trustworthy.
_OPEN = re.compile(
    r"(\bNEXT:|\bTHE WALL\b|\bWALL:|\bREMAINING\b|gated on|not yet live|"
    r"needs a (?:real )?(?:free )?(?:key|token|vantage|portal|cred|GITHUB_TOKEN|ANTHROPIC))"
)
# a sentence that's really CLOSED ("DONE", "PROVEN") even if it brushes a marker — skip
_CLOSED = re.compile(r"\b(DONE|PROVEN|FIXED|resolved|shipped)\b", re.IGNORECASE)


def extract_threads(body: str) -> list[str]:
    """The open-thread sentences in a commit body (deduped, trimmed). Pure."""
    out: list[str] = []
    seen: set[str] = set()
    # split on sentence ends + newlines; commit bodies are wrapped, so join soft wraps first
    flat = re.sub(r"\n(?=\S)", " ", body)
    for frag in re.split(r"(?:\. |\n|; )", flat):
        s = frag.strip(" .-—\t")
        if not (12 <= len(s) <= 240):
            continue
        if _OPEN.search(s) and not _CLOSED.search(s):
            key = re.sub(r"\W+", "", s.lower())
            if key not in seen:
                seen.add(key)
                out.append(s)
    return out


async def mine_threads(
    actions: Actions, *, source_id: str = _SOURCE, case_id: uuid.UUID | None = None
) -> dict[str, Any]:
    """Scan every Commit's rationale, mint a Thread per open-thread sentence (idempotent on a
    content hash), and link it `noted_in` the commit. Returns counts."""
    pool = actions.pool
    rows = await pool.fetch(
        "SELECT o.id, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='rationale') AS body, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='authored_date') AS date "
        "FROM objects o WHERE o.type='Commit'"
    )
    threads = 0
    for r in rows:
        body = r["body"] or ""
        if not body:
            continue
        observed = datetime.fromisoformat(r["date"]) if r["date"] else datetime.now(UTC)
        for text in extract_threads(body):
            canon = f"thread:{hashlib.sha1(text.encode()).hexdigest()[:12]}"
            t = await actions.create_or_find_object("Thread", canon, source_id, case_id)
            await actions.assert_property(t, "summary", text, source_id, observed, _CONF,
                                          case_id=case_id, evidence_class=_EC)
            await actions.assert_property(t, "status", "open", source_id, observed, _CONF,
                                          case_id=case_id, evidence_class=_EC)
            await actions.create_link(t, r["id"], "noted_in", source_id, observed, _CONF,
                                      case_id=case_id, evidence_class=_EC)
            threads += 1
    return {"threads": threads, "commits_scanned": len(rows)}
