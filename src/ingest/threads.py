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
from src.config.settings import get_settings
from src.db.pool import create_pool
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

# Generic engineering vocabulary — present in half the commits, so a SHARED generic word
# is no evidence a later commit addressed a thread. Resolution requires shared DISTINCTIVE
# tokens (the project's own nouns: renderer, satellite, composer, briefing), never these.
_GENERIC = frozenset({
    "wire", "fold", "adds", "added", "build", "built", "ship", "make", "made", "feat",
    "fix", "test", "tests", "code", "into", "over", "with", "this", "that", "then",
    "when", "next", "wall", "remaining", "needs", "need", "live", "gated", "work",
    "thing", "things", "part", "pass", "runs", "real", "free", "file", "files", "from",
    "have", "still", "yet", "via", "all", "new", "key", "token", "first", "more", "one",
    "two", "the", "and", "for", "but", "now", "its",
})


def _distinctive(text: str) -> set[str]:
    """The project-distinctive tokens of a string (>=4 chars, minus generic vocabulary)."""
    return {t for t in re.findall(r"[a-z][a-z0-9_]{3,}", text.lower())} - _GENERIC


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


async def resolve_threads(
    actions: Actions, *, source_id: str = _SOURCE, case_id: uuid.UUID | None = None
) -> dict[str, Any]:
    """Self-heal the briefing: close an open Thread when a LATER commit addressed it.

    The trust problem with mined memory is staleness — `NEXT: the renderer` lingers as a
    wall long after the renderer shipped, and a prosthesis you rely on instead of memory is
    only as good as it is current. A commit resolves a thread when (a) it is strictly later
    than the commit that raised the thread, and (b) it shares >=2 DISTINCTIVE tokens with the
    thread's text (the project's own nouns — generic engineering words don't count). The
    EARLIEST such commit wins (when it was actually closed). Conservative by design: a false
    'resolved' hides a real wall, so we'd rather leave a stale thread open than close a live
    one — and every close is provenanced (`resolved_in` / `resolved_because` / `resolved_by`)
    so a human can audit and reverse it.
    """
    pool = actions.pool
    open_threads = await pool.fetch(
        "SELECT o.id, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='summary') AS summary, "
        " (SELECT a.value #>> '{}' FROM current_assertions a JOIN links l "
        "  ON l.to_id=a.object_id "
        "  WHERE l.from_id=o.id AND l.type='noted_in' AND a.name='authored_date' "
        "  LIMIT 1) AS origin_date "
        "FROM objects o WHERE o.type='Thread' AND EXISTS ("
        "  SELECT 1 FROM current_assertions s WHERE s.object_id=o.id "
        "  AND s.name='status' AND s.value #>> '{}' = 'open')"
    )
    commit_rows = await pool.fetch(
        "SELECT o.id, o.canonical, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='summary') AS summary, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='scope') AS scope, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='authored_date') AS date "
        "FROM objects o WHERE o.type='Commit'"
    )
    # oldest-first, so the first matching commit per thread is the one that actually closed it
    commits = sorted(
        ((datetime.fromisoformat(c["date"]), c["id"], c["canonical"],
          _distinctive(f"{c['summary'] or ''} {c['scope'] or ''}"))
         for c in commit_rows if c["date"]),
        key=lambda c: c[0],
    )

    resolved = 0
    for t in open_threads:
        topic = _distinctive(t["summary"] or "")
        if len(topic) < 2:
            continue
        origin = datetime.fromisoformat(t["origin_date"]) if t["origin_date"] else None
        for cdate, cid, canonical, ctokens in commits:
            if origin and cdate <= origin:
                continue
            shared = topic & ctokens
            if len(shared) < 2:
                continue
            because = ", ".join(sorted(shared))
            await actions.assert_property(t["id"], "status", "resolved", source_id, cdate,
                                          _CONF, case_id=case_id, evidence_class=_EC)
            await actions.assert_property(t["id"], "resolved_in", canonical, source_id, cdate,
                                          _CONF, case_id=case_id, evidence_class=_EC)
            await actions.assert_property(t["id"], "resolved_because", because, source_id,
                                          cdate, _CONF, case_id=case_id, evidence_class=_EC)
            await actions.create_link(t["id"], cid, "resolved_by", source_id, cdate, _CONF,
                                      case_id=case_id, evidence_class=_EC)
            resolved += 1
            break
    return {"resolved": resolved, "open_remaining": len(open_threads) - resolved}


def main() -> None:  # pragma: no cover - CLI
    """Mine open threads from commit rationale, then self-heal the resolved ones."""
    import asyncio

    async def run() -> None:
        pool = await create_pool(get_settings().database_url)
        try:
            actions = Actions(pool)
            print(await mine_threads(actions))
            print(await resolve_threads(actions))
        finally:
            await pool.close()

    asyncio.run(run())


if __name__ == "__main__":  # pragma: no cover
    main()
