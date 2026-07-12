"""THE CLOSURE MINER — a commit may witness that a transcript's promise was kept.

Operator ruling, 2026-07-12 ("almost 1000 open but the eternal question is whether they are
stale... there has to be a way for the miner to know what to close, maybe off the graph").

WHAT WAS MEASURED FIRST (decision 462e7ec7). Nothing in the pile is stale by AGE — not one open
thread is older than 14 days. The pile exists because Osiris hears every promise and witnesses
almost no delivery: the session-miner reads transcripts from ~20 projects while the pulse read
commits from SEVEN. For 13 of 20 trees the graph held no record that anything was ever done, so
nothing could ever close their threads. Run the miner's own matcher over those trees' real git
logs and 52% of their open threads have a LATER COMMIT that matches them. The closer was never
broken. It was blind.

THE ONE SANCTIONED CROSSING. Constitution #5 keeps miners behind ownership boundaries — a miner
never touches another source's objects — and that is exactly what stopped a commit from closing
a thread a transcript opened. The operator's ruling permits this ONE crossing, because the git
log is the ground truth for "was this done" and the graph's own opinion is not. So the crossing
gets its own SOURCE (`closure-miner`), never smuggled in under the session-miner's name: a
boundary that is crossed deliberately must be legible in the provenance.

SPLIT BY CONFIDENCE (the operator's ruling, over auto-close-everything):
  · STRONG  → resolve, DERIVED, citing the commit. Reversible, provenanced, never a DELETE.
  · WEAK    → NEVER closes. It becomes a `rot_candidate` carrying its evidence, for the human
              to confirm. The membrane holds where the evidence is thin (constitution #6: the
              loop may close, but never silently and never irreversibly).

AND THE HARD GUARD, which outranks every signal below: a thread ANY MIND HAS TOUCHED
(evidence_class self_declared — an operator's duty, an agent's declared obligation) is NEVER
auto-closed by a machine reading a git log. A guess may be swept by evidence; a declaration is
answered only by its owner. This is the line that makes the crossing safe.
"""
from __future__ import annotations

import math
import re
from datetime import UTC, datetime
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.ingest.mined import distinctive_terms
from src.parsers.base import EvidenceClass
from src.parsers.evidence import confidence_for

_SOURCE = "closure-miner"
_EC = EvidenceClass.DERIVED.value   # a machine reading a git log is an inference, never testimony
_CONF = confidence_for(EvidenceClass.DERIVED)

# THE BAR — AND WHAT THE DRY RUNS ACTUALLY PROVED (2026-07-12).
#
# CUT 1 counted shared "distinctive" tokens. The dry run caught it lying in seconds: it wanted to
# close "Order paradox: whether order in text is relational" against a commit about init-hooks, on
# four "distinctive" shared terms — `predicts, rather, than, where`. A hand-kept stopword list is a
# hardcode and it will always leak; adding "rather" to it only moves the leak.
#
# CUT 2 weighted terms by rarity (IDF over the tree's own corpus, so the corpus grades its own
# vocabulary instead of a legislated list). That killed the stopword garbage — and the dry run
# STILL caught it lying: it wanted to close "The daemon's structural assignment and naming"
# against a census-encoder commit on `bearing(5.2), load(4.3)`, and "TRC application + JAX backend"
# against a spectral-diagnostics commit on `track, backend, should`. Precision at the strong bar
# was ~50%. Raising the bar does not save it: "Stage 3 ledger campaigns" scores 21.3 against a
# binding-organ commit and is still simply wrong.
#
# THE VERDICT, against my own design: LEXICAL SIMILARITY IS NOT EVIDENCE THAT WORK WAS DONE. Two
# texts about the same codebase share vocabulary because they are about the same codebase. It is
# enough to ASK a human. It is never enough to assert into an append-only record, where a false
# "done" outlives everyone who could remember it was false.
#
# So the automatic lane is reserved for the ONE unambiguous witness — the signal the operator's
# own ruling named: THE COMMIT NAMES THE THREAD. A mind wrote that id into a commit message on
# purpose; nothing else here is a fact. Everything lexical, however strong it scores, becomes a
# rot_candidate carrying its evidence, for a human to confirm in bulk. The membrane holds exactly
# where the evidence stops.
STRONG_SCORE = 9.0       # kept as the RANKING knob — best candidates first, never an auto-close
WEAK_SCORE = 4.0         # below this there is nothing worth even asking about
_SHORT_ID = re.compile(r"\b[0-9a-f]{8}\b")


def build_idf(docs: list[str]) -> dict[str, float]:
    """Rarity, LEARNED from the corpus rather than legislated by a stopword list."""
    n = max(1, len(docs))
    df: dict[str, int] = {}
    for d in docs:
        for t in distinctive_terms(d):
            df[t] = df.get(t, 0) + 1
    return {t: math.log(n / c) for t, c in df.items()}


def _evidence(
    thread_id: str, summary: str, commit_text: str, idf: dict[str, float],
    *, weak: float = WEAK_SCORE, strong: float = STRONG_SCORE,
) -> tuple[str, float, str] | None:
    """Grade one (thread, commit) pair. Returns (verdict, score, why) or None for no evidence.

    Only ONE verdict is "strong", and it is not a similarity score: the commit NAMES the thread.
    Everything else is "weak" — a question, never an answer (see the header: two dry runs killed
    the lexical auto-close lane on its own evidence).
    """
    short = thread_id[:8]
    if short in _SHORT_ID.findall(commit_text.lower()):
        return ("strong", 99.0, f"the commit names this thread by id ({short})")
    shared = distinctive_terms(summary) & distinctive_terms(commit_text)
    if not shared:
        return None
    score = sum(idf.get(t, 0.0) for t in shared)
    if score < weak:
        return None
    top = sorted(shared, key=lambda t: -idf.get(t, 0.0))[:5]
    hedge = "likely" if score >= strong else "possibly"
    why = (f"{hedge} — score {score:.1f} · rarest shared terms: "
           + ", ".join(f"{t}({idf.get(t, 0.0):.1f})" for t in top))
    return ("weak", score, why)      # lexical similarity ASKS. It never asserts.


async def _open_untouched_threads(
    pool: asyncpg.Pool, repo: str | None,
) -> list[dict[str, Any]]:
    """Open threads NO MIND HAS TOUCHED, in one tree (or all). The self_declared exclusion is
    the hard guard: a declared duty is never a machine's to close."""
    return [dict(r) for r in await pool.fetch(
        "SELECT o.id, o.created_at, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='summary' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS summary "
        "FROM objects o JOIN links l ON l.from_id=o.id AND l.type='in_repo' "
        "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
        "WHERE o.type='Thread' AND o.status='active' AND o.merged_into IS NULL "
        "AND ($1::text IS NULL OR p.canonical = 'repo:' || $1) "
        "AND COALESCE((SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='status' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1),'open') "
        "  = 'open' "
        # THE HARD GUARD — a mind touched it, so a git log does not get to answer for it
        "AND NOT EXISTS (SELECT 1 FROM assertions a WHERE a.object_id=o.id "
        "  AND a.evidence_class='self_declared') "
        # and never re-judge one already awaiting the human's confirmation
        "AND NOT EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='rot_candidate')", repo)]


async def _commits(pool: asyncpg.Pool, repo: str | None) -> list[dict[str, Any]]:
    """Every ingested commit in the tree, with the text a thread could match against."""
    return [dict(r) for r in await pool.fetch(
        "SELECT o.id, o.canonical, o.created_at, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='subject' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS subject, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='rationale' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS rationale, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "   AND a.name='authored_date' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) "
        "   AS at "
        "FROM objects o JOIN links l ON l.from_id=o.id AND l.type='in_repo' "
        "JOIN objects p ON p.id=l.to_id AND p.type='SoftwareProject' "
        "WHERE o.type='Commit' AND o.status='active' "
        "AND ($1::text IS NULL OR p.canonical = 'repo:' || $1)", repo)]


async def close_by_commits(
    actions: Actions, *, repo: str | None = None, dry_run: bool = True,
    strong: float = STRONG_SCORE, weak: float = WEAK_SCORE,
) -> dict[str, Any]:
    """Sweep one tree (or the whole garden): find the commit that witnesses each untouched open
    thread, and grade it. `dry_run=True` (the default, DELIBERATELY) writes NOTHING and reports
    exactly what it would do — a sweep that can close hundreds of threads is a thing you read
    before you let it run, and the first dry run here caught the matcher lying.

    `strong`/`weak` are the rarity bars. They are tuned to a real tree's corpus; a small corpus
    yields small IDF values, so they are parameters rather than constants — the bar belongs to
    the caller who can see the distribution, not to this module.
    """
    threads = await _open_untouched_threads(actions.pool, repo)
    commits = await _commits(actions.pool, repo)
    if not threads or not commits:
        return {"repo": repo, "threads": len(threads), "commits": len(commits),
                "resolved": 0, "candidates": 0,
                "note": "nothing to witness — this tree's work is not in the graph"}

    prepared = [(c, f"{c['subject'] or ''} {c['rationale'] or ''}",
                 c["at"] or c["created_at"].isoformat()) for c in commits]
    # the corpus grades its own vocabulary — every text in this tree, threads and commits alike
    idf = build_idf([t["summary"] or "" for t in threads] + [txt for _, txt, _ in prepared])
    observed = datetime.now(UTC)
    resolved: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []

    for t in threads:
        summary, tid = t["summary"] or "", str(t["id"])
        if not summary:
            continue
        born = t["created_at"].isoformat()
        best: tuple[float, str, str, dict[str, Any]] | None = None
        for c, text, at in prepared:
            if at <= born:              # only a LATER commit can witness a promise
                continue
            got = _evidence(tid, summary, text, idf, weak=weak, strong=strong)
            if got and (best is None or got[1] > best[0]):
                best = (got[1], got[0], got[2], c)
        if best is None:
            continue
        weight, verdict, why, c = best
        cite = f"{(c['canonical'] or '')[:14]} {c['subject'] or ''}".strip()
        row = {"thread": tid[:8], "summary": summary[:90], "commit": cite[:90],
               "why": why, "score": round(weight, 1)}
        if verdict == "strong":
            resolved.append(row)
            if not dry_run:
                await actions.assert_property(t["id"], "status", "resolved", _SOURCE, observed,
                                              _CONF, evidence_class=_EC, actor=_SOURCE)
                await actions.assert_property(t["id"], "resolved_in", _SOURCE, _SOURCE, observed,
                                              _CONF, evidence_class=_EC, actor=_SOURCE)
                await actions.assert_property(
                    t["id"], "resolved_because",
                    f"witnessed by a later commit — {cite} ({why})"[:300],
                    _SOURCE, observed, _CONF, evidence_class=_EC, actor=_SOURCE)
        else:
            candidates.append(row)
            # DON'T WRITE WHAT YOU WOULDN'T ACT ON (measured on the second tree, 2026-07-12).
            # The weak band (WEAK..STRONG) is topically related and mostly NOT completion: on
            # xxit it matched "User must verify on mobile that stem sampling works" to the commit
            # that BUILT stem sampling — a commit that CREATED that obligation rather than
            # discharging it. Persisting those puts ~90%-wrong guesses at the TOP of the human's
            # triage queue (the echoes lens sorts evidenced threads first), which is worse than
            # saying nothing. They stay in the dry-run REPORT, where a reader can weigh them; the
            # GRAPH only carries what is worth a click.
            if not dry_run and weight >= strong:
                await actions.assert_property(
                    t["id"], "rot_candidate",
                    f"a later commit may have done this — {cite} ({why})"[:300],
                    _SOURCE, observed, _CONF, evidence_class=_EC, actor=_SOURCE)
    return {
        "repo": repo, "dry_run": dry_run,
        "threads": len(threads), "commits": len(commits),
        "resolved": len(resolved), "candidates": len(candidates),
        "annotated": sum(1 for c in candidates if c["score"] >= strong),
        "resolved_rows": resolved[:20], "candidate_rows": candidates[:20],
        "note": ("DRY RUN — nothing written" if dry_run else
                 f"{len(resolved)} closed citing a commit; {len(candidates)} await your word"),
    }
