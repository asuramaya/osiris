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
  · STRONG  → resolve, DERIVED, citing the commit — AND, since Thoth DM 2581/decision
              cb38d922/fc5b6c5f, a real `resolved_by` edge to that commit, not just the
              property: this was the second live gap still widening the topology-derived
              closure pile after Phase 1a fixed capture.py's own resolve_thread. Retrofit,
              not a new design — the SAME commit id this module already selected as the
              witness. Reversible, provenanced, never a DELETE.
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
import uuid
from datetime import UTC, datetime
from typing import Any

import asyncpg

from src.actions.core import Actions
from src.config.settings import Settings, get_settings
from src.ingest.mined import distinctive_terms
from src.orchestrator.capture import _find_artifact
from src.orchestrator.monitor import get_cursor, set_cursor
from src.orchestrator.thread_closure import thread_closure_status
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


async def _commits(
    pool: asyncpg.Pool, repo: str | None, *, since: datetime | None = None,
) -> list[dict[str, Any]]:
    """Every ingested commit in the tree, with the text a thread could match against.

    `since` (the watermark, Thoth DM 2635) filters on `o.created_at` — GRAPH INGESTION
    time, deliberately not `authored_date` (the `at` column, read separately below): a
    commit's author date can be backdated or ingested late relative to when it was
    authored, so ordering the watermark by anything but "when THIS graph first saw it"
    risks silently skipping a commit that lands out of author-date order. Ingestion time
    is monotonic by construction (a bigserial-backed `created_at`, never revised)."""
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
        "AND ($1::text IS NULL OR p.canonical = 'repo:' || $1) "
        "AND ($2::timestamptz IS NULL OR o.created_at > $2)", repo, since)]


_WATERMARK_PREFIX = "closure-miner"


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

    THE WATERMARK (Thoth DM 2635, decision ff20869a): this was O(threads × ALL commits)
    EVERY call, forever — no watermark, so a scheduled sweep would re-derive work it had
    already fully derived last time, unboundedly, as both corpora grow. `threads` stays
    unscoped by design (it's already a small, slow-changing set — open AND untouched, which
    shrinks the moment a mind or this sweep itself acts on a row); `commits` is now scoped
    to `since` the last successful (non-dry-run) pass's watermark, stored under key
    `f"{_WATERMARK_PREFIX}:{repo or '*'}"` in the generic `watermarks` table
    (monitor.get_cursor/set_cursor — the same primitive session-sensing's transcript cursor
    already uses, not a new mechanism). CORRECTNESS: narrowing only the commit axis is
    sound because any commit older than the watermark was already graded against every
    thread that was ALREADY open-and-untouched at the time it was ingested — the one
    residual gap is a commit whose OWN ingestion lands out of order relative to a thread
    opened in between two runs, which `since` filtering on `created_at` (ingestion time,
    not `authored_date`) already closes; see `_commits`'s own docstring. The watermark
    never advances on a dry run (a read must stay a read) and never moves backward (a
    partial/failed pass leaves it exactly where the last SUCCESSFUL one left it, so a
    retry after a crash re-examines the same commits rather than silently skipping them).
    """
    since_raw = None if dry_run else await get_cursor(
        actions.pool, f"{_WATERMARK_PREFIX}:{repo or '*'}")
    since = datetime.fromisoformat(since_raw) if since_raw else None

    threads = await _open_untouched_threads(actions.pool, repo)
    commits = await _commits(actions.pool, repo, since=since)
    if not threads or not commits:
        return {"repo": repo, "threads": len(threads), "commits": len(commits),
                "since": since.isoformat() if since else None,
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
                # THE MISSING EDGE (Thoth DM 2581, decision cb38d922/fc5b6c5f): "strong" here
                # is the commit literally naming the thread's short id (_evidence's ONLY
                # strong path) -- exactly resolved_by's own contract (the strong closure
                # witness), so this closure deserves the same traversable edge
                # resolve_thread(artifact=...) mints, not just the property. Idempotent
                # check-then-create, same shape capture.py's own resolved_by minting uses,
                # so a re-run of this sweep over an already-closed thread is a no-op here too.
                if not await actions.pool.fetchval(
                        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 "
                        "AND type='resolved_by' LIMIT 1", t["id"], c["id"]):
                    await actions.create_link(t["id"], c["id"], "resolved_by", _SOURCE,
                                              observed, _CONF, evidence_class=_EC,
                                              actor=_SOURCE)
        else:
            candidates.append(row)
            # DON'T WRITE WHAT YOU WOULDN'T ACT ON (measured on the second tree, 2026-07-12).
            # The weak band (WEAK..STRONG) is topically related and mostly NOT completion: on
            # in one repo it matched "User must verify on mobile that stem sampling works" to
            # the commit that BUILT stem sampling — a commit that CREATED that obligation
            # rather than
            # discharging it. Persisting those puts ~90%-wrong guesses at the TOP of the human's
            # triage queue (the echoes lens sorts evidenced threads first), which is worse than
            # saying nothing. They stay in the dry-run REPORT, where a reader can weigh them; the
            # GRAPH only carries what is worth a click.
            if not dry_run and weight >= strong:
                await actions.assert_property(
                    t["id"], "rot_candidate",
                    f"a later commit may have done this — {cite} ({why})"[:300],
                    _SOURCE, observed, _CONF, evidence_class=_EC, actor=_SOURCE)
    new_watermark = max(c["created_at"] for c in commits)
    if not dry_run:
        # advance only past what was actually fetched THIS run -- never ahead of it, so a
        # crash between here and the caller seeing the report re-examines these same
        # commits next time rather than silently skipping them
        await set_cursor(actions.pool, f"{_WATERMARK_PREFIX}:{repo or '*'}",
                         new_watermark.isoformat())

    # THE SCAN-BOUNDARY LINE (Thoth DM 2635/2629, decision 04ad4bb8/975ec1eb — presence
    # read as coverage): a thread with NO in_repo edge at all is structurally excluded
    # from `_open_untouched_threads`'s own INNER JOIN, permanently, on any cadence. Cheap
    # (one COUNT(*), no join against this sweep's own O(threads) work) and only meaningful
    # fleet-wide -- a single-repo scope has no "unreachable" concept, a thread not in THAT
    # repo is simply out of scope, not invisible.
    unreachable_no_repo = None
    if repo is None:
        unreachable_no_repo = await actions.pool.fetchval(
            "SELECT count(*) FROM objects o WHERE o.type='Thread' AND o.status='active' "
            "AND o.merged_into IS NULL "
            "AND NOT EXISTS (SELECT 1 FROM links l WHERE l.from_id=o.id "
            "  AND l.type='in_repo')")

    return {
        "repo": repo, "dry_run": dry_run,
        "since": since.isoformat() if since else None,
        "until": new_watermark.isoformat(),
        "threads": len(threads), "commits": len(commits),
        "unreachable_no_repo": unreachable_no_repo,
        "resolved": len(resolved), "candidates": len(candidates),
        "annotated": sum(1 for c in candidates if c["score"] >= strong),
        "resolved_rows": resolved[:20], "candidate_rows": candidates[:20],
        "note": ("DRY RUN — nothing written" if dry_run else
                 f"{len(resolved)} closed citing a commit; {len(candidates)} await your word"),
    }


async def close_by_commits_scheduled_tick(
    actions: Actions, *, settings: Settings | None = None,
) -> dict[str, Any]:
    """THE SCHEDULED LEG's own tick — `arq_worker.closure_miner_heartbeat` calls this
    unconditionally, the same thin-shim shape trigger_mail/pit_watch_heartbeat/
    fleet_reconcile_heartbeat already use: the flag gate and the acting logic both live
    here, never in the cron wrapper, so a test can exercise the real gate without
    touching arq.

    OFF unless `osiris_closure_miner_enabled` — the kill switch (Thoth DM 2679, the same
    law fleet_reconcile_heartbeat already stands on: a mechanism that WRITES to the graph
    on a schedule earns its own kill switch, never inherits one). The code ships inert;
    flipping the flag is a second signature separate from approving the diff. When on,
    composes `close_by_commits(dry_run=False)` fleet-wide — the exact same acting verb
    reachable by hand, so the schedule and a human's own manual call are provably the
    same path, never two implementations that could drift.

    `settings` is the injected test seam (`reconcile_scheduled_tick`'s own convention:
    `st = settings or get_settings()`) so a test can flip the flag without touching the
    real environment or monkeypatching `get_settings`.
    """
    st = settings or get_settings()
    if not st.osiris_closure_miner_enabled:
        return {"enabled": False, "resolved": 0, "candidates": 0,
                "note": "the closure miner's scheduled leg is dark "
                        "(osiris_closure_miner_enabled=0)"}
    out = await close_by_commits(actions, repo=None, dry_run=False)
    return {"enabled": True, **out}


# --- THE PROSE BACKFILL (Thoth DM 2958/2975, thread 13725dbb) --------------------------
#
# THE FINDING THAT MOTIVATES THIS: closure_health's own needs_human split (commit af20ad9)
# proved most of the fleet-wide resolved-edgeless pile is not missing evidence at all —
# it's evidence sitting in `resolved_because` PROSE ("SHIPPED in 9a12b71", "Fixed in
# adaptive.js: ...") that a mind wrote on purpose and _find_artifact structurally could
# never see, because it only ever looked at `resolved_artifact`. A 110-thread hand-read
# sample found this is the DOMINANT pattern, not a rare one — a crude keyword floor alone
# caught 429 of 929 (46%), and reading the residual by hand suggested the true recoverable
# share is far higher, just phrased differently than any keyword list would catch.
#
# WHAT THIS DOES NOT DO: guess. It extracts hash-shaped TOKENS (git's own convention, 7-40
# hex chars, reusing the exact same shape `_find_artifact`'s own bare-hash branch already
# validates) and a thread-graph's own 8-hex short-id convention (`_SHORT_ID`, the exact
# regex `_evidence` above already uses to detect a commit NAMING a thread) — then asks the
# SAME resolver every citation goes through, `_find_artifact`, whether that token names a
# real object. A coincidental hex-shaped word finding nothing real is reported as a MISS,
# never guessed into a mint (Thoth's constraint 2) — see `unresolvable` below.
#
# KNOWN, DELIBERATE MISS (Thoth DM 3052, not fixed here): this regex extracts the bare
# hex tail only, so a fully-qualified cross-project canonical in prose — "thread:
# 83b35671dfd4 in heinrich" — loses its "thread:" prefix before `_find_artifact` ever
# sees it, and the bare tail is not a prefix of that Thread's own `id` (a different
# convention than Decision/Commit/Thread/Tension/Practice's own short-id-is-an-id-prefix
# shape), so it can never resolve either way. Confirmed exactly ONE such case in the
# fleet-wide 77. Fixing it would mean a second regex capturing the qualified form and
# trying `_find_artifact`'s canonical-exact branch before the bare-hex branch — real,
# targeted, and NOT built: one confirmed instance does not justify a second extraction
# path, and the failure mode here is silent-safe (stays `unresolvable`, never a wrong
# mint), not a defect that compounds. Revisit only if characterizing a later population
# finds more than a handful.
_HASH_TOKEN = re.compile(r"\b[0-9a-f]{7,40}\b", re.I)
# The wake-permission-storm mined-echo cluster's own boilerplate (Thoth's constraint 4):
# near-duplicate Thread objects for ONE incident, all citing "root cause thread ba73c0c8"
# verbatim. A dedup problem, not a missing-evidence problem — excluded here, named as its
# own smaller, separate task, never entangled with this backfill.
_WAKE_STORM_MARKER = "root cause thread"
_BACKFILL_SOURCE = "closure-backfill"


async def _needs_human_threads(
    pool: asyncpg.Pool, repo_id: uuid.UUID | None,
) -> list[dict[str, Any]]:
    """The population this backfill targets: resolved, no closure edge, and whatever
    `resolved_artifact` it already carries (if any) does not resolve to a graph object
    right now — closure_health's own `needs_human` bucket (compositions.py, commit
    af20ad9), independently re-derived here from the same primitives
    (`thread_closure_status`, `_find_artifact`) rather than imported, since this ingest-
    layer module never reaches into the orchestrator's read-composition layer. Recomputed
    fresh every call, never trusted from a prior report — the graph keeps growing, so an
    artifact that failed to resolve yesterday may resolve today."""
    rows = await thread_closure_status(pool, repo=repo_id)
    resolved_edgeless = [r for r in rows if not r["closed_by_topology"]
                         and r["property_status"] == "resolved"]
    ids = [r["thread_id"] for r in resolved_edgeless]
    if not ids:
        return []
    detail = await pool.fetch(
        "SELECT o.id, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='resolved_because' ORDER BY a.confidence DESC, a.observed_at DESC "
        "  LIMIT 1) AS resolved_because, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='resolved_artifact' ORDER BY a.confidence DESC, a.observed_at DESC "
        "  LIMIT 1) AS resolved_artifact "
        "FROM objects o WHERE o.id = ANY($1::uuid[])", ids)
    out = []
    for r in detail:
        if r["resolved_artifact"] and await _find_artifact(pool, r["resolved_artifact"]):
            continue  # already commit_closeable via the direct field — not this miner's job
        out.append(dict(r))
    return out


async def _classify_miss(
    pool: asyncpg.Pool, thread_id: uuid.UUID, tokens: list[str],
    projects_with_commits: set[uuid.UUID],
) -> str:
    """Why a token that _find_artifact already refused stays refused (Thoth DM 3052) —
    called only for the unresolvable minority, never the 610-wide candidate sweep, so the
    extra queries here stay cheap. `not_a_hash` beats the other two: a token that merely
    LOOKS like a miss but actually names a real Agent is never a commit question at all."""
    for tok in tokens:
        if await pool.fetchval(
                "SELECT 1 FROM objects WHERE type='Agent' AND canonical LIKE "
                "'agent:' || $1 || '%' LIMIT 1", tok):
            return "not_a_hash"
    proj_id = await pool.fetchval(
        "SELECT to_id FROM links WHERE from_id=$1 AND type='in_repo' LIMIT 1", thread_id)
    if proj_id is not None and proj_id not in projects_with_commits:
        return "out_of_scope_repo"
    return "genuinely_missing"


async def close_by_prose_backfill(
    actions: Actions, *, repo: str | None = None, dry_run: bool = True,
) -> dict[str, Any]:
    """Mine `resolved_because` PROSE for hash-shaped evidence a mind already wrote but
    `_find_artifact` could never see, because that resolver only ever reads
    `resolved_artifact` (Thoth DM 2958/2975, piece 2 of the closure-backfill work — piece 1
    widened `_find_artifact` itself to match Thread, commit 8710ede). `repo` (a project
    name) scopes the read; `None` (the default) is fleet-wide, on purpose — closure_health's
    own scope finding (Thoth DM 2958) showed only 40% of the needs_human pile is even
    osiris's own; a backfill that only served osiris would leave the other 60% untouched in
    houses with nobody awake to run it.

    `dry_run=True` (the default, matching `close_by_commits`' own convention) writes
    NOTHING and reports exactly what it would do.

    THE THREE-WAY REPORT IS THE DELIVERABLE (Thoth's own framing), not the mints:
      - `resolved` — a hash-shaped token in the thread's prose resolved to a real graph
        object (Decision, Commit, Thread, Tension, or Practice). Mintable.
      - `unresolvable` — hash-shaped text WAS found, but nothing in the graph matches it.
        Never silently dropped or folded into a generic failure count.
      - `no_candidate` — nothing hash-shaped in the prose at all. No change from today;
        still genuinely needs a human.

    `unresolvable` ITSELF WAS A CONFLATED LABEL (Thoth DM 3052, found by hand-characterizing
    all 77 fleet-wide unresolvable rows rather than trusting the count): a first pass called
    every miss "the commit-ingestion gap," but that single number silently mixed three
    unrelated facts. `unresolvable_breakdown` (and its matching `*_rows` samples) splits it
    for real, mirroring closure_health's own strong/weak split one layer down:
      - `not_a_hash` — the token names something real in the graph (checked directly,
        never guessed), but of a kind that never CLOSES a thread — today that means an
        Agent canonical (e.g. "ad1a1cb0" inside "agent:ad1a1cb0-xxxv"). Deliberately never
        resolved into an edge (Thoth DM 3052: an Agent is not what closed a thread —
        `closed_by` already exists for that shape) and deliberately never counted toward
        the ingestion gap below — it was never a missing commit to begin with.
      - `out_of_scope_repo` — the token matches nothing, AND the thread's own project has
        never had a single Commit object ingested into this graph at all. Structurally
        unresolvable by design (no pipeline exists for that repo), not evidence of a
        mining defect — characterizing the 77 found this was the LARGEST bucket by far
        (hector-vector alone was 27 of 77), and it had been silently inflating the
        headline number.
      - `genuinely_missing` — the token matches nothing, AND the thread's own project DOES
        have Commit objects actively ingested. This, and only this, is the real
        commit-ingestion gap — measured honestly for the first time by this split.
    An unscoped thread (no `in_repo` link at all) is conservatively counted as
    `genuinely_missing` — absence of scope is not evidence the repo is out of scope.

    NEVER MINTS resolved_by AT THE SAME TRUST LEVEL A DIRECT CITATION CARRIES (constraint
    1): every mint here uses the SAME resolved_by edge shape (correct — it IS what closed
    the thread, structurally), but from a DISTINCT source (`closure-backfill`, not a
    mind's own `session`/agent id) at `EvidenceClass.DERIVED` — this module's own existing
    `_EC`/`_CONF` constants, the same ones `close_by_commits` already uses, deliberately
    never `SELF_DECLARED`. Migration 0045 reads `thread_closure_edges`' strength from
    `source_id` for exactly this reason: a `closure-backfill`-sourced resolved_by edge
    reports `strength='weak'` downstream (closure_health's own strong/weak split, commit
    af20ad9), never silently promoted to the same confidence as a hand-typed artifact=.

    Idempotent per (thread, target) pair, same check-then-create shape `close_by_commits`'
    own strong path already uses — a re-run over an already-backfilled thread is a no-op."""
    repo_id = None
    if repo:
        repo_id = await actions.pool.fetchval(
            "SELECT o.id FROM objects o JOIN current_assertions a ON a.object_id=o.id "
            "WHERE o.type='SoftwareProject' AND a.name='name' AND a.value #>> '{}'=$1 LIMIT 1",
            repo)
        if repo_id is None:
            return {"note": f"no SoftwareProject named {repo!r}"}

    candidates = await _needs_human_threads(actions.pool, repo_id)
    resolved: list[dict[str, Any]] = []
    unresolvable: list[dict[str, Any]] = []
    breakdown: dict[str, list[dict[str, Any]]] = {
        "not_a_hash": [], "out_of_scope_repo": [], "genuinely_missing": [],
    }
    no_candidate = 0
    observed = datetime.now(UTC)
    projects_with_commits = {
        r["to_id"] for r in await actions.pool.fetch(
            "SELECT DISTINCT l.to_id FROM objects c "
            "JOIN links l ON l.from_id = c.id AND l.type = 'in_repo' "
            "WHERE c.type = 'Commit'")
    }

    for t in candidates:
        text = " ".join(filter(None, [t.get("resolved_because"), t.get("resolved_artifact")]))
        if _WAKE_STORM_MARKER in text.lower():
            continue  # constraint 4 — its own smaller dedup task, not this one
        tokens = _HASH_TOKEN.findall(text)
        if not tokens:
            no_candidate += 1
            continue
        target = None
        matched: str | None = None
        for tok in tokens:
            target = await _find_artifact(actions.pool, tok)
            if target is not None:
                matched = tok
                break
        tid = str(t["id"])
        if target is None:
            row = {"thread": tid[:8], "candidate_tokens": tokens[:5]}
            unresolvable.append(row)
            kind = await _classify_miss(actions.pool, t["id"], tokens, projects_with_commits)
            breakdown[kind].append(row)
            continue
        resolved.append({"thread": tid[:8], "matched": matched, "target": str(target)[:8]})
        if not dry_run and not await actions.pool.fetchval(
                "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 "
                "AND type='resolved_by' LIMIT 1", t["id"], target):
            await actions.create_link(t["id"], target, "resolved_by", _BACKFILL_SOURCE,
                                      observed, _CONF, evidence_class=_EC,
                                      actor=_BACKFILL_SOURCE)

    return {
        "repo": repo, "dry_run": dry_run,
        "candidates": len(candidates),
        "resolved": len(resolved), "unresolvable": len(unresolvable),
        "no_candidate": no_candidate,
        "resolved_rows": resolved[:20], "unresolvable_rows": unresolvable[:20],
        "unresolvable_breakdown": {k: len(v) for k, v in breakdown.items()},
        "unresolvable_breakdown_rows": {k: v[:20] for k, v in breakdown.items()},
        "note": ("DRY RUN — nothing written" if dry_run else
                 f"{len(resolved)} closed via prose-mined citation (weak, source="
                 f"{_BACKFILL_SOURCE!r}); {len(unresolvable)} had a hash-shaped candidate "
                 "that resolved to nothing (not_a_hash="
                 f"{len(breakdown['not_a_hash'])}, "
                 f"out_of_scope_repo={len(breakdown['out_of_scope_repo'])}, "
                 f"genuinely_missing={len(breakdown['genuinely_missing'])} — only the last "
                 "is a real commit-ingestion gap)"),
    }
