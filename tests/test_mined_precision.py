"""Miner precision + re-mine reconciliation — regression from the 2026-07 live audit.

The receipts below are ACTUAL rows that landed on the operator's briefing (garbage) or
must keep landing there (legit). Each garbage case must NOT be mined; each legit case
MUST be; ruling restatements collapse to ONE Decision; and a re-mine HEALS the graph —
stale mined objects are archived via the event-sourced set_status (reversible), while a
human's deliberate archive is never overridden by the cron.
"""
from __future__ import annotations

import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.ingest.decisions import extract_decisions, mine_decisions
from src.ingest.project import ingest_project
from src.ingest.threads import extract_threads, mine_threads, resolve_threads
from src.orchestrator.capture import open_thread

NOW = datetime(2026, 7, 1, tzinfo=UTC)

# --- the receipts -----------------------------------------------------------

GARBAGE_THREADS = (
    # mid-sentence fragment, dangling closer — a split artifact, not a thread
    "anchor), app start gated on the migrate one-shot, satellite behind a profile",
    # the miner ate the commit message DOCUMENTING its own marker list
    "author-intended markers (NEXT: / THE WALL / gated on / needs-a-key / not-yet-live);",
    # marker-enumeration WITHOUT the word "markers" — the enumeration guard alone must catch it
    "Tuned the flags (NEXT: / THE WALL / gated on) shown in the tray",
    # a commit describing the miner; the marker appears only inside quotes + trailing cut-off
    'only the exact phrase "THE WALL" (a real',
    # a resolved design note; "X WALL:" heading is not "THE WALL"
    "INSPECTOR WALL: a long property value (commit rationale, doc body) now CLAMPS to",
)
LEGIT_THREADS = (
    "live compose needs a key",  # lowercase start is a REAL thread shape — no case guard
    "THE WALL: running this live needs a satellite on a box with Harris-portal access",
    "marked built (cron ladder), not yet live",
)

GARBAGE_DECISIONS = (
    # starts mid-sentence with a quote fragment (uppercase, so a case guard can't catch it)
    'Leon" vs "Daniel Leon") render as separate principals — Person never auto-merges '
    "(ruling #3), so each Form D name string stands",
    # a note about TUNING THE MINER, previously mined as a ruling
    "Tightened `override` to decision-shaped phrasings + a lowercase-fragment guard → "
    "11 clean decisions (ruling #3 the never-auto-merge- Person rule appears 5×)",
)
LEGIT_DECISIONS = (
    ("Self-hosted single-operator runtime (Cloudflare edge scratched)", "rejection"),
    ("Triggers as a pure projection of manifests (ruling #5)", "ruling"),
    ("DELIBERATELY NOT DONE: acronym-aware cross-base for SPV LPs (BP <-> Brilliant Phoenix) "
     '— token-overlap on "neuralink" would merge every Neuralink SPV', "rejection"),
    ("Aim the watch at a real persona beat — the foreclosure broker (ForeScan's customer) — "
     "and build the subscriber-facing surface: a sourced tripwire feed, deliberately NOT a CRM",
     "rejection"),
    ("Deliberately NOT added to sources.py — that registry is the per-entity investigation "
     "playbook", "rejection"),
)


# --- pure extraction: every receipt, verbatim --------------------------------

def test_garbage_threads_are_not_mined() -> None:
    for text in GARBAGE_THREADS:
        assert extract_threads(text) == [], f"garbage thread survived: {text!r}"


def test_legit_threads_survive() -> None:
    for text in LEGIT_THREADS:
        got = extract_threads(text)
        assert len(got) == 1 and got[0] == text, f"legit thread lost: {text!r} -> {got!r}"


def test_garbage_decisions_are_not_mined() -> None:
    for text in GARBAGE_DECISIONS:
        assert extract_decisions(text) == [], f"garbage decision survived: {text!r}"


def test_legit_decisions_survive_with_kind() -> None:
    for text, kind in LEGIT_DECISIONS:
        got = extract_decisions(text)
        assert got == [(text, kind)], f"legit decision lost: {text!r} -> {got!r}"


# --- DB helpers ---------------------------------------------------------------

async def _commit(actions: Actions, canon: str, rationale: str, date: str) -> Any:
    cm = await actions.create_or_find_object("Commit", canon, "git")
    await actions.assert_property(cm, "rationale", rationale, "git", NOW, 0.85)
    await actions.assert_property(cm, "authored_date", date, "git", NOW, 0.85)
    return cm


async def _active(pool: Any, type_: str) -> dict[str, str]:
    """canonical -> summary for the ACTIVE objects of a mined type."""
    rows = await pool.fetch(
        "SELECT o.canonical, (SELECT value #>> '{}' FROM current_assertions a "
        " WHERE a.object_id=o.id AND a.name='summary') AS s "
        "FROM objects o WHERE o.type=$1 AND o.status='active'", type_)
    return {r["canonical"]: r["s"] for r in rows}


# --- restatement dedup: ruling #3 five ways is ONE decision --------------------

async def test_ruling_restatements_collapse_to_one(actions: Actions) -> None:
    await _commit(actions, "commit:a",
                  "Probabilistic ER (ontology/resolution.py) — never auto-merges Person "
                  "(ruling #3).", "2026-06-20T00:00:00+00:00")
    await _commit(actions, "commit:b",
                  "Still never auto-flags (ruling #3).", "2026-06-22T00:00:00+00:00")
    await _commit(actions, "commit:c",
                  "Never asserts same-entity (ruling #3).", "2026-06-24T00:00:00+00:00")
    await _commit(actions, "commit:d",  # a DIFFERENT ruling must stay distinct
                  "Triggers as a pure projection of manifests (ruling #5).",
                  "2026-06-21T00:00:00+00:00")

    res = await mine_decisions(actions)
    assert res["decisions"] == 2                       # ruling-3 (collapsed) + ruling-5
    active = await _active(actions.pool, "Decision")
    r3 = [s for s in active.values() if "ruling #3" in s]
    assert len(r3) == 1                                # five-ways-restated → ONE decision
    assert r3[0].startswith("Probabilistic ER")        # the EARLIEST statement is the keeper
    assert any("ruling #5" in s for s in active.values())
    # the keeper is decided_in the EARLIEST commit only
    n = await actions.pool.fetchval("SELECT count(*) FROM links WHERE type='decided_in'")
    assert n == 2
    # idempotent: a re-mine adds nothing, archives nothing
    again = await mine_decisions(actions)
    assert again["decisions"] == 2 and again["archived"] == 0
    assert len(await _active(actions.pool, "Decision")) == 2


# --- reconciliation: a re-mine HEALS, and it is event-sourced -------------------

async def test_remine_archives_stale_thread_and_resurrects(actions: Actions) -> None:
    p = actions.pool
    cm = await _commit(actions, "commit:x",
                       "THE WALL: the live scrape needs a vantage on the county box.",
                       "2026-06-25T00:00:00+00:00")
    res = await mine_threads(actions)
    assert res["threads"] == 1 and res["archived"] == 0
    tid = await p.fetchval("SELECT id FROM objects WHERE type='Thread'")

    # the rationale is superseded — the old miner would have left the stale thread forever
    await actions.assert_property(cm, "rationale", "Refactor only, nothing blocked.",
                                  "git", NOW, 0.85)
    healed = await mine_threads(actions)
    assert healed["threads"] == 0 and healed["archived"] == 1
    assert await p.fetchval("SELECT status FROM objects WHERE id=$1", tid) == "archived"
    # event-sourced: the archive is an object_event by the miner, hence reversible
    ev = await p.fetchrow(
        "SELECT actor FROM object_events WHERE object_id=$1 AND event_type='archive'", tid)
    assert ev is not None and ev["actor"] == "git-memory"

    # the text comes back → the miner resurrects ITS OWN archive
    await actions.assert_property(
        cm, "rationale", "THE WALL: the live scrape needs a vantage on the county box.",
        "git", NOW, 0.85)
    back = await mine_threads(actions)
    assert back["resurrected"] == 1
    assert await p.fetchval("SELECT status FROM objects WHERE id=$1", tid) == "active"


async def test_remine_never_overrides_a_human_archive(actions: Actions) -> None:
    p = actions.pool
    await _commit(actions, "commit:y", "NEXT: wire the delivery sink for the broker beat.",
                  "2026-06-25T00:00:00+00:00")
    await mine_threads(actions)
    tid = await p.fetchval("SELECT id FROM objects WHERE type='Thread'")
    # the OPERATOR archives it deliberately; the text still exists in the commit record
    await actions.set_status(tid, "archived", "operator curation", "analyst:priya")
    again = await mine_threads(actions)
    assert again["resurrected"] == 0                   # curation is sacred
    assert await p.fetchval("SELECT status FROM objects WHERE id=$1", tid) == "archived"


async def test_remine_archives_stale_decision(actions: Actions) -> None:
    p = actions.pool
    cm = await _commit(actions, "commit:z", "We chose to keep the set (multi-source).",
                       "2026-06-25T00:00:00+00:00")
    await mine_decisions(actions)
    did = await p.fetchval("SELECT id FROM objects WHERE type='Decision'")
    await actions.assert_property(cm, "rationale", "Nothing decided here.", "git", NOW, 0.85)
    healed = await mine_decisions(actions)
    assert healed["decisions"] == 0 and healed["archived"] == 1
    assert await p.fetchval("SELECT status FROM objects WHERE id=$1", did) == "archived"


async def test_remine_link_dedup_never_inflates(actions: Actions) -> None:
    await _commit(actions, "commit:w", "THE WALL: live email needs a real cred for SMTP.",
                  "2026-06-25T00:00:00+00:00")
    await mine_threads(actions)
    await mine_threads(actions)                        # the old code appended a dup per run
    n = await actions.pool.fetchval("SELECT count(*) FROM links WHERE type='noted_in'")
    assert n == 1


# --- end-to-end through a real git repo (the ingest_project path) ---------------

def _git(repo: Path, *args: str) -> None:
    env = {**os.environ, "GIT_AUTHOR_NAME": "Ada", "GIT_AUTHOR_EMAIL": "ada@x.io",
           "GIT_COMMITTER_NAME": "Ada", "GIT_COMMITTER_EMAIL": "ada@x.io"}
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, env=env)


async def test_end_to_end_receipts_through_git(actions: Actions, tmp_path: Path) -> None:
    repo = tmp_path / "proj"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Ada")
    _git(repo, "config", "user.email", "ada@x.io")
    (repo / "a.txt").write_text("1")
    _git(repo, "add", ".")
    # the deploy-shaped body whose `; `-split used to yield the `anchor), …` fragment,
    # wrapped mid-sentence like a real commit body, plus a REAL wall that must survive
    _git(repo, "commit", "-q", "-m",
         "feat(deploy): hosting cuts\n\n"
         "Compose file (postgres, redis, one-shot migrate gate; the surfaces share one "
         "PG+Redis bus via a YAML\nanchor), app start gated on the migrate one-shot, "
         "satellite behind a profile.\n"
         "THE WALL: running this live needs a satellite on a box with Harris-portal access.")
    (repo / "b.txt").write_text("2")
    _git(repo, "add", ".")
    # a miner-tuning note (must NOT be mined) + a real ruling restatement (must be)
    _git(repo, "commit", "-q", "-m",
         "docs(memory): tune the miner\n\n"
         "Tightened `override` to decision-shaped phrasings + a lowercase-fragment guard "
         "-> 11 clean decisions (ruling #3 appears 5x).\n"
         "Never auto-merges Persons (ruling #3) — only queues for the tray.")

    await ingest_project(actions, str(repo))           # ingest + mine_decisions
    await mine_threads(actions)

    threads = await _active(actions.pool, "Thread")
    assert any("Harris-portal access" in s for s in threads.values())
    assert not any("anchor)" in s for s in threads.values())
    assert not any("gated on the migrate one-shot" in s for s in threads.values())

    decisions = await _active(actions.pool, "Decision")
    r3 = [s for s in decisions.values() if "ruling #3" in s]
    assert r3 == ["Never auto-merges Persons (ruling #3) — only queues for the tray"]
    assert not any("Tightened" in s for s in decisions.values())


# --- the miner must NOT eat a session's write-back (prosthesis boundary) -------

async def _thread_state(pool: Any, tid: Any) -> tuple[str, str | None]:
    """(object status, current 'status' assertion) for a thread — the two ways it can die."""
    obj = await pool.fetchval("SELECT status FROM objects WHERE id=$1", tid)
    assertion = await pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions "
        "WHERE object_id=$1 AND name='status' AND source_id='session'", tid)
    return obj, assertion


async def test_a_session_thread_survives_a_full_remine(actions: Actions) -> None:
    """A session-captured thread must outlive the pulse re-mine. The live bug: resolve_threads
    grabbed EVERY open thread (source-blind), false-resolved a `session` thread off two generic
    shared tokens (`claude`, `local`), and the resulting `git-memory` assertion then made
    reconcile_mined archive it as 'stale'. Both halves must now leave session captures alone —
    while the miner still self-heals its OWN mined threads (the positive control below)."""
    # a session write-back — the operator/Claude opened it, source='session'
    sess = await open_thread(
        actions, "neuralink sanctions screening still needs a manual review pass")

    # a mined thread the miner DOES own, from a commit body carrying a real marker
    await _commit(actions, "commit:spike",
                  "NEXT: the wikidata enricher backfills the qid stubs.",
                  "2026-06-20T00:00:00+00:00")
    # a later commit whose SUMMARY (what resolve_threads tokenizes) collides with BOTH threads:
    # 'wikidata'+'enricher' resolves the mined one; 'neuralink'+'sanctions' would have
    # false-resolved the session one under the old, source-blind code.
    cm = await actions.create_or_find_object("Commit", "commit:later", "git")
    await actions.assert_property(
        cm, "summary",
        "wikidata enricher shipped; neuralink sanctions fusion landed", "git", NOW, 0.85)
    await actions.assert_property(
        cm, "authored_date", "2026-06-28T00:00:00+00:00", "git", NOW, 0.85)

    # two full pulse ticks (mine → resolve, mine → resolve). The SECOND mine is where the old
    # code archived the session thread, once resolve had stamped git-memory onto it.
    for _ in range(2):
        await mine_threads(actions)
        await resolve_threads(actions)

    # the session thread is untouched: object active, its own status still 'open'
    obj_status, sess_status = await _thread_state(actions.pool, sess)
    assert obj_status == "active", "reconcile_mined archived a session write-back"
    assert sess_status == "open", "resolve_threads false-resolved a session thread"
    # and it carries NO git-memory assertion — the miner never reached across the boundary
    crossed = await actions.pool.fetchval(
        "SELECT count(*) FROM assertions WHERE object_id=$1 AND source_id='git-memory'", sess)
    assert crossed == 0

    # positive control: the fix is surgical — the miner STILL self-heals a thread it authored
    mined = await actions.pool.fetchval(
        "SELECT o.id FROM objects o WHERE o.type='Thread' AND EXISTS ("
        "  SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='summary' AND a.value #>> '{}' LIKE 'NEXT: the wikidata%')")
    mined_status = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions "
        "WHERE object_id=$1 AND name='status'", mined)
    assert mined_status == "resolved", "the fix wrongly disabled legitimate self-heal"
