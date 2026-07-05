"""Compositions — the composer's primitive: a saved, forkable spec over the graph.

The front end was never a page; it's the *composer* — the place where intent becomes a
composition over neutral primitives. A composition is a small op-tree the substrate
executes. It unifies the *watch* (a saved subscription) and the *lens* (a saved query)
into ONE first-class object, so opinion lives in the composition the USER owns — not
welded into engine code. Claude authors them from a sentence (the MCP tools); the
substrate runs them; the views render them.

The op set is small and CLOSED — grounded in Palantir's Object Set API + Notion's
rollups, which independently land on the same vocabulary (see docs/COMPOSER.md). Anything
the ops can't express is a Function (a named transform), never a new op.

Ops (neutral, composable — the equivalent of Notion's filter/relation/rollup):
  {"op":"subject"}                                 -> the object you're looking at
  {"op":"select","object_type":?,"where":[...]}    -> objects matching conditions (.filter)
  {"op":"traverse","from":N,"direction":,"hops":}  -> objects N hops away (.searchAround)
  {"op":"collect","from":N,"properties":[],"transform":?} -> the values of those props
  {"op":"subtract","left":N,"right":N}             -> values in left not in right (.subtract)
  {"op":"union","sets":[N,...]}                     -> combine sets (.union)
  {"op":"intersect","sets":[N,...]}                 -> objects/values in ALL sets (.intersect)
  {"op":"aggregate","from":N,"group_by":[],"metric":{...}} -> group + a metric (.groupBy / rollup)
  {"op":"table","from":N,"columns":[...]}          -> one ROW per object, columns = a property
       OR a rollup-over-a-link (Notion's database+rollups / Palantir's object-set+per-object
       aggregate). column = {"name":,"property":P} | {"name":,"rollup":{"direction":in|out|both,
       "link_type":?,"object_type":?,"of":count|first|max|min|sum|avg,"property":?}}. `first` =
       Notion's show-original (pluck a single relation's value, incl. an object column like
       `canonical` — how a linked commit/entity is named).
  {"op":"order","from":N,"by":?,"dir":}            -> rank a set/rows (.orderBy)
  {"op":"take","from":N,"n":K}                      -> top-N (.take)
  {"op":"sections","sections":[{"title":,"body":N},...]} -> stack named sub-compositions into
       one titled read-model (Notion's page-of-blocks). Each body is its own op-tree; the
       result is {title: rendered-items}. This is what a "briefing"/"dossier" IS — a page of
       compositions, not bespoke code.
  {"op":"function","name":,"args":{}}              -> a registered Function (the escape hatch)

The old `discrepancy` read-model is just one composition (opinion left the engine):
  subtract( collect(location, country) over traverse(subject, 2 hops),
            collect(home-props, country) over subject )

There is deliberately NO generic `join` — relating two sets is `intersect` (set algebra)
or `traverse` (a link), and fuzzy matching (screening) is a Function. Caps (Palantir's,
load-tested): `traverse` ≤ 3 hops, `aggregate` ≤ 3 group_by dimensions.
"""

from __future__ import annotations

import json
import re
import uuid
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg

from src.ontology.resolution import screen_network
from src.orchestrator.coinvest import coinvestment_ties
from src.orchestrator.discrepancy import _HOME_PROPS, country_of
from src.orchestrator.frontier import subject_report
from src.orchestrator.monitor import match_condition

# Named pure transforms a `collect` op may apply to a value. Kept tiny and neutral —
# `country` is the only domain helper, shared with the (soon-vestigial) discrepancy code.
_TRANSFORMS: dict[str, Any] = {
    "identity": lambda v: v,
    "country": country_of,
    "lower": lambda v: v.lower() if isinstance(v, str) else v,
}

# Functions — the escape hatch (Palantir's exact split: a small closed op set + arbitrary
# registered logic for anything the ops can't express). A read-model whose precision lives
# in domain logic — merge-aware cluster resolution, a platform-degree filter, multi-signal
# fuzzy matching — is a FUNCTION, not a worse pure-op rewrite. Registering it here lets a
# forkable composition REFERENCE it ({"op":"function","name":...}), so the opinion leaves
# engine code and becomes a named, listable, swappable artifact the user owns — without
# losing a drop of the analytics. The subject passed to `run_composition` is the function's
# anchor (an entity for coinvest/screen; a case for subject_report).
Function = Callable[[asyncpg.Pool, uuid.UUID | None, dict[str, Any]], Awaitable[Any]]


async def _fn_coinvest(pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]) -> Any:
    assert subject is not None  # the op guard requires a subject for this Function
    return await coinvestment_ties(
        pool, subject,
        limit=int(args.get("limit", 25)), platform_degree=int(args.get("platform_degree", 12)),
    )


async def _fn_subject_report(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    assert subject is not None
    return await subject_report(pool, subject)  # `subject` is the case id here


async def _fn_screen(pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]) -> Any:
    assert subject is not None
    return await screen_network(pool, subject, min_len=int(args.get("min_len", 5)))


# NB: `briefing` was a hand-written Function (three SQL queries). It is GONE — it decomposed
# into a `sections` op-tree (see BRIEFING below): each section is a pure select→table, so the
# "where am I?" read-model is now a composition the user owns, not bespoke code. This is the
# canonical proof that a "briefing"/"dossier" is a PAGE OF COMPOSITIONS, never a coded page.

_CANON_MAX_HITS = 12
_CANON_SNIPPET = 500


def _canon_sections(body: str) -> list[tuple[str, str]]:
    """Split a markdown reference into (heading, text) sections by `## ` headers. Text before
    the first `##` (minus the H1) is the '(overview)' section. Pure."""
    parts = re.split(r"^##\s+(.+)$", body, flags=re.M)
    out: list[tuple[str, str]] = []
    intro = re.sub(r"^#\s+.+$", "", parts[0], count=1, flags=re.M).strip()
    if intro:
        out.append(("(overview)", intro))
    for i in range(1, len(parts), 2):
        out.append((parts[i].strip(), parts[i + 1].strip() if i + 1 < len(parts) else ""))
    return out


def _trim(text: str) -> str:
    return text[:_CANON_SNIPPET] + ("…" if len(text) > _CANON_SNIPPET else "")


async def _fn_canon(pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]) -> Any:
    """Consult the design canon — Palantir/Notion's models + Osiris's own docs, ingested as
    `Reference` objects (src/ingest/reference.py). 'Cite, don't re-derive': given a query (a
    topic, a `grounds` module path, or a design word), return the matching canon SECTIONS
    ranked, each with its source + the module it grounds. Empty query → the canon index (one
    overview row per reference). Subject-FREE — it answers a design question, not an entity;
    this is what a designer (human or Claude, via `consult_canon`) calls BEFORE re-deriving a
    problem Palantir/Notion already solved (the closed op set, aggregation caps, the kinetic
    write path, the renderer's view rules)."""
    q = str(args.get("q", "")).strip().lower()
    refs = await pool.fetch(
        "SELECT "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='name') AS title, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='vendor') AS vendor, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='topic') AS topic, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='grounds') AS grounds, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='source_url') AS source, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='body') AS body "
        "FROM objects o WHERE o.type='Reference' AND o.status='active'"
    )
    if not q:  # the index — one overview row per reference, ordered by vendor then title
        index = []
        for r in sorted(refs, key=lambda x: (x["vendor"] or "", x["title"] or "")):
            secs = _canon_sections(r["body"] or "")
            index.append({"reference": r["title"], "vendor": r["vendor"],
                          "grounds": r["grounds"], "source": r["source"],
                          "text": _trim(secs[0][1]) if secs else ""})
        return {"Design canon — Palantir · Notion · own docs": index}
    hits: list[tuple[int, dict[str, Any]]] = []
    for r in refs:
        meta = " ".join(
            filter(None, [r["title"], r["topic"], r["vendor"], r["grounds"]])).lower()
        meta_score = 3 if q in meta else 0      # the whole reference is about this
        for heading, text in _canon_sections(r["body"] or ""):
            score = (meta_score + (2 if q in heading.lower() else 0)
                     + (1 if q in text.lower() else 0))
            if score <= 0:
                continue
            hits.append((score, {
                "reference": r["title"], "vendor": r["vendor"], "grounds": r["grounds"],
                "section": heading, "source": r["source"], "text": _trim(text),
            }))
    hits.sort(key=lambda h: h[0], reverse=True)
    return {f'Canon — "{q}"': [h[1] for h in hits[:_CANON_MAX_HITS]]}


# NB: `decisions` was a hand-written Function (a SQL query joining decided_in → commit). It is
# GONE — it decomposed into DECISION_LOG (a `sections` op-tree): select Decision (summary-present)
# → table with `of:"first"` rollups plucking the decided_in commit's canonical + date → order.
# The show-original rollup is what made this possible without abusing max(). A kind filter is now
# a `where` on the select the user forks in, not a bespoke arg.


# config roles where two repos' files SHOULD agree in CONTENT (legal/config), unlike prose
# (readme/changelog/contributing) which are expected to differ — so the drift audit skips prose.
_CONFIG_ROLES = frozenset(
    {"license", "gitignore", "editorconfig", "makefile", "ci", "manifest", "dockerfile"})


async def _family_repos(pool: asyncpg.Pool, args: dict[str, Any]) -> dict[uuid.UUID, str]:
    """Repos in scope for a family audit — those with an ingested file tree, optionally filtered
    to `args.repos`. Returns {repo_id: name}; a family needs ≥2 (caller guards)."""
    want = {str(w).lower() for w in (args.get("repos") or [])}
    repos = await pool.fetch(
        "SELECT o.id, (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='name' LIMIT 1) AS name "
        "FROM objects o WHERE o.type='SoftwareProject' AND o.status='active'")
    have_files = {r["repo"] for r in await pool.fetch(
        "SELECT DISTINCT l.to_id AS repo FROM links l "
        "JOIN objects f ON f.id=l.from_id AND f.type='File' WHERE l.type='in_repo'")}
    return {r["id"]: r["name"] for r in repos
            if r["name"] and r["id"] in have_files and (not want or r["name"].lower() in want)}


async def _fn_family(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """Family consistency audit — make a set of similar repos a real FAMILY by auditing what
    drifted across them. Compares repos by file ROLE (license/readme/ci/manifest/…): for each
    role, which repos have it and which LACK it. A role present in some-but-not-all is an
    inconsistency (a missing license, a CI one repo skipped). Subject-free; `args.repos` scopes
    the family (default: every repo whose tree is ingested). CANDIDATE-GATED — blocks by role,
    never all-files-all-pairs — so it holds as the family grows. (Presence audit; content drift
    is `family_drift`.)"""
    rmap = await _family_repos(pool, args)
    if len(rmap) < 2:
        return {"Family consistency": [{"note": "need ≥2 repos with ingested file trees"}]}
    pairs = await pool.fetch(
        "SELECT DISTINCT l.to_id AS repo, a.value #>> '{}' AS role "
        "FROM links l JOIN objects f ON f.id=l.from_id AND f.type='File' "
        "JOIN current_assertions a ON a.object_id=f.id AND a.name='role' "
        "WHERE l.type='in_repo' AND l.to_id = ANY($1::uuid[])", list(rmap))
    role_repos: dict[str, set[uuid.UUID]] = {}
    for p in pairs:
        role_repos.setdefault(p["role"], set()).add(p["repo"])
    everyone = set(rmap)
    rows = []
    for role in role_repos:
        missing = everyone - role_repos[role]
        rows.append({"role": role,
                     "have": ", ".join(sorted(rmap[i] for i in role_repos[role])),
                     "missing": ", ".join(sorted(rmap[i] for i in missing)) or "—",
                     "consistent": not missing})
    rows.sort(key=lambda r: (r["consistent"], r["role"]))   # inconsistencies (the findings) first
    return {f"Family consistency — {len(rmap)} repos ({', '.join(sorted(rmap.values()))})": rows}


async def _fn_family_drift(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """Content-drift audit — the deeper layer over the presence audit. For the files a family
    SHARES, do they AGREE? License TYPE across the family (MIT in one repo, Apache in another is
    a real inconsistency), and config files (gitignore/ci/editorconfig/makefile/manifest/
    dockerfile) byte-identical or diverged. Prose roles (readme/changelog/contributing) are
    EXPECTED to differ, so they're excluded. Reads pre-computed hashes/license-types from the
    graph (content stays in git). Role-blocked, so it scales like the presence audit."""
    rmap = await _family_repos(pool, args)
    if len(rmap) < 2:
        return {"Family content drift": [{"note": "need ≥2 repos with ingested file trees"}]}
    rows = await pool.fetch(
        "SELECT l.to_id AS repo, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=f.id AND a.name='role' LIMIT 1) AS role, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=f.id AND a.name='content_hash' LIMIT 1) AS h, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=f.id AND a.name='license_type' LIMIT 1) AS lt "
        "FROM links l JOIN objects f ON f.id=l.from_id AND f.type='File' "
        "WHERE l.type='in_repo' AND l.to_id = ANY($1::uuid[])", list(rmap))
    # role -> repo -> [hashes]; a repo may have several files of one role (e.g. CI workflows),
    # so its signature is all of them combined — then signatures compare across repos.
    sigs: dict[str, dict[uuid.UUID, list[str]]] = {}
    ltypes: dict[uuid.UUID, set[str]] = {}
    for r in rows:
        if r["role"] in _CONFIG_ROLES and r["h"]:
            sigs.setdefault(r["role"], {}).setdefault(r["repo"], []).append(r["h"])
            if r["role"] == "license" and r["lt"]:
                ltypes.setdefault(r["repo"], set()).add(r["lt"])
    out = []
    for role, per in sigs.items():
        if len(per) < 2:               # only one repo has it → presence audit's job, not drift
            continue
        if role == "license":
            types = {rmap[rid]: "/".join(sorted(ltypes.get(rid) or {"unknown"})) for rid in per}
            drift = len(set(types.values())) > 1
            detail = ", ".join(f"{r}: {t}" for r, t in sorted(types.items()))
        else:
            signatures = {rid: "|".join(sorted(hs)) for rid, hs in per.items()}
            distinct = len(set(signatures.values()))
            drift = distinct > 1
            repos_l = ", ".join(sorted(rmap[rid] for rid in per))
            detail = (f"{distinct} distinct versions across {repos_l}" if drift
                      else f"identical across {repos_l}")
        out.append({"role": role, "drift": drift, "detail": detail})
    out.sort(key=lambda x: (not x["drift"], x["role"]))       # drift (the findings) first
    return {f"Family content drift — {len(rmap)} repos": out}


async def _fn_project(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """One project's scope — its recent commits, decisions, and file roles, as lists. The drill
    from the project browser (`projects`). The subject is the repo (focus it, or pass
    `args.repo` = the name). Volume as lists, never a graph."""
    repo = subject
    if repo is None and args.get("repo"):
        repo = await pool.fetchval(
            "SELECT o.id FROM objects o JOIN current_assertions a ON a.object_id=o.id "
            "WHERE o.type='SoftwareProject' AND a.name='name' AND a.value #>> '{}'=$1 LIMIT 1",
            str(args["repo"]))
    if repo is None:
        return {"Project": [{"note": "focus a repo, or pass args.repo = its name"}]}
    name = await pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='name' LIMIT 1",
        repo) or "(project)"
    commits = await pool.fetch(
        "SELECT (SELECT value #>> '{}' FROM current_assertions a WHERE a.object_id=c.id "
        "        AND a.name='summary' LIMIT 1) AS summary, "
        " (SELECT value #>> '{}' FROM current_assertions a WHERE a.object_id=c.id "
        "        AND a.name='authored_date' LIMIT 1) AS date "
        "FROM links l JOIN objects c ON c.id=l.from_id AND c.type='Commit' "
        "WHERE l.to_id=$1 AND l.type='in_repo' ORDER BY date DESC NULLS LAST LIMIT 15", repo)
    decisions = await pool.fetch(
        "SELECT DISTINCT ON (d.id) "
        " (SELECT value #>> '{}' FROM current_assertions a WHERE a.object_id=d.id "
        "  AND a.name='summary' LIMIT 1) AS summary, "
        " (SELECT value #>> '{}' FROM current_assertions a WHERE a.object_id=d.id "
        "  AND a.name='kind' LIMIT 1) AS kind "
        "FROM objects d JOIN links dl ON dl.from_id=d.id AND dl.type='decided_in' "
        "JOIN links rl ON rl.from_id=dl.to_id AND rl.type='in_repo' AND rl.to_id=$1 "
        "WHERE d.type='Decision' LIMIT 20", repo)
    roles = await pool.fetch(
        "SELECT DISTINCT (SELECT value #>> '{}' FROM current_assertions a WHERE a.object_id=f.id "
        "        AND a.name='role' LIMIT 1) AS role "
        "FROM links l JOIN objects f ON f.id=l.from_id AND f.type='File' "
        "WHERE l.to_id=$1 AND l.type='in_repo'", repo)
    role_list = sorted(r["role"] for r in roles if r["role"])
    return {
        f"{name} — recent commits": [
            {"when": (c["date"] or "")[:10], "commit": c["summary"]}
            for c in commits if c["summary"]],
        f"{name} — decisions": [
            {"kind": d["kind"], "decision": d["summary"]} for d in decisions if d["summary"]],
        f"{name} — file roles": [{"roles": ", ".join(role_list) or "—"}],
    }


_WORD = re.compile(r"[a-z][a-z0-9_+]{3,}")   # a term ≥4 chars — the derivation's unit
_EXT = re.compile(r"\.([a-z0-9]{1,6})$")     # a file extension


async def _fn_portfolio(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """The developer's project portfolio, DERIVED — for each ingested repo: its STACK (top file
    types), what it's ABOUT (the terms DISTINCTIVE to it), and its size. The substrate half of
    cross-repo cognition: it gathers the candidate signal deterministically; NAMING the shared
    primitive that recurs across repos is a judgment left to the lens (Claude on the MCP) or the
    tripwire (a small model on the cron) — gather here, judge there.

    'Distinctive' = LOW document-frequency (a term in ≤ ~a third of repos), which is the whole
    trick: the terms every repo shares are generic noise ('config', 'test', 'while'); the signal
    is in the rare terms that name a repo ('cgroup'/'metered' → a throttle; 'miracast'/'airplay'
    → a cast receiver). So generic words fall out FOR FREE with no stoplist — the same 'never
    the whole flat corpus, only the distinctive bounded set' law the resolver uses. Subject-free;
    `args.repos` scopes it, `args.terms` caps the per-repo term count (default 10)."""
    want = {str(w).lower() for w in (args.get("repos") or [])}
    rmap = {r["id"]: r["name"] for r in await pool.fetch(
        "SELECT o.id, (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='name' LIMIT 1) AS name "
        "FROM objects o WHERE o.type='SoftwareProject' AND o.status='active'")
        if r["name"] and (not want or r["name"].lower() in want)}
    if not rmap:
        return {"Portfolio": [{"note": "no ingested repos"}]}

    exts: dict[uuid.UUID, Counter[str]] = {rid: Counter() for rid in rmap}
    for r in await pool.fetch(
        "SELECT l.to_id AS repo, o.canonical FROM links l "
        "JOIN objects o ON o.id=l.from_id AND o.type='File' "
        "WHERE l.type='in_repo' AND l.to_id = ANY($1::uuid[])", list(rmap)):
        m = _EXT.search((r["canonical"] or "").lower())
        if m and r["repo"] in exts:
            exts[r["repo"]][m.group(1)] += 1

    tf: dict[uuid.UUID, Counter[str]] = {rid: Counter() for rid in rmap}
    ncommits: Counter[uuid.UUID] = Counter()
    for r in await pool.fetch(
        "SELECT l.to_id AS repo, "
        " (SELECT value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='summary' LIMIT 1) AS s, "
        " (SELECT value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='rationale' LIMIT 1) AS r "
        "FROM links l JOIN objects o ON o.id=l.from_id AND o.type='Commit' "
        "WHERE l.type='in_repo' AND l.to_id = ANY($1::uuid[])", list(rmap)):
        if r["repo"] not in tf:
            continue
        ncommits[r["repo"]] += 1
        for t in set(_WORD.findall(f"{r['s'] or ''} {r['r'] or ''}".lower())):
            tf[r["repo"]][t] += 1

    docfreq: Counter[str] = Counter()
    for c in tf.values():
        docfreq.update(c.keys())
    cutoff = max(2, round(len(rmap) * 0.35))     # distinctive: in ≤ ~a third of repos
    cap = max(1, int(args.get("terms", 10)))

    rows = []
    for rid, name in rmap.items():
        distinctive = sorted(
            (t for t in tf[rid] if docfreq[t] <= cutoff and tf[rid][t] >= 2),
            key=lambda t: (docfreq[t], -tf[rid][t]))       # rarest-across-repos first
        rows.append({
            "repo": name,
            "stack": ", ".join(e for e, _ in exts[rid].most_common(4)) or "—",
            "about": ", ".join(distinctive[:cap]) or "—",
            "commits": ncommits[rid],
        })
    rows.sort(key=lambda x: -x["commits"])
    return {f"Portfolio — {len(rmap)} repos": rows}


# The digest reads the pulse LOG on demand, so its liveness verdict is re-derived every time
# you look — the loop can die (the daemon stops) without the dead-man's-switch dying with it.
# A newest-pulse older than this ⇒ the heartbeat is DEAD, surfaced as the lead row.
_PULSE_STALE = timedelta(minutes=45)
# Default window: aggregate findings across the last 24h of pulses, ANCHORED at the most recent
# one (so a stale/dead loop still shows what changed before it stopped) — not just the single
# last tick. Bounded so a fast `--watch` loop can't return thousands of rows.
_PULSE_WINDOW = timedelta(hours=24)
_PULSE_CAP = 200
_PULSE_TITLE = "Pulse — what changed while you were away"


def _ago(delta: timedelta) -> str:
    """A compact human age: 'under a minute' / 'N minutes' / 'N hours' / 'N days'."""
    mins = max(0, int(delta.total_seconds())) // 60
    if mins < 1:
        return "under a minute"
    if mins < 60:
        return f"{mins} minute{'s' if mins != 1 else ''}"
    hours = mins // 60
    if hours < 24:
        return f"{hours} hour{'s' if hours != 1 else ''}"
    days = hours // 24
    return f"{days} day{'s' if days != 1 else ''}"


def _pulse_now(args: dict[str, Any]) -> datetime:
    """'Now' for the staleness verdict — overridable (a datetime or ISO string) so tests can
    freeze it; production reads the wall clock ON EACH LOOK (that IS the dead-man's-switch)."""
    v = args.get("now")
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=UTC)
    if isinstance(v, str):
        try:
            d = datetime.fromisoformat(v)
            return d if d.tzinfo else d.replace(tzinfo=UTC)
        except ValueError:
            pass
    return datetime.now(UTC)


async def _fn_pulse(
    pool: asyncpg.Pool, subject: uuid.UUID | None, args: dict[str, Any]
) -> Any:
    """The heartbeat digest — what the off-the-clock loop (src/orchestrator/pulse.py) found
    while you were away, aggregated across a real WINDOW of pulses (newest first), not just the
    last tick. (The old bug: `LIMIT 1` read only the newest pulse, so a quiet last tick lied
    'no pulse yet' with a full log, and a loop that DIED with its daemon looked like one that
    never ran.) It ALWAYS leads with a heartbeat-status row derived ON READ: a newest pulse
    older than ~45 min ⇒ the loop is DEAD — the lens re-checks liveness every look, so it can't
    die with the daemon (the dead-man's-switch). `args.last` overrides the window to the last N
    pulses; default = the last 24h of pulses (bounded)."""
    override = args.get("last")
    cap = max(1, int(override)) if override is not None else _PULSE_CAP
    rows = await pool.fetch(
        "SELECT ran_at, synced, findings FROM dev_pulses ORDER BY id DESC LIMIT $1", cap)
    if not rows:                                    # the loop has NEVER run — keep the how-to row
        return {_PULSE_TITLE: [
            {"finding": "no pulse yet — run `python -m src.orchestrator.pulse`",
             "when": "—", "synced": "—"}]}

    last_ran = rows[0]["ran_at"]
    age = _pulse_now(args) - last_ran
    if age > _PULSE_STALE:                          # DEAD — lead with the dead-since row
        status = {"finding": f"heartbeat DEAD — no pulse since {str(last_ran)[:19]} "
                             f"({_ago(age)} ago); the loop is not running",
                  "when": str(last_ran)[:19], "synced": "—"}
    else:
        status = {"finding": f"heartbeat alive — last pulse {_ago(age)} ago",
                  "when": str(last_ran)[:19], "synced": "—"}

    # explicit `last` ⇒ exactly those N pulses; else the last 24h anchored at the newest pulse.
    window = rows if override is not None else [
        r for r in rows if r["ran_at"] >= last_ran - _PULSE_WINDOW]
    findings: list[dict[str, Any]] = []
    for r in window:
        for finding in (_coerce(r["findings"]) or []):
            findings.append({"finding": finding, "when": str(r["ran_at"])[:19],
                             "synced": ", ".join(_coerce(r["synced"]) or []) or "—"})
    if not findings:                                # pulses ran, nothing changed in the window
        findings = [{"finding": f"quiet — no changes across {len(window)} pulse"
                                f"{'s' if len(window) != 1 else ''} since "
                                f"{str(window[-1]['ran_at'])[:19]}",
                     "when": str(last_ran)[:19], "synced": "—"}]
    return {_PULSE_TITLE: [status, *findings]}


_FUNCTIONS: dict[str, Function] = {
    "coinvest": _fn_coinvest,
    "subject_report": _fn_subject_report,
    "screen_network": _fn_screen,
    "canon": _fn_canon,
    "family": _fn_family,
    "family_drift": _fn_family_drift,
    "portfolio": _fn_portfolio,
    "pulse": _fn_pulse,
    "project": _fn_project,
}

# Functions that brief the whole project rather than anchor on one entity — no subject needed.
# `project` is here too: it drills into ONE repo, taken from the focused subject OR `args.repo`,
# so it must run without a bound subject (it returns a "focus a repo" note if given neither).
# NB: `projects`, `briefing`, `decisions` are GONE as Functions — they decomposed into pure
# op-trees (a `table`, a `sections`, a `sections`+show-original — see DEFAULT_COMPOSITIONS):
# opinion → primitives the user owns.
_SUBJECT_FREE = {"canon", "family", "family_drift", "portfolio", "pulse", "project"}


def list_functions() -> list[str]:
    """The registered Functions a composition may reference (the authoring channel reads
    this to know what's beyond the closed op set)."""
    return sorted(_FUNCTIONS)


# Guardrails adopted from Palantir's Object Set API (load-tested, not arbitrary).
MAX_TRAVERSE_HOPS = 3
MAX_AGGREGATE_DIMS = 3


@dataclass
class Result:
    """A composition's output — an object set, a value list, or aggregate rows."""

    kind: str  # "objects" | "values" | "rows" | "data"
    objects: list[uuid.UUID] = field(default_factory=list)
    values: list[str] = field(default_factory=list)
    rows: list[dict[str, Any]] = field(default_factory=list)
    data: Any = None  # a Function's native output (list/dict) — opaque to the ops


def _coerce(v: Any) -> Any:
    return json.loads(v) if isinstance(v, str) else v


def _num(v: Any) -> float | None:
    """Best-effort numeric coercion for ordering/aggregation; None if not a number."""
    if isinstance(v, bool):  # bool is an int subclass — exclude it
        return None
    if isinstance(v, int | float):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v.replace(",", "").strip())
        except ValueError:
            return None
    return None


async def _props(pool: asyncpg.Pool, oid: uuid.UUID) -> dict[str, str]:
    # The current value of each property is the WINNING assertion across sources, resolved
    # by evidence GRADE first (constitution #5: SELF_DECLARED > … > DERIVED), then recency —
    # so a fresh DERIVED re-assertion never overrides an older SELF_DECLARED one (the miner
    # re-opening a thread a session already resolved must NOT win). `confidence` is the
    # faithful projection of the class, so it ranks grade; matches ontology/export.py.
    rows = await pool.fetch(
        "SELECT DISTINCT ON (name) name, value #>> '{}' AS v FROM current_assertions "
        "WHERE object_id=$1 ORDER BY name, confidence DESC, observed_at DESC",
        oid,
    )
    return {r["name"]: r["v"] for r in rows}


def _distinct[T](values: list[T]) -> list[T]:
    seen: set[T] = set()
    out: list[T] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _setop[T](op: str, lists: list[list[T]]) -> list[T]:
    """union (concat+dedup) or intersect, preserving the first set's order."""
    if not lists:
        return []
    if op == "intersect":
        common = set(lists[0]).intersection(*(set(x) for x in lists[1:]))
        return [x for x in _distinct(lists[0]) if x in common]
    return _distinct([x for lst in lists for x in lst])


async def _eval(pool: asyncpg.Pool, node: dict[str, Any], subject: uuid.UUID | None) -> Result:
    op = node.get("op")

    if op == "subject":
        return Result("objects", objects=[subject] if subject else [])

    if op == "select":
        ot = node.get("object_type")
        cp = node.get("canonical_prefix")
        where = node.get("where", []) or []
        rows = await pool.fetch(
            "SELECT id FROM objects WHERE status='active' AND ($1::text IS NULL OR type=$1) "
            "AND ($2::text IS NULL OR canonical LIKE $2 || '%')", ot, cp
        )
        out: list[uuid.UUID] = []
        for r in rows:
            facts = await _props(pool, r["id"])
            if all(match_condition(facts.get(c.get("property")), c.get("op", "contains"),
                                   c.get("value")) for c in where):
                out.append(r["id"])
        return Result("objects", objects=out)

    if op == "traverse":
        base = await _eval(pool, node["from"], subject)
        seeds = base.objects
        direction = node.get("direction", "both")
        hops = min(int(node.get("hops", 1)), MAX_TRAVERSE_HOPS)
        ltype = node.get("link_type")
        seen, frontier = set(seeds), set(seeds)
        for _ in range(hops):
            if not frontier:
                break
            ids = list(frontier)
            clause = {
                "out": "from_id = ANY($1::uuid[])",
                "in": "to_id = ANY($1::uuid[])",
            }.get(direction, "(from_id = ANY($1::uuid[]) OR to_id = ANY($1::uuid[]))")
            rows = await pool.fetch(
                "SELECT CASE WHEN from_id = ANY($1::uuid[]) THEN to_id ELSE from_id END AS n "
                f"FROM links WHERE {clause} AND ($2::text IS NULL OR type=$2)",
                ids, ltype,
            )
            nxt = {r["n"] for r in rows} - seen
            seen |= nxt
            frontier = nxt
        # the neighbourhood is everything reached EXCEPT the seeds themselves
        return Result("objects", objects=[i for i in seen if i not in set(seeds)])

    if op == "collect":
        base = await _eval(pool, node["from"], subject)
        props = node.get("properties", []) or []
        transform = _TRANSFORMS.get(node.get("transform", "identity"), _TRANSFORMS["identity"])
        vals: list[str] = []
        for oid in base.objects:
            facts = await _props(pool, oid)
            for p in props:
                v = facts.get(p)
                if v is None:
                    continue
                t = transform(v)
                if t:
                    vals.append(t)
        return Result("values", values=_distinct(vals))

    if op == "subtract":
        left = await _eval(pool, node["left"], subject)
        right = await _eval(pool, node["right"], subject)
        rset = set(right.values)
        return Result("values", values=[v for v in left.values if v not in rset])

    if op in ("union", "intersect"):
        sets = [await _eval(pool, s, subject) for s in node.get("sets", [])]
        if not sets:
            return Result("objects")
        kind = sets[0].kind
        if any(s.kind != kind for s in sets) or kind == "rows":
            raise ValueError(f"{op} requires same-kind object/value sets")
        if kind == "objects":
            return Result("objects", objects=_setop(op, [s.objects for s in sets]))
        return Result("values", values=_setop(op, [s.values for s in sets]))

    if op == "aggregate":
        base = await _eval(pool, node["from"], subject)
        group_by = node.get("group_by", []) or []
        if len(group_by) > MAX_AGGREGATE_DIMS:
            raise ValueError(f"aggregate supports ≤{MAX_AGGREGATE_DIMS} group_by dimensions")
        metric = node.get("metric", {"type": "count"}) or {"type": "count"}
        return Result("rows", rows=await _aggregate(pool, base.objects, group_by, metric))

    if op == "table":
        base = await _eval(pool, node["from"], subject)
        return Result("rows", rows=await _table(pool, base.objects, node.get("columns", []) or []))

    if op == "order":
        base = await _eval(pool, node["from"], subject)
        return await _order(pool, base, node.get("by"), node.get("dir", "asc"))

    if op == "take":
        base = await _eval(pool, node["from"], subject)
        n = max(0, int(node.get("n", 0)))
        return Result(base.kind, objects=base.objects[:n], values=base.values[:n],
                      rows=base.rows[:n], data=base.data)

    if op == "sections":
        # a page of compositions: eval each section's body, render it to items, key by title.
        # This is Notion's page-of-blocks / a "briefing" — the shape every subject-free
        # read-model already returns, now composed from primitives instead of hand-written SQL.
        data: dict[str, Any] = {}
        for sec in node.get("sections", []) or []:
            title = str(sec.get("title", "section"))
            body = sec.get("body")
            res = await _eval(pool, body, subject) if body else Result("values")
            data[title] = await _package(pool, res)
        return Result("data", data=data)

    if op == "function":
        name = str(node.get("name", ""))
        fn = _FUNCTIONS.get(name)
        if fn is None:
            raise ValueError(f"unknown function: {name!r}")
        if subject is None and name not in _SUBJECT_FREE:
            raise ValueError(f"function {name!r} requires a subject")
        return Result("data", data=await fn(pool, subject, node.get("args", {}) or {}))

    raise ValueError(f"unknown composition op: {op!r}")


async def _table(
    pool: asyncpg.Pool, objects: list[uuid.UUID], columns: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """One ROW per object; each column is a property value or a rollup over a link (Notion's
    database+rollups / Palantir's object-set + per-object aggregate). Over a bounded set (a
    select/traverse result), so per-object queries are fine — the whole point is that the SET
    is already candidate-gated by the op that produced it."""
    rows: list[dict[str, Any]] = []
    for oid in objects:
        facts = await _props(pool, oid)
        row: dict[str, Any] = {}
        for col in columns:
            name = str(col.get("name") or col.get("property") or "col")
            if "property" in col:
                row[name] = facts.get(str(col["property"]))
            elif "rollup" in col:
                row[name] = await _rollup(pool, oid, col["rollup"])
            else:
                row[name] = None
        rows.append(row)
    return rows


# object columns a rollup may pluck directly (not assertions) — the canonical is how a linked
# Commit/entity is named, so `show-original` over a link needs it. Whitelisted → safe to inline.
_OBJ_COLS = frozenset({"canonical", "type", "status"})


async def _rollup(pool: asyncpg.Pool, oid: uuid.UUID, spec: dict[str, Any]) -> Any:
    """A single rollup over one object's links: count / max / min / sum / avg of a property on
    the objects reached by `link_type` in `direction`, optionally filtered to `object_type`.
    `of:"first"` is Notion's SHOW-ORIGINAL — for a single relation, plucks the one related
    object's value (a property OR an object column like `canonical`); the enabler for showing
    a linked commit's hash/date without abusing max()."""
    direction = spec.get("direction", "both")
    ltype = spec.get("link_type")
    if direction == "in":
        clause, rel = "l.to_id = $1", "l.from_id"
    elif direction == "out":
        clause, rel = "l.from_id = $1", "l.to_id"
    else:
        clause = "(l.from_id = $1 OR l.to_id = $1)"
        rel = "CASE WHEN l.from_id = $1 THEN l.to_id ELSE l.from_id END"
    rids = [r["rid"] for r in await pool.fetch(
        f"SELECT {rel} AS rid FROM links l WHERE {clause} AND ($2::text IS NULL OR l.type=$2)",
        oid, ltype)]
    otype = spec.get("object_type")
    if otype and rids:
        rids = [r["id"] for r in await pool.fetch(
            "SELECT id FROM objects WHERE id = ANY($1::uuid[]) AND type=$2", rids, otype)]
    of = spec.get("of", "count")
    if of == "count":
        return len(rids)
    prop = spec.get("property")
    if not rids or not prop:
        return None
    if str(prop) in _OBJ_COLS:            # an object column (canonical/type/status), whitelisted
        values = [r["v"] for r in await pool.fetch(
            f"SELECT {prop} AS v FROM objects WHERE id = ANY($1::uuid[]) ORDER BY {prop}",
            rids) if r["v"] is not None]
    else:
        values = [r["v"] for r in await pool.fetch(
            "SELECT DISTINCT ON (object_id) value #>> '{}' AS v FROM current_assertions "
            "WHERE object_id = ANY($1::uuid[]) AND name=$2 ORDER BY object_id, observed_at DESC",
            rids, str(prop)) if r["v"] is not None]
    if not values:
        return None
    if of == "first":                     # show-original: the single related object's value
        return values[0]
    if of == "max":
        return max(values)
    if of == "min":
        return min(values)
    nums = [n for n in (_num(v) for v in values) if n is not None]
    if of == "sum":
        return sum(nums)
    if of == "avg":
        return sum(nums) / len(nums) if nums else None
    return None


async def _aggregate(
    pool: asyncpg.Pool, objects: list[uuid.UUID], group_by: list[str], metric: dict[str, Any]
) -> list[dict[str, Any]]:
    """Group objects by property values, compute one metric per group (Palantir groupBy /
    Notion rollup). group_by=[] aggregates the whole set into a single row."""
    mtype = metric.get("type", "count")
    field_name = metric.get("field")
    groups: dict[tuple[str | None, ...], list[dict[str, str]]] = {}
    for oid in objects:
        facts = await _props(pool, oid)
        key = tuple(facts.get(g) for g in group_by)
        groups.setdefault(key, []).append(facts)
    rows: list[dict[str, Any]] = []
    for key, members in groups.items():
        group = {g: k for g, k in zip(group_by, key, strict=True)}
        if mtype == "count":
            value: float | int = len(members)
        else:
            raw = [m.get(field_name) for m in members] if field_name else []
            if mtype == "cardinality":
                value = len({v for v in raw if v is not None})
            else:
                nums = [n for n in (_num(v) for v in raw) if n is not None]
                value = _metric_over(mtype, nums)
        rows.append({"group": group, "metric": value})
    return rows


def _metric_over(mtype: str, nums: list[float]) -> float:
    if not nums:
        return 0.0
    if mtype == "sum":
        return sum(nums)
    if mtype == "avg":
        return sum(nums) / len(nums)
    if mtype == "min":
        return min(nums)
    if mtype == "max":
        return max(nums)
    raise ValueError(f"unknown aggregate metric: {mtype!r}")


async def _order(
    pool: asyncpg.Pool, base: Result, by: str | None, direction: str
) -> Result:
    rev = direction == "desc"
    if base.kind == "rows":
        def rkey(r: dict[str, Any]) -> tuple[float, str]:
            if by in (None, "metric"):
                v = r.get("metric")
            elif by in r:                       # a flat `table` column
                v = r.get(by)
            else:                               # an aggregate group dimension
                v = r.get("group", {}).get(by)
            n = _num(v)
            return (n if n is not None else 0.0, str(v))
        return Result("rows", rows=sorted(base.rows, key=rkey, reverse=rev))
    if base.kind == "values":
        def vkey(v: str) -> tuple[float, str]:
            n = _num(v)
            return (n if n is not None else float("inf"), v)
        return Result("values", values=sorted(base.values, key=vkey, reverse=rev))
    # objects — order by a property (numeric if possible, else lexical)
    keyed: list[tuple[float, str, uuid.UUID]] = []
    for oid in base.objects:
        raw = (await _props(pool, oid)).get(by) if by else None
        n = _num(raw)
        keyed.append((n if n is not None else float("inf"), str(raw or ""), oid))
    keyed.sort(key=lambda t: (t[0], t[1]), reverse=rev)
    return Result("objects", objects=[t[2] for t in keyed])


# --- persistence + run ------------------------------------------------------

async def save_composition(
    pool: asyncpg.Pool, name: str, spec: dict[str, Any], kind: str = "lens",
    *, webhook_url: str | None = None, active: bool = True, room_id: uuid.UUID | None = None,
) -> uuid.UUID:
    """Save (or update) a composition by name. Fork = save under a new name. `webhook_url`
    and `active` are a watch's execution metadata (a lens ignores them). `room_id` scopes it
    to a stance (NULL = unassigned; a re-save without a room keeps the existing one)."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "INSERT INTO compositions (name, kind, spec, webhook_url, active, room_id) "
        "VALUES ($1,$2,$3,$4,$5,$6) "
        "ON CONFLICT (name) DO UPDATE SET spec=EXCLUDED.spec, kind=EXCLUDED.kind, "
        "  webhook_url=EXCLUDED.webhook_url, active=EXCLUDED.active, "
        "  room_id=COALESCE(EXCLUDED.room_id, compositions.room_id) RETURNING id",
        name, kind, spec, webhook_url, active, room_id,
    )


async def save_watch(
    pool: asyncpg.Pool, name: str, object_type: str | None, where: list[dict[str, Any]],
    *, canonical_prefix: str | None = None, webhook_url: str | None = None,
    room_id: uuid.UUID | None = None,
) -> uuid.UUID:
    """Save a WATCH — a composition whose spec is a `select` op. The same spec runs as a
    lens (current members) and drives the evaluator (alert on a new member). One primitive."""
    spec: dict[str, Any] = {"op": "select", "object_type": object_type, "where": where}
    if canonical_prefix:
        spec["canonical_prefix"] = canonical_prefix
    return await save_composition(pool, name, spec, "watch", webhook_url=webhook_url,
                                  room_id=room_id)


async def _spec_of(pool: asyncpg.Pool, ref: str) -> dict[str, Any] | None:
    row = await pool.fetchrow(
        "SELECT spec FROM compositions WHERE name=$1 OR id::text=$1", ref
    )
    return _coerce(row["spec"]) if row else None


# --- rooms: the stance (a Room is a composition of compositions — authorable by Claude) --

async def create_room(
    pool: asyncpg.Pool, name: str, config: dict[str, Any] | None = None
) -> uuid.UUID:
    """Create (or update) a Room — a saved stance the operator switches between. The FDE
    move: Claude mints one from a sentence, then assigns compositions to it."""
    return await pool.fetchval(  # type: ignore[no-any-return]
        "INSERT INTO rooms (name, config) VALUES ($1,$2) "
        "ON CONFLICT (name) DO UPDATE SET config=EXCLUDED.config RETURNING id",
        name, config or {},
    )


async def resolve_room(pool: asyncpg.Pool, ref: str | None) -> uuid.UUID | None:
    """A room by name or id (None ref → None = the All/unassigned scope)."""
    if not ref:
        return None
    return await pool.fetchval(  # type: ignore[no-any-return]
        "SELECT id FROM rooms WHERE name=$1 OR id::text=$1", ref
    )


async def list_rooms(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    return [
        {"id": str(r["id"]), "name": r["name"], "config": _coerce(r["config"])}
        for r in await pool.fetch("SELECT id, name, config FROM rooms ORDER BY created_at")
    ]


async def list_compositions(
    pool: asyncpg.Pool, room_id: uuid.UUID | None = None
) -> list[dict[str, Any]]:
    """Saved compositions. `room_id` scopes to a stance (None = all rooms — the god view)."""
    return [
        {"id": str(r["id"]), "name": r["name"], "kind": r["kind"], "spec": _coerce(r["spec"]),
         "webhook_url": r["webhook_url"], "active": r["active"],
         "room_id": str(r["room_id"]) if r["room_id"] else None}
        for r in await pool.fetch(
            "SELECT id, name, kind, spec, webhook_url, active, room_id FROM compositions "
            "WHERE ($1::uuid IS NULL OR room_id=$1) ORDER BY created_at", room_id
        )
    ]


async def object_items(pool: asyncpg.Pool, ids: list[uuid.UUID]) -> list[dict[str, Any]]:
    """Label a result set's objects AND carry their compact properties — in two batch
    queries, not N. The view-switcher needs this: the Graph view uses label/type, the
    Table view shows property columns (sector, date, …) without a per-row fetch."""
    if not ids:
        return []
    objs = await pool.fetch(
        "SELECT id, type, canonical FROM objects WHERE id = ANY($1::uuid[])", ids
    )
    props: dict[uuid.UUID, dict[str, str]] = {}
    for r in await pool.fetch(
        "SELECT DISTINCT ON (object_id, name) object_id, name, value #>> '{}' AS v "
        "FROM current_assertions WHERE object_id = ANY($1::uuid[]) "
        "ORDER BY object_id, name, observed_at DESC",
        ids,
    ):
        props.setdefault(r["object_id"], {})[r["name"]] = r["v"]
    meta = {o["id"]: o for o in objs}
    out: list[dict[str, Any]] = []
    for oid in ids:  # preserve the composition's order
        o = meta.get(oid)
        if o is None:
            continue
        p = props.get(oid, {})
        # the best human label — never a raw hash when a name/title/summary exists
        label = (p.get("name") or p.get("title") or p.get("summary")
                 or p.get("subject") or o["canonical"])
        out.append({"id": str(oid), "type": o["type"], "canonical": o["canonical"],
                    "label": label, "props": p})
    return out


async def _package(pool: asyncpg.Pool, res: Result) -> Any:
    """Render a Result into the items the generic renderer consumes — objects become labelled
    rows, rows/values/data pass through. Shared by `run_spec` and the `sections` op (a section
    body is packaged exactly as a top-level composition would be)."""
    if res.kind == "objects":
        return await object_items(pool, res.objects)
    if res.kind == "rows":
        return res.rows
    if res.kind == "data":
        return res.data  # a Function's / sections' native output, passed through untouched
    return res.values


async def run_spec(
    pool: asyncpg.Pool, spec: dict[str, Any], subject: uuid.UUID | None = None,
    name: str = "(spec)",
) -> dict[str, Any]:
    """Evaluate an op-tree and package the Result for the generic renderer. The inline
    composer (W4) runs an EPHEMERAL working spec through here as you edit chips — no save."""
    res = await _eval(pool, spec, subject)
    items = await _package(pool, res)
    count = len(items) if isinstance(items, list | dict) else 1
    return {"composition": name, "kind": res.kind, "count": count, "items": items, "spec": spec}


async def run_composition(
    pool: asyncpg.Pool, ref: str, subject: uuid.UUID | None = None
) -> dict[str, Any]:
    """Execute a saved composition (by name or id), optionally against a subject."""
    spec = await _spec_of(pool, ref)
    if spec is None:
        return {"error": f"no composition {ref!r}"}
    return await run_spec(pool, spec, subject, name=ref)


# --- default compositions (templates — the engine's opinions, now forkable) --
# `operational-vs-disclosed-geography` IS the former `discrepancy` read-model, expressed
# as a composition: opinion has left the engine — it's a named, forkable spec the user
# owns. (Single-subject; the cluster-following nuance of discrepancy.py is left aside.)
GEOGRAPHY_DISCREPANCY: dict[str, Any] = {
    "op": "subtract",
    "left": {"op": "collect", "transform": "country", "properties": ["location"],
             "from": {"op": "traverse", "from": {"op": "subject"},
                      "direction": "both", "hops": 2}},
    "right": {"op": "collect", "transform": "country", "properties": list(_HOME_PROPS),
              "from": {"op": "subject"}},
}
# `briefing` — "where am I?" restored in one read (the arrival prosthesis: a returning human
# and a fresh Claude share the same zero-context state). Formerly three hand-written SQL queries;
# now a `sections` op-tree — each section a pure select→(order→take)→table. Opinion left the
# engine: the briefing is a page of compositions the user can fork, not coded output.
BRIEFING: dict[str, Any] = {
    "op": "sections",
    "sections": [
        {"title": "Open threads — what's unresolved",
         "body": {"op": "table",
                  "from": {"op": "select", "object_type": "Thread",
                           "where": [{"property": "status", "op": "eq", "value": "open"}]},
                  "columns": [{"name": "thread", "property": "summary"}]}},
        {"title": "Recent work — what just happened",
         "body": {"op": "table",
                  "from": {"op": "take", "n": 8,
                           "from": {"op": "order", "by": "authored_date", "dir": "desc",
                                    "from": {"op": "select", "object_type": "Commit",
                                             "where": [{"property": "summary",
                                                        "op": "present"}]}}},
                  "columns": [{"name": "change", "property": "summary"},
                              {"name": "scope", "property": "scope"},
                              {"name": "when", "property": "authored_date"}]}},
        {"title": "Resolved — self-healed by later commits",
         "body": {"op": "table",
                  "from": {"op": "select", "object_type": "Thread",
                           "where": [{"property": "status", "op": "eq", "value": "resolved"}]},
                  "columns": [{"name": "thread", "property": "summary"},
                              {"name": "by", "property": "resolved_in"},
                              {"name": "because", "property": "resolved_because"}]}},
    ],
}
# `decision-log` — the project's WHY, mined into `Decision` objects. Formerly a SQL Function;
# now a single-section `sections` op-tree: the `of:"first"` (show-original) rollup plucks each
# decision's `decided_in` commit canonical + date (the renderer formats the ISO date), ordered
# newest-first. `summary present` drops empty rows. A kind filter = a `where` the user forks in.
DECISION_LOG: dict[str, Any] = {
    "op": "sections",
    "sections": [
        {"title": "Decisions — the project's WHY (mined from commit rationale)",
         "body": {"op": "order", "by": "when", "dir": "desc",
                  "from": {"op": "table",
                           "from": {"op": "select", "object_type": "Decision",
                                    "where": [{"property": "summary", "op": "present"}]},
                           "columns": [
                               {"name": "decision", "property": "summary"},
                               {"name": "kind", "property": "kind"},
                               {"name": "in", "rollup": {"direction": "out",
                                    "link_type": "decided_in", "of": "first",
                                    "property": "canonical"}},
                               {"name": "when", "rollup": {"direction": "out",
                                    "link_type": "decided_in", "of": "first",
                                    "property": "authored_date"}},
                           ]}}},
    ],
}
DEFAULT_COMPOSITIONS: dict[str, dict[str, Any]] = {
    "operational-vs-disclosed-geography": GEOGRAPHY_DISCREPANCY,
    # the arrival briefing — a `sections` op-tree, no longer a hand-written Function.
    "briefing": BRIEFING,
    # the former bespoke read-models, now forkable compositions over named Functions —
    # opinion left engine code (no more hardcoded read-model + bespoke MCP tool per lens).
    "co-investment-ties": {"op": "function", "name": "coinvest"},
    "who-is-this": {"op": "function", "name": "subject_report"},
    "screen-financing-network": {"op": "function", "name": "screen_network"},
    # the dedicated canon view: the project's design memory (Palantir/Notion + own docs),
    # rendered as a sectioned read-model. Run with no subject; `consult_canon(q)` queries it.
    "design-canon": {"op": "function", "name": "canon", "args": {}},
    # the decision log: the project's WHY — a `sections` op-tree (was a Function).
    "decision-log": DECISION_LOG,
    # the family audit: what drifted across a set of similar repos (every ingested family).
    "family-consistency": {"op": "function", "name": "family"},
    # content drift: for the files a family shares, do they AGREE (license type / config bytes)?
    "family-drift": {"op": "function", "name": "family_drift"},
    # the portfolio map: every ingested repo's stack + what it's ABOUT (distinctive terms) —
    # cross-repo cognition's gather step; the lens/tripwire names the shared primitives.
    "portfolio": {"op": "function", "name": "portfolio"},
    # the heartbeat digest: what the off-the-clock pulse found while you were away.
    "pulse-digest": {"op": "function", "name": "pulse"},
    # the fleet roster — every Claude instance registered in the shared graph, its model and
    # project. "A man and all his imaginary friends", on the human console. A pure op-tree
    # (select Agent → table), not a hardcoded panel: the composer discipline holds even here.
    "fleet": {
        "op": "table",
        "from": {"op": "select", "object_type": "Agent"},
        "columns": [
            {"name": "agent", "property": "name"},
            {"name": "model", "property": "source_model"},
            {"name": "project", "property": "project"},
        ],
    },
    # the developer project browser — DECOMPOSED: no longer a hardcoded Function, a pure op-tree
    # the user owns. `select` the repos → `table` with rollup columns (Notion database+rollups)
    # → `order` by last-touched. This is what "everything is composed" means in practice.
    "projects": {
        "op": "order", "by": "last_touched", "dir": "desc",
        "from": {
            "op": "table",
            "from": {"op": "select", "object_type": "SoftwareProject"},
            "columns": [
                {"name": "project", "property": "name"},
                {"name": "commits", "rollup": {"direction": "in", "link_type": "in_repo",
                                               "object_type": "Commit", "of": "count"}},
                {"name": "files", "rollup": {"direction": "in", "link_type": "in_repo",
                                             "object_type": "File", "of": "count"}},
                {"name": "last_touched", "rollup": {"direction": "in", "link_type": "in_repo",
                                                    "object_type": "Commit", "of": "max",
                                                    "property": "authored_date"}},
            ],
        },
    },
    "project": {"op": "function", "name": "project"},
}


async def seed_default_compositions(pool: asyncpg.Pool) -> int:
    for name, spec in DEFAULT_COMPOSITIONS.items():
        await save_composition(pool, name, spec, "lens")
    return len(DEFAULT_COMPOSITIONS)
