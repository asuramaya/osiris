"""The heartbeat — the autonomic loop that makes the developer persona come alive.

A pile of lenses only moves when you query it. This is the involuntary pulse that runs WITHOUT
you: each tick it SENSES change (a repo's HEAD moved), re-INGESTS it, re-runs the lenses
(REFLECT), and ACCUMULATES the delta vs the last pulse as FINDINGS — so you return to a "what
changed since I last looked" digest assembled off the clock. Deterministic + cheap (the bg-Claude
reflection layer comes later). It NEVER mutates your repos — it reads, it tells (the brain and
the alarm clock, not the hands).

Run once:    `python -m src.orchestrator.pulse [repo-path ...]`
Run forever: `python -m src.orchestrator.pulse --watch 300 [repo-path ...]`
Repos default to OSIRIS_DEV_REPOS (comma-separated paths).
"""

from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.config.settings import get_settings
from src.db.pool import create_pool
from src.ingest.decisions import mine_decisions
from src.ingest.files import ingest_files
from src.ingest.gitlog import ingest_repo
from src.ingest.threads import mine_threads, resolve_threads
from src.ontology.resolution import find_dev_identity_candidates
from src.orchestrator.monitor import get_cursor, set_cursor

_CONFIG_ROLES = (
    "license", "gitignore", "editorconfig", "makefile", "ci", "manifest", "dockerfile")


def _git_head(path: str) -> str | None:
    try:
        return subprocess.run(["git", "-C", path, "rev-parse", "HEAD"],
                              capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def repo_name(path: str) -> str | None:
    """The repo's basename (its `repo:<name>` identity), or None if `path` isn't a git repo."""
    try:
        top = subprocess.run(["git", "-C", path, "rev-parse", "--show-toplevel"],
                             capture_output=True, text=True, check=True).stdout.strip()
        return Path(top).name
    except (OSError, subprocess.CalledProcessError):
        return None


async def _snapshot(pool: Any) -> dict[str, Any]:
    """The metrics a pulse diffs on — cheap graph queries, no Claude. The developer graph's
    vital signs: commits/repo, open threads, decisions, review candidates, drifting roles."""
    repos = {
        r["name"]: r["c"] for r in await pool.fetch(
            "SELECT (SELECT value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
            "        AND a.name='name' "
            "        ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS name, "
            " (SELECT count(*) FROM links l JOIN objects c ON c.id=l.from_id AND c.type='Commit' "
            "  WHERE l.to_id=o.id AND l.type='in_repo') AS c "
            "FROM objects o WHERE o.type='SoftwareProject' AND o.status='active'")
        if r["name"]
    }
    open_threads = await pool.fetchval(
        "SELECT count(*) FROM objects o WHERE o.type='Thread' AND EXISTS ("
        " SELECT 1 FROM current_assertions s WHERE s.object_id=o.id AND s.name='status' "
        " AND s.value #>> '{}' = 'open')")
    decisions = await pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='Decision' AND status='active'")
    candidates = await pool.fetchval(
        "SELECT count(*) FROM merge_candidates WHERE resolved IS NULL")
    drift_rows = await pool.fetch(
        "SELECT role FROM ("
        " SELECT (SELECT value #>> '{}' FROM current_assertions a WHERE a.object_id=f.id "
        "         AND a.name='role' "
        "         ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS role, "
        "  (SELECT value #>> '{}' FROM current_assertions a WHERE a.object_id=f.id "
        "         AND a.name='content_hash' "
        "         ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1) AS h "
        " FROM objects f WHERE f.type='File') t "
        "WHERE role = ANY($1::text[]) AND h IS NOT NULL "
        "GROUP BY role HAVING count(DISTINCT h) > 1", list(_CONFIG_ROLES))
    return {"repos": repos, "open_threads": open_threads or 0, "decisions": decisions or 0,
            "candidates": candidates or 0, "drift_roles": sorted(r["role"] for r in drift_rows)}


def _diff(prev: dict[str, Any], cur: dict[str, Any]) -> list[str]:
    """The findings — what changed since the last pulse, in plain language."""
    out: list[str] = []
    pr = prev.get("repos", {})
    for name, c in cur["repos"].items():
        if name not in pr:
            out.append(f"now tracking {name} ({c} commits)")
        elif c > pr[name]:
            n = c - pr[name]
            out.append(f"{n} new commit{'s' if n != 1 else ''} in {name}")
    # (noun, plural) — pluralize the NOUN, not the trailing phrase ("2 decisions recorded",
    # never "2 decision recordeds")
    for one, many, d in (
        ("open thread", "open threads",
         cur["open_threads"] - prev.get("open_threads", 0)),
        ("decision recorded", "decisions recorded",
         cur["decisions"] - prev.get("decisions", 0)),
        ("identity/merge candidate to review", "identity/merge candidates to review",
         cur["candidates"] - prev.get("candidates", 0)),
    ):
        if d > 0:
            out.append(f"{d} new {one if d == 1 else many}")
    for role in sorted(set(cur["drift_roles"]) - set(prev.get("drift_roles", []))):
        out.append(f"new content drift in '{role}' across the family")
    return out


async def pulse(
    actions: Actions, repos: list[tuple[str, str]], *, now: datetime
) -> dict[str, Any]:
    """One heartbeat. SENSE which repos' HEAD moved → re-ingest those (commits + tree) → re-run
    the deterministic lenses (decisions/threads/identity) → SNAPSHOT the vitals → diff vs the
    last pulse → record the FINDINGS. Returns what synced + what changed. Read-only on the repos."""
    pool = actions.pool
    synced: list[str] = []
    for name, path in repos:
        head = _git_head(path)
        if head is None:
            continue
        if head != await get_cursor(pool, f"devhead:{name}"):
            await ingest_repo(actions, path)
            await ingest_files(actions, path)
            await set_cursor(pool, f"devhead:{name}", head)
            synced.append(name)
    if synced:                                  # only re-reflect when something actually moved
        # THE OBSERVATIONS ABOVE ARE FREE AND ALWAYS RUN: a repo's HEAD moved, so its commits and
        # its tree enter the graph. That is sensing, it cannot be wrong, and nothing here gates it.
        #
        # WHAT FOLLOWS IS INFERENCE, AND IT IS DARK BY DEFAULT (operator's ruling, 2026-07-14).
        # `mine_decisions` and `mine_threads` decide that a SENTENCE IN A COMMIT BODY is a durable
        # decision or a standing duty. That is a judgement, and both failed the licence the fleet
        # already lives under:
        #     mine_threads    26 minted,  9 judged,  1 admitted  -> 11.1%   (floor is 15%)
        #     mine_decisions  41 minted,  0 judged,  0 admitted  -> NOBODY HAS EVER USED ONE
        #
        # AND THIS IS HOW THEY SURVIVED SO LONG: they cost NOTHING. The daily ceiling gates on
        # measured dollars and they spend zero. The adversary's licence was keyed to the adversary.
        # The miner kill-switch named `session-miner` and this is a different daemon entirely — it
        # was still minting thirteen hours after we "killed the miner".
        #
        #     THE CHARTER'S LINE IS OBSERVE vs INFER, NOT PAID vs FREE. A critter may observe for
        #     nothing; it may only infer on a licence. BEING FREE IS NOT A LICENCE — it is merely
        #     the reason nobody was watching.
        #
        # Nothing is lost by darkening them: GIT ALREADY HAS THE COMMIT BODIES, in full, with the
        # context the fragment strips away. What died here was never knowledge — it was a keyword
        # match wearing a duty's clothes, and two of the last nine it minted were OUR OWN LAWS,
        # filed back at us as chores.
        if get_settings().osiris_mine_commits:
            await mine_decisions(actions)
            await mine_threads(actions)
            await resolve_threads(actions)
        await find_dev_identity_candidates(pool)
    cur = await _snapshot(pool)
    prev = await pool.fetchrow("SELECT snapshot FROM dev_pulses ORDER BY id DESC LIMIT 1")
    if prev is None:
        findings = [f"baseline — tracking {len(cur['repos'])} repos, "
                    f"{sum(cur['repos'].values())} commits, {cur['decisions']} decisions"]
    else:
        findings = _diff(prev["snapshot"], cur)
    await pool.execute(
        "INSERT INTO dev_pulses (ran_at, synced, snapshot, findings) VALUES ($1,$2,$3,$4)",
        now, synced, cur, findings)
    return {"synced": synced, "findings": findings}


def _repos(argv: list[str]) -> list[tuple[str, str]]:
    paths = [a for a in argv if not a.startswith("--")]
    if not paths:
        paths = [p.strip() for p in get_settings().osiris_dev_repos.split(",") if p.strip()]
    out: list[tuple[str, str]] = []
    for p in paths:
        nm = repo_name(p)
        if nm:
            out.append((nm, p))
    return out


def main() -> None:  # pragma: no cover - CLI
    import asyncio
    import sys
    from datetime import UTC

    argv = sys.argv[1:]
    interval: int | None = None
    if "--watch" in argv:
        i = argv.index("--watch")
        interval = int(argv[i + 1])
        argv = argv[:i] + argv[i + 2:]
    repos = _repos(argv)

    async def tick() -> None:
        pool = await create_pool(
            get_settings().database_url, application_name="osiris-script:pulse")
        try:
            res = await pulse(Actions(pool), repos, now=datetime.now(UTC))
            stamp = datetime.now(UTC).strftime("%H:%M:%S")
            print(f"[{stamp}] synced={res['synced']} | " + (
                "; ".join(res["findings"]) if res["findings"] else "no change"))
        finally:
            await pool.close()

    async def loop() -> None:
        while True:
            await tick()
            await asyncio.sleep(interval or 0)

    asyncio.run(loop() if interval else tick())


if __name__ == "__main__":  # pragma: no cover
    main()
