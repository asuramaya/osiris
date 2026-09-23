"""Git-history ingest: Osiris modelling a repository (and, live, itself).

Proves the collector pattern generalizes to a non-OSINT domain: a git history becomes
SoftwareProject / Commit / Person(dev) objects + authored_by / in_repo / follows links,
graded AUTHORITATIVE_API, all through the same Actions waist. Hermetic: a throwaway repo.
"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
from pathlib import Path

from src.actions.core import Actions
from src.ingest.gitlog import declare_machine_identity, ingest_repo, parse_git_log, strip_trailers


def test_parse_git_log_is_tolerant() -> None:
    raw = ("h1\x1fAda\x1fada@x.io\x1f2026-01-01T00:00:00+00:00\x1f\x1fgenesis\x1e"
           "h2\x1fAda\x1fada@x.io\x1f2026-01-02T00:00:00+00:00\x1fh1\x1fsecond\x1e")
    commits = parse_git_log(raw)
    assert [c.subject for c in commits] == ["genesis", "second"]
    assert commits[0].parents == [] and commits[1].parents == ["h1"]


def _git(repo: Path, *args: str) -> None:
    env = {**os.environ, "GIT_AUTHOR_NAME": "Ada", "GIT_AUTHOR_EMAIL": "ada@x.io",
           "GIT_COMMITTER_NAME": "Ada", "GIT_COMMITTER_EMAIL": "ada@x.io"}
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, env=env)


async def test_ingests_a_repo_history(actions: Actions, tmp_path: Path) -> None:
    repo = tmp_path / "proj"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Ada")
    _git(repo, "config", "user.email", "ada@x.io")
    (repo / "a.txt").write_text("1")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "genesis")
    (repo / "a.txt").write_text("2")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "second commit")

    res = await ingest_repo(actions, str(repo))
    assert res == {"repo": "proj", "commits": 2, "developers": 1}

    p = actions.pool
    # the SoftwareProject + 2 Commits + 1 dev Person exist
    assert await p.fetchval("SELECT count(*) FROM objects WHERE type='Commit'") == 2
    assert await p.fetchval(
        "SELECT count(*) FROM objects WHERE type='Person' AND canonical='dev:ada@x.io'"
    ) == 1
    # the genesis commit (no parent) is flagged
    assert await p.fetchval("SELECT count(*) FROM current_assertions WHERE name='genesis'") == 1
    # the DAG + authorship are edges, graded authoritative
    assert await p.fetchval("SELECT count(*) FROM links WHERE type='authored_by'") == 2
    assert await p.fetchval("SELECT count(*) FROM links WHERE type='follows'") == 1
    ec = await p.fetchval("SELECT evidence_class FROM links WHERE type='in_repo' LIMIT 1")
    assert ec == "authoritative_api"


def _dated_commit(repo: Path, *, date: str, message: str, content: str) -> None:
    env = {**os.environ, "GIT_AUTHOR_NAME": "Ada", "GIT_AUTHOR_EMAIL": "ada@x.io",
           "GIT_COMMITTER_NAME": "Ada", "GIT_COMMITTER_EMAIL": "ada@x.io",
           "GIT_AUTHOR_DATE": date, "GIT_COMMITTER_DATE": date}
    (repo / "f").write_text(content)
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True, capture_output=True,
                   timeout=10)
    subprocess.run(["git", "-C", str(repo), "commit", "-q", "-m", message], check=True,
                   capture_output=True, env=env, timeout=10)


def _toplevel(repo: Path) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "--show-toplevel"],
        check=True, capture_output=True, text=True, timeout=10).stdout.strip()


async def test_committed_by_resolves_the_seat_holder_at_the_authors_own_time(
    actions: Actions, tmp_path: Path,
) -> None:
    """COMMITS ATTRIBUTED TO AGENT IDENTITIES: a
    commit made while agent:committer-i held the seat gets committed_by=agent:committer-i,
    even once agent:committer-ii holds it by the time ingest actually runs. The resolver
    reads the HOLDS LINK'S OWN HISTORY at the commit's own author time, never "whoever
    holds it now"."""
    from datetime import UTC, datetime

    from src.orchestrator.seats import bind_holder, bind_seat_tree, ensure_seat

    repo = tmp_path / "committed-by-proj"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Ada")
    _git(repo, "config", "user.email", "ada@x.io")
    toplevel = _toplevel(repo)

    seat = await ensure_seat(actions, house="osiris", handle="CommittedBy1", source="test")
    await actions.create_or_find_object("Agent", "agent:committer-i", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:committer-i")
    bound = await bind_seat_tree(actions, seat_id=seat["seat_id"], tree_cwd=toplevel,
                                 actor="agent:committer-i", because="test")
    assert "error" not in bound, bound

    # GIT'S OWN COMMIT-DATE PRECISION IS WHOLE SECONDS ONLY (no sub-second component
    # survives a round trip through GIT_AUTHOR_DATE/%aI): a commit dated `now()` can
    # store as a timestamp EARLIER, at full DB precision, than the very bind_holder call
    # that ran moments before it in the same wall-clock second. A real sleep across the
    # second boundary is the honest fix, not a smaller and smaller synthetic offset.

    # the FIRST commit's own author time sits inside agent:committer-i's holds window
    await asyncio.sleep(1.1)
    _dated_commit(repo, date=datetime.now(UTC).isoformat(), message="first", content="1")

    # the holder changes BEFORE ingest ever runs: the SECOND commit's own author time
    # sits inside committer-ii's holds window instead, again past the second boundary
    await actions.create_or_find_object("Agent", "agent:committer-ii", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:committer-ii")
    await asyncio.sleep(1.1)
    _dated_commit(repo, date=datetime.now(UTC).isoformat(), message="second", content="2")

    await ingest_repo(actions, str(repo))

    rows = await actions.pool.fetch(
        "SELECT c.canonical AS commit, a.canonical AS committer FROM links l "
        "JOIN objects c ON c.id=l.from_id AND c.type='Commit' "
        "JOIN objects a ON a.id=l.to_id AND a.type='Agent' "
        "WHERE l.type='committed_by' ORDER BY c.canonical")
    by_subject = {}
    for r in rows:
        short = r["commit"].removeprefix("commit:")
        by_subject[short] = r["committer"]
    assert len(by_subject) == 2
    committers = set(by_subject.values())
    assert committers == {"agent:committer-i", "agent:committer-ii"}


async def test_committed_by_never_asserted_when_no_seat_is_bound_to_the_worktree(
    actions: Actions, tmp_path: Path,
) -> None:
    """A bare checkout nobody bound as a seat's tree: never guesses, never mints."""
    repo = tmp_path / "no-seat-proj"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Ada")
    _git(repo, "config", "user.email", "ada@x.io")
    (repo / "a.txt").write_text("1")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "solo")

    await ingest_repo(actions, str(repo))
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE type='committed_by'") == 0


async def test_committed_by_falls_back_to_the_ingest_actor_when_no_seat_is_bound(
    actions: Actions, tmp_path: Path,
) -> None:
    """Piece (1) of the follow-up sequence: a bare
    checkout with no seat bound to it at all is a DIFFERENT shape from a seat that IS
    bound but whose holds history doesn't cover the commit's own author time (that one
    stays a genuine miss, never masked by this fallback). Here there is no seat to ask,
    so the caller's own identity (`actor`, when it has one to offer) is the best
    available signal, scoped to commits this run actually touches."""
    repo = tmp_path / "actor-fallback-proj"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Ada")
    _git(repo, "config", "user.email", "ada@x.io")
    (repo / "a.txt").write_text("1")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "solo")

    await ingest_repo(actions, str(repo), actor="agent:the-ingest-caller")
    committer = await actions.pool.fetchval(
        "SELECT a.canonical FROM links l JOIN objects a ON a.id=l.to_id "
        "WHERE l.type='committed_by' LIMIT 1")
    assert committer == "agent:the-ingest-caller"


async def test_committed_by_worktree_time_wins_over_the_ingest_actor_when_both_apply(
    actions: Actions, tmp_path: Path,
) -> None:
    """The fallback only ever fires when there is NO seat bound at all: a genuinely
    resolved worktree+time holder always wins over whoever happens to be running this
    particular ingest, even when they differ."""
    from datetime import UTC, datetime

    from src.orchestrator.seats import bind_holder, bind_seat_tree, ensure_seat

    repo = tmp_path / "actor-loses-proj"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Ada")
    _git(repo, "config", "user.email", "ada@x.io")
    (repo / "a.txt").write_text("1")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "solo")
    toplevel = _toplevel(repo)

    seat = await ensure_seat(actions, house="osiris", handle="ActorLoses", source="test")
    await actions.create_or_find_object("Agent", "agent:actor-loses-holder", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:actor-loses-holder")
    bound = await bind_seat_tree(actions, seat_id=seat["seat_id"], tree_cwd=toplevel,
                                 actor="agent:actor-loses-holder", because="test")
    assert "error" not in bound, bound
    # the holds link's own first_seen moves into the past, no real-clock wait needed,
    # this test never asks git to date anything
    await actions.pool.execute(
        "UPDATE links SET first_seen=$1 WHERE from_id="
        "(SELECT id FROM objects WHERE canonical='agent:actor-loses-holder') "
        "AND type='holds'", datetime(2020, 1, 1, tzinfo=UTC))

    await ingest_repo(actions, str(repo), actor="agent:the-ingest-caller")
    committer = await actions.pool.fetchval(
        "SELECT a.canonical FROM links l JOIN objects a ON a.id=l.to_id "
        "WHERE l.type='committed_by' LIMIT 1")
    assert committer == "agent:actor-loses-holder"


async def test_reingest_same_history_does_not_regrow_dev_assertions(
    actions: Actions, tmp_path: Path
) -> None:
    """A full re-ingest re-walks EVERY commit each time
    (`--all --reverse`, no since-cursor), so a dev's name/email used to be re-asserted once
    per commit, every ingest run, forever: the worst live triple hit 164,149 rows for 3
    distinct values. Two ingests of the SAME unchanged history must not grow the dev's own
    name/email assertion COUNT (the append-only `assertions` table, not just the
    current-value view)."""
    repo = tmp_path / "proj3"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Ada")
    _git(repo, "config", "user.email", "ada@x.io")
    (repo / "a.txt").write_text("1")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "genesis")

    await ingest_repo(actions, str(repo))
    p = actions.pool
    dev_id = await p.fetchval("SELECT id FROM objects WHERE canonical='dev:ada@x.io'")
    name_rows_1 = await p.fetchval(
        "SELECT count(*) FROM assertions WHERE object_id=$1 AND name='name'", dev_id)
    email_rows_1 = await p.fetchval(
        "SELECT count(*) FROM assertions WHERE object_id=$1 AND name='email'", dev_id)
    assert name_rows_1 == 1 and email_rows_1 == 1

    await ingest_repo(actions, str(repo))  # re-ingest, nothing changed
    name_rows_2 = await p.fetchval(
        "SELECT count(*) FROM assertions WHERE object_id=$1 AND name='name'", dev_id)
    email_rows_2 = await p.fetchval(
        "SELECT count(*) FROM assertions WHERE object_id=$1 AND name='email'", dev_id)
    assert name_rows_2 == name_rows_1, "unchanged name re-asserted on a no-op re-ingest"
    assert email_rows_2 == email_rows_1, "unchanged email re-asserted on a no-op re-ingest"


async def test_a_second_author_name_under_a_known_email_lands_as_an_alias_not_a_rewrite(
    actions: Actions, tmp_path: Path
) -> None:
    """GIT IDENTITIES WEARING THE WRONG NAME: THE INGEST GUARD, superseding
    test_reingest_with_a_real_name_change_still_writes, which asserted the OLD, now-
    retired "last commit's author name becomes `name`" policy. That policy is exactly
    the bug class traced live: the operator's own git identity's displayed name
    flipping to whichever agent's commit happened to touch it last. `name` is now a
    pure function of the (stable) canonical itself, a SECOND author name under the
    SAME known email must never rewrite it, no matter how many ingests see that second
    name. It still isn't silently dropped: it rides along as `author_alias`."""
    repo = tmp_path / "proj4"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Ada")
    _git(repo, "config", "user.email", "ada@x.io")
    (repo / "a.txt").write_text("1")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "genesis")
    await ingest_repo(actions, str(repo))

    (repo / "b.txt").write_text("2")
    _git(repo, "add", ".")
    env = {**os.environ, "GIT_AUTHOR_NAME": "Ada Lovelace", "GIT_AUTHOR_EMAIL": "ada@x.io",
           "GIT_COMMITTER_NAME": "Ada Lovelace", "GIT_COMMITTER_EMAIL": "ada@x.io"}
    await asyncio.to_thread(
        subprocess.run, ["git", "-C", str(repo), "commit", "-q", "-m", "second"],
        check=True, capture_output=True, env=env)
    await ingest_repo(actions, str(repo))

    p = actions.pool
    dev_id = await p.fetchval("SELECT id FROM objects WHERE canonical='dev:ada@x.io'")
    current_name = await p.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='name'",
        dev_id)
    assert current_name == "ada", "a second author name under a known email must not rewrite name"
    name_rows = await p.fetchval(
        "SELECT count(*) FROM assertions WHERE object_id=$1 AND name='name'", dev_id)
    assert name_rows == 1, "name is written once, from the identity itself, never reasserted"
    aliases = await p.fetchval(
        "SELECT value FROM current_assertions WHERE object_id=$1 AND name='author_alias'",
        dev_id)
    assert aliases == ["Ada Lovelace"], "the second author name is preserved as an alias"


async def test_author_aliases_merge_across_runs_instead_of_replacing(
    actions: Actions, tmp_path: Path,
) -> None:
    """The alias set is additive across separate ingest runs (mirroring `dev_info`'s own
    within-run accumulation). A name seen on an earlier run is never lost just because a
    later run's own commit walk didn't happen to repeat it."""
    repo = tmp_path / "proj4b"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Ada")
    _git(repo, "config", "user.email", "ada@x.io")
    (repo / "a.txt").write_text("1")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "genesis")
    env1 = {**os.environ, "GIT_AUTHOR_NAME": "Ada Lovelace", "GIT_AUTHOR_EMAIL": "ada@x.io",
            "GIT_COMMITTER_NAME": "Ada Lovelace", "GIT_COMMITTER_EMAIL": "ada@x.io"}
    (repo / "b.txt").write_text("2")
    _git(repo, "add", ".")
    await asyncio.to_thread(
        subprocess.run, ["git", "-C", str(repo), "commit", "-q", "-m", "second"],
        check=True, capture_output=True, env=env1)
    await ingest_repo(actions, str(repo))

    env2 = {**os.environ, "GIT_AUTHOR_NAME": "A. Lovelace", "GIT_AUTHOR_EMAIL": "ada@x.io",
            "GIT_COMMITTER_NAME": "A. Lovelace", "GIT_COMMITTER_EMAIL": "ada@x.io"}
    (repo / "c.txt").write_text("3")
    _git(repo, "add", ".")
    await asyncio.to_thread(
        subprocess.run, ["git", "-C", str(repo), "commit", "-q", "-m", "third"],
        check=True, capture_output=True, env=env2)
    await ingest_repo(actions, str(repo))

    p = actions.pool
    dev_id = await p.fetchval("SELECT id FROM objects WHERE canonical='dev:ada@x.io'")
    aliases = await p.fetchval(
        "SELECT value FROM current_assertions WHERE object_id=$1 AND name='author_alias'",
        dev_id)
    assert aliases == ["A. Lovelace", "Ada Lovelace"], \
        "both distinct author names, from separate runs, are preserved together"


async def test_repo_name_survives_conventional_commits(actions: Actions, tmp_path: Path) -> None:
    """Regression: the property loop used to reuse `name`, shadowing the repo name, so a repo
    of Conventional Commits returned repo='summary' (the last property key). The node was fine
    but the return was wrong, and multi-repo ingest leans on that return."""
    repo = tmp_path / "proj2"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Ada")
    _git(repo, "config", "user.email", "ada@x.io")
    (repo / "f").write_text("x")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "feat(core): the thing")   # conventional → triggers the loop
    res = await ingest_repo(actions, str(repo))
    assert res["repo"] == "proj2"                                # not "summary"


async def test_ingest_is_idempotent(actions: Actions, tmp_path: Path) -> None:
    repo = tmp_path / "p"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Ada")
    _git(repo, "config", "user.email", "ada@x.io")
    (repo / "f").write_text("x")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "only")
    (repo / "g").write_text("y")          # a second commit, so there's a follows edge to dedup
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "feat(x): two")
    for _ in range(3):  # re-ingesting the same history must not fork the graph OR its edges
        await ingest_repo(actions, str(repo))
    p = actions.pool
    assert await p.fetchval("SELECT count(*) FROM objects WHERE type='Commit'") == 2
    # the regression: links are append-only, so a naive re-ingest used to triple every edge
    assert await p.fetchval("SELECT count(*) FROM links WHERE type='authored_by'") == 2
    assert await p.fetchval("SELECT count(*) FROM links WHERE type='in_repo'") == 2
    assert await p.fetchval("SELECT count(*) FROM links WHERE type='follows'") == 1
    # the source-level fix (operator ruling, "fix the
    # sources"): one dev's own name/email is one value for the whole run and across
    # reruns: 2 commits ingested 4 times total (1 fresh + 3 reruns) must never write
    # more than the single genuine name/email row each, not 8.
    dev = await p.fetchval("SELECT id FROM objects WHERE canonical='dev:ada@x.io'")
    assert await p.fetchval(
        "SELECT count(*) FROM assertions WHERE object_id=$1 AND name='name'", dev) == 1
    assert await p.fetchval(
        "SELECT count(*) FROM assertions WHERE object_id=$1 AND name='email'", dev) == 1


async def test_ingest_walks_every_branch_not_just_head(
    actions: Actions, tmp_path: Path,
) -> None:
    """Ingest registration phase 2, measured constraint 3:
    a bare `git log` never sees a commit that lives only on a branch other than the
    one currently checked out. 17 live worktree-agent-* branches proved this in
    production. A commit made on a topic branch, HEAD left on main, must still be
    ingested."""
    repo = tmp_path / "multi-branch"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Ada")
    _git(repo, "config", "user.email", "ada@x.io")
    (repo / "a.txt").write_text("1")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "on main")
    _git(repo, "checkout", "-q", "-b", "topic")
    (repo / "b.txt").write_text("2")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "on topic branch only")
    _git(repo, "checkout", "-q", "-")  # back to main, HEAD never touches the topic commit

    res = await ingest_repo(actions, str(repo))
    assert res["commits"] == 2

    subjects = {r["value"] for r in await actions.pool.fetch(
        "SELECT value #>> '{}' AS value FROM current_assertions WHERE name='subject'")}
    assert subjects == {"on main", "on topic branch only"}


def test_parse_subject_extracts_conventional_commit() -> None:
    """The structure that makes the log queryable memory: type + scope + summary."""
    from src.ingest.gitlog import parse_subject
    assert parse_subject("feat(composer): W5 — author a Room") == {
        "change_type": "feat", "scope": "composer", "summary": "W5 — author a Room"}
    assert parse_subject("docs: reflect the vision") == {
        "change_type": "docs", "summary": "reflect the vision"}
    assert parse_subject("Phase 0: schema") == {}  # not conventional → no false structure


async def test_commit_carries_type_scope_and_rationale(actions: Actions, tmp_path: Path) -> None:
    """A commit becomes a lightweight DECISION record: its type/scope are groupable and its
    body is the rationale (why, not just what): project memory, queryable."""
    repo = tmp_path / "p2"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Ada")
    _git(repo, "config", "user.email", "ada@x.io")
    (repo / "f").write_text("1")
    _git(repo, "add", ".")
    msg = "feat(engine): close the op set\n\nwe chose a closed set + a Function hatch"
    _git(repo, "commit", "-q", "-m", msg)

    await ingest_repo(actions, str(repo))
    p = actions.pool

    async def prop(name: str) -> str | None:
        return await p.fetchval(
            "SELECT value #>> '{}' FROM current_assertions WHERE name=$1", name)

    assert await prop("change_type") == "feat"
    assert await prop("scope") == "engine"
    assert "Function hatch" in (await prop("rationale") or "")


async def test_machine_identity_routes_when_local_part_matches_repo_and_domain_is_machine_shaped(
    actions: Actions, tmp_path: Path,
) -> None:
    """MACHINE GIT IDENTITIES ARE NOT PEOPLE: the live
    specimen was dev:ballgem@local wrongly typed Person for repo:ballgem's own bootstrap
    committer: local part == an already-ingested repo's own canonical name AND a
    machine-shaped domain ('local'). A fresh ingest must route straight to MachineIdentity,
    never mint the wrong-typed Person at all, and mint a committer_for edge with `since`
    set to the first commit's own date."""
    repo = tmp_path / "ballgem"
    repo.mkdir()
    env = {**os.environ, "GIT_AUTHOR_NAME": "Bootstrap", "GIT_AUTHOR_EMAIL": "ballgem@local",
           "GIT_COMMITTER_NAME": "Bootstrap", "GIT_COMMITTER_EMAIL": "ballgem@local"}
    await asyncio.to_thread(
        subprocess.run, ["git", "-C", str(repo), "init", "-q"], check=True, capture_output=True)
    (repo / "a.txt").write_text("1")
    await asyncio.to_thread(
        subprocess.run, ["git", "-C", str(repo), "add", "."], check=True, capture_output=True)
    await asyncio.to_thread(
        subprocess.run, ["git", "-C", str(repo), "commit", "-q", "-m", "genesis"],
        check=True, capture_output=True, env=env)

    await ingest_repo(actions, str(repo))
    p = actions.pool

    assert await p.fetchval(
        "SELECT count(*) FROM objects WHERE canonical='dev:ballgem@local'") == 0
    machine_id = await p.fetchval(
        "SELECT id FROM objects WHERE type='MachineIdentity' AND canonical='machine:ballgem@local'")
    assert machine_id is not None
    project_id = await p.fetchval(
        "SELECT id FROM objects WHERE type='SoftwareProject' AND canonical='repo:ballgem'")
    link = await p.fetchrow(
        "SELECT properties FROM links WHERE from_id=$1 AND to_id=$2 AND type='committer_for'",
        machine_id, project_id)
    assert link is not None
    props = link["properties"]
    props = json.loads(props) if isinstance(props, str) else props
    assert "since" in props and props["since"]  # a real, non-empty first-commit timestamp


async def test_machine_identity_bridges_a_pre_existing_wrongly_typed_person_via_same_as(
    actions: Actions, tmp_path: Path,
) -> None:
    """A Person minted under the OLD, pre-heuristic routing for the same email is never
    deleted or retyped (objects.type is immutable): a same_as link (loser -> winner)
    bridges it to the new MachineIdentity instead, exactly the live dev:ballgem@local
    correction the ruling calls for."""
    repo = tmp_path / "ballgem2"
    repo.mkdir()
    env = {**os.environ, "GIT_AUTHOR_NAME": "Bootstrap", "GIT_AUTHOR_EMAIL": "ballgem2@local",
           "GIT_COMMITTER_NAME": "Bootstrap", "GIT_COMMITTER_EMAIL": "ballgem2@local"}
    await asyncio.to_thread(
        subprocess.run, ["git", "-C", str(repo), "init", "-q"], check=True, capture_output=True)
    (repo / "a.txt").write_text("1")
    await asyncio.to_thread(
        subprocess.run, ["git", "-C", str(repo), "add", "."], check=True, capture_output=True)
    await asyncio.to_thread(
        subprocess.run, ["git", "-C", str(repo), "commit", "-q", "-m", "genesis"],
        check=True, capture_output=True, env=env)
    legacy_person = await actions.create_or_find_object("Person", "dev:ballgem2@local", "test")

    await ingest_repo(actions, str(repo))
    p = actions.pool

    # never deleted, never retyped
    assert await p.fetchval(
        "SELECT type FROM objects WHERE id=$1", legacy_person) == "Person"
    machine_id = await p.fetchval(
        "SELECT id FROM objects WHERE type='MachineIdentity' "
        "AND canonical='machine:ballgem2@local'")
    assert machine_id is not None
    assert await p.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='same_as'",
        legacy_person, machine_id) == 1


async def test_declare_machine_identity_refuses_without_a_because(actions: Actions) -> None:
    out = await declare_machine_identity(
        actions, email="bot@example.com", project="whatever", because="", actor="test")
    assert "error" in out and "because" in out["error"]


async def test_declare_machine_identity_refuses_an_unknown_project(actions: Actions) -> None:
    out = await declare_machine_identity(
        actions, email="bot@example.com", project="no-such-project-anywhere",
        because="test", actor="test")
    assert "error" in out and "no-such-project-anywhere" in out["error"]


async def test_declare_machine_identity_mints_bridges_and_links(actions: Actions) -> None:
    """THE declare-machine-identity DOOR: covers what the ingest
    heuristic misses: a bot on a real-looking domain the heuristic would never flag as
    machine-shaped. Mints MachineIdentity, bridges a pre-existing wrongly-typed Person via
    same_as (never deleted), and mints committer_for; re-declaring is a no-op."""
    project_id = await actions.create_or_find_object("SoftwareProject", "repo:widget", "test")
    legacy_person = await actions.create_or_find_object("Person", "dev:ci@widget.io", "test")

    out = await declare_machine_identity(
        actions, email="CI@Widget.io", project="widget",
        because="CI bot, real domain, heuristic never fires", actor="operator")
    assert out["machine_identity"] == "machine:ci@widget.io"
    assert out["project"] == "repo:widget"
    assert out["bridged_person"] == "dev:ci@widget.io"
    assert out["minted_committer_for"] is True

    p = actions.pool
    machine_id = await p.fetchval(
        "SELECT id FROM objects WHERE type='MachineIdentity' AND canonical='machine:ci@widget.io'")
    assert machine_id is not None
    assert await p.fetchval("SELECT type FROM objects WHERE id=$1", legacy_person) == "Person"
    assert await p.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='same_as'",
        legacy_person, machine_id) == 1
    assert await p.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='committer_for'",
        machine_id, project_id) == 1

    # re-declaring is idempotent: no duplicate links, no error
    out2 = await declare_machine_identity(
        actions, email="ci@widget.io", project="widget", because="repeat", actor="operator")
    assert out2["minted_committer_for"] is False
    assert await p.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND to_id=$2 AND type='committer_for'",
        machine_id, project_id) == 1
    assert await p.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND to_id=$2 AND type='same_as'",
        legacy_person, machine_id) == 1


def test_strip_trailers_removes_machine_provenance_keeps_rationale() -> None:
    """The body is memory; the trailers are noise. Strip Co-Authored-By / *-Session / Generated
    lines (which made 'claude'/'anthropic' the top cross-repo 'concern'), keep the human why,
    and never eat a plain 'Note:'/'Fixes:' line, which is real rationale, not a trailer."""
    body = (
        "we chose a closed op set + a Function hatch\n"
        "Note: this supersedes the earlier DSL idea\n"
        "\n"
        "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>\n"
        "Claude-Session: https://claude.ai/code/session_abc\n"
        "🤖 Generated with [Claude Code](https://claude.com/claude-code)"
    )
    out = strip_trailers(body)
    assert "closed op set" in out and "Note: this supersedes" in out   # rationale kept
    assert "claude" not in out.lower() and "anthropic" not in out.lower()  # trailers gone
    assert "Generated with" not in out
    # a body that is ONLY a trailer collapses to empty (→ no rationale asserted)
    assert strip_trailers("Co-Authored-By: Claude <noreply@anthropic.com>") == ""
