"""Rung 4 — neighborhood consolidation (ruling a0cfcca1).

The mechanical pass folds echoes (delegates to consolidate_memory — its own tests hold
the merge law); these tests hold the SUMMARY pass: one Reference per active repo
neighborhood, fingerprint-watermarked so an unchanged neighborhood never costs an LLM
call, refreshed when the neighborhood moves, metered in llm_usage.
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.ingest.providers import Usage
from src.orchestrator.capture import open_thread, record_decision
from src.orchestrator.monitor import set_cursor
from src.orchestrator.neighborhoods import consolidate_pass, discover_trees, summarize_neighborhoods


class FakeLLM:
    def __init__(self) -> None:
        self.calls = 0

    async def complete(self, *, system: str, prompt: str, model: str,
                       max_tokens: int = 2048,
                       usage_out: list[Usage] | None = None) -> str:
        self.calls += 1
        assert "Project: demoproj" in prompt  # the neighborhood names itself
        if usage_out is not None:
            usage_out.append(Usage(model=model, input_tokens=100, output_tokens=50))
        return f"Digest v{self.calls}: the demoproj neighborhood is mid-arc."


async def _seed(actions: Actions) -> None:
    await open_thread(actions, "wire the composer renderer to the schema", repo="demoproj")
    await open_thread(actions, "the satellite dispatcher needs a vantage", repo="demoproj")
    await record_decision(actions, "the renderer dispatches on result shape",
                          kind="ruling", repo="demoproj")


async def _ref(actions: Actions) -> dict | None:
    row = await actions.pool.fetchrow(
        "SELECT o.id, "
        " (SELECT value #>> '{}' FROM current_assertions WHERE object_id=o.id "
        "   AND name='body' ORDER BY confidence DESC, observed_at DESC LIMIT 1) AS body, "
        " (SELECT value #>> '{}' FROM current_assertions WHERE object_id=o.id "
        "   AND name='watermark' "
        "   ORDER BY confidence DESC, observed_at DESC LIMIT 1) AS watermark "
        "FROM objects o WHERE o.canonical='ref:neighborhood-demoproj'")
    return dict(row) if row else None


async def test_summary_is_watermarked_incremental_and_metered(actions: Actions) -> None:
    await _seed(actions)
    llm = FakeLLM()
    out = await summarize_neighborhoods(actions, llm)
    assert out["summarized"] == 1 and llm.calls == 1
    ref = await _ref(actions)
    assert ref is not None and "Digest v1" in ref["body"] and ref["watermark"]
    # the cost stream saw the call
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM llm_usage WHERE purpose='neighborhood-summary'") == 1
    # unchanged neighborhood → fingerprint matches → free skip, no second call
    out2 = await summarize_neighborhoods(actions, llm)
    assert out2["summarized"] == 0 and out2["skipped"] >= 1 and llm.calls == 1
    # the neighborhood MOVES → the fingerprint moves → the digest refreshes in place
    await open_thread(actions, "the delivery sink needs dedup before alerts flow",
                      repo="demoproj")
    out3 = await summarize_neighborhoods(actions, llm)
    assert out3["summarized"] == 1 and llm.calls == 2
    ref2 = await _ref(actions)
    assert ref2 is not None and "Digest v2" in ref2["body"]
    assert ref2["id"] == ref["id"]  # refreshed, never twinned
    assert ref2["watermark"] != ref["watermark"]


async def test_thin_neighborhoods_are_not_summarized(actions: Actions) -> None:
    """Two members is a hallway, not a neighborhood (the >=3 floor): no LLM spend on
    repos with nothing to consolidate."""
    await open_thread(actions, "a single lonely thread", repo="tinyproj")
    await record_decision(actions, "a single lonely ruling", repo="tinyproj")
    llm = FakeLLM()
    out = await summarize_neighborhoods(actions, llm)
    assert out == {"candidates": 0, "summarized": 0, "skipped": 0} and llm.calls == 0


async def test_consolidate_pass_walks_both_memory_types(actions: Actions) -> None:
    """The mechanical motion delegates to consolidate_memory for Threads AND Decisions —
    the counters come back namespaced per type."""
    out = await consolidate_pass(actions)
    assert set(out) == {"threads_merged", "threads_for_review",
                        "decisions_merged", "decisions_for_review"}


# --- the disk census (thread 5e37630b) --------------------------------------------------

async def test_census_trees_mints_the_unmodeled_and_paths_the_known(
    actions: Actions, tmp_path: Path,
) -> None:
    """'A memory that doesn't know your ancestors cannot stop you reinventing them':
    a repo on disk the graph never met becomes a SoftwareProject with on_disk_path and
    discovered='disk-census'; a known project gains its path; nothing touches the watch
    list. Idempotent: the second walk writes nothing."""
    from src.orchestrator.neighborhoods import census_trees
    (tmp_path / "code" / "ancient-evals" / ".git").mkdir(parents=True)
    (tmp_path / "code" / "known-tree" / ".git").mkdir(parents=True)
    (tmp_path / "code" / "not-a-repo").mkdir()
    (tmp_path / "code" / "node_modules" / "dep" / ".git").mkdir(parents=True)
    known = await actions.create_or_find_object(
        "SoftwareProject", "repo:known-tree", "analyst:test")

    out = await census_trees(actions, roots=[str(tmp_path / "code")])
    assert out["minted"] == ["ancient-evals"]
    assert out["known"] == 1
    assert out["pathed"] == ["known-tree"]
    assert out["refused"] == []
    row = await actions.pool.fetchrow(
        "SELECT o.id, o.status, "
        " (SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='discovered' LIMIT 1) AS disc, "
        " (SELECT a.value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='on_disk_path' LIMIT 1) AS path "
        "FROM objects o WHERE o.type='SoftwareProject' AND o.canonical='repo:ancient-evals'")
    assert row is not None and row["status"] == "active"
    assert row["disc"] == "disk-census"
    assert row["path"] == str(tmp_path / "code" / "ancient-evals")
    kpath = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a "
        "WHERE a.object_id=$1 AND a.name='on_disk_path' LIMIT 1", known)
    assert kpath == str(tmp_path / "code" / "known-tree")

    again = await census_trees(actions, roots=[str(tmp_path / "code")])
    assert again["minted"] == [] and again["pathed"] == []
    assert again["known"] == 2  # both now known; the unchanged disk writes nothing


def _real_git_repo(tmp_path: Path, name: str, remote: str | None) -> Path:
    import subprocess
    path = tmp_path / "code" / name
    path.mkdir(parents=True)
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)
    if remote:
        subprocess.run(["git", "remote", "add", "origin", remote], cwd=path, check=True)
    return path


def _relocate_remote(path: Path, new_url: str) -> None:
    import subprocess
    subprocess.run(["git", "remote", "set-url", "origin", new_url], cwd=path, check=True)


async def test_census_trees_captures_remote_url_on_mint_and_self_heals_on_change(
    actions: Actions, tmp_path: Path,
) -> None:
    """#144 Rule 2 (decision ffc193a4ce07): the census is the ONLY place remote_url is
    ever written, so the hot mount()-time path never shells out live. A freshly-minted
    repo gets remote_url alongside on_disk_path; a repo with no configured origin
    (ballgem's own shape) gets no remote_url assertion at all — absence stays absence,
    never a persisted null standing in for "checked, found nothing" (60bc15db). A later
    walk that finds the origin CHANGED (the real /srv/git relocation shape) overwrites it
    — the cache self-heals on its own next 10-minute cadence, never held stale forever."""
    from src.orchestrator.neighborhoods import census_trees
    _real_git_repo(tmp_path, "withremote", "git@github.com:x/withremote.git")
    _real_git_repo(tmp_path, "noremote", None)

    out = await census_trees(actions, roots=[str(tmp_path / "code")])
    assert set(out["minted"]) == {"withremote", "noremote"}
    assert out["remoted"] == ["withremote"]

    with_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:withremote'")
    no_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:noremote'")
    with_url = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a "
        "WHERE a.object_id=$1 AND a.name='remote_url' LIMIT 1", with_id)
    no_url = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a "
        "WHERE a.object_id=$1 AND a.name='remote_url' LIMIT 1", no_id)
    assert with_url == "git@github.com:x/withremote.git"
    assert no_url is None  # never written, not written-as-null

    # unchanged origin: idempotent, no second write
    again = await census_trees(actions, roots=[str(tmp_path / "code")])
    assert again["remoted"] == []

    # the origin RELOCATES (the real /srv/git shape) — the next walk catches it
    _relocate_remote(tmp_path / "code" / "withremote", "git@newhost:x/withremote.git")
    healed = await census_trees(actions, roots=[str(tmp_path / "code")])
    assert healed["remoted"] == ["withremote"]
    healed_url = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a "
        "WHERE a.object_id=$1 AND a.name='remote_url' LIMIT 1", with_id)
    assert healed_url == "git@newhost:x/withremote.git"


async def test_census_trees_reconnects_a_renamed_directory_by_remote_url(
    actions: Actions, tmp_path: Path,
) -> None:
    """CREATE-SHAPE's location half (obligation e5b0ece4, decision fa0eb021, Thoth msg
    4762): a renamed directory fails the name lookup and, pre-fix, minted a DUPLICATE
    SoftwareProject rather than reconnecting to the one that already existed. An
    unambiguous remote_url match now reconnects the EXISTING object — only on_disk_path/
    remote_url move; name/canonical never change (THE BINDING LINE: this carries LOCATION
    forward and never RE-DERIVES IDENTITY — that stays rename_project's own deliberate
    act, 1db1ff41)."""
    import shutil

    from src.orchestrator.neighborhoods import census_trees
    old_path = _real_git_repo(tmp_path, "oldname", "git@github.com:x/thing.git")

    first = await census_trees(actions, roots=[str(tmp_path / "code")])
    assert first["minted"] == ["oldname"]
    proj_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:oldname'")

    new_path = tmp_path / "code" / "newname"
    shutil.move(str(old_path), str(new_path))

    out = await census_trees(actions, roots=[str(tmp_path / "code")])
    assert out["reconnected"] == ["newname"]
    assert out["minted"] == []
    assert out["refused"] == []

    row = await actions.pool.fetchrow(
        "SELECT o.canonical, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='name' LIMIT 1) AS name, "
        " (SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='on_disk_path' LIMIT 1) AS path "
        "FROM objects o WHERE o.id=$1", proj_id)
    assert row["canonical"] == "repo:oldname"  # identity untouched
    assert row["name"] == "oldname"
    assert row["path"] == str(new_path)  # location carried forward
    only_one = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='SoftwareProject' AND status='active'")
    assert only_one == 1  # no twin minted under the new basename

    # idempotent: a third walk over the settled state writes nothing further
    again = await census_trees(actions, roots=[str(tmp_path / "code")])
    assert again["reconnected"] == [] and again["minted"] == []
    assert again["known"] == 1


async def test_census_trees_refuses_an_ambiguous_remote_url_match(
    actions: Actions, tmp_path: Path,
) -> None:
    """More than one active SoftwareProject already carries the SAME remote_url — an
    un-repointed fork is exactly this shape (the local-git-fork-detection blind spot,
    #144 Rule 2). Refuses per entry rather than guessing which one a renamed directory
    reconnects to, the same doctrine as fork_project (declared, never inferred)."""
    from src.orchestrator.neighborhoods import census_trees
    remote = "git@github.com:x/shared.git"
    a = await actions.create_or_find_object("SoftwareProject", "repo:proj-a", "analyst:test")
    await actions.assert_property(a, "remote_url", remote, "analyst:test",
                                  datetime.now(UTC), 0.9)
    b = await actions.create_or_find_object("SoftwareProject", "repo:proj-b", "analyst:test")
    await actions.assert_property(b, "remote_url", remote, "analyst:test",
                                  datetime.now(UTC), 0.9)

    _real_git_repo(tmp_path, "mystery", remote)
    out = await census_trees(actions, roots=[str(tmp_path / "code")])
    assert out["minted"] == [] and out["reconnected"] == []
    assert [r["name"] for r in out["refused"]] == ["mystery"]
    assert "ambiguous remote_url" in out["refused"][0]["reason"]
    assert await actions.pool.fetchval(
        "SELECT 1 FROM objects WHERE canonical='repo:mystery'") is None


async def test_census_trees_does_not_reconnect_a_live_copy_of_an_existing_checkout(
    actions: Actions, tmp_path: Path,
) -> None:
    """GATE (a), Thoth's binding lean (msg 4762): reconnect only once the OLD on_disk_path
    is confirmed GONE. A second, independent clone of the same remote sitting ALONGSIDE
    the first — a COPY, not a MOVE — must mint its own object rather than steal the first
    checkout's identity, even though its remote_url matches."""
    from src.orchestrator.neighborhoods import census_trees
    remote = "git@github.com:x/thing.git"
    _real_git_repo(tmp_path, "original", remote)
    first = await census_trees(actions, roots=[str(tmp_path / "code")])
    assert first["minted"] == ["original"]

    _real_git_repo(tmp_path, "second-checkout", remote)  # original still on disk
    out = await census_trees(actions, roots=[str(tmp_path / "code")])
    assert out["minted"] == ["second-checkout"]
    assert out["reconnected"] == []
    assert out["refused"] == []
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='SoftwareProject' AND status='active'") == 2


def test_git_dirs_skips_a_bare_venv_one_level_shallower_than_the_depth_cutoff(
    tmp_path: Path,
) -> None:
    """Task #122's mechanical fix, proven as a negative control both ways. `mirror/venv`
    (depth 2, AT the max_depth=2 cutoff) was already accidentally safe: `depth >= max_depth`
    returns before venv's own children are ever iterated, regardless of the skip list. A
    bare `venv` ONE LEVEL SHALLOWER (depth 1) has no such protection — pre-fix, the walker
    descends straight into it and finds whatever sits inside; post-fix (_CENSUS_SKIP now
    matches "venv" as well as ".venv"), it does not."""
    from src.orchestrator import neighborhoods
    from src.orchestrator.neighborhoods import _git_dirs

    (tmp_path / "code" / "venv" / "nested-repo" / ".git").mkdir(parents=True)
    (tmp_path / "code" / "mirror" / "venv" / "also-nested" / ".git").mkdir(parents=True)

    # PRE-FIX (reproduced directly, not by inference): without "venv" in the skip set, the
    # shallow venv's own nested repo IS found — the trap, live.
    without_fix = neighborhoods._CENSUS_SKIP - {"venv"}
    original = neighborhoods._CENSUS_SKIP
    neighborhoods._CENSUS_SKIP = without_fix
    try:
        pre_fix = _git_dirs([str(tmp_path / "code")])
    finally:
        neighborhoods._CENSUS_SKIP = original
    assert any(p.name == "nested-repo" for p in pre_fix)

    # POST-FIX (the real, currently-shipped _CENSUS_SKIP): the shallow venv is never
    # descended into at all.
    post_fix = _git_dirs([str(tmp_path / "code")])
    assert not any(p.name == "nested-repo" for p in post_fix)

    # the depth-2 mirror/venv case was ALREADY safe before this fix (the accidental
    # protection Thoth named) and stays safe after it, for a different reason.
    assert not any(p.name == "also-nested" for p in post_fix)


async def test_census_trees_refuses_a_malformed_name_and_keeps_walking(
    actions: Actions, tmp_path: Path,
) -> None:
    """Ruling 1db1ff41 half (a): the mint now runs through `_mint_or_find_repo`, the same
    validated choke point `link_repo` uses (task #107's `_validate_repo_name`) — this walk
    used to mint straight off `create_or_find_object` with no guard at all, which is
    exactly how the two real "/home/.../ballgem" and "/home/.../REPOS/sutra" duplicates
    (folded separately, decision 1db1ff41) reached the graph. A directory basename that
    isn't a well-formed project ref now REFUSES — and costs only that one census entry, per
    the operator's degrade-per-item ruling on #107's own batch-abort defect: a legitimate
    sibling repo in the same walk still mints clean."""
    from src.orchestrator.neighborhoods import census_trees
    (tmp_path / "code" / "--hostile" / ".git").mkdir(parents=True)
    (tmp_path / "code" / "legit-repo" / ".git").mkdir(parents=True)

    out = await census_trees(actions, roots=[str(tmp_path / "code")])
    assert out["minted"] == ["legit-repo"]
    assert [r["name"] for r in out["refused"]] == ["--hostile"]
    assert "well-formed project ref" in out["refused"][0]["reason"]
    refused_row = await actions.pool.fetchval(
        "SELECT 1 FROM objects WHERE type='SoftwareProject' "
        "AND canonical LIKE '%hostile%'")
    assert refused_row is None
    minted_row = await actions.pool.fetchval(
        "SELECT o.status FROM objects o WHERE o.type='SoftwareProject' "
        "AND o.canonical='repo:legit-repo'")
    assert minted_row == "active"


# --- the read-back (thread 2309: "I could not have discovered my own zero") -------------

async def test_discover_trees_includes_a_project_with_no_activity_at_all(
        actions: Actions) -> None:
    """The gap the old version had: a truly fresh project (no Thread/Commit ever filed
    under it) used to be silently absent from this function's own output — the exact
    'invisible until someone tells you' defect redmonth named. Every active SoftwareProject
    is reported now, zero-everything or not."""
    await actions.create_or_find_object("SoftwareProject", "repo:freshproject", "analyst:test")
    out = await discover_trees(actions.pool, watched=[])
    row = next(r for r in out if r["tree"] == "freshproject")
    assert row["commits"] == 0
    assert row["activity"] == 0
    assert row["path"] is None
    assert row["reason"] == "no on_disk_path registered — the disk census hasn't found it yet"


async def test_discover_trees_reads_the_stored_path_never_guesses_one(
        actions: Actions) -> None:
    """redmonth's own warning (thread 2309): a directory-name match against a search root
    is a GUESS, and his mounted cwd is not even a git repository — the real repo lives
    under a different name entirely. `path` comes ONLY from the stored on_disk_path
    property (census_trees's write, or a future explicit registration); nothing here
    re-derives it from any caller-supplied root, so a wrong guess is structurally
    impossible, not just avoided by convention."""
    proj = await actions.create_or_find_object("SoftwareProject", "repo:ballgemlike", "git")
    await actions.assert_property(proj, "on_disk_path", "/home/x/code/ballgem", "disk-census",
                                  datetime.now(UTC),
                                  0.9)
    out = await discover_trees(actions.pool, watched=[])
    row = next(r for r in out if r["tree"] == "ballgemlike")
    assert row["path"] == "/home/x/code/ballgem"
    assert row["reason"] == "on disk at /home/x/code/ballgem, not in the ingest watch list"
    assert row["blind"] is True  # a path exists, nobody watches it


async def test_discover_trees_names_watched_but_never_ticked(actions: Actions) -> None:
    """A project can be honestly in the watch list and still have zero commits, simply
    because pulse.py hasn't ticked since it was added — no devhead:<name> watermark yet.
    That is a DIFFERENT fact from 'not watched at all' and must read as one."""
    proj = await actions.create_or_find_object("SoftwareProject", "repo:justadded", "git")
    await actions.assert_property(proj, "on_disk_path", "/home/x/code/justadded", "disk-census",
                                  datetime.now(UTC),
                                  0.9)
    out = await discover_trees(actions.pool, watched=["/home/x/code/justadded"])
    row = next(r for r in out if r["tree"] == "justadded")
    assert row["watched"] is True
    assert row["commits"] == 0
    assert row["last_ingested_at"] is None
    assert row["reason"] == "in the watch list but ingest has never ticked for it yet"
    assert row["blind"] is False  # watched, just not yet ticked — not the same as blind


async def test_discover_trees_names_ticked_but_still_empty(actions: Actions) -> None:
    """The rarest, most alarming case: pulse HAS run (a real watermark exists) and STILL
    found zero commits — the path may no longer point at a real git repo. Distinguishing
    this from 'never ticked' is the whole point of reading the watermark's own timestamp,
    not just its presence."""
    proj = await actions.create_or_find_object("SoftwareProject", "repo:emptiedout", "git")
    await actions.assert_property(proj, "on_disk_path", "/home/x/code/emptiedout", "disk-census",
                                  datetime.now(UTC),
                                  0.9)
    await set_cursor(actions.pool, "devhead:emptiedout", "deadbeef")
    out = await discover_trees(actions.pool, watched=["/home/x/code/emptiedout"])
    row = next(r for r in out if r["tree"] == "emptiedout")
    assert row["commits"] == 0
    assert row["last_ingested_at"] is not None
    assert row["reason"] == (
        "ingest has run and found nothing — the path may no longer be a git repo")
