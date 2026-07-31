"""PROJECT IDENTITY (#110, decision 1db1ff41): project_identity_evidence, the read-only
resolver that gathers whichever tiers have signal and never picks a winner; rename_project
and fork_project, the two DECLARED-succession writers a caller invokes once a human has
read that report. (correct_project_name, the third writer, lives in projects.py/
test_projects.py beside its lifecycle-verb siblings.)"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.project_identity import (
    fork_project,
    project_identity_evidence,
    rename_project,
    unfork_project,
)
from src.orchestrator.seats import bind_holder, ensure_seat


async def _mk_agent(actions: Actions, label: str) -> str:
    return await actions.create_or_find_object("Agent", label, label)


async def _mk_project(actions: Actions, name: str, on_disk_path: str | None = None) -> str:
    oid = await actions.create_or_find_object("SoftwareProject", f"repo:{name}", "test")
    await actions.assert_property(oid, "name", name, "test", datetime.now(UTC), 0.9,
                                  evidence_class="self_declared")
    if on_disk_path:
        await actions.assert_property(oid, "on_disk_path", on_disk_path, "disk-census",
                                      datetime.now(UTC), 0.9, evidence_class="self_declared")
    return oid


def _git_repo(tmp_path, name: str, remote: str | None) -> str:
    import subprocess
    path = tmp_path / name
    path.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)
    if remote:
        subprocess.run(["git", "remote", "add", "origin", remote], cwd=path, check=True)
    return str(path)


async def test_clean_agreement_when_every_tier_matches(actions: Actions, tmp_path) -> None:
    """The healthy case: charter, pin, write-attribution and remote all name the same
    project — a single candidate, agreement, nothing to flag."""
    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "agreeproj"\n')
    seat = await ensure_seat(actions, house="osiris", handle="Cleanseat",
                             anchor_cwd=str(office), source="test")
    await _mk_agent(actions, "agent:c1ea0001")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:c1ea0001")
    proj = await _mk_project(actions, "agreeproj",
                             on_disk_path=_git_repo(tmp_path, "agreeproj-repo",
                                                    "https://github.com/x/agreeproj.git"))
    seat_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", seat["seat_id"])
    await actions.create_link(seat_oid, proj, "governs", "test", datetime.now(UTC), 0.9)
    thread = await actions.create_or_find_object("Thread", "thread:agree0001", "test")
    await actions.create_link(thread, proj, "in_repo", "agent:c1ea0001", datetime.now(UTC), 0.9)

    out = await project_identity_evidence(actions.pool, seat_id=seat["seat_id"])

    assert out["agreement"] == "single-candidate"
    assert list(out["candidates"]) == ["agreeproj"]
    c = out["candidates"]["agreeproj"]
    assert c["declared_charter"] and c["pin_match"] and c["remote_agrees"]
    assert c["write_attribution"]["count"] == 1


async def test_stale_remote_disagrees_while_the_rest_still_agree(
    actions: Actions, tmp_path,
) -> None:
    """The xxit/deckard shape: charter, pin and write-attribution all agree on the OLD
    label — still a single candidate, still not ambiguous — but the real git remote has
    already moved on, and that specific disagreement must be visible, not swallowed by
    the overall single-candidate verdict."""
    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "oldname"\n')
    seat = await ensure_seat(actions, house="osiris", handle="Staleseat",
                             anchor_cwd=str(office), source="test")
    await _mk_agent(actions, "agent:5ta1e001")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:5ta1e001")
    proj = await _mk_project(actions, "oldname",
                             on_disk_path=_git_repo(tmp_path, "oldname-repo",
                                                    "https://github.com/x/newname.git"))
    seat_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", seat["seat_id"])
    await actions.create_link(seat_oid, proj, "governs", "test", datetime.now(UTC), 0.9)

    out = await project_identity_evidence(actions.pool, seat_id=seat["seat_id"])

    assert out["agreement"] == "single-candidate"
    c = out["candidates"]["oldname"]
    assert c["declared_charter"] and c["pin_match"]
    assert c["is_git_repo"] is True
    assert c["remote_url"] == "https://github.com/x/newname.git"
    assert c["remote_agrees"] is False


async def test_no_fixed_precedence_two_real_candidates_disagree(
    actions: Actions, tmp_path,
) -> None:
    """The ballgem shape, the case that killed Thoth's own proposed rule: the pin names
    one project (wrong, no disk evidence), write-attribution is split across two real
    projects with neither carrying a remote — remote has NOTHING to say here, and this
    function must report a genuine disagreement rather than pick the majority silently."""
    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "wrongpin"\n')
    seat = await ensure_seat(actions, house="osiris", handle="Forkseat",
                             anchor_cwd=str(office), source="test")
    await _mk_agent(actions, "agent:f0rk0001")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:f0rk0001")
    old_proj = await _mk_project(actions, "oldproj")  # no on_disk_path — pin is wrong
    new_proj = await _mk_project(actions, "newproj",
                                 on_disk_path=_git_repo(tmp_path, "newproj-repo", None))
    for i in range(2):
        t = await actions.create_or_find_object("Thread", f"thread:fork000{i}", "test")
        await actions.create_link(t, old_proj, "in_repo", "agent:f0rk0001", datetime.now(UTC),
                                  0.9)
    for i in range(5):
        t = await actions.create_or_find_object("Thread", f"thread:fork00a{i}", "test")
        await actions.create_link(t, new_proj, "in_repo", "agent:f0rk0001", datetime.now(UTC),
                                  0.9)

    out = await project_identity_evidence(actions.pool, seat_id=seat["seat_id"])

    assert out["agreement"] == "disagree"
    # every candidate with ANY signal is reported, including the write-attribution
    # minority (oldproj, 2 of 7) — this function never drops a row just because it lost
    assert set(out["candidates"]) == {"wrongpin", "newproj", "oldproj"}
    assert out["candidates"]["newproj"]["write_attribution"]["count"] == 5
    assert out["candidates"]["newproj"]["is_git_repo"] is True
    assert out["candidates"]["newproj"]["remote_url"] is None  # a real repo, no origin
    assert out["candidates"]["oldproj"]["write_attribution"]["count"] == 2
    assert not out["candidates"]["oldproj"]["declared_charter"]
    assert not out["candidates"]["oldproj"]["pin_match"]  # the pin says "wrongpin", not this


async def test_no_signal_reports_honestly_not_a_guess(actions: Actions, tmp_path) -> None:
    """A seat with no charter, no pin, no history — this must never invent a candidate to
    fill the silence."""
    office = tmp_path / "office"
    office.mkdir()
    seat = await ensure_seat(actions, house="osiris", handle="Blankseat",
                             anchor_cwd=str(office), source="test")

    out = await project_identity_evidence(actions.pool, seat_id=seat["seat_id"])

    assert out["agreement"] == "no-signal"
    assert out["candidates"] == {}


async def test_operator_citation_is_reported_never_interpreted(
    actions: Actions, tmp_path,
) -> None:
    """A caller-supplied operator citation is the ONE tier this function does not compute
    — it is echoed back verbatim and never used to pick a candidate, because parsing
    decision prose for a project claim is a human's read, not this function's job."""
    office = tmp_path / "office"
    office.mkdir()
    seat = await ensure_seat(actions, house="osiris", handle="Citedseat",
                             anchor_cwd=str(office), source="test")

    out = await project_identity_evidence(
        actions.pool, seat_id=seat["seat_id"],
        operator_citation="decision:abc123 — operator said X governs Y")

    assert out["operator_confirmed"] == {
        "citation": "decision:abc123 — operator said X governs Y", "checked": True}
    assert out["agreement"] == "no-signal"  # unchanged — the citation names nothing structurally


async def test_declared_charter_reads_both_seat_and_agent_origin_governs(
    actions: Actions, tmp_path,
) -> None:
    """Ruling 1db1ff41's ruling 3 re-keyed governs onto the Seat (schema + code, b9b5ce9),
    but migrate_charter_to_seat is dry-run-only as of this build — every LIVE governs
    edge today is still Agent-typed. This tier must see BOTH shapes, not just the one the
    schema now prefers, or it goes blind on the exact data the real graph currently
    holds."""
    office = tmp_path / "office"
    office.mkdir()
    seat = await ensure_seat(actions, house="osiris", handle="Bothseat",
                             anchor_cwd=str(office), source="test")
    await _mk_agent(actions, "agent:b07h0001")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:b07h0001")
    proj = await _mk_project(actions, "legacyproj")
    agent_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='agent:b07h0001'")
    await actions.create_link(agent_oid, proj, "governs", "test", datetime.now(UTC), 0.9)

    out = await project_identity_evidence(actions.pool, seat_id=seat["seat_id"])

    assert out["candidates"]["legacyproj"]["declared_charter"] is True


async def test_candidates_key_by_live_name_not_stale_canonical_after_a_rename(
    actions: Actions, tmp_path,
) -> None:
    """Caught live on the real xxit->handlingtheloop rename (#110): rename_project
    changes ONLY the `name` property, never `canonical` — a reader keyed on canonical
    would report the OLD label forever, and the read-back meant to CONFIRM a rename
    would instead show it as still unresolved. charter/write-attribution here must key
    by the object's CURRENT name."""
    from src.orchestrator.project_identity import rename_project

    office = tmp_path / "office"
    office.mkdir()
    seat = await ensure_seat(actions, house="osiris", handle="Renameseat",
                             anchor_cwd=str(office), source="test")
    await _mk_agent(actions, "agent:1e11ab01")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:1e11ab01")
    proj = await _mk_project(actions, "staleslug")
    seat_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", seat["seat_id"])
    await actions.create_link(seat_oid, proj, "governs", "test", datetime.now(UTC), 0.9)
    t = await actions.create_or_find_object("Thread", "thread:renamekeep1", "test")
    await actions.create_link(t, proj, "in_repo", "agent:1e11ab01", datetime.now(UTC), 0.9)

    out = await rename_project(actions, project="staleslug", new_name="freshname",
                               because="x", actor="agent:test")
    assert out["new_name"] == "freshname"

    ev = await project_identity_evidence(actions.pool, seat_id=seat["seat_id"])

    assert "freshname" in ev["candidates"]
    assert "staleslug" not in ev["candidates"]  # the OLD label must not linger as a ghost
    fresh = ev["candidates"]["freshname"]
    assert fresh["declared_charter"] is True
    assert fresh["write_attribution"]["count"] == 1


async def test_write_attribution_ignores_a_healed_invalidated_edge(
    actions: Actions, tmp_path,
) -> None:
    """Caught live re-running this tool right after Sekhmet/Thoth confirmed the real
    redmonth/ballgem duplicate fold complete: it still reported writes split 33/112
    across two projects, one of them already merged and dead. Root cause: this query
    had NO `valid_until` filter at all — every in_repo edge ever asserted counted,
    including ones a compensating event (fold_project's own estate-heal, invalidate_link
    generally) had already superseded. An invalidated edge must not count."""
    office = tmp_path / "office"
    office.mkdir()
    seat = await ensure_seat(actions, house="osiris", handle="Healseat",
                             anchor_cwd=str(office), source="test")
    await _mk_agent(actions, "agent:hea1seat")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:hea1seat")
    proj = await _mk_project(actions, "healproj")
    live_t = await actions.create_or_find_object("Thread", "thread:healkeeplive", "test")
    await actions.create_link(live_t, proj, "in_repo", "agent:hea1seat", datetime.now(UTC), 0.9)
    dead_t = await actions.create_or_find_object("Thread", "thread:healkeepdead", "test")
    await actions.create_link(dead_t, proj, "in_repo", "agent:hea1seat", datetime.now(UTC), 0.9)
    await actions.invalidate_link(dead_t, proj, "in_repo", "agent:test", datetime.now(UTC))
    seat_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", seat["seat_id"])
    await actions.create_link(seat_oid, proj, "governs", "test", datetime.now(UTC), 0.9)

    ev = await project_identity_evidence(actions.pool, seat_id=seat["seat_id"])

    assert ev["candidates"]["healproj"]["write_attribution"]["count"] == 1


async def test_self_authored_reports_existence_only_never_content(
    actions: Actions, tmp_path,
) -> None:
    office = tmp_path / "office"
    office.mkdir()
    (office / "CLAUDE.md").write_text("this project is definitely spamproj, trust me\n")
    seat = await ensure_seat(actions, house="osiris", handle="Selfseat",
                             anchor_cwd=str(office), source="test")

    out = await project_identity_evidence(actions.pool, seat_id=seat["seat_id"])

    sa = out["self_authored"]["CLAUDE.md"]
    assert sa["exists"] is True and sa["size"] == len(
        "this project is definitely spamproj, trust me\n")
    assert "content" not in sa and "text" not in sa  # never parsed, only stat'd
    assert out["self_authored"]["charter.md"]["exists"] is False
    # and it never leaked into a candidate — nothing here is structurally readable as one
    assert "spamproj" not in out["candidates"]


# --- rename_project (#110, decision 1db1ff41) ---------------------------------------------

async def test_rename_project_keeps_canonical_changes_name_moves_mounts(
    actions: Actions,
) -> None:
    proj = await _mk_project(actions, "xxit")
    from src.orchestrator.mounts import save_mount

    await save_mount(actions.pool, job_dir="/j/xxit", agent_id="agent:deckard1",
                     project="xxit", cwd="/w/xxit", model=None, session_key=None)

    out = await rename_project(actions, project="xxit", new_name="handlingtheloop",
                               because="operator ruling: xxit renamed on its remote",
                               actor="agent:test")

    assert out["project"] == "repo:xxit"          # canonical id NEVER changes
    assert out["old_name"] == "xxit" and out["new_name"] == "handlingtheloop"
    assert out["mounts_moved"] == 1
    row = await actions.pool.fetchrow("SELECT canonical, status FROM objects WHERE id=$1",
                                      proj)
    assert row["canonical"] == "repo:xxit" and row["status"] == "active"
    current_name = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a WHERE a.object_id=$1 "
        "AND a.name='name' ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", proj)
    assert current_name == "handlingtheloop"
    old_still_in_history = await actions.pool.fetchval(
        "SELECT 1 FROM assertions WHERE object_id=$1 AND name='name' "
        "AND value #>> '{}' = 'xxit'", proj)
    assert old_still_in_history == 1               # old name never deleted
    mount_row = await actions.pool.fetchval(
        "SELECT project FROM agent_mounts WHERE job_dir='/j/xxit'")
    assert mount_row == "handlingtheloop"


async def test_rename_project_refuses_blank_new_name_or_because(actions: Actions) -> None:
    await _mk_project(actions, "blankcase")
    out1 = await rename_project(actions, project="blankcase", new_name="",
                                because="x", actor="agent:test")
    assert "new_name is required" in out1["error"]
    out2 = await rename_project(actions, project="blankcase", new_name="newname",
                                because="  ", actor="agent:test")
    assert "because is required" in out2["error"]


async def test_rename_project_refuses_an_unresolved_project(actions: Actions) -> None:
    out = await rename_project(actions, project="nope", new_name="x", because="x",
                               actor="agent:test")
    assert "no such SoftwareProject" in out["error"]


async def test_rename_project_refuses_ambiguity_rather_than_guess(actions: Actions) -> None:
    await _mk_project(actions, "ambirename")
    oid2 = await actions.create_or_find_object("SoftwareProject", "repo:ambirename-other",
                                                "test")
    await actions.assert_property(oid2, "name", "ambirename", "test", datetime.now(UTC), 0.9,
                                  evidence_class="self_declared")

    out = await rename_project(actions, project="ambirename", new_name="clean",
                               because="x", actor="agent:test")

    assert "ambiguous" in out["error"]


async def test_rename_project_refuses_colliding_with_a_different_active_project(
    actions: Actions,
) -> None:
    await _mk_project(actions, "sourceproj")
    await _mk_project(actions, "targetproj")

    out = await rename_project(actions, project="sourceproj", new_name="targetproj",
                               because="x", actor="agent:test")

    assert "already names a DIFFERENT active project" in out["error"]
    assert "fold_project" in out["error"]


# --- fork_project / unfork_project (#110, decision 1db1ff41) ------------------------------

async def test_fork_project_mints_the_edge_and_moves_no_estate(actions: Actions) -> None:
    redmonth = await _mk_project(actions, "forkredmonth")
    ballgem = await _mk_project(actions, "forkballgem")
    t = await actions.create_or_find_object("Thread", "thread:forkkeep1", "test")
    await actions.create_link(t, redmonth, "in_repo", "agent:john", datetime.now(UTC), 0.9)

    out = await fork_project(actions, project="forkredmonth", fork_into="forkballgem",
                             because="John's own decision: new sibling, redmonth untouched",
                             actor="agent:test")

    assert out["forked_from"] == "repo:forkredmonth" and out["into"] == "repo:forkballgem"
    edge = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='forked_from' "
        "AND (valid_until IS NULL OR valid_until > now())", ballgem, redmonth)
    assert edge == 1
    # the estate never moved — redmonth's own in_repo edge still points at redmonth
    still_there = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='in_repo' "
        "AND (valid_until IS NULL OR valid_until > now())", t, redmonth)
    assert still_there == 1


async def test_fork_project_refuses_when_either_side_does_not_exist(actions: Actions) -> None:
    await _mk_project(actions, "onlyone")
    out = await fork_project(actions, project="onlyone", fork_into="phantom",
                             because="x", actor="agent:test")
    assert "unknown SoftwareProject" in out["error"]
    assert "onlyone" not in out["error"]  # only the MISSING side is named


async def test_fork_project_refuses_the_same_label_twice(actions: Actions) -> None:
    await _mk_project(actions, "selfsame")
    out = await fork_project(actions, project="selfsame", fork_into="selfsame",
                             because="x", actor="agent:test")
    assert "nothing to fork" in out["error"]


async def test_fork_project_is_idempotent_not_a_duplicate_mint(actions: Actions) -> None:
    await _mk_project(actions, "idemp1")
    await _mk_project(actions, "idemp2")
    first = await fork_project(actions, project="idemp1", fork_into="idemp2",
                               because="x", actor="agent:test")
    assert "forked_from" in first
    second = await fork_project(actions, project="idemp1", fork_into="idemp2",
                                because="x again", actor="agent:test")
    assert "already carries a live forked_from edge" in second["error"]


async def test_fork_project_refuses_a_retired_side(actions: Actions) -> None:
    from src.orchestrator.projects import retire_project

    await _mk_project(actions, "deadside")
    await _mk_project(actions, "liveside")
    await retire_project(actions, project="deadside", actor="agent:test", because="x")

    out = await fork_project(actions, project="deadside", fork_into="liveside",
                             because="x", actor="agent:test")
    assert "not active" in out["error"]


async def test_unfork_project_reverses_the_edge_reversibility_proven(actions: Actions) -> None:
    """Thoth's own gate (DM 2427): reversibility PROVEN, not claimed — round-trip fork
    then unfork and confirm the edge is actually gone, not just unreported."""
    await _mk_project(actions, "revfrom")
    await _mk_project(actions, "revinto")
    await fork_project(actions, project="revfrom", fork_into="revinto",
                       because="x", actor="agent:test")

    out = await unfork_project(actions, project="revfrom", fork_into="revinto",
                               because="wrongly forked, reverting", actor="agent:test")

    assert out["unforked"] == "repo:revfrom" and out["was_into"] == "repo:revinto"
    from_oid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:revfrom'")
    into_oid = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:revinto'")
    live = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='forked_from' "
        "AND (valid_until IS NULL OR valid_until > now())", into_oid, from_oid)
    assert live is None
    # re-forking after an unfork is not blocked by the idempotency check (the old edge
    # is healed, not still "live")
    again = await fork_project(actions, project="revfrom", fork_into="revinto",
                               because="re-declaring it", actor="agent:test")
    assert "forked_from" in again


async def test_unfork_project_refuses_when_nothing_to_unfork(actions: Actions) -> None:
    await _mk_project(actions, "neverforked1")
    await _mk_project(actions, "neverforked2")
    out = await unfork_project(actions, project="neverforked1", fork_into="neverforked2",
                               because="x", actor="agent:test")
    assert "nothing to unfork" in out["error"]
