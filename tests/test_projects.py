"""THE STUB CULL (msg 1675) — retire_project, a sanctioned status-flip for a dead
SoftwareProject. Every test here demonstrates a refusal the live-signal check exists to
catch, or the disambiguation Thoth flagged by name (a stub project 'ra'/'seshat' is not the
seat of the same name)."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.mounts import save_mount
from src.orchestrator.projects import (
    assert_project_property,
    correct_project_name,
    find_case_variant_projects,
    fold_project,
    normalize_project_casing,
    reconcile_project_fold,
    retire_project,
    unfold_project,
)
from src.orchestrator.seats import ensure_seat

NOW = datetime.now(UTC)


async def _stub_project(actions: Actions, canon: str, name: str | None = "stub") -> None:
    pid = await actions.create_or_find_object("SoftwareProject", canon, "test")
    if name is not None:
        await actions.assert_property(pid, "name", name, "test", NOW, 0.9)


async def test_retire_project_retires_a_dead_stub(actions: Actions) -> None:
    await _stub_project(actions, "repo:tmp", "tmp")
    out = await retire_project(actions, project="tmp", actor="agent:test",
                               because="reap: dead stub")
    assert out["retired_project"] == "repo:tmp"
    assert out["because"] == "reap: dead stub"
    assert len(out["id"]) == 8
    row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical='repo:tmp'")
    assert row["status"] == "retired"
    event = await actions.pool.fetchrow(
        "SELECT event_type, payload FROM object_events WHERE event_type='status_change' "
        "ORDER BY id DESC LIMIT 1")
    assert event is not None
    assert event["payload"]["status"] == "retired"


async def test_retire_project_refuses_blank_because(actions: Actions) -> None:
    await _stub_project(actions, "repo:blank", "blank")
    out = await retire_project(actions, project="blank", actor="agent:test", because="  ")
    assert "because is required" in out["error"]
    row = await actions.pool.fetchrow("SELECT status FROM objects WHERE canonical='repo:blank'")
    assert row["status"] == "active"


async def test_retire_project_refuses_unknown_project(actions: Actions) -> None:
    out = await retire_project(actions, project="does-not-exist", actor="agent:test",
                               because="reap")
    assert "no such SoftwareProject" in out["error"]


async def test_retire_project_refuses_an_already_retired_project(actions: Actions) -> None:
    await _stub_project(actions, "repo:twice", "twice")
    first = await retire_project(actions, project="twice", actor="agent:test", because="reap")
    assert "retired_project" in first
    second = await retire_project(actions, project="twice", actor="agent:test",
                                  because="reap again")
    assert "already retired" in second["error"]


async def test_retire_project_refuses_a_project_with_a_commit(actions: Actions) -> None:
    pid = await actions.create_or_find_object("SoftwareProject", "repo:has-commits", "test")
    await actions.assert_property(pid, "name", "has-commits", "test", NOW, 0.9)
    c = await actions.create_or_find_object("Commit", "commit:c1", "test")
    await actions.create_link(c, pid, "in_repo", "test", NOW, 0.9)
    out = await retire_project(actions, project="has-commits", actor="agent:test", because="reap")
    assert "commit(s)" in out["error"]
    row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical='repo:has-commits'")
    assert row["status"] == "active"


async def test_retire_project_refuses_a_project_with_an_open_thread(actions: Actions) -> None:
    pid = await actions.create_or_find_object("SoftwareProject", "repo:has-thread", "test")
    await actions.assert_property(pid, "name", "has-thread", "test", NOW, 0.9)
    t = await actions.create_or_find_object("Thread", "thread:t1", "test")
    await actions.create_link(t, pid, "in_repo", "test", NOW, 0.9)
    out = await retire_project(actions, project="has-thread", actor="agent:test", because="reap")
    assert "open thread(s)" in out["error"]


async def test_retire_project_ignores_a_resolved_thread(actions: Actions) -> None:
    pid = await actions.create_or_find_object("SoftwareProject", "repo:closed-thread", "test")
    await actions.assert_property(pid, "name", "closed-thread", "test", NOW, 0.9)
    t = await actions.create_or_find_object("Thread", "thread:t2", "test")
    await actions.create_link(t, pid, "in_repo", "test", NOW, 0.9)
    await actions.pool.execute("UPDATE objects SET status='resolved' WHERE id=$1", t)
    out = await retire_project(actions, project="closed-thread", actor="agent:test", because="reap")
    assert "retired_project" in out


async def test_retire_project_refuses_a_live_mount(actions: Actions) -> None:
    await _stub_project(actions, "repo:live-mount", "live-mount")
    await save_mount(actions.pool, job_dir="/j/1", agent_id="agent:x", project="live-mount",
                     cwd="/w/live-mount", model="claude-fable-5", session_key=None, alive=True)
    out = await retire_project(actions, project="live-mount", actor="agent:test", because="reap")
    assert "live mount" in out["error"]


async def test_retire_project_ignores_a_stale_mount(actions: Actions) -> None:
    await _stub_project(actions, "repo:stale-mount", "stale-mount")
    await save_mount(actions.pool, job_dir="/j/2", agent_id="agent:x", project="stale-mount",
                     cwd="/w/stale-mount", model="claude-fable-5", session_key=None, alive=True)
    stale = datetime.now(UTC) - timedelta(hours=6)
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen=$1 WHERE project='stale-mount'", stale)
    out = await retire_project(actions, project="stale-mount", actor="agent:test", because="reap")
    assert "retired_project" in out


async def test_retire_project_resolves_by_short_id_and_bare_canonical(actions: Actions) -> None:
    pid = await actions.create_or_find_object("SoftwareProject", "repo:short-id", "test")
    await actions.assert_property(pid, "name", "short-id", "test", NOW, 0.9)
    short = str(pid)[:8]
    out = await retire_project(actions, project=short, actor="agent:test", because="reap")
    assert out["retired_project"] == "repo:short-id"


async def test_retire_project_never_touches_a_seat_of_the_same_name(actions: Actions) -> None:
    """The exact disambiguation Thoth flagged: 'ra' and 'seshat' name stub PROJECTS, not
    the seats of the same names — retiring the project must leave a same-named seat alone,
    and must never accidentally resolve INTO the seat."""
    seat = await ensure_seat(actions, house="osiris", handle="ra", source="test")
    await _stub_project(actions, "repo:ra", "ra")
    out = await retire_project(actions, project="ra", actor="agent:test", because="reap stub")
    assert out["retired_project"] == "repo:ra"          # resolved the PROJECT, not the seat
    seat_row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical=$1", seat["seat_id"])
    assert seat_row["status"] == "active"               # the seat is untouched
    project_row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical='repo:ra'")
    assert project_row["status"] == "retired"


async def test_retire_project_refuses_when_only_a_seat_of_that_name_exists(
    actions: Actions,
) -> None:
    """No SoftwareProject named 'ghost' exists — only a same-named Seat. The resolver must
    stay scoped to type='SoftwareProject' and refuse rather than silently retiring the seat."""
    await ensure_seat(actions, house="osiris", handle="ghost", source="test")
    out = await retire_project(actions, project="ghost", actor="agent:test", because="reap")
    assert "no such SoftwareProject" in out["error"]


# ═══ assert_project_property (task #74) — the sanctioned write for a single
# project-scoped property, closing the gap that forced in-process scripts for anything
# beyond a status flip during the reap.

async def test_assert_project_property_stamps_a_named_property(actions: Actions) -> None:
    await _stub_project(actions, "repo:app1", "app1")
    out = await assert_project_property(actions, project="app1", name="merged_into",
                                        value="repo:bytebye", actor="agent:test")
    assert out == {"project": "repo:app1", "name": "merged_into", "value": "repo:bytebye"}
    val = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
        "ON a.object_id=o.id AND a.name='merged_into' WHERE o.canonical='repo:app1'")
    assert val == "repo:bytebye"


async def test_assert_project_property_refuses_a_blank_project(actions: Actions) -> None:
    out = await assert_project_property(actions, project=" ", name="x", value="y",
                                        actor="agent:test")
    assert "project is required" in out["error"]


async def test_assert_project_property_refuses_a_blank_name(actions: Actions) -> None:
    await _stub_project(actions, "repo:app2", "app2")
    out = await assert_project_property(actions, project="app2", name=" ", value="y",
                                        actor="agent:test")
    assert "name is required" in out["error"]


async def test_assert_project_property_refuses_a_blank_value(actions: Actions) -> None:
    await _stub_project(actions, "repo:app3", "app3")
    out = await assert_project_property(actions, project="app3", name="x", value=" ",
                                        actor="agent:test")
    assert "value is required" in out["error"]


async def test_assert_project_property_refuses_an_unknown_project(actions: Actions) -> None:
    out = await assert_project_property(actions, project="does-not-exist", name="x",
                                        value="y", actor="agent:test")
    assert "no such SoftwareProject" in out["error"]


async def test_assert_project_property_refuses_status(actions: Actions) -> None:
    """status has its own compensating-event path (retire_project) — a bare assertion
    here would reopen the exact STATUS GAP class already fixed once for seats."""
    await _stub_project(actions, "repo:app4", "app4")
    out = await assert_project_property(actions, project="app4", name="status",
                                        value="retired", actor="agent:test")
    assert "its own compensating-event path" in out["error"]
    row = await actions.pool.fetchrow("SELECT status FROM objects WHERE canonical='repo:app4'")
    assert row["status"] == "active"


async def test_assert_project_property_never_touches_a_seat_of_the_same_name(
    actions: Actions,
) -> None:
    seat = await ensure_seat(actions, house="osiris", handle="ra2", source="test")
    await _stub_project(actions, "repo:ra2", "ra2")
    out = await assert_project_property(actions, project="ra2", name="note", value="x",
                                        actor="agent:test")
    assert out["project"] == "repo:ra2"
    seat_val = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM objects o JOIN current_assertions a "
        "ON a.object_id=o.id AND a.name='note' WHERE o.canonical=$1", seat["seat_id"])
    assert seat_val is None


# ═══ fold_project (task #102's lane 2, Thoth's dispatch DM 2302/2310) — the deliberate,
# evidence-gated cure for a genuine TWIN. Never executed live tonight: these tests build
# and prove the verb, they do not fold osiris's own real fragments.

async def test_fold_project_moves_the_estate_and_merges(actions: Actions) -> None:
    await _stub_project(actions, "repo:dupe1", "dupe1")
    await _stub_project(actions, "repo:into1", "into1")
    dupe_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:dupe1'")
    into_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:into1'")

    commit = await actions.create_or_find_object("Commit", "commit:foldc1", "test")
    await actions.create_link(commit, dupe_id, "in_repo", "test", NOW, 0.9)
    agent = await actions.create_or_find_object("Agent", "agent:foldworker", "test")
    await actions.create_link(agent, dupe_id, "works_in", "test", NOW, 0.9)
    await actions.create_link(agent, dupe_id, "governs", "test", NOW, 0.9)
    ref = await actions.create_or_find_object("Reference", "ref:foldref", "test")
    await actions.create_link(ref, dupe_id, "informs", "test", NOW, 0.9)
    await save_mount(actions.pool, job_dir="/j/fold1", agent_id="agent:x", project="dupe1",
                     cwd="/w/dupe1", model="claude-fable-5", session_key=None, alive=True)

    out = await fold_project(actions, dupe="dupe1", into="into1",
                             evidence="both mint the same repo, confirmed by the operator",
                             actor="agent:test")
    assert out["folded"] == "repo:dupe1" and out["into"] == "repo:into1"
    assert out["edges_moved"] == {"in_repo": 1, "works_in": 1, "governs": 1, "informs": 1}
    assert out["mounts_moved"] == 1

    row = await actions.pool.fetchrow(
        "SELECT status, merged_into FROM objects WHERE id=$1", dupe_id)
    assert row["status"] == "merged" and row["merged_into"] == into_id

    for from_id, link_type in ((commit, "in_repo"), (agent, "works_in"), (agent, "governs"),
                               (ref, "informs")):
        live_to_into = await actions.pool.fetchval(
            "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type=$3 "
            "AND (valid_until IS NULL OR valid_until > now())", from_id, into_id, link_type)
        assert live_to_into == 1, f"{link_type} edge never re-pointed to into"
        live_to_dupe = await actions.pool.fetchval(
            "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type=$3 "
            "AND (valid_until IS NULL OR valid_until > now())", from_id, dupe_id, link_type)
        assert live_to_dupe is None, f"{link_type} edge still live on dupe after fold"

    mount_project = await actions.pool.fetchval(
        "SELECT project FROM agent_mounts WHERE job_dir='/j/fold1'")
    assert mount_project == "into1"


async def test_fold_project_is_idempotent_on_an_edge_already_live_to_into(
    actions: Actions,
) -> None:
    """The estate re-point must never duplicate a link `into` already has — the SAME
    "re-capture is a no-op" discipline link_repo/grounds already follow."""
    await _stub_project(actions, "repo:dupe2", "dupe2")
    await _stub_project(actions, "repo:into2", "into2")
    dupe_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:dupe2'")
    into_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:into2'")
    commit = await actions.create_or_find_object("Commit", "commit:foldc2", "test")
    await actions.create_link(commit, dupe_id, "in_repo", "test", NOW, 0.9)
    await actions.create_link(commit, into_id, "in_repo", "test", NOW, 0.9)  # already there

    await fold_project(actions, dupe="dupe2", into="into2", evidence="one project, two mints",
                       actor="agent:test")
    count = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND to_id=$2 AND type='in_repo'",
        commit, into_id)
    assert count == 1, "the already-live edge to into was duplicated, not deduped"


async def test_fold_project_refuses_blank_evidence(actions: Actions) -> None:
    await _stub_project(actions, "repo:fe1", "fe1")
    await _stub_project(actions, "repo:fe2", "fe2")
    out = await fold_project(actions, dupe="fe1", into="fe2", evidence="  ", actor="agent:test")
    assert "auto-merge wearing a signature" in out["error"]
    row = await actions.pool.fetchrow("SELECT status FROM objects WHERE canonical='repo:fe1'")
    assert row["status"] == "active"


async def test_fold_project_refuses_dupe_equals_into(actions: Actions) -> None:
    await _stub_project(actions, "repo:same1", "same1")
    out = await fold_project(actions, dupe="same1", into="same1", evidence="x",
                             actor="agent:test")
    assert "nothing to fold" in out["error"]


async def test_fold_project_refuses_an_unknown_dupe(actions: Actions) -> None:
    await _stub_project(actions, "repo:realinto", "realinto")
    out = await fold_project(actions, dupe="ghost-project", into="realinto", evidence="x",
                             actor="agent:test")
    assert "unknown SoftwareProject" in out["error"] and "ghost-project" in out["error"]


async def test_fold_project_refuses_a_missing_target_and_names_rename_as_the_right_tool(
    actions: Actions,
) -> None:
    """The redmonth correction (Thoth DM 2310): fold_project NEVER find-or-CREATEs `into`
    — a missing target means the caller wants a rename, and the refusal says so."""
    await _stub_project(actions, "repo:realdupe", "realdupe")
    out = await fold_project(actions, dupe="realdupe", into="does-not-exist-yet",
                             evidence="x", actor="agent:test")
    assert "unknown SoftwareProject" in out["error"]
    assert "RENAME, not a fold" in out["error"]
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE canonical='repo:does-not-exist-yet'") == 0


async def test_fold_project_refuses_when_dupe_already_merged(actions: Actions) -> None:
    await _stub_project(actions, "repo:am1", "am1")
    await _stub_project(actions, "repo:am2", "am2")
    await _stub_project(actions, "repo:am3", "am3")
    await fold_project(actions, dupe="am1", into="am2", evidence="first fold",
                       actor="agent:test")
    out = await fold_project(actions, dupe="am1", into="am3", evidence="second fold",
                             actor="agent:test")
    assert "already folded" in out["error"]


async def test_fold_project_refuses_when_into_is_already_merged(actions: Actions) -> None:
    await _stub_project(actions, "repo:ai1", "ai1")
    await _stub_project(actions, "repo:ai2", "ai2")
    await _stub_project(actions, "repo:ai3", "ai3")
    await fold_project(actions, dupe="ai1", into="ai2", evidence="first fold",
                       actor="agent:test")
    out = await fold_project(actions, dupe="ai3", into="ai1", evidence="second fold",
                             actor="agent:test")
    assert "itself folded" in out["error"]


async def test_fold_project_refuses_a_genuine_cross_object_contradiction(
    actions: Actions,
) -> None:
    """The operator's rule (task #102): SAME tag, DIFFERENT data means these may be TWO
    projects, not one under two names. A wrong merge destroys the recorded disagreement,
    which was data — fold_project must refuse rather than silently pick a winner."""
    await _stub_project(actions, "repo:cd1", "cd1")
    await _stub_project(actions, "repo:cd2", "cd2")
    cd1_id = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:cd1'")
    cd2_id = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:cd2'")
    await actions.assert_property(cd1_id, "language", "python", "agent:alice", NOW, 0.9)
    await actions.assert_property(cd2_id, "language", "go", "agent:bob", NOW, 0.9)

    out = await fold_project(actions, dupe="cd1", into="cd2", evidence="looks like a twin",
                             actor="agent:test")
    assert "contradicting values" in out["error"] and "language" in out["error"]
    assert out["contradicted_on"] == ["language"]
    row = await actions.pool.fetchrow("SELECT status FROM objects WHERE canonical='repo:cd1'")
    assert row["status"] == "active"


async def test_fold_project_does_not_refuse_on_a_differing_name(actions: Actions) -> None:
    """Two different `name` properties is the fold's own PREMISE (two tags, one referent)
    — never treated as a conflict, unlike a genuinely differing OTHER property."""
    await _stub_project(actions, "repo:dn1", "dn1-label")
    await _stub_project(actions, "repo:dn2", "dn2-label")
    out = await fold_project(actions, dupe="dn1", into="dn2",
                             evidence="two labels, one repo", actor="agent:test")
    assert "folded" in out


async def test_fold_project_never_gates_on_commits(actions: Actions) -> None:
    """Thoth's explicit requirement from redmonth's own warning (DM 2310): unlike
    retire_project's stub-cull guard, a project WITH commits attached must still fold —
    graph presence being real is exactly the case this verb has to handle."""
    await _stub_project(actions, "repo:hc1", "hc1")
    await _stub_project(actions, "repo:hc2", "hc2")
    dupe_id = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:hc1'")
    commit = await actions.create_or_find_object("Commit", "commit:foldc3", "test")
    await actions.create_link(commit, dupe_id, "in_repo", "test", NOW, 0.9)

    out = await fold_project(actions, dupe="hc1", into="hc2", evidence="commits and all",
                             actor="agent:test")
    assert "folded" in out


async def test_fold_project_is_reversible_via_unmerge(actions: Actions) -> None:
    """Reversibility is non-negotiable (Thoth's explicit ask): prove the UNWIND, not just
    the merge — merged_into + status='merged', never a delete, and unmerge_objects
    restores the projection exactly."""
    await _stub_project(actions, "repo:rev1", "rev1")
    await _stub_project(actions, "repo:rev2", "rev2")
    dupe_id = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:rev1'")
    into_id = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:rev2'")

    await fold_project(actions, dupe="rev1", into="rev2", evidence="reversibility check",
                       actor="agent:test")
    merged_row = await actions.pool.fetchrow(
        "SELECT status, merged_into FROM objects WHERE id=$1", dupe_id)
    assert merged_row["status"] == "merged" and merged_row["merged_into"] == into_id

    await actions.unmerge_objects(dupe_id, justification="reversibility check undone",
                                  actor="agent:test")
    restored_row = await actions.pool.fetchrow(
        "SELECT status, merged_into FROM objects WHERE id=$1", dupe_id)
    assert restored_row["status"] == "active" and restored_row["merged_into"] is None
    # the merge event and the same_as link both stay — witnesses of the era, never erased
    # (merge_objects stamps object_id=winner, related_id=loser — core.py:484-492)
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM object_events WHERE object_id=$1 AND related_id=$2 "
        "AND event_type='merge'", into_id, dupe_id) == 1
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND to_id=$2 AND type='same_as'",
        dupe_id, into_id) == 1


# ═══ unfold_project (ruling 31c02dca's PARITY requirement: fold_project had NO reversal
# before this — a Project fold was permanent, task #127's own named case) ═══


async def test_unfold_project_dry_run_plans_the_estate_restore(actions: Actions) -> None:
    await _stub_project(actions, "repo:up1dupe0", "up1dupe0")
    await _stub_project(actions, "repo:up1into0", "up1into0")
    dupe_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:up1dupe0'")
    commit = await actions.create_or_find_object("Commit", "commit:up1c0001", "test")
    await actions.create_link(commit, dupe_id, "in_repo", "test", NOW, 0.9)

    await fold_project(actions, dupe="up1dupe0", into="up1into0", evidence="test: a twin",
                       actor="agent:test")

    out = await unfold_project(actions, dupe="repo:up1dupe0",
                               because="wrongful fold — a real second project",
                               actor="agent:judge")
    assert out["execute"] is False
    assert out["was_merged_into"] == "repo:up1into0"
    ops = {p["op"] for p in out["plan"]}
    assert "unmerge_objects" in ops and "move_link" in ops
    st = await actions.pool.fetchval(
        "SELECT status FROM objects WHERE canonical='repo:up1dupe0'")
    assert st == "merged"  # dry run never writes


async def test_unfold_project_executed_restores_the_estate(actions: Actions) -> None:
    await _stub_project(actions, "repo:up2dupe0", "up2dupe0")
    await _stub_project(actions, "repo:up2into0", "up2into0")
    dupe_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:up2dupe0'")
    into_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:up2into0'")
    commit = await actions.create_or_find_object("Commit", "commit:up2c0001", "test")
    await actions.create_link(commit, dupe_id, "in_repo", "test", NOW, 0.9)

    await fold_project(actions, dupe="up2dupe0", into="up2into0", evidence="test: a twin",
                       actor="agent:test")

    out = await unfold_project(actions, dupe="repo:up2dupe0",
                               because="a real second project, wrongly folded",
                               actor="agent:judge", execute=True)

    assert out["unmerged"] is True
    assert out["edges_restored"] == 1
    row = await actions.pool.fetchrow(
        "SELECT status, merged_into FROM objects WHERE canonical='repo:up2dupe0'")
    assert row["status"] == "active" and row["merged_into"] is None
    live_to_dupe = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='in_repo' "
        "AND (valid_until IS NULL OR valid_until > now())", commit, dupe_id)
    assert live_to_dupe == 1
    live_to_into = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='in_repo' "
        "AND (valid_until IS NULL OR valid_until > now())", commit, into_id)
    assert live_to_into is None
    same_as = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='same_as'",
        dupe_id, into_id)
    assert same_as == 1


async def test_unfold_project_refuses_a_never_folded_dupe(actions: Actions) -> None:
    await _stub_project(actions, "repo:up3free0", "up3free0")
    out = await unfold_project(actions, dupe="up3free0", because="x", actor="agent:judge")
    assert "not folded" in out["error"]


async def test_unfold_project_refuses_a_blank_because(actions: Actions) -> None:
    await _stub_project(actions, "repo:up4dupe0", "up4dupe0")
    await _stub_project(actions, "repo:up4into0", "up4into0")
    await fold_project(actions, dupe="up4dupe0", into="up4into0", evidence="x",
                       actor="agent:test")
    out = await unfold_project(actions, dupe="up4dupe0", because="   ", actor="agent:judge")
    assert "because" in out["error"]


async def test_unfold_project_refuses_an_unknown_dupe(actions: Actions) -> None:
    out = await unfold_project(actions, dupe="does-not-exist-up5", because="x",
                               actor="agent:judge")
    assert "no such SoftwareProject" in out["error"]


async def test_unfold_project_refuses_an_operator_blessed_fold_without_fresh_operator_word(
    actions: Actions,
) -> None:
    await _stub_project(actions, "repo:up6dupe0", "up6dupe0")
    await _stub_project(actions, "repo:up6into0", "up6into0")
    await fold_project(actions, dupe="up6dupe0", into="up6into0",
                       evidence="the operator confirmed these are one project, 2026-07-01",
                       actor="operator")

    out = await unfold_project(actions, dupe="repo:up6dupe0",
                               because="I think this was wrong", actor="agent:judge")
    assert "operator" in out["error"]
    st = await actions.pool.fetchval(
        "SELECT status FROM objects WHERE canonical='repo:up6dupe0'")
    assert st == "merged"  # refused, nothing written

    out2 = await unfold_project(
        actions, dupe="repo:up6dupe0",
        because="the operator's fresh word, 2026-07-28: this fold was wrong",
        actor="agent:judge", execute=True)
    assert out2["unmerged"] is True


async def test_unfold_project_does_not_restore_an_edge_that_moved_on_since(
    actions: Actions,
) -> None:
    """The unfold_agent honesty model, generalized to links: an edge only gets restored to
    dupe when it is still live and pointing at into exactly where the fold left it. An
    edge later re-pointed to a THIRD project is never guessed back."""
    await _stub_project(actions, "repo:up7dupe0", "up7dupe0")
    await _stub_project(actions, "repo:up7into0", "up7into0")
    await _stub_project(actions, "repo:up7thrd0", "up7thrd0")
    dupe_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:up7dupe0'")
    into_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:up7into0'")
    third_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:up7thrd0'")
    commit = await actions.create_or_find_object("Commit", "commit:up7c0001", "test")
    await actions.create_link(commit, dupe_id, "in_repo", "test", NOW, 0.9)

    await fold_project(actions, dupe="up7dupe0", into="up7into0", evidence="test: a twin",
                       actor="agent:test")
    # the edge moves on to a THIRD project, unrelated to the fold
    now = datetime.now(UTC)
    await actions.invalidate_link(commit, into_id, "in_repo", "test", now)
    await actions.create_link(commit, third_id, "in_repo", "test", now, 0.9)

    out = await unfold_project(actions, dupe="repo:up7dupe0", because="wrongly folded",
                               actor="agent:judge", execute=True)
    assert out["edges_restored"] == 0
    live_to_third = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='in_repo' "
        "AND (valid_until IS NULL OR valid_until > now())", commit, third_id)
    assert live_to_third == 1, (
        "an edge that moved on since the fold is never pulled back onto the reversed project")


async def test_unfold_project_reports_unreturnable_mounts(actions: Actions) -> None:
    await _stub_project(actions, "repo:up8dupe0", "up8dupe0")
    await _stub_project(actions, "repo:up8into0", "up8into0")
    await save_mount(actions.pool, job_dir="/j/up8", agent_id="agent:x", project="up8into0",
                     cwd="/w/up8", model="claude-fable-5", session_key=None, alive=True)

    await fold_project(actions, dupe="up8dupe0", into="up8into0", evidence="x",
                       actor="agent:test")

    out = await unfold_project(actions, dupe="repo:up8dupe0", because="wrongly folded",
                               actor="agent:judge")  # dry run
    assert len(out["estate_unreturnable"]["mounts"]) == 1


# ═══ the fold_project MCP wrapper — now `merge` (decision 5dbd4dce closed; collapsed
# under ruling 31c02dca) — the gate must survive exposure through the tool layer, and the
# reversibility witnesses must reach the caller, not just the graph. ═══


class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


async def _mounted(actions: Actions, agent_id: str, project: str) -> _Ctx:
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    ctx = _Ctx()
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id=agent_id, session=agent_id, project=project, model=None, cwd=None)
    return ctx


async def test_fold_project_tool_still_refuses_a_contradiction_through_the_wrapper(
    actions: Actions,
) -> None:
    """The gate must survive exposure — the wrapper is not a second, softer path to the
    same act. Now reached through `merge`, not a dedicated fold_project tool (ruling
    31c02dca) — "wcd1"/"wcd2" carry no agent:/seat: prefix, so `merge` routes them to
    fold_project exactly as the old dedicated tool did."""
    from src import mcp_server as srv

    await _stub_project(actions, "repo:wcd1", "wcd1")
    await _stub_project(actions, "repo:wcd2", "wcd2")
    cd1_id = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:wcd1'")
    cd2_id = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:wcd2'")
    await actions.assert_property(cd1_id, "language", "python", "agent:alice", NOW, 0.9)
    await actions.assert_property(cd2_id, "language", "go", "agent:bob", NOW, 0.9)

    saved_pool = srv._pool
    ctx = await _mounted(actions, "agent:foldtool1", "foldtoolproj")
    try:
        out = await srv.merge(dupe="wcd1", into="wcd2",
                              evidence="looks like a twin", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert "contradicting values" in out["error"] and out["contradicted_on"] == ["language"]
    row = await actions.pool.fetchrow("SELECT status FROM objects WHERE canonical='repo:wcd1'")
    assert row["status"] == "active"


async def test_fold_project_tool_surfaces_the_merge_event_and_same_as_link(
    actions: Actions,
) -> None:
    """Reversibility must reach the CALLER, not stay implicit in the graph — the receipt
    names the merge event and the same_as link `merge_objects` mints, the two witnesses
    an `unmerge_objects` reversal needs. Reached through `merge` now (ruling 31c02dca)."""
    from src import mcp_server as srv

    await _stub_project(actions, "repo:wrev1", "wrev1")
    await _stub_project(actions, "repo:wrev2", "wrev2")
    dupe_id = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:wrev1'")
    into_id = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:wrev2'")

    saved_pool = srv._pool
    ctx = await _mounted(actions, "agent:foldtool2", "foldtoolproj")
    try:
        out = await srv.merge(dupe="wrev1", into="wrev2",
                              evidence="reversibility reaches the caller", ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    assert out["folded"] == "repo:wrev1" and out["into"] == "repo:wrev2"
    event_id = await actions.pool.fetchval(
        "SELECT id FROM object_events WHERE object_id=$1 AND related_id=$2 "
        "AND event_type='merge'", into_id, dupe_id)
    link_id = await actions.pool.fetchval(
        "SELECT id FROM links WHERE from_id=$1 AND to_id=$2 AND type='same_as'",
        dupe_id, into_id)
    assert out["merge_event_id"] == event_id
    assert out["same_as_link_id"] == link_id


# --- ambiguity refusal (#110, decision 1db1ff41) ------------------------------------------

async def test_resolve_refuses_a_label_collision_rather_than_guess(actions: Actions) -> None:
    """The ballgem/sutra SHAPE, synthesized: one object findable by canonical alone
    (repo:twin), a SIBLING findable only through its own `name` property (canonicaled
    something else entirely) — the old fallback was LIMIT 1 with no ORDER BY, whichever
    row postgres felt like. Every caller (retire_project here; fold_project/
    correct_project_name share the same wrapper) must refuse and name both, never pick."""
    await _stub_project(actions, "repo:twin", "twin")
    await _stub_project(actions, "repo:twin-elsewhere", "twin")

    out = await retire_project(actions, project="twin", actor="agent:test", because="x")

    assert "ambiguous" in out["error"]
    assert "repo:twin" in out["error"] and "repo:twin-elsewhere" in out["error"]
    row = await actions.pool.fetchrow("SELECT status FROM objects WHERE canonical='repo:twin'")
    assert row["status"] == "active"  # refused — nothing written


async def test_resolve_by_exact_canonical_is_never_ambiguous_even_with_a_name_collision(
    actions: Actions,
) -> None:
    """A caller who names the exact canonical is never blocked by a collision on the
    FUZZY name lookup alone — an id/canonical reference is a real disambiguation the
    caller already did, and the combined check still finds it as one of the matches,
    not zero."""
    await _stub_project(actions, "repo:exactcase", "exactcase")
    await _stub_project(actions, "repo:exactcase-twin", "exactcase")

    out = await retire_project(actions, project="repo:exactcase", actor="agent:test",
                               because="x")
    assert "ambiguous" in out["error"]  # still correctly caught — canonical alone matched
    assert "repo:exactcase" in out["error"] and "repo:exactcase-twin" in out["error"]


async def test_fold_project_refuses_an_ambiguous_dupe_or_into(actions: Actions) -> None:
    await _stub_project(actions, "repo:famb1", "amb")
    await _stub_project(actions, "repo:famb2", "amb")
    await _stub_project(actions, "repo:fclean", "clean")

    out = await fold_project(actions, dupe="amb", into="fclean",
                             evidence="x", actor="agent:test")
    assert "ambiguous" in out["error"]


# --- correct_project_name (#110, decision 1db1ff41 — the delegated exception) -------------

async def _name_history(actions: Actions, canon: str, values: list[str]) -> None:
    pid = await actions.create_or_find_object("SoftwareProject", canon, "test")
    for i, v in enumerate(values):
        await actions.assert_property(pid, "name", v, f"agent:hist{i:04d}",
                                      NOW + timedelta(minutes=i), 0.9,
                                      evidence_class="self_declared")


async def test_correct_project_name_refuses_the_real_bytebye_pair_its_not_case_drift(
    actions: Actions,
) -> None:
    """THE OPENING TEST, per Thoth's own order — and it does NOT settle. I mischaracterized
    repo:bytebye's contradiction as case-only drift when I first found it (DM 2441/2443),
    and Thoth built on that framing too. Building this verb caught the error: 'bytebye'
    (byte+bye: b-y-t-e-b-y-e) and 'ByeByte' (Bye+Byte: b-y-e-b-y-t-e) are NOT case
    variants of one string — they differ at the THIRD character even after casefold
    ('t' vs 'e'). It is a genuine transposition, not a flip-flop, and the negative
    control is exactly what stops correct_project_name from settling it wrong. Using the
    real values here, not a synthesized stand-in, because the mistake was in reading the
    real data, and the regression test has to be the real data."""
    await _name_history(actions, "repo:bytebyereal",
                        ["bytebye", "ByeByte", "bytebye", "ByeByte", "bytebye"])

    out = await correct_project_name(actions, project="bytebyereal", actor="agent:test")

    assert "error" in out and "rename_project" in out["error"]
    assert set(out["distinct_names"]) == {"bytebye", "ByeByte"}
    winner = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='repo:bytebyereal' AND a.name='name' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1")
    assert winner == "bytebye"  # unchanged — refused, nothing written


async def test_correct_project_name_settles_a_genuine_case_only_flip_flop(
    actions: Actions,
) -> None:
    """The shape correct_project_name actually exists for: TRUE case variants of the
    SAME letters, oscillating because the resolver picks whichever was asserted most
    recently at identical confidence. Settling to the MAJORITY (not the latest) is what
    stops the oscillation; settling to the latest would just be the same bug with extra
    steps."""
    await _name_history(actions, "repo:trueflipflop",
                        ["byteclub", "ByteClub", "byteclub", "ByteClub", "byteclub"])

    out = await correct_project_name(actions, project="trueflipflop", actor="agent:test")

    assert out["corrected"] is True
    assert out["settled_to"] == "byteclub"  # 3 of 5, the majority
    assert out["vote"] == {"byteclub": 3, "ByteClub": 2}
    winner = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='repo:trueflipflop' AND a.name='name' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1")
    assert winner == "byteclub"


async def test_correct_project_name_surfaces_prior_art_never_refuses_on_it(
    actions: Actions, monkeypatch,
) -> None:
    """obligation e4612853's sibling (Thoth DM 3169/3185): this verb's own majority-vote
    can legitimately re-settle onto a casing a standing operator ruling specifically
    rejected — the receipt must surface that ruling, never block the settle."""
    await _name_history(
        actions, "repo:byebyte", ["byebyte", "ByeByte", "byebyte", "ByeByte", "byebyte"])

    async def _fake_prior_art(pool, *, subject_canonical, field, new_value, actor, because=""):
        return {"prior_art": [{"id": "1db87191", "type": "Decision"}],
               "prior_art_flag": f"a standing ruling (1db87191) may already cover "
                                  f"{subject_canonical}'s {field!r}"}

    monkeypatch.setattr("src.orchestrator.capture.property_prior_art", _fake_prior_art)
    out = await correct_project_name(actions, project="byebyte", actor="agent:test")
    assert out["corrected"] is True  # the settle still happened
    assert out["prior_art_flag"] == (
        "a standing ruling (1db87191) may already cover repo:byebyte's 'name'")


async def test_correct_project_name_refuses_a_genuine_rename(actions: Actions) -> None:
    """THE NEGATIVE CONTROL (ruling 1db1ff41's own bar): redmonth vs ballgem is not
    case/whitespace drift, it is two different names — correct_project_name must refuse
    rather than silently pick a majority, or the delegation stops being safe."""
    await _name_history(actions, "repo:driftorfork", ["redmonth", "redmonth", "ballgem"])

    out = await correct_project_name(actions, project="driftorfork", actor="agent:test")

    assert "error" in out and "rename_project" in out["error"]
    assert set(out["distinct_names"]) == {"redmonth", "ballgem"}
    winner = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='repo:driftorfork' AND a.name='name' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1")
    assert winner == "ballgem"  # unchanged — refused, nothing written


async def test_correct_project_name_refuses_a_tie_rather_than_guess(actions: Actions) -> None:
    await _name_history(actions, "repo:tiedname", ["Foo", "foo"])

    out = await correct_project_name(actions, project="tiedname", actor="agent:test")

    assert "error" in out and "tied" in out["error"]


async def test_correct_project_name_is_a_noop_on_an_already_settled_name(
    actions: Actions,
) -> None:
    await _stub_project(actions, "repo:settled", "settled")

    out = await correct_project_name(actions, project="settled", actor="agent:test")

    assert out == {"project": "repo:settled", "corrected": False,
                   "note": "already a single value — nothing to correct"}


async def test_correct_project_name_refuses_unknown_project(actions: Actions) -> None:
    out = await correct_project_name(actions, project="nope-does-not-exist",
                                     actor="agent:test")
    assert "no such SoftwareProject" in out["error"]


# --- find_case_variant_projects (operator ruling 2026-07-31: "the capitalization "
# --- merging should be automatic not bottlenecked by me") --------------------------------

async def test_find_case_variant_projects_classifies_proven_vs_genuine(
    actions: Actions,
) -> None:
    """The exact split the operator's widened rule depends on: a TRUE case variant is
    `proven` (correct_project_name can settle it unaided); a bytebye-shaped
    transposition, or a genuine rename like redmonth/ballgem, is `genuine` and stays
    the operator's — same proof, same classification, as correct_project_name's own
    guard, so a caller of this survey never gets a different verdict at settle time."""
    await _name_history(actions, "repo:surveycase", ["surveyname", "SurveyName"])
    await _name_history(actions, "repo:surveytrans", ["bytebye", "ByeByte"])
    await _name_history(actions, "repo:surveyrename", ["redmonth", "ballgem"])
    await _stub_project(actions, "repo:surveysingle", "onlyone")  # not a variant at all

    out = await find_case_variant_projects(actions.pool)

    proven = {e["project"] for e in out["proven_case_variant"]}
    genuine = {e["project"] for e in out["genuine_contradiction"]}
    assert "repo:surveycase" in proven
    assert "repo:surveytrans" in genuine and "repo:surveyrename" in genuine
    assert "repo:surveysingle" not in proven and "repo:surveysingle" not in genuine


async def test_find_case_variant_projects_ignores_retired_projects(
    actions: Actions,
) -> None:
    await _name_history(actions, "repo:surveydead", ["deadname", "DeadName"])
    await actions.pool.execute("UPDATE objects SET status='retired' WHERE canonical=$1",
                               "repo:surveydead")

    out = await find_case_variant_projects(actions.pool)

    all_labels = {e["project"] for e in out["proven_case_variant"]} | \
        {e["project"] for e in out["genuine_contradiction"]}
    assert "repo:surveydead" not in all_labels


# --- reconcile_project_fold (#127, P0 — the fold repair path) ----------------------------

async def test_reconcile_repairs_an_orphaned_edge_from_a_partial_fold(
    actions: Actions,
) -> None:
    """The exact bytebye/ByeByte shape: an OLD-style merge (a raw merge_objects call
    with no estate-move at all — the pre-fold_project shape) leaves a live governs edge
    stranded on the now-merged dupe. reconcile repairs it without re-performing the
    merge."""
    await _stub_project(actions, "repo:orphandupe", "orphandupe")
    await _stub_project(actions, "repo:orphaninto", "orphaninto")
    dupe_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:orphandupe'")
    into_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:orphaninto'")
    holder = await actions.create_or_find_object("Agent", "agent:0rpha001", "test")
    await actions.create_link(holder, dupe_oid, "governs", "test", NOW, 0.9)
    # simulate the OLD, estate-blind merge path directly — no fold_project involved
    await actions.merge_objects(into_oid, dupe_oid, justification="old-style merge",
                                actor="agent:test")
    events_before = await actions.pool.fetchval(
        "SELECT count(*) FROM object_events WHERE event_type='merge'")

    out = await reconcile_project_fold(actions, dupe="orphandupe", into="orphaninto",
                                       actor="agent:reconciler")

    assert out["reconciled"] == "repo:orphandupe" and out["into"] == "repo:orphaninto"
    assert out["edges_moved"] == {"governs": 1}
    live = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='governs' "
        "AND (valid_until IS NULL OR valid_until > now())", holder, into_oid)
    assert live == 1
    dangling = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='governs' "
        "AND (valid_until IS NULL OR valid_until > now())", holder, dupe_oid)
    assert dangling is None
    # never re-performed the merge
    events_after = await actions.pool.fetchval(
        "SELECT count(*) FROM object_events WHERE event_type='merge'")
    assert events_after == events_before
    status = await actions.pool.fetchval("SELECT status FROM objects WHERE id=$1", dupe_oid)
    assert status == "merged"  # unchanged — still exactly one merge, ever


async def test_reconcile_is_a_true_noop_on_a_healthy_fold(actions: Actions) -> None:
    """NEGATIVE CONTROL, by construction: a fold_project run that already moved
    everything must come out UNCHANGED when reconcile runs on it — a repair that
    touches a clean fold is worse than the bug it exists to fix."""
    await _stub_project(actions, "repo:cleandupe", "cleandupe")
    await _stub_project(actions, "repo:cleaninto", "cleaninto")
    holder = await actions.create_or_find_object("Agent", "agent:c1ean001", "test")
    dupe_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:cleandupe'")
    await actions.create_link(holder, dupe_oid, "governs", "test", NOW, 0.9)
    fold_out = await fold_project(actions, dupe="cleandupe", into="cleaninto",
                                  evidence="same project", actor="agent:test")
    assert fold_out["edges_moved"] == {"governs": 1}

    out = await reconcile_project_fold(actions, dupe="cleandupe", into="cleaninto",
                                       actor="agent:reconciler")

    assert out["edges_moved"] == {}
    assert out["mounts_moved"] == 0


async def test_reconcile_refuses_a_still_active_dupe(actions: Actions) -> None:
    """REFUSAL CONTROL: reconcile must never become a side door into performing a
    merge — an active (never-folded) dupe is fold_project's job, not this one's."""
    await _stub_project(actions, "repo:stillactive", "stillactive")
    await _stub_project(actions, "repo:activeinto", "activeinto")

    out = await reconcile_project_fold(actions, dupe="stillactive", into="activeinto",
                                       actor="agent:test")

    assert "not merged" in out["error"] and "fold_project" in out["error"]
    status = await actions.pool.fetchval(
        "SELECT status FROM objects WHERE canonical='repo:stillactive'")
    assert status == "active"


async def test_reconcile_refuses_to_redirect_a_merge(actions: Actions) -> None:
    """A dupe already merged into A is not this pair's business if the caller names B —
    reconcile never guesses or redirects which merge a repair applies to."""
    await _stub_project(actions, "repo:redirdupe", "redirdupe")
    await _stub_project(actions, "repo:realtarget", "realtarget")
    await _stub_project(actions, "repo:wrongtarget", "wrongtarget")
    await fold_project(actions, dupe="redirdupe", into="realtarget",
                       evidence="x", actor="agent:test")

    out = await reconcile_project_fold(actions, dupe="redirdupe", into="wrongtarget",
                                       actor="agent:test")

    assert "not" in out["error"] and "repo:realtarget" in out["error"]


async def test_reconcile_refuses_unknown_and_ambiguous_refs(actions: Actions) -> None:
    await _stub_project(actions, "repo:realdupe2", "realdupe2")
    await _stub_project(actions, "repo:realinto2", "realinto2")
    await fold_project(actions, dupe="realdupe2", into="realinto2",
                       evidence="x", actor="agent:test")

    missing = await reconcile_project_fold(actions, dupe="realdupe2", into="nope-at-all",
                                           actor="agent:test")
    assert "no such SoftwareProject" in missing["error"]

    oid2 = await actions.create_or_find_object("SoftwareProject", "repo:realinto2-twin",
                                                "test")
    await actions.assert_property(oid2, "name", "realinto2", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    ambiguous = await reconcile_project_fold(actions, dupe="realdupe2", into="realinto2",
                                             actor="agent:test")
    assert "ambiguous" in ambiguous["error"]


# ═══ normalize_project_casing (operator ruling d02f2cdd, thread 3ed5b3d2) — the
# twin-collapse composition: fold_project + correct_pin_value, atomic-or-refused. Never
# executed against a real specimen (RAMstein/ramstein, bytebye/byebyte) — every test here
# builds fixtures and proves the verb on those, exactly as instructed.

def _write_pin(tmp_path_factory_dir, project_value: str) -> str:
    """A real `.osiris` file on disk under a fresh directory, project= as its one key —
    the shape `_peek_pin_value`/`correct_pin_value` both read."""
    d = tmp_path_factory_dir
    d.mkdir(parents=True, exist_ok=True)
    (d / ".osiris").write_text(f'project = "{project_value}"\n')
    return str(d)


async def test_normalize_project_casing_folds_and_corrects_the_pin(
    actions: Actions, tmp_path,
) -> None:
    await _stub_project(actions, "repo:RAMstein", "RAMstein")
    await _stub_project(actions, "repo:ramstein", "ramstein")
    dupe_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='repo:RAMstein'")
    agent = await actions.create_or_find_object("Agent", "agent:normcase1", "test")
    await actions.create_link(agent, dupe_id, "works_in", "test", NOW, 0.9)

    seat_path = _write_pin(tmp_path / "till", "RAMstein")
    out = await normalize_project_casing(
        actions, wrong_case="RAMstein", correct_case="ramstein",
        evidence="operator confirmed: same repo, wrong case", actor="agent:test",
        seat_pin_paths=(seat_path,))

    assert out["folded"] == "repo:RAMstein" and out["into"] == "repo:ramstein"
    assert out["edges_moved"] == {"works_in": 1}
    assert len(out["pins_written"]) == 1
    assert out["pins_written"][0]["path"] == str(Path(seat_path) / ".osiris")
    assert out["pins_written"][0]["written"] is True
    assert out["pins_written"][0]["old_value"] == "RAMstein"
    assert out["pins_written"][0]["new_value"] == "ramstein"
    assert out["pins_already_correct"] == []
    assert "pin_write_failed" not in out

    row = await actions.pool.fetchrow("SELECT status FROM objects WHERE canonical='repo:RAMstein'")
    assert row["status"] == "merged"
    pin_text = (Path(seat_path) / ".osiris").read_text()
    assert 'project = "ramstein"' in pin_text
    live_edge = await actions.pool.fetchval(
        "SELECT 1 FROM links l JOIN objects into_o ON into_o.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='works_in' AND into_o.canonical='repo:ramstein' "
        "AND (l.valid_until IS NULL OR l.valid_until > now())", agent)
    assert live_edge == 1


async def test_normalize_project_casing_skips_an_already_correct_pin(
    actions: Actions, tmp_path,
) -> None:
    await _stub_project(actions, "repo:RAMstein2", "RAMstein2")
    await _stub_project(actions, "repo:ramstein2", "ramstein2")
    seat_path = _write_pin(tmp_path / "already", "ramstein2")
    out = await normalize_project_casing(
        actions, wrong_case="RAMstein2", correct_case="ramstein2",
        evidence="op confirmed", actor="agent:test", seat_pin_paths=(seat_path,))
    assert out["pins_already_correct"] == [seat_path]
    assert out["pins_written"] == []


async def test_normalize_project_casing_works_with_no_pins_named(actions: Actions) -> None:
    await _stub_project(actions, "repo:nopindupe", "nopindupe")
    await _stub_project(actions, "repo:nopininto", "nopininto")
    out = await normalize_project_casing(
        actions, wrong_case="nopindupe", correct_case="nopininto",
        evidence="op confirmed", actor="agent:test")
    assert out["folded"] == "repo:nopindupe"
    assert out["pins_written"] == [] and out["pins_already_correct"] == []


async def test_normalize_project_casing_refuses_wholesale_on_a_missing_pin_file(
    actions: Actions, tmp_path,
) -> None:
    """A partial normalization is worse than none — the whole operation refuses, and
    NOTHING is written, not even the graph side."""
    await _stub_project(actions, "repo:RAMstein3", "RAMstein3")
    await _stub_project(actions, "repo:ramstein3", "ramstein3")
    missing_path = str(tmp_path / "does-not-exist")
    out = await normalize_project_casing(
        actions, wrong_case="RAMstein3", correct_case="ramstein3",
        evidence="op confirmed", actor="agent:test", seat_pin_paths=(missing_path,))
    assert "pin_failures" in out
    assert out["pin_failures"][0]["path"] == missing_path
    row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical='repo:RAMstein3'")
    assert row["status"] == "active", "the graph side must NOT have written anything"


async def test_normalize_project_casing_refuses_on_invalid_toml(
    actions: Actions, tmp_path,
) -> None:
    await _stub_project(actions, "repo:RAMstein4", "RAMstein4")
    await _stub_project(actions, "repo:ramstein4", "ramstein4")
    bad = tmp_path / "badtoml"
    bad.mkdir()
    (bad / ".osiris").write_text("this is not valid toml {{{")
    out = await normalize_project_casing(
        actions, wrong_case="RAMstein4", correct_case="ramstein4",
        evidence="op confirmed", actor="agent:test", seat_pin_paths=(str(bad),))
    assert "pin_failures" in out
    assert "not valid TOML" in out["pin_failures"][0]["error"]
    row = await actions.pool.fetchrow(
        "SELECT status FROM objects WHERE canonical='repo:RAMstein4'")
    assert row["status"] == "active"


async def test_normalize_project_casing_refuses_on_a_pin_missing_the_key(
    actions: Actions, tmp_path,
) -> None:
    await _stub_project(actions, "repo:RAMstein5", "RAMstein5")
    await _stub_project(actions, "repo:ramstein5", "ramstein5")
    d = tmp_path / "nokey"
    d.mkdir()
    (d / ".osiris").write_text('handle = "till"\n')
    out = await normalize_project_casing(
        actions, wrong_case="RAMstein5", correct_case="ramstein5",
        evidence="op confirmed", actor="agent:test", seat_pin_paths=(str(d),))
    assert "pin_failures" in out
    assert "is not declared" in out["pin_failures"][0]["error"]


async def test_normalize_project_casing_refuses_blank_evidence(actions: Actions) -> None:
    await _stub_project(actions, "repo:be1", "be1")
    await _stub_project(actions, "repo:be2", "be2")
    out = await normalize_project_casing(
        actions, wrong_case="be1", correct_case="be2", evidence="", actor="agent:test")
    assert "evidence is required" in out["error"]


async def test_normalize_project_casing_refuses_same_label(actions: Actions) -> None:
    await _stub_project(actions, "repo:same1", "same1")
    out = await normalize_project_casing(
        actions, wrong_case="same1", correct_case="same1", evidence="x", actor="agent:test")
    assert "nothing to normalize" in out["error"]


async def test_normalize_project_casing_refuses_an_unknown_side(actions: Actions) -> None:
    await _stub_project(actions, "repo:known1", "known1")
    out = await normalize_project_casing(
        actions, wrong_case="known1", correct_case="ghost-project", evidence="x",
        actor="agent:test")
    assert "unknown SoftwareProject" in out["error"]
    assert "ghost-project" in out["error"]


async def test_normalize_project_casing_refuses_when_dupe_already_merged(
    actions: Actions,
) -> None:
    await _stub_project(actions, "repo:am1", "am1")
    await _stub_project(actions, "repo:am2", "am2")
    await _stub_project(actions, "repo:am3", "am3")
    await fold_project(actions, dupe="am1", into="am2", evidence="x", actor="agent:test")
    out = await normalize_project_casing(
        actions, wrong_case="am1", correct_case="am3", evidence="x", actor="agent:test")
    assert "already folded" in out["error"]


async def test_normalize_project_casing_refuses_a_genuine_cross_object_contradiction(
    actions: Actions,
) -> None:
    """Reuses fold_project's own guard verbatim — must never drift from what a bare
    fold_project call would decide."""
    await _stub_project(actions, "repo:ncc1", "ncc1")
    await _stub_project(actions, "repo:ncc2", "ncc2")
    ncc1_id = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:ncc1'")
    ncc2_id = await actions.pool.fetchval("SELECT id FROM objects WHERE canonical='repo:ncc2'")
    await actions.assert_property(ncc1_id, "language", "python", "agent:alice", NOW, 0.9)
    await actions.assert_property(ncc2_id, "language", "go", "agent:bob", NOW, 0.9)
    out = await normalize_project_casing(
        actions, wrong_case="ncc1", correct_case="ncc2", evidence="x", actor="agent:test")
    assert "contradicting values" in out["error"] and out["contradicted_on"] == ["language"]
    row = await actions.pool.fetchrow("SELECT status FROM objects WHERE canonical='repo:ncc1'")
    assert row["status"] == "active"


async def test_normalize_project_casing_multiple_pins_mixed_state(
    actions: Actions, tmp_path,
) -> None:
    await _stub_project(actions, "repo:MultiCase", "MultiCase")
    await _stub_project(actions, "repo:multicase", "multicase")
    already = _write_pin(tmp_path / "already2", "multicase")
    needs_write = _write_pin(tmp_path / "needs2", "MultiCase")
    out = await normalize_project_casing(
        actions, wrong_case="MultiCase", correct_case="multicase", evidence="op confirmed",
        actor="agent:test", seat_pin_paths=(already, needs_write))
    assert out["pins_already_correct"] == [already]
    assert len(out["pins_written"]) == 1
    assert out["pins_written"][0]["path"] == str(Path(needs_write) / ".osiris")
