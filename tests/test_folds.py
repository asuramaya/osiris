"""AGENT FOLDS — the reconciliation primitive (thread b975851b, operator directive
2026-07-16: append-only merging so the fleet census deflates without one DELETE).

The kernel merge (Actions.merge_objects) is already covered by the entity tests; these
witness the AGENT half: the estate follows the fold, provenance survives, the census
deflates, and every refusal is loud.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.folds import (
    canonical_agent,
    fold_agent,
    living_head,
    reconcile_agent_fold,
    unfold_agent,
    wakeable_identity,
)
from src.orchestrator.mailbox import send_message, unread_count
from src.orchestrator.mounts import save_mount


async def _mk_agent(actions: Actions, label: str, project: str = "foldhouse") -> None:
    a = await actions.create_or_find_object("Agent", label, label)
    await actions.assert_property(a, "project", project, label, datetime.now(UTC), 0.9,
                                  evidence_class="self_declared")


async def test_fold_moves_the_estate_and_deflates_the_census(actions: Actions) -> None:
    p = actions.pool
    await _mk_agent(actions, "agent:f01dbeef")            # the clone (a tab-alias mint)
    await _mk_agent(actions, "agent:0c11ec7a")            # the real lineage, base...
    await _mk_agent(actions, "agent:0c11ec7a-ii")         # ...whose HEAD is generation ii
    await save_mount(p, job_dir="/jobs/0c11ec7a", agent_id="agent:0c11ec7a-ii",
                     project="foldhouse", cwd="/w/foldhouse", model=None, session_key=None)
    await save_mount(p, job_dir="/jobs/f01dbeef", agent_id="agent:f01dbeef",
                     project="foldhouse", cwd="/w/foldhouse", model=None, session_key=None)
    await send_message(p, from_agent="agent:sender", from_project="osiris",
                       to_agent="agent:f01dbeef", body="answers you never got")
    tid = await actions.create_or_find_object("Thread", "thread:f01d0001", "agent:f01dbeef")
    now = datetime.now(UTC)
    await actions.assert_property(tid, "owner", "agent:f01dbeef", "agent:f01dbeef",
                                  now, 0.9, evidence_class="self_declared")
    before = await p.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' AND status='active'")

    out = await fold_agent(actions, dupe="agent:f01dbeef", into="agent:0c11ec7a",
                           evidence="census: tmp-only jobs dir, no transcript, co-timed "
                                    "with the living session at the same cwd",
                           actor="operator")

    assert out["living_head"] == "agent:0c11ec7a-ii"       # estate lands on the HEAD
    assert out["mail_readdressed"] == 1
    assert out["mount_rows_repointed"] == 1
    assert out["threads_reowned"] == 1
    # the projection: folded label leaves the census; canonical resolves at read
    after = await p.fetchval(
        "SELECT count(*) FROM objects WHERE type='Agent' AND status='active'")
    assert after == before - 1
    assert await canonical_agent(p, "agent:f01dbeef") == "agent:0c11ec7a"
    # the mail reached the living head, readable by IT
    assert await unread_count(p, "foldhouse", reader_agent="agent:0c11ec7a-ii") >= 1
    # authorship SURVIVES: the thread's creation is still the dupe's act on the record
    src = await p.fetchval(
        "SELECT a.source_id FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='thread:f01d0001' AND a.name='owner' "
        "ORDER BY a.observed_at LIMIT 1")
    assert src == "agent:f01dbeef"


async def test_fold_survives_a_crash_between_estate_move_and_merge(actions: Actions) -> None:
    """Task #59's own precondition fix: the estate now moves BEFORE Actions.merge_objects,
    so a process death in that window leaves dupe.status=='active' — a retry re-enters
    fold_agent and simply CONTINUES rather than hitting the "already folded — nothing to
    do" refusal with the estate stranded forever (the old order's failure, the exact #127
    class of bug). Simulated by performing the estate move by hand (mirroring fold_agent's
    own first half) and then calling the real verb — proving both that it does not refuse
    and that re-moving already-moved estate is a true no-op, not a duplicate."""
    p = actions.pool
    await _mk_agent(actions, "agent:crash001")
    await _mk_agent(actions, "agent:crash002")
    await save_mount(p, job_dir="/jobs/crash001", agent_id="agent:crash001",
                     project="foldhouse", cwd="/w/foldhouse", model=None, session_key=None)
    await send_message(p, from_agent="agent:sender", from_project="osiris",
                       to_agent="agent:crash001", body="mail sent before the crash")
    tid = await actions.create_or_find_object("Thread", "thread:crash0001", "agent:crash001")
    now = datetime.now(UTC)
    await actions.assert_property(tid, "owner", "agent:crash001", "agent:crash001",
                                  now, 0.9, evidence_class="self_declared")

    # THE SIMULATED CRASH: exactly fold_agent's own estate-move half, run by hand, with
    # merge_objects never called — the state a real crash in that window would leave.
    await p.execute("UPDATE fleet_messages SET to_agent=$1 WHERE to_agent=$2 "
                    "AND read_at IS NULL", "agent:crash002", "agent:crash001")
    await p.execute("UPDATE agent_mounts SET agent_id=$1 WHERE agent_id=$2",
                    "agent:crash002", "agent:crash001")
    await actions.assert_property(tid, "owner", "agent:crash002", "agent:crash001",
                                  now, 0.9, evidence_class="self_declared")
    still_active = await p.fetchval(
        "SELECT status FROM objects WHERE canonical='agent:crash001'")
    assert still_active == "active"          # confirms the crash left it retryable, not refused

    out = await fold_agent(actions, dupe="agent:crash001", into="agent:crash002",
                           evidence="retry after a simulated mid-fold crash",
                           actor="operator")

    assert "error" not in out                 # the retry completed, it did not refuse
    assert out["mail_readdressed"] == 0        # already moved by the "crash" — a true no-op
    assert out["mount_rows_repointed"] == 0
    assert out["threads_reowned"] == 0
    assert await canonical_agent(p, "agent:crash001") == "agent:crash002"
    assert await unread_count(p, "foldhouse", reader_agent="agent:crash002") >= 1


async def test_send_forwards_a_folded_address_and_confesses(actions: Actions) -> None:
    p = actions.pool
    await _mk_agent(actions, "agent:dead1111")
    await _mk_agent(actions, "agent:beef2222")
    await save_mount(p, job_dir="/jobs/beef2222", agent_id="agent:beef2222",
                     project="foldhouse", cwd="/w/f2", model=None, session_key=None)
    await fold_agent(actions, dupe="agent:dead1111", into="agent:beef2222",
                     evidence="test evidence", actor="operator")

    out = await send_message(p, from_agent="agent:sender", from_project="osiris",
                             to_agent="agent:dead1111", body="stale address book")

    assert out["to_agent"] == "agent:beef2222"      # the forwarding order executed
    assert out["folded_from"] == "agent:dead1111"   # ...and confessed in the receipt


async def test_fold_refusals_are_loud_and_write_nothing(actions: Actions) -> None:
    await _mk_agent(actions, "agent:aaaa5555")
    await _mk_agent(actions, "agent:bbbb6666")

    out = await fold_agent(actions, dupe="agent:aaaa5555", into="agent:bbbb6666",
                           evidence="   ", actor="operator")
    assert "auto-merge" in out["error"]              # evidence is mandatory
    out = await fold_agent(actions, dupe="agent:aaaa5555", into="agent:aaaa5555-ii",
                           evidence="x", actor="operator")
    assert "succession" in out["error"]              # generations never fold
    out = await fold_agent(actions, dupe="agent:nobody99", into="agent:bbbb6666",
                           evidence="x", actor="operator")
    assert "unknown" in out["error"]
    # nothing was written by any refusal
    st = await actions.pool.fetchval(
        "SELECT status FROM objects WHERE canonical='agent:aaaa5555'")
    assert st == "active"


async def test_fold_agent_refuses_a_non_operator_actor(actions: Actions) -> None:
    """NEGATIVE CONTROL for the operator-actor gate (census a5e53ed8/3f97f9c7, fixed
    2026-08-02): this docstring claimed "operator's word or an approved merge_candidate"
    for weeks while ANY mounted caller could fold any two agents — confirmed against
    pre-fix code (any non-empty evidence + valid, unheld, unmerged labels used to
    succeed for actor="agent:some-mind"). Now it refuses, names the actor, and writes
    nothing."""
    await _mk_agent(actions, "agent:gate0001")
    await _mk_agent(actions, "agent:gate0002")

    out = await fold_agent(actions, dupe="agent:gate0001", into="agent:gate0002",
                           evidence="a real reason, correctly formed",
                           actor="agent:some-mind")

    assert "not authorized" in out["error"]
    assert "agent:some-mind" in out["error"]
    st = await actions.pool.fetchval(
        "SELECT status FROM objects WHERE canonical='agent:gate0001'")
    assert st == "active"  # refused, nothing written


async def test_fold_agent_allows_the_sanctioned_cron_actor(actions: Actions) -> None:
    """The scheduled reaper's own name is trusted — it is ALREADY gated separately by
    osiris_fleet_reconcile_enabled, a distinct operator signature (see
    test_fleet_reconcile.py's own positive control for the end-to-end path)."""
    from src.orchestrator.folds import _SANCTIONED_AUTO_FOLD_ACTOR

    await _mk_agent(actions, "agent:gate0003")
    await _mk_agent(actions, "agent:gate0004")

    out = await fold_agent(actions, dupe="agent:gate0003", into="agent:gate0004",
                           evidence="the scheduled reaper's own sweep",
                           actor=_SANCTIONED_AUTO_FOLD_ACTOR)

    assert "error" not in out
    assert out["folded"] == "agent:gate0003"


async def test_resolve_fold_candidate_merged_inherits_the_gate_rejected_does_not(
    actions: Actions, tmp_path,
) -> None:
    """Per-branch honesty (Thoth's decision rule, msg 3273): 'merged' carries fold_agent's
    own blast radius and its gate; 'rejected' judges two things are NOT the same mind,
    never merges an identity, and stays open to any mounted caller."""
    from src.orchestrator.folds import find_agent_fold_candidates, resolve_fold_candidate

    p = actions.pool
    root = tmp_path / "projects"
    jobs = tmp_path / "jobs"
    slug = root / "-w-gatecheck-repo"
    slug.mkdir(parents=True)
    (slug / "rea1gate-full-session.jsonl").write_text("{}\n")
    await _mk_agent(actions, "agent:a11agate")
    await _mk_agent(actions, "agent:rea1gate")
    await save_mount(p, job_dir=str(jobs / "a11agate"), agent_id="agent:a11agate",
                     project="foldhouse", cwd="/w/gatecheck-repo", model=None,
                     session_key="whisper:a11agate")
    await save_mount(p, job_dir=str(jobs / "rea1gate"), agent_id="agent:rea1gate",
                     project="foldhouse", cwd="/w/gatecheck-repo", model=None,
                     session_key="sid:conn")
    out = await find_agent_fold_candidates(p, projects_root=root, jobs_home=jobs)
    mine = [c for c in out["pending"] if c["dupe"] == "agent:a11agate"]
    assert mine

    # a mounted agent judging its OWN proposal, unauthorized — the exact self-approval
    # the constitution forbids, refused rather than silently permitted
    verdict = await resolve_fold_candidate(actions, candidate_id=mine[0]["id"],
                                           decision="merged", actor="agent:self-approver")
    assert "not authorized" in verdict["error"]
    st = await p.fetchval("SELECT status FROM objects WHERE canonical='agent:a11agate'")
    assert st == "active"  # refused, nothing written, candidate stays unresolved

    # the SAME non-operator actor may still reject it — no gate, no blast radius
    verdict2 = await resolve_fold_candidate(actions, candidate_id=mine[0]["id"],
                                            decision="rejected", actor="agent:self-approver")
    assert verdict2["resolved"] == "rejected"


async def test_fold_refuses_a_seated_dupe(actions: Actions) -> None:
    from src.orchestrator.seats import attach_session, ensure_seat, mint_attach_token

    p = actions.pool
    await _mk_agent(actions, "agent:cccc7777", project="seathouse")
    await _mk_agent(actions, "agent:dddd8888", project="seathouse")
    await save_mount(p, job_dir="/jobs/cccc7777", agent_id="agent:cccc7777",
                     project="seathouse", cwd="/w/s1", model=None, session_key=None)
    seat = await ensure_seat(actions, house="seathouse", handle="Folded",
                             anchor_cwd="/w/s1", source="test")
    token = await mint_attach_token(p, seat_id=seat["seat_id"])
    await attach_session(actions, seat_id=seat["seat_id"], token=token,
                         job_dir="/jobs/cccc7777", agent_id="agent:cccc7777")

    out = await fold_agent(actions, dupe="agent:cccc7777", into="agent:dddd8888",
                           evidence="x", actor="operator")

    assert "holds" in out["error"] and "seat" in out["error"].lower()


async def test_fold_of_an_already_folded_label_points_home(actions: Actions) -> None:
    await _mk_agent(actions, "agent:eeee9999")
    await _mk_agent(actions, "agent:ffff0000")
    await _mk_agent(actions, "agent:abab1212")
    await fold_agent(actions, dupe="agent:eeee9999", into="agent:ffff0000",
                     evidence="x", actor="operator")

    out = await fold_agent(actions, dupe="agent:eeee9999", into="agent:abab1212",
                           evidence="x", actor="operator")
    assert "already folded" in out["error"] and "agent:ffff0000" in out["error"]
    out = await fold_agent(actions, dupe="agent:abab1212", into="agent:eeee9999",
                           evidence="x", actor="operator")
    assert "living label" in out["error"]            # fold into the canonical, not a ghost


async def test_living_head_reads_the_registry(actions: Actions) -> None:
    p = actions.pool
    await save_mount(p, job_dir="/jobs/12ab34cd", agent_id="agent:12ab34cd-iii",
                     project="headhouse", cwd="/w/h", model=None, session_key=None)
    assert await living_head(p, "agent:12ab34cd") == "agent:12ab34cd-iii"
    assert await living_head(p, "agent:12ab34cd-ii") == "agent:12ab34cd-iii"
    assert await living_head(p, "agent:never5een") == "agent:never5een"


async def test_archaeologist_proposes_a_view_alias(actions: Actions, tmp_path) -> None:
    """The finder pairs a bodiless anonymous mount (no transcript, no daemon receipt)
    with the co-resident session that HAS a body — proposal only, score .9."""
    from src.orchestrator.folds import find_agent_fold_candidates, resolve_fold_candidate

    p = actions.pool
    root = tmp_path / "projects"
    jobs = tmp_path / "jobs"
    slug = root / "-w-alias-repo"
    slug.mkdir(parents=True)
    (slug / "rea1baaa-full-session.jsonl").write_text("{}\n")   # the REAL session's body
    await _mk_agent(actions, "agent:a11a5000")                  # the doorbell ring
    await _mk_agent(actions, "agent:rea1baaa")                  # the living original
    await save_mount(p, job_dir=str(jobs / "a11a5000"), agent_id="agent:a11a5000",
                     project="aliashouse", cwd="/w/alias-repo", model=None,
                     session_key="whisper:a11a5000")
    await save_mount(p, job_dir=str(jobs / "rea1baaa"), agent_id="agent:rea1baaa",
                     project="aliashouse", cwd="/w/alias-repo", model=None,
                     session_key="sid:conn")

    out = await find_agent_fold_candidates(p, projects_root=root, jobs_home=jobs)

    assert out["proposed"]["view-alias"] == 1
    mine = [c for c in out["pending"] if c["dupe"] == "agent:a11a5000"]
    assert mine and mine[0]["into_label"] == "agent:rea1baaa"
    # judging it MERGED executes the estate-carrying fold and stamps the row — inherits
    # fold_agent's own operator-actor gate, so the judge must be the operator too
    verdict = await resolve_fold_candidate(actions, candidate_id=mine[0]["id"],
                                           decision="merged", actor="operator")
    assert verdict["resolved"] == "merged" and verdict["folded"] == "agent:a11a5000"
    st = await p.fetchval("SELECT status FROM objects WHERE canonical='agent:a11a5000'")
    assert st == "merged"
    # idempotent: a second sweep proposes nothing for the folded pair
    again = await find_agent_fold_candidates(p, projects_root=root, jobs_home=jobs)
    assert not [c for c in again["pending"] if c["dupe"] == "agent:a11a5000"]


async def test_archaeologist_rejection_is_remembered(actions: Actions, tmp_path) -> None:
    from src.orchestrator.folds import find_agent_fold_candidates, resolve_fold_candidate

    p = actions.pool
    root = tmp_path / "projects"
    jobs = tmp_path / "jobs"
    slug = root / "-w-rej-repo"
    slug.mkdir(parents=True)
    (slug / "0riginaa-full.jsonl").write_text("{}\n")
    await _mk_agent(actions, "agent:0eadbea7")
    await _mk_agent(actions, "agent:0riginaa")
    await save_mount(p, job_dir=str(jobs / "0eadbea7"), agent_id="agent:0eadbea7",
                     project="rejhouse", cwd="/w/rej-repo", model=None,
                     session_key="whisper:0eadbea7")
    await save_mount(p, job_dir=str(jobs / "0riginaa"), agent_id="agent:0riginaa",
                     project="rejhouse", cwd="/w/rej-repo", model=None,
                     session_key="sid:conn2")
    out = await find_agent_fold_candidates(p, projects_root=root, jobs_home=jobs)
    mine = [c for c in out["pending"] if c["dupe"] == "agent:0eadbea7"]
    assert mine

    # 'rejected' is open to any mounted caller, deliberately (census a5e53ed8) — a
    # rejection judges two things are NOT the same mind, never merges an identity
    verdict = await resolve_fold_candidate(actions, candidate_id=mine[0]["id"],
                                           decision="rejected", actor="agent:any-mind")
    assert verdict["resolved"] == "rejected"
    again = await find_agent_fold_candidates(p, projects_root=root, jobs_home=jobs)
    assert not [c for c in again["pending"] if c["dupe"] == "agent:0eadbea7"]
    st = await p.fetchval("SELECT status FROM objects WHERE canonical='agent:0eadbea7'")
    assert st == "active"                       # a rejection folds nothing


async def test_archaeologist_flags_a_restart_mint(actions: Actions, tmp_path) -> None:
    """An anonymous agent WITH a body, mounted in a NAMED lineage's own home, is the
    restart-mint class — proposed at the lower score, behind the aliases in the tray."""
    from datetime import UTC, datetime

    from src.orchestrator.folds import find_agent_fold_candidates

    p = actions.pool
    root = tmp_path / "projects"
    jobs = tmp_path / "jobs"
    slug = root / "-w-mint-repo"
    slug.mkdir(parents=True)
    (slug / "annon111-full.jsonl").write_text("{}\n")   # the anon HAS a body (not an alias)
    await _mk_agent(actions, "agent:annon111")
    await _mk_agent(actions, "agent:5eated00")
    named = await actions.create_or_find_object("Agent", "agent:5eated00", "agent:5eated00")
    await actions.assert_property(named, "handle", "Minty", "agent:5eated00",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    await save_mount(p, job_dir=str(jobs / "annon111"), agent_id="agent:annon111",
                     project="minthouse", cwd="/w/mint-repo", model=None,
                     session_key="whisper:annon111")
    await save_mount(p, job_dir=str(jobs / "5eated00"), agent_id="agent:5eated00",
                     project="minthouse", cwd="/w/mint-repo", model=None,
                     session_key="whisper:5eated00")

    out = await find_agent_fold_candidates(p, projects_root=root, jobs_home=jobs)

    mine = [c for c in out["pending"] if c["dupe"] == "agent:annon111"]
    assert mine and mine[0]["into_label"] == "agent:5eated00"
    assert float(mine[0]["score"]) < 0.9        # weaker class ranks behind aliases


async def test_archaeologist_charter_match_finds_a_migrated_seat(
    actions: Actions, tmp_path,
) -> None:
    """THE ARCHAEOLOGIST'S BLIND SPOT, cured (thread 3430c32b): the office migrations
    moved every seat's mount row home to ~/.osiris/seats/<handle>, so an anon stranded
    in the seat's OLD project room matches nothing by cwd. The room's seat is a GRAPH
    fact — a live works_in edge to repo:<project> — and the charter does not move
    house. Single seat on the charter → the single-seat presumption holds at .75."""
    from src.orchestrator.folds import find_agent_fold_candidates

    p = actions.pool
    root = tmp_path / "projects"
    jobs = tmp_path / "jobs"
    slug = root / "-w-charter-repo"
    slug.mkdir(parents=True)
    (slug / "a10ne111-full.jsonl").write_text("{}\n")   # the anon has a body
    await _mk_agent(actions, "agent:a10ne111", project="charterhouse")
    seat = await actions.create_or_find_object("Agent", "agent:c4a97e01",
                                               "agent:c4a97e01")
    await actions.assert_property(seat, "handle", "Chartreuse", "agent:c4a97e01",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    room = await actions.create_or_find_object("SoftwareProject", "repo:charterhouse",
                                               "repo:charterhouse")
    await actions.create_link(seat, room, "works_in", "agent:c4a97e01",
                              datetime.now(UTC), 0.9)
    # the seat holds NO mount row anywhere — the ceremony-migrated case, rows died
    await save_mount(p, job_dir=str(jobs / "a10ne111"), agent_id="agent:a10ne111",
                     project="charterhouse", cwd="/w/charter-repo", model=None,
                     session_key="whisper:a10ne111")

    out = await find_agent_fold_candidates(p, projects_root=root, jobs_home=jobs)

    mine = [c for c in out["pending"] if c["dupe"] == "agent:a10ne111"]
    assert mine and mine[0]["into_label"] == "agent:c4a97e01"
    assert mine[0]["class"] == "charter-match"
    assert float(mine[0]["score"]) == 0.75      # the room's only seat


async def test_archaeologist_leaves_a_seatless_room_alone(
    actions: Actions, tmp_path,
) -> None:
    """A room whose charter names NO seat proposes nothing — its anons are the visitor
    class (demotion candidates for the visitor gate), and folding them anywhere would
    be a guess. The archaeologist counts them in `seatless` instead."""
    from src.orchestrator.folds import find_agent_fold_candidates

    p = actions.pool
    root = tmp_path / "projects"
    jobs = tmp_path / "jobs"
    slug = root / "-w-orphan-repo"
    slug.mkdir(parents=True)
    (slug / "05eat111-full.jsonl").write_text("{}\n")
    await _mk_agent(actions, "agent:05eat111", project="orphanage")
    await save_mount(p, job_dir=str(jobs / "05eat111"), agent_id="agent:05eat111",
                     project="orphanage", cwd="/w/orphan-repo", model=None,
                     session_key="whisper:05eat111")

    out = await find_agent_fold_candidates(p, projects_root=root, jobs_home=jobs)

    assert not [c for c in out["pending"] if c["dupe"] == "agent:05eat111"]
    assert out["seatless"].get("orphanage") == 1


async def test_archaeologist_charter_match_prefers_the_declared_governor(
    actions: Actions, tmp_path,
) -> None:
    """When a room's charter names SEVERAL souls (a resident works_in beside a
    supervising governs — the coldspot shape: Aegis lives there, Alfred governs it),
    the anon is presumed the GOVERNOR's (ruling 1db1ff41: declared beats derived — this
    REVERSES the prior resident-wins tie-break), at the multi-seat score: nuanced,
    verify by hand, both names in the signal."""
    from src.orchestrator.folds import find_agent_fold_candidates

    p = actions.pool
    root = tmp_path / "projects"
    jobs = tmp_path / "jobs"
    slug = root / "-w-shared-repo"
    slug.mkdir(parents=True)
    (slug / "e5a12111-full.jsonl").write_text("{}\n")
    await _mk_agent(actions, "agent:e5a12111", project="sharedroom")
    room = await actions.create_or_find_object("SoftwareProject", "repo:sharedroom",
                                               "repo:sharedroom")
    resident = await actions.create_or_find_object("Agent", "agent:4e51den7",
                                                   "agent:4e51den7")
    await actions.assert_property(resident, "handle", "Resi", "agent:4e51den7",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    await actions.create_link(resident, room, "works_in", "agent:4e51den7",
                              datetime.now(UTC), 0.9)
    boss = await actions.create_or_find_object("Agent", "agent:b055a1f4",
                                               "agent:b055a1f4")
    await actions.assert_property(boss, "handle", "Boss", "agent:b055a1f4",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    await actions.create_link(boss, room, "governs", "agent:b055a1f4",
                              datetime.now(UTC), 0.9)
    await save_mount(p, job_dir=str(jobs / "e5a12111"), agent_id="agent:e5a12111",
                     project="sharedroom", cwd="/w/shared-repo", model=None,
                     session_key="whisper:e5a12111")

    out = await find_agent_fold_candidates(p, projects_root=root, jobs_home=jobs)

    mine = [c for c in out["pending"] if c["dupe"] == "agent:e5a12111"]
    assert mine and mine[0]["into_label"] == "agent:b055a1f4"  # the declared governor wins
    assert abs(float(mine[0]["score"]) - 0.55) < 1e-6  # several souls — hand-verify
    assert "agent:4e51den7" in str(mine[0]["signals"])  # the resident is named


async def test_archaeologist_charter_match_reads_a_seat_keyed_governs_edge(
    actions: Actions, tmp_path,
) -> None:
    """RULING 3 (decision 1db1ff41) re-keys governs onto the Seat, not any one Agent
    generation (Imhotep, DM 2415/2416 — caught before it shipped): a governs join
    hard-coded to fo.type='Agent' would silently stop matching every governs link the
    instant that re-key lands, degrading this whole reversal back to works_in-only with
    no crash and no failing test unless something exercises this exact shape. A
    Seat-origin governs edge must resolve through its CURRENT holder and still win."""
    from src.orchestrator.folds import find_agent_fold_candidates
    from src.orchestrator.seats import bind_holder

    p = actions.pool
    root = tmp_path / "projects"
    jobs = tmp_path / "jobs"
    slug = root / "-w-seatgoverned-repo"
    slug.mkdir(parents=True)
    (slug / "5ea7go10-full.jsonl").write_text("{}\n")
    await _mk_agent(actions, "agent:5ea7go10", project="seatgoverned")
    room = await actions.create_or_find_object("SoftwareProject", "repo:seatgoverned",
                                               "repo:seatgoverned")
    resident = await actions.create_or_find_object("Agent", "agent:4e51den8",
                                                   "agent:4e51den8")
    await actions.assert_property(resident, "handle", "Resi2", "agent:4e51den8",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    await actions.create_link(resident, room, "works_in", "agent:4e51den8",
                              datetime.now(UTC), 0.9)
    boss_seat = await actions.create_or_find_object("Seat", "seat:b0551eat",
                                                     "seat:b0551eat")
    await actions.assert_property(boss_seat, "handle", "Boss2", "seat:b0551eat",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    boss_holder = await actions.create_or_find_object("Agent", "agent:b055ho1d",
                                                       "agent:b055ho1d")
    await actions.assert_property(boss_holder, "handle", "Boss2", "agent:b055ho1d",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    await bind_holder(actions, seat_id="seat:b0551eat", agent_id="agent:b055ho1d")
    await actions.create_link(boss_seat, room, "governs", "seat:b0551eat",
                              datetime.now(UTC), 0.9)
    await save_mount(p, job_dir=str(jobs / "5ea7go10"), agent_id="agent:5ea7go10",
                     project="seatgoverned", cwd="/w/seatgoverned-repo", model=None,
                     session_key="whisper:5ea7go10")

    out = await find_agent_fold_candidates(p, projects_root=root, jobs_home=jobs)

    mine = [c for c in out["pending"] if c["dupe"] == "agent:5ea7go10"]
    assert mine and mine[0]["into_label"] == "agent:b055ho1d"  # resolved through the seat
    assert abs(float(mine[0]["score"]) - 0.55) < 1e-6


async def test_unfold_reverses_a_fold_dry_run_writes_nothing(actions: Actions) -> None:
    await _mk_agent(actions, "agent:un1dead0")
    await _mk_agent(actions, "agent:un1live0")
    await fold_agent(actions, dupe="agent:un1dead0", into="agent:un1live0",
                     evidence="census: co-timed sessions, same cwd", actor="operator")

    out = await unfold_agent(actions, dupe="agent:un1dead0",
                             because="wrongful fold — a real second mind",
                             actor="agent:judge")

    assert out["execute"] is False
    assert out["was_merged_into"] == "agent:un1live0"
    assert any(p["op"] == "unmerge_objects" for p in out["plan"])
    st = await actions.pool.fetchval(
        "SELECT status FROM objects WHERE canonical='agent:un1dead0'")
    assert st == "merged"  # dry run never writes


async def test_unfold_executed_restores_the_dupe(actions: Actions) -> None:
    await _mk_agent(actions, "agent:un2dead0")
    await _mk_agent(actions, "agent:un2live0")
    await fold_agent(actions, dupe="agent:un2dead0", into="agent:un2live0",
                     evidence="census: co-timed sessions, same cwd", actor="operator")

    out = await unfold_agent(actions, dupe="agent:un2dead0",
                             because="a real second mind, wrongly folded",
                             actor="agent:judge", execute=True)

    assert out["unmerged"] is True
    row = await actions.pool.fetchrow(
        "SELECT status, merged_into FROM objects WHERE canonical='agent:un2dead0'")
    assert row["status"] == "active" and row["merged_into"] is None
    # the merge event and same_as link stay as witnesses (unmerge_objects' own contract)
    same_as = await actions.pool.fetchval(
        "SELECT 1 FROM links l JOIN objects f ON f.id=l.from_id JOIN objects t ON t.id=l.to_id "
        "WHERE f.canonical='agent:un2dead0' AND t.canonical='agent:un2live0' "
        "AND l.type='same_as'")
    assert same_as == 1


async def test_unfold_refuses_a_never_folded_dupe(actions: Actions) -> None:
    await _mk_agent(actions, "agent:un3free0")
    out = await unfold_agent(actions, dupe="agent:un3free0", because="x", actor="agent:judge")
    assert "not folded" in out["error"]


async def test_unfold_refuses_a_blank_because(actions: Actions) -> None:
    await _mk_agent(actions, "agent:un4dead0")
    await _mk_agent(actions, "agent:un4live0")
    await fold_agent(actions, dupe="agent:un4dead0", into="agent:un4live0",
                     evidence="x", actor="operator")
    out = await unfold_agent(actions, dupe="agent:un4dead0", because="   ",
                             actor="agent:judge")
    assert "because" in out["error"]


async def test_unfold_refuses_an_unknown_dupe(actions: Actions) -> None:
    out = await unfold_agent(actions, dupe="agent:nobody99", because="x", actor="agent:judge")
    assert "unknown" in out["error"]


async def test_unfold_refuses_an_operator_blessed_fold_without_fresh_operator_word(
    actions: Actions,
) -> None:
    await _mk_agent(actions, "agent:un5dead0")
    await _mk_agent(actions, "agent:un5live0")
    await fold_agent(actions, dupe="agent:un5dead0", into="agent:un5live0",
                     evidence="the operator confirmed these are one mind, 2026-07-01",
                     actor="operator")

    out = await unfold_agent(actions, dupe="agent:un5dead0",
                             because="I think this was wrong", actor="agent:judge")
    assert "operator" in out["error"]
    st = await actions.pool.fetchval(
        "SELECT status FROM objects WHERE canonical='agent:un5dead0'")
    assert st == "merged"  # refused, nothing written

    out2 = await unfold_agent(
        actions, dupe="agent:un5dead0",
        because="the operator's fresh word, 2026-07-28: this fold was wrong",
        actor="agent:judge", execute=True)
    assert out2["unmerged"] is True


async def test_unfold_clears_a_cross_lineage_succeeded_by_stitch(actions: Actions) -> None:
    p = actions.pool
    await _mk_agent(actions, "agent:un6dead0")
    await _mk_agent(actions, "agent:un6live0")
    dupe_oid = await p.fetchval("SELECT id FROM objects WHERE canonical='agent:un6dead0'")
    now = datetime.now(UTC)
    # a stitch: dupe's succeeded_by wrongly points into the WINNER's own lineage
    await actions.assert_property(dupe_oid, "succeeded_by", "agent:un6live0-ii",
                                  "agent:bad-heal", now, 0.9, evidence_class="self_declared")
    await fold_agent(actions, dupe="agent:un6dead0", into="agent:un6live0",
                     evidence="x", actor="operator")

    out = await unfold_agent(actions, dupe="agent:un6dead0", because="a real second mind",
                             actor="agent:judge", execute=True)

    assert out["chain_restored"] is True
    sb = await p.fetchval(
        "SELECT value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='agent:un6dead0' AND a.name='succeeded_by' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1")
    assert sb == ""  # the stitch is cleared — dupe reads as its own lineage's tail again


async def test_unfold_never_touches_a_real_same_lineage_successor(actions: Actions) -> None:
    p = actions.pool
    await _mk_agent(actions, "agent:un7dead0")
    await _mk_agent(actions, "agent:un7dead0-ii")   # a REAL successor, same base
    await _mk_agent(actions, "agent:un7live0")
    dupe_oid = await p.fetchval("SELECT id FROM objects WHERE canonical='agent:un7dead0'")
    now = datetime.now(UTC)
    await actions.assert_property(dupe_oid, "succeeded_by", "agent:un7dead0-ii",
                                  "agent:un7dead0-ii", now, 0.6,
                                  evidence_class="direct_observation")
    await fold_agent(actions, dupe="agent:un7dead0", into="agent:un7live0",
                     evidence="x", actor="operator")

    out = await unfold_agent(actions, dupe="agent:un7dead0", because="wrongly folded",
                             actor="agent:judge", execute=True)

    assert out["chain_restored"] is False
    sb = await p.fetchval(
        "SELECT value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='agent:un7dead0' AND a.name='succeeded_by' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1")
    assert sb == "agent:un7dead0-ii"  # real succession is never this verb's business


async def test_unfold_reports_unreturnable_mail_and_restores_reversible_threads(
    actions: Actions,
) -> None:
    p = actions.pool
    await _mk_agent(actions, "agent:un8dead0")
    await _mk_agent(actions, "agent:un8live0")
    # mail sent BEFORE the fold, still unread — lands on the winner at fold time
    await send_message(p, from_agent="agent:sender", from_project="osiris",
                       to_agent="agent:un8dead0", body="a question for the dupe")
    tid = await actions.create_or_find_object("Thread", "thread:un8t0001", "agent:un8dead0")
    now = datetime.now(UTC)
    await actions.assert_property(tid, "owner", "agent:un8dead0", "agent:un8dead0", now, 0.9,
                                  evidence_class="self_declared")
    await fold_agent(actions, dupe="agent:un8dead0", into="agent:un8live0",
                     evidence="x", actor="operator")

    out = await unfold_agent(actions, dupe="agent:un8dead0", because="wrongly folded",
                             actor="agent:judge")  # dry run

    assert len(out["estate_unreturnable"]["mail"]) == 1
    assert any(p2["op"] == "assert_property" and p2["target"] == "thread:un8t0001"
              for p2 in out["plan"])

    out2 = await unfold_agent(actions, dupe="agent:un8dead0", because="wrongly folded",
                              actor="agent:judge", execute=True)
    assert out2["threads_reowned"] == 1
    owner = await p.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical='thread:un8t0001' AND a.name='owner' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1")
    assert owner == "agent:un8dead0"


# ═══ reconcile_agent_fold (#127, the repair path fold_agent never had — mirrors
# reconcile_project_fold's exact design, sharing the SAME _move_agent_estate fold_agent
# itself calls) ═══


async def test_reconcile_agent_fold_repairs_an_orphaned_edge_from_a_partial_fold(
    actions: Actions,
) -> None:
    """An OLD-style merge (a raw merge_objects call with no estate-move at all) leaves
    unread mail stranded on the now-merged dupe. reconcile repairs it without
    re-performing the merge."""
    await _mk_agent(actions, "agent:ra1dead0")
    await _mk_agent(actions, "agent:ra1live0")
    await send_message(actions.pool, from_agent="agent:sender", from_project="osiris",
                       to_agent="agent:ra1dead0", body="stranded by an old-style merge")
    dupe_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='agent:ra1dead0'")
    into_id = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='agent:ra1live0'")
    # simulate the OLD, estate-blind merge path directly — no fold_agent involved
    await actions.merge_objects(into_id, dupe_id, justification="old-style merge",
                                actor="operator")
    events_before = await actions.pool.fetchval(
        "SELECT count(*) FROM object_events WHERE event_type='merge'")

    out = await reconcile_agent_fold(actions, dupe="agent:ra1dead0", into="agent:ra1live0",
                                     actor="operator")

    assert out["reconciled"] == "agent:ra1dead0" and out["into"] == "agent:ra1live0"
    assert out["mail_readdressed"] == 1
    unread = await unread_count(actions.pool, "foldhouse", reader_agent="agent:ra1live0")
    assert unread >= 1
    # never re-performed the merge
    events_after = await actions.pool.fetchval(
        "SELECT count(*) FROM object_events WHERE event_type='merge'")
    assert events_after == events_before
    status = await actions.pool.fetchval("SELECT status FROM objects WHERE id=$1", dupe_id)
    assert status == "merged"  # unchanged — still exactly one merge, ever


async def test_reconcile_agent_fold_is_a_true_noop_on_a_healthy_fold(
    actions: Actions,
) -> None:
    """NEGATIVE CONTROL, by construction: a fold_agent run that already moved everything
    must come out UNCHANGED when reconcile runs on it."""
    await _mk_agent(actions, "agent:ra2dead0")
    await _mk_agent(actions, "agent:ra2live0")
    await send_message(actions.pool, from_agent="agent:sender", from_project="osiris",
                       to_agent="agent:ra2dead0", body="moved by a clean fold")
    fold_out = await fold_agent(actions, dupe="agent:ra2dead0", into="agent:ra2live0",
                                evidence="census match", actor="operator")
    assert fold_out["mail_readdressed"] == 1

    out = await reconcile_agent_fold(actions, dupe="agent:ra2dead0", into="agent:ra2live0",
                                     actor="operator")

    assert out["mail_readdressed"] == 0
    assert out["mount_rows_repointed"] == 0
    assert out["threads_reowned"] == 0


async def test_reconcile_agent_fold_refuses_a_still_active_dupe(actions: Actions) -> None:
    """REFUSAL CONTROL: reconcile must never become a side door into performing a fold —
    an active (never-folded) dupe is fold_agent's job, not this one's."""
    await _mk_agent(actions, "agent:ra3active0")
    await _mk_agent(actions, "agent:ra3into000")

    out = await reconcile_agent_fold(actions, dupe="agent:ra3active0",
                                     into="agent:ra3into000", actor="operator")

    assert "not merged" in out["error"] and "merge" in out["error"]
    status = await actions.pool.fetchval(
        "SELECT status FROM objects WHERE canonical='agent:ra3active0'")
    assert status == "active"


async def test_reconcile_agent_fold_refuses_to_redirect_a_merge(actions: Actions) -> None:
    """A dupe already merged into A is not this pair's business if the caller names B —
    reconcile never guesses or redirects which merge a repair applies to."""
    await _mk_agent(actions, "agent:ra4dupe00")
    await _mk_agent(actions, "agent:ra4real00")
    await _mk_agent(actions, "agent:ra4wrong00")
    await fold_agent(actions, dupe="agent:ra4dupe00", into="agent:ra4real00",
                     evidence="x", actor="operator")

    out = await reconcile_agent_fold(actions, dupe="agent:ra4dupe00",
                                     into="agent:ra4wrong00", actor="operator")

    assert "not" in out["error"] and "agent:ra4real00" in out["error"]


async def test_reconcile_agent_fold_refuses_unknown_refs(actions: Actions) -> None:
    await _mk_agent(actions, "agent:ra5dupe00")
    await _mk_agent(actions, "agent:ra5into00")
    await fold_agent(actions, dupe="agent:ra5dupe00", into="agent:ra5into00",
                     evidence="x", actor="operator")

    missing_into = await reconcile_agent_fold(actions, dupe="agent:ra5dupe00",
                                              into="agent:nope-at-all", actor="operator")
    assert "no such agent" in missing_into["error"]

    missing_dupe = await reconcile_agent_fold(actions, dupe="agent:nope-either",
                                              into="agent:ra5into00", actor="operator")
    assert "no such agent" in missing_dupe["error"]


async def test_reconcile_agent_fold_refuses_a_non_operator_actor(actions: Actions) -> None:
    """SAME GATE AS fold_agent (finding 962579a6): repairing a merge needs the same
    authority as making one."""
    await _mk_agent(actions, "agent:ra6dupe00")
    await _mk_agent(actions, "agent:ra6into00")
    await fold_agent(actions, dupe="agent:ra6dupe00", into="agent:ra6into00",
                     evidence="x", actor="operator")

    out = await reconcile_agent_fold(actions, dupe="agent:ra6dupe00",
                                     into="agent:ra6into00", actor="agent:rando")

    assert "not authorized" in out["error"]


async def test_living_head_follows_a_cross_base_succession(actions: Actions) -> None:
    """THE AUTO-HEAL (operator, 2026-07-17: 'the folds have to auto heal'): a tray row
    citing a DEAD generation of a rebased lineage still lands its estate on the one who
    answers to the name today — the graph walk crosses id bases wherever a succession
    was recorded (Ra XV -> XVI), and the registry can never regress the answer."""
    from datetime import UTC, datetime

    from src.orchestrator.mounts import save_mount

    now = datetime.now(UTC)
    old = await actions.create_or_find_object("Agent", "agent:01dbaaaa-xv",
                                              "agent:01dbaaaa-xv")
    await actions.create_or_find_object("Agent", "agent:4ebaaaaa", "agent:4ebaaaaa")
    nb2 = await actions.create_or_find_object("Agent", "agent:4ebaaaaa-ii",
                                              "agent:4ebaaaaa-ii")
    nb1 = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical='agent:4ebaaaaa' AND type='Agent'")
    await actions.assert_property(old, "succeeded_by", "agent:4ebaaaaa",
                                  "agent:test", now, 0.95,
                                  evidence_class="direct_observation")
    await actions.assert_property(nb1, "succeeded_by", "agent:4ebaaaaa-ii",
                                  "agent:test", now, 0.95,
                                  evidence_class="direct_observation")
    # the dead generation resolves ACROSS the rebase to the living head
    assert await living_head(actions.pool, "agent:01dbaaaa-xv") == "agent:4ebaaaaa-ii"
    # and a stale registry row for an OLDER generation can never regress the answer
    await save_mount(actions.pool, job_dir="/jobs/4ebaaaaa", agent_id="agent:4ebaaaaa",
                     project="p", cwd="/w/p", model=None, session_key=None)
    assert await living_head(actions.pool, "agent:4ebaaaaa-ii") == "agent:4ebaaaaa-ii"
    assert nb2 is not None


# --- wakeable_identity (thread 28842543): wake's own question, distinct from delivery ---

async def test_wakeable_identity_finds_the_live_body_past_a_phantom_successor(
    actions: Actions,
) -> None:
    """Reproduces thread 28842543 at the resolver level: a declared successor that never
    mounted must not hide the live body behind it. living_head is right to trust the
    declared succession for DELIVERY (ruling 1db1ff41: declared beats derived) —
    wakeable_identity answers a DIFFERENT question ('which OS session can be resumed') and
    must disagree here, on purpose."""
    await save_mount(actions.pool, job_dir="/jobs/e08c3850", agent_id="agent:e08c3850",
                     project="imhotep", cwd="/w/imhotep", model=None, session_key=None)
    base = await actions.create_or_find_object("Agent", "agent:e08c3850", "agent:e08c3850")
    # the successor is MINTED (a real Agent object, exactly what mint_heir does) — just
    # never mounted an OS session; lineage_head only advances past a succeeded_by pointer
    # that resolves to a real, active Agent object, so this is the shape that matters
    await actions.create_or_find_object("Agent", "agent:e08c3850-xi", "agent:e08c3850")
    await actions.assert_property(base, "succeeded_by", "agent:e08c3850-xi",
                                  "agent:test", datetime.now(UTC), 0.95,
                                  evidence_class="direct_observation")
    # living_head trusts the declaration (correct for delivery) — the successor never mounted
    assert await living_head(actions.pool, "agent:e08c3850") == "agent:e08c3850-xi"
    # wakeable_identity answers wake's own question and finds the body that actually can
    assert await wakeable_identity(actions.pool, "agent:e08c3850") == "agent:e08c3850"
    assert await wakeable_identity(actions.pool, "agent:e08c3850-xi") == "agent:e08c3850"


async def test_wakeable_identity_follows_a_real_completed_succession(actions: Actions) -> None:
    """Negative control: when the declared successor has ALSO mounted, more recently than
    the original, wakeable_identity follows it exactly like living_head does — a healthy
    succession is unaffected by this fix, only a phantom one is."""
    await save_mount(actions.pool, job_dir="/jobs/aaaa0000", agent_id="agent:aaaa0000",
                     project="p", cwd="/w/p", model=None, session_key=None)
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen = now() - interval '1 hour' "
        "WHERE agent_id='agent:aaaa0000'")
    base = await actions.create_or_find_object("Agent", "agent:aaaa0000", "agent:aaaa0000")
    await actions.assert_property(base, "succeeded_by", "agent:aaaa0000-ii",
                                  "agent:test", datetime.now(UTC), 0.95,
                                  evidence_class="direct_observation")
    await save_mount(actions.pool, job_dir="/jobs/aaaa0000ii", agent_id="agent:aaaa0000-ii",
                     project="p", cwd="/w/p", model=None, session_key=None)
    assert await living_head(actions.pool, "agent:aaaa0000") == "agent:aaaa0000-ii"
    assert await wakeable_identity(actions.pool, "agent:aaaa0000") == "agent:aaaa0000-ii"


async def test_wakeable_identity_is_none_when_the_lineage_never_mounted(
    actions: Actions,
) -> None:
    assert await wakeable_identity(actions.pool, "agent:never5een") is None
