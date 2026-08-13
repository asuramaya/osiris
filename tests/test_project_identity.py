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
    rename_evidence_verdict,
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


async def test_rename_project_surfaces_prior_art_never_refuses_on_it(
    actions: Actions, monkeypatch,
) -> None:
    """obligation e4612853's sibling (Thoth DM 3169/3185): a standing Decision covering
    this exact rename is surfaced in the receipt, never blocks the write."""
    await _mk_project(actions, "bytebye")

    async def _fake_prior_art(pool, *, subject_canonical, field, new_value, because, actor):
        return {"prior_art": [{"id": "1db87191", "type": "Decision"}],
               "prior_art_flag": f"a standing ruling (1db87191) may already cover "
                                  f"{subject_canonical}'s {field!r}"}

    monkeypatch.setattr(
        "src.orchestrator.capture.property_prior_art", _fake_prior_art)
    out = await rename_project(actions, project="bytebye", new_name="ByeByte",
                               because="tidying casing", actor="agent:test")
    assert out["new_name"] == "ByeByte"  # the write still happened
    assert out["prior_art_flag"] == (
        "a standing ruling (1db87191) may already cover repo:bytebye's 'name'")


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


# --- rename_evidence_verdict (#137's arc, operator ruling: DO NOT CROWN A TIER) --------
# a NAMED signal against a SPECIFIC new_name, distinct from project_identity_evidence's
# own "agreement" field (which only says whether a seat's tiers agree with EACH OTHER) —
# though the verdict function reuses that exact field rather than re-deriving it.

def _evidence(candidates: dict[str, dict[str, object]], agreement: str) -> dict[str, object]:
    """A minimal project_identity_evidence-shaped dict — only the fields the verdict
    function reads, so these tests pin its contract without a live seat/DB round trip.
    `agreement` must be supplied explicitly (never recomputed here) so each test states
    the exact input the real function would have produced, rather than a second copy of
    its ranking logic drifting alongside these tests."""
    return {"candidates": candidates, "agreement": agreement}


def test_rename_evidence_verdict_no_signal_on_empty_candidates() -> None:
    assert rename_evidence_verdict(_evidence({}, "no-signal"), "anything") == "no-signal"


def test_rename_evidence_verdict_confirms_when_new_name_is_the_sole_strong_candidate() -> None:
    ev = _evidence({"newname": {"declared_charter": True, "pin_match": False,
                                "remote_agrees": None}}, "single-candidate")
    assert rename_evidence_verdict(ev, "newname") == "confirms"
    ev2 = _evidence({"newname": {"declared_charter": False, "pin_match": True,
                                 "remote_agrees": None}}, "single-candidate")
    assert rename_evidence_verdict(ev2, "newname") == "confirms"
    ev3 = _evidence({"newname": {"declared_charter": False, "pin_match": False,
                                 "remote_agrees": True}}, "single-candidate")
    assert rename_evidence_verdict(ev3, "newname") == "confirms"


def test_rename_evidence_verdict_disagrees_when_the_sole_strong_candidate_is_not_new_name() -> None:
    ev = _evidence({"oldname": {"declared_charter": True, "pin_match": True,
                                "remote_agrees": None}}, "single-candidate")
    assert rename_evidence_verdict(ev, "newname") == "disagrees"


def test_rename_evidence_verdict_disagrees_on_internal_disagreement_even_if_new_name_wins() -> None:
    """LIVE-VERIFIED SPECIMEN, run against production data 2026-08-13 (Thoth's dispatch
    msg 4213, requirement 3): seat:ddafff44 (khepri, governs repo:tony). remote_agrees
    AND write_attribution both back "cultural-infrastructure" (the current declared
    name) — the STRONGER case by any tiebreak — while the seat's own PIN still says
    "tony". A verdict that only asked "does new_name have real signal" would have
    called this "confirms" and buried exactly the stale-pin disagreement #137 exists to
    catch. Reusing `agreement == "disagree"` directly (rather than re-deriving a
    per-name "is it the strongest" comparison) is what catches it: ambiguity itself is
    the finding, and new_name having a stronger case among the rivals does not resolve
    it — that would be crowning a tier by magnitude instead of by name, the same
    mistake the operator's ruling forbids."""
    live_shape = _evidence({
        "cultural-infrastructure": {"declared_charter": False, "pin_match": False,
                                    "remote_agrees": True},
        "tony": {"declared_charter": False, "pin_match": True, "remote_agrees": False},
    }, "disagree")
    assert rename_evidence_verdict(live_shape, "cultural-infrastructure") == "disagrees"


def test_rename_evidence_verdict_no_signal_when_agreement_says_so() -> None:
    # project_identity_evidence itself only ever computes "no-signal" when candidates is
    # empty, but the verdict function still honors an explicit no-signal agreement value
    # defensively rather than assuming that invariant holds forever unchecked.
    ev = _evidence({"newname": {"declared_charter": False, "pin_match": False,
                                "remote_agrees": False}}, "no-signal")
    assert rename_evidence_verdict(ev, "newname") == "no-signal"


# --- MCP surface (task #163's arc: this whole module existed, tested, and had ZERO ------
# MCP wiring until now — grep against src/mcp_server.py before this change: zero hits) ---

async def test_mcp_rename_project_surfaces_evidence_by_governing_seat(
    actions: Actions, tmp_path,
) -> None:
    """The MCP `rename_project` tool wires project_identity_evidence in as a PRE-WRITE
    CHECK (task #163's arc, #137's own root-cause fix, operator ruling: DO NOT CROWN A
    TIER): every Seat currently GOVERNING the project being renamed gets its own evidence
    report attached to the receipt, classified into a NAMED verdict — no-signal/confirms/
    disagrees — against the specific new_name declared, so a caller sees in the SAME turn
    whether that seat's pin/charter/remote still disagrees. NOT A TIER RULING: this never
    refuses and never picks a winner on it; the rename itself always proceeds regardless,
    but a disagreement surfaces as an unmissable top-level warning, never buried."""
    import src.mcp_server as srv
    from src.mcp_server import _agents, _conn_key
    from src.mcp_server import rename_project as rename_tool
    from src.orchestrator.agents import AgentIdentity

    office = tmp_path / "office"
    office.mkdir()
    (office / ".osiris").write_text('project = "oldname"\n')
    seat = await ensure_seat(actions, house="osiris", handle="Renameseat",
                             anchor_cwd=str(office), source="test")
    await _mk_agent(actions, "agent:re0001ab")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:re0001ab")
    proj = await _mk_project(actions, "oldname")
    seat_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", seat["seat_id"])
    await actions.create_link(seat_oid, proj, "governs", "test", datetime.now(UTC), 0.9)

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    _agents[_conn_key(ctx)] = AgentIdentity(
        agent_id="agent:renamer1", session="renamer1", project="rename-land",
        model=None, cwd=None)
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        out = await rename_tool(project="oldname", new_name="newname",
                                because="test rename", ctx=ctx)
        assert out["new_name"] == "newname"
        assert seat["seat_id"] in out["rename_evidence"]
        entry = out["rename_evidence"][seat["seat_id"]]
        seat_evidence = entry["evidence"]
        assert seat_evidence["seat_id"] == seat["seat_id"]
        assert "candidates" in seat_evidence
        # the pin still says the OLD name — exactly the #137 disagreement this must
        # surface, not hide, since the rename never touches the seat's own .osiris file
        assert "oldname" in seat_evidence["candidates"]
        # NAMED, NEVER SILENT: the pin's disagreement becomes an explicit verdict, and
        # an unmissable top-level warning — never something a caller has to notice by
        # diffing candidates themselves
        assert entry["verdict"] == "disagrees"
        assert out["evidence_disagrees"] is True
        assert seat["seat_id"] in out["warning"]
        # SELF-CONSISTENCY, NOT VERIFICATION (Thoth's msg 4232): "confirms" must never be
        # readable as "verified correct" — the receipt says so in its own wording, not
        # only in a docstring a caller may never read
        assert "self-consistency" in out["rename_evidence_note"].lower()
        assert "not" in out["rename_evidence_note"].lower()
    finally:
        srv._pool = saved_pool
        _agents.pop(_conn_key(ctx), None)


async def test_mcp_project_identity_evidence_and_fork_doors(
    actions: Actions, tmp_path,
) -> None:
    """Smoke test for the three other doors this arc wired: project_identity_evidence,
    fork_project, unfork_project — each already existed and was tested via direct import,
    but nothing outside a Python import could ever reach them."""
    import src.mcp_server as srv
    from src.mcp_server import _agents, _conn_key
    from src.mcp_server import fork_project as fork_tool
    from src.mcp_server import project_identity_evidence as pie_tool
    from src.mcp_server import unfork_project as unfork_tool
    from src.orchestrator.agents import AgentIdentity

    office = tmp_path / "office2"
    office.mkdir()
    seat = await ensure_seat(actions, house="osiris", handle="Forkseat",
                             anchor_cwd=str(office), source="test")
    ancestor = await _mk_project(actions, "ancestorproj")
    successor = await _mk_project(actions, "successorproj")

    class _Ctx:
        class request_context:  # noqa: N801
            request = None
            session = object()

    ctx = _Ctx()
    _agents[_conn_key(ctx)] = AgentIdentity(
        agent_id="agent:forker1", session="forker1", project="fork-land",
        model=None, cwd=None)
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        evidence = await pie_tool(seat_id=seat["seat_id"], ctx=ctx)
        assert evidence["seat_id"] == seat["seat_id"]
        assert "candidates" in evidence

        forked = await fork_tool(project="ancestorproj", fork_into="successorproj",
                                 because="test fork", ctx=ctx)
        assert forked["forked_from"] == "repo:ancestorproj"
        assert forked["into"] == "repo:successorproj"
        edge = await actions.pool.fetchval(
            "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='forked_from' "
            "AND (valid_until IS NULL OR valid_until > now())", successor, ancestor)
        assert edge == 1

        unforked = await unfork_tool(project="ancestorproj", fork_into="successorproj",
                                     because="test unfork", ctx=ctx)
        assert unforked["unforked"] == "repo:ancestorproj"
        edge_after = await actions.pool.fetchval(
            "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='forked_from' "
            "AND (valid_until IS NULL OR valid_until > now())", successor, ancestor)
        assert edge_after is None
    finally:
        srv._pool = saved_pool
        _agents.pop(_conn_key(ctx), None)


async def test_rename_project_migrates_edges_never_orphans_them(
    actions: Actions, tmp_path,
) -> None:
    """ADVERSARIAL TEST (Thoth's ask, msg 4083 — Alfred's charter rides on the answer):
    rename a project carrying a `governs` edge from one Seat AND a `works_in` edge from a
    DIFFERENT-source Agent, then confirm both still resolve from the renamed object and
    no second object with the old canonical survives. `rename_project` never calls
    create_or_find_object at all (confirmed by re-reading it end to end for this
    question) — it resolves the EXISTING row, then `assert_property`s `name` on that SAME
    `id`; `canonical` is never rewritten. So there is nothing to migrate: every edge was
    always keyed on the object's immutable `id`, never on its name or canonical, and stays
    correct automatically. This proves it against a live object rather than trusting the
    docstring's own claim."""
    office = tmp_path / "governing_office"
    office.mkdir()
    seat = await ensure_seat(actions, house="osiris", handle="Governseat",
                             anchor_cwd=str(office), source="test")
    seat_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", seat["seat_id"])
    agent_oid = await _mk_agent(actions, "agent:worksin0001")
    proj = await _mk_project(actions, "beforename")

    now = datetime.now(UTC)
    await actions.create_link(seat_oid, proj, "governs", "test", now, 0.9)
    await actions.create_link(agent_oid, proj, "works_in", "test", now, 0.9)

    old_canonical = await actions.pool.fetchval(
        "SELECT canonical FROM objects WHERE id=$1", proj)

    out = await rename_project(actions, project="beforename", new_name="aftername",
                               because="adversarial edge-migration test", actor="agent:test")
    assert out["new_name"] == "aftername"
    assert out["project"] == old_canonical  # canonical is UNCHANGED — the receipt's own
                                            # "project" key IS the canonical, by contract

    # the object's own id and canonical are byte-identical to before the rename
    new_canonical = await actions.pool.fetchval(
        "SELECT canonical FROM objects WHERE id=$1", proj)
    assert new_canonical == old_canonical

    # BOTH edges still resolve FROM THE SAME object id — nothing needed to migrate because
    # nothing ever pointed at name/canonical to begin with
    governs_live = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='governs' "
        "AND (valid_until IS NULL OR valid_until > now())", seat_oid, proj)
    works_in_live = await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='works_in' "
        "AND (valid_until IS NULL OR valid_until > now())", agent_oid, proj)
    assert governs_live == 1
    assert works_in_live == 1

    # no second object minted under any name — exactly one SoftwareProject answers to
    # either the old or the new label
    count = await actions.pool.fetchval(
        "SELECT count(*) FROM objects WHERE type='SoftwareProject' AND canonical=$1",
        old_canonical)
    assert count == 1

    # the NEW name resolves to the SAME object (never a twin) via the live name property
    from src.orchestrator.projects import _resolve_software_project
    resolved_new = await _resolve_software_project(actions.pool, "aftername")
    assert resolved_new is not None
    assert resolved_new["id"] == proj
