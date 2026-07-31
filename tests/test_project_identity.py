"""PROJECT IDENTITY EVIDENCE (#110, decision 1db1ff41) — the read-only resolver: gather
whichever tiers have signal, name each one's answer, mark disagreement, never pick a
winner. These tests hold that contract, not any downstream rename/fork verb (still to
come)."""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.project_identity import project_identity_evidence
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
