"""SEAT REBIND — move a seat's anchor cwd, preserving identity, lineage, attribution, and mail
(Phase 1 §4.1, ruling `dd47c1da`: 'path = project = identity' orphaned alfred when the operator
moved his folder — the operator is BLOCKED on this cure). Pilot shape exercised here: house
bytebye, alfred's seat, a pure office with no code in the folder.
"""
from __future__ import annotations

import time
import tomllib
from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator import mounts
from src.orchestrator.agents import (
    claim_name,
    house_of,
    register_agent,
    resolve_handle,
    resolve_identity,
)
from src.orchestrator.mailbox import send_message, unread_count
from src.orchestrator.mounts import rebind_seat


async def test_rebind_moves_the_anchor_preserving_everything(
    actions: Actions, tmp_path: Path
) -> None:
    a_dir = tmp_path / "bytebye-a"
    b_dir = tmp_path / "bytebye-b"
    a_dir.mkdir()

    ident = resolve_identity(cwd=str(a_dir), session="alfred01", project_label="bytebye")
    await register_agent(actions, ident, actor="analyst:operator")
    await claim_name(actions, ident.agent_id, "alfred", source=ident.agent_id)
    await mounts.save_mount(actions.pool, job_dir="/j/alfred01", agent_id=ident.agent_id,
                            project="bytebye", cwd=str(a_dir), model="claude-fable-5",
                            session_key="k")
    # mail addressed to alfred's PROJECT — unread, waiting
    await send_message(actions.pool, from_agent="agent:ux", from_project="bytebye",
                       to_project="bytebye", body="the office move is happening")
    assert await unread_count(actions.pool, "bytebye", reader_agent=ident.agent_id) == 1

    receipt = await rebind_seat(actions, seat_or_agent="alfred", new_cwd=str(b_dir))
    assert receipt["agent"] == ident.agent_id
    assert receipt["project"] == "bytebye"
    assert receipt["old_cwd"] == str(a_dir)
    assert receipt["new_cwd"] == str(b_dir)
    assert receipt["mount_rows_updated"] == 1
    assert receipt["osiris_written"] == str(b_dir / ".osiris")

    # (a) the SAME agent_id resolves — no mint, no fork
    assert await resolve_handle(actions, "alfred") == ident.agent_id
    # (b) the durable project label is UNCHANGED
    assert await house_of(actions.pool, ident.agent_id) == "bytebye"
    # (c) mail is still readable under the same label
    assert await unread_count(actions.pool, "bytebye", reader_agent=ident.agent_id) == 1
    # (d) the mount row points at B
    row = await actions.pool.fetchrow(
        "SELECT cwd FROM agent_mounts WHERE agent_id=$1", ident.agent_id)
    assert row is not None and row["cwd"] == str(b_dir)
    # (e) .osiris in B carries the label
    osiris_file = b_dir / ".osiris"
    assert osiris_file.is_file()
    assert tomllib.loads(osiris_file.read_text())["project"] == "bytebye"
    # (f) an assertion records the move
    moved = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='anchor_moved' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", ident.agent_id)
    assert moved is not None and str(a_dir) in moved and str(b_dir) in moved


async def test_rebind_moves_every_generation_of_the_lineage(
    actions: Actions, tmp_path: Path
) -> None:
    """(d): the WHOLE lineage's durable mount rows move, not just the current holder's — or an
    earlier generation's row resurrects the seat at the old path the instant anything reads it
    by job_dir."""
    a_dir = tmp_path / "multi-a"
    b_dir = tmp_path / "multi-b"
    a_dir.mkdir()
    oid = await actions.create_or_find_object("Agent", "agent:multi", "agent:multi")
    await actions.assert_property(oid, "project", "multihouse", "agent:multi",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    await claim_name(actions, "agent:multi", "Multi", source="agent:multi")
    await mounts.save_mount(actions.pool, job_dir="/j/multi-1", agent_id="agent:multi",
                            project="multihouse", cwd=str(a_dir), model="claude-fable-5",
                            session_key="k1")
    # a second generation's row (an earlier holder's), same lineage, same old path
    await mounts.save_mount(actions.pool, job_dir="/j/multi-0", agent_id="agent:multi-ii",
                            project="multihouse", cwd=str(a_dir), model="claude-opus-4-8",
                            session_key="k0")

    receipt = await rebind_seat(actions, seat_or_agent="Multi", new_cwd=str(b_dir))
    assert receipt["mount_rows_updated"] == 2
    rows = await actions.pool.fetch(
        "SELECT job_dir, cwd FROM agent_mounts WHERE job_dir IN ('/j/multi-1','/j/multi-0')")
    assert {r["cwd"] for r in rows} == {str(b_dir)}


async def test_rebind_of_a_raw_agent_id_works(actions: Actions, tmp_path: Path) -> None:
    """The grave rule: an explicit id is intent — a seat with NO claimed name can still be
    rebound by its raw agent id."""
    a_dir = tmp_path / "raw-a"
    b_dir = tmp_path / "raw-b"
    a_dir.mkdir()
    oid = await actions.create_or_find_object("Agent", "agent:rawid01", "agent:rawid01")
    await actions.assert_property(oid, "project", "rawhouse", "agent:rawid01",
                                  datetime.now(UTC), 0.9, evidence_class="self_declared")
    await mounts.save_mount(actions.pool, job_dir="/j/rawid01", agent_id="agent:rawid01",
                            project="rawhouse", cwd=str(a_dir), model=None, session_key="k")

    receipt = await rebind_seat(actions, seat_or_agent="agent:rawid01", new_cwd=str(b_dir))
    assert receipt["agent"] == "agent:rawid01"
    assert receipt["project"] == "rawhouse"
    row = await actions.pool.fetchrow("SELECT cwd FROM agent_mounts WHERE job_dir='/j/rawid01'")
    assert row is not None and row["cwd"] == str(b_dir)


async def test_rebind_refuses_an_unknown_seat_loudly(actions: Actions, tmp_path: Path) -> None:
    target = tmp_path / "nowhere"
    out = await rebind_seat(actions, seat_or_agent="NobodyHome", new_cwd=str(target))
    assert "error" in out
    assert not target.exists()  # refused — nothing written, not even the directory


async def test_rebind_of_an_unmounted_bare_id_is_also_refused(
    actions: Actions, tmp_path: Path
) -> None:
    """A raw id with no Agent object at all is still 'unknown' — the grave rule is intent
    about a REAL grave, not licence to mint one."""
    out = await rebind_seat(actions, seat_or_agent="agent:never-existed",
                            new_cwd=str(tmp_path / "ghost"))
    assert "error" in out


# --- the harness half (operator's arbitrary-move directive, 2026-07-15) ---


def test_migrate_harness_metadata_moves_transcripts_and_rekeys_project_state(
    tmp_path: Path,
) -> None:
    """mv + rebind = a complete non-event: the transcripts dir merges old→new (never
    clobbering — both sides of a fracture may hold real sessions) and the .claude.json
    project entry re-keys, atomically."""
    import json as _json

    from src.orchestrator.mounts import migrate_harness_metadata

    root = tmp_path / "projects"
    old = root / "-w-code-bytebye"
    old.mkdir(parents=True)
    (old / "aaaa.jsonl").write_text("{}\n")
    (old / "bbbb.jsonl").write_text("{}\n")
    (old / "subagents").mkdir()
    (old / "subagents" / "child.jsonl").write_text("{}\n")
    new = root / "-w-code-REPOS-ByeByte"
    new.mkdir(parents=True)
    (new / "bbbb.jsonl").write_text('{"other": true}\n')   # exists on BOTH sides — stays
    cj = tmp_path / "claude.json"
    cj.write_text(_json.dumps({"projects": {
        "/w/code/bytebye": {"allowedTools": ["Bash"], "hasTrustDialogAccepted": True}}}))

    out = migrate_harness_metadata("/w/code/bytebye", "/w/code/REPOS/ByeByte",
                                   projects_root=root, claude_json=cj)

    assert out["transcripts_moved"] == 2                   # aaaa.jsonl + subagents/child
    assert out["transcripts_left_behind"] == 1             # bbbb.jsonl, never clobbered
    assert (new / "aaaa.jsonl").is_file()
    assert (new / "subagents" / "child.jsonl").is_file()
    assert (new / "bbbb.jsonl").read_text() == '{"other": true}\n'
    assert (old / "bbbb.jsonl").is_file()                  # the conflict stays put, honest
    assert out["project_state"] == "re-keyed to the new path"
    data = _json.loads(cj.read_text())
    assert "/w/code/bytebye" not in data["projects"]
    assert data["projects"]["/w/code/REPOS/ByeByte"]["hasTrustDialogAccepted"] is True


def test_migrate_harness_metadata_never_overwrites_the_new_paths_own_state(
    tmp_path: Path,
) -> None:
    import json as _json

    from src.orchestrator.mounts import migrate_harness_metadata

    cj = tmp_path / "claude.json"
    cj.write_text(_json.dumps({"projects": {
        "/w/old": {"allowedTools": ["Bash"]},
        "/w/new": {"allowedTools": ["Bash", "Edit"]}}}))
    out = migrate_harness_metadata("/w/old", "/w/new",
                                   projects_root=tmp_path / "projects", claude_json=cj)
    assert out["transcripts_moved"] == 0                   # no old dir: nothing to move
    assert "left in place" in out["project_state"]
    data = _json.loads(cj.read_text())
    assert data["projects"]["/w/new"]["allowedTools"] == ["Bash", "Edit"]  # untouched
    assert "/w/old" in data["projects"]                    # nothing silently dropped


async def test_rebind_seat_carries_the_harness_half(actions: Actions, tmp_path: Path) -> None:
    """The full receipt: graph half (label pinned, rows re-pointed) + harness half
    (transcripts moved, project state re-keyed) in one act."""
    import json as _json

    from src.orchestrator.mounts import rebind_seat, save_mount

    old_cwd = str(tmp_path / "office-old")
    new_cwd = str(tmp_path / "office-new")
    Path(old_cwd).mkdir()
    (Path(old_cwd) / ".osiris").write_text('project = "movinghouse"\n')
    a = await actions.create_or_find_object("Agent", "agent:wwww0001", "agent:wwww0001")
    now = datetime.now(UTC)
    await actions.assert_property(a, "project", "movinghouse", "agent:wwww0001", now, 0.9,
                                  evidence_class="self_declared")
    await save_mount(actions.pool, job_dir="/jobs/wwww0001", agent_id="agent:wwww0001",
                     project="movinghouse", cwd=old_cwd, model=None, session_key=None)
    root = tmp_path / "projects"
    old_slug = root / old_cwd.replace("/", "-")
    old_slug.mkdir(parents=True)
    (old_slug / "sess.jsonl").write_text("{}\n")
    cj = tmp_path / "claude.json"
    cj.write_text(_json.dumps({"projects": {old_cwd: {"hasTrustDialogAccepted": True}}}))

    out = await rebind_seat(actions, seat_or_agent="agent:wwww0001", new_cwd=new_cwd,
                            actor="agent:test", projects_root=root, claude_json=cj)

    assert out["project"] == "movinghouse"
    assert out["mount_rows_updated"] == 1
    assert out["harness"]["transcripts_moved"] == 1
    assert out["harness"]["project_state"] == "re-keyed to the new path"
    assert (root / new_cwd.replace("/", "-") / "sess.jsonl").is_file()
    row = await actions.pool.fetchval(
        "SELECT cwd FROM agent_mounts WHERE agent_id='agent:wwww0001'")
    assert row == new_cwd


# --- extraction mode (the seat-offices ruling, ed5f5ce2) ---


async def test_extraction_takes_only_the_seats_own_lineage(
    actions: Actions, tmp_path: Path,
) -> None:
    """Moving a seat OUT of a shared cwd (into its Osiris office) takes only its own
    lineage's transcripts — the co-resident repo sessions' history stays, their slug
    survives, and the .claude.json entry for the old path is never touched (it is still
    a living project)."""
    import json as _json

    from src.orchestrator.mounts import rebind_seat, save_mount

    shared = str(tmp_path / "shared-repo")
    office = str(tmp_path / "seats" / "butler")
    Path(shared).mkdir()
    (Path(shared) / ".osiris").write_text('project = "butlerhouse"\n')
    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", "agent:cafe77aa", "agent:cafe77aa")
    await actions.assert_property(a, "project", "butlerhouse", "agent:cafe77aa", now, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(a, "session", "cafe77aa", "agent:cafe77aa", now, 0.9,
                                  evidence_class="self_declared")
    await save_mount(actions.pool, job_dir="/jobs/cafe77aa", agent_id="agent:cafe77aa",
                     project="butlerhouse", cwd=shared, model=None, session_key=None)
    root = tmp_path / "projects"
    slug = root / shared.replace("/", "-")
    slug.mkdir(parents=True)
    (slug / "cafe77aa-full-session-id.jsonl").write_text("{}\n")   # the seat's own
    (slug / "aaaa1111-somebody-else.jsonl").write_text("{}\n")     # a co-resident repo session
    cj = tmp_path / "claude.json"
    cj.write_text(_json.dumps({"projects": {shared: {"hasTrustDialogAccepted": True}}}))

    out = await rebind_seat(actions, seat_or_agent="agent:cafe77aa", new_cwd=office,
                            actor="agent:test", projects_root=root, claude_json=cj,
                            extract=True)

    assert out["harness"]["mode"].startswith("extraction")
    assert out["harness"]["transcripts_moved"] == 1
    assert (root / office.replace("/", "-") / "cafe77aa-full-session-id.jsonl").is_file()
    assert (slug / "aaaa1111-somebody-else.jsonl").is_file()       # the repo session STAYS
    assert not (slug / "cafe77aa-full-session-id.jsonl").exists()
    data = _json.loads(cj.read_text())
    assert shared in data["projects"]                              # old path still a project
    assert office not in data["projects"]                          # office earns its own later
    assert (Path(office) / ".osiris").read_text().startswith('project = "butlerhouse"')
    row = await actions.pool.fetchval(
        "SELECT cwd FROM agent_mounts WHERE agent_id='agent:cafe77aa'")
    assert row == office


async def test_extraction_carries_a_registry_less_lineage_by_transcript_evidence(
    actions: Actions, tmp_path: Path,
) -> None:
    """A lineage whose generations all predate the mount registry (no agent_mounts row
    anywhere) still gets its estate carried: the anchor derives from where its sid
    transcripts actually live — their internal cwd, the address authority — instead of
    minting an office while the whole mind stays in the old slug (the children's-rollout
    catch: all five rollout children were this case)."""
    shared = str(tmp_path / "repo-home")
    office = str(tmp_path / "seats" / "orphan")
    Path(shared).mkdir()
    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", "agent:beadfeed", "agent:beadfeed")
    await actions.assert_property(a, "project", "orphanhouse", "agent:beadfeed", now, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(a, "session", "beadfeed", "agent:beadfeed", now, 0.9,
                                  evidence_class="self_declared")
    root = tmp_path / "projects"
    slug = root / shared.replace("/", "-")
    slug.mkdir(parents=True)
    _jsonl(slug / "beadfeed-full-session-id.jsonl", shared, None, shared)
    _jsonl(slug / "bbbb2222-somebody-else.jsonl", shared)

    out = await rebind_seat(actions, seat_or_agent="agent:beadfeed", new_cwd=office,
                            actor="agent:test", projects_root=root,
                            claude_json=tmp_path / "cj.json", extract=True)

    assert out["old_cwd"] == shared
    assert out["old_cwd_evidence"].startswith("transcript-location")
    assert out["harness"]["transcripts_moved"] == 1
    moved = root / office.replace("/", "-") / "beadfeed-full-session-id.jsonl"
    assert moved.is_file()
    assert _cwds_of(moved) == [office, None, office]           # re-addressed to the office
    assert (slug / "bbbb2222-somebody-else.jsonl").is_file()   # the co-resident stays


def test_lineage_cwd_evidence_reads_the_freshest_transcript(tmp_path: Path) -> None:
    """The freshest sid transcript's internal cwd decides; no transcript anywhere → None
    (a bodiless lineage carries nothing, and nothing is guessed)."""
    import os

    root = tmp_path / "projects"
    old_slug = root / "-old-home"
    new_slug = root / "-new-home"
    old_slug.mkdir(parents=True)
    new_slug.mkdir(parents=True)
    stale = old_slug / "feed0001-old.jsonl"
    fresh = new_slug / "feed0002-new.jsonl"
    _jsonl(stale, "/old/home")
    _jsonl(fresh, "/new/home")
    os.utime(stale, (1_000_000, 1_000_000))

    assert mounts.lineage_cwd_evidence({"feed0001", "feed0002"},
                                       projects_root=root) == "/new/home"
    assert mounts.lineage_cwd_evidence({"beefbeef"}, projects_root=root) is None


async def test_rebind_updates_the_held_seats_anchor(actions: Actions, tmp_path: Path) -> None:
    """A rebound seat-holder's Seat OBJECT follows: anchor_cwd re-asserts to the new path —
    the daemon summons at the office."""
    from src.orchestrator.mounts import rebind_seat, save_mount
    from src.orchestrator.seats import attach_session, ensure_seat, mint_attach_token

    old = str(tmp_path / "old-office")
    new = str(tmp_path / "new-office")
    Path(old).mkdir()
    (Path(old) / ".osiris").write_text('project = "anchorhouse"\n')
    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", "agent:dddd77bb", "agent:dddd77bb")
    await actions.assert_property(a, "project", "anchorhouse", "agent:dddd77bb", now, 0.9,
                                  evidence_class="self_declared")
    await save_mount(actions.pool, job_dir="/jobs/dddd77bb", agent_id="agent:dddd77bb",
                     project="anchorhouse", cwd=old, model=None, session_key=None)
    seat = await ensure_seat(actions, house="anchorhouse", handle="Jeeves",
                             anchor_cwd=old, source="test")
    token = await mint_attach_token(actions.pool, seat_id=seat["seat_id"])
    await attach_session(actions, seat_id=seat["seat_id"], token=token,
                         job_dir="/jobs/dddd77bb", agent_id="agent:dddd77bb")

    await rebind_seat(actions, seat_or_agent="agent:dddd77bb", new_cwd=new,
                      actor="agent:test", projects_root=tmp_path / "projects",
                      claude_json=tmp_path / "cj.json")

    anchor = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='anchor_cwd' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", seat["seat_id"])
    assert anchor == new


# --- the resume heal (thread 39ea074c: the alfred transition test's catch) ---


def _jsonl(path: Path, *cwds: str | None) -> None:
    """A minimal transcript: one line per cwd (None = a summary line with no cwd)."""
    import json as _json

    lines = []
    for i, c in enumerate(cwds):
        obj: dict[str, object] = {"type": "user", "n": i}
        if c is not None:
            obj["cwd"] = c
        lines.append(_json.dumps(obj))
    path.write_text("\n".join(lines) + "\n")


def _cwds_of(path: Path) -> list[str | None]:
    import json as _json

    out: list[str | None] = []
    for line in path.read_text().splitlines():
        if line.strip():
            out.append(_json.loads(line).get("cwd"))
    return out


def test_moved_transcripts_get_readdressed(tmp_path: Path) -> None:
    """A wholesale move re-addresses every transcript it lands: the harness validates
    resume against the per-line cwd, so a moved file keeping its old address stays listed
    but refuses to resume ('This conversation is from a different directory')."""
    from src.orchestrator.mounts import migrate_harness_metadata

    root = tmp_path / "projects"
    old = root / "-w-old"
    old.mkdir(parents=True)
    # a wanderer: three historical cwds + a summary line with none (alfred's real shape)
    _jsonl(old / "aaaa.jsonl", "/w/old", None, "/w/older-still", "/w/elsewhere")
    new = root / "-w-new"
    new.mkdir(parents=True)
    _jsonl(new / "bbbb.jsonl", "/w/old")          # exists ONLY on the new side: co-resident
    _jsonl(old / "bbbb.jsonl", "/w/old")          # conflict — stays put, never re-addressed

    out = migrate_harness_metadata("/w/old", "/w/new", projects_root=root,
                                   claude_json=tmp_path / "cj.json")

    assert out["cwd_readdressed"] == {"files": 1, "lines": 3}
    assert _cwds_of(new / "aaaa.jsonl") == ["/w/new", None, "/w/new", "/w/new"]
    assert _cwds_of(new / "bbbb.jsonl") == ["/w/old"]   # co-resident: not this move's to touch
    assert _cwds_of(old / "bbbb.jsonl") == ["/w/old"]   # the conflict stays, address intact


def test_extraction_takes_sid_dirs_memory_and_readdresses(tmp_path: Path) -> None:
    """Extraction takes the seat's WHOLE session estate: the .jsonl, the sid DIRECTORY
    (subagents/ + tool-results/ — session state as much as the transcript), and the slug's
    memory/ (the seat's knowledge; an office booting blind defeats the office) — and the
    moved transcript is re-addressed. Co-residents stay, untouched."""
    from src.orchestrator.mounts import migrate_harness_metadata

    root = tmp_path / "projects"
    old = root / "-w-shared"
    (old / "838639d1-full-sid" / "subagents").mkdir(parents=True)
    (old / "838639d1-full-sid" / "subagents" / "agent-x.jsonl").write_text("{}\n")
    _jsonl(old / "838639d1-full-sid.jsonl", "/w/shared", "/w/somewhere-older")
    _jsonl(old / "cccc-co-resident.jsonl", "/w/shared")
    (old / "memory").mkdir()
    (old / "memory" / "MEMORY.md").write_text("# the seat's knowledge\n")

    out = migrate_harness_metadata("/w/shared", "/w/office", projects_root=root,
                                   claude_json=tmp_path / "cj.json",
                                   only_sids={"838639d1-full-sid"})

    new = root / "-w-office"
    assert out["transcripts_moved"] == 2                    # the .jsonl + the sid dir
    assert out["memory"] == "moved with the seat"
    assert out["cwd_readdressed"] == {"files": 1, "lines": 2}
    assert (new / "838639d1-full-sid" / "subagents" / "agent-x.jsonl").is_file()
    assert (new / "memory" / "MEMORY.md").is_file()
    assert not (old / "memory").exists()
    assert _cwds_of(new / "838639d1-full-sid.jsonl") == ["/w/office", "/w/office"]
    assert (old / "cccc-co-resident.jsonl").is_file()       # the co-resident stays
    assert _cwds_of(old / "cccc-co-resident.jsonl") == ["/w/shared"]


def test_extraction_never_clobbers_the_destinations_own_memory(tmp_path: Path) -> None:
    from src.orchestrator.mounts import migrate_harness_metadata

    root = tmp_path / "projects"
    old = root / "-w-shared"
    old.mkdir(parents=True)
    (old / "memory").mkdir()
    (old / "memory" / "MEMORY.md").write_text("old\n")
    new = root / "-w-office"
    (new / "memory").mkdir(parents=True)
    (new / "memory" / "MEMORY.md").write_text("the office's own\n")

    out = migrate_harness_metadata("/w/shared", "/w/office", projects_root=root,
                                   claude_json=tmp_path / "cj.json", only_sids={"dddd"})

    assert out["memory"] == "left in place — the destination has its own"
    assert (new / "memory" / "MEMORY.md").read_text() == "the office's own\n"
    assert (old / "memory" / "MEMORY.md").read_text() == "old\n"


def test_heal_slug_transcripts_converges_the_listed_directory(tmp_path: Path) -> None:
    """The automount-time heal: a transcript LISTED here but ADDRESSED elsewhere is
    rewritten to point here — unless it is the mounting session's own, a live-pulse sid's,
    or still warm from an open tab's pen (deferred, converges on a later launch)."""
    import os

    from src.orchestrator.mounts import heal_slug_transcripts

    root = tmp_path / "projects"
    slug = root / "-w-office"
    slug.mkdir(parents=True)
    stale = time.time() - 3600
    _jsonl(slug / "aaaa1111-moved.jsonl", "/w/former-home", "/w/former-home")
    os.utime(slug / "aaaa1111-moved.jsonl", (stale, stale))
    _jsonl(slug / "bbbb2222-converged.jsonl", "/w/office")
    converged_before = (slug / "bbbb2222-converged.jsonl").read_text()
    os.utime(slug / "bbbb2222-converged.jsonl", (stale, stale))
    _jsonl(slug / "cccc3333-me.jsonl", "/w/former-home")     # the mounting session itself
    os.utime(slug / "cccc3333-me.jsonl", (stale, stale))
    _jsonl(slug / "dddd4444-live.jsonl", "/w/former-home")   # a live-pulse sid's
    os.utime(slug / "dddd4444-live.jsonl", (stale, stale))
    _jsonl(slug / "eeee5555-warm.jsonl", "/w/former-home")   # fresh mtime: an open tab's pen

    out = heal_slug_transcripts("/w/office", projects_root=root,
                                skip_sids={"cccc3333-me"}, skip_sid_prefixes={"dddd4444"})

    assert out["healed"] == {"aaaa1111": 2}
    assert out["skipped_live"] == 2
    assert out["deferred_fresh"] == 1
    assert _cwds_of(slug / "aaaa1111-moved.jsonl") == ["/w/office", "/w/office"]
    assert (slug / "bbbb2222-converged.jsonl").read_text() == converged_before
    assert _cwds_of(slug / "cccc3333-me.jsonl") == ["/w/former-home"]
    assert _cwds_of(slug / "dddd4444-live.jsonl") == ["/w/former-home"]
    assert _cwds_of(slug / "eeee5555-warm.jsonl") == ["/w/former-home"]
    assert not list(slug.glob(".*heal-tmp"))                 # no residue, atomic all the way


async def test_automount_heals_the_slug_it_mounts_into(
    actions: Actions, tmp_path: Path,
) -> None:
    """The whole loop, whisper-shaped: a session starting in a directory heals the moved
    transcripts listed there — the operator's ruling made flesh (part of the system,
    never a one-time patch)."""
    import os

    from src.orchestrator.handshake import automount

    cwd = str(tmp_path / "office")
    Path(cwd).mkdir()
    root = tmp_path / "projects"
    slug = root / cwd.replace("/", "-")
    slug.mkdir(parents=True)
    _jsonl(slug / "ffff6666-moved.jsonl", "/w/former-home")
    stale = time.time() - 3600
    os.utime(slug / "ffff6666-moved.jsonl", (stale, stale))

    out = await automount(actions, session_id="9999aaaa-heal-test", cwd=cwd,
                          actor="whisper", root=root, jobs_home=tmp_path / "jobs")

    assert out["transcripts_healed"]["healed"] == {"ffff6666": 1}
    assert _cwds_of(slug / "ffff6666-moved.jsonl") == [cwd]


# --- the bridged resume + the recollection guard (thread 90f0cb3a) ---


def test_resumed_anchor_reads_the_bridge_receipt(tmp_path: Path) -> None:
    """A session-picker resume mints a new job whose state.json names resumeSessionId —
    the harness's own receipt of the pair. resumed_anchor follows it; garbage is a None,
    never a verdict."""
    import json as _json

    from src.orchestrator.mounts import resumed_anchor

    jobs = tmp_path / "jobs"
    (jobs / "ceed2d2e").mkdir(parents=True)
    (jobs / "ceed2d2e" / "state.json").write_text(_json.dumps({
        "sessionId": "ceed2d2e-2813-437d-92c9-8ad0d7a732cf",
        "resumeSessionId": "838639d1-3f1b-4776-be63-c12fde60a75e",
        "backend": "daemon"}))
    (jobs / "aaaa0000").mkdir()
    (jobs / "aaaa0000" / "state.json").write_text("{not json")
    (jobs / "bbbb0000").mkdir()
    (jobs / "bbbb0000" / "state.json").write_text(_json.dumps({"sessionId": "bbbb0000-x"}))

    assert resumed_anchor(str(jobs / "ceed2d2e")) == str(jobs / "838639d1")
    assert resumed_anchor(str(jobs / "aaaa0000")) is None      # illegible: a hint, never a guess
    assert resumed_anchor(str(jobs / "bbbb0000")) is None      # not a resume job
    assert resumed_anchor(str(jobs / "never-was")) is None


async def test_automount_adopts_a_bridged_resume_instead_of_minting_a_twin(
    actions: Actions, tmp_path: Path,
) -> None:
    """The ctrl+a resume: a new sid with NO transcript of its own (appends continue in the
    resumed file) used to mint a twin over a living seat — the job-state receipt now names
    who it continues, and the whisper adopts."""
    import json as _json

    from src.orchestrator.handshake import automount
    from src.orchestrator.mounts import save_mount

    office = str(tmp_path / "office")
    Path(office).mkdir()
    root = tmp_path / "projects"
    jobs = tmp_path / "jobs"
    old_sid = "beef0001-2222-3333-4444-555566667777"
    await save_mount(actions.pool, job_dir=str(jobs / "beef0001"), agent_id="agent:beef0001",
                     project="bridgehouse", cwd=office, model=None, session_key=None)
    new_sid = "feed9999-8888-7777-6666-555544443333"
    (jobs / "feed9999").mkdir(parents=True)
    (jobs / "feed9999" / "state.json").write_text(_json.dumps(
        {"sessionId": new_sid, "resumeSessionId": old_sid, "backend": "daemon"}))

    out = await automount(actions, session_id=new_sid, cwd=office, actor="whisper",
                          root=root, jobs_home=jobs)

    assert out["agent"] == "agent:beef0001"                    # the resumed mind, no twin
    row = await actions.pool.fetchval(
        "SELECT agent_id FROM agent_mounts WHERE job_dir=$1", str(jobs / "feed9999"))
    assert row == "agent:beef0001"                             # the new anchor knows him too


def test_stale_recollection_trusts_the_transcripts_address(tmp_path: Path) -> None:
    """The recollection guard's evidence rule: the harness writes a session's transcript
    under the directory it actually runs in — the row's cwd holding it while the declared
    cwd does not marks the declaration as a stale memory. Everything else is conservative."""
    from src.orchestrator.mounts import stale_recollection

    root = tmp_path / "projects"
    office = str(tmp_path / "office")
    husk = str(tmp_path / "husk")
    (root / office.replace("/", "-")).mkdir(parents=True)
    (root / office.replace("/", "-") / "abcd1234-session.jsonl").write_text("{}\n")
    (root / husk.replace("/", "-")).mkdir(parents=True)

    job = str(tmp_path / "jobs" / "abcd1234")
    assert stale_recollection(job, husk, office, projects_root=root) is True
    assert stale_recollection(job, office, husk, projects_root=root) is False  # declared holds it
    assert stale_recollection(job, husk, husk, projects_root=root) is False    # neither: stand
    (root / husk.replace("/", "-") / "abcd1234-session.jsonl").write_text("{}\n")
    assert stale_recollection(job, husk, office, projects_root=root) is False  # both: stand


def test_rewrite_aborts_when_the_file_changes_underfoot(tmp_path: Path) -> None:
    """The torn-write guard: an off-the-rails live pen appending mid-rewrite must lose
    NOTHING — the rewrite re-checks the caller's stat at the last instant and aborts,
    leaving the original (appended words included) untouched."""
    import pytest
    from src.orchestrator.mounts import _rewrite_transcript_cwd

    p = tmp_path / "t.jsonl"
    _jsonl(p, "/w/former-home")
    st = p.stat()
    with p.open("a") as f:                               # the live pen strikes mid-heal
        f.write('{"cwd": "/w/former-home", "type": "user"}\n')

    with pytest.raises(OSError, match="changed while being re-addressed"):
        _rewrite_transcript_cwd(p, "/w/office", expect=(st.st_size, st.st_mtime_ns))

    assert _cwds_of(p) == ["/w/former-home", "/w/former-home"]   # every word kept
    assert not list(tmp_path.glob(".*heal-tmp"))                 # no residue


async def test_wholesale_rebind_repoints_the_co_residents_too(
    actions: Actions, tmp_path: Path,
) -> None:
    """A wholesale move moves EVERYONE (Werner's catch): when the directory itself has
    moved, every row anchored there is stale, whatever its seat — co-residents' rows
    follow. Extraction keeps today's law: the seat leaves, the co-residents stay."""
    from src.orchestrator.mounts import rebind_seat, save_mount

    old = str(tmp_path / "old-house")
    new = str(tmp_path / "new-house")
    Path(old).mkdir()
    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", "agent:abbe0001", "agent:abbe0001")
    await actions.assert_property(a, "project", "movers", "agent:abbe0001", now, 0.9,
                                  evidence_class="self_declared")
    await save_mount(actions.pool, job_dir="/jobs/abbe0001", agent_id="agent:abbe0001",
                     project="movers", cwd=old, model=None, session_key=None)
    await save_mount(actions.pool, job_dir="/jobs/cafe0002", agent_id="agent:cafe0002",
                     project="movers", cwd=old, model=None, session_key=None)

    out = await rebind_seat(actions, seat_or_agent="agent:abbe0001", new_cwd=new,
                            actor="agent:test", projects_root=tmp_path / "projects",
                            claude_json=tmp_path / "cj.json")

    assert out["mount_rows_updated"] == 1
    assert out["co_resident_rows_repointed"] == 1
    co = await actions.pool.fetchval(
        "SELECT cwd FROM agent_mounts WHERE agent_id='agent:cafe0002'")
    assert co == new                                    # the housemate moved with the house
