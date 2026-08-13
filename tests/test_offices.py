"""THE OFFICE CEREMONY (ruling ed5f5ce2) — one act moves a seat into its Osiris-owned home.
alfred's office was hand-assembled; the rollout to his chartered children is one call per
seat, and these witness the call.
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.mounts import save_mount
from src.orchestrator.offices import (
    correct_pin_value,
    establish_office,
    plan_pin_migration,
    revert_pin_write,
    write_pin_additions,
)


async def _seat_fixture(actions: Actions, tmp_path: Path, *, handle: str | None) -> str:
    """A mounted lineage in a shared repo cwd, optionally named — the pre-office shape."""
    agent = "agent:0ff1cee1"
    now = datetime.now(UTC)
    a = await actions.create_or_find_object("Agent", agent, agent)
    await actions.assert_property(a, "project", "butlerhouse", agent, now, 0.9,
                                  evidence_class="self_declared")
    await actions.assert_property(a, "session", "0ff1cee1", agent, now, 0.9,
                                  evidence_class="self_declared")
    if handle:
        await actions.assert_property(a, "handle", handle, agent, now, 0.9,
                                      evidence_class="self_declared")
    shared = str(tmp_path / "shared-repo")
    Path(shared).mkdir(exist_ok=True)
    await save_mount(actions.pool, job_dir="/jobs/0ff1cee1", agent_id=agent,
                     project="butlerhouse", cwd=shared, model=None, session_key=None)
    # the seat is QUIET (the ceremony refuses a live one; the refusal test re-warms it)
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen = now() - interval '1 hour' WHERE agent_id=$1",
        agent)
    slug = tmp_path / "projects" / shared.replace("/", "-")
    slug.mkdir(parents=True, exist_ok=True)
    (slug / "0ff1cee1-own-session.jsonl").write_text(f'{{"cwd": "{shared}"}}\n')
    (slug / "cafe0000-co-resident.jsonl").write_text(f'{{"cwd": "{shared}"}}\n')
    return agent


async def test_establish_office_the_whole_ceremony(
    actions: Actions, tmp_path: Path,
) -> None:
    agent = await _seat_fixture(actions, tmp_path, handle="Butler")

    out = await establish_office(
        actions, seat_or_agent=agent, actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json")

    office = tmp_path / "seats" / "butler"
    assert out["office"] == str(office)
    assert out["handle"] == "Butler"
    assert out["house"] == "butlerhouse"
    assert out["standing_orders"] == "written"
    orders = (office / "CLAUDE.md").read_text()
    assert "Butler — seat office" in orders
    assert "house **butlerhouse**" in orders
    assert "never formally declared" in orders          # no governs links yet: instruct
    assert "not yet seated" in orders                   # unbound lineage: the on-ramp note
    assert "GRADE EVERY DM" in orders                    # every seat born knowing this now
    assert (office / ".osiris").read_text().startswith('project = "butlerhouse"')
    # THE CHARTER FILE (d80621a7 piece 3): a fresh office gets a live-state scratchpad
    # beside its standing orders
    assert out["charter_file"] == "written"
    charter = (office / "charter.md").read_text()
    assert "Butler's charter" in charter
    assert "OFFLOAD TARGET when context runs high" in charter
    assert "## Current work" in charter and "## Key ids" in charter
    # the extraction rode along: his transcript moved and re-addressed, co-resident stayed
    assert out["rebind"]["harness"]["transcripts_moved"] == 1
    moved = tmp_path / "projects" / str(office).replace("/", "-")
    assert (moved / "0ff1cee1-own-session.jsonl").is_file()
    shared_slug = tmp_path / "projects" / str(tmp_path / "shared-repo").replace("/", "-")
    assert (shared_slug / "cafe0000-co-resident.jsonl").is_file()
    row = await actions.pool.fetchval(
        "SELECT cwd FROM agent_mounts WHERE agent_id=$1", agent)
    assert row == str(office)
    # THE DEED (a2d06410): the ceremony records office ownership in the GRAPH, so the
    # fourth door still opens after the seat's death takes its mount rows
    assert out["office_deed"] == "filed"
    deed = await actions.pool.fetchval(
        "SELECT d.value #>> '{}' FROM current_assertions d "
        "JOIN objects o2 ON o2.id=d.object_id "
        "WHERE d.name='office' AND o2.canonical=$1", agent)
    assert deed == str(office)

    # idempotent: the second ceremony converges, never clobbers the standing orders OR a
    # hand-edited charter — alfred's stays his, the whole point of the never-overwrite rule
    (office / "charter.md").write_text("# Butler's charter\n\nMY OWN HAND-WRITTEN NOTES.\n")
    again = await establish_office(
        actions, seat_or_agent=agent, actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json")
    assert again["standing_orders"].startswith("left in place")
    assert again["office_deed"] == "already on the lineage's record"
    assert again["charter_file"].startswith("left in place")
    assert "MY OWN HAND-WRITTEN NOTES" in (office / "charter.md").read_text()


async def test_establish_office_carries_a_declared_charter(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.charter import set_charter
    from src.orchestrator.seats import bind_holder, ensure_seat

    agent = await _seat_fixture(actions, tmp_path, handle="Butler")
    # a charter is the SEAT's (ruling 1db1ff41) — bind a real Seat, same primitive the
    # daemon's own attach ceremony uses, and pre-mint the repos (set_charter refuses any
    # name the graph has no independent evidence for)
    seat = await ensure_seat(actions, house="butlerhouse", handle="Butler", source="test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id=agent)
    for repo in ("repoA", "repoB"):
        await actions.create_or_find_object("SoftwareProject", f"repo:{repo}", "test")
    await set_charter(actions, seat["seat_id"], ["repoA", "repoB"], actor=agent)

    out = await establish_office(
        actions, seat_or_agent=agent, actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json")

    assert out["charter"] == ["repoA", "repoB"]
    orders = (tmp_path / "seats" / "butler" / "CLAUDE.md").read_text()
    assert "You govern: `repoA`, `repoB`." in orders


async def test_establish_office_refuses_the_anonymous(
    actions: Actions, tmp_path: Path,
) -> None:
    agent = await _seat_fixture(actions, tmp_path, handle=None)

    out = await establish_office(
        actions, seat_or_agent=agent, actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json")

    assert "error" in out
    assert "claim_name" in out["error"]
    assert not (tmp_path / "seats").exists()            # a refusal writes NOTHING


async def test_establish_office_refuses_the_unknown(
    actions: Actions, tmp_path: Path,
) -> None:
    out = await establish_office(
        actions, seat_or_agent="agent:00000000", actor="agent:test",
        office_root=tmp_path / "seats")
    assert "error" in out
    assert "never invents" in out["error"]


async def test_establish_office_builds_for_a_never_claimed_seat(
    actions: Actions, tmp_path: Path,
) -> None:
    """Thread 236d3940 (mirroring 3ae57d36's rebind_seat fix): a seat minted by
    mint_seat/ensure_seat but never claim_name'd by any agent (grantprobe's real shape) used
    to refuse outright here — agent resolution found nobody, and the ceremony never checked
    the Seat record directly. Calling by the seat's OWN handle must now build a full office
    off the seat alone, with no Agent object involved anywhere in this test."""
    from src.orchestrator.seats import ensure_seat

    seat = await ensure_seat(actions, house="anchorhouse", handle="Orphaned", source="test")

    out = await establish_office(
        actions, seat_or_agent="Orphaned", actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json")

    assert "error" not in out
    assert out["seat"] == seat["seat_id"]
    assert out["handle"] == "Orphaned"
    assert out["house"] == "anchorhouse"
    assert out["office_deed"].startswith("n/a")
    office = tmp_path / "seats" / "orphaned"
    assert out["office"] == str(office)
    orders = (office / "CLAUDE.md").read_text()
    assert f"durable identity `{seat['seat_id']}`" in orders
    assert "never formally declared" in orders          # no governs links: nobody to declare
    assert (office / ".osiris").read_text().startswith('project = "anchorhouse"')
    assert out["charter_file"] == "written"
    anchor = await actions.pool.fetchval(
        "SELECT a.value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.canonical=$1 AND a.name='anchor_cwd' "
        "ORDER BY a.confidence DESC, a.observed_at DESC LIMIT 1", seat["seat_id"])
    assert anchor == str(office)

    # idempotent, same guarantee as the agent-lineage path
    again = await establish_office(
        actions, seat_or_agent="Orphaned", actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json")
    assert again["standing_orders"].startswith("left in place")


async def test_establish_office_renders_peer_addendum_when_peered(
    actions: Actions, tmp_path: Path,
) -> None:
    """LEGIBILITY leg 2 (ruling d74492ee, spec e6636c7e): a peer_of bond is rendered
    into the seat's own standing orders — computed LIVE at establish_office's own call
    time (unlike mintseat.py's fresh-mint scaffold, which never has a peer yet)."""
    from src.orchestrator.seats import bind_holder, peer_seats

    agent = await _seat_fixture(actions, tmp_path, handle="Butler")
    await bind_holder(actions, seat_id="seat:butlerseat", agent_id=agent, source="test")
    await actions.assert_property(
        await actions.create_or_find_object("Seat", "seat:peerseat9", "test"), "handle",
        "Halcyon", "test", datetime.now(UTC), 0.9, evidence_class="self_declared")
    await peer_seats(actions, "seat:butlerseat", "seat:peerseat9", because="the pairing",
                     actor="test")

    out = await establish_office(
        actions, seat_or_agent=agent, actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json")

    assert out["standing_orders"] == "written"
    orders = (tmp_path / "seats" / "butler" / "CLAUDE.md").read_text()
    assert "## Peer" in orders
    assert "peered with **Halcyon** (`seat:peerseat9`)" in orders
    assert "Two-tier decisions" in orders and "Mutual hold" in orders
    # the surrounding sections are untouched — one blank line on either side, same as
    # the unpeered rendering's own spacing
    assert "## How to work from an office" in orders


async def test_establish_office_renders_no_peer_addendum_when_unpeered(
    actions: Actions, tmp_path: Path,
) -> None:
    """A SEATED but unpeered lineage gets none of the addendum — the block is gated on
    an actual peer_of edge, not merely on holding a durable seat."""
    from src.orchestrator.seats import bind_holder

    agent = await _seat_fixture(actions, tmp_path, handle="Butler")
    await bind_holder(actions, seat_id="seat:butlerseat2", agent_id=agent, source="test")

    out = await establish_office(
        actions, seat_or_agent=agent, actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json")

    assert out["standing_orders"] == "written"
    orders = (tmp_path / "seats" / "butler" / "CLAUDE.md").read_text()
    assert "## Peer" not in orders


async def test_establish_office_refuses_a_live_seat(
    actions: Actions, tmp_path: Path,
) -> None:
    """The rollout guard: extraction moves the lineage's transcripts, and a running
    harness appends to its own by path — a live seat (any generation) is never moved."""
    agent = await _seat_fixture(actions, tmp_path, handle="Butler")
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen=now() WHERE agent_id=$1", agent)

    out = await establish_office(
        actions, seat_or_agent=agent, actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json")

    assert "error" in out
    assert "LIVE right now" in out["error"]


# ═══ plan_pin_migration (ruling 719ed5b1's five-key schema — DRY RUN, never writes) ═══

async def _seat_with_office(
    actions: Actions, tmp_path: Path, *, seat_id: str, handle: str, house: str | None,
    office_dir: str, tree_dir: str | None = None,
) -> None:
    """A Seat object with the facts `roster()`/`derive_house` actually read: handle, an
    optional own `house` stamp (a HEAD, no managed_by edge — derive_house returns exactly
    this), anchor_cwd, and an optional distinct tree_cwd. Mirrors test_seats.py's own
    bind_seat_tree fixture shape rather than inventing a new one."""
    from datetime import UTC, datetime

    seat = await actions.create_or_find_object("Seat", seat_id, "test")
    await actions.assert_property(seat, "handle", handle, "test", datetime.now(UTC), 0.9)
    await actions.assert_property(seat, "anchor_cwd", office_dir, "test",
                                  datetime.now(UTC), 0.9)
    if house:
        await actions.assert_property(seat, "house", house, "test", datetime.now(UTC), 0.9)
    if tree_dir:
        from src.orchestrator.seats import bind_seat_tree
        out = await bind_seat_tree(actions, seat_id=seat_id, tree_cwd=tree_dir,
                                   actor="operator", because="test fixture")
        assert out.get("error") is None, out  # operator is in _OPERATOR_ACTORS — must succeed


async def test_plan_pin_migration_proposes_seat_house_kind_for_a_fresh_office(
    actions: Actions, tmp_path: Path,
) -> None:
    """The ordinary case: a seat with a derivable house and an unpinned office proposes all
    three new keys, none of them written (this is dry-run only)."""
    office = tmp_path / "seats" / "planalpha"
    office.mkdir(parents=True)
    await _seat_with_office(actions, tmp_path, seat_id="seat:plan0001", handle="Planalpha",
                            house="planhouse", office_dir=str(office))

    out = await plan_pin_migration(actions.pool)
    entry = next(e for e in out["plan"] if e["path"] == str(office))
    assert entry["current"] == {"house": None, "seat": None, "kind": None}
    assert entry["proposed"] == {"seat": "Planalpha", "house": "planhouse", "kind": "office"}
    assert entry["changes"] == entry["proposed"]  # nothing on disk yet — the whole diff is new
    assert entry["unknown"] == []
    assert not office.joinpath(".osiris").exists(), "plan must never write a file"


async def test_plan_pin_migration_is_idempotent_against_an_already_correct_pin(
    actions: Actions, tmp_path: Path,
) -> None:
    """A pin already carrying the right values proposes no changes — the no-churn discipline
    every existing writer keeps, proven for the read side too."""
    office = tmp_path / "seats" / "planbeta"
    office.mkdir(parents=True)
    (office / ".osiris").write_text(
        'project = "planhouse"\nhouse = "planhouse"\nseat = "Planbeta"\nkind = "office"\n')
    await _seat_with_office(actions, tmp_path, seat_id="seat:plan0002", handle="Planbeta",
                            house="planhouse", office_dir=str(office))

    out = await plan_pin_migration(actions.pool)
    assert not any(e["path"] == str(office) for e in out["plan"]), (
        "an already-correct pin must propose nothing, not even an empty no-op entry")


async def test_plan_pin_migration_reports_an_underivable_house_as_a_gap_not_a_guess(
    actions: Actions, tmp_path: Path,
) -> None:
    """A seat with no own `house` stamp and no manager to derive one from: derive_house
    honestly returns None, and the plan must name that as a gap for `house` while still
    proposing `seat`/`kind`, which do not depend on it — never silently drop the whole entry,
    never guess a house into the gap."""
    office = tmp_path / "seats" / "plangamma"
    office.mkdir(parents=True)
    await _seat_with_office(actions, tmp_path, seat_id="seat:plan0003", handle="Plangamma",
                            house=None, office_dir=str(office))

    out = await plan_pin_migration(actions.pool)
    entry = next(e for e in out["plan"] if e["path"] == str(office))
    assert entry["proposed"] == {"seat": "Plangamma", "kind": "office"}
    assert "house" not in entry["proposed"]
    assert any("house" in u for u in entry["unknown"])


async def test_plan_pin_migration_never_picks_a_seat_when_two_claim_the_same_path(
    actions: Actions, tmp_path: Path,
) -> None:
    """Two Seat objects naming the same anchor_cwd (a graph bug, not a legitimate shape):
    `seat` must never be silently resolved to either one — the whole point of the pin outranking
    inference fails the moment it can confidently state a coin-flip. house/kind still propose
    since both claimants agree on them."""
    office = tmp_path / "seats" / "plandelta"
    office.mkdir(parents=True)
    await _seat_with_office(actions, tmp_path, seat_id="seat:plan0004a", handle="Plandelta",
                            house="samehouse", office_dir=str(office))
    await _seat_with_office(actions, tmp_path, seat_id="seat:plan0004b", handle="Plandelta2",
                            house="samehouse", office_dir=str(office))

    out = await plan_pin_migration(actions.pool)
    entry = next(e for e in out["plan"] if e["path"] == str(office))
    assert "seat" not in entry["proposed"]
    assert any("conflicting claims" in u for u in entry["unknown"])
    assert entry["proposed"]["house"] == "samehouse"  # both claimants agree — still proposed


async def test_plan_pin_migration_infers_worktree_kind_from_path_shape(
    actions: Actions, tmp_path: Path,
) -> None:
    """tree_cwd distinct from anchor_cwd: kind is read from PATH SHAPE, not the graph — a
    `.claude/worktrees/` path proposes kind="worktree", never "office" (that's reserved for
    anchor_cwd alone)."""
    office = tmp_path / "seats" / "planepsilon"
    office.mkdir(parents=True)
    tree = tmp_path / ".claude" / "worktrees" / "planepsilon"
    tree.mkdir(parents=True)
    await _seat_with_office(actions, tmp_path, seat_id="seat:plan0005", handle="Planepsilon",
                            house="planhouse", office_dir=str(office), tree_dir=str(tree))

    out = await plan_pin_migration(actions.pool)
    tree_entry = next(e for e in out["plan"] if e["path"] == str(tree))
    assert tree_entry["proposed"]["kind"] == "worktree"
    office_entry = next(e for e in out["plan"] if e["path"] == str(office))
    assert office_entry["proposed"]["kind"] == "office"
    assert not (tree / ".osiris").exists()      # plan is dry-run only — nothing written


# ═══ write_pin_additions / revert_pin_write (Thoth's three constraints, msg 3929) ═══

def test_write_pin_additions_creates_a_fresh_pin(tmp_path: Path) -> None:
    office = tmp_path / "freshoffice"
    office.mkdir()
    out = write_pin_additions(str(office), {"seat": "Fresh", "house": "freshhouse",
                                            "kind": "office"})
    assert out["written"] is True
    assert out["added"] == ["house", "kind", "seat"]
    assert out["skipped"] == []
    text = (office / ".osiris").read_text()
    assert 'seat = "Fresh"' in text
    assert 'house = "freshhouse"' in text
    assert 'kind = "office"' in text
    # the backup captures the PRE-write state — no file existed, so it's empty
    assert (office / ".osiris.bak").read_text() == ""


def test_write_pin_additions_never_touches_an_existing_key_constraint_1(
    tmp_path: Path,
) -> None:
    """Constraint 1, ADDITIVE ONLY: a pin that already says `project = "Like-Us"` keeps
    saying exactly that — even when the caller's own `proposed` dict disagrees. This proves
    the writer refuses to resolve a disagreement by overwriting, not merely that it happens
    not to today."""
    office = tmp_path / "existingoffice"
    office.mkdir()
    (office / ".osiris").write_text('project = "Like-Us"\nhouse = "wronghouse"\n')

    out = write_pin_additions(str(office), {"seat": "Newcomer", "house": "correcthouse",
                                            "kind": "office"})
    assert out["written"] is True
    assert out["added"] == ["kind", "seat"]           # house was already declared — skipped
    assert out["skipped"] == ["house"]
    text = (office / ".osiris").read_text()
    assert 'house = "wronghouse"' in text             # UNTOUCHED, even though it disagrees
    assert 'project = "Like-Us"' in text               # untouched, never this writer's key
    assert 'seat = "Newcomer"' in text
    assert 'kind = "office"' in text


def test_write_pin_additions_is_idempotent_byte_identical_on_second_call(
    tmp_path: Path,
) -> None:
    """Constraint 2, PROVEN BY TEST: two calls with the same `proposed` leave the file
    byte-identical after the second, and the second call reports written=False."""
    office = tmp_path / "idempotentoffice"
    office.mkdir()
    proposed = {"seat": "Twice", "house": "twicehouse", "kind": "office"}

    first = write_pin_additions(str(office), proposed)
    assert first["written"] is True
    bytes_after_first = (office / ".osiris").read_bytes()

    second = write_pin_additions(str(office), proposed)
    assert second == {"written": False, "added": [], "skipped": ["house", "kind", "seat"],
                      "path": str(office / ".osiris")}
    bytes_after_second = (office / ".osiris").read_bytes()
    assert bytes_after_second == bytes_after_first, (
        "a second call with the same proposal must leave the file byte-identical")


def test_write_pin_additions_refuses_broken_toml(tmp_path: Path) -> None:
    office = tmp_path / "brokenoffice"
    office.mkdir()
    (office / ".osiris").write_text('project = "unterminated\n')

    out = write_pin_additions(str(office), {"seat": "Nope"})
    assert "error" in out
    assert "not valid TOML" in out["error"]
    assert not (office / ".osiris.bak").exists(), (
        "a refusal must write nothing, including no backup")


def test_write_pin_additions_appends_after_a_trailing_comment(tmp_path: Path) -> None:
    """A pin with a trailing comment (no newline convention broken) still gets its addition
    appended cleanly, and the comment survives untouched — proof this never re-serializes the
    whole file through tomllib (which would silently drop it, `_write_osiris_file`'s own
    documented limit)."""
    office = tmp_path / "commentoffice"
    office.mkdir()
    (office / ".osiris").write_text('# a hand-written note\nproject = "commented"\n')

    out = write_pin_additions(str(office), {"seat": "Commented"})
    assert out["written"] is True
    text = (office / ".osiris").read_text()
    assert "# a hand-written note" in text
    assert 'project = "commented"' in text
    assert 'seat = "Commented"' in text


def test_revert_pin_write_restores_the_pre_write_state(tmp_path: Path) -> None:
    """Constraint 3, REVERSIBLE: a revert after a write restores the exact pre-write bytes."""
    office = tmp_path / "revertoffice"
    office.mkdir()
    (office / ".osiris").write_text('project = "revertme"\n')
    original = (office / ".osiris").read_bytes()

    write_pin_additions(str(office), {"seat": "Reverted", "house": "reverthouse"})
    assert (office / ".osiris").read_bytes() != original

    out = revert_pin_write(str(office))
    assert out["reverted"] is True
    assert (office / ".osiris").read_bytes() == original


def test_revert_pin_write_deletes_a_pin_that_did_not_exist_before(tmp_path: Path) -> None:
    """A revert after a write that CREATED the file (empty backup) must delete it, not leave
    a stray empty `.osiris` behind — true absence restored, not a hollow file."""
    office = tmp_path / "createdoffice"
    office.mkdir()

    write_pin_additions(str(office), {"seat": "Created"})
    assert (office / ".osiris").exists()

    out = revert_pin_write(str(office))
    assert out["reverted"] is True
    assert not (office / ".osiris").exists()


# ═══ correct_pin_value (task #152's khepri repair — the named exception to additive-only) ═══

def test_correct_pin_value_rewrites_an_existing_key(tmp_path: Path) -> None:
    office = tmp_path / "correctoffice"
    office.mkdir()
    (office / ".osiris").write_text('project = "tony"\nseat = "khepri"\n')

    out = correct_pin_value(str(office), "project", "cultural-infrastructure",
                            reason="task #152: repo:tony was renamed, khepri's pin never followed")
    assert out["written"] is True
    assert out["old_value"] == "tony"
    assert out["new_value"] == "cultural-infrastructure"
    text = (office / ".osiris").read_text()
    assert 'project = "cultural-infrastructure"' in text
    assert 'seat = "khepri"' in text                  # untouched, a different key


def test_correct_pin_value_refuses_a_missing_key(tmp_path: Path) -> None:
    """This function corrects an EXISTING declaration only — a missing key is
    write_pin_additions' job, and blurring the two would blur their audit trails."""
    office = tmp_path / "missingkeyoffice"
    office.mkdir()
    (office / ".osiris").write_text('seat = "khepri"\n')

    out = correct_pin_value(str(office), "project", "cultural-infrastructure", reason="x")
    assert "error" in out
    assert "not declared" in out["error"]
    assert not (office / ".osiris.bak").exists()


def test_correct_pin_value_refuses_an_empty_reason(tmp_path: Path) -> None:
    """A correction with no reason is exactly the silent overwrite 719ed5b1 rules against —
    this function's entire justification for existing is auditability, so an empty reason
    is refused outright rather than accepted and hoped-for."""
    office = tmp_path / "noreasonoffice"
    office.mkdir()
    (office / ".osiris").write_text('project = "tony"\n')

    out = correct_pin_value(str(office), "project", "cultural-infrastructure", reason="   ")
    assert "error" in out
    assert "silent overwrite" in out["error"]
    text = (office / ".osiris").read_text()
    assert 'project = "tony"' in text                 # nothing written


def test_correct_pin_value_is_a_noop_when_the_value_already_matches(tmp_path: Path) -> None:
    office = tmp_path / "alreadycorrectoffice"
    office.mkdir()
    (office / ".osiris").write_text('project = "cultural-infrastructure"\n')

    out = correct_pin_value(str(office), "project", "cultural-infrastructure", reason="x")
    assert out == {"written": False, "old_value": "cultural-infrastructure",
                   "path": str(office / ".osiris")}
    assert not (office / ".osiris.bak").exists()


def test_correct_pin_value_refuses_broken_toml(tmp_path: Path) -> None:
    office = tmp_path / "brokencorrectoffice"
    office.mkdir()
    (office / ".osiris").write_text('project = "unterminated\n')

    out = correct_pin_value(str(office), "project", "fixed", reason="x")
    assert "error" in out
    assert "not valid TOML" in out["error"]
    assert not (office / ".osiris.bak").exists()


def test_correct_pin_value_preserves_other_lines_and_comments(tmp_path: Path) -> None:
    office = tmp_path / "preserveoffice"
    office.mkdir()
    (office / ".osiris").write_text(
        '# a hand-written note\nproject = "tony"\nmodel = "claude-fable-5"\n')

    out = correct_pin_value(str(office), "project", "cultural-infrastructure",
                            reason="task #152")
    assert out["written"] is True
    text = (office / ".osiris").read_text()
    assert "# a hand-written note" in text
    assert 'model = "claude-fable-5"' in text
    assert 'project = "cultural-infrastructure"' in text


def test_correct_pin_value_is_reversible_via_revert_pin_write(tmp_path: Path) -> None:
    office = tmp_path / "revertcorrectoffice"
    office.mkdir()
    (office / ".osiris").write_text('project = "tony"\n')
    original = (office / ".osiris").read_bytes()

    correct_pin_value(str(office), "project", "cultural-infrastructure", reason="x")
    assert (office / ".osiris").read_bytes() != original

    out = revert_pin_write(str(office))
    assert out["reverted"] is True
    assert (office / ".osiris").read_bytes() == original


def test_revert_pin_write_refuses_when_no_backup_exists(tmp_path: Path) -> None:
    office = tmp_path / "neverwrittenoffice"
    office.mkdir()
    out = revert_pin_write(str(office))
    assert "error" in out
    assert "no backup" in out["error"]
