"""THE OFFICE CEREMONY (ruling ed5f5ce2) — one act moves a seat into its Osiris-owned home.
alfred's office was hand-assembled; the rollout to his chartered children is one call per
seat, and these witness the call.
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.orchestrator.mounts import save_mount
from src.orchestrator.offices import (
    correct_own_pin_value,
    correct_pin_value,
    establish_office,
    plan_pin_migration,
    revert_own_pin_write,
    revert_pin_write,
    self_heal_project_pin,
    sweep_retired_office,
    sweep_seat_workspace,
    write_pin_additions,
)

NOW = datetime.now(UTC)


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
    harness appends to its own by path — a live seat (any generation) is never moved.

    Confirmed via a fake harness census (door census item 4/ninth-specimen fix, Thoth msg
    5772/5741, thread 2c3c2b9a): a bare fresh mount row is no longer sufficient by
    itself — _seat_fixture's own job_dir ("/jobs/0ff1cee1") is exactly 8 chars on purpose
    (registry_census keys agent_mounts.job_dir's basename against sessionId[:8])."""
    agent = await _seat_fixture(actions, tmp_path, handle="Butler")
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen=now() WHERE agent_id=$1", agent)

    async def _agents_json(**kw: Any) -> list[dict[str, Any]]:
        return [{"sessionId": "0ff1cee1-0000-4000-8000-000000000000", "pid": 666,
                 "cwd": "/x", "name": "[OS] Butler"}]

    out = await establish_office(
        actions, seat_or_agent=agent, actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json", agents_json=_agents_json,
        read_exe=lambda pid: "/home/x/.local/share/claude/versions/2.1.210",
        read_cwd=lambda pid: "/x")

    assert "error" in out
    assert "LIVE right now" in out["error"]


async def test_establish_office_no_longer_refuses_on_a_fresh_but_bodiless_seat(
    actions: Actions, tmp_path: Path,
) -> None:
    """THE ATLAS SHAPE ITSELF: a fresh/refreshing mount row with no harness-confirmed body
    behind it must not block the ceremony — the exact false refusal this fix closes."""
    agent = await _seat_fixture(actions, tmp_path, handle="Ghostly")
    await actions.pool.execute(
        "UPDATE agent_mounts SET last_seen=now() WHERE agent_id=$1", agent)

    async def _empty_agents_json(**kw: Any) -> list[dict[str, Any]]:
        return []

    out = await establish_office(
        actions, seat_or_agent=agent, actor="agent:test",
        office_root=tmp_path / "seats", projects_root=tmp_path / "projects",
        claude_json=tmp_path / "cj.json", agents_json=_empty_agents_json)

    assert "error" not in out


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
    # THE WRITE-BOUNDARY HONESTY RULE (decision beb046cfbdf9/42176e16, Alfred's own
    # scenario, obligation 71f637e8): `skipped` alone cannot say whether "wronghouse" was
    # already correct or was left wrong on purpose — `discarded` names it explicitly.
    assert out["discarded"] == {"house": "correcthouse"}
    text = (office / ".osiris").read_text()
    assert 'house = "wronghouse"' in text             # UNTOUCHED, even though it disagrees
    assert 'project = "Like-Us"' in text               # untouched, never this writer's key
    assert 'seat = "Newcomer"' in text
    assert 'kind = "office"' in text


def test_write_pin_additions_is_idempotent_byte_identical_on_second_call(
    tmp_path: Path,
) -> None:
    """Constraint 2, PROVEN BY TEST: two calls with the same `proposed` leave the file
    byte-identical after the second, and the second call reports written=False. A skipped
    key whose value already MATCHES `proposed` earns no `discarded` entry — a genuine
    no-op, not a disagreement (decision beb046cfbdf9/42176e16's own discriminator)."""
    office = tmp_path / "idempotentoffice"
    office.mkdir()
    proposed = {"seat": "Twice", "house": "twicehouse", "kind": "office"}

    first = write_pin_additions(str(office), proposed)
    assert first["written"] is True
    bytes_after_first = (office / ".osiris").read_bytes()

    second = write_pin_additions(str(office), proposed)
    assert second == {"written": False, "added": [], "skipped": ["house", "kind", "seat"],
                      "discarded": {}, "path": str(office / ".osiris")}
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


# ═══ correct_own_pin_value — the self-scoped MCP door onto correct_pin_value (msg 4761,
# obligation 114f7ac9): a caller names WHAT to correct, never WHERE — resolved off held_seat,
# never identity.cwd or a directory-basename guess. ═══

async def test_correct_own_pin_value_resolves_the_callers_own_office(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.agents import claim_name

    claimed = await claim_name(actions, "agent:cov1owner", "CovOwner", source="test")
    assert claimed.get("error") is None
    office = tmp_path / "covowner"
    office.mkdir()
    (office / ".osiris").write_text('project = "tony"\n')

    out = await correct_own_pin_value(
        actions.pool, "agent:cov1owner", "project", "cultural-infrastructure",
        reason="task #152", office_root=tmp_path)
    assert out["written"] is True
    assert out["seat_id"] == claimed["seat_id"]
    text = (office / ".osiris").read_text()
    assert 'project = "cultural-infrastructure"' in text


async def test_correct_own_pin_value_refuses_a_caller_with_no_seat(
    actions: Actions, tmp_path: Path,
) -> None:
    out = await correct_own_pin_value(
        actions.pool, "agent:cov2unseated", "project", "x", reason="x", office_root=tmp_path)
    assert "holds no seat" in out["error"]


async def test_correct_own_pin_value_never_touches_a_different_seats_office(
    actions: Actions, tmp_path: Path,
) -> None:
    """The constraint that matters most: this door must not become a path traversal onto
    another seat's pin just because a caller happens to know its handle."""
    from src.orchestrator.agents import claim_name

    await claim_name(actions, "agent:cov3self0", "CovSelf", source="test")
    other_office = tmp_path / "covother"
    other_office.mkdir()
    (other_office / ".osiris").write_text('project = "someone-elses"\n')
    self_office = tmp_path / "covself"
    self_office.mkdir()
    (self_office / ".osiris").write_text('project = "tony"\n')

    out = await correct_own_pin_value(
        actions.pool, "agent:cov3self0", "project", "cultural-infrastructure",
        reason="x", office_root=tmp_path)
    assert out["written"] is True
    assert (self_office / ".osiris").read_text() == 'project = "cultural-infrastructure"\n'
    assert (other_office / ".osiris").read_text() == 'project = "someone-elses"\n'  # untouched


async def test_correct_own_pin_value_propagates_an_empty_reason_refusal(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.agents import claim_name

    await claim_name(actions, "agent:cov4noreas", "CovNoreas", source="test")
    office = tmp_path / "covnoreas"
    office.mkdir()
    (office / ".osiris").write_text('project = "tony"\n')

    out = await correct_own_pin_value(
        actions.pool, "agent:cov4noreas", "project", "cultural-infrastructure",
        reason="  ", office_root=tmp_path)
    assert "silent overwrite" in out["error"]
    assert (office / ".osiris").read_text() == 'project = "tony"\n'  # nothing written


# ═══ THE SECOND COPY (ruling b30e2b38, the Jesus/Godel live specimen): correct_own_pin_
# value ALSO reaches the seat's own anchor_cwd pin, never a caller-supplied path. ═══

async def test_correct_own_pin_value_also_corrects_the_anchor_copy(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.agents import claim_name

    claimed = await claim_name(actions, "agent:cov5anchor", "CovAnchor", source="test")
    office = tmp_path / "covanchor"
    office.mkdir()
    (office / ".osiris").write_text('project = "Jesus"\n')
    anchor = tmp_path / "REPOS" / "Godel"
    anchor.mkdir(parents=True)
    (anchor / ".osiris").write_text('project = "Jesus"\n')
    seat_oid = await actions.create_or_find_object("Seat", claimed["seat_id"], "test")
    await actions.assert_property(seat_oid, "anchor_cwd", str(anchor), "test", NOW, 0.9)

    out = await correct_own_pin_value(
        actions.pool, "agent:cov5anchor", "project", "Godel",
        reason="fold+rebind complete", office_root=tmp_path)
    assert out["written"] is True
    assert (office / ".osiris").read_text() == 'project = "Godel"\n'
    assert out["anchor"]["corrected"] is True
    assert (anchor / ".osiris").read_text() == 'project = "Godel"\n'


async def test_correct_own_pin_value_skips_the_anchor_when_it_is_the_office_itself(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.agents import claim_name

    claimed = await claim_name(actions, "agent:cov6same", "CovSame", source="test")
    office = tmp_path / "covsame"
    office.mkdir()
    (office / ".osiris").write_text('project = "tony"\n')
    seat_oid = await actions.create_or_find_object("Seat", claimed["seat_id"], "test")
    await actions.assert_property(seat_oid, "anchor_cwd", str(office), "test", NOW, 0.9)

    out = await correct_own_pin_value(
        actions.pool, "agent:cov6same", "project", "cultural-infrastructure",
        reason="x", office_root=tmp_path)
    assert out["written"] is True
    assert "anchor" not in out


async def test_correct_own_pin_value_skips_an_anchor_with_no_pin_of_its_own(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.agents import claim_name

    claimed = await claim_name(actions, "agent:cov7nopin", "CovNopin", source="test")
    office = tmp_path / "covnopin"
    office.mkdir()
    (office / ".osiris").write_text('project = "tony"\n')
    anchor = tmp_path / "bare-anchor"
    anchor.mkdir()  # no .osiris here at all
    seat_oid = await actions.create_or_find_object("Seat", claimed["seat_id"], "test")
    await actions.assert_property(seat_oid, "anchor_cwd", str(anchor), "test", NOW, 0.9)

    out = await correct_own_pin_value(
        actions.pool, "agent:cov7nopin", "project", "cultural-infrastructure",
        reason="x", office_root=tmp_path)
    assert out["written"] is True
    assert "anchor" not in out


# ═══ the THIRD copy — the ~/code/<handle> workspace convention (thread 6483/6504,
# decision 87457dc1: a real, mint-scaffolded location, not an undeclared scratch dir). ═══

async def test_correct_own_pin_value_also_corrects_the_workspace_copy(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.agents import claim_name

    await claim_name(actions, "agent:cov8work", "CovWork", source="test")
    office = tmp_path / "office" / "covwork"
    office.mkdir(parents=True)
    (office / ".osiris").write_text('project = "Jesus"\n')
    workspace = tmp_path / "workspace" / "covwork"
    workspace.mkdir(parents=True)
    (workspace / ".osiris").write_text('project = "Jesus"\n')

    out = await correct_own_pin_value(
        actions.pool, "agent:cov8work", "project", "Godel", reason="third copy",
        office_root=tmp_path / "office", workspace_root=tmp_path / "workspace")
    assert out["written"] is True
    assert (office / ".osiris").read_text() == 'project = "Godel"\n'
    assert out["workspace"]["corrected"] is True
    assert (workspace / ".osiris").read_text() == 'project = "Godel"\n'


async def test_correct_own_pin_value_corrects_anchor_and_workspace_together(
    actions: Actions, tmp_path: Path,
) -> None:
    """All three copies, genuinely distinct — the live jesus/chad shape once the anchor
    fix alone (b30e2b38) is not enough because anchor_cwd points at the office itself."""
    from src.orchestrator.agents import claim_name

    claimed = await claim_name(actions, "agent:cov9three", "CovThree", source="test")
    office = tmp_path / "office" / "covthree"
    office.mkdir(parents=True)
    (office / ".osiris").write_text('project = "Jesus"\n')
    anchor = tmp_path / "REPOS" / "Godel3"
    anchor.mkdir(parents=True)
    (anchor / ".osiris").write_text('project = "Jesus"\n')
    workspace = tmp_path / "workspace" / "covthree"
    workspace.mkdir(parents=True)
    (workspace / ".osiris").write_text('project = "Jesus"\n')
    seat_oid = await actions.create_or_find_object("Seat", claimed["seat_id"], "test")
    await actions.assert_property(seat_oid, "anchor_cwd", str(anchor), "test", NOW, 0.9)

    out = await correct_own_pin_value(
        actions.pool, "agent:cov9three", "project", "Godel", reason="all three",
        office_root=tmp_path / "office", workspace_root=tmp_path / "workspace")
    assert (office / ".osiris").read_text() == 'project = "Godel"\n'
    assert out["anchor"]["corrected"] is True
    assert (anchor / ".osiris").read_text() == 'project = "Godel"\n'
    assert out["workspace"]["corrected"] is True
    assert (workspace / ".osiris").read_text() == 'project = "Godel"\n'


async def test_correct_own_pin_value_skips_the_workspace_when_it_equals_the_anchor(
    actions: Actions, tmp_path: Path,
) -> None:
    """Jesus's own live shape (msg 6374): anchor_cwd WAS rebound to the workspace
    directory itself — the anchor branch already corrects it, so the workspace branch
    must not double-write (and must not error) on the same file."""
    from src.orchestrator.agents import claim_name

    claimed = await claim_name(actions, "agent:cov10same", "Cov10same", source="test")
    office = tmp_path / "office" / "cov10same"
    office.mkdir(parents=True)
    (office / ".osiris").write_text('project = "Jesus"\n')
    workspace = tmp_path / "workspace" / "cov10same"
    workspace.mkdir(parents=True)
    (workspace / ".osiris").write_text('project = "Jesus"\n')
    seat_oid = await actions.create_or_find_object("Seat", claimed["seat_id"], "test")
    await actions.assert_property(seat_oid, "anchor_cwd", str(workspace), "test", NOW, 0.9)

    out = await correct_own_pin_value(
        actions.pool, "agent:cov10same", "project", "Godel", reason="anchor==workspace",
        office_root=tmp_path / "office", workspace_root=tmp_path / "workspace")
    assert out["anchor"]["corrected"] is True
    assert "workspace" not in out
    assert (workspace / ".osiris").read_text() == 'project = "Godel"\n'  # still fixed, once


async def test_correct_own_pin_value_skips_a_workspace_with_no_pin_of_its_own(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.agents import claim_name

    await claim_name(actions, "agent:cov11noworkpin", "Cov11noworkpin", source="test")
    office = tmp_path / "office" / "cov11noworkpin"
    office.mkdir(parents=True)
    (office / ".osiris").write_text('project = "tony"\n')
    workspace = tmp_path / "workspace" / "cov11noworkpin"
    workspace.mkdir(parents=True)  # no .osiris here at all

    out = await correct_own_pin_value(
        actions.pool, "agent:cov11noworkpin", "project", "cultural-infrastructure",
        reason="x", office_root=tmp_path / "office", workspace_root=tmp_path / "workspace")
    assert out["written"] is True
    assert "workspace" not in out


# ═══ revert_own_pin_write — the self-scoped door onto revert_pin_write (ruling b30e2b38:
# a seat that followed the rules into a bad pin state had no sanctioned way back out). ═══

async def test_revert_own_pin_write_restores_the_office(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.agents import claim_name

    await claim_name(actions, "agent:rov1self", "RovSelf", source="test")
    office = tmp_path / "rovself"
    office.mkdir()
    (office / ".osiris").write_text('project = "tony"\n')
    await correct_own_pin_value(
        actions.pool, "agent:rov1self", "project", "wrong-value",
        reason="x", office_root=tmp_path)
    assert (office / ".osiris").read_text() == 'project = "wrong-value"\n'

    out = await revert_own_pin_write(actions.pool, "agent:rov1self", office_root=tmp_path)
    assert out["office"]["reverted"] is True
    assert (office / ".osiris").read_text() == 'project = "tony"\n'


async def test_revert_own_pin_write_refuses_a_caller_with_no_seat(
    actions: Actions, tmp_path: Path,
) -> None:
    out = await revert_own_pin_write(actions.pool, "agent:rov2unseated", office_root=tmp_path)
    assert "holds no seat" in out["error"]


async def test_revert_own_pin_write_also_reverts_the_anchor_copy(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.agents import claim_name

    claimed = await claim_name(actions, "agent:rov3anchor", "RovAnchor", source="test")
    office = tmp_path / "rovanchor"
    office.mkdir()
    (office / ".osiris").write_text('project = "Jesus"\n')
    anchor = tmp_path / "REPOS" / "Godel2"
    anchor.mkdir(parents=True)
    (anchor / ".osiris").write_text('project = "Jesus"\n')
    seat_oid = await actions.create_or_find_object("Seat", claimed["seat_id"], "test")
    await actions.assert_property(seat_oid, "anchor_cwd", str(anchor), "test", NOW, 0.9)
    await correct_own_pin_value(
        actions.pool, "agent:rov3anchor", "project", "Godel",
        reason="fold+rebind complete", office_root=tmp_path)

    out = await revert_own_pin_write(actions.pool, "agent:rov3anchor", office_root=tmp_path)
    assert out["office"]["reverted"] is True
    assert (office / ".osiris").read_text() == 'project = "Jesus"\n'
    assert out["anchor"]["reverted"] is True
    assert (anchor / ".osiris").read_text() == 'project = "Jesus"\n'


async def test_revert_own_pin_write_skips_anchor_with_no_backup(
    actions: Actions, tmp_path: Path,
) -> None:
    """The office was corrected while no anchor_cwd was on record at all — so the
    correction never reached a second copy, and no backup exists there. A revert must
    never invent one or error on its absence, even once an anchor_cwd shows up later."""
    from src.orchestrator.agents import claim_name

    claimed = await claim_name(actions, "agent:rov4noanchor", "RovNoanchor", source="test")
    office = tmp_path / "rovnoanchor"
    office.mkdir()
    (office / ".osiris").write_text('project = "tony"\n')
    await correct_own_pin_value(
        actions.pool, "agent:rov4noanchor", "project", "cultural-infrastructure",
        reason="x", office_root=tmp_path)  # no anchor_cwd on record yet — office only
    anchor = tmp_path / "some-other-tree"
    anchor.mkdir()
    (anchor / ".osiris").write_text('project = "unrelated"\n')  # never corrected
    seat_oid = await actions.create_or_find_object("Seat", claimed["seat_id"], "test")
    await actions.assert_property(seat_oid, "anchor_cwd", str(anchor), "test", NOW, 0.9)

    out = await revert_own_pin_write(actions.pool, "agent:rov4noanchor", office_root=tmp_path)
    assert out["office"]["reverted"] is True
    assert "anchor" not in out
    assert (anchor / ".osiris").read_text() == 'project = "unrelated"\n'  # untouched


async def test_revert_own_pin_write_also_reverts_the_workspace_copy(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.agents import claim_name

    await claim_name(actions, "agent:rov5work", "Rov5work", source="test")
    office = tmp_path / "office" / "rov5work"
    office.mkdir(parents=True)
    (office / ".osiris").write_text('project = "Jesus"\n')
    workspace = tmp_path / "workspace" / "rov5work"
    workspace.mkdir(parents=True)
    (workspace / ".osiris").write_text('project = "Jesus"\n')
    await correct_own_pin_value(
        actions.pool, "agent:rov5work", "project", "Godel", reason="third copy",
        office_root=tmp_path / "office", workspace_root=tmp_path / "workspace")

    out = await revert_own_pin_write(
        actions.pool, "agent:rov5work",
        office_root=tmp_path / "office", workspace_root=tmp_path / "workspace")
    assert out["office"]["reverted"] is True
    assert (office / ".osiris").read_text() == 'project = "Jesus"\n'
    assert out["workspace"]["reverted"] is True
    assert (workspace / ".osiris").read_text() == 'project = "Jesus"\n'


async def test_revert_own_pin_write_skips_workspace_with_no_backup(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.agents import claim_name

    await claim_name(actions, "agent:rov6noworkbak", "Rov6noworkbak", source="test")
    office = tmp_path / "office" / "rov6noworkbak"
    office.mkdir(parents=True)
    (office / ".osiris").write_text('project = "tony"\n')
    await correct_own_pin_value(
        actions.pool, "agent:rov6noworkbak", "project", "cultural-infrastructure",
        reason="x", office_root=tmp_path / "office",
        workspace_root=tmp_path / "no-workspace-here")  # workspace dir never existed

    out = await revert_own_pin_write(
        actions.pool, "agent:rov6noworkbak",
        office_root=tmp_path / "office", workspace_root=tmp_path / "no-workspace-here")
    assert out["office"]["reverted"] is True
    assert "workspace" not in out


async def test_correct_own_pin_value_propagates_a_missing_key_refusal(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.agents import claim_name

    await claim_name(actions, "agent:cov5nokey", "CovNokey", source="test")
    office = tmp_path / "covnokey"
    office.mkdir()
    (office / ".osiris").write_text('seat = "covnokey"\n')

    out = await correct_own_pin_value(
        actions.pool, "agent:cov5nokey", "project", "x", reason="x", office_root=tmp_path)
    assert "not declared" in out["error"]


# ═══ _pin_backup_path (obligation 27ae4f89) — a REPO-side pin's backup must never land
# inside the tracked git working tree; a SEAT-OFFICE pin's backup is unaffected. ═══

def test_pin_backup_stays_beside_the_file_for_a_plain_office(tmp_path: Path) -> None:
    """No .git anywhere near the pin (the seat-office case, ruling ed5f5ce2) — unchanged
    behavior, backup lands right next to the pin, same as before this fix."""
    office = tmp_path / "plainoffice"
    office.mkdir()
    (office / ".osiris").write_text('seat = "plain"\n')

    write_pin_additions(str(office), {"project": "cultural-infrastructure"})
    assert (office / ".osiris.bak").exists()
    assert not (office / ".git").exists()


def test_pin_backup_goes_inside_dot_git_for_a_real_repo_root(tmp_path: Path) -> None:
    """A REPO pin sitting inside an ordinary (non-worktree) git repo root: the backup must
    land inside .git/, never beside the tracked .osiris file."""
    repo = tmp_path / "somerepo"
    (repo / ".git").mkdir(parents=True)
    (repo / ".osiris").write_text('project = "tony"\n')

    out = correct_pin_value(str(repo), "project", "cultural-infrastructure", reason="x")
    assert out["written"] is True
    assert not (repo / ".osiris.bak").exists()          # never beside the tracked file
    backup = repo / ".git" / "osiris-pin.bak"
    assert backup.is_file()
    assert backup.read_text() == 'project = "tony"\n'

    reverted = revert_pin_write(str(repo))
    assert reverted["reverted"] is True
    assert (repo / ".osiris").read_text() == 'project = "tony"\n'


def test_pin_backup_resolves_a_worktree_gitlink_to_its_own_private_gitdir(
    tmp_path: Path,
) -> None:
    """A worktree checkout's `.git` is a FILE (a gitlink: "gitdir: <real path>"), not a
    directory — the backup must resolve through it to the worktree's own private gitdir,
    never fail, and never land beside the tracked .osiris file either."""
    real_gitdir = tmp_path / "mainrepo" / ".git" / "worktrees" / "wt1"
    real_gitdir.mkdir(parents=True)
    worktree = tmp_path / "wt1-checkout"
    worktree.mkdir()
    (worktree / ".git").write_text(f"gitdir: {real_gitdir}\n")
    (worktree / ".osiris").write_text('project = "tony"\n')

    out = correct_pin_value(str(worktree), "project", "cultural-infrastructure", reason="x")
    assert out["written"] is True
    assert not (worktree / ".osiris.bak").exists()
    backup = real_gitdir / "osiris-pin.bak"
    assert backup.is_file()
    assert backup.read_text() == 'project = "tony"\n'

    reverted = revert_pin_write(str(worktree))
    assert reverted["reverted"] is True
    assert (worktree / ".osiris").read_text() == 'project = "tony"\n'


# ═══ SELF-HEAL: PIN `project` UNSET IS A VALID STATE (ruling fe8ec7ff, operator df646654)
# governs + works_in + anchor_cwd must ALL agree before a seat's own mount writes anything;
# any one absent or disagreeing leaves the pin unset, valid, with a reason. ═══

async def _seat_with_project(
    actions: Actions, *, agent: str, handle: str, project: str, charter: bool, works_in: bool,
) -> str:
    from src.orchestrator.agents import claim_name

    claimed = await claim_name(actions, agent, handle, source="test")
    seat_id = claimed["seat_id"]
    now = datetime.now(UTC)
    proj = await actions.create_or_find_object("SoftwareProject", f"repo:{project}", "test")
    if charter:
        from src.orchestrator.charter import set_charter
        await set_charter(actions, seat_id, [project], actor=agent)
    if works_in:
        agent_oid = await actions.create_or_find_object("Agent", agent, agent)
        await actions.create_link(agent_oid, proj, "works_in", agent, now, 0.9)
    return seat_id


async def test_self_heal_writes_when_all_three_signals_agree(
    actions: Actions, tmp_path: Path,
) -> None:
    office = tmp_path / "heal1"
    office.mkdir()
    real = office / "dealer-to-fb"
    real.mkdir()  # anchor_cwd's own basename IS the project name — the Marquee shape
    (real / ".osiris").write_text('model = "claude-sonnet-5"\n')
    await _seat_with_project(
        actions, agent="agent:heal1", handle="Heal1", project="dealer-to-fb",
        charter=True, works_in=True)

    out = await self_heal_project_pin(actions.pool, "agent:heal1", str(real))
    assert out["state"] == "self-healed"
    assert out["project"] == "dealer-to-fb"
    assert 'project = "dealer-to-fb"' in (real / ".osiris").read_text()
    assert 'model = "claude-sonnet-5"' in (real / ".osiris").read_text()  # additive, untouched


async def test_self_heal_leaves_unset_when_governs_and_works_in_disagree(
    actions: Actions, tmp_path: Path,
) -> None:
    office = tmp_path / "amb2"
    office.mkdir()
    real = office / "dealer-to-fb"
    real.mkdir()
    await actions.create_or_find_object("SoftwareProject", "repo:some-other-project", "test")
    await _seat_with_project(
        actions, agent="agent:amb2", handle="Amb2", project="dealer-to-fb",
        charter=True, works_in=False)
    # works_in points somewhere ELSE — the two signals disagree
    agent_oid = await actions.create_or_find_object("Agent", "agent:amb2", "agent:amb2")
    other = await actions.create_or_find_object("SoftwareProject", "repo:some-other-project",
                                                 "test")
    await actions.create_link(agent_oid, other, "works_in", "agent:amb2",
                              datetime.now(UTC), 0.9)

    out = await self_heal_project_pin(actions.pool, "agent:amb2", str(real))
    assert out["state"] == "unset"
    assert "not all three agree" in out["reason"]
    assert not (real / ".osiris").exists()


async def test_self_heal_leaves_unset_with_no_seat_held(
    actions: Actions, tmp_path: Path,
) -> None:
    office = tmp_path / "noseat3"
    office.mkdir()
    out = await self_heal_project_pin(actions.pool, "agent:noseat3-unclaimed", str(office))
    assert out["state"] == "unset"
    assert "no seat held" in out["reason"]


async def test_self_heal_is_a_noop_when_project_already_declared(
    actions: Actions, tmp_path: Path,
) -> None:
    office = tmp_path / "already4"
    office.mkdir()
    (office / ".osiris").write_text('project = "tony"\n')
    out = await self_heal_project_pin(actions.pool, "agent:already4", str(office))
    assert out == {"state": "n/a"}


# ═══ sweep_retired_office — the missing disk half (Thoth's msg 6026/6035 lane).
# DRY-RUN ONLY this pass: execute stays deliberately unwired until Thoth reviews real
# dry-run output; the guard is registry_census read TWICE (now, and after a heal-interval
# wait), never a single instant's read — wave6probe's own lesson. ═══

def _sweep_agents_json(rows: list[dict]) -> object:
    async def _f() -> list[dict]:
        return rows
    return _f


async def _instant_sleep(_secs: float) -> None:
    pass


_LIVE_EXE = "/home/x/.local/share/claude/versions/2.1.210"


async def test_sweep_execute_refuses_without_because(actions: Actions, tmp_path: Path) -> None:
    office = tmp_path / "someoffice"
    office.mkdir()
    out = await sweep_retired_office(
        actions.pool, handle="someoffice", dry_run=False, office_root=tmp_path,
        sleep=_instant_sleep)
    assert "because is required" in out["error"]
    assert office.is_dir()  # refused before ever touching the filesystem


async def test_sweep_execute_deletes_a_stranger_office_with_no_seat_row_at_all(
    actions: Actions, tmp_path: Path,
) -> None:
    office = tmp_path / "climintworker1"
    office.mkdir()
    (office / ".osiris").write_text('project = "cliproj1"\n')
    (office / "CLAUDE.md").write_text("orders\n")
    out = await sweep_retired_office(
        actions.pool, handle="climintworker1", dry_run=False, because="test cleanup",
        office_root=tmp_path, agents_json=_sweep_agents_json([]), sleep=_instant_sleep)
    assert out["status"] == "deleted"
    assert out["dry_run"] is False
    assert out["seat"] is None
    assert set(out["entries"]) == {".osiris", "CLAUDE.md"}
    assert not office.exists()


async def test_sweep_execute_deletes_a_retired_seats_office(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.seats import ensure_seat, retire_seat

    seat = await ensure_seat(actions, house="sweephouse", handle="SweepExec1",
                             source="test")
    await retire_seat(actions, seat["seat_id"], reason="role is over", actor="test")
    office = tmp_path / "sweepexec1"
    office.mkdir()
    (office / "charter.md").write_text("charter\n")
    out = await sweep_retired_office(
        actions.pool, handle="SweepExec1", dry_run=False, because="test cleanup",
        office_root=tmp_path, agents_json=_sweep_agents_json([]), sleep=_instant_sleep)
    assert out["status"] == "deleted"
    assert out["seat"] == seat["seat_id"]
    assert not office.exists()


async def test_sweep_execute_leaves_an_active_seats_office_untouched(
    actions: Actions, tmp_path: Path,
) -> None:
    """The guard is IDENTICAL in execute mode — a refusal deletes nothing, exactly as
    dry-run would have predicted."""
    from src.orchestrator.seats import ensure_seat

    await ensure_seat(actions, house="sweephouse", handle="SweepExecActive1", source="test")
    office = tmp_path / "sweepexecactive1"
    office.mkdir()
    out = await sweep_retired_office(
        actions.pool, handle="SweepExecActive1", dry_run=False, because="test cleanup",
        office_root=tmp_path, agents_json=_sweep_agents_json([]), sleep=_instant_sleep)
    assert "status='active'" in out["error"]
    assert office.is_dir()


async def test_sweep_refuses_no_office_directory(actions: Actions, tmp_path: Path) -> None:
    out = await sweep_retired_office(
        actions.pool, handle="nosuchoffice", office_root=tmp_path, sleep=_instant_sleep)
    assert "nothing to sweep" in out["error"]


async def test_sweep_would_delete_a_stranger_office_with_no_seat_row_at_all(
    actions: Actions, tmp_path: Path,
) -> None:
    """The climintworker1/inferredworker1 shape exactly: a real office directory, no
    matching Seat object at any status — pure test-run filesystem debris."""
    office = tmp_path / "climintworker1"
    office.mkdir()
    (office / ".osiris").write_text('project = "cliproj1"\n')
    (office / "CLAUDE.md").write_text("orders\n")
    out = await sweep_retired_office(
        actions.pool, handle="climintworker1", office_root=tmp_path,
        agents_json=_sweep_agents_json([]), sleep=_instant_sleep)
    assert out["status"] == "would-delete"
    assert out["seat"] is None
    assert out["dry_run"] is True
    assert set(out["entries"]) == {".osiris", "CLAUDE.md"}


async def test_sweep_would_delete_a_retired_seats_office(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.seats import ensure_seat, retire_seat

    seat = await ensure_seat(actions, house="sweephouse", handle="SweepRetired1",
                             source="test")
    await retire_seat(actions, seat["seat_id"], reason="role is over", actor="test")
    office = tmp_path / "sweepretired1"
    office.mkdir()
    (office / "charter.md").write_text("charter\n")
    out = await sweep_retired_office(
        actions.pool, handle="SweepRetired1", office_root=tmp_path,
        agents_json=_sweep_agents_json([]), sleep=_instant_sleep)
    assert out["status"] == "would-delete"
    assert out["seat"] == seat["seat_id"]
    assert out["seat_status"] == "retired"
    assert out["entries"] == ["charter.md"]


async def test_sweep_refuses_an_active_seats_office(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.seats import ensure_seat

    await ensure_seat(actions, house="sweephouse", handle="SweepActive1", source="test")
    office = tmp_path / "sweepactive1"
    office.mkdir()
    out = await sweep_retired_office(
        actions.pool, handle="SweepActive1", office_root=tmp_path,
        agents_json=_sweep_agents_json([]), sleep=_instant_sleep)
    assert "status='active'" in out["error"]
    assert out["seat_status"] == "active"


async def test_sweep_refuses_ambiguous_multiple_seats_sharing_a_handle(
    actions: Actions, tmp_path: Path,
) -> None:
    now = datetime.now(UTC)
    for canon in ("seat:sweeptwin1", "seat:sweeptwin2"):
        oid = await actions.create_or_find_object("Seat", canon, "test")
        await actions.assert_property(oid, "handle", "SweepTwin", "test", now, 0.9,
                                      evidence_class="self_declared")
    office = tmp_path / "sweeptwin"
    office.mkdir()
    out = await sweep_retired_office(
        actions.pool, handle="SweepTwin", office_root=tmp_path,
        agents_json=_sweep_agents_json([]), sleep=_instant_sleep)
    assert "ambiguous" in out["error"]
    assert len(out["seats"]) == 2


async def test_sweep_refuses_a_retired_seat_with_a_stray_active_holder(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.seats import bind_holder, ensure_seat

    seat = await ensure_seat(actions, house="sweephouse", handle="SweepStray1",
                             source="test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:sweepstray1",
                      source="test")
    # force the graph-should-never-have shape: retired, but the holds link never cleared
    await actions.pool.execute(
        "UPDATE objects SET status='retired' WHERE canonical=$1", seat["seat_id"])
    office = tmp_path / "sweepstray1"
    office.mkdir()
    out = await sweep_retired_office(
        actions.pool, handle="SweepStray1", office_root=tmp_path,
        agents_json=_sweep_agents_json([]), sleep=_instant_sleep)
    assert "active holder" in out["error"]


async def test_sweep_refuses_a_live_body_at_the_office_right_now(
    actions: Actions, tmp_path: Path,
) -> None:
    office = tmp_path / "sweeplive1"
    office.mkdir()
    out = await sweep_retired_office(
        actions.pool, handle="sweeplive1", office_root=tmp_path,
        agents_json=_sweep_agents_json(
            [{"sessionId": "sweeplive-1111-2222-3333-444444444444", "pid": 555,
              "cwd": str(office)}]),
        read_exe=lambda pid: _LIVE_EXE, read_cwd=lambda pid: str(office),
        sleep=_instant_sleep)
    assert out["status"] == "refused-live-body"
    assert "right now" in out["detail"]


async def test_sweep_refuses_a_live_body_that_appears_after_the_heal_wait(
    actions: Actions, tmp_path: Path,
) -> None:
    """The exact wave6probe race: clean at the first read, a body is live by the second —
    the daemon's own auto-respawn window. The guard must catch it on the SECOND read."""
    office = tmp_path / "sweepheal1"
    office.mkdir()
    calls = {"n": 0}

    async def _flaky_agents_json() -> list[dict]:
        calls["n"] += 1
        if calls["n"] == 1:
            return []
        return [{"sessionId": "sweepheal-1111-2222-3333-444444444444", "pid": 666,
                 "cwd": str(office)}]

    out = await sweep_retired_office(
        actions.pool, handle="sweepheal1", office_root=tmp_path,
        agents_json=_flaky_agents_json,
        read_exe=lambda pid: _LIVE_EXE, read_cwd=lambda pid: str(office),
        sleep=_instant_sleep)
    assert out["status"] == "refused-live-body-after-heal-wait"
    assert "daemon-respawn race" in out["detail"]


async def test_sweep_refuses_on_a_blind_census_never_reading_silence_as_empty(
    actions: Actions, tmp_path: Path,
) -> None:
    office = tmp_path / "sweepblind1"
    office.mkdir()

    async def _raises() -> list[dict]:
        raise TimeoutError("harness read timed out")

    out = await sweep_retired_office(
        actions.pool, handle="sweepblind1", office_root=tmp_path,
        agents_json=_raises, sleep=_instant_sleep)
    assert out["status"] == "refused-live-body"
    assert "blind census" in out["detail"]


# ═══ Path containment. Every guard above interrogates THE SEAT; this one interrogates THE
# PATH, and it is the guard the execute path shipped without (Thoth LXXXIX, wave 8 merge
# review). `handle` is caller-supplied and lands in a `/` join, so a traversal names a real
# directory outside the office root that matches no Seat, holds no holder and hosts no live
# body — clearing all five seat guards on its way to shutil.rmtree. ═══

async def test_sweep_refuses_a_handle_that_traverses_out_of_the_office_root(
    actions: Actions, tmp_path: Path,
) -> None:
    """The traversal must be refused BEFORE any seat lookup, and the outside directory must
    survive intact. Built so it would really have been deleted without the containment
    check: no Seat row, no holder, no live body — every other guard passes."""
    root = tmp_path / "seats"
    root.mkdir()
    outside = tmp_path / "notanoffice"
    outside.mkdir()
    (outside / "precious.txt").write_text("real work that is not an office\n")

    out = await sweep_retired_office(
        actions.pool, handle="../notanoffice", dry_run=False, because="traversal attempt",
        office_root=root, agents_json=_sweep_agents_json([]), sleep=_instant_sleep)

    assert "does not name an office directly under" in out["error"]
    assert outside.is_dir()
    assert (outside / "precious.txt").read_text() == "real work that is not an office\n"


async def test_sweep_refuses_a_traversal_in_dry_run_too(
    actions: Actions, tmp_path: Path,
) -> None:
    """Dry-run must refuse on the SAME line, not report would-delete over someone else's
    directory — the two modes stay trustworthy only while dry-run predicts execute exactly."""
    root = tmp_path / "seats"
    root.mkdir()
    outside = tmp_path / "notanoffice"
    outside.mkdir()

    out = await sweep_retired_office(
        actions.pool, handle="../notanoffice", office_root=root,
        agents_json=_sweep_agents_json([]), sleep=_instant_sleep)

    assert "does not name an office directly under" in out["error"]
    assert "status" not in out


async def test_sweep_refuses_a_nested_handle_even_inside_the_root(
    actions: Actions, tmp_path: Path,
) -> None:
    """An office is a DIRECT child of the root. A nested path stays inside the root and is
    still not an office — containment alone would admit it, so the check is on the parent."""
    root = tmp_path / "seats"
    (root / "realoffice" / "subdir").mkdir(parents=True)

    out = await sweep_retired_office(
        actions.pool, handle="realoffice/subdir", dry_run=False, because="nested attempt",
        office_root=root, agents_json=_sweep_agents_json([]), sleep=_instant_sleep)

    assert "does not name an office directly under" in out["error"]
    assert (root / "realoffice" / "subdir").is_dir()


async def test_sweep_still_accepts_an_ordinary_handle(
    actions: Actions, tmp_path: Path,
) -> None:
    """The negative control: containment must not have broken the real path."""
    root = tmp_path / "seats"
    office = root / "plainoffice1"
    office.mkdir(parents=True)
    (office / ".osiris").write_text('project = "p"\n')

    out = await sweep_retired_office(
        actions.pool, handle="plainoffice1", office_root=root,
        agents_json=_sweep_agents_json([]), sleep=_instant_sleep)

    assert out["status"] == "would-delete"
    assert out["entries"] == [".osiris"]


# ═══ sweep_seat_workspace — the OTHER disk half (thread 6272): mint_seat/found_seat
# scaffold a workspace (~/code/<handle>/) alongside the office, sweep_retired_office never
# touched it. Same guard shape, applied to workspace_root instead of office_root — the
# tests below mirror sweep_retired_office's own coverage, not the full matrix, since the
# guard bodies are identical and already proven above. ═══

async def test_workspace_sweep_deletes_a_stranger_with_no_seat_row_at_all(
    actions: Actions, tmp_path: Path,
) -> None:
    """The climintworker1/inferredworker1/deliberato shape: a real workspace directory, no
    matching Seat object at any status."""
    ws = tmp_path / "climintworker1"
    ws.mkdir()
    (ws / ".osiris").write_text('project = "cliproj1"\n')
    out = await sweep_seat_workspace(
        actions.pool, handle="climintworker1", dry_run=False, because="test cleanup",
        workspace_root=tmp_path, agents_json=_sweep_agents_json([]), sleep=_instant_sleep)
    assert out["status"] == "deleted"
    assert out["seat"] is None
    assert not ws.exists()


async def test_workspace_sweep_would_delete_a_retired_seats_workspace(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.seats import ensure_seat, retire_seat

    seat = await ensure_seat(actions, house="sweephouse", handle="WsRetired1",
                             source="test")
    await retire_seat(actions, seat["seat_id"], reason="role is over", actor="test")
    ws = tmp_path / "wsretired1"
    ws.mkdir()
    (ws / "README.md").write_text("work\n")
    out = await sweep_seat_workspace(
        actions.pool, handle="WsRetired1", workspace_root=tmp_path,
        agents_json=_sweep_agents_json([]), sleep=_instant_sleep)
    assert out["status"] == "would-delete"
    assert out["seat"] == seat["seat_id"]
    assert out["entries"] == ["README.md"]


async def test_workspace_sweep_refuses_an_active_seats_workspace(
    actions: Actions, tmp_path: Path,
) -> None:
    from src.orchestrator.seats import ensure_seat

    await ensure_seat(actions, house="sweephouse", handle="WsActive1", source="test")
    ws = tmp_path / "wsactive1"
    ws.mkdir()
    out = await sweep_seat_workspace(
        actions.pool, handle="WsActive1", workspace_root=tmp_path,
        agents_json=_sweep_agents_json([]), sleep=_instant_sleep)
    assert "status='active'" in out["error"]
    assert ws.is_dir()


async def test_workspace_sweep_refuses_a_live_body_at_the_workspace_right_now(
    actions: Actions, tmp_path: Path,
) -> None:
    ws = tmp_path / "wslive1"
    ws.mkdir()
    out = await sweep_seat_workspace(
        actions.pool, handle="wslive1", workspace_root=tmp_path,
        agents_json=_sweep_agents_json(
            [{"sessionId": "wslive-1111-2222-3333-444444444444", "pid": 777,
              "cwd": str(ws)}]),
        read_exe=lambda pid: _LIVE_EXE, read_cwd=lambda pid: str(ws),
        sleep=_instant_sleep)
    assert out["status"] == "refused-live-body"
    assert "right now" in out["detail"]


async def test_workspace_sweep_refuses_a_handle_that_traverses_out_of_the_root(
    actions: Actions, tmp_path: Path,
) -> None:
    root = tmp_path / "code"
    root.mkdir()
    outside = tmp_path / "notaworkspace"
    outside.mkdir()
    (outside / "precious.txt").write_text("real work\n")

    out = await sweep_seat_workspace(
        actions.pool, handle="../notaworkspace", dry_run=False, because="traversal attempt",
        workspace_root=root, agents_json=_sweep_agents_json([]), sleep=_instant_sleep)

    assert "does not name a workspace directly under" in out["error"]
    assert outside.is_dir()
    assert (outside / "precious.txt").read_text() == "real work\n"


async def test_workspace_sweep_execute_refuses_without_because(
    actions: Actions, tmp_path: Path,
) -> None:
    ws = tmp_path / "wsnobecause"
    ws.mkdir()
    out = await sweep_seat_workspace(
        actions.pool, handle="wsnobecause", dry_run=False, workspace_root=tmp_path,
        sleep=_instant_sleep)
    assert "because is required" in out["error"]
    assert ws.is_dir()


async def test_workspace_sweep_defaults_to_home_code(actions: Actions) -> None:
    """No `workspace_root` override — resolves against the real Path.home()/'code', the
    documented mint-time default. A handle nobody minted just reports nothing to sweep,
    proving the default resolved somewhere real rather than erroring on its own."""
    out = await sweep_seat_workspace(
        actions.pool, handle="no-such-handle-ever-minted-zzz", sleep=_instant_sleep)
    assert "nothing to sweep" in out["error"]
