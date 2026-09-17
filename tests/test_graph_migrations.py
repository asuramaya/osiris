"""DRAWING THE WHOLE GRAPH, THE MIGRATIONS (thread 325ef660): hermetic coverage for
the three name-dispatched graph-shape repairs in graph_migrations.py."""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.orchestrator.graph_migrations import (
    migrate_assertion_links,
    migrate_commits_to_agents,
    migrate_file_the_residual,
    migrate_file_the_unfiled,
    migrate_house_to_project,
    migrate_owned_by_second_pass,
    migrate_repo_seats_fix,
    run_migration,
)


async def test_run_migration_unknown_target_reports_valid_targets(actions: Actions) -> None:
    out = await run_migration(actions.pool, "nope", actor="test")
    assert "error" in out
    assert "repo_seats_fix" in out["valid_targets"]


# --- repo_seats_fix ----------------------------------------------------------------


async def test_repo_seats_fix_requires_because_to_apply(actions: Actions) -> None:
    out = await migrate_repo_seats_fix(actions, actor="test", dry_run=False, because="")
    assert "error" in out


async def test_repo_seats_fix_no_repo_seats_object_is_a_clean_noop(actions: Actions) -> None:
    out = await migrate_repo_seats_fix(actions, actor="test", dry_run=True)
    assert out["already_clean"] is True


async def test_repo_seats_fix_refiles_agents_and_edges_then_retires_repo_seats(
    actions: Actions,
) -> None:
    now = datetime.now(UTC)
    osiris = await actions.create_or_find_object("SoftwareProject", "repo:osiris", "test")
    seats = await actions.create_or_find_object("SoftwareProject", "repo:seats", "test")
    agent = await actions.create_or_find_object("Agent", "agent:gm-seats-agent", "test")
    await actions.assert_property(agent, "project", "seats", "test", now, 0.9)
    await actions.create_link(agent, seats, "works_in", "test", now, 1.0)
    thread = await actions.create_or_find_object("Thread", "thread:gm-seats-thread", "test")
    await actions.create_link(thread, seats, "in_repo", "test", now, 1.0)

    dry = await migrate_repo_seats_fix(actions, actor="test", dry_run=True)
    assert dry["agents_scanned"] == 1
    assert dry["edges_scanned"] == 2
    # a dry run touches nothing
    still_seats = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='project'",
        agent)
    assert still_seats == "seats"

    out = await migrate_repo_seats_fix(
        actions, actor="test", dry_run=False, because="test cleanup")
    assert out["agents_scanned"] == 1
    assert out["edges_scanned"] == 2
    assert out["osiris_seats_edges_after"] == 0
    assert out["retired"] == "repo:seats"

    new_project = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='project'",
        agent)
    assert new_project == "osiris"
    now2 = datetime.now(UTC)
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='works_in' "
        "AND (valid_until IS NULL OR valid_until > $3)", agent, osiris, now2)
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='in_repo' "
        "AND (valid_until IS NULL OR valid_until > $3)", thread, osiris, now2)
    seats_status = await actions.pool.fetchval(
        "SELECT status FROM objects WHERE canonical='repo:seats'")
    assert seats_status == "retired"

    # idempotent: a repeat call finds nothing left to touch (repo:seats is retired,
    # not active, so its own edges are no longer scanned; no agent still reads "seats")
    again = await migrate_repo_seats_fix(actions, actor="test", dry_run=True)
    assert again["agents_scanned"] == 0
    assert again["edges_scanned"] == 0


async def test_repo_seats_fix_scans_edges_regardless_of_container_status(
    actions: Actions,
) -> None:
    """THE STATUS-GATE BUG (Thoth mail 11448, found by cross-checking the first dry
    run against tree_ledger): a phantom container's own edges must be fixed
    whatever status the container itself is in -- the original gate silently
    skipped the whole edge repair the instant repo:seats drifted out of
    status='active', dropping every one of the 54 agents' own re-filed edges from
    the receipt with no error."""
    now = datetime.now(UTC)
    osiris = await actions.create_or_find_object("SoftwareProject", "repo:osiris", "test")
    seats = await actions.create_or_find_object("SoftwareProject", "repo:seats", "test")
    await actions.pool.execute(
        "UPDATE objects SET status='draft' WHERE id=$1", seats)
    thread = await actions.create_or_find_object("Thread", "thread:gm-seats-draft", "test")
    await actions.create_link(thread, seats, "in_repo", "test", now, 1.0)

    out = await migrate_repo_seats_fix(actions, actor="test", dry_run=True)
    assert out["edges_scanned"] == 1
    assert out["seats_status_before"] == "draft"
    assert out["edges_retired_or_refiled_by_type"] == {"in_repo": 1}

    applied = await migrate_repo_seats_fix(
        actions, actor="test", dry_run=False, because="test cleanup")
    assert applied["edges_scanned"] == 1
    now2 = datetime.now(UTC)
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='in_repo' "
        "AND (valid_until IS NULL OR valid_until > $3)", thread, osiris, now2)
    # retire_project itself refuses a non-'active' status -- draft never became
    # active here, so the attempt is reported honestly, not silently dropped.
    assert applied["retired"] is not None


async def test_repo_seats_fix_supersedes_cross_source_project_assertion(
    actions: Actions,
) -> None:
    """THE w316 LIVE FINDING (Thoth mail 11469): the original cut wrote the re-stamp
    via plain `assert_property`, whose own supersession is same-source-only -- when
    the prior "seats" assertion's source (the agent's own self-declaration) differs
    from the migration's `actor` (the console/migration actor), the new "osiris" row
    landed BESIDE the old one instead of retiring it, leaving two simultaneously
    `is_current` rows and the stream header still filing the agent under repo:seats.
    Regression: seed the prior assertion from a DIFFERENT source than the migration's
    own actor (mirrors the live agent-self-declared-vs-console split) and confirm
    exactly ONE current `project` row survives the apply, reading "osiris"."""
    now = datetime.now(UTC)
    await actions.create_or_find_object("SoftwareProject", "repo:osiris", "test")
    await actions.create_or_find_object("SoftwareProject", "repo:seats", "test")
    agent = await actions.create_or_find_object("Agent", "agent:gm-cross-source", "test")
    await actions.assert_property(agent, "project", "seats", "agent:gm-cross-source", now, 0.9)

    out = await migrate_repo_seats_fix(
        actions, actor="console", dry_run=False, because="test cleanup")
    assert out["agents_plan"][0]["superseded"] == 1
    assert out["superseded_total"] == 1

    rows = await actions.pool.fetch(
        "SELECT value #>> '{}' AS v FROM current_assertions "
        "WHERE object_id=$1 AND name='project'", agent)
    assert len(rows) == 1
    assert rows[0]["v"] == "osiris"


async def test_repo_seats_fix_reports_the_unchanged_agent_to_osiris_link_count(
    actions: Actions,
) -> None:
    """Read-only, for the record (Thoth mail 11448): a live link between a
    seats-stamped agent and an object already in osiris becomes same-district
    the moment the agent is re-stamped -- it needs no edge rewrite of its own,
    but the receipt still names how many there are."""
    now = datetime.now(UTC)
    await actions.create_or_find_object("SoftwareProject", "repo:osiris", "test")
    seats = await actions.create_or_find_object("SoftwareProject", "repo:seats", "test")
    agent = await actions.create_or_find_object("Agent", "agent:gm-seats-osiris-link", "test")
    await actions.assert_property(agent, "project", "seats", "test", now, 0.9)
    await actions.create_link(agent, seats, "works_in", "test", now, 1.0)
    osiris_member = await actions.create_or_find_object(
        "Thread", "thread:gm-osiris-member", "test")
    await actions.assert_property(osiris_member, "project", "osiris", "test", now, 0.9)
    await actions.create_link(agent, osiris_member, "cites", "test", now, 1.0)

    out = await migrate_repo_seats_fix(actions, actor="test", dry_run=True)
    assert out["agents_to_osiris_links_unchanged"] >= 1


# --- file_the_unfiled --------------------------------------------------------------


async def test_file_the_unfiled_majority_vote_files_the_object(actions: Actions) -> None:
    now = datetime.now(UTC)
    proj = await actions.create_or_find_object("SoftwareProject", "repo:gm-fileme", "test")
    filed_member = await actions.create_or_find_object("Thread", "thread:gm-filed", "test")
    await actions.create_link(filed_member, proj, "in_repo", "test", now, 1.0)
    unfiled = await actions.create_or_find_object("Thread", "thread:gm-unfiled", "test")
    await actions.create_link(unfiled, filed_member, "cites", "test", now, 1.0)

    dry = await migrate_file_the_unfiled(actions, actor="test", dry_run=True)
    assert dry["filed"] >= 1

    out = await migrate_file_the_unfiled(
        actions, actor="test", dry_run=False, because="test cleanup")
    assert out["filed"] >= 1
    project_name = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='project'",
        unfiled)
    assert project_name == "gm-fileme"


async def test_file_the_unfiled_tie_stays_unfiled_and_is_counted(actions: Actions) -> None:
    now = datetime.now(UTC)
    proj_a = await actions.create_or_find_object("SoftwareProject", "repo:gm-tie-a", "test")
    proj_b = await actions.create_or_find_object("SoftwareProject", "repo:gm-tie-b", "test")
    unfiled = await actions.create_or_find_object("Thread", "thread:gm-tie-unfiled", "test")
    await actions.create_link(unfiled, proj_a, "cites", "test", now, 1.0)
    await actions.create_link(unfiled, proj_b, "cites", "test", now, 1.0)

    out = await migrate_file_the_unfiled(actions, actor="test", dry_run=True)
    tie_entries = [t for t in out["ties_plan"] if t["object"] == str(unfiled)[:8]]
    assert len(tie_entries) == 1
    assert out["ties"] >= 1


async def test_file_the_unfiled_no_neighbour_project_stays_unfiled(actions: Actions) -> None:
    await actions.create_or_find_object("Thread", "thread:gm-lonely", "test")
    out = await migrate_file_the_unfiled(actions, actor="test", dry_run=True)
    assert out["filed"] == 0
    assert out["still_unfiled"] >= 1


async def test_file_the_unfiled_iterates_to_a_fixed_point(actions: Actions) -> None:
    """THE FIXED-POINT FIX (Thoth mail 11448): a whole cluster of mutually-unfiled
    objects only touches a real project through ANOTHER unfiled object -- a
    single pass cannot see across two hops, but a second pass (now able to see
    the first pass's own newly-filed neighbour) can."""
    now = datetime.now(UTC)
    proj = await actions.create_or_find_object("SoftwareProject", "repo:gm-chain", "test")
    member = await actions.create_or_find_object("Thread", "thread:gm-chain-member", "test")
    await actions.create_link(member, proj, "in_repo", "test", now, 1.0)
    hop1 = await actions.create_or_find_object("Thread", "thread:gm-chain-hop1", "test")
    await actions.create_link(hop1, member, "cites", "test", now, 1.0)
    hop2 = await actions.create_or_find_object("Thread", "thread:gm-chain-hop2", "test")
    await actions.create_link(hop2, hop1, "cites", "test", now, 1.0)

    out = await migrate_file_the_unfiled(
        actions, actor="test", dry_run=False, because="test cleanup")
    assert out["passes"] >= 2
    hop2_project = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='project'",
        hop2)
    assert hop2_project == "gm-chain"


async def test_file_the_unfiled_excludes_person_and_seat_from_filing(actions: Actions) -> None:
    """Thoth mail 11448: a global with heavy structural fan-in (a principal, a
    seat) is a landmark, not a member of whatever district it happens to touch
    most -- Person/Seat objects are never filed, even when they'd otherwise win
    a clean majority vote."""
    now = datetime.now(UTC)
    proj = await actions.create_or_find_object("SoftwareProject", "repo:gm-landmark", "test")
    member = await actions.create_or_find_object("Thread", "thread:gm-landmark-member", "test")
    await actions.create_link(member, proj, "in_repo", "test", now, 1.0)
    person = await actions.create_or_find_object("Person", "person:gm-landmark-person", "test")
    await actions.create_link(person, member, "cites", "test", now, 1.0)
    seat = await actions.create_or_find_object("Seat", "seat:gm-landmark-seat", "test")
    await actions.create_link(seat, member, "cites", "test", now, 1.0)

    await migrate_file_the_unfiled(actions, actor="test", dry_run=False, because="test cleanup")
    person_project = await actions.pool.fetchval(
        "SELECT 1 FROM current_assertions WHERE object_id=$1 AND name='project'", person)
    seat_project = await actions.pool.fetchval(
        "SELECT 1 FROM current_assertions WHERE object_id=$1 AND name='project'", seat)
    assert person_project is None
    assert seat_project is None


# --- assertion_links -----------------------------------------------------------------


async def test_assertion_links_requires_because_to_apply(actions: Actions) -> None:
    out = await migrate_assertion_links(actions, actor="test", dry_run=False, because="  ")
    assert "error" in out


async def test_assertion_links_recorded_by_resolves_a_real_agent_source(
    actions: Actions,
) -> None:
    now = datetime.now(UTC)
    agent = await actions.create_or_find_object("Agent", "agent:gm-recorder", "test")
    decision = await actions.create_or_find_object("Decision", "decision:gm-recorded", "test")
    await actions.assert_property(
        decision, "summary", "a real decision", "agent:gm-recorder", now, 0.9)

    dry = await migrate_assertion_links(actions, actor="test", dry_run=True)
    assert dry["receipt"]["recorded_by"]["minted"] >= 1

    out = await migrate_assertion_links(
        actions, actor="test", dry_run=False, because="test cleanup")
    assert out["receipt"]["recorded_by"]["minted"] >= 1
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='recorded_by' "
        "AND (valid_until IS NULL OR valid_until > now())", decision, agent)

    # idempotent: a repeat call finds it already present, mints nothing new for this pair
    again = await migrate_assertion_links(actions, actor="test", dry_run=True)
    assert again["receipt"]["recorded_by"]["already_present"] >= 1


async def test_assertion_links_supersedes_mints_one_edge_from_either_property(
    actions: Actions,
) -> None:
    now = datetime.now(UTC)
    newer = await actions.create_or_find_object("Decision", "decision:gm-newer", "test")
    older = await actions.create_or_find_object("Decision", "decision:gm-older", "test")
    await actions.assert_property(
        newer, "supersedes", "decision:gm-older", "test", now, 0.9)

    out = await migrate_assertion_links(
        actions, actor="test", dry_run=False, because="test cleanup")
    assert out["receipt"]["supersedes"]["minted"] >= 1
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='supersedes' "
        "AND (valid_until IS NULL OR valid_until > now())", newer, older)
    # the OTHER direction was never minted
    assert not await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='supersedes' "
        "AND (valid_until IS NULL OR valid_until > now())", older, newer)


async def test_assertion_links_closed_by_skips_when_already_present(actions: Actions) -> None:
    now = datetime.now(UTC)
    thread = await actions.create_or_find_object("Thread", "thread:gm-closed", "test")
    closer = await actions.create_or_find_object("Agent", "agent:gm-closer", "test")
    await actions.assert_property(thread, "resolved_in", "agent:gm-closer", "test", now, 0.9)
    await actions.create_link(thread, closer, "closed_by", "test", now, 1.0)

    out = await migrate_assertion_links(actions, actor="test", dry_run=True)
    assert out["receipt"]["closed_by"]["already_present"] >= 1
    assert out["receipt"]["closed_by"]["minted"] == 0


async def test_assertion_links_vendor_of_resolves_against_a_real_software_project(
    actions: Actions,
) -> None:
    now = datetime.now(UTC)
    vendor_proj = await actions.create_or_find_object(
        "SoftwareProject", "repo:gm-vendor-proj", "test")
    ref = await actions.create_or_find_object("Reference", "ref:gm-vendored", "test")
    await actions.assert_property(ref, "vendor", "gm-vendor-proj", "test", now, 0.9)

    out = await migrate_assertion_links(
        actions, actor="test", dry_run=False, because="test cleanup")
    assert out["receipt"]["vendor_of"]["minted"] >= 1
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='vendor_of' "
        "AND (valid_until IS NULL OR valid_until > now())", vendor_proj, ref)


async def test_assertion_links_unresolvable_source_is_skipped_not_guessed(
    actions: Actions,
) -> None:
    now = datetime.now(UTC)
    thread = await actions.create_or_find_object("Thread", "thread:gm-ghost-owner", "test")
    await actions.assert_property(
        thread, "owner", "seat:gm-no-such-seat", "test", now, 0.9)

    out = await migrate_assertion_links(actions, actor="test", dry_run=True)
    assert out["receipt"]["owned_by"]["skipped_unresolvable"] >= 1
    assert "seat:gm-no-such-seat" in out["receipt"]["owned_by"]["unresolvable_samples"]


async def test_assertion_links_acknowledges_was_dropped(actions: Actions) -> None:
    """Thoth mail 11448, "you read it right": the confirmation already mints a
    real `cites` edge (acknowledge_prior_art's own docstring) -- a distinct
    acknowledges link would be redundant, dropped from the migration entirely."""
    out = await migrate_assertion_links(actions, actor="test", dry_run=True)
    assert "acknowledges" not in out["receipt"]


async def test_assertion_links_owned_by_falls_back_to_a_project_name(
    actions: Actions,
) -> None:
    """Thoth mail 11448, "the owner law's legacy shape": a Thread.owner value
    that never resolves as a Seat/Agent canonical is tried against an active
    SoftwareProject's own bare name before it's given up as unresolvable."""
    now = datetime.now(UTC)
    proj = await actions.create_or_find_object("SoftwareProject", "repo:gm-owner-proj", "test")
    thread = await actions.create_or_find_object("Thread", "thread:gm-project-owner", "test")
    await actions.assert_property(thread, "owner", "gm-owner-proj", "test", now, 0.9)

    out = await migrate_assertion_links(
        actions, actor="test", dry_run=False, because="test cleanup")
    assert out["receipt"]["owned_by"]["minted_as_project"] >= 1
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='owned_by' "
        "AND (valid_until IS NULL OR valid_until > now())", thread, proj)


# --- owned_by_second_pass -----------------------------------------------------------


async def test_owned_by_second_pass_requires_because_to_apply(actions: Actions) -> None:
    out = await migrate_owned_by_second_pass(actions, actor="test", dry_run=False, because="")
    assert "error" in out


async def test_owned_by_second_pass_resolves_operator_to_the_principal(
    actions: Actions,
) -> None:
    now = datetime.now(UTC)
    operator = await actions.create_or_find_object(
        "Person", "principal:analyst:operator", "test")
    thread = await actions.create_or_find_object("Thread", "thread:gm-op-owner", "test")
    await actions.assert_property(thread, "owner", "operator", "test", now, 0.9)

    dry = await migrate_owned_by_second_pass(actions, actor="test", dry_run=True)
    assert dry["minted"] >= 1
    assert dry["minted_as_operator"] >= 1

    out = await migrate_owned_by_second_pass(
        actions, actor="test", dry_run=False, because="test cleanup")
    assert out["minted_as_operator"] >= 1
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='owned_by' "
        "AND (valid_until IS NULL OR valid_until > now())", thread, operator)

    # idempotent: a repeat call finds it already present
    again = await migrate_owned_by_second_pass(actions, actor="test", dry_run=True)
    assert again["already_present"] >= 1


async def test_owned_by_second_pass_resolves_a_raw_seat_canonical(actions: Actions) -> None:
    """Thoth mail 11567, the live catch: the first cut's own docstring claimed it
    re-tried the plain canonical resolve, but the code never did -- a value that is
    ALREADY a live, active Seat canonical (never a bare handle name) must resolve
    directly, not fall through to the handle lookup (which would fail: a full
    canonical is never itself a `handle` assertion's own value)."""
    now = datetime.now(UTC)
    seat = await actions.create_or_find_object("Seat", "seat:gm-owner-canonical", "test")
    thread = await actions.create_or_find_object("Thread", "thread:gm-canonical-owner", "test")
    await actions.assert_property(
        thread, "owner", "seat:gm-owner-canonical", "test", now, 0.9)

    dry = await migrate_owned_by_second_pass(actions, actor="test", dry_run=True)
    assert dry["minted"] >= 1
    assert dry["minted_as_canonical"] >= 1

    out = await migrate_owned_by_second_pass(
        actions, actor="test", dry_run=False, because="test cleanup")
    assert out["minted_as_canonical"] >= 1
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='owned_by' "
        "AND (valid_until IS NULL OR valid_until > now())", thread, seat)


async def test_owned_by_second_pass_resolves_a_bare_seat_handle(actions: Actions) -> None:
    now = datetime.now(UTC)
    seat = await actions.create_or_find_object("Seat", "seat:gm-owner-handle", "test")
    await actions.assert_property(seat, "handle", "gm-owner-handle-name", "test", now, 0.9)
    thread = await actions.create_or_find_object("Thread", "thread:gm-handle-owner", "test")
    await actions.assert_property(
        thread, "owner", "gm-owner-handle-name", "test", now, 0.9)

    out = await migrate_owned_by_second_pass(
        actions, actor="test", dry_run=False, because="test cleanup")
    assert out["minted_as_handle"] >= 1
    assert await actions.pool.fetchval(
        "SELECT 1 FROM links WHERE from_id=$1 AND to_id=$2 AND type='owned_by' "
        "AND (valid_until IS NULL OR valid_until > now())", thread, seat)


async def test_owned_by_second_pass_still_unresolvable_is_counted_not_guessed(
    actions: Actions,
) -> None:
    now = datetime.now(UTC)
    thread = await actions.create_or_find_object("Thread", "thread:gm-still-ghost", "test")
    await actions.assert_property(
        thread, "owner", "gm-no-such-handle-at-all", "test", now, 0.9)

    out = await migrate_owned_by_second_pass(actions, actor="test", dry_run=True)
    assert out["skipped_unresolvable"] >= 1
    assert "gm-no-such-handle-at-all" in out["unresolvable_samples"]


# --- file_the_residual ---------------------------------------------------------------


async def test_file_the_residual_requires_because_to_apply(actions: Actions) -> None:
    out = await migrate_file_the_residual(actions, actor="test", dry_run=False, because="")
    assert "error" in out


async def test_file_the_residual_files_a_message_via_broadcast_to(actions: Actions) -> None:
    now = datetime.now(UTC)
    proj = await actions.create_or_find_object(
        "SoftwareProject", "repo:gm-residual-broadcast", "test")
    msg = await actions.create_or_find_object("Message", "message:gm-residual-1", "test")
    await actions.create_link(msg, proj, "broadcast_to", "test", now, 1.0)

    dry = await migrate_file_the_residual(actions, actor="test", dry_run=True)
    assert dry["filed"] >= 1
    assert dry["filed_via_broadcast"] >= 1

    out = await migrate_file_the_residual(
        actions, actor="test", dry_run=False, because="test cleanup")
    assert out["filed_via_broadcast"] >= 1
    project_name = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='project'",
        msg)
    assert project_name == "gm-residual-broadcast"


async def test_file_the_residual_falls_back_to_sent_by_agent_project(
    actions: Actions,
) -> None:
    now = datetime.now(UTC)
    await actions.create_or_find_object("SoftwareProject", "repo:gm-residual-sender", "test")
    agent = await actions.create_or_find_object("Agent", "agent:gm-residual-sender", "test")
    await actions.assert_property(agent, "project", "gm-residual-sender", "test", now, 0.9)
    msg = await actions.create_or_find_object("Message", "message:gm-residual-2", "test")
    await actions.create_link(msg, agent, "sent_by", "test", now, 1.0)

    out = await migrate_file_the_residual(
        actions, actor="test", dry_run=False, because="test cleanup")
    assert out["filed"] >= 1
    project_name = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='project'",
        msg)
    assert project_name == "gm-residual-sender"


async def test_file_the_residual_broadcast_wins_over_sender_disagreement(
    actions: Actions,
) -> None:
    now = datetime.now(UTC)
    broadcast_proj = await actions.create_or_find_object(
        "SoftwareProject", "repo:gm-residual-priority", "test")
    await actions.create_or_find_object(
        "SoftwareProject", "repo:gm-residual-sender-other", "test")
    agent = await actions.create_or_find_object(
        "Agent", "agent:gm-residual-priority-sender", "test")
    await actions.assert_property(
        agent, "project", "gm-residual-sender-other", "test", now, 0.9)
    msg = await actions.create_or_find_object("Message", "message:gm-residual-3", "test")
    await actions.create_link(msg, broadcast_proj, "broadcast_to", "test", now, 1.0)
    await actions.create_link(msg, agent, "sent_by", "test", now, 1.0)

    out = await migrate_file_the_residual(
        actions, actor="test", dry_run=False, because="test cleanup")
    assert out["filed_via_broadcast"] >= 1
    project_name = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id=$1 AND name='project'",
        msg)
    assert project_name == "gm-residual-priority"


async def test_file_the_residual_cross_project_dm_ties_and_stays_unfiled(
    actions: Actions,
) -> None:
    now = datetime.now(UTC)
    await actions.create_or_find_object("SoftwareProject", "repo:gm-residual-tie-a", "test")
    await actions.create_or_find_object("SoftwareProject", "repo:gm-residual-tie-b", "test")
    sender = await actions.create_or_find_object(
        "Agent", "agent:gm-residual-tie-sender", "test")
    await actions.assert_property(sender, "project", "gm-residual-tie-a", "test", now, 0.9)
    recipient = await actions.create_or_find_object(
        "Agent", "agent:gm-residual-tie-recipient", "test")
    await actions.assert_property(recipient, "project", "gm-residual-tie-b", "test", now, 0.9)
    msg = await actions.create_or_find_object("Message", "message:gm-residual-tie", "test")
    await actions.create_link(msg, sender, "sent_by", "test", now, 1.0)
    await actions.create_link(msg, recipient, "addressed_to", "test", now, 1.0)

    out = await migrate_file_the_residual(actions, actor="test", dry_run=True)
    tie_entries = [t for t in out["ties_plan"] if t["object"] == str(msg)[:8]]
    assert len(tie_entries) == 1
    assert out["still_unfiled"] >= 1


async def test_file_the_residual_no_signal_stays_unfiled(actions: Actions) -> None:
    await actions.create_or_find_object("Message", "message:gm-residual-lonely", "test")
    out = await migrate_file_the_residual(actions, actor="test", dry_run=True)
    assert out["filed"] == 0
    assert out["still_unfiled"] >= 1


async def test_file_the_residual_only_touches_messages(actions: Actions) -> None:
    """SCOPE DELIBERATELY NARROW (docstring): a non-Message residual object is left
    exactly as `migrate_file_the_unfiled` already reported it -- never guessed at
    here, even if it happens to carry a broadcast_to-shaped edge."""
    now = datetime.now(UTC)
    proj = await actions.create_or_find_object(
        "SoftwareProject", "repo:gm-residual-non-message", "test")
    thread = await actions.create_or_find_object(
        "Thread", "thread:gm-residual-non-message", "test")
    await actions.create_link(thread, proj, "broadcast_to", "test", now, 1.0)

    out = await migrate_file_the_residual(actions, actor="test", dry_run=True)
    assert out["scanned"] == 0


# --- commits_to_agents (backfill door, WAVE 27 ruling 4cf5e4b3/b8fb26494e0e) --------


async def test_commits_to_agents_requires_because_to_apply(actions: Actions) -> None:
    out = await migrate_commits_to_agents(actions, actor="test", dry_run=False, because="")
    assert "error" in out


async def test_commits_to_agents_mints_the_correct_generation_and_is_idempotent(
    actions: Actions,
) -> None:
    from src.orchestrator.seats import bind_holder, bind_seat_tree, ensure_seat

    t1 = datetime(2026, 1, 1, tzinfo=UTC)
    t2 = datetime(2026, 1, 2, tzinfo=UTC)
    proj = await actions.create_or_find_object("SoftwareProject", "repo:gm-c2a-proj", "test")
    await actions.assert_property(proj, "on_disk_path", "/tmp/gm-c2a-proj", "test", t1, 0.9)

    seat = await ensure_seat(actions, house="osiris", handle="GmC2a", source="test")
    await actions.create_or_find_object("Agent", "agent:gm-c2a-i", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:gm-c2a-i")
    bound = await bind_seat_tree(actions, seat_id=seat["seat_id"], tree_cwd="/tmp/gm-c2a-proj",
                                 actor="agent:gm-c2a-i", because="test")
    assert "error" not in bound, bound
    # this migration reads Commit.authored_date, a plain assertion value -- unlike
    # ingest_repo's own live tests (git's own whole-second-truncated timestamp), no
    # real-clock sleep is needed here: the holds link's own first_seen moves straight
    # into the past.
    await actions.pool.execute(
        "UPDATE links SET first_seen=$1 WHERE from_id="
        "(SELECT id FROM objects WHERE canonical='agent:gm-c2a-i') AND type='holds'", t1)

    commit = await actions.create_or_find_object("Commit", "commit:gm-c2a-1", "test")
    await actions.assert_property(commit, "authored_date", t2.isoformat(), "test", t2, 0.9)
    await actions.create_link(commit, proj, "in_repo", "test", t2, 1.0)

    dry = await migrate_commits_to_agents(actions, actor="test", dry_run=True)
    assert dry["minted_by_worktree_time"] >= 1
    assert dry["sharpened_by_trailer"] == 0
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE type='committed_by'") == 0

    out = await migrate_commits_to_agents(
        actions, actor="test", dry_run=False, because="test backfill")
    assert out["minted_by_worktree_time"] >= 1
    committer = await actions.pool.fetchval(
        "SELECT a.canonical FROM links l JOIN objects a ON a.id=l.to_id "
        "WHERE l.from_id=$1 AND l.type='committed_by'", commit)
    assert committer == "agent:gm-c2a-i"

    # idempotent: a repeat apply never re-mints or duplicates
    await migrate_commits_to_agents(actions, actor="test", dry_run=False, because="again")
    count = await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='committed_by'", commit)
    assert count == 1


async def test_commits_to_agents_abstains_when_no_seat_bound_to_the_worktree(
    actions: Actions,
) -> None:
    now = datetime.now(UTC)
    proj = await actions.create_or_find_object(
        "SoftwareProject", "repo:gm-c2a-unbound", "test")
    await actions.assert_property(
        proj, "on_disk_path", "/tmp/gm-c2a-unbound-nobody", "test", now, 0.9)
    commit = await actions.create_or_find_object("Commit", "commit:gm-c2a-unbound-1", "test")
    await actions.assert_property(commit, "authored_date", now.isoformat(), "test", now, 0.9)
    await actions.create_link(commit, proj, "in_repo", "test", now, 1.0)

    out = await migrate_commits_to_agents(actions, actor="test", dry_run=True)
    assert out["minted_by_worktree_time"] == 0
    assert out["abstained"] >= 1
    assert out["abstained_reasons"].get("no-seat-bound-to-worktree", 0) >= 1
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='committed_by'", commit) == 0


async def test_commits_to_agents_abstains_when_the_commit_predates_any_holder(
    actions: Actions,
) -> None:
    from src.orchestrator.seats import bind_holder, bind_seat_tree, ensure_seat

    old = datetime(2020, 1, 1, tzinfo=UTC)
    now = datetime.now(UTC)
    proj = await actions.create_or_find_object(
        "SoftwareProject", "repo:gm-c2a-predates", "test")
    await actions.assert_property(
        proj, "on_disk_path", "/tmp/gm-c2a-predates", "test", now, 0.9)
    seat = await ensure_seat(actions, house="osiris", handle="GmC2aPredates", source="test")
    await actions.create_or_find_object("Agent", "agent:gm-c2a-predates-i", "test")
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:gm-c2a-predates-i")
    bound = await bind_seat_tree(
        actions, seat_id=seat["seat_id"], tree_cwd="/tmp/gm-c2a-predates",
        actor="agent:gm-c2a-predates-i", because="test")
    assert "error" not in bound, bound

    commit = await actions.create_or_find_object("Commit", "commit:gm-c2a-predates-1", "test")
    await actions.assert_property(commit, "authored_date", old.isoformat(), "test", old, 0.9)
    await actions.create_link(commit, proj, "in_repo", "test", old, 1.0)

    out = await migrate_commits_to_agents(actions, actor="test", dry_run=True)
    assert out["abstained_reasons"].get("no-holder-at-author-time", 0) >= 1
    assert await actions.pool.fetchval(
        "SELECT count(*) FROM links WHERE from_id=$1 AND type='committed_by'", commit) == 0


# --- house_to_project (Thoth mail 12000, implements 70c001ec, "ONE TAXONOMY") --------------

async def test_migrate_house_to_project_repairs_a_null_house_on_apply(
    actions: Actions,
) -> None:
    from src.orchestrator.charter import set_charter
    from src.orchestrator.seats import ensure_seat

    seat = await ensure_seat(actions, house=None, handle="GmH2pRepair", source="test")
    await actions.create_or_find_object("SoftwareProject", "repo:gm-h2p-realproj", "test")
    await set_charter(actions, seat["seat_id"], ["gm-h2p-realproj"], actor="test")

    dry = await migrate_house_to_project(actions, actor="test", dry_run=True)
    entry = next(e for e in dry["entries"] if e["seat"] == seat["seat_id"])
    assert entry["old_house"] is None
    assert entry["new_project"] == "gm-h2p-realproj"
    assert entry["refused_why"] is None
    # dry run writes nothing
    still = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id="
        "(SELECT id FROM objects WHERE canonical=$1) AND name='house' "
        "ORDER BY confidence DESC, observed_at DESC LIMIT 1", seat["seat_id"])
    assert still is None

    out = await migrate_house_to_project(
        actions, actor="test", dry_run=False, because="test repair")
    assert out["repaired"] >= 1
    rows = await actions.pool.fetch(
        "SELECT value #>> '{}' AS v FROM current_assertions WHERE object_id="
        "(SELECT id FROM objects WHERE canonical=$1) AND name='house'", seat["seat_id"])
    assert len(rows) == 1
    assert rows[0]["v"] == "gm-h2p-realproj"


async def test_migrate_house_to_project_refuses_a_stamped_house_that_disagrees(
    actions: Actions,
) -> None:
    """w347 (Thoth mail 12153): a NON-NULL stamped house that disagrees with the
    charter's own governed project is the house anchor's own carve-out — the door
    refuses rather than overwriting a real value it has no standing to guess about."""
    from src.orchestrator.charter import set_charter
    from src.orchestrator.seats import ensure_seat

    seat = await ensure_seat(actions, house="gm-h2p-fabricated", handle="GmH2pDisagree",
                             source="test")
    await actions.create_or_find_object("SoftwareProject", "repo:gm-h2p-realproj", "test")
    await set_charter(actions, seat["seat_id"], ["gm-h2p-realproj"], actor="test")

    dry = await migrate_house_to_project(actions, actor="test", dry_run=True)
    entry = next(e for e in dry["entries"] if e["seat"] == seat["seat_id"])
    assert entry["old_house"] == "gm-h2p-fabricated"
    assert entry["new_project"] is None
    assert entry["refused_why"] == (
        "stamped house disagrees with charter: gm-h2p-fabricated vs gm-h2p-realproj")

    out = await migrate_house_to_project(
        actions, actor="test", dry_run=False, because="test refusal")
    entry = next(e for e in out["entries"] if e["seat"] == seat["seat_id"])
    assert entry["refused_why"] == (
        "stamped house disagrees with charter: gm-h2p-fabricated vs gm-h2p-realproj")
    # never written
    still = await actions.pool.fetchval(
        "SELECT value #>> '{}' FROM current_assertions WHERE object_id="
        "(SELECT id FROM objects WHERE canonical=$1) AND name='house' "
        "ORDER BY confidence DESC, observed_at DESC LIMIT 1", seat["seat_id"])
    assert still == "gm-h2p-fabricated"


async def test_migrate_house_to_project_reports_no_charter_without_guessing(
    actions: Actions,
) -> None:
    from src.orchestrator.seats import ensure_seat

    seat = await ensure_seat(actions, house="gm-h2p-nocharter", handle="GmH2pNoCharter",
                             source="test")

    out = await migrate_house_to_project(actions, actor="test", dry_run=True)
    entry = next(e for e in out["entries"] if e["seat"] == seat["seat_id"])
    assert entry["new_project"] is None
    assert entry["refused_why"] == "no charter"


async def test_migrate_house_to_project_skips_a_seat_already_correct(
    actions: Actions,
) -> None:
    from src.orchestrator.charter import set_charter
    from src.orchestrator.seats import ensure_seat

    seat = await ensure_seat(actions, house="gm-h2p-already", handle="GmH2pAlready",
                             source="test")
    await actions.create_or_find_object("SoftwareProject", "repo:gm-h2p-already", "test")
    await set_charter(actions, seat["seat_id"], ["gm-h2p-already"], actor="test")

    out = await migrate_house_to_project(actions, actor="test", dry_run=True)
    assert not any(e["seat"] == seat["seat_id"] for e in out["entries"])


async def test_run_migration_accepts_house_to_project(actions: Actions) -> None:
    out = await run_migration(actions.pool, "house_to_project", actor="test")
    assert "error" not in out
    assert out["dry_run"] is True
