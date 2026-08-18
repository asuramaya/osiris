"""Fleet tree render — grouped by project, live expanded, history collapsed. Pure."""
from __future__ import annotations

from datetime import UTC, datetime

from src.orchestrator.fleetview import render_fleet_tree

T0 = datetime(2026, 7, 7, 12, 0, tzinfo=UTC)
T1 = datetime(2026, 7, 7, 13, 0, tzinfo=UTC)


def _n(model: str | None = "claude-fable-5", project: str | None = "osiris",
       parent: str | None = None, live: bool = False, ts: datetime | None = None,
       retired: bool = False, seat: str | None = None) -> dict:
    return {"model": model, "project": project, "parent": parent, "live": live, "ts": ts,
            "retired": retired, "seat": seat}


def test_groups_by_project_and_collapses_the_past() -> None:
    nodes = {
        "agent:live1": _n(live=True, ts=T1),
        "agent:old1": _n(ts=T0),
        "agent:old2": _n(ts=T1),
        "agent:h1": _n(project="sibling-one", model="claude-opus-4-8"),
        "agent:h2": _n(project="sibling-one"),
    }
    tree = render_fleet_tree(nodes)
    lines = tree.splitlines()
    # one section per project, sorted ALPHABETICALLY; counts in the header
    assert lines[0].startswith("▸ osiris — 1 live · 3 sessions")
    assert any(line.startswith("▸ sibling-one — 0 live · 2 sessions") for line in lines)
    # the live agent is expanded; the retired collapse to one counted line with the freshest id
    assert "● agent:live1  fable-5" in tree
    assert "○ 2 past sessions (latest agent:old2)" in tree
    assert "agent:old1" not in tree  # folded away
    # sibling-one has no timestamps at all → count only, no latest note
    assert "○ 2 past sessions" in tree.split("▸ sibling-one")[1]


def test_swarm_children_fold_into_a_model_tally() -> None:
    nodes = {
        "agent:root": _n(live=True, ts=T1),
        "agent:kid1": _n(model="claude-opus-4-8", parent="agent:root"),
        "agent:kid2": _n(model="claude-opus-4-8", parent="agent:root"),
        "agent:kid3": _n(model="claude-sonnet-5", parent="agent:root"),
        "agent:grandkid": _n(model="claude-haiku-4-5-20251001", parent="agent:kid3"),
    }
    tree = render_fleet_tree(nodes)
    assert "● agent:root" in tree
    # all four descendants fold into one line (the grandkid counts through its parent)
    assert "○ swarm: 4 retired (opus-4-8 ×2, sonnet-5 ×1, haiku-4-5-20251001 ×1)" in tree
    assert "agent:kid1" not in tree
    # the header carries the swarm total
    assert "▸ osiris — 1 live · 1 sessions · swarm 4" in tree


def test_a_live_descendant_keeps_its_line_open() -> None:
    # a retired root holding a LIVE sub-agent must not be folded — the live path stays visible
    nodes = {
        "agent:root": _n(ts=T0),  # itself retired
        "agent:kid": _n(parent="agent:root", live=True, ts=T1),
    }
    tree = render_fleet_tree(nodes)
    assert "○ agent:root" in tree and "● agent:kid" in tree


def test_full_mode_expands_everything_grouped() -> None:
    nodes = {
        "agent:a": _n(ts=T0),
        "agent:b": _n(ts=T1),
        "agent:kid": _n(parent="agent:b", model="claude-opus-4-8"),
    }
    tree = render_fleet_tree(nodes, full=True)
    assert "○ agent:a" in tree and "○ agent:b" in tree and "agent:kid" in tree
    assert "past sessions" not in tree  # nothing collapsed in full mode

def test_quiet_is_never_called_retired() -> None:
    """THE GHOSTS (operator, 2026-07-12). The fold line said "N retired sessions", but the fold
    means nothing more than NOT LIVE — and only 41 of 517 root minds (8%) ever signed a death
    certificate. The tree was awarding the word to the other 92%.

    RETIRED IS NOT A SYNONYM FOR QUIET. It is a deliberate, signed close that the wake trigger is
    bound never to reanimate — a word with teeth. Spending it on minds that merely stopped talking
    is an inference wearing a declaration's authority, and it is how a graph grows ghosts.
    """
    nodes = {
        "agent:signed": _n(ts=T1, retired=True),    # called retire() — a real death certificate
        "agent:quiet1": _n(ts=T0),                  # just... stopped. Nobody signed anything.
        "agent:quiet2": _n(ts=T0),
    }
    tree = render_fleet_tree(nodes)
    assert "○ 3 past sessions" in tree      # all three are PAST...
    assert "· 1 retired" in tree            # ...and exactly ONE of them retired


def test_a_fleet_that_never_retires_never_says_retired() -> None:
    """The common case, and the one that produced the lie: nobody signed off, so the word does
    not appear at all. We say what we observed — they went quiet — and no more."""
    tree = render_fleet_tree({"agent:a": _n(ts=T0), "agent:b": _n(ts=T0)})
    assert "○ 2 past sessions" in tree
    assert "retired" not in tree


def test_a_claimed_seat_rides_beside_its_id() -> None:
    """dd47c1da: "fleet() must print claimed names" — wherever the tree renders an id, a
    CLAIMED seat (dd47c1da) rides beside it; an anonymous agent renders exactly as before."""
    nodes = {
        "agent:live1": _n(live=True, ts=T1, seat="Ra V"),
        "agent:live2": _n(live=True, ts=T1),  # anonymous — no seat key changes its render
    }
    tree = render_fleet_tree(nodes)
    assert "● agent:live1 (Ra V)  fable-5" in tree
    assert "● agent:live2  fable-5" in tree  # unchanged: no parenthetical for an anonymous agent


def test_a_claimed_seat_names_the_latest_of_a_folded_past() -> None:
    """The 'latest' pointer on a collapsed past-sessions line is still AN ID — the same rule
    applies: a claimed seat rides beside it."""
    nodes = {
        "agent:old1": _n(ts=T0),
        "agent:old2": _n(ts=T1, seat="Soundwave XI"),
    }
    tree = render_fleet_tree(nodes)
    assert "(latest agent:old2 (Soundwave XI))" in tree


def test_os_bodies_is_additive_and_absent_by_default() -> None:
    """heinrich's ghost-seat filing (thread 1fe6811c): omitting `os_bodies` (every existing
    caller, until fleet() is taught to pass it) must render EXACTLY as before — no new text,
    no behavior change to what `live` means."""
    nodes = {"agent:live1": _n(live=True, ts=T1)}
    assert render_fleet_tree(nodes) == render_fleet_tree(nodes, os_bodies=None)
    assert "os " not in render_fleet_tree(nodes)
    assert "ghost" not in render_fleet_tree(nodes)


def test_os_bodies_rides_beside_the_project_head_and_names_the_gap() -> None:
    """The graph believes 2 are live in osiris; the OS backs only 1 — the gap IS the ghost
    (a closed tab mid-decay, or a phantom mount that never backed a real session). sibling-one
    has a real body backing its one live agent: no ghost note. (thread #174, 2026-08-18: the
    ghost note itself is driven by the explicit PER-IDENTITY `ghost_gap`, never re-derived
    here as a netted `live_n - bodies` subtraction — that netting is exactly the bug that let
    a false-live row and a false-dead body cancel silently.)"""
    nodes = {
        "agent:live1": _n(live=True, ts=T1),
        "agent:live2": _n(live=True, ts=T1),
        "agent:h1": _n(project="sibling-one", live=True, ts=T1),
    }
    tree = render_fleet_tree(
        nodes, os_bodies={"osiris": 1, "sibling-one": 1},
        ghost_gap={"osiris": {"false_live": ["agent:live2"], "false_dead": []}})
    osiris_line = tree.splitlines()[0]
    assert osiris_line.startswith("▸ osiris — 2 live · 2 sessions")
    assert "1 os body" in osiris_line
    assert "⚠ 1 ghost (1 false-live)" in osiris_line
    sibling_line = next(line for line in tree.splitlines() if line.startswith("▸ sibling-one"))
    assert "1 os body" in sibling_line and "ghost" not in sibling_line


def test_os_bodies_defaults_an_unlisted_project_to_zero() -> None:
    """A project census never even mentions (no real process, ever) still gets an honest '0 os
    bodies' line rather than silently omitting the signal."""
    nodes = {"agent:live1": _n(live=True, ts=T1)}
    tree = render_fleet_tree(
        nodes, os_bodies={},
        ghost_gap={"osiris": {"false_live": ["agent:live1"], "false_dead": []}})
    assert "0 os bodies" in tree and "⚠ 1 ghost (1 false-live)" in tree


def test_ghost_gap_names_false_live_and_false_dead_without_cancelling() -> None:
    """rotten-apple's own specimen (thread #174, 2026-08-18): ONE false-live row and THREE
    unclaimed real bodies in the SAME project. The old netted math (1 live - 3 bodies = -2)
    would have shown NOTHING wrong at all; per-identity shows both, honestly."""
    nodes = {"agent:live1": _n(live=True, ts=T1)}
    tree = render_fleet_tree(
        nodes, os_bodies={"osiris": 3},
        ghost_gap={"osiris": {
            "false_live": ["agent:live1"],
            "false_dead": [{"cwd": "/a", "pids": [1]}, {"cwd": "/b", "pids": [2]},
                            {"cwd": "/c", "pids": [3]}],
        }})
    line = tree.splitlines()[0]
    assert "3 os bodies" in line
    assert "⚠ 4 ghosts (1 false-live, 3 unclaimed bodies)" in line


def test_the_binding_renders_anchored_beside_the_claimed_name() -> None:
    """Phase B (5cef856b): a mind that actively HOLDS a Seat shows its binding in the tree —
    the declared identity beside the inferred one; neither claimed nor bound renders bare."""
    from datetime import UTC, datetime
    now = datetime.now(UTC)
    nodes = {
        "agent:b0nd0001": {"model": "claude-fable-5", "project": "osiris", "parent": None,
                           "depth": 0, "ts": now, "live": True, "retired": False,
                           "seat": "Thoth XXXVIII", "bound": "seat:ab12cd34"},
        "agent:b0nd0002": {"model": "claude-fable-5", "project": "osiris", "parent": None,
                           "depth": 0, "ts": now, "live": True, "retired": False,
                           "seat": None, "bound": "seat:ffee0011"},
        "agent:b0nd0003": {"model": "claude-fable-5", "project": "osiris", "parent": None,
                           "depth": 0, "ts": now, "live": True, "retired": False,
                           "seat": None, "bound": None},
    }
    tree = render_fleet_tree(nodes)
    assert "agent:b0nd0001 (Thoth XXXVIII ⚓seat:ab12cd34)" in tree
    assert "agent:b0nd0002 (⚓seat:ffee0011)" in tree
    assert "agent:b0nd0003 " in tree and "agent:b0nd0003 (" not in tree
