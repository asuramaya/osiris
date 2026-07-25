"""TERSE-RENDER (task #55, thread 9092ed51) — a verbosity dial on MCP tool receipts. Terse
(verbose=False, the default) drops guidance PROSE from mount()/orient(); every structured
fact a caller could parse (counts, ids, lists — fleet_open_threads_total, co_agents.live,
open_threads_more) survives in BOTH modes. The regression guard throughout: verbose's
payload is always a strict superset of terse's — remove exactly the declared keys from
verbose and you get terse back, byte-for-byte, nothing else moves (the additive-only golden
shape, mirroring surface.py's own byte-exact convention).
"""
from __future__ import annotations

from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.capture import open_thread
from src.orchestrator.mounts import save_mount

# ═══ _terse() — the mechanism itself, tested in isolation before any tool uses it ═══════


def test_terse_strips_a_top_level_key() -> None:
    from src.mcp_server import _terse

    payload = {"agent": "agent:x", "note": "guidance prose"}
    out = _terse(payload, ("note",))
    assert out == {"agent": "agent:x"}


def test_terse_strips_a_nested_key_leaving_siblings() -> None:
    from src.mcp_server import _terse

    payload = {"co_agents": {"live": [{"agent": "agent:y"}], "note": "etiquette reminder"}}
    out = _terse(payload, ("co_agents", "note"))
    assert out == {"co_agents": {"live": [{"agent": "agent:y"}]}}


def test_terse_is_a_no_op_on_a_path_this_payload_never_populated() -> None:
    """Conditional fields (a note that only appears in some branches) must not make _terse
    raise or misbehave when the receipt in hand never grew that branch."""
    from src.mcp_server import _terse

    payload = {"agent": "agent:x"}
    out = _terse(payload, ("note",), ("co_agents", "note"), ("a", "b", "c"))
    assert out == {"agent": "agent:x"}


def test_terse_never_touches_an_undeclared_key_even_a_long_one() -> None:
    """The whole point of an explicit allowlist over a length/heuristic strip (the
    reachability().detail lesson, thread aeae9977): a long string NOT named in the
    allowlist survives untouched, because some other function may consume it as data."""
    from src.mcp_server import _terse

    long_structural = "a very long explanation string a downstream caller concatenates " * 5
    payload = {"detail": long_structural, "note": "drop me"}
    out = _terse(payload, ("note",))
    assert out["detail"] == long_structural
    assert "note" not in out


# ═══ mount() ══════════════════════════════════════════════════════════════════════════


async def test_mount_terse_by_default_drops_the_linked_note(
    actions: Actions, tmp_path: Path,
) -> None:
    from src import mcp_server as srv

    saved = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(tmp_path / "trsproja"),
                              job_dir=str(tmp_path / "jobs" / "trsa0001"))
        assert out["agent"] and out["project"] == "trsproja"  # structural fields present
        assert "note" not in out
    finally:
        srv._pool = saved


async def test_mount_verbose_restores_the_linked_note(actions: Actions, tmp_path: Path) -> None:
    from src import mcp_server as srv

    saved = srv._pool
    srv._pool = actions.pool
    try:
        out = await srv.mount(cwd=str(tmp_path / "trsprojb"),
                              job_dir=str(tmp_path / "jobs" / "trsb0001"), verbose=True)
        assert out["note"] == "linked — writes now attributed to you; call orient() next"
    finally:
        srv._pool = saved


async def test_mount_terse_keeps_co_agents_data_but_drops_its_note(
    actions: Actions, tmp_path: Path,
) -> None:
    """The live-sibling FACT (who, where) is structural and must survive terse mode; the
    'never git add -A' etiquette reminder is guidance prose and may drop."""
    from src import mcp_server as srv

    proj = "trsprojc"
    await save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "trscsib1"),
                     agent_id="agent:trscsib1", project=proj, cwd=str(tmp_path / "sib"),
                     model=None, session_key=None)
    saved = srv._pool
    srv._pool = actions.pool
    try:
        terse = await srv.mount(cwd=str(tmp_path / proj),
                                job_dir=str(tmp_path / "jobs" / "trscme01"))
        assert terse["co_agents"]["live"][0]["agent"] == "agent:trscsib1"
        assert "note" not in terse["co_agents"]

        verbose = await srv.mount(cwd=str(tmp_path / proj),
                                  job_dir=str(tmp_path / "jobs" / "trscme02"), verbose=True)
        assert verbose["co_agents"]["note"].startswith("2 other LIVE agent(s)")
        # ADDITIVE-ONLY: strip exactly the declared key from verbose, get terse's shape back
        assert verbose["co_agents"].keys() - {"note"} == terse["co_agents"].keys()
    finally:
        srv._pool = saved


# ═══ orient() ═════════════════════════════════════════════════════════════════════════


async def test_orient_terse_drops_the_scoped_note_but_keeps_the_same_count(
    actions: Actions,
) -> None:
    """orient()'s top-level note is 100% redundant with fleet_open_threads_total — the
    biggest single site by call-frequency×weight (every normal scoped call). Both terse and
    verbose must report the SAME count; only the sentence explaining it drops."""
    from src import mcp_server as srv

    await actions.create_or_find_object("SoftwareProject", "repo:trsorntb", "session")
    saved = srv._pool
    srv._pool = actions.pool
    try:
        terse = await srv.orient(project="trsorntb")
        verbose = await srv.orient(project="trsorntb", verbose=True)
        assert "note" not in terse
        assert verbose["note"].startswith("scoped to trsorntb;")
        assert terse["fleet_open_threads_total"] == verbose["fleet_open_threads_total"]
        # ADDITIVE-ONLY at the top level too
        assert verbose.keys() - {"note"} == terse.keys()
    finally:
        srv._pool = saved


async def test_orient_terse_keeps_co_agents_data_but_drops_its_note(
    actions: Actions, tmp_path: Path,
) -> None:
    from src import mcp_server as srv

    proj = "trsorntd"
    await save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "trsdsib1"),
                     agent_id="agent:trsdsib1", project=proj, cwd=str(tmp_path / "sib"),
                     model=None, session_key=None)
    await save_mount(actions.pool, job_dir=str(tmp_path / "jobs" / "trsdme01"),
                     agent_id="agent:trsdme01", project=proj, cwd=str(tmp_path / "me"),
                     model=None, session_key=None)
    saved = srv._pool
    srv._pool = actions.pool
    try:
        terse = await srv.orient(project=proj, session_anchor=str(tmp_path / "jobs" / "trsdme01"))
        verbose = await srv.orient(project=proj, verbose=True,
                                   session_anchor=str(tmp_path / "jobs" / "trsdme01"))
        assert terse.get("co_agents", {}).get("live", [{}])[0].get("agent") == "agent:trsdsib1"
        assert "note" not in terse.get("co_agents", {})
        assert verbose["co_agents"]["note"].startswith("1 other LIVE agent(s)")
    finally:
        srv._pool = saved


async def test_orient_terse_promotes_open_threads_more_as_a_structured_sibling(
    actions: Actions,
) -> None:
    """The one genuinely NEW field this build adds: open_threads_note (prose) is redundant
    with open_threads_more (a plain int) once that sibling exists — so the fact ('N more not
    shown') survives terse mode even though the sentence explaining it doesn't. Without the
    sibling, stripping the note would have been a silent regression (the reachability().detail
    lesson) — this proves the promotion, not just the strip."""
    from src import mcp_server as srv
    from src.orchestrator.compositions import ORIENT_OPEN_THREADS

    for i in range(ORIENT_OPEN_THREADS + 3):
        await open_thread(actions, f"terse-render wall filler #{i}", repo="trsorente",
                          kind="obligation", source="session")
    saved = srv._pool
    srv._pool = actions.pool
    try:
        terse = await srv.orient(project="trsorente")
        verbose = await srv.orient(project="trsorente", verbose=True)
        assert terse["open_threads_more"] == 3
        assert "open_threads_note" not in terse
        assert verbose["open_threads_more"] == 3
        assert verbose["open_threads_note"].startswith("showing 25 of 28 open threads")
    finally:
        srv._pool = saved


async def test_orient_unmounted_terse_drops_the_redundant_note(actions: Actions) -> None:
    """'who' already says 'call mount(cwd) first' when unmounted — the top-level note
    restates it; safe to drop in terse, present in verbose."""
    from src import mcp_server as srv

    saved = srv._pool
    srv._pool = actions.pool
    try:
        terse = await srv.orient()
        verbose = await srv.orient(verbose=True)
        assert "note" not in terse
        assert verbose["note"].startswith("un-mounted →")
        assert terse["fleet_map"] == verbose["fleet_map"]
    finally:
        srv._pool = saved
