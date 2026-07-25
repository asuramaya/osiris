"""TERSE-RENDER (task #55, thread 9092ed51) — a verbosity dial on MCP tool receipts. Terse
(verbose=False, the default) drops guidance PROSE from mount()/orient(); every structured
fact a caller could parse (counts, ids, lists — fleet_open_threads_total, co_agents.live,
open_threads_more) survives in BOTH modes. The regression guard throughout: verbose's
payload is always a strict superset of terse's — remove exactly the declared keys from
verbose and you get terse back, byte-for-byte, nothing else moves (the additive-only golden
shape, mirroring surface.py's own byte-exact convention).

TASK #60 (thread b81b0fac) extends the same discipline to DATA, not prose: _terse() only
ever deletes a whole key, but the byte measurement found open_threads/recent_decisions
summaries are 96-98% of orient()'s bytes — a fact no key-deletion could touch. _cap_text()
truncates those summaries to 160 chars in terse mode (an explicit '…' marks it, unlike the
existing but silent [:160]/[:800] precedents elsewhere in this file); every decision now
also carries `id` so a capped summary stays addressable via verbose=True or search().
"""
from __future__ import annotations

from datetime import UTC, datetime
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


async def test_mount_terse_keeps_co_agents_note_in_both_modes(
    actions: Actions, tmp_path: Path,
) -> None:
    """CORRECTION (Thoth's review, DM 1238, thread 1233): co_agents.note is the shared-tree
    SAFETY WARNING ('never git add -A, stage your own hunks') — the `live` list says WHO is
    here, this says WHAT TO DO about it. Not redundant guidance; stays in BOTH modes, same
    class as the identity-safety banners already left untouched."""
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
        assert terse["co_agents"]["note"].startswith("1 other LIVE agent(s)")

        verbose = await srv.mount(cwd=str(tmp_path / proj),
                                  job_dir=str(tmp_path / "jobs" / "trscme02"), verbose=True)
        assert verbose["co_agents"]["note"].startswith("2 other LIVE agent(s)")
        assert verbose["co_agents"].keys() == terse["co_agents"].keys()
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


async def test_orient_terse_keeps_co_agents_note_in_both_modes(
    actions: Actions, tmp_path: Path,
) -> None:
    """Same correction as mount()'s: co_agents.note is safety guidance, not redundant
    prose — present in both terse and verbose."""
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
        assert terse["co_agents"]["note"].startswith("1 other LIVE agent(s)")
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


async def test_orient_unmounted_terse_keeps_its_note_in_both_modes(actions: Actions) -> None:
    """CORRECTION (Thoth's review, DM 1238, thread 1233): a pre-existing test
    (test_unmounted_orient_is_a_bounded_map_never_the_firehose) asserts this note
    unconditionally — restoring the tested contract rather than re-litigating it inside
    the same fix that caught the co_agents.note regression. Terse and verbose are
    identical for the un-mounted branch; nothing here was terse-safe to strip after all."""
    from src import mcp_server as srv

    saved = srv._pool
    srv._pool = actions.pool
    try:
        terse = await srv.orient()
        verbose = await srv.orient(verbose=True)
        assert terse["note"].startswith("un-mounted →")
        assert terse == verbose
    finally:
        srv._pool = saved


# ═══ _cap_text() — the DATA-VOLUME mechanism (task #60, thread b81b0fac) ═════════════════


def test_cap_text_truncates_and_marks_a_long_value() -> None:
    from src.mcp_server import _cap_text

    items = [{"summary": "x" * 200}]
    out = _cap_text(items, "summary", limit=160)
    assert out[0]["summary"] == "x" * 160 + "…"


def test_cap_text_leaves_a_short_value_unmarked() -> None:
    """A value AT or under the limit is never touched — no marker on something that isn't
    actually truncated, or a caller can't trust the marker's own meaning."""
    from src.mcp_server import _cap_text

    items = [{"summary": "short and sweet"}]
    out = _cap_text(items, "summary", limit=160)
    assert out[0]["summary"] == "short and sweet"


def test_cap_text_ignores_a_missing_or_non_string_key() -> None:
    from src.mcp_server import _cap_text

    items = [{"kind": "ruling"}, {"summary": None}]
    out = _cap_text(items, "summary", limit=160)
    assert out == [{"kind": "ruling"}, {"summary": None}]


# ═══ orient() data-volume — the tool-level integration ═══════════════════════════════════


async def test_orient_terse_caps_a_long_decision_summary_and_carries_its_id(
    actions: Actions,
) -> None:
    """The measured win: terse shortens+marks the summary; verbose restores it in full;
    both carry the SAME `id`, so a capped decision stays addressable either way."""
    from src import mcp_server as srv
    from src.orchestrator.compositions import seed_default_compositions

    proj = await actions.create_or_find_object("SoftwareProject", "repo:trsdecap", "session")
    d = await actions.create_or_find_object("Decision", "decision:trsdecap-1", "session")
    long_summary = "a very long ruling that goes on and on " * 10
    await actions.assert_property(d, "summary", long_summary, "session", datetime.now(UTC), 0.9)
    await actions.assert_property(d, "kind", "ruling", "session", datetime.now(UTC), 0.9)
    await actions.create_link(d, proj, "in_repo", "session", datetime.now(UTC), 0.9)
    await seed_default_compositions(actions.pool)

    saved = srv._pool
    srv._pool = actions.pool
    try:
        terse = await srv.orient(project="trsdecap")
        verbose = await srv.orient(project="trsdecap", verbose=True)
        t_row = next(r for r in terse["recent_decisions"] if r["id"] == str(d)[:8])
        v_row = next(r for r in verbose["recent_decisions"] if r["id"] == str(d)[:8])
        assert t_row["summary"] == long_summary[:160] + "…"
        assert v_row["summary"] == long_summary
    finally:
        srv._pool = saved


async def test_orient_terse_caps_a_long_thread_summary(actions: Actions) -> None:
    from src import mcp_server as srv

    long_summary = "a very long open thread that goes on and on " * 10
    await open_thread(actions, long_summary, repo="trsthcap", kind="obligation",
                      source="session")

    saved = srv._pool
    srv._pool = actions.pool
    try:
        terse = await srv.orient(project="trsthcap")
        verbose = await srv.orient(project="trsthcap", verbose=True)
        assert terse["open_threads"][0]["summary"] == long_summary[:160] + "…"
        assert verbose["open_threads"][0]["summary"] == long_summary
    finally:
        srv._pool = saved


async def test_orient_recent_decisions_more_only_past_a_full_page(actions: Actions) -> None:
    """Symmetry with open_threads_more (task #55): the composition's own take(n=15) means a
    full page MAY hide more — count for real rather than assume, and stay silent under a
    full page (nothing hidden, nothing to report)."""
    from src import mcp_server as srv
    from src.orchestrator.compositions import seed_default_compositions

    proj = await actions.create_or_find_object("SoftwareProject", "repo:trsdmore", "session")
    for i in range(16):
        d = await actions.create_or_find_object("Decision", f"decision:trsdmore-{i}", "session")
        await actions.assert_property(d, "summary", f"ruling #{i}", "session",
                                      datetime.now(UTC), 0.9)
        await actions.create_link(d, proj, "in_repo", "session", datetime.now(UTC), 0.9)

    proj2 = await actions.create_or_find_object("SoftwareProject", "repo:trsdfew", "session")
    d2 = await actions.create_or_find_object("Decision", "decision:trsdfew-1", "session")
    await actions.assert_property(d2, "summary", "the only ruling", "session",
                                  datetime.now(UTC), 0.9)
    await actions.create_link(d2, proj2, "in_repo", "session", datetime.now(UTC), 0.9)
    await seed_default_compositions(actions.pool)

    saved = srv._pool
    srv._pool = actions.pool
    try:
        many = await srv.orient(project="trsdmore")
        few = await srv.orient(project="trsdfew")
        assert many["recent_decisions_more"] == 1
        assert "recent_decisions_more" not in few
    finally:
        srv._pool = saved
