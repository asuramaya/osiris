"""Bootstrap — migrate a project's markdown memory into the shared graph.

The generalization of the osiris md-kill into a reusable onboarding: a project's CLAUDE.md
log / DESIGN.md / memory essays become retrieval-sized Reference nodes, the project is
registered, and a boot-sector is suggested (Osiris never writes the project's files).
"""
from __future__ import annotations

from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.bootstrap import bootstrap_project

_LOG = """# CLAUDE.md — sibling-two

## Build order
- **Phase 0 (DONE, 2026-05-01):** the megatron core — """ + "spark " * 90 + """
- **THE ALLSPARK NIGHT (DONE, 2026-06-02):** convergence — """ + "energon " * 60 + """
"""
_ESSAY = "---\nname: decept-strategy\ndescription: the plan\n---\n\n# Strategy\n\nRoll out.\n"


async def _make_project(tmp: Path) -> Path:
    root = tmp / "sibling-two"
    (root / "memory").mkdir(parents=True)
    (root / "CLAUDE.md").write_text(_LOG)
    (root / "memory" / "decept-strategy.md").write_text(_ESSAY)
    (root / "memory" / "MEMORY.md").write_text("# index (skipped)\n")
    (root / "README.md").write_text("# public export — must NOT be migrated\n")
    return root


async def test_bootstrap_migrates_memory_and_registers_project(
    actions: Actions, tmp_path: Path
) -> None:
    root = await _make_project(tmp_path)
    res = await bootstrap_project(actions, str(root))

    assert res["project"] == "sibling-two"
    # the dated build-log bullets became their OWN retrieval-sized nodes, namespaced
    allspark = await actions.pool.fetchrow(
        "SELECT o.canonical, (SELECT value#>>'{}' FROM current_assertions a "
        " WHERE a.object_id=o.id AND a.name='date') AS d FROM objects o "
        "WHERE o.type='Reference' AND o.canonical LIKE 'ref:sibling-two-history-%' "
        "AND EXISTS (SELECT 1 FROM current_assertions a WHERE a.object_id=o.id "
        "  AND a.name='name' AND a.value#>>'{}' ILIKE '%ALLSPARK%')")
    assert allspark is not None and allspark["d"] == "2026-06-02"
    # the essay became canon (titled from its frontmatter name)
    assert await actions.pool.fetchval(
        "SELECT 1 FROM objects WHERE type='Reference' AND canonical='ref:decept-strategy'")
    # the public README was NOT migrated (it's an export, not memory)
    assert not await actions.pool.fetchval(
        "SELECT 1 FROM objects WHERE type='Reference' AND canonical LIKE '%readme%'")
    # the project is registered, and a boot sector is suggested (not written)
    assert await actions.pool.fetchval(
        "SELECT 1 FROM objects WHERE type='SoftwareProject' AND canonical='repo:sibling-two'")
    assert "mount(cwd=" in res["suggested_boot_sector"]
    assert "sibling-two constitution" in res["suggested_boot_sector"]
    assert (root / "CLAUDE.md").read_text() == _LOG  # NO HANDS: the file is untouched


async def test_bootstrap_links_every_ingested_reference_in_repo(
    actions: Actions, tmp_path: Path
) -> None:
    """Operator ruling, 2026-08-27, on decision 49231693's trace of Reference's 37%
    orphan rate: bootstrap_project resolves/creates the SoftwareProject it's onboarding
    THEN threw that identity away calling ingest_log/ingest_reference_doc with no repo=
    — a doc only gets migrated because someone was working a project, and that context
    existed at write time. Every log-chunk Reference AND every essay Reference must now
    carry a live in_repo edge to the project bootstrap just registered."""
    root = await _make_project(tmp_path)
    await bootstrap_project(actions, str(root))

    unlinked_logs = await actions.pool.fetchval(
        "SELECT count(*) FROM objects o WHERE o.type='Reference' "
        "AND o.canonical LIKE 'ref:sibling-two-history-%' AND NOT EXISTS ("
        "  SELECT 1 FROM links l JOIN objects p ON p.id=l.to_id "
        "  WHERE l.from_id=o.id AND l.type='in_repo' AND p.canonical='repo:sibling-two')")
    assert unlinked_logs == 0

    essay_linked = await actions.pool.fetchval(
        "SELECT 1 FROM objects o JOIN links l ON l.from_id=o.id "
        "JOIN objects p ON p.id=l.to_id WHERE o.type='Reference' "
        "AND o.canonical='ref:decept-strategy' AND l.type='in_repo' "
        "AND p.canonical='repo:sibling-two'")
    assert essay_linked == 1


async def test_bootstrap_stamps_writes_with_the_given_source_not_a_hardcoded_literal(
    actions: Actions, tmp_path: Path
) -> None:
    """NEGATIVE CONTROL (2026-08-03, Thoth's Tier 1 dispatch off the silent-authority
    census, decision 497a066a): every write bootstrap_project makes used to be hardcoded
    "ref:osiris" regardless of who actually called it — worse than anonymous, a bad
    injection read as deliberate system canon and could not be traced back to a caller
    even after the fact. Confirmed against pre-fix code (git stash): the SoftwareProject's
    own `name` assertion and a migrated log entry's `name` assertion both carried
    source_id='ref:osiris' no matter what caller identity was passed in."""
    root = await _make_project(tmp_path)
    res = await bootstrap_project(actions, str(root), source="agent:realcaller")

    proj_source = await actions.pool.fetchval(
        "SELECT source_id FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.type='SoftwareProject' AND o.canonical='repo:sibling-two' "
        "AND a.name='name'")
    assert proj_source == "agent:realcaller"

    essay_source = await actions.pool.fetchval(
        "SELECT source_id FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.type='Reference' AND o.canonical LIKE 'ref:sibling-two-history-%' "
        "AND a.name='name' "
        "AND a.value#>>'{}' ILIKE '%ALLSPARK%'")
    assert essay_source == "agent:realcaller"
    assert res["project"] == "sibling-two"  # the fix didn't disturb the actual migration


async def test_bootstrap_defaults_source_to_ref_osiris_for_the_bare_cli_case(
    actions: Actions, tmp_path: Path
) -> None:
    """The bare-CLI entrypoint (bootstrap.main(), no MCP ctx exists at all) has no caller
    identity to thread through — this proves the default is unchanged, not merely that
    passing a source works."""
    root = await _make_project(tmp_path)
    await bootstrap_project(actions, str(root))
    proj_source = await actions.pool.fetchval(
        "SELECT source_id FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.type='SoftwareProject' AND o.canonical='repo:sibling-two' "
        "AND a.name='name'")
    assert proj_source == "ref:osiris"


async def test_bootstrap_is_idempotent(actions: Actions, tmp_path: Path) -> None:
    root = await _make_project(tmp_path)
    r1 = await bootstrap_project(actions, str(root))
    n1 = await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type='Reference'")
    r2 = await bootstrap_project(actions, str(root))
    n2 = await actions.pool.fetchval("SELECT count(*) FROM objects WHERE type='Reference'")
    assert r1["entries"] == r2["entries"] and n1 == n2  # re-run mints nothing new


async def test_bootstrap_empty_project_is_a_clean_noop(
    actions: Actions, tmp_path: Path
) -> None:
    root = tmp_path / "greenfield"
    root.mkdir()
    res = await bootstrap_project(actions, str(root))
    assert res["entries"] == 0 and res["ingested"] == []
    assert "start clean" in res["note"]
    # a greenfield project still gets registered (so agents can work_in it)
    assert await actions.pool.fetchval(
        "SELECT 1 FROM objects WHERE type='SoftwareProject' AND canonical='repo:greenfield'")


class _Ctx:
    class request_context:  # noqa: N801
        request = None
        session = object()


async def test_bootstrap_wrapper_stamps_writes_with_the_callers_own_mounted_identity(
    actions: Actions, tmp_path: Path
) -> None:
    """The MCP tool wrapper (mcp_server.bootstrap), not just the core function: a mounted
    caller's writes must carry THEIR OWN identity, same pattern test_charter.py's stranger
    tests use for a fake connection. Same dispatch/decision as the core-function test above."""
    from src import mcp_server as srv
    from src.orchestrator.agents import AgentIdentity

    root = await _make_project(tmp_path)
    ctx = _Ctx()
    saved_pool = srv._pool
    srv._pool = actions.pool
    srv._agents[srv._conn_key(ctx)] = AgentIdentity(
        agent_id="agent:mountedcaller", session="s1", project="sibling-two", model=None,
        cwd=None)
    try:
        await srv.bootstrap(str(root), ctx=ctx)
    finally:
        srv._pool = saved_pool
        srv._agents.pop(srv._conn_key(ctx), None)
    proj_source = await actions.pool.fetchval(
        "SELECT source_id FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.type='SoftwareProject' AND o.canonical='repo:sibling-two' "
        "AND a.name='name'")
    assert proj_source == "agent:mountedcaller"


async def test_bootstrap_wrapper_falls_back_to_session_when_unmounted(
    actions: Actions, tmp_path: Path
) -> None:
    """An unmounted caller still bootstraps (never gated) — its writes carry the same
    coarse "session" bucket every other unmounted write already uses, never the old fixed
    literal "ref:osiris" that masqueraded as a deliberate system source."""
    from src import mcp_server as srv

    root = await _make_project(tmp_path)
    saved_pool = srv._pool
    srv._pool = actions.pool
    try:
        await srv.bootstrap(str(root))
    finally:
        srv._pool = saved_pool
    proj_source = await actions.pool.fetchval(
        "SELECT source_id FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.type='SoftwareProject' AND o.canonical='repo:sibling-two' "
        "AND a.name='name'")
    assert proj_source == "session"


async def test_bootstrap_discovers_docs_layout_not_just_osiris_shape(
    actions: Actions, tmp_path: Path
) -> None:
    """A repo that keeps its memory under docs/ (not memory/) must be FULLY migrated — the
    dogfood gap sibling-one surfaced: bootstrap was osiris-shaped and would have silently dropped
    the whole docs/ pile, migrating only the root CLAUDE.md."""
    root = tmp_path / "sibling-one"
    (root / "docs").mkdir(parents=True)
    (root / "web").mkdir()
    (root / "CLAUDE.md").write_text(_LOG)
    (root / "PLAN.md").write_text("# Plan\n\nship the probe library.\n")
    (root / "docs" / "DESIGN.md").write_text(
        "# Design\n\n## The between\n" + "geometry " * 80 + "\n")
    (root / "docs" / "session11-findings.md").write_text(
        "# Session 11 findings\n\nthe null-shart protocol held.\n")
    (root / "docs" / "probe-library.md").write_text("# Probe library\n\nthe probes.\n")
    (root / "web" / "README.md").write_text("# public web export — must NOT migrate\n")

    res = await bootstrap_project(actions, str(root))

    # the docs/ notes each became a Reference node (the pile that used to be dropped)
    for canon in ("ref:session11-findings", "ref:probe-library", "ref:plan"):
        assert await actions.pool.fetchval(
            "SELECT 1 FROM objects WHERE type='Reference' AND canonical=$1", canon), canon
    # docs/DESIGN.md was chunked as a LOG under the design topic, not a single doc node
    assert await actions.pool.fetchval(
        "SELECT 1 FROM objects WHERE type='Reference' "
        "AND canonical LIKE 'ref:sibling-one-design-%'")
    # the public web/ tree was NOT swept (only docs/ | memory/ | paper/ + root PLAN.md)
    assert not await actions.pool.fetchval(
        "SELECT 1 FROM objects WHERE type='Reference' AND canonical LIKE '%readme%'")
    # the displayed manifest names the docs/ files by their real relative path
    files = {i["file"] for i in res["ingested"]}
    assert "docs/session11-findings.md" in files and "PLAN.md" in files
