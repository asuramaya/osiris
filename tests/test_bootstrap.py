"""Bootstrap — migrate a project's markdown memory into the shared graph.

The generalization of the osiris md-kill into a reusable onboarding: a project's CLAUDE.md
log / DESIGN.md / memory essays become retrieval-sized Reference nodes, the project is
registered, and a boot-sector is suggested (Osiris never writes the project's files).
"""
from __future__ import annotations

from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.bootstrap import bootstrap_project

_LOG = """# CLAUDE.md — decepticons

## Build order
- **Phase 0 (DONE, 2026-05-01):** the megatron core — """ + "spark " * 90 + """
- **THE ALLSPARK NIGHT (DONE, 2026-06-02):** convergence — """ + "energon " * 60 + """
"""
_ESSAY = "---\nname: decept-strategy\ndescription: the plan\n---\n\n# Strategy\n\nRoll out.\n"


async def _make_project(tmp: Path) -> Path:
    root = tmp / "decepticons"
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

    assert res["project"] == "decepticons"
    # the dated build-log bullets became their OWN retrieval-sized nodes, namespaced
    allspark = await actions.pool.fetchrow(
        "SELECT o.canonical, (SELECT value#>>'{}' FROM current_assertions a "
        " WHERE a.object_id=o.id AND a.name='date') AS d FROM objects o "
        "WHERE o.type='Reference' AND o.canonical LIKE 'ref:decepticons-history-%' "
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
        "SELECT 1 FROM objects WHERE type='SoftwareProject' AND canonical='repo:decepticons'")
    assert "mount(cwd=" in res["suggested_boot_sector"]
    assert "decepticons constitution" in res["suggested_boot_sector"]
    assert (root / "CLAUDE.md").read_text() == _LOG  # NO HANDS: the file is untouched


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


async def test_bootstrap_discovers_docs_layout_not_just_osiris_shape(
    actions: Actions, tmp_path: Path
) -> None:
    """A repo that keeps its memory under docs/ (not memory/) must be FULLY migrated — the
    dogfood gap Heinrich surfaced: bootstrap was osiris-shaped and would have silently dropped
    the whole docs/ pile, migrating only the root CLAUDE.md."""
    root = tmp_path / "heinrich"
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
        "SELECT 1 FROM objects WHERE type='Reference' AND canonical LIKE 'ref:heinrich-design-%'")
    # the public web/ tree was NOT swept (only docs/ | memory/ | paper/ + root PLAN.md)
    assert not await actions.pool.fetchval(
        "SELECT 1 FROM objects WHERE type='Reference' AND canonical LIKE '%readme%'")
    # the displayed manifest names the docs/ files by their real relative path
    files = {i["file"] for i in res["ingested"]}
    assert "docs/session11-findings.md" in files and "PLAN.md" in files
