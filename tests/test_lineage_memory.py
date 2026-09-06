"""LINEAGE MEMORY CUSTODY (thread 4dcc1849, decision f9e47d3c) — the sentinel +
archive-on-collision mechanism for Claude Code's own harness-native memory files."""
from __future__ import annotations

from pathlib import Path

import pytest
from src.orchestrator.lineage_memory import (
    MemoryCustodyResult,
    claude_memory_dir,
    encode_claude_project_slug,
    ensure_lineage_memory_custody,
    stamp_lineage_sentinel,
)

# THREE LIVE SPECIMENS (verified against real ~/.claude/projects/ directories, not
# assumed): a plain repo, a worktree, and a seat office's dotted ~/.osiris path. The
# rule confirmed by all three: every '/' AND every '.' in cwd becomes '-'.
_SPECIMENS = [
    ("/home/asuramaya/code/osiris", "-home-asuramaya-code-osiris"),
    ("/home/asuramaya/code/osiris/.claude/worktrees/imhotep",
     "-home-asuramaya-code-osiris--claude-worktrees-imhotep"),
    ("/home/asuramaya/.osiris/seats/aegis", "-home-asuramaya--osiris-seats-aegis"),
]


def test_encode_claude_project_slug_matches_live_specimens() -> None:
    for cwd, expected_slug in _SPECIMENS:
        assert encode_claude_project_slug(cwd) == expected_slug


def test_claude_memory_dir_resolves_under_the_encoded_slug(tmp_path: Path) -> None:
    cwd, slug = _SPECIMENS[0]
    assert claude_memory_dir(cwd, home=tmp_path) == (
        tmp_path / ".claude" / "projects" / slug / "memory")


def test_ensure_custody_is_a_noop_when_the_memory_dir_does_not_exist_yet(
    tmp_path: Path,
) -> None:
    result = ensure_lineage_memory_custody("/home/asuramaya/code/osiris",
                                            "agent:freshlineage", home=tmp_path)
    assert result == MemoryCustodyResult(action="noop")


def test_ensure_custody_is_a_noop_when_the_sentinel_already_names_this_lineage(
    tmp_path: Path,
) -> None:
    cwd = "/home/asuramaya/code/osiris"
    mem_dir = claude_memory_dir(cwd, home=tmp_path)
    mem_dir.mkdir(parents=True)
    (mem_dir / ".osiris-lineage").write_text("agent:samelineage", encoding="utf-8")
    (mem_dir / "MEMORY.md").write_text("- some note", encoding="utf-8")

    result = ensure_lineage_memory_custody(cwd, "agent:samelineage", home=tmp_path)

    assert result == MemoryCustodyResult(action="noop")
    assert mem_dir.is_dir()  # nothing moved
    assert (mem_dir / "MEMORY.md").read_text(encoding="utf-8") == "- some note"


def test_ensure_custody_flags_migration_needed_for_pre_existing_content_with_no_sentinel(
    tmp_path: Path,
) -> None:
    """Pre-existing content from before this system shipped must NEVER be silently
    moved — the membrane law (constitution #6): the loop may close, but never
    silently. This is the one case that stays untouched no matter what."""
    cwd = "/home/asuramaya/code/osiris"
    mem_dir = claude_memory_dir(cwd, home=tmp_path)
    mem_dir.mkdir(parents=True)
    (mem_dir / "MEMORY.md").write_text("- real, pre-existing memory", encoding="utf-8")

    result = ensure_lineage_memory_custody(cwd, "agent:newlineage", home=tmp_path)

    assert result.action == "migration_needed"
    assert result.path == str(mem_dir)
    assert mem_dir.is_dir()  # untouched
    assert (mem_dir / "MEMORY.md").read_text(encoding="utf-8") == "- real, pre-existing memory"


def test_ensure_custody_archives_a_different_lineages_memory_sideways(
    tmp_path: Path,
) -> None:
    """The actual collision case the ruling exists for: rename, never delete —
    constitution #3, heal with compensating events, never DELETE."""
    cwd = "/home/asuramaya/code/osiris"
    mem_dir = claude_memory_dir(cwd, home=tmp_path)
    mem_dir.mkdir(parents=True)
    (mem_dir / ".osiris-lineage").write_text("agent:oldlineage", encoding="utf-8")
    (mem_dir / "MEMORY.md").write_text("- the old lineage's own notes", encoding="utf-8")

    result = ensure_lineage_memory_custody(cwd, "agent:newlineage", home=tmp_path)

    assert result.action == "archived"
    assert result.prior_lineage == "agent:oldlineage"
    assert result.path is not None
    archived = Path(result.path)
    assert archived.is_dir()
    assert archived.name.startswith("memory.archived-agent_oldlineage-")
    assert (archived / "MEMORY.md").read_text(encoding="utf-8") == (
        "- the old lineage's own notes")
    assert not mem_dir.exists()  # renamed away, not copied — nothing left at the old path


def test_ensure_custody_degrades_to_noop_when_the_archive_rename_itself_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Best-effort, never raises: memory-custody plumbing must never be able to fail
    a mount, even mid-archive (a real collision found, then the rename itself hits a
    filesystem error — e.g. a cross-device move, or a permission race)."""
    cwd = "/home/asuramaya/code/osiris"
    mem_dir = claude_memory_dir(cwd, home=tmp_path)
    mem_dir.mkdir(parents=True)
    (mem_dir / ".osiris-lineage").write_text("agent:oldlineage", encoding="utf-8")

    def _boom(self: Path, target: object) -> None:
        raise OSError("simulated rename failure")

    monkeypatch.setattr(Path, "rename", _boom)

    result = ensure_lineage_memory_custody(cwd, "agent:newlineage", home=tmp_path)

    assert result == MemoryCustodyResult(action="noop")
    assert mem_dir.is_dir()  # the failed rename left the original directory in place


def test_stamp_lineage_sentinel_creates_the_directory_and_writes_the_owner(
    tmp_path: Path,
) -> None:
    cwd = "/home/asuramaya/code/osiris"
    stamp_lineage_sentinel(cwd, "agent:brandnew", home=tmp_path)

    mem_dir = claude_memory_dir(cwd, home=tmp_path)
    assert mem_dir.is_dir()
    assert (mem_dir / ".osiris-lineage").read_text(encoding="utf-8") == "agent:brandnew"


def test_stamp_lineage_sentinel_refreshes_an_existing_sentinel(tmp_path: Path) -> None:
    cwd = "/home/asuramaya/code/osiris"
    stamp_lineage_sentinel(cwd, "agent:first", home=tmp_path)
    stamp_lineage_sentinel(cwd, "agent:second", home=tmp_path)

    mem_dir = claude_memory_dir(cwd, home=tmp_path)
    assert (mem_dir / ".osiris-lineage").read_text(encoding="utf-8") == "agent:second"
