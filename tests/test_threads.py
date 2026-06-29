"""Thread mining — the project's open questions, derived from its own commit rationale."""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.ingest.threads import extract_threads, mine_threads

NOW = datetime(2026, 6, 28, tzinfo=UTC)


def test_extract_threads_is_high_signal() -> None:
    body = (
        "Shipped the dossier. DONE, proven live.\n"
        "THE WALL: running this live needs a satellite with portal access.\n"
        "NEXT: wire the renderer and fold the watch in.\n"
        "This handles pending handoffs nicely and the tray opens."
    )
    out = extract_threads(body)
    assert any("THE WALL" in t for t in out)          # a wall is surfaced
    assert any(t.startswith("NEXT:") for t in out)    # a next-step is surfaced
    assert not any("handoffs" in t for t in out)      # 'pending' is no longer a marker (noise)
    assert not any("DONE" in t for t in out)          # a CLOSED sentence is skipped


async def test_mine_threads_creates_linked_threads(actions: Actions) -> None:
    cm = await actions.create_or_find_object("Commit", "commit:abc", "git")
    await actions.assert_property(
        cm, "rationale",
        "Built the satellite seam. THE WALL: running this live needs portal access.",
        "git", NOW, 0.85)

    res = await mine_threads(actions)
    assert res["threads"] == 1 and res["commits_scanned"] == 1
    p = actions.pool
    summary = await p.fetchval(
        "SELECT value #>> '{}' FROM current_assertions a JOIN objects o ON o.id=a.object_id "
        "WHERE o.type='Thread' AND a.name='summary'")
    assert "THE WALL" in summary
    # graded DERIVED (a mined inference, not authoritative) + linked to its commit
    ec = await p.fetchval("SELECT evidence_class FROM links WHERE type='noted_in' LIMIT 1")
    assert ec == "derived"
    # idempotent: re-mining the same body adds nothing new
    again = await mine_threads(actions)
    assert await p.fetchval("SELECT count(*) FROM objects WHERE type='Thread'") == 1
    assert again["threads"] == 1  # found it, but find-or-create deduped
