"""Thread mining — the project's open questions, derived from its own commit rationale."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.actions.core import Actions
from src.ingest.threads import extract_threads, mine_threads, resolve_threads

NOW = datetime(2026, 6, 28, tzinfo=UTC)


async def _commit(actions: Actions, canon: str, date: str, **props: str) -> None:
    cm = await actions.create_or_find_object("Commit", canon, "git")
    await actions.assert_property(cm, "authored_date", date, "git",
                                  datetime.fromisoformat(date), 0.85)
    for name, value in props.items():
        await actions.assert_property(cm, name, value, "git",
                                      datetime.fromisoformat(date), 0.85)


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


async def _thread_status(p: Any, like: str) -> tuple[str, str | None, str | None]:
    return await p.fetchrow(  # type: ignore[no-any-return]
        "SELECT "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='status') AS status, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='resolved_in') AS resolved_in, "
        " (SELECT value #>> '{}' FROM current_assertions a "
        "  WHERE a.object_id=o.id AND a.name='resolved_because') AS because "
        "FROM objects o JOIN current_assertions s ON s.object_id=o.id "
        "WHERE o.type='Thread' AND s.name='summary' AND s.value #>> '{}' LIKE $1", like)


async def test_resolve_threads_self_heals_addressed_threads(actions: Actions) -> None:
    """A later commit that addressed a thread closes it (the briefing self-heals); a wall
    with no follow-up stays open; the closer must be strictly LATER than the raising commit."""
    p = actions.pool
    # one commit raises two threads: a satellite WALL and a renderer NEXT-step
    await _commit(actions, "commit:org", "2026-06-20T00:00:00+00:00",
                  rationale="THE WALL: the satellite needs portal access. "
                            "NEXT: the generic renderer for compositions.")
    await mine_threads(actions)
    # a PRIOR renderer commit must NOT resolve it (can't close a thread before it's raised)
    await _commit(actions, "commit:pre", "2026-06-10T00:00:00+00:00",
                  summary="early generic renderer spike")
    # the real closer — strictly later, shares >=2 distinctive tokens (generic, renderer)
    await _commit(actions, "commit:later", "2026-06-25T00:00:00+00:00",
                  summary="implement the generic renderer", scope="renderer")

    res = await resolve_threads(actions)
    assert res["resolved"] == 1 and res["open_remaining"] == 1

    status, resolved_in, because = await _thread_status(p, "%renderer%")
    assert status == "resolved"
    assert resolved_in == "commit:later"          # the LATER commit, not the prior spike
    assert "renderer" in because                  # auditable: WHY it was closed

    wall_status, _, _ = await _thread_status(p, "%satellite%")
    assert wall_status == "open"                   # no follow-up → stays a live wall

    # idempotent: a second pass closes nothing new
    again = await resolve_threads(actions)
    assert again["resolved"] == 0
