"""Cross-repo developer-identity resolution — the first real ER case for the developer persona.

The same person commits under several emails (personal vs GitHub no-reply) across repos, so a
multi-repo graph fragments them into multiple `dev:` Person nodes and double-counts you. The
OSINT resolver misses it (same name, different email, no DOB/employer). This blocks dev Persons
by name/handle and surfaces the merge for review — never auto-merging (#3), candidate-gated.
"""
from __future__ import annotations

from datetime import UTC, datetime

from src.actions.core import Actions
from src.ontology.resolution import _github_handle, find_dev_identity_candidates

NOW = datetime(2026, 6, 28, tzinfo=UTC)


def test_github_handle_parse() -> None:
    assert _github_handle("69973947+asuramaya@users.noreply.github.com") == "asuramaya"
    assert _github_handle("asuramaya@users.noreply.github.com") == "asuramaya"
    assert _github_handle("dakota.jm@gmail.com") is None        # a real address, not a handle
    assert _github_handle("") is None


async def _dev(actions: Actions, email: str, name: str) -> str:
    p = await actions.create_or_find_object("Person", f"dev:{email}", "git")
    await actions.assert_property(p, "name", name, "git", NOW, 0.85)
    await actions.assert_property(p, "email", email, "git", NOW, 0.85)
    return str(p)


async def test_find_dev_identity_candidates(actions: Actions) -> None:
    # the same person, two emails across repos — the no-reply email encodes the handle
    a = await _dev(actions, "dakota.jm@gmail.com", "asuramaya")
    b = await _dev(actions, "69973947+asuramaya@users.noreply.github.com", "asuramaya")
    # a genuinely different developer — must NOT match
    await _dev(actions, "ada@x.io", "Ada Lovelace")

    n = await find_dev_identity_candidates(actions.pool)
    assert n == 1                                                   # exactly the asuramaya pair
    p = actions.pool
    row = await p.fetchrow(
        "SELECT a_id::text a, b_id::text b, score, reasons FROM merge_candidates")
    assert {row["a"], row["b"]} == {a, b}
    assert row["score"] >= 0.89                                     # ~0.9 (float4); no-reply==name
    assert "github handle" in str(row["reasons"])
    # ruling #3: never auto-merged — all three dev Persons still active
    assert await p.fetchval(
        "SELECT count(*) FROM objects WHERE type='Person' AND status='active'") == 3
    # idempotent — re-running queues nothing new
    assert await find_dev_identity_candidates(p) == 0


async def test_shared_dev_name_is_a_weaker_lead(actions: Actions) -> None:
    """Two devs with the same plain name but no handle link are a weaker (0.6) candidate —
    a lead to review, not a confident match."""
    a = await _dev(actions, "j@a.com", "jordan")
    b = await _dev(actions, "j@b.com", "jordan")
    assert await find_dev_identity_candidates(actions.pool) == 1
    score = await actions.pool.fetchval("SELECT score FROM merge_candidates")
    assert 0.55 < score < 0.7
    assert {a, b}                                                   # both ids referenced
