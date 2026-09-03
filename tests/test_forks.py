"""THE FORK — one mind, a new session id, and a transcript that swears it was born this morning.

`claude --fork-session --resume` continues a conversation under a BRAND-NEW session id and
rewrites `sessionId` on every copied record, so the fork is structurally indistinguishable from
a newborn. SessionStart fires, the whisper posts, automount seats it: ONE MIND, TWO SEATS.

Field evidence, and it is exquisite: Anubis XII (heinrich, msg 424) was FORCED TO FORK HIMSELF to
file the bug report, because his real seat's mail bounced as an impostor's. He described the twin
perfectly, from inside it, without knowing that was its name.

The join is a record uuid: a copy rewrites session ids but PRESERVES record uuids, so a session
whose FIRST record uuid was EMITTED BY another session is a fork of it. Free, on disk, and it
cannot be wrong.

The hardest-guarded test in this file is test_a_MENTION_is_not_AUTHORSHIP, because that is the bug
I nearly shipped inside the module that cures it.
"""

from __future__ import annotations

import json
from pathlib import Path

from src.actions.core import Actions
from src.orchestrator import mounts
from src.orchestrator.forks import _find, fork_key, resolve_parent, seat_of_fork


def _write(root: Path, project: str, sid: str, records: list[dict]) -> Path:
    d = root / project
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{sid}-0000-0000-0000-000000000000.jsonl"
    p.write_text("".join(json.dumps(r) + "\n" for r in records))
    return p


def _turn(uid: str, text: str = "hello") -> dict:
    return {"type": "user", "uuid": uid, "sessionId": "rewritten-by-the-fork",
            "message": {"role": "user", "content": text}}


async def _seat(actions: Actions, sid: str, agent_id: str, project: str = "repo") -> None:
    await mounts.save_mount(
        actions.pool, job_dir=f"/home/x/.claude/jobs/{sid}", agent_id=agent_id,
        project=project, cwd="/home/x/repo", model="claude-opus-4-8", session_key=f"t:{sid}")


async def test_a_fork_is_found_by_the_uuid_ITS_PARENT_EMITTED(
    actions: Actions, tmp_path: Path,
) -> None:
    """The whole thesis. The fork carries copied records; the copy kept their uuids."""
    _write(tmp_path, "-repo", "aaaaaaaa", [_turn("u1"), _turn("u2"), _turn("u3")])
    fork = _write(tmp_path, "-repo", "bbbbbbbb", [_turn("u2"), _turn("u3"), _turn("u9")])
    assert await resolve_parent(actions.pool, fork, root=tmp_path) == "aaaaaaaa"


async def test_a_MENTION_is_not_AUTHORSHIP(actions: Actions, tmp_path: Path) -> None:
    """THE BUG I ALMOST SHIPPED, IN THE FILE THAT CURES IT.

    The first cut scanned raw BYTES: "does this transcript contain that uuid?" But a transcript
    is full of text that is not its own — tool outputs, pasted files, greps of OTHER transcripts.
    The very session that wrote this module had another agent's record uuids sitting in its own
    scrollback, because it had gone and looked at them. A substring hit would have made THE
    READER the parent of the session it READ.

    That is an inference wearing the authority of a declaration — the named disease of this
    codebase — and a byte scan commits it silently, in the one place where being wrong re-seats
    a living mind onto a stranger.

    So the test is structural: a record whose OWN `uuid` field is the key. Merely quoting it —
    in a tool result, in a file, in prose — proves nothing and must count for nothing.
    """
    _write(tmp_path, "-repo", "aaaaaaaa", [_turn("u1"), _turn("u2")])
    # a BYSTANDER that merely read the parent's transcript and printed a uuid into its own log
    _write(tmp_path, "-repo", "cccccccc", [
        _turn("z1"),
        {"type": "assistant", "uuid": "z2", "toolUseResult": "I grepped it and saw uuid u1 there"},
    ])
    fork = _write(tmp_path, "-repo", "bbbbbbbb", [_turn("u1"), _turn("u2"), _turn("u9")])

    got = await resolve_parent(actions.pool, fork, root=tmp_path)
    assert got == "aaaaaaaa", "the parent EMITTED u1"
    assert got != "cccccccc", "the bystander merely QUOTED u1 — reading is not authorship"


async def test_a_session_that_is_NOBODYS_CHILD_is_left_alone(
    actions: Actions, tmp_path: Path,
) -> None:
    """A true original must not be re-parented onto anyone. Being wrong HERE would hand a
    stranger's whole identity — its mail, its seat, its succession — to a newborn."""
    _write(tmp_path, "-repo", "aaaaaaaa", [_turn("u1")])
    solo = _write(tmp_path, "-repo", "dddddddd", [_turn("q1"), _turn("q2")])
    assert await resolve_parent(actions.pool, solo, root=tmp_path) is None


async def test_a_fork_adopts_the_nearest_ancestor_THAT_HAS_A_SEAT(
    actions: Actions, tmp_path: Path,
) -> None:
    """NOT the transcript's root — the nearest ancestor the GRAPH already knows.

    A root is a fact about a FILE; a seat is a fact about the GRAPH. Thoth's chain roots at a
    session id the fleet has never heard of, while the fleet has known that mind as
    `agent:ad1a1cb0` for nine generations. Deriving an id from the root would invent a THIRD
    identity while curing a second one.
    """
    _write(tmp_path, "-repo", "aaaaaaaa", [_turn("u1")])                    # root: NO seat
    _write(tmp_path, "-repo", "bbbbbbbb", [_turn("u1"), _turn("u2")])       # middle: HAS a seat
    fork = _write(tmp_path, "-repo", "cccccccc", [_turn("u2"), _turn("u3")])
    await _seat(actions, "bbbbbbbb", "agent:a8c15486-xii")

    assert await seat_of_fork(actions.pool, fork, root=tmp_path) == "agent:a8c15486-xii"


async def test_forks_CHAIN_and_every_link_lands_on_the_one_seat(
    actions: Actions, tmp_path: Path,
) -> None:
    """A→B→C: a resumed session gets resumed again (15 of 36 in the field had more than one
    ancestor). Every generation is the SAME MIND and must land on the same seat, so the walk
    climbs past an unseated ancestor instead of stopping at it."""
    _write(tmp_path, "-repo", "aaaaaaaa", [_turn("u1"), _turn("u2")])       # A — the seat
    _write(tmp_path, "-repo", "bbbbbbbb", [_turn("u2"), _turn("u3")])       # B — unseated
    c = _write(tmp_path, "-repo", "cccccccc", [_turn("u3"), _turn("u4")])   # C — unseated
    await _seat(actions, "aaaaaaaa", "agent:aaaaaaaa-iii")

    assert await seat_of_fork(actions.pool, c, root=tmp_path) == "agent:aaaaaaaa-iii"


async def test_a_fork_that_CROSSED_PROJECTS_is_still_found(
    actions: Actions, tmp_path: Path,
) -> None:
    """2 of 57 field pairs cross project dirs (the operator `cd`s into a subrepo and resumes).
    Scoping the search to the session's own dir would have been tidy, cheap, and WRONG TWICE —
    and a lineage engine that silently loses 2 lineages in 57 is not a lineage engine."""
    _write(tmp_path, "-code", "aaaaaaaa", [_turn("u1"), _turn("u2")])
    fork = _write(tmp_path, "-code-subrepo", "bbbbbbbb", [_turn("u2"), _turn("u3")])
    await _seat(actions, "aaaaaaaa", "agent:aaaaaaaa")

    assert await seat_of_fork(actions.pool, fork, root=tmp_path) == "agent:aaaaaaaa"


async def test_a_SPARE_with_no_records_is_not_anybodys_child(
    actions: Actions, tmp_path: Path,
) -> None:
    """`claude bg-spare` / pty-host fires SessionStart with a real id and cwd but never holds a
    conversation (the live one: 1 line, zero turns). It has no first record uuid, so it joins to
    nothing — and must never be handed a living agent's seat by accident."""
    _write(tmp_path, "-repo", "aaaaaaaa", [_turn("u1")])
    spare = _write(tmp_path, "-repo", "eeeeeeee", [{"type": "summary", "sessionId": "eeeeeeee"}])
    assert await resolve_parent(actions.pool, spare, root=tmp_path) is None
    assert await seat_of_fork(actions.pool, spare, root=tmp_path) is None


async def test_the_answer_is_memoized_and_NEVER_RESCANNED(
    actions: Actions, tmp_path: Path,
) -> None:
    """NOT THE CRAWL COMING BACK. The crawl re-read every transcript forever, on a clock. A
    session's ancestry is IMMUTABLE, so it is resolved once, at birth, and cached — including
    the negative answer, which is a real ANSWER ("we looked; nobody's child"), not a gap to be
    re-dug on every mount. That distinction is exactly how a cheap check turns back into a crawl.
    """
    _write(tmp_path, "-repo", "aaaaaaaa", [_turn("u1")])
    fork = _write(tmp_path, "-repo", "bbbbbbbb", [_turn("u1"), _turn("u2")])
    assert await resolve_parent(actions.pool, fork, root=tmp_path) == "aaaaaaaa"
    assert await actions.pool.fetchval(
        "SELECT cursor FROM watermarks WHERE key=$1", fork_key("bbbbbbbb")) == "aaaaaaaa"

    # the parent's transcript is DELETED — a re-scan would now find nothing and lose the lineage
    next(iter((tmp_path / "-repo").glob("aaaaaaaa*.jsonl"))).unlink()
    assert await resolve_parent(actions.pool, fork, root=tmp_path) == "aaaaaaaa", "it re-scanned"

    solo = _write(tmp_path, "-repo", "dddddddd", [_turn("q1")])
    assert await resolve_parent(actions.pool, solo, root=tmp_path) is None
    assert await actions.pool.fetchval(
        "SELECT cursor FROM watermarks WHERE key=$1", fork_key("dddddddd")) == "-", \
        "a negative answer must be CACHED, not re-dug forever"


async def test_an_undetermined_first_uuid_is_never_cached_as_a_negative(
    actions: Actions, tmp_path: Path,
) -> None:
    """60bc15db specimen 4 (decision 01e0c69a): a session whose transcript is not yet
    flushed (first_uuid can't even determine its own join key — plausible at the exact
    moment SessionStart fires) is NOT the same fact as a real search that ran to
    completion and found nobody. Caching the first as if it were the second would freeze
    a transient condition into a permanent "nobody's child" — precisely the twin-seat
    mistake this module exists to prevent. Proven by writing the real content only AFTER
    the first call: the retry must still find the true parent, unlike the sibling test
    above where a genuine negative stays cached even after its evidence is deleted."""
    _write(tmp_path, "-repo", "aaaaaaaa", [_turn("u1"), _turn("u2")])
    fork = _write(tmp_path, "-repo", "eeeeeeee", [])  # not yet flushed at birth

    assert await resolve_parent(actions.pool, fork, root=tmp_path) is None
    assert await actions.pool.fetchval(
        "SELECT cursor FROM watermarks WHERE key=$1", fork_key("eeeeeeee")) is None, \
        "an undetermined result must never be persisted"

    fork.write_text("".join(json.dumps(r) + "\n" for r in [_turn("u1"), _turn("u9")]))
    assert await resolve_parent(actions.pool, fork, root=tmp_path) == "aaaaaaaa", \
        "the retry, now with real content, finds the real parent"


async def test_a_CYCLE_cannot_spin_the_walk(actions: Actions, tmp_path: Path) -> None:
    """Two transcripts that each emitted the other's first uuid (a state that should be
    impossible, which is exactly why it must be survivable). Stand still rather than loop."""
    a = _write(tmp_path, "-repo", "aaaaaaaa", [_turn("u1"), _turn("u2")])
    b = _write(tmp_path, "-repo", "bbbbbbbb", [_turn("u2"), _turn("u1")])
    assert await resolve_parent(actions.pool, a, root=tmp_path) == "bbbbbbbb"
    assert await resolve_parent(actions.pool, b, root=tmp_path) == "aaaaaaaa"
    assert await seat_of_fork(actions.pool, a, root=tmp_path) is None   # no seat, and no hang


def test_find_is_anchored_not_a_substring_match(tmp_path: Path) -> None:
    """Thoth dispatch 6715: the old `glob(f"*/{sid}*.jsonl")` matched `sid` ANYWHERE in the
    filename — a file whose stem merely CONTAINS the target sid, not just one that starts
    with it, would wrongly match. `_find` must return the genuine stem-prefix match and
    never a look-alike."""
    real = _write(tmp_path, "-repo", "aaaaaaaa", [_turn("u1")])
    # a look-alike: "aaaaaaaa" appears as a SUBSTRING, but the stem does not START with it —
    # the old bare-substring glob would have matched this one too (whichever sorts first).
    _write(tmp_path, "-repo", "zzzz-aaaaaaaa", [_turn("u9")])

    assert _find(tmp_path, "aaaaaaaa") == real


def test_find_returns_none_for_an_unknown_sid(tmp_path: Path) -> None:
    _write(tmp_path, "-repo", "aaaaaaaa", [_turn("u1")])
    assert _find(tmp_path, "ffffffff") is None


def test_find_never_resolves_into_the_extractors_own_scratch_tree(tmp_path: Path) -> None:
    """The old glob's own guard, preserved: an ancestor's transcript must never resolve
    into `-osiris-extract`, the instrument's own self-reading scratch tree — even though
    `locate_current_transcript` itself carries no such exclusion."""
    _write(tmp_path, "-repo-osiris-extract", "aaaaaaaa", [_turn("u1")])
    assert _find(tmp_path, "aaaaaaaa") is None
