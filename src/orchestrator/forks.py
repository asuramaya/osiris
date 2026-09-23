"""Fork lineage resolution.

`claude --fork-session --resume <parent>` is how the harness continues a conversation: it
mints a new session id, copies the history, and runs the same agent forward. The new id
triggers SessionStart, which triggers automount, which seats the new session. One agent now
has two seats.

That produces a duplicate identity: what looks like a new generation with a missing
predecessor in the numbering, a message that lands in a seat nobody is reading, and a
co-agent report of contention on a shared tree when the only other "agent" present was
itself (a session had to fork itself just to file that report, and described the bug
precisely without realizing that was its own cause).

And the transcript will not answer honestly if you ask it structurally. Checked on a live
pair: the fork rewrites `sessionId` on all of its records to its own. None carry the
parent's. There is no `forkedFrom` field, no `parentSessionId`. The parent's id survives only
as incidental residue: inside a terminal hook's notify payload, inside the prose of a compact
summary, inside mcpMeta. Read structurally, a fork looks newly created. That is why this went
undetected through several successive identity fixes: the check kept interrogating a source
that had effectively been coached to say "new."

The join used here is an observation, not an inference. A copy rewrites session ids but
preserves record uuids. So:

    A session whose first record uuid belongs to another session is a fork of that session.

Set membership, on disk, deterministic. It cannot be wrong (uuids do not collide), it costs
no model call and no money, and under this project's distinction between observation and
inference, an observation is free while an inference requires stronger justification, so this
sits squarely on the free side of that line.

Why not the process table, which declares `--fork-session --resume <path>` in argv and would
be O(1): because it is a witness that disappears. It cannot see forks already on disk, it
cannot answer for a session whose process has exited, and it would make lineage tracking,
the whole purpose of this system, depend on a process still being alive. The record must be
readable from the record.

Why not `model IS NULL` (an earlier proposed fix): because absence of an observation is not
evidence of absence. Of two seats once reported as inactive, one had a resolved model, a live
heartbeat, and no transcript at all. `model IS NULL` does not mean "nobody's session"; it
means "not yet checked," and this project has been bitten by that exact shape more than once:
a quiet agent misread as dead, and `last_active IS NULL` misread as "never happened."

Resolved once, ever. The scan is cheap but not free, and a session's lineage is immutable: the
answer is memoized in `watermarks` under `fork:<sid>` and never recomputed. This is not a
recurring background scan coming back; an earlier version of this logic re-read every
transcript repeatedly on a schedule. This reads one new session's head, once, at its birth.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import asyncpg

from src.ingest.sessions import locate_current_transcript

# watermark key prefix: this session's lineage root, resolved once and never again
_FORK = "fork:"
# memoized "looked, and it is nobody's child": a real answer, not a gap
_NONE = "-"
# 1 MiB chunks; the needle is 36 bytes, so a small overlap covers every straddle
_CHUNK = 1 << 20
# forks can chain (A to B to C: 15 of 36 in the field). Cycle- and runaway-guarded.
_MAX_DEPTH = 16


def fork_key(sid: str) -> str:
    return f"{_FORK}{sid}"


def sid_of(path: Path) -> str:
    """The 8-char handle, matching the harness's job-dir scheme (~/.claude/jobs/<sid[:8]>)."""
    return path.stem.split("-")[0]


def first_uuid(path: Path) -> str | None:
    """The transcript's first record uuid, used as the join key. Reads only the head of the
    file, never the whole thing: the answer is in the first few lines or it is nowhere.

    Deliberately not a JSON parse of the file. We want one field off the first record that has
    one, and a transcript's head is small.
    """
    import json
    try:
        with path.open(errors="replace") as fh:
            for _ in range(64):          # a bounded peek; a record with a uuid comes early
                line = fh.readline()
                if not line:
                    return None
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue
                u = rec.get("uuid")
                if isinstance(u, str) and u:
                    return u
    except OSError:
        return None
    return None


def _mentions(path: Path, needle: bytes) -> bool:
    """Do these bytes occur anywhere in the file? A cheap, streaming pre-filter, and nothing
    more. It answers "worth parsing?", never "is this the parent?".

    Chunked with an overlap, because the needle can straddle a chunk boundary; getting that
    wrong would silently miss a real parent, which mints a duplicate seat and looks exactly
    like success.
    """
    tail = b""
    try:
        with path.open("rb") as fh:
            while chunk := fh.read(_CHUNK):
                if needle in tail + chunk:
                    return True
                tail = chunk[-(len(needle) - 1):]
    except OSError:
        return False
    return False


def _emitted(path: Path, uid: str) -> bool:
    """Did this session emit a record with that uuid? The structural test, and the only one
    whose answer means anything.

    A byte match is not authorship, and the difference is the whole bug this module exists to
    prevent. A transcript is full of text that is not its own: tool outputs, pasted files,
    greps of other transcripts. Even the session that wrote this module has other sessions'
    record uuids sitting in its own scrollback, because it looked at them. Treat a substring
    hit as a parentage claim, and eventually an agent identity gets reseated onto whichever
    session happened to read its transcript: an inference wearing the authority of a
    declaration, a mistake this module exists specifically to prevent.

    So: the pre-filter rejects the files that never mention the uuid at all (streaming bytes,
    no parse), and only a file that mentions it gets read as JSON to ask the real question: is
    there a record whose own `uuid` field is this?
    """
    import json
    if not _mentions(path, uid.encode()):
        return False
    try:
        with path.open(errors="replace") as fh:
            for line in fh:
                if uid not in line:            # cheap reject before the parse
                    continue
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue
                if rec.get("uuid") == uid:     # its own record: authorship, not mention
                    return True
    except OSError:
        return False
    return False


def _candidates(path: Path, root: Path) -> list[Path]:
    """Transcripts that could be this one's parent, nearest first.

    Its own project directory leads, because that is where a parent almost always is (55 of
    57 pairs in a field census). But the broader root directory is the fallback and it is not
    optional: a fork does cross project directories when the working directory changes into a
    subrepo (2 of 57 in that same census, one session forked from a session rooted in a parent
    directory). Scoping to the same directory alone would have been tidy, cheap, and wrong
    twice, and a lineage engine that silently loses 2 lineages out of 57 is not one.
    """
    here, seen = path.parent, {path.resolve()}
    out: list[Path] = []
    for scope in (here.glob("*.jsonl"), root.expanduser().glob("*/*.jsonl")):
        for p in sorted(scope):
            rp = p.resolve()
            if rp in seen or p.parent.name.endswith("-osiris-extract"):
                # exclude the extractor's own scratch sessions: an instrument reading itself
                continue
            seen.add(rp)
            out.append(p)
    return out


def _parent(path: Path, root: Path) -> tuple[Path | None, bool]:
    """The session this one was forked from, or None if it is nobody's child, paired with
    whether a real search actually ran (`determined`). `first_uuid` returning nothing at
    session birth (the transcript may not be fully flushed yet) is not the same fact as
    searching every candidate and finding no match: the first case never even attempted the
    search, so it must never be cached as a genuine negative by the caller. `determined` is
    only False for the never-searched case; every other exit (a match, or an exhausted
    search) is a real answer."""
    u = first_uuid(path)
    if not u:
        return None, False
    for cand in _candidates(path, root):
        if _emitted(cand, u):
            return cand, True
    return None, True


def _find(root: Path, sid: str) -> Path | None:
    """The transcript for a session id, anywhere in the fleet.

    Anchored: `glob(f"*/{sid}*.jsonl")` is a prefix match on the whole filename, not an
    exact stem match, looser than the other call sites that already anchor this same lookup
    in trigger.py, and reachable by the materializer's own duplicate copies (a second file
    whose stem starts with this `sid` but isn't it). `locate_current_transcript` is the same
    anchor those other sites use, given a synthesized `jobs/<sid>` job_dir (this function
    only ever has a bare sid, never a caller-supplied hint; a hint-first approach broke 5
    tests elsewhere because a hint can be a bare id with no "jobs/" component).
    `anchored_only=True`: a sid that matches nothing must return None, never a co-tenant's
    newest transcript.

    `locate_current_transcript` itself carries no `-osiris-extract` exclusion (its own
    callers each apply that separately), kept here unchanged from the previous glob's own
    guard, since an ancestor's transcript must never resolve into the extractor's own
    self-reading scratch tree."""
    found = locate_current_transcript(root, f"jobs/{sid}", anchored_only=True)
    if found is not None and found.parent.name.endswith("-osiris-extract"):
        return None
    return found


async def resolve_parent(
    pool: asyncpg.Pool, path: Path, *, root: Path, refresh: bool = False,
) -> str | None:
    """The sid this session was forked from, one hop. None if it is nobody's child.

    Memoized in `watermarks` under `fork:<sid>`. A session's ancestry is immutable, so a
    determined scan runs once for a given session and never again; `_NONE` is a real
    cached answer ("we looked, and it is nobody's child"), not a hole to be re-dug on
    every mount, which is precisely how a cheap check turns back into a recurring scan. But
    an undetermined scan (this session's own first_uuid could not be read yet, plausible at
    birth, before the transcript is fully flushed) is never cached at all: caching "I
    don't know" as if it were "no" would freeze a transient condition into a permanent
    wrong answer, in the one module whose job is preventing exactly that kind of
    duplicate-seat mistake. An undetermined call returns None for this call only; the next
    mount gets a fresh, real attempt.
    """
    sid = sid_of(path)
    key = fork_key(sid)
    if not refresh:
        got = await pool.fetchval("SELECT cursor FROM watermarks WHERE key=$1", key)
        if got is not None:
            return None if got == _NONE else str(got)
    found, determined = await asyncio.to_thread(_parent, path, root)
    parent = sid_of(found) if found is not None else None
    if not determined:
        return None
    await pool.execute(
        "INSERT INTO watermarks (key, cursor, updated_at) VALUES ($1,$2,now()) "
        "ON CONFLICT (key) DO UPDATE SET cursor=EXCLUDED.cursor, updated_at=now()",
        key, parent or _NONE)
    return parent


async def seat_of_fork(pool: asyncpg.Pool, path: Path, *, root: Path) -> str | None:
    """The agent_id this session should mount as: its nearest ancestor that already has a seat.

    Not the lineage root, and the difference is the whole safety of this function. The root
    is a fact about a transcript; a seat is a fact about the graph, and the two are not the
    same object. One agent's fork chain can root at a transcript id the fleet has never used
    as an identity, because that agent has instead been known under a different agent id for
    many generations; minting a new identity off the transcript root would invent a third
    identity while trying to fix a second one.

    So this asks reality instead of deriving it: climb the fork chain and return the first
    ancestor the registry already has a seat for. That reuses the whole tested succession
    path (the caller's `lineage_head` then walks it forward to the current generation), and
    an ancestor nobody ever seated simply contributes nothing. If no ancestor has a seat,
    this session is genuinely new: return None and let it be born.
    """
    seen = {sid_of(path)}
    cur = path
    for _ in range(_MAX_DEPTH):
        parent = await resolve_parent(pool, cur, root=root)
        if not parent or parent in seen:
            return None          # nobody's child, or a cycle: stand still rather than lie
        seen.add(parent)
        agent = await pool.fetchval(
            "SELECT agent_id FROM agent_mounts WHERE job_dir LIKE $1 "
            "ORDER BY last_seen DESC NULLS LAST, mounted_at DESC LIMIT 1", f"%/{parent}")
        if agent:
            return str(agent)
        nxt = await asyncio.to_thread(_find, root, parent)
        if nxt is None:
            return None          # the ancestor's transcript is gone; the chain ends here
        cur = nxt
    return None
