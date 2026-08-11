"""THE FORK — one mind, a new name, and a transcript that swears it was born this morning.

`claude --fork-session --resume <parent>` is how the harness continues a conversation: it mints
a NEW session id, COPIES the history, and runs the same mind on. New id → SessionStart fires →
the whisper posts → automount seats it. ONE MIND, TWO SEATS.

That is the twin (539ae43b). It is also the SKIPPED GENERATION (Khepri III with no Khepri II —
the fork ate the numeral), the DM that lands in a seat nobody is reading, and the co-agent
cry-wolf that told Anubis XII his tree was contended when the only other "agent" on it was
himself (msg 424 — he was forced to fork himself just to file the report, and narrated the bug
perfectly without knowing that was its name).

AND THE TRANSCRIPT WILL LIE TO YOU IF YOU ASK IT. I checked, on the live pair: the fork rewrites
`sessionId` on ALL 486 of its records to its own. Zero carry the parent's. There is no
`forkedFrom` field, no `parentSessionId`. The parent's id survives only as INCIDENTAL RESIDUE —
inside a terminal hook's notify payload, inside the prose of a compact summary, inside mcpMeta.
Read structurally, a fork SWEARS IT IS NEWBORN. That is why this went unfound through three
successive identity fixes: we kept interrogating a witness that had been coached.

THE JOIN, AND IT IS AN OBSERVATION, NOT AN INFERENCE. A copy rewrites session ids but PRESERVES
RECORD uuids. So:

    A SESSION WHOSE FIRST RECORD UUID BELONGS TO ANOTHER SESSION IS A FORK OF THAT SESSION.

Set membership, on disk, deterministic. It cannot be wrong (uuids do not collide), it costs no
model and no money, and under the charter — a critter may OBSERVE for nothing and may only INFER
on a licence — it sits squarely on the free side of the line.

WHY NOT THE PROCESS TABLE, which declares `--fork-session --resume <path>` in argv and would be
O(1): because it is a witness that DIES. It cannot see the 36 forks already on disk, it cannot
answer for a session whose process exited, and it makes lineage — the thing this whole system is
for — depend on a process still being alive. The record must be readable from the record.

WHY NOT `model IS NULL` (the fix the field report asked for): because absence of an observation
is not evidence of absence. Of the two seats reported as ghosts, one had a RESOLVED model, a LIVE
heartbeat, and NO TRANSCRIPT AT ALL. `model IS NULL` does not mean "not anybody" — it means WE
HAVE NOT LOOKED YET, and this project has now been bitten by that exact shape three times
(456960e5, the quiet agent read as dead; and `last_active IS NULL` read as "it never happened").

RESOLVED ONCE, EVER. The scan is cheap but it is not free, and a session's lineage is immutable:
the answer is memoized in `watermarks` under `fork:<sid>` and never recomputed. This is NOT the
crawl coming back — the crawl re-read every transcript forever on a clock. This reads one new
session's HEAD, once, at its birth.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import asyncpg

_FORK = "fork:"        # watermark: this session's lineage root, resolved once and never again
_NONE = "-"            # memoized "looked, and it is nobody's child" — an answer, not a gap
_CHUNK = 1 << 20       # 1 MiB; the needle is 36 bytes, so a small overlap covers every straddle
_MAX_DEPTH = 16        # forks chain (A→B→C — 15 of 36 in the field). Cycle- and runaway-guarded.


def fork_key(sid: str) -> str:
    return f"{_FORK}{sid}"


def sid_of(path: Path) -> str:
    """The 8-char handle, matching the harness's job-dir scheme (~/.claude/jobs/<sid[:8]>)."""
    return path.stem.split("-")[0]


def first_uuid(path: Path) -> str | None:
    """The transcript's FIRST record uuid — the join key. Reads the HEAD of the file, never
    the whole thing: the answer is in the first few lines or it is nowhere.

    Deliberately NOT a JSON parse of the file. We want one field off the first record that has
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
    """Do these BYTES occur anywhere in the file? A cheap, streaming PRE-FILTER — and nothing
    more. It answers "worth parsing?", never "is this the parent?".

    Chunked with an overlap, because the needle can straddle a chunk boundary; getting that
    wrong would silently MISS a real parent, which mints a twin and looks exactly like success.
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
    """Did this session EMIT a record with that uuid? The structural test, and the only one
    whose answer means anything.

    A BYTE MATCH IS NOT AUTHORSHIP, and the difference is the whole bug this module exists to
    kill. A transcript is full of text that is not its own: tool outputs, pasted files, greps of
    OTHER transcripts. The very session that wrote this module has another session's record uuids
    sitting in its own scrollback, because it went and looked at them. Take a substring hit for a
    parentage claim and you will one day re-seat a mind onto whichever agent happened to `cat` its
    transcript — an inference wearing the authority of a declaration, which is the named disease
    of this codebase and which I committed, here, in the file that cures it.

    So: the pre-filter rejects the ~99% that never mention the uuid at all (streaming bytes, no
    parse), and ONLY a file that mentions it gets read as JSON to ask the real question — is
    there a record whose OWN `uuid` field is this?
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
                if rec.get("uuid") == uid:     # ITS OWN record — authorship, not mention
                    return True
    except OSError:
        return False
    return False


def _candidates(path: Path, root: Path) -> list[Path]:
    """Transcripts that could be this one's parent — NEAREST FIRST.

    Its own project dir leads, because that is where a parent almost always is (55 of 57 pairs
    in the field census). But the fleet is the fallback and it is NOT optional: a fork DOES cross
    project dirs when the operator `cd`s into a subrepo (2 of 57 — phanspeed forked from a
    session rooted in `code/`). Scoping to the same dir would have been tidy, cheap, and wrong
    twice, and a lineage engine that silently loses 2 lineages in 57 is not one.
    """
    here, seen = path.parent, {path.resolve()}
    out: list[Path] = []
    for scope in (here.glob("*.jsonl"), root.expanduser().glob("*/*.jsonl")):
        for p in sorted(scope):
            rp = p.resolve()
            if rp in seen or p.parent.name.endswith("-osiris-extract"):
                continue  # the adversary's own scratch sessions: an instrument reading itself
            seen.add(rp)
            out.append(p)
    return out


def _parent(path: Path, root: Path) -> tuple[Path | None, bool]:
    """The session this one was forked from, or None if it is nobody's child — paired with
    whether a real search actually ran (`determined`). `first_uuid` returning nothing at
    session birth (the transcript may not be fully flushed yet) is NOT the same fact as
    searching every candidate and finding no match (60bc15db specimen 4, decision
    01e0c69a): the first case never even attempted the search, so it must never be cached
    as a genuine negative by the caller. `determined` is only False for the never-searched
    case — every other exit (a match, or an exhausted search) is a real answer."""
    u = first_uuid(path)
    if not u:
        return None, False
    for cand in _candidates(path, root):
        if _emitted(cand, u):
            return cand, True
    return None, True


def _find(root: Path, sid: str) -> Path | None:
    """The transcript for a session id, anywhere in the fleet."""
    for p in root.expanduser().glob(f"*/{sid}*.jsonl"):
        if not p.parent.name.endswith("-osiris-extract"):
            return p
    return None


async def resolve_parent(
    pool: asyncpg.Pool, path: Path, *, root: Path, refresh: bool = False,
) -> str | None:
    """The sid this session was forked FROM — ONE hop. None if it is nobody's child.

    Memoized in `watermarks` under `fork:<sid>`. A session's ancestry is IMMUTABLE, so a
    DETERMINED scan runs once for a given session and never again; `_NONE` is a real
    cached ANSWER ("we looked, and it is nobody's child"), not a hole to be re-dug on
    every mount — which is precisely how a cheap check turns back into a crawl. But an
    UNDETERMINED scan (this session's own first_uuid could not be read yet — plausible at
    birth, before the transcript is fully flushed) is never cached at all: caching "I
    don't know" as if it were "no" would freeze a transient condition into a permanent
    wrong answer, in the one file whose job is preventing exactly that kind of twin-seat
    mistake (60bc15db specimen 4, decision 01e0c69a). An undetermined call returns None
    for THIS call only — the next mount gets a fresh, real attempt.
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
    """The agent_id this session should mount as: ITS NEAREST ANCESTOR THAT ALREADY HAS A SEAT.

    NOT the lineage ROOT, and the difference is the whole safety of this thing. The root is a
    fact about a TRANSCRIPT; a seat is a fact about the GRAPH, and the two are not the same
    object. Thoth's chain roots at 556403ee, but the fleet has known that mind as
    `agent:ad1a1cb0` for nine generations — minting `agent:556403ee` off the transcript would
    invent a THIRD identity while trying to cure a second one.

    So we ask reality instead of deriving: climb the fork chain and return the first ancestor the
    registry ALREADY has a seat for. That reuses the whole tested succession path (the caller's
    `lineage_head` then walks it forward to the current generation), and an ancestor nobody ever
    seated simply contributes nothing. If no ancestor has a seat, this session is genuinely new:
    return None and let it be born.
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
