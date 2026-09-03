"""The soul store — verbatim ingest + hash chain (task #51 piece 1, ruling 62dc6397).

INFINITE RETENTION, deliberately separate from transcript_store.py (see 0050's own
docstring for the full reasoning): that store is a derived, disposable index of per-turn
facts; this one is the durable, byte-exact copy of the transcript itself, so Osiris
remains the record once the source file is gone. Reuses the harness adapters' own
SessionLocator discovery (the ONE parser that already knows where a session lives) but
never their read_turns — that reparses into TurnRow and throws the raw bytes away. Piece
1 covers the claude-code harness only (line-oriented JSONL); Crush is SQLite-backed and
needs its own verbatim strategy, out of scope here on purpose.

No semantic filtering: every line is stored exactly as it appears, including
compaction-summary and meta lines. re_materialize() is the acceptance test this piece
stands or falls on — a store that cannot hand a transcript back byte-for-byte is not a
soul store, just another index.

RAW BYTES, NOT TEXT (0052, thread 173cbf11, Thoth DM 5350): a real transcript can carry a
literal NUL byte (0x00) — measured live, thousands of them across two ordinary sessions —
which Postgres `text`/`varchar` columns cannot hold AT ALL, a hard server limitation, not
a query bug. `soul_lines.raw_line` is `bytea`; every line is read, hashed, stored, and
rematerialized as raw bytes end to end, so nothing here can silently mangle a byte the
source file actually carried. `raw_lines()`/`re_materialize()`/`mining_view()` still hand
callers `str` (decoded utf-8, errors='replace' as a last resort) — the text-processing
consumers (session mining, JSON parsing) never need to know the storage layer changed.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg

from src.ingest.harness import HarnessAdapter
from src.ingest.sessions import _COMPACT_BOUNDARY_MARKERS

_HARNESS = "claude-code"

# Default adapters for locating session files — tried in order
_DEFAULT_ADAPTERS: list[HarnessAdapter] | None = None


def _default_adapters() -> list[HarnessAdapter]:
    global _DEFAULT_ADAPTERS
    if _DEFAULT_ADAPTERS is None:
        from src.ingest.harness.claude_jsonl import ClaudeJsonlAdapter
        from src.ingest.harness.crush_sqlite import CrushSqliteAdapter
        from src.ingest.harness.dsh import DshSessionAdapter
        _DEFAULT_ADAPTERS = [
            ClaudeJsonlAdapter(), DshSessionAdapter(), CrushSqliteAdapter(),
        ]
    return _DEFAULT_ADAPTERS


def _read_source(source_path: str) -> bytes:
    """Kept in a sync helper for the blocking-call lint, same as transcript_store.py's
    own `_stat` — the read is still blocking either way, this only satisfies ASYNC240's
    syntactic check on the call site. Raw bytes, not text (0052) — the ONLY way to carry
    a source byte (a literal NUL included) through untouched."""
    return Path(source_path).read_bytes()


def _split_lines(content: bytes) -> list[bytes]:
    """Every COMPLETE line, `\\n`-terminated — NEVER a trailing partial fragment (msg
    6583, the live-write safety gap: Jesus was resumed and being actively appended to
    while this lane was dispatched). A session writes one JSON object per line, each
    ending in `\\n`; if `ingest_path` reads mid-write of the LAST line on disk, the
    bytes on disk end WITHOUT that terminating `\\n`. Returning that fragment as a
    "line" would bake a half-written JSON object permanently into the hash chain at its
    own line_idx — `ingest_path` never revisits an index once ingested, so a
    subsequent re-read (once the write actually finishes) could never correct it.

    THE FIX IS STRUCTURAL, NOT AN OCCUPANCY CHECK: rather than asking "is a session
    live right now" (racy by construction — every occupancy read in this house tonight
    has gone stale between the check and the write it gated), only ever trust content
    up to and including the LAST complete `\\n`. Whatever trails it — complete or
    mid-write, this function cannot tell and does not need to — waits for the next
    sweep, when the same bytes (now `\\n`-terminated, if the write finished) are read
    fresh and become the same line_idx's real content. A quiet file loses nothing (its
    last real line already ends in `\\n`); a file being appended to right now loses
    only the not-yet-complete tail, forever safe to re-read later.

    Decodes nothing (0052: no `str.encode` round-trip to silently move an embedded NUL
    byte off its exact position)."""
    if not content:
        return []
    end = content.rfind(b"\n")
    if end == -1:
        return []  # not even one complete line on disk yet
    return content[:end].split(b"\n")


_STREAM_CHUNK_BYTES = 4 * 1024 * 1024  # read granularity, not a line boundary
_INGEST_BATCH_LINES = 2000  # rows per INSERT + progress checkpoint


def _iter_complete_line_batches(
    source_path: str, batch_lines: int = _INGEST_BATCH_LINES,
) -> Iterator[list[bytes]]:
    """Stream `source_path` in bounded chunks, yielding lists of up to `batch_lines`
    COMPLETE lines each — never holding the whole file, nor the whole split line list,
    in memory at once (msg 6583: measured ~881MB peak RSS ingesting a 307MB session the
    old whole-file way; a long-lived shared process serving on-demand resume ingests
    cannot carry that). Same completeness guarantee as `_split_lines` (never yields a
    trailing, non-`\\n`-terminated fragment — the live-write safety fix applies exactly
    the same way here, just applied across a chunk boundary instead of within one
    `read_bytes()` call), by carrying any partial tail across chunk reads until it either
    completes or the file ends, in which case it's silently dropped rather than yielded.

    Pure generator, no I/O interleaved with a caller's own async work inside the loop
    body — the file handle stays open only for the duration of iteration, a sync
    generator is fine here for the same blocking-call-lint reasoning `_read_source`
    already documents (the read is blocking either way)."""
    batch: list[bytes] = []
    carry = b""
    with open(source_path, "rb") as f:
        while True:
            chunk = f.read(_STREAM_CHUNK_BYTES)
            if not chunk:
                break
            data = carry + chunk
            end = data.rfind(b"\n")
            if end == -1:
                carry = data  # no complete line in this window yet — keep accumulating
                continue
            complete, carry = data[:end], data[end + 1:]
            batch.extend(complete.split(b"\n"))
            while len(batch) >= batch_lines:
                yield batch[:batch_lines]
                batch = batch[batch_lines:]
    if batch:
        yield batch
    # `carry`, if non-empty here, is a trailing unterminated fragment — dropped, exactly
    # as `_split_lines` drops it; the next sweep re-reads it once it's `\n`-complete.


def _path_mtime(path: Path) -> datetime | None:
    """Sync helper, same blocking-call-lint reasoning as `_read_source` — None when the
    path doesn't exist (nothing to compare against, never an error)."""
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    except OSError:
        return None


def _write_dest(path: Path, content: bytes) -> None:
    """Sync helper, same blocking-call-lint reasoning as `_read_source`. Raw bytes
    (0052) — the write side of the same byte-exact promise `_read_source` makes. An
    existing file at `path` is parked, never overwritten (`_park_superseded`)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    _park_superseded(path)
    path.write_bytes(content)


_REMATERIALIZE_PAGE_LINES = 2000  # rows fetched per soul_lines page during a streamed write


def _open_tmp_writer(target: Path) -> tuple[Any, Path]:
    """A sibling temp file, same directory as `target` (so the final rename is same-
    filesystem and atomic) — the streaming half of `rematerialize_to_disk`'s own
    promise that a broken chain writes NOTHING at `target`: every line lands in the temp
    file first, verified as it's written, and only a fully clean pass ever gets renamed
    onto the real destination. Sync helper, same blocking-call-lint reasoning as
    `_read_source`/`_write_dest`."""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f"{target.name}.osiris-tmp-{uuid.uuid4().hex}")
    return open(tmp, "wb"), tmp  # noqa: SIM115 — lifetime spans a whole async streaming loop


def _discard_tmp(f: Any, tmp: Path) -> None:
    """Close and delete the temp writer — a broken chain or a raised exception mid-
    stream must leave no half-written file anywhere a caller could mistake for the real
    thing, temp name included."""
    f.close()
    tmp.unlink(missing_ok=True)


def _park_superseded(target: Path) -> Path | None:
    """Move whatever already sits at `target` into a sibling `.superseded-stubs/` directory
    (a timestamped, id-preserving name) instead of overwriting it — Constitution 3 applied
    to disk: the store is canon and a slug's file is a CACHE of it, but a cache the store
    never ingested (a divergent fork, a truncated stub, a copy from a cwd the seat has since
    left) is still evidence, never deleted. Two levels below `~/.claude/projects/<slug>/`,
    so neither the harness's own session listing nor `locate_current_transcript`'s
    one-level `*/*.jsonl` glob can ever pick the parked copy back up as the freshest —
    Sekhmet's own precedent for Marquee's shadowing stub (decision b348e902), made the
    materializer's standing rule rather than a one-off hand move. Returns where it went,
    None when nothing was there. Sync helper, same blocking-call-lint reasoning as
    `_write_dest`."""
    if not target.exists():
        return None
    park_dir = target.parent / ".superseded-stubs"
    park_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    parked = park_dir / f"{target.stem}-superseded-{stamp}{target.suffix}"
    target.replace(parked)
    return parked


def _finalize_tmp(f: Any, tmp: Path, target: Path) -> None:
    """Close the temp writer and atomically rename it onto `target` — the moment a
    streamed rematerialize actually becomes visible at its destination, all at once,
    never as a partially-written file a concurrent reader could observe mid-stream.
    An existing file at `target` is PARKED first (`_park_superseded`), never overwritten."""
    f.close()
    _park_superseded(target)
    tmp.replace(target)


def _chain_hash(prev_hash: str | None, raw_line: bytes) -> str:
    """sha256(prev_hash_bytes + raw_line_bytes) — a gap or a tampered line breaks the
    chain from that point on, detectable by re-walking and recomputing, never by trusting
    a stored hash in isolation. Hashes the RAW BYTES directly (0052) — no
    `str.encode("utf-8")` re-derivation step to silently diverge from what was actually
    stored."""
    h = hashlib.sha256()
    if prev_hash is not None:
        h.update(prev_hash.encode("utf-8"))
    h.update(raw_line)
    return h.hexdigest()


def _hash_rows(
    harness: str, anchor_sid: str, lines: list[bytes], start_idx: int,
    prev_hash: str | None,
) -> tuple[list[tuple[str, str, int, bytes, str, str | None]], int, str | None]:
    """Build `soul_lines` rows for `lines`, continuing the hash chain from `(start_idx,
    prev_hash)` — the one row-construction shape `ingest_path` and `splice_sources` both
    need, extracted so a caller ingesting several sources in sequence for one anchor_sid
    (splice_sources) chains them exactly the way a single growing file chains its own
    lines, rather than a second, drifting copy of this loop."""
    rows: list[tuple[str, str, int, bytes, str, str | None]] = []
    idx = start_idx
    for raw_line in lines:
        line_hash = _chain_hash(prev_hash, raw_line)
        rows.append((harness, anchor_sid, idx, raw_line, line_hash, prev_hash))
        prev_hash = line_hash
        idx += 1
    return rows, idx, prev_hash


def _addressable_entries(lines: list[bytes]) -> list[tuple[int, str, str | None]]:
    """`(line_idx, uuid, parentUuid)` for every `user`/`assistant` line — the entry types
    a human or an agent can meaningfully ask to SEEK to (`rematerialize_to_disk(upto=...)`).
    `queue-operation`/`mode`/`last-prompt`/`custom-title`/`agent-name` lines carry no
    `uuid` at all and are naturally excluded; `attachment` lines DO carry a real `uuid`/
    `parentUuid` and a genuine link in the harness's own chain (found live, thread
    6483/6559/6565 — the jesus/chad splice's own false-positive orphan, traced to this
    exact gap) but are never a valid seek target — a caller must never land a resume on a
    hook-injected attachment. Chain-CONTINUITY verification must NOT use this function;
    see `_chain_linked_entries` below, the ONLY correct input to that check. Two named
    functions, not one with a flag (Thoth's own instruction, thread 6567) — a flag
    invites exactly the conflation that produced the false positive."""
    out: list[tuple[int, str, str | None]] = []
    for idx, raw in enumerate(lines):
        try:
            d = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if not isinstance(d, dict) or d.get("type") not in ("user", "assistant"):
            continue
        u = d.get("uuid")
        if isinstance(u, str) and u:
            out.append((idx, u, d.get("parentUuid")))
    return out


def _chain_linked_entries(lines: list[bytes]) -> list[tuple[int, str, str | None]]:
    """`(line_idx, uuid, parentUuid)` for EVERY line that carries a `uuid`, regardless of
    `type` — the correct, and ONLY correct, input to a chain-continuity walk. Found live
    (thread 6483/6559/6565): `verify_jsonl_chain_boundary`'s first draft used
    `_addressable_entries` (user/assistant only) and reported a false "orphan" on BOTH
    jesus and chad, at the identical relative position (the second real entry after each
    seat's own cwd-move cut) — traced to `type:"attachment"` lines, which genuinely
    chain (`uuid`/`parentUuid` both real, verified by direct inspection of the raw file)
    but were invisible to that walk. 2e05d662's own by-hand proof got the right answer by
    luck: it checked only the JOIN (B's first entry's parentUuid against A's last), never
    B's own full internal continuity — this function is what that protocol should have
    used throughout. Never use this for a SEEK target (`rematerialize_to_disk`'s own
    `upto`) — an attachment is a real chain link but never a place a caller should land a
    resume; `_addressable_entries` stays the seek-only door, unchanged."""
    out: list[tuple[int, str, str | None]] = []
    for idx, raw in enumerate(lines):
        try:
            d = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if not isinstance(d, dict):
            continue
        u = d.get("uuid")
        if isinstance(u, str) and u:
            out.append((idx, u, d.get("parentUuid")))
    return out


def _confession_line(seek_entry: dict[str, Any], withheld: int) -> bytes:
    """The withheld-entries confession (thread 6483/6534/6540, ratified as a standing rule
    — an unexercised guard that states its own condition is worth having even the day it
    doesn't fire): a synthetic `user`-typed entry, `isMeta: true` matching the marker real
    hook-injected content already carries (so osiris's own mining/rendering skips it the
    same way it already skips other isMeta lines), chained onto the seek point as its
    `parentUuid` so it reads as the NEXT thing that happened, not a foreign insert. Never
    a fabricated compaction — this states a fact about what was withheld, never invents
    what the model itself would have summarized.

    UNVERIFIED against a live Claude Code resume as of this build — see
    `rematerialize_to_disk`'s own docstring."""
    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    text = (f"[osiris] this session was resumed as of an earlier point in its own "
            f"history — {withheld} later entr{'y' if withheld == 1 else 'ies'} exist in "
            f"the stored record but were withheld from this transcript. Ask osiris to "
            f"rematerialize the full session if you need them.")
    entry = {
        "parentUuid": seek_entry.get("uuid"),
        "isSidechain": False,
        "isMeta": True,
        "type": "user",
        "message": {"role": "user", "content": text},
        "uuid": str(uuid.uuid4()),
        "timestamp": now,
        "sessionId": seek_entry.get("sessionId"),
        "cwd": seek_entry.get("cwd"),
    }
    return json.dumps(entry).encode("utf-8")


def verify_jsonl_chain_boundary(file_a: str, file_b: str) -> str | None:
    """Is `file_b` genuinely the CONTINUATION of `file_a` — one gapless conversation cut
    by a mid-session cwd move (thread 6483/6534, decision 2e05d662's own by-hand proof on
    jesus/chad, generalized here so a future pair is never spliced on the strength of a
    same-session-id coincidence alone) — or two things that merely share a session id?
    None means clean; otherwise the reason, matching `resume_verdict`'s own shape.

    THREE CHECKS, the same ones 2e05d662 ran by hand, and no more: (1) `file_b`'s FIRST
    chain-linked entry's `parentUuid` must equal `file_a`'s LAST chain-linked entry's
    `uuid` — the literal join point; (2) zero `uuid` overlap between the two files — two
    genuinely different halves, never the same content ingested twice under different
    names; (3) every entry in `file_b` (after its first, already checked by (1)) must
    have a `parentUuid` resolving to a `uuid` seen earlier in `file_a` or `file_b` — "B
    entries whose parent is in neither file: ZERO", 2e05d662's own phrasing. `file_a`'s
    OWN internal structure is trusted, not re-audited — it is a real, harness-written
    prefix already ingested as itself; this function's job is the JOIN, not a second
    from-scratch chain audit of content nobody is disputing.

    WALKS `_chain_linked_entries` (every uuid-bearing line), NEVER `_addressable_entries`
    (user/assistant only, the seek-only door) — found live (thread 6483/6559/6565): the
    first draft used the addressable-only walk and reported a false orphan on BOTH jesus
    and chad at the identical relative position (the second real entry after each seat's
    own cwd-move cut), because `type:"attachment"` lines carry real chain links this walk
    must see. 2e05d662's own by-hand protocol got the right answer by luck — it checked
    only the join, never file_b's full internal continuity, so it never hit this gap.

    A file with NO chain-linked entries at all can never be verified as either half of a
    chain — refused, not silently treated as trivially clean."""
    a = _chain_linked_entries(_split_lines(_read_source(file_a)))
    b = _chain_linked_entries(_split_lines(_read_source(file_b)))
    if not a:
        return f"{file_a} has no uuid-bearing entries — nothing to verify"
    if not b:
        return f"{file_b} has no uuid-bearing entries — nothing to verify"
    uuids_a = {u for _, u, _ in a}
    uuids_b = {u for _, u, _ in b}
    overlap = uuids_a & uuids_b
    if overlap:
        return (f"uuid overlap between {file_a} and {file_b}: {sorted(overlap)[:5]!r} — "
                f"not two distinct halves of one conversation")
    last_a_uuid = a[-1][1]
    first_b_uuid, first_b_parent = b[0][1], b[0][2]
    if first_b_parent != last_a_uuid:
        return (f"chain broken at the join — {file_b}'s first entry ({first_b_uuid}) has "
                f"parentUuid={first_b_parent!r}, expected {file_a}'s last entry "
                f"({last_a_uuid!r})")
    seen: set[str] = set(uuids_a)
    for i, (_, u, parent) in enumerate(b):
        if i > 0 and parent is not None and parent not in seen:
            return (f"orphan entry {u} in {file_b} — its parentUuid {parent!r} matches "
                    f"nothing in {file_a} or earlier in {file_b}")
        seen.add(u)
    return None


class SoulStore:
    """The append-only, content-bearing transcript store.

    Harness-agnostic: accepts a list of HarnessAdapters to discover session files.
    Defaults to Claude JSONL, DSH zstd, and Crush SQLite in order.
    """

    def __init__(
        self, pool: asyncpg.Pool, adapters: list[HarnessAdapter] | None = None,
    ) -> None:
        self.pool = pool
        self._adapters = adapters or _default_adapters()

    def _detect_harness(self, source_path: str) -> str:
        """Auto-detect harness from a source path by trying each adapter's discover_at.
        Defaults to 'claude-code' for backward compatibility."""
        from pathlib import Path
        p = Path(source_path)
        for adapter in self._adapters:
            discover_at = getattr(adapter, "discover_at", None)
            if discover_at is not None:
                try:
                    locator = discover_at(p)
                    if locator is not None:
                        return str(locator.harness)
                except Exception:  # noqa: BLE001
                    continue
        return "claude-code"

    def _claude_code_adapters(self) -> list[HarnessAdapter]:
        """`backfill()`'s own default — piece 1's claude-code-only scope, filtered out of
        `self._adapters`' full set by class name rather than a second adapter list, so a
        future adapter this instance was constructed with (a custom claude-code variant,
        say) still participates without a maintained duplicate registry."""
        return [a for a in self._adapters if type(a).__name__ == "ClaudeJsonlAdapter"]

    async def _progress(
        self, anchor_sid: str, harness: str = "claude-code",
    ) -> tuple[int, str | None]:
        """(last_line_idx, last_hash) already ingested for this session, or (0, None).
        `harness` defaults to 'claude-code' for backward compatibility with existing rows."""
        row = await self.pool.fetchrow(
            "SELECT last_line_idx, last_hash FROM soul_sessions "
            "WHERE harness=$1 AND anchor_sid=$2", harness, anchor_sid)
        if row is None:
            return 0, None
        return int(row["last_line_idx"]), row["last_hash"]

    async def _checkpoint(
        self, harness: str, anchor_sid: str, source_path: str, idx: int,
        prev_hash: str | None,
    ) -> None:
        """One batch's worth of progress, committed — the same upsert `ingest_path`
        always ran once at the end, now run once per batch so an interrupted 300MB+
        ingest (msg 6583/ba329ccb: "the first run backfills 2,070 files and will be
        interrupted") resumes from its last committed BATCH, not from scratch."""
        await self.pool.execute(
            "INSERT INTO soul_sessions "
            "   (harness, anchor_sid, source_path, last_line_idx, last_hash) "
            "VALUES ($1, $2, $3, $4, $5) "
            "ON CONFLICT (harness, anchor_sid) DO UPDATE "
            "   SET last_ingested_at = now(), "
            "       last_line_idx = EXCLUDED.last_line_idx, "
            "       last_hash = EXCLUDED.last_hash, "
            "       source_path = EXCLUDED.source_path",
            harness, anchor_sid, source_path, idx, prev_hash,
        )

    async def ingest_path(
        self, source_path: str, anchor_sid: str, harness: str | None = None,
    ) -> int:
        """Eat every line of `source_path` not yet ingested for `anchor_sid`. Idempotent —
        re-running over an unchanged file appends nothing (progress already covers every
        line). Returns the count of NEW lines ingested. Never re-reads or re-hashes lines
        already stored: the chain resumes from last_hash, exactly where it left off.

        STREAMS THE SOURCE IN BOUNDED BATCHES (msg 6583, the 307MB question): measured
        ~881MB peak RSS ingesting a real 307MB session the old whole-file way (read the
        entire file, split every line, build one giant rows list, one executemany) — not
        viable in a long-lived shared process (osiris-worker, or an on-demand ingest
        called from inside a resume request) serving other work at the same time. This
        version reads via `_iter_complete_line_batches` (bounded chunks, never the whole
        file in memory) and INSERTs + checkpoints progress once per batch of
        `_INGEST_BATCH_LINES` lines, so peak memory is O(batch size) regardless of file
        size, and an interruption mid-file resumes from the last committed batch rather
        than redoing the whole ingest. SAME CONTRACT as before: same return value (count
        of new lines), same idempotence, same signature — a caller (Khnum's resume wire
        included) needs no changes.

        `harness` names the harness that owns this transcript (e.g. 'claude-code', 'dsh',
        'crush'). When None, it's auto-detected from `self._adapters` by matching
        `source_path` to an adapter's discovery."""
        harness = harness or self._detect_harness(source_path)
        since, prev_hash = await self._progress(anchor_sid, harness=harness)
        seen = 0
        idx = since
        total_new = 0
        for batch in _iter_complete_line_batches(source_path):
            # skip lines already ingested — counted, never hashed or held past this loop
            skip = max(0, since - seen)
            seen += len(batch)
            new_lines = batch[skip:]
            if not new_lines:
                continue
            rows = []
            for raw_line in new_lines:
                line_hash = _chain_hash(prev_hash, raw_line)
                rows.append((harness, anchor_sid, idx, raw_line, line_hash, prev_hash))
                prev_hash = line_hash
                idx += 1
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    await conn.executemany(
                        "INSERT INTO soul_lines "
                        "   (harness, anchor_sid, line_idx, raw_line, line_hash, "
                        "    prev_hash) "
                        "VALUES ($1, $2, $3, $4, $5, $6) "
                        "ON CONFLICT (harness, anchor_sid, line_idx) DO NOTHING",
                        rows,
                    )
            await self._checkpoint(harness, anchor_sid, source_path, idx, prev_hash)
            total_new += len(rows)
        if total_new == 0 and since > 0 and seen == since:
            # THE STORE'S CLOCK IS "LAST SYNCED", NOT "LAST GREW" (operator, 2026-09-03 —
            # Chad's live shape): a full scan that finds the file holds EXACTLY the lines
            # the store already has is a verified sync, and `last_ingested_at` must say
            # so. Before this, a scan with nothing new never touched the row, so a file
            # whose mtime had moved for any reason other than growth (a byte-identical
            # re-materialize, a copy, a `touch`) read as "LIVE — modified more recently
            # than the store's last ingest" forever, and every resume refused to emit
            # canon into the office and fell back to a stale copy. Only an EXACT match
            # earns the touch — a shorter file (a stale partial) is never mistaken for
            # the canon, and a longer one already took the checkpoint above.
            await self._touch(harness, anchor_sid, source_path)
        return total_new

    async def _touch(self, harness: str, anchor_sid: str, source_path: str) -> None:
        """Stamp `last_ingested_at = now()` (and the path just verified as complete) on a
        session the store re-scanned and found fully ingested — see `ingest_path`."""
        await self.pool.execute(
            "UPDATE soul_sessions SET last_ingested_at = now(), source_path = $3 "
            "WHERE harness = $1 AND anchor_sid = $2",
            harness, anchor_sid, source_path,
        )

    async def splice_sources(
        self, anchor_sid: str, source_paths: list[str], *, harness: str | None = None,
        verify: bool = True,
    ) -> int:
        """Ingest SEVERAL DIFFERENT source files as one continuous soul_lines chain for
        `anchor_sid` — the operation `ingest_path` cannot do (thread 6483/6534/6538,
        caught before shipping the wrong claim: `ingest_path`'s own `since` offset slices
        INTO the given file's own line list, which is correct for re-ingesting the SAME
        file after it has grown, and silently WRONG for a second, different file that
        should continue where the first left off — verified live: a 25-line probe
        transcript split at line 12 and fed through two `ingest_path` calls lost 12 of the
        second half's 13 lines, no error, because `since=12` sliced into a 13-line file).

        Reads `_progress` ONCE, then ingests each source's FULL content from ITS OWN line
        0, continuing the running (idx, prev_hash) across every source in a single
        transaction — one soul_lines chain, indistinguishable at read time from a single
        file that never split. `harness` auto-detects off the FIRST source when not
        given. `soul_sessions.source_path` ends up naming the LAST source ingested (same
        "most recent location" convention `ingest_path` already keeps) — a caller that
        needs every physical source a chain was spliced from reads soul_lines' own
        distinct raw content, not this one column.

        `verify=True` (default) runs `verify_jsonl_chain_boundary` on every consecutive
        pair before ingesting anything — refuses the WHOLE splice, writing nothing, on the
        first pair that isn't a genuine continuation (Thoth's own instruction: jesus/chad
        passed this check by hand, the next pair may not, and a blind append would chain
        them anyway). Pass `verify=False` only for sources already known clean by another
        route (e.g. a caller that ran the check itself moments earlier)."""
        if not source_paths:
            return 0
        if verify:
            for a, b in zip(source_paths, source_paths[1:], strict=False):
                reason = verify_jsonl_chain_boundary(a, b)
                if reason is not None:
                    raise ValueError(f"splice_sources refused — {reason}")
        harness = harness or self._detect_harness(source_paths[0])
        idx, prev_hash = await self._progress(anchor_sid, harness=harness)
        all_rows: list[tuple[str, str, int, bytes, str, str | None]] = []
        for source_path in source_paths:
            lines = _split_lines(_read_source(source_path))
            rows, idx, prev_hash = _hash_rows(harness, anchor_sid, lines, idx, prev_hash)
            all_rows.extend(rows)
        if not all_rows:
            return 0
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(
                    "INSERT INTO soul_lines "
                    "   (harness, anchor_sid, line_idx, raw_line, line_hash, prev_hash) "
                    "VALUES ($1, $2, $3, $4, $5, $6) "
                    "ON CONFLICT (harness, anchor_sid, line_idx) DO NOTHING",
                    all_rows,
                )
                await conn.execute(
                    "INSERT INTO soul_sessions "
                    "   (harness, anchor_sid, source_path, last_line_idx, last_hash) "
                    "VALUES ($1, $2, $3, $4, $5) "
                    "ON CONFLICT (harness, anchor_sid) DO UPDATE "
                    "   SET last_ingested_at = now(), "
                    "       last_line_idx = EXCLUDED.last_line_idx, "
                    "       last_hash = EXCLUDED.last_hash, "
                    "       source_path = EXCLUDED.source_path",
                    harness, anchor_sid, source_paths[-1], idx, prev_hash,
                )
        return len(all_rows)

    async def ingest(self, *, cwd: str | None, job_dir: str | None,
                      root: Path | None = None) -> int:
        """Discover this session's transcript via the same locator identity already
        uses, and ingest it. Returns lines newly ingested, 0 when nothing is found or
        nothing changed."""
        for adapter in self._adapters:
            locator = adapter.discover(cwd=cwd, job_dir=job_dir, root=root)
            if locator is not None:
                harness = getattr(locator, "harness", "claude-code")
                return await self.ingest_path(
                    locator.source_path, locator.anchor_sid, harness=harness,
                )
        return 0

    async def ensure_ingested(
        self, *, cwd: str | None, job_dir: str | None, root: Path | None = None,
    ) -> str | None:
        """THE MATERIALIZER'S OWN "ENSURE" STEP (design (b), ruling d161a156/d63b2ca6,
        Thoth dispatch 6620): discover this session via the same adapter `ingest()` uses,
        ingest whatever is new (idempotent — a no-op past the first call), and hand back
        the `anchor_sid` the caller needs for every store query that follows
        (`resume_diagnostics`, `rematerialize_to_disk`) — `ingest()` itself only returns a
        line count, discarding the one thing a caller chaining further store calls
        actually needs. None when no adapter can discover anything at this `job_dir`/`cwd`
        (the session was never anchored on disk at all — a genuine absence, not a store
        miss), distinct from "found and already fully ingested" (returns the anchor_sid
        with 0 new lines, still a real ensure)."""
        for adapter in self._adapters:
            locator = adapter.discover(cwd=cwd, job_dir=job_dir, root=root)
            if locator is not None:
                harness = getattr(locator, "harness", "claude-code")
                await self.ingest_path(locator.source_path, locator.anchor_sid, harness=harness)
                return str(locator.anchor_sid)
        return None

    async def resume_diagnostics(
        self, anchor_sid: str, harness: str = _HARNESS,
    ) -> tuple[int, int, int] | None:
        """The STORE's own (compaction_count, tail_bytes, tail_lines) — the same triple
        `src.ingest.sessions.resume_diagnostics` computes from a transcript FILE, computed
        here from `soul_lines` instead (design (c), ruling d161a156: "check resume_verdict
        against the STORE's own tail measurement, never a disk stat" — the load-bearing
        move: today's verdict is a measurement of whatever file the hunt happened to land
        on; this makes it a measurement of the SESSION, the only thing it was ever supposed
        to mean). None when nothing has been ingested for this `anchor_sid` — a distinct
        fact from "ingested, zero compactions" (which returns `(0, total_bytes, total_lines)`
        same as the file-based function does for a session that never compacted).

        PAGED (same discipline as `verify_chain`/`_stream_verified_write`, msg 6583): a
        300MB-class session's raw content never sits in memory all at once just to answer
        three numbers — each line is measured (a trailing `\\n` counted in, matching how
        the file-based reader counts each line it iterates) and discarded before the next
        page is fetched.

        `harness` (Thoth dispatch 6715, the fourth `_HARNESS` occurrence — worse than its
        five siblings in soul_store.py: this one took no override at all): defaults to
        'claude-code' for backward compatibility, same as `verify_chain`'s own note."""
        total = 0
        count = 0
        lines_total = 0
        last_boundary_bytes: int | None = None
        last_boundary_lines: int | None = None
        seen_any = False
        i = 0
        while True:
            rows = await self.pool.fetch(
                "SELECT raw_line FROM soul_lines WHERE harness=$1 AND anchor_sid=$2 "
                "AND line_idx >= $3 AND line_idx < $4 ORDER BY line_idx ASC",
                harness, anchor_sid, i, i + _REMATERIALIZE_PAGE_LINES)
            if not rows:
                break
            seen_any = True
            for row in rows:
                raw = bytes(row["raw_line"])
                if all(marker in raw for marker in _COMPACT_BOUNDARY_MARKERS):
                    count += 1
                    last_boundary_bytes = total
                    last_boundary_lines = lines_total
                total += len(raw) + 1  # +1: the newline `soul_lines` doesn't store itself
                lines_total += 1
                i += 1
        if not seen_any:
            return None
        tail_bytes = total - last_boundary_bytes if last_boundary_bytes is not None else total
        tail_lines = (lines_total - last_boundary_lines if last_boundary_lines is not None
                      else lines_total)
        return count, tail_bytes, tail_lines

    async def _last_ingested_at(self, harness: str, anchor_sid: str) -> datetime | None:
        """One indexed row lookup — the cheap half of the spend gate, same shape
        transcript_store.py's own `_freshness` uses against `harness_sessions`."""
        return await self.pool.fetchval(  # type: ignore[no-any-return]
            "SELECT last_ingested_at FROM soul_sessions WHERE harness=$1 AND anchor_sid=$2",
            harness, anchor_sid)

    async def backfill(
        self, *, adapters: list[HarnessAdapter] | None = None,
        root: Path | None = None, limit_per_adapter: int = 0,
    ) -> dict[str, int]:
        """The periodic sweep — eat every session file on disk into the store, lines
        newly ingested (task #51 piece 1, Lane 1 msg 6527/ruling ba329ccb). Reuses each
        HarnessAdapter's own `enumerate()` walk, the SAME file discovery
        transcript_store.py's sibling backfill already trusts — no second file-walker.

        DEFAULTS TO CLAUDE-CODE ONLY, never `self._adapters`' full set — piece 1's own
        scope line ("Crush is SQLite-backed with no line-oriented raw concept... out of
        scope here on purpose") is load-bearing here, not cosmetic: `ingest_path` raw-
        byte-splits its source on `\\n`, which would silently mangle a crush.db's binary
        content into garbage "lines" rather than erroring. Confirmed live while testing
        this sweep: CrushSqliteAdapter.enumerate() IGNORES `root` entirely (walks
        projects.json's registered cwds and the fixed seat-office path regardless of what
        `root` is passed) — a caller has no way to sandbox it away by scoping `root`, so
        the exclusion has to happen here, at the adapter-selection level, not by trusting
        `root` to contain the blast radius. Pass `adapters=` explicitly to widen this once
        a piece 2/3 verbatim strategy for DSH/Crush actually exists.

        INCREMENTAL BY A STAT-ONLY SPEND GATE, same law as the sibling store (task #19,
        the one-switch-one-cost rule, 51000597): a session whose source mtime is no newer
        than this store's own `last_ingested_at` costs one stat + one indexed row lookup
        and is never opened — a quiet fleet's steady-state sweep does no file IO beyond
        that. `ingest_path` itself is separately idempotent/resumable (last_line_idx/
        last_hash), so a file that DID change only pays for the lines actually new, never
        a full re-hash — this gate exists purely to skip the `Path.read_bytes()` call
        the resumable path would otherwise still pay for on every untouched file.

        One bad session must not abort the sweep (matches TranscriptStore.backfill's own
        per-session try/except) — a single malformed or vanished file logs nothing here
        (fire-and-forget from a cron tick) and is retried next tick. `limit_per_adapter`
        caps each adapter's sweep (0 = unlimited) so a first run over 2,000+ files does
        not hog one tick — the interrupted-backfill case ruling ba329ccb named explicitly
        is exactly this: resumable across many ticks by construction, never a special
        case. Returns per-adapter counts of SESSIONS touched (not lines) — the same shape
        TranscriptStore.backfill() returns, so a caller summing both stays simple."""
        counts: dict[str, int] = {}
        for adapter in (adapters if adapters is not None else self._claude_code_adapters()):
            sessions = 0
            for locator in adapter.enumerate(root=root):
                try:
                    mtime = _path_mtime(Path(locator.source_path))
                    if mtime is None:
                        continue  # vanished between discovery and here — skip, not an error
                    harness = getattr(locator, "harness", "claude-code")
                    last = await self._last_ingested_at(harness, locator.anchor_sid)
                    if last is not None and mtime <= last:
                        continue  # unchanged since our own last ingest — stat + lookup only
                    new = await self.ingest_path(
                        locator.source_path, locator.anchor_sid, harness=harness)
                    if new:
                        sessions += 1
                except Exception:  # noqa: BLE001 — one bad session must not abort the sweep
                    continue
                if limit_per_adapter and sessions >= limit_per_adapter:
                    break
            counts[adapter.name] = sessions
        return counts

    async def verify_chain(self, anchor_sid: str, harness: str = _HARNESS) -> bool:
        """Re-walk every stored line and recompute the chain from scratch — the honest
        check, never trusting a stored line_hash in isolation. False on the first gap or
        mismatch; True (including vacuously, on zero rows) otherwise. `harness` (thread
        6483/6587, Seshat's own catch): `ingest_path`/`splice_sources` already accept and
        store a real harness — this read-back door defaulted to the module constant
        regardless, so a spliced DSH/Crush anchor_sid would report "nothing ingested"
        against a session genuinely fully stored. Defaults to 'claude-code' for backward
        compatibility with every existing caller.

        PAGED (msg 6583, the 307MB question): reads `soul_lines` `_REMATERIALIZE_PAGE_LINES`
        rows at a time by `line_idx` range (the same index `soul_lines`' own PK already
        gives this query for free) rather than one `fetch()` of every row — a 300MB-class
        session's raw content never sits in memory all at once just to answer a yes/no."""
        expected_prev: str | None = None
        i = 0
        while True:
            rows = await self.pool.fetch(
                "SELECT line_idx, raw_line, line_hash, prev_hash FROM soul_lines "
                "WHERE harness=$1 AND anchor_sid=$2 AND line_idx >= $3 AND line_idx < $4 "
                "ORDER BY line_idx ASC",
                harness, anchor_sid, i, i + _REMATERIALIZE_PAGE_LINES)
            if not rows:
                return True
            for row in rows:
                if row["line_idx"] != i:
                    return False  # a gap in the sequence
                if row["prev_hash"] != expected_prev:
                    return False
                if _chain_hash(row["prev_hash"], row["raw_line"]) != row["line_hash"]:
                    return False
                expected_prev = row["line_hash"]
                i += 1

    async def re_materialize(self, anchor_sid: str, harness: str = _HARNESS) -> str | None:
        """PIECE 2'S SMALLEST HALF, and piece 1's own acceptance test: reconstruct the
        transcript from stored lines alone, byte-for-byte reconstructable from the
        store — no read of the source file. None when nothing has been ingested for this
        session. Callers wanting the newline-joined text (mod a possible trailing
        newline the source may or may not have carried) get exactly that. Decodes the
        stored raw bytes (0052) to `str` at this boundary — `errors='replace'` only ever
        matters for a genuinely non-UTF-8 source, never for the NUL-byte class this
        module exists to survive (NUL is valid UTF-8). `harness` (thread 6483/6587):
        defaults to 'claude-code' for backward compatibility — see `verify_chain`'s own
        note."""
        rows = await self.pool.fetch(
            "SELECT raw_line FROM soul_lines WHERE harness=$1 AND anchor_sid=$2 "
            "ORDER BY line_idx ASC", harness, anchor_sid)
        if not rows:
            return None
        return "\n".join(bytes(r["raw_line"]).decode("utf-8", errors="replace") for r in rows)

    async def raw_lines(self, anchor_sid: str, harness: str = _HARNESS) -> list[str] | None:
        """The stored lines as a plain list, in order — the SAME shape
        `Path.read_text().splitlines()` gives the disk-based miner, so a lines-consuming
        function (distill/models_in, in src.ingest.sessions) works unchanged whether its
        input came from a file or from here. None when nothing has been ingested (never
        an empty list — the two are different facts: 'not ingested' vs 'ingested,
        genuinely empty', though the latter can't happen since ingest_path only ever
        writes a session that had at least one line). Decodes the stored raw bytes (0052)
        to `str` at this boundary, same as `re_materialize`. `harness` (thread 6483/6587):
        defaults to 'claude-code' for backward compatibility — see `verify_chain`'s own
        note."""
        rows = await self.pool.fetch(
            "SELECT raw_line FROM soul_lines WHERE harness=$1 AND anchor_sid=$2 "
            "ORDER BY line_idx ASC", harness, anchor_sid)
        if not rows:
            return None
        return [bytes(r["raw_line"]).decode("utf-8", errors="replace") for r in rows]

    async def mining_view(
        self, anchor_sid: str, harness: str = _HARNESS,
    ) -> list[dict[str, Any]] | None:
        """THE MINING VIEW (task #51 piece 3, ruling 62dc6397): soul_lines projected into
        a mineable per-turn shape — {session, turn_index, role, text, tool_calls} — so a
        miner reads the STORE, never re-parsing raw JSONL by hand. None when nothing has
        been ingested for this session.

        Skips sidechain/meta/compaction-summary lines and anything that isn't a user/
        assistant turn — the same discipline distill() (src.ingest.sessions) already
        applies for the adversary's own text extraction, moved here to the projection
        layer so OTHER miners (closure, decision) get it for free instead of each
        re-implementing the same filter. `text` joins every text block (assistant) or
        the raw string content (user); `tool_calls` is `[{"name", "input"}, ...]` for
        every tool_use block on that turn, `[]` when none. `harness` (thread 6483/6587):
        defaults to 'claude-code' for backward compatibility — see `verify_chain`'s own
        note."""
        rows = await self.pool.fetch(
            "SELECT line_idx, raw_line FROM soul_lines WHERE harness=$1 AND anchor_sid=$2 "
            "ORDER BY line_idx ASC", harness, anchor_sid)
        if not rows:
            return None
        out: list[dict[str, Any]] = []
        for r in rows:
            try:
                d = json.loads(r["raw_line"])
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if not isinstance(d, dict) or d.get("isSidechain") or d.get("isMeta"):
                continue
            role = d.get("type")
            if role not in ("user", "assistant") or d.get("isCompactSummary"):
                continue
            content = (d.get("message") or {}).get("content")
            text_parts: list[str] = []
            tool_calls: list[dict[str, Any]] = []
            if isinstance(content, str):
                if content.strip():
                    text_parts.append(content)
            elif isinstance(content, list):
                for block in content:
                    if not isinstance(block, dict):
                        continue
                    btype = block.get("type")
                    if btype == "text" and block.get("text"):
                        text_parts.append(block["text"])
                    elif btype == "tool_use":
                        tool_calls.append({"name": block.get("name"),
                                           "input": block.get("input")})
            out.append({
                "session": anchor_sid, "turn_index": r["line_idx"], "role": role,
                "text": "\n".join(text_parts), "tool_calls": tool_calls,
            })
        return out

    async def _verified_lines(
        self, anchor_sid: str, harness: str = _HARNESS,
    ) -> tuple[list[bytes] | None, dict[str, Any] | None]:
        """Walk soul_lines in order, verifying the hash chain AS it collects — a break is
        a NAMED state (the second element), never a silent partial result. Returns
        (lines, None) on a clean chain, (None, break_receipt) on the first gap/mismatch,
        (None, None) when nothing was ever ingested for this session — three distinct
        outcomes, never conflated. Raw bytes (0052) — `rematerialize_to_disk`'s own
        byte-exact promise starts here. `harness` (thread 6483/6587): defaults to
        'claude-code' for backward compatibility — see `verify_chain`'s own note.

        KEPT WHOLE-FILE ON PURPOSE, alongside the streamed `_stream_verified_write`
        below (Imhotep's streaming rewrite, cc4bb6a, msg 6593/the 307MB question): a
        SEEK (`rematerialize_to_disk`'s own `upto=`) needs to know where to STOP before
        it can decide whether the tail was withheld — genuinely a two-pass question the
        streamed one-pass writer doesn't answer for free, and reconciling that properly
        is real future work (msg 6620/0c9ac60f's own named coordination cost), not a
        same-night rewrite. The default path (`upto=None`, every real resume call)
        never reaches this — it uses the streamed writer, at full 307MB-scale memory
        savings. Only an explicit seek (Marquee's own repair, a controlled, human-
        supervised operation, never the hot resume path) pays the whole-file cost this
        function still carries."""
        rows = await self.pool.fetch(
            "SELECT line_idx, raw_line, line_hash, prev_hash FROM soul_lines "
            "WHERE harness=$1 AND anchor_sid=$2 ORDER BY line_idx ASC",
            harness, anchor_sid)
        if not rows:
            return None, None
        expected_prev: str | None = None
        lines: list[bytes] = []
        for i, row in enumerate(rows):
            if row["line_idx"] != i:
                return None, {
                    "error": f"chain broken — a GAP at line_idx {i} (expected {i}, "
                             f"found {row['line_idx']})",
                    "verified_through": i - 1}
            if row["prev_hash"] != expected_prev:
                return None, {
                    "error": f"chain broken at line {i} — prev_hash does not match the "
                             "prior line's own hash",
                    "verified_through": i - 1}
            if _chain_hash(row["prev_hash"], row["raw_line"]) != row["line_hash"]:
                return None, {
                    "error": f"chain broken at line {i} — stored hash does not match "
                             "its own content (tampered or corrupted)",
                    "verified_through": i - 1}
            expected_prev = row["line_hash"]
            lines.append(row["raw_line"])
        return lines, None

    async def _stream_verified_write(
        self, anchor_sid: str, target: Path, harness: str = _HARNESS,
    ) -> dict[str, Any]:
        """Page through soul_lines, verifying the hash chain AS each page arrives, and
        write each verified line straight to a temp file next to `target` — never
        holding the reconstructed content in memory (msg 6583: the old whole-file join
        measured another ~888MB peak RSS on top of ingest's own, on a real 307MB
        session). Same promise as before, just streamed: a break is a NAMED receipt
        (`{"error": ..., "verified_through": N}`) and NOTHING lands at `target` — the
        temp file is discarded, never renamed, on the first gap/mismatch or on zero rows
        ever ingested. Only a fully clean pass gets the atomic rename onto `target`.
        `harness` (thread 6483/6587, the same parity fix `verify_chain` carries):
        defaults to 'claude-code' for backward compatibility."""
        f = None
        tmp: Path | None = None
        hasher = hashlib.sha256()
        expected_prev: str | None = None
        i = 0
        wrote_any = False
        while True:
            rows = await self.pool.fetch(
                "SELECT line_idx, raw_line, line_hash, prev_hash FROM soul_lines "
                "WHERE harness=$1 AND anchor_sid=$2 AND line_idx >= $3 AND line_idx < $4 "
                "ORDER BY line_idx ASC",
                harness, anchor_sid, i, i + _REMATERIALIZE_PAGE_LINES)
            if not rows:
                break
            if f is None:
                f, tmp = _open_tmp_writer(target)
            for row in rows:
                if row["line_idx"] != i:
                    _discard_tmp(f, tmp)  # type: ignore[arg-type]
                    return {"error": f"chain broken — a GAP at line_idx {i} (expected "
                                     f"{i}, found {row['line_idx']})",
                            "verified_through": i - 1}
                if row["prev_hash"] != expected_prev:
                    _discard_tmp(f, tmp)  # type: ignore[arg-type]
                    return {"error": f"chain broken at line {i} — prev_hash does not "
                                     "match the prior line's own hash",
                            "verified_through": i - 1}
                if _chain_hash(row["prev_hash"], row["raw_line"]) != row["line_hash"]:
                    _discard_tmp(f, tmp)  # type: ignore[arg-type]
                    return {"error": f"chain broken at line {i} — stored hash does not "
                                     "match its own content (tampered or corrupted)",
                            "verified_through": i - 1}
                expected_prev = row["line_hash"]
                chunk = bytes(row["raw_line"]) + b"\n"
                f.write(chunk)
                hasher.update(chunk)
                wrote_any = True
                i += 1
        if not wrote_any:
            if f is not None:
                _discard_tmp(f, tmp)  # type: ignore[arg-type]
            return {"error": f"no soul_lines ingested for {anchor_sid!r} — nothing to "
                             "materialize"}
        _finalize_tmp(f, tmp, target)  # type: ignore[arg-type]
        return {"written": str(target), "lines": i, "sha256": hasher.hexdigest()}

    async def rematerialize_to_disk(
        self, anchor_sid: str, *, dest: str | None = None, force: bool = False,
        upto: str | None = None, confess: bool = True, harness: str = _HARNESS,
    ) -> dict[str, Any]:
        """PIECE 2: write a session's transcript back to disk, byte-for-byte, from
        soul_lines alone — the acceptance test a soul store stands or falls on, made
        durable rather than in-memory-only (re_materialize's own job, unchanged).

        STREAMS the verify-and-write (`_stream_verified_write`, msg 6583/the 307MB
        question) rather than collecting every line in memory first: a break is still a
        NAMED state in the receipt (`{"error": ..., "verified_through": N}`) and NOTHING
        is written at `target` — never a silently truncated file that LOOKS complete,
        now guaranteed by writing to a temp file and only renaming it onto `target` on a
        fully clean pass, instead of by simply not calling `_write_dest` until the whole
        content was already assembled. SAME RETURN SHAPE, SAME SIGNATURE as before — a
        caller (Khnum's resume-materializing emit included) needs no changes.

        `dest` defaults to this session's own recorded source_path (soul_sessions) — the
        harness's OWN projects-slug convention, so `claude --resume` on a host that never
        had the original file finds the reconstruction in the exact place a live session
        would have written it.

        REFUSES to overwrite a LIVE transcript: if `dest` already exists and its mtime is
        newer than this session's last_ingested_at, something has written to it more
        recently than what the store knows about (the file's own mtime is the "lease" —
        a live process still appending to it keeps moving it forward) — writing over it
        would clobber content the store never saw. `force=True` skips this guard.

        `upto` (thread 6483/6534/6540, the operator's own framing: "the store eats whole
        transcripts, any resume point should be able to be spawned, defaulting to the
        latest") SEEKS to a specific `user`/`assistant` entry's own `uuid` — the ONLY
        valid seek targets (`_addressable_entries`), deliberately narrower than the full
        uuid-bearing chain `verify_jsonl_chain_boundary` walks (`_chain_linked_entries`):
        a caller must never land a resume on a hook-injected `attachment` line, even
        though such a line is a real chain link (thread 6483/6559/6565). None (default)
        emits the full latest chain, unchanged. Given, emits a genuine byte-exact PREFIX
        through that entry — REFUSES (an error dict, nothing written) when `upto` matches
        no addressable line, never a silent full-chain fallback.

        `confess` (default True, no effect when `upto` is None or already the true
        latest): a seek that excludes real, later content is honest only if it SAYS so —
        #102's "mark, never pick a winner" grammar, applied here as "never let a resumed
        mind believe it is whole when it was seeked." Appends ONE synthetic `type":"user"`
        entry (`isMeta: true`, the same marker real hook-injected content already uses,
        so osiris's own mining/rendering skips it as it already skips other isMeta lines)
        naming the seek point and how many later entries were withheld — never silent,
        never a fabricated compaction (osiris does not invent what the model itself never
        summarized). VERIFIED against a live Claude Code resume (thread 6483/6545/6553,
        operator-authorized probe on disposable content): the harness accepts this exact
        confession shape without error, and a resumed mind reads it honestly rather than
        believing itself whole. Pass confess=False for a caller that wants the bare
        prefix with no injected line. `harness` (thread 6483/6587): defaults to
        'claude-code' for backward compatibility — see `verify_chain`'s own note."""
        row = await self.pool.fetchrow(
            "SELECT source_path, last_ingested_at FROM soul_sessions "
            "WHERE harness=$1 AND anchor_sid=$2", harness, anchor_sid)
        if row is None:
            return {"error": f"no soul_lines ingested for {anchor_sid!r} — nothing to "
                             "materialize"}
        target = Path(dest) if dest is not None else Path(row["source_path"])
        if not force:
            existing_mtime = _path_mtime(target)
            if existing_mtime is not None and existing_mtime > row["last_ingested_at"]:
                return {
                    "error": "refused — a LIVE transcript exists at the target",
                    "target": str(target),
                    "target_mtime": existing_mtime.isoformat(),
                    "last_ingested_at": row["last_ingested_at"].isoformat(),
                    "note": "the file was modified more recently than the store's last "
                            "ingest — writing over it would clobber content the store "
                            "never saw. Pass force=True to override.",
                }
            if (upto is None and existing_mtime is not None
                    and str(target) == str(row["source_path"])):
                # THE CANON IS ALREADY HERE (operator, 2026-09-03: "osiris should already
                # have a copy of the latest canon transcript to resume from"): the target
                # IS the very file this session was last ingested FROM, and the guard above
                # just proved nothing touched it since. Re-emitting it byte-for-byte is a
                # 300MB no-op at best; at worst a caller read the write's absence as "no
                # canon at the spawn cwd" and fell back to a STALE copy in another slug —
                # Chad's live shape, 2026-09-03: the office held the full 15.9MB canon,
                # the fabricated tree slug held a 1.8MB partial, and every resume attempt
                # refused here and spawned at the partial. A named success, never an
                # error: `unchanged` tells the caller the canon is at `target` right now.
                return {"written": str(target), "unchanged": True,
                        "note": "target is this session's own last-ingested source — "
                                "the store holds nothing newer to emit"}
        if upto is None:
            # THE FAST PATH, unconditionally: every real resume call reaches here with
            # no seek, and gets Imhotep's streamed writer at full 307MB-scale memory
            # savings — never the whole-file join below.
            return await self._stream_verified_write(anchor_sid, target, harness)
        # A SEEK, genuinely two-pass by nature (need to know where to stop before
        # deciding whether the tail was withheld) — the whole-file `_verified_lines`
        # path, unchanged from before the streaming rewrite. See `_verified_lines`'s
        # own docstring for why this stays whole-file rather than being streamed too.
        lines, broken = await self._verified_lines(anchor_sid, harness)
        if broken is not None:
            return broken
        if lines is None:
            return {"error": f"no soul_lines ingested for {anchor_sid!r} — nothing to "
                             "materialize"}
        addressable = _addressable_entries(lines)
        match = next(((i, u, p) for i, u, p in addressable if u == upto), None)
        if match is None:
            return {"error": f"upto={upto!r} matches no user/assistant entry in "
                             f"{anchor_sid!r}'s stored chain — refusing to guess"}
        match_idx = match[0]
        withheld = len(lines) - (match_idx + 1)
        lines = lines[: match_idx + 1]
        seek_entry: dict[str, Any] | None = None
        if withheld > 0:
            seek_entry = json.loads(lines[match_idx])
        content = b"\n".join(lines) + b"\n"
        if withheld > 0 and confess and seek_entry is not None:
            confession = _confession_line(seek_entry, withheld)
            content += confession + b"\n"
        _write_dest(target, content)
        return {
            "written": str(target), "lines": len(lines),
            "sha256": hashlib.sha256(content).hexdigest(),
            "seek": upto, "withheld_entries": withheld,
        }
