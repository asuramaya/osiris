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
    (0052) — the write side of the same byte-exact promise `_read_source` makes."""
    path.parent.mkdir(parents=True, exist_ok=True)
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


def _finalize_tmp(f: Any, tmp: Path, target: Path) -> None:
    """Close the temp writer and atomically rename it onto `target` — the moment a
    streamed rematerialize actually becomes visible at its destination, all at once,
    never as a partially-written file a concurrent reader could observe mid-stream."""
    f.close()
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
        return total_new

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

    async def resume_diagnostics(self, anchor_sid: str) -> tuple[int, int, int] | None:
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
        page is fetched."""
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
                _HARNESS, anchor_sid, i, i + _REMATERIALIZE_PAGE_LINES)
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

    async def verify_chain(self, anchor_sid: str) -> bool:
        """Re-walk every stored line and recompute the chain from scratch — the honest
        check, never trusting a stored line_hash in isolation. False on the first gap or
        mismatch; True (including vacuously, on zero rows) otherwise.

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
                _HARNESS, anchor_sid, i, i + _REMATERIALIZE_PAGE_LINES)
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

    async def re_materialize(self, anchor_sid: str) -> str | None:
        """PIECE 2'S SMALLEST HALF, and piece 1's own acceptance test: reconstruct the
        transcript from stored lines alone, byte-for-byte reconstructable from the
        store — no read of the source file. None when nothing has been ingested for this
        session. Callers wanting the newline-joined text (mod a possible trailing
        newline the source may or may not have carried) get exactly that. Decodes the
        stored raw bytes (0052) to `str` at this boundary — `errors='replace'` only ever
        matters for a genuinely non-UTF-8 source, never for the NUL-byte class this
        module exists to survive (NUL is valid UTF-8)."""
        rows = await self.pool.fetch(
            "SELECT raw_line FROM soul_lines WHERE harness=$1 AND anchor_sid=$2 "
            "ORDER BY line_idx ASC", _HARNESS, anchor_sid)
        if not rows:
            return None
        return "\n".join(bytes(r["raw_line"]).decode("utf-8", errors="replace") for r in rows)

    async def raw_lines(self, anchor_sid: str) -> list[str] | None:
        """The stored lines as a plain list, in order — the SAME shape
        `Path.read_text().splitlines()` gives the disk-based miner, so a lines-consuming
        function (distill/models_in, in src.ingest.sessions) works unchanged whether its
        input came from a file or from here. None when nothing has been ingested (never
        an empty list — the two are different facts: 'not ingested' vs 'ingested,
        genuinely empty', though the latter can't happen since ingest_path only ever
        writes a session that had at least one line). Decodes the stored raw bytes (0052)
        to `str` at this boundary, same as `re_materialize`."""
        rows = await self.pool.fetch(
            "SELECT raw_line FROM soul_lines WHERE harness=$1 AND anchor_sid=$2 "
            "ORDER BY line_idx ASC", _HARNESS, anchor_sid)
        if not rows:
            return None
        return [bytes(r["raw_line"]).decode("utf-8", errors="replace") for r in rows]

    async def mining_view(self, anchor_sid: str) -> list[dict[str, Any]] | None:
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
        every tool_use block on that turn, `[]` when none."""
        rows = await self.pool.fetch(
            "SELECT line_idx, raw_line FROM soul_lines WHERE harness=$1 AND anchor_sid=$2 "
            "ORDER BY line_idx ASC", _HARNESS, anchor_sid)
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

    async def _stream_verified_write(
        self, anchor_sid: str, target: Path,
    ) -> dict[str, Any]:
        """Page through soul_lines, verifying the hash chain AS each page arrives, and
        write each verified line straight to a temp file next to `target` — never
        holding the reconstructed content in memory (msg 6583: the old whole-file join
        measured another ~888MB peak RSS on top of ingest's own, on a real 307MB
        session). Same promise as before, just streamed: a break is a NAMED receipt
        (`{"error": ..., "verified_through": N}`) and NOTHING lands at `target` — the
        temp file is discarded, never renamed, on the first gap/mismatch or on zero rows
        ever ingested. Only a fully clean pass gets the atomic rename onto `target`."""
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
                _HARNESS, anchor_sid, i, i + _REMATERIALIZE_PAGE_LINES)
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
        would clobber content the store never saw. `force=True` skips this guard."""
        row = await self.pool.fetchrow(
            "SELECT source_path, last_ingested_at FROM soul_sessions "
            "WHERE harness=$1 AND anchor_sid=$2", _HARNESS, anchor_sid)
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
        return await self._stream_verified_write(anchor_sid, target)
