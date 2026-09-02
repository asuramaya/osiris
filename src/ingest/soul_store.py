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
import uuid as _uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg

from src.ingest.harness import HarnessAdapter

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
    """`content.split(b"\\n")`, minus the phantom trailing empty element a terminating
    newline would otherwise produce — matches `str.splitlines()`'s own behavior for the
    ordinary `\\n`-terminated case (every real transcript), without decoding to text
    first (0052: decoding here is exactly the step that used to normalize away an
    embedded NUL's exact byte position)."""
    if not content:
        return []
    lines = content.split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    return lines


def _dest_mtime(path: Path) -> datetime | None:
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
    """`(line_idx, uuid, parentUuid)` for every `user`/`assistant` line — the ONLY entry
    types that carry a `uuid` at all (confirmed live: `queue-operation`/`attachment`/
    `mode`/`last-prompt` lines never do) and therefore the only lines a chain-continuity
    check or a `rematerialize_to_disk(upto=...)` seek can address. Lines that fail to
    parse as JSON, or parse but carry no `uuid`, are silently skipped — they are real
    transcript content (kept byte-exact by the store regardless) but not addressable
    nodes in the harness's own parentUuid chain."""
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
        "uuid": str(_uuid.uuid4()),
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
    addressable entry's `parentUuid` must equal `file_a`'s LAST addressable entry's
    `uuid` — the literal join point; (2) zero `uuid` overlap between the two files — two
    genuinely different halves, never the same content ingested twice under different
    names; (3) every entry in `file_b` (after its first, already checked by (1)) must
    have a `parentUuid` resolving to a `uuid` seen earlier in `file_a` or `file_b` — "B
    entries whose parent is in neither file: ZERO", 2e05d662's own phrasing. `file_a`'s
    OWN internal structure is trusted, not re-audited — it is a real, harness-written
    prefix already ingested as itself; this function's job is the JOIN, not a second
    from-scratch chain audit of content nobody is disputing.

    A file with NO addressable entries at all can never be verified as either half of a
    chain — refused, not silently treated as trivially clean."""
    a = _addressable_entries(_split_lines(_read_source(file_a)))
    b = _addressable_entries(_split_lines(_read_source(file_b)))
    if not a:
        return f"{file_a} has no addressable (user/assistant) entries — nothing to verify"
    if not b:
        return f"{file_b} has no addressable (user/assistant) entries — nothing to verify"
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

    async def ingest_path(
        self, source_path: str, anchor_sid: str, harness: str | None = None,
    ) -> int:
        """Eat every line of `source_path` not yet ingested for `anchor_sid`. Idempotent —
        re-running over an unchanged file appends nothing (progress already covers every
        line). Returns the count of NEW lines ingested. Never re-reads or re-hashes lines
        already stored: the chain resumes from last_hash, exactly where it left off.

        `harness` names the harness that owns this transcript (e.g. 'claude-code', 'dsh',
        'crush'). When None, it's auto-detected from `self._adapters` by matching
        `source_path` to an adapter's discovery."""
        harness = harness or self._detect_harness(source_path)
        content = _read_source(source_path)
        lines = _split_lines(content)
        since, prev_hash = await self._progress(anchor_sid, harness=harness)
        new_lines = lines[since:]
        if not new_lines:
            return 0
        rows, idx, prev_hash = _hash_rows(harness, anchor_sid, new_lines, since, prev_hash)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(
                    "INSERT INTO soul_lines "
                    "   (harness, anchor_sid, line_idx, raw_line, line_hash, prev_hash) "
                    "VALUES ($1, $2, $3, $4, $5, $6) "
                    "ON CONFLICT (harness, anchor_sid, line_idx) DO NOTHING",
                    rows,
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
                    harness, anchor_sid, source_path, idx, prev_hash,
                )
        return len(rows)

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

    async def verify_chain(self, anchor_sid: str) -> bool:
        """Re-walk every stored line and recompute the chain from scratch — the honest
        check, never trusting a stored line_hash in isolation. False on the first gap or
        mismatch; True (including vacuously, on zero rows) otherwise."""
        rows = await self.pool.fetch(
            "SELECT line_idx, raw_line, line_hash, prev_hash FROM soul_lines "
            "WHERE harness=$1 AND anchor_sid=$2 ORDER BY line_idx ASC",
            _HARNESS, anchor_sid)
        expected_prev: str | None = None
        for i, row in enumerate(rows):
            if row["line_idx"] != i:
                return False  # a gap in the sequence
            if row["prev_hash"] != expected_prev:
                return False
            if _chain_hash(row["prev_hash"], row["raw_line"]) != row["line_hash"]:
                return False
            expected_prev = row["line_hash"]
        return True

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

    async def _verified_lines(
        self, anchor_sid: str,
    ) -> tuple[list[bytes] | None, dict[str, Any] | None]:
        """Walk soul_lines in order, verifying the hash chain AS it collects — a break is
        a NAMED state (the second element), never a silent partial result. Returns
        (lines, None) on a clean chain, (None, break_receipt) on the first gap/mismatch,
        (None, None) when nothing was ever ingested for this session — three distinct
        outcomes, never conflated. Raw bytes (0052) — `rematerialize_to_disk`'s own
        byte-exact promise starts here."""
        rows = await self.pool.fetch(
            "SELECT line_idx, raw_line, line_hash, prev_hash FROM soul_lines "
            "WHERE harness=$1 AND anchor_sid=$2 ORDER BY line_idx ASC",
            _HARNESS, anchor_sid)
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

    async def rematerialize_to_disk(
        self, anchor_sid: str, *, dest: str | None = None, force: bool = False,
        upto: str | None = None, confess: bool = True,
    ) -> dict[str, Any]:
        """PIECE 2: write a session's transcript back to disk, byte-for-byte, from
        soul_lines alone — the acceptance test a soul store stands or falls on, made
        durable rather than in-memory-only (re_materialize's own job, unchanged).

        Verifies the hash chain WHILE collecting (`_verified_lines`), not as a separate
        pass before a separate write: a break is a NAMED state in the receipt
        (`{"error": ..., "verified_through": N}`) and NOTHING is written — never a
        silently truncated file that LOOKS complete.

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
        latest") SEEKS to a specific `user`/`assistant` entry's own `uuid` — the harness's
        own parentUuid-chained addressable nodes (`_addressable_entries`), the same chain
        `verify_jsonl_chain_boundary` walks. None (default) emits the full latest chain,
        unchanged. Given, emits a genuine byte-exact PREFIX through that entry — REFUSES
        (an error dict, nothing written) when `upto` matches no line in the chain, never a
        silent full-chain fallback.

        `confess` (default True, no effect when `upto` is None or already the true
        latest): a seek that excludes real, later content is honest only if it SAYS so —
        #102's "mark, never pick a winner" grammar, applied here as "never let a resumed
        mind believe it is whole when it was seeked." Appends ONE synthetic `type":"user"`
        entry (`isMeta: true`, the same marker real hook-injected content already uses,
        so osiris's own mining/rendering skips it as it already skips other isMeta lines)
        naming the seek point and how many later entries were withheld — never silent,
        never a fabricated compaction (osiris does not invent what the model itself never
        summarized). UNVERIFIED against a live Claude Code resume as of this build — the
        prefix-cut mechanism itself is proven byte-exact; whether the harness accepts
        THIS confession entry's exact shape on `--resume` has not been probed. Pass
        confess=False for a caller that wants the bare prefix with no injected line."""
        row = await self.pool.fetchrow(
            "SELECT source_path, last_ingested_at FROM soul_sessions "
            "WHERE harness=$1 AND anchor_sid=$2", _HARNESS, anchor_sid)
        if row is None:
            return {"error": f"no soul_lines ingested for {anchor_sid!r} — nothing to "
                             "materialize"}
        target = Path(dest) if dest is not None else Path(row["source_path"])
        if not force:
            existing_mtime = _dest_mtime(target)
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
        lines, broken = await self._verified_lines(anchor_sid)
        if broken is not None:
            return broken
        if lines is None:
            return {"error": f"no soul_lines ingested for {anchor_sid!r} — nothing to "
                             "materialize"}
        withheld = 0
        seek_entry: dict[str, Any] | None = None
        if upto is not None:
            addressable = _addressable_entries(lines)
            match = next(((i, u, p) for i, u, p in addressable if u == upto), None)
            if match is None:
                return {"error": f"upto={upto!r} matches no user/assistant entry in "
                                 f"{anchor_sid!r}'s stored chain — refusing to guess"}
            match_idx = match[0]
            withheld = len(lines) - (match_idx + 1)
            lines = lines[: match_idx + 1]
            if withheld > 0:
                seek_entry = json.loads(lines[match_idx])
        content = b"\n".join(lines) + b"\n"
        if withheld > 0 and confess and seek_entry is not None:
            confession = _confession_line(seek_entry, withheld)
            content += confession + b"\n"
        _write_dest(target, content)
        out = {
            "written": str(target), "lines": len(lines),
            "sha256": hashlib.sha256(content).hexdigest(),
        }
        if upto is not None:
            out["seek"] = upto
            out["withheld_entries"] = withheld
        return out
