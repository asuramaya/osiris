#!/usr/bin/env python3
"""The retention ladder for the DB dump population in both `backups/` and the vault, and
for `<vault>/basebackups/` (the weekly pg_basebackup taken by osiris_base_backup.sh). A
base backup is, like a DB dump, complete and independently restorable on its own, so the
same ladder applies unmodified. It is a classic GFS (grandfather-father-son) thinning
schedule: every survivor inside the last 48h stays whole (nothing thinned there at all);
48h-30d thins to one per calendar day; 30d-1y thins to one per calendar week; beyond 1y
thins to one per calendar month, forever. Within any bucket the newest survives: the
graph is append-only, so a newer full snapshot contains every older one, and every
survivor here is a full snapshot by construction, so no special-casing is needed.

`plan_prune`/`--apply`'s main pass is scoped to DB dumps only (osiris-*.dump, or the
pre-.dump-switch osiris-*.sql still on disk during the transition). An individual
transcript tarball is never a candidate here, since pruning one out of the middle of its
own incremental chain breaks every later day's restorability. `plan_prune_transcript_chains`
is the separate, chain-aware sibling for that: osiris_backup.sh keys the snapshot file
and each day's tarball by ISO week, so restorability only ever needs to span one week's
own chain. This prunes whole weekly chains at a time (every tarball plus the snapshot
file sharing one week-key, together, never a partial chain), keeping the last 4 weekly
chains plus one chain per calendar month beyond that.

Dry-run is the default mode: nothing is deleted until a human reviews the printed list.
`--apply` exists so a human can actually execute the ladder directly, by hand, any time;
this script itself never calls `--apply` on its own. `--manifest`/`--apply-if-clear` are
wired into a weekly timer pair but stay gated the same way: `--apply-if-clear` refuses
outright unless the prior day's `--manifest` brief is at least 20h old and was never
dimmed (find_clear_manifest). Leaving the brief undimmed is the explicit go-ahead; this
is not an unconditioned cron.

Also covers, wired into the same manifest/apply-if-clear gate as everything above: the
35 legacy pre-week-key transcript tarballs (`plan_prune_legacy_tarballs`/
`_scan_legacy_tarballs`) and the transcript-cache-prune session population
(`_collect_session_prune_plan`, delegating to
osiris_transcript_cache_prune.find_prunable_sessions unchanged).

Dormant seat transcripts: a re-minted seat can carry a large resumable transcript (one
observed case: 57 MB, 8 compactions, last touched days ago) under its own office
directory that nothing currently ever revisits once the seat succeeds and its lineage
moves on. `attribute_sessions_to_seats` groups the same already-safety-checked
`_collect_session_prune_plan` population (dead and fully captured, never a raw mtime
guess) by which seat's own office directory produced it, purely for the manifest's own
reading, for example "worker-a: 3 file(s), 57.2 MB" instead of an unattributed flat path
list. This changes nothing about what gets deleted, when, or how safely; it rides the
identical manifest-then-dim gate and the identical `_apply` call every other population
here does.

Pure logic (`plan_prune`) is tested directly; the CLI is a thin scan-report-optionally-
delete shell around it."""
from __future__ import annotations

import argparse
import contextlib
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

if TYPE_CHECKING:
    from scripts.osiris_transcript_cache_prune import SessionRow

_NAME_RE = re.compile(r"^osiris-(\d{8})-(\d{6})\.(?:dump|sql)$")
_BASEBACKUP_RE = re.compile(r"^osiris-basebackup-(\d{8})-(\d{6})\.tar\.gz$")


@dataclass(frozen=True)
class DumpFile:
    path: str
    when: datetime
    size_bytes: int = 0


def _bucket_key(when: datetime, tier: str) -> str:
    if tier == "daily":
        return when.date().isoformat()
    if tier == "weekly":
        iso = when.isocalendar()
        return f"{iso[0]}-W{iso[1]:02d}"
    if tier == "monthly":
        return when.strftime("%Y-%m")
    raise ValueError(f"unknown tier: {tier!r}")


def plan_prune(
    files: list[DumpFile], *, now: datetime,
    hot_window: timedelta = timedelta(hours=48),
    daily_window: timedelta = timedelta(days=30),
    weekly_window: timedelta = timedelta(days=365),
) -> dict[str, list[DumpFile]]:
    """The ladder itself, pure and total-order-free of any filesystem call: given every
    dump's own (path, timestamp), returns {"keep": [...], "remove": [...]}, both sorted
    oldest-first. `remove` is what a caller would delete; this function never deletes
    anything itself."""
    files_sorted = sorted(files, key=lambda f: f.when)
    keep: set[str] = set()
    tiers: dict[str, list[DumpFile]] = {"hot": [], "daily": [], "weekly": [], "monthly": []}
    for f in files_sorted:
        age = now - f.when
        if age <= hot_window:
            tiers["hot"].append(f)
        elif age <= daily_window:
            tiers["daily"].append(f)
        elif age <= weekly_window:
            tiers["weekly"].append(f)
        else:
            tiers["monthly"].append(f)
    keep.update(f.path for f in tiers["hot"])  # the hot window is never thinned
    for tier_name in ("daily", "weekly", "monthly"):
        buckets: dict[str, DumpFile] = {}
        for f in tiers[tier_name]:
            key = _bucket_key(f.when, tier_name)
            if key not in buckets or f.when > buckets[key].when:
                buckets[key] = f
        keep.update(f.path for f in buckets.values())
    return {
        "keep": [f for f in files_sorted if f.path in keep],
        "remove": [f for f in files_sorted if f.path not in keep],
    }


_DUMP_LIKE_SUFFIXES = (".dump", ".sql", ".tar.gz")


def _scan(directory: Path, *, glob: str = "osiris-*") -> list[DumpFile]:
    """Every file matching `glob` in `directory` (non-recursive: `backups/`, the vault,
    and `<vault>/basebackups/` are each flat). Timestamp is parsed from the filename
    itself (osiris_backup.sh/osiris_base_backup.sh already stamp it in, either
    `_NAME_RE` for a DB dump or `_BASEBACKUP_RE` for a base backup, tried in that order),
    falling back to the file's own mtime for a name that carries one of the dump-shaped
    extensions (`_DUMP_LIKE_SUFFIXES`) but not the exact naming pattern. It is never
    silently skipped, so a stray real dump/base-backup still gets a real answer instead
    of vanishing from the ladder's view. A base backup is, like a DB dump, a complete and
    independently restorable unit on its own (`pg_basebackup`'s whole point), so the same
    `plan_prune` ladder applies to both, unlike the transcript tarballs' own chain-scoped
    sibling.

    Anything else matching `glob` but not one of those extensions is skipped outright,
    never given a mtime fallback. The broad `osiris-*` glob this function's own callers
    default to also matches `osiris-repo.bundle` (osiris_backup.sh's own git-bundle
    refresh, rewritten every 6-hourly run), and that file's mtime is always the most
    recent thing in the vault. Before this guard, the mtime fallback let it win
    `max(dumps, key=lambda f: f.when)` in osiris_disk_guard.py's own `main()` every
    single time, silently replacing a roughly 2.7GB real dump with a roughly 10MB bundle
    as the last dump. The disk guard's own margin check (the exact safety net this vault
    lane was built to add) was checking against the wrong file's size on every run,
    found while investigating a WAL-pull gap."""
    out: list[DumpFile] = []
    if not directory.is_dir():
        return out
    for p in sorted(directory.glob(glob)):
        if not p.is_file():
            continue
        m = _NAME_RE.match(p.name) or _BASEBACKUP_RE.match(p.name)
        if m:
            when = datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S").replace(tzinfo=UTC)
        elif p.name.endswith(_DUMP_LIKE_SUFFIXES):
            when = datetime.fromtimestamp(p.stat().st_mtime, tz=UTC)
        else:
            continue
        out.append(DumpFile(str(p), when, p.stat().st_size))
    return out


_WEEK_RE = re.compile(r"^claude-transcripts-(\d{4})-W(\d{2})-\d{8}\.tar\.gz$")


@dataclass(frozen=True)
class TranscriptChain:
    week_key: str  # "2026-W37"
    week_start: datetime  # the ISO week's own Monday, for month-bucketing
    files: list[str]  # every tarball + snapshot file belonging to this chain
    size_bytes: int = 0


def plan_prune_transcript_chains(
    chains: list[TranscriptChain], *, keep_recent: int = 4,
) -> dict[str, list[TranscriptChain]]:
    """Chain-aware, unlike `plan_prune`: the unit thinned is a whole weekly chain (every
    tarball sharing one week-key, plus that week's snapshot file), never a single tarball
    out of the middle of one. Removing a chain removes exactly the week it belongs to,
    always in full, so a survivor's own restorability never depends on a chain this
    function didn't keep. The most recent `keep_recent` chains (by week, not by age
    against `now`; the last four weekly chains, not a fixed cutoff) survive whole; older
    chains thin to one per calendar month, newest-in-month wins."""
    chains_sorted = sorted(chains, key=lambda c: c.week_start, reverse=True)
    keep_keys = {c.week_key for c in chains_sorted[:keep_recent]}
    buckets: dict[str, TranscriptChain] = {}
    for c in chains_sorted[keep_recent:]:
        mkey = c.week_start.strftime("%Y-%m")
        if mkey not in buckets or c.week_start > buckets[mkey].week_start:
            buckets[mkey] = c
    keep_keys.update(c.week_key for c in buckets.values())
    return {
        "keep": [c for c in chains_sorted if c.week_key in keep_keys],
        "remove": [c for c in chains_sorted if c.week_key not in keep_keys],
    }


@dataclass(frozen=True)
class WalSegment:
    path: str
    when: datetime  # the segment's own file mtime in the vault, see _scan_wal
    size_bytes: int = 0


def plan_prune_wal(
    segments: list[WalSegment], *, oldest_kept_backup_when: datetime | None,
) -> dict[str, list[WalSegment]]:
    """WAL retention: keep archived segments only back to the oldest still-kept base
    backup. A segment archived before that backup was even taken can never be replayed
    into any base backup this ladder still keeps (PITR always starts from a base and
    replays forward), so it is dead weight the instant the backup it could have served
    is itself pruned.

    `oldest_kept_backup_when=None` (no base backup survives the ladder at all, for
    example a fresh install with none taken yet) means keep everything. Refusing to
    guess at a retention point with nothing to anchor it to is safer than deleting WAL
    that might still be needed for the very next backup taken."""
    if oldest_kept_backup_when is None:
        return {"keep": list(segments), "remove": []}
    keep = [s for s in segments if s.when >= oldest_kept_backup_when]
    remove = [s for s in segments if s.when < oldest_kept_backup_when]
    return {"keep": keep, "remove": remove}


def _scan_wal(directory: Path) -> list[WalSegment]:
    """Every archived WAL segment in `directory` (`<vault>/wal_archive`). A 24-hex-char
    name carries no embedded timestamp of its own, unlike a dump or base backup
    filename, so `when` is the file's own mtime: postgres archives strictly in
    ascending LSN order and osiris_backup.sh's own WAL pull processes `ls -1`'s
    lexicographic order (which sorts correctly for these zero-padded hex names), so
    mtime-in-the-vault tracks real archive order closely enough for a retention
    decision. Hidden files (a stray `.gitkeep` or similar) are skipped, never
    silently pruned."""
    if not directory.is_dir():
        return []
    out = []
    for p in sorted(directory.iterdir()):
        if not p.is_file() or p.name.startswith("."):
            continue
        out.append(WalSegment(str(p), datetime.fromtimestamp(p.stat().st_mtime, tz=UTC),
                              p.stat().st_size))
    return out


def _scan_transcript_chains(directory: Path) -> list[TranscriptChain]:
    """Every `claude-transcripts-<week>-<day>.tar.gz` in `directory`, grouped by its own
    week-key into a `TranscriptChain`. The corresponding `.transcript-archive-<week>.snar`
    snapshot file, if still present, rides along as part of the same chain (it's only ever
    needed to produce that week's own next incremental, never to extract an existing one,
    but it belongs to the chain's identity all the same)."""
    if not directory.is_dir():
        return []
    groups: dict[str, list[Path]] = {}
    for p in sorted(directory.glob("claude-transcripts-*.tar.gz")):
        m = _WEEK_RE.match(p.name)
        if m:
            groups.setdefault(f"{m.group(1)}-W{m.group(2)}", []).append(p)
    chains = []
    for key, paths in groups.items():
        year, week = int(key[:4]), int(key[6:8])
        week_start = datetime.fromisocalendar(year, week, 1).replace(tzinfo=UTC)
        snar = directory / f".transcript-archive-{key}.snar"
        files = [str(p) for p in paths]
        if snar.is_file():
            files.append(str(snar))
        size = sum(Path(f).stat().st_size for f in files)
        chains.append(TranscriptChain(key, week_start, files, size))
    return chains


def plan_prune_legacy_tarballs(files: list[DumpFile]) -> dict[str, list[DumpFile]]:
    """The pre-week-key transcript tarballs, now retired: `claude-transcripts-*` files
    that never reached the finished, week-keyed shape `_scan_transcript_chains`/
    `_WEEK_RE` recognizes. That is 32 abandoned `.tar.gz.new` staging leftovers (the old
    archive block wrote to `.new` then renamed on success; these never got that rename)
    plus 3 finished pre-week-key `.tar.gz` files, 35 total on this box the day this was
    written. Unlike `plan_prune_transcript_chains`'s own keep-4-plus-monthly ladder,
    this population is removed in full, no ladder, no survivors: the transcript-tarball
    archive line is retired (soul_lines now carries every byte, proven by the round-trip
    sweep against the full population), and no restorability concern favors keeping any
    one of these over another. They are just the last of a superseded scheme, invisible
    to every prior manifest because nothing before this ever scanned for this shape at
    all."""
    return {"keep": [], "remove": list(files)}


def _scan_legacy_tarballs(vault: Path) -> list[DumpFile]:
    """Every `claude-transcripts-*` file in the vault that `_scan_transcript_chains`
    does not already own (i.e. does not match `_WEEK_RE`): the complement, computed
    against the same regex that function uses, so the two scans can never double-count
    a single file between them. Matches both extensions on disk: finished `.tar.gz`
    and abandoned `.tar.gz.new`."""
    if not vault.is_dir():
        return []
    out = []
    for p in sorted(vault.glob("claude-transcripts-*")):
        if not p.is_file() or _WEEK_RE.match(p.name):
            continue
        out.append(DumpFile(str(p), datetime.fromtimestamp(p.stat().st_mtime, tz=UTC),
                            p.stat().st_size))
    return out


def _report_chains_text(label: str, plan: dict[str, list[TranscriptChain]]) -> str:
    removed_bytes = sum(c.size_bytes for c in plan["remove"])
    lines = [f"\n{label} transcript chains: {len(plan['keep'])} kept, "
             f"{len(plan['remove'])} whole chains would be removed "
             f"({removed_bytes / (1024**3):.2f} GB)"]
    for c in plan["remove"]:
        lines.append(f"  REMOVE CHAIN {c.week_key}  ({len(c.files)} files, "
                     f"{c.size_bytes / (1024**2):.1f} MB)")
    return "\n".join(lines)


def _report_text(label: str, plan: dict[str, list[DumpFile]]) -> str:
    removed_bytes = sum(f.size_bytes for f in plan["remove"])
    lines = [f"\n{label}: {len(plan['keep'])} kept, {len(plan['remove'])} would be removed "
             f"({removed_bytes / (1024**3):.2f} GB)"]
    for f in plan["remove"]:
        lines.append(f"  REMOVE  {f.path}  ({f.when.isoformat()}, "
                     f"{f.size_bytes / (1024**2):.1f} MB)")
    return "\n".join(lines)


def _report_chains(label: str, plan: dict[str, list[TranscriptChain]]) -> None:
    print(_report_chains_text(label, plan))


def _report(label: str, plan: dict[str, list[DumpFile]]) -> None:
    print(_report_text(label, plan))


def _report_wal_text(plan: dict[str, list[WalSegment]]) -> str:
    removed_bytes = sum(s.size_bytes for s in plan["remove"])
    lines = [f"\nvault/wal_archive: {len(plan['keep'])} segment(s) kept, "
             f"{len(plan['remove'])} would be removed "
             f"({removed_bytes / (1024**3):.2f} GB)"]
    for s in plan["remove"][:10]:  # a segment list can run into the hundreds; sample it
        lines.append(f"  REMOVE  {s.path}  ({s.when.isoformat()}, "
                     f"{s.size_bytes / (1024**2):.1f} MB)")
    if len(plan["remove"]) > 10:
        lines.append(f"  ... and {len(plan['remove']) - 10} more segment(s)")
    return "\n".join(lines)


def _report_wal(plan: dict[str, list[WalSegment]]) -> None:
    print(_report_wal_text(plan))


def _report_sessions_text(plan: list[SessionRow]) -> str:
    """The transcript-cache-prune population: unlike the file-scan populations above,
    `plan` is already the finished list of prunable sessions
    (osiris_transcript_cache_prune.find_prunable_sessions has already applied the
    dead/fully-captured test), so there is no separate "keep" side to report here,
    same as that script's own dry-run print."""
    lines = [f"\ntranscript cache (dead subagent session file(s)): {len(plan)} "
             "would be removed"]
    for s in plan[:10]:
        lines.append(f"  PRUNE  {s.source_path}  (anchor {s.anchor_sid})")
    if len(plan) > 10:
        lines.append(f"  ... and {len(plan) - 10} more")
    return "\n".join(lines)


def _report_sessions(plan: list[SessionRow]) -> None:
    print(_report_sessions_text(plan))


def _seat_handles(office_root: Path) -> list[str]:
    """Every seat with a real office directory on disk right now: one subdir per handle
    (offices.py's own `_default_office_root`/`seat_office_target` convention: `<office_root>/
    <handle>`, lowercased). Purely a disk scan, no DB, the same offline-usable shape
    every other `_scan_*` helper in this file already has. Missing `office_root` (a fresh
    box, or a synthetic tmp_path in a test) is a normal empty case, never an error."""
    if not office_root.is_dir():
        return []
    return sorted(p.name for p in office_root.iterdir() if p.is_dir())


def _seat_transcript_dir(handle: str, *, office_root: Path, projects_root: Path) -> Path:
    """The harness transcript directory a seat's own office cwd writes to: the same
    `_harness_slug` the harness itself keys `~/.claude/projects/` on (mounts.py), so this
    always agrees with wherever a real seat's session files actually landed, never a
    separately-maintained guess that could drift from the harness's own convention."""
    from src.orchestrator.mounts import _harness_slug

    cwd = str(office_root / handle)
    return projects_root / _harness_slug(cwd)


def attribute_sessions_to_seats(
    sessions: list[SessionRow], *, office_root: Path, projects_root: Path,
) -> dict[str, list[SessionRow]]:
    """Groups an already-computed prune-eligible `sessions` list
    (`find_prunable_sessions`'s own output: dead and fully captured, the identical
    safety test every other session-cache prune already uses) by which seat's office
    directory produced it. Nothing here changes what gets deleted or when: same session
    rows, same `_apply` call, same manifest-then-dim gate. This only adds a per-seat
    rollup for the manifest's own reading, for example "worker-a: 3 file(s), 57.2 MB"
    rather than a flat, unattributed path list. A session whose directory matches no
    known seat's own transcript directory (an ordinary, non-office project) is left out
    of the grouping entirely; it still appears in the existing flat session report,
    untouched."""
    handles = _seat_handles(office_root)
    dir_to_handle = {
        str(_seat_transcript_dir(h, office_root=office_root, projects_root=projects_root)): h
        for h in handles
    }
    out: dict[str, list[SessionRow]] = {}
    for s in sessions:
        handle = dir_to_handle.get(str(Path(s.source_path).parent))
        if handle is None:
            continue
        out.setdefault(handle, []).append(s)
    return out


def _report_seat_transcripts_text(groups: dict[str, list[SessionRow]]) -> str:
    """Sizes are computed here, at report time. `SessionRow` itself carries no size
    field (unlike `DumpFile`/`WalSegment`, `soul_sessions` never stores one), so this
    does a plain `Path.stat()` per file, tolerant of a file that's already gone (0 bytes
    counted, never a crash: the same file a concurrent `--apply-if-clear` run might have
    already removed moments earlier)."""
    if not groups:
        return "\ndormant seat transcripts: none"
    lines = [f"\ndormant seat transcripts, {len(groups)} seat(s):"]
    for handle in sorted(groups):
        rows = groups[handle]
        size = 0
        for r in rows:
            with contextlib.suppress(OSError):
                size += Path(r.source_path).stat().st_size
        newest = max((r.file_mtime for r in rows if r.file_mtime is not None), default=None)
        touched = f", last touched {newest.isoformat()}" if newest else ""
        lines.append(f"  {handle}: {len(rows)} file(s), {size / (1024**2):.1f} MB{touched}")
    return "\n".join(lines)


def _report_seat_transcripts(groups: dict[str, list[SessionRow]]) -> None:
    print(_report_seat_transcripts_text(groups))


def build_manifest_body(
    plans: dict[str, dict[str, list[DumpFile]]],
    chain_plan: dict[str, list[TranscriptChain]],
    wal_plan: dict[str, list[WalSegment]] | None = None,
    legacy_plan: dict[str, list[DumpFile]] | None = None,
    session_plan: list[SessionRow] | None = None,
    seat_transcript_groups: dict[str, list[SessionRow]] | None = None,
) -> str:
    """Pure: the exact text mailed to the operator's desk, the same wording the dry-run
    CLI prints, so a human reading the mail sees exactly what a human running the
    command by hand would have seen. `wal_plan`, `legacy_plan`, `session_plan`, and
    `seat_transcript_groups` (a rollup of `session_plan`, never a separate deletion) are
    all optional, so omitted callers/tests that predate each addition still get a valid
    manifest, just without that section."""
    total_remove = sum(len(p["remove"]) for p in plans.values())
    total_chains_remove = len(chain_plan["remove"])
    total_wal_remove = len(wal_plan["remove"]) if wal_plan else 0
    total_legacy_remove = len(legacy_plan["remove"]) if legacy_plan else 0
    total_session_remove = len(session_plan) if session_plan else 0
    parts = [f"PRUNE LADDER MANIFEST: {total_remove} dump file(s), "
             f"{total_chains_remove} transcript chain(s), {total_wal_remove} WAL "
             f"segment(s), {total_legacy_remove} legacy transcript tarball(s), and "
             f"{total_session_remove} transcript cache file(s) are planned for removal "
             "tomorrow unless this brief is dimmed before then (thread 9fac4e0d)."]
    parts.extend(_report_text(label, plan) for label, plan in plans.items())
    parts.append(_report_chains_text("vault", chain_plan))
    if wal_plan is not None:
        parts.append(_report_wal_text(wal_plan))
    if legacy_plan is not None:
        parts.append(_report_text("vault/legacy-transcripts", legacy_plan))
    if session_plan is not None:
        parts.append(_report_sessions_text(session_plan))
    if seat_transcript_groups is not None:
        parts.append(_report_seat_transcripts_text(seat_transcript_groups))
    parts.append("\nRun scripts/osiris_prune_ladder.py (no flags) yourself for the "
                 "identical dry-run at any time. To stop tomorrow's apply, dim this "
                 "brief.")
    return "\n".join(parts)


def _oldest_kept_backup_when(
    basebackup_plan: dict[str, list[DumpFile]],
) -> datetime | None:
    """The anchor WAL retention prunes against: the oldest base backup this ladder run
    still keeps (not the oldest one that exists on disk, which may itself be about to
    be pruned). None when no base backup survives at all."""
    kept = basebackup_plan["keep"]
    return min((f.when for f in kept), default=None)


def _compute_plans(
    backups: Path, vault: Path,
) -> tuple[dict[str, dict[str, list[DumpFile]]], dict[str, list[TranscriptChain]],
           dict[str, list[WalSegment]], dict[str, list[DumpFile]]]:
    """The scan-and-plan step, shared by the dry-run CLI, --apply, --manifest, and
    --apply-if-clear: one computation, never four copies to drift apart. Everything
    here is a pure file scan (no DB, no network). The session-cache-prune population
    is deliberately not part of this function since it needs a real DB query; it is
    gathered separately, only by the --manifest/--apply-if-clear branches, so the plain
    dry-run/--apply CLI stays usable with no DB at all, exactly as every existing test
    here already assumes."""
    now = datetime.now(UTC)
    plans = {
        "backups/": plan_prune(_scan(backups), now=now),
        "vault": plan_prune(_scan(vault), now=now),
        "vault/basebackups": plan_prune(_scan(vault / "basebackups"), now=now),
    }
    chain_plan = plan_prune_transcript_chains(_scan_transcript_chains(vault))
    wal_plan = plan_prune_wal(
        _scan_wal(vault / "wal_archive"),
        oldest_kept_backup_when=_oldest_kept_backup_when(plans["vault/basebackups"]))
    legacy_plan = plan_prune_legacy_tarballs(_scan_legacy_tarballs(vault))
    return plans, chain_plan, wal_plan, legacy_plan


DSN = "postgresql://osiris:osiris@127.0.0.1:5601/osiris"
_MANIFEST_FROM_AGENT = "system:prune-ladder"
MANIFEST_MIN_AGE = timedelta(hours=20)


async def _collect_session_prune_plan(*, dead_after_days: int = 30) -> list[SessionRow]:
    """The transcript-cache-prune population, gathered fresh: reuses
    osiris_transcript_cache_prune's own `_collect_sessions`/`find_prunable_sessions`
    unchanged. This is a real DB query, which is exactly why this stays its own async
    step rather than folding into `_compute_plans`'s pure file scans."""
    from scripts.osiris_transcript_cache_prune import _collect_sessions, find_prunable_sessions

    sessions = await _collect_sessions()
    return find_prunable_sessions(
        sessions, now=datetime.now(UTC), dead_after=timedelta(days=dead_after_days))


async def mail_manifest(
    plans: dict[str, dict[str, list[DumpFile]]],
    chain_plan: dict[str, list[TranscriptChain]],
    wal_plan: dict[str, list[WalSegment]] | None = None,
    legacy_plan: dict[str, list[DumpFile]] | None = None,
    session_plan: list[SessionRow] | None = None,
    seat_transcript_groups: dict[str, list[SessionRow]] | None = None,
) -> int:
    """Send the dry-run plan to the operator's desk as a decision-band brief. Uses
    src.db.pool.create_pool, not bare asyncpg.create_pool: the jsonb codec it registers
    is what makes send_message's own graph-edge write actually land. Returns the sent
    message id, the same id --apply-if-clear looks up the next day."""
    from src.db.pool import create_pool
    from src.orchestrator.mailbox import send_message

    body = build_manifest_body(
        plans, chain_plan, wal_plan, legacy_plan, session_plan, seat_transcript_groups)
    pool = await create_pool(
        DSN, min_size=1, max_size=1,
        application_name="osiris-script:prune-ladder-manifest")
    try:
        result = await send_message(
            pool, from_agent=_MANIFEST_FROM_AGENT, from_project="osiris",
            to_project="operator", desk_kind="decision", body=body)
        return int(result["id"])
    finally:
        await pool.close()


async def find_clear_manifest(
    *, min_age: timedelta = MANIFEST_MIN_AGE,
) -> tuple[int | None, str]:
    """The apply-side gate: apply the next day unless the brief was dimmed. Looks up
    the newest manifest this script itself sent to the operator's desk and returns
    (message_id, reason). message_id is None whenever applying would be wrong: no
    manifest was ever sent, the newest one is younger than `min_age` (today is not
    genuinely "the day after" yet), or it was dimmed (`fleet_messages.moot_at` set,
    `dim_brief`'s own mechanism in mailbox.py: an agent annotating a brief moot, never
    the operator's own settle, which stays a human act; a dim here means something
    judged the plan no longer current). A found, non-dimmed, old-enough manifest
    returns its id and a `reason` describing why applying is clear."""
    import asyncpg

    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=1)
    try:
        row = await pool.fetchrow(
            "SELECT id, created_at, moot_at FROM fleet_messages "
            "WHERE from_agent=$1 AND to_project='operator' "
            "ORDER BY created_at DESC LIMIT 1", _MANIFEST_FROM_AGENT)
    finally:
        await pool.close()
    if row is None:
        return None, "no manifest has ever been sent: run --manifest first"
    if row["moot_at"] is not None:
        return None, (f"manifest {row['id']} was dimmed at "
                      f"{row['moot_at'].isoformat()}, not applying")
    age = datetime.now(UTC) - row["created_at"]
    if age < min_age:
        return None, (f"manifest {row['id']} is only {age} old (need "
                      f"{min_age}), not yet old enough to apply")
    return int(row["id"]), f"manifest {row['id']}, sent {age} ago, not dimmed, clear"


def _apply(
    plans: dict[str, dict[str, list[DumpFile]]],
    chain_plan: dict[str, list[TranscriptChain]],
    wal_plan: dict[str, list[WalSegment]] | None = None,
    legacy_plan: dict[str, list[DumpFile]] | None = None,
    session_plan: list[SessionRow] | None = None,
) -> None:
    for plan in plans.values():
        for f in plan["remove"]:
            Path(f.path).unlink(missing_ok=True)
    for c in chain_plan["remove"]:
        for path in c.files:
            Path(path).unlink(missing_ok=True)
    if wal_plan is not None:
        for s in wal_plan["remove"]:
            Path(s.path).unlink(missing_ok=True)
    if legacy_plan is not None:
        for f in legacy_plan["remove"]:
            Path(f.path).unlink(missing_ok=True)
    if session_plan is not None:
        for sess in session_plan:
            Path(sess.source_path).unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    import asyncio

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backups", type=Path, default=Path("backups"))
    parser.add_argument("--vault", type=Path,
                        default=Path.home() / "osiris-vault")
    parser.add_argument("--apply", action="store_true",
                        help="ACTUALLY DELETE the files the dry-run lists; never run "
                             "this without explicit approval on the printed list "
                             "first.")
    parser.add_argument("--manifest", action="store_true",
                        help="Mail the dry-run plan to the operator as a "
                             "decision brief instead of "
                             "printing it. Deletes nothing.")
    parser.add_argument("--apply-if-clear", action="store_true",
                        help="Apply ONLY if the newest --manifest brief this script "
                             "sent is at least 20h old and has not been dimmed; "
                             "refuses with a named reason otherwise. Deletes nothing "
                             "when refusing.")
    parser.add_argument("--seat-root", type=Path, default=None,
                        help="Root directory holding every seat's own scaffolded "
                             "identity directory, for the dormant-seat-transcript "
                             "rollup (default: src.orchestrator.offices' own real "
                             "~/.osiris/seats). A test override, never set in production.")
    parser.add_argument("--projects-root", type=Path, default=None,
                        help="Harness transcript root for the dormant-seat-transcript "
                             "rollup (default: ~/.claude/projects). A test override, "
                             "never set in production.")
    args = parser.parse_args(argv)

    def _office_root() -> Path:
        if args.seat_root is not None:
            return Path(args.seat_root)
        from src.orchestrator.offices import _default_office_root
        return _default_office_root()

    def _projects_root() -> Path:
        return args.projects_root or (Path.home() / ".claude" / "projects")

    if args.manifest:
        plans, chain_plan, wal_plan, legacy_plan = _compute_plans(args.backups, args.vault)
        session_plan = asyncio.run(_collect_session_prune_plan())
        seat_groups = attribute_sessions_to_seats(
            session_plan, office_root=_office_root(), projects_root=_projects_root())
        mid = asyncio.run(mail_manifest(
            plans, chain_plan, wal_plan, legacy_plan, session_plan, seat_groups))
        print(f"manifest mailed to the operator: message {mid}")
        return 0

    if args.apply_if_clear:
        existing_mid, reason = asyncio.run(find_clear_manifest())
        if existing_mid is None:
            print(f"REFUSING: {reason}", file=sys.stderr)
            return 1
        plans, chain_plan, wal_plan, legacy_plan = _compute_plans(args.backups, args.vault)
        session_plan = asyncio.run(_collect_session_prune_plan())
        total_remove = sum(len(p["remove"]) for p in plans.values())
        total_chains_remove = len(chain_plan["remove"])
        total_wal_remove = len(wal_plan["remove"])
        total_legacy_remove = len(legacy_plan["remove"])
        total_session_remove = len(session_plan)
        print(f"{reason}, applying {total_remove} dump file(s), "
              f"{total_chains_remove} transcript chain(s), {total_wal_remove} WAL "
              f"segment(s), {total_legacy_remove} legacy transcript tarball(s), and "
              f"{total_session_remove} transcript cache file(s) now.")
        _apply(plans, chain_plan, wal_plan, legacy_plan, session_plan)
        return 0

    plans, chain_plan, wal_plan, legacy_plan = _compute_plans(args.backups, args.vault)
    for label, plan in plans.items():
        _report(label, plan)
    # transcript chains live only in the vault (osiris_backup.sh never writes them to
    # backups/): a distinct population, reported and pruned as whole chains
    _report_chains("vault", chain_plan)
    _report_wal(wal_plan)
    _report("vault/legacy-transcripts", legacy_plan)

    total_remove = sum(len(p["remove"]) for p in plans.values())
    total_chains_remove = len(chain_plan["remove"])
    total_wal_remove = len(wal_plan["remove"])
    total_legacy_remove = len(legacy_plan["remove"])
    if not args.apply:
        print(f"\nDRY RUN ONLY: {total_remove} dump file(s), {total_chains_remove} "
              f"transcript chain(s), {total_wal_remove} WAL segment(s), and "
              f"{total_legacy_remove} legacy transcript tarball(s) would be removed, "
              "none deleted. Re-run with --apply once the operator has ruled on this "
              "list.")
        return 0
    print(f"\n--apply given: deleting {total_remove} dump file(s), "
          f"{total_chains_remove} transcript chain(s), {total_wal_remove} WAL "
          f"segment(s), and {total_legacy_remove} legacy transcript tarball(s) now.")
    _apply(plans, chain_plan, wal_plan, legacy_plan)
    return 0


if __name__ == "__main__":
    sys.exit(main())
