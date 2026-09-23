"""The adoption meter: an instrument, not a fix. The risk this answers: a gate that exists in
code and refuses nothing in production is the same artifact as a confession nobody acts on.
The acceptance test used to be a `triage(mode='census')` call somebody had to remember to run
and compare by hand against a baseline held in a decision's own prose, exactly the shape that
decays (one earlier diagnosis of this identical failure found the population it named had grown
by roughly 1,800 in the 24 days the record sat unbuilt).

THE HEADLINE METRIC WAS REPLACED, superseding an earlier target of "Decision median_links
moving off 1": measurement showed that a Thread structurally cannot declare a forward
relational link at its own birth. `open_thread` has no grounds=/relates_to= parameter, so a
Thread's eventual connectivity is entirely a function of whether a later Decision's `resolves=`
cites it back. A Reference is the same shape (its only route to connectivity is being named in
a later Decision's `grounds=`). Even a well-connected Decision gets most of its own links from
later objects citing it (supersedes/rediscovers/confirms/refutes), not from what its own writer
declared. A population-wide snapshot median cannot tell "born yesterday, correctly not yet
cited" apart from "born long ago, never cited by anyone"; those are opposite conditions the old
metric reported identically, and at roughly 20 new Decisions/day the always-young,
legitimately-uncited population dominates the snapshot forever. median_links was never going to
move off 1 regardless of whether declaration-at-creation was actually working.

COHORT-AGED CONNECTIVITY replaces it: objects are bucketed by birth week
(`date_trunc('week', created_at)`), and each cohort's own live link count is measured at three
fixed historical ages: at birth, +7 days, +30 days (`links.created_at <= objects.
created_at + N days`, using the same live-link definition `triage`'s own `_TRIAGE_LINK_CTE`
uses). The question stops being "are writes born connected" (they structurally cannot be) and
becomes "do writes become connected": does a cohort's own median link count climb between birth
and day 30, or does it sit flat.

NO BASELINE ROW IS NEEDED HERE, unlike the metric this replaces (deliberately checked before
building): `links.created_at` has existed since migration 0001, so every cohort old enough to
have reached a checkpoint age is a fixed historical fact the instant that window has fully
elapsed; re-querying it tomorrow, next week, or a year from now returns the identical number,
because the query only ever counts links that existed within a bounded historical interval,
never "as of right now". This is structurally different from the old metric, a live snapshot of
an ever-growing present, which is exactly what made a fixed comparison point necessary and
hazardous to re-derive. Agreement by construction, not by a persisted snapshot: the same
principle an earlier preflight fix already applied to `wake_gate_preflight`.

THE HATCH HALF IS UNCHANGED (the `unlinked_because` property read below): the meter itself was
correct and the criterion it reported against was wrong; this file's hatch-reading half was
never implicated and needed no rebuild.

SCOPE, MATCHING THE OBLIGATION'S OWN EXCLUSIONS: File (single-link Files were shown to be
benign: in_repo only, zero (repo,relpath) collisions) and Type (does not participate in `links`
like an ordinary object, per `triage`'s own contract) are never counted here.

NEVER A GATE: this module makes zero writes anywhere, not to `objects`/`links`/assertions, and
not even to `watermarks` (the retired metric's one write)."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import asyncpg

SCOPED_TYPES = ("Decision", "Thread", "Reference")
"""The obligation's own scope: every other object type is either explicitly carved out
(File, Type; see this module's own docstring) or was never part of the original diagnosis,
which named Decision/Thread/Reference specifically."""

HEADLINE_TYPE = "Decision"
"""Kept as the deploy line's headlined type for continuity with the original ruling, which
singled out Decision specifically. Thread's own cohort curve is arguably the sharper signal
going forward (its connectivity is entirely inbound-accrued, so its birth->30d delta isolates
citation discipline with none of a Decision's own self-declared-link noise), but swapping the
headlined type is a second, unrequested change this build does not make unilaterally.
Thread/Reference cohorts are computed and returned alongside Decision's in `cohorts`
regardless; a future call can re-point the headline without touching this module's own
query."""


async def _cohort_connectivity(pool: asyncpg.Pool) -> dict[str, dict[str, Any]]:
    """Per SCOPED_TYPES: the latest birth-week cohort old enough to have reached its own
    30-day checkpoint, plus that same cohort's 7-day figure (always available once 30 days
    have passed) and the prior eligible cohort's own 30-day figure (so a reader watching this
    number move deploy over deploy sees trend without two cohorts crammed into one line, see
    `render_adoption_line`). `status='active'` only, matching the retired metric's own scope.
    Returns `{}` for a type with no 30-day-eligible cohort yet (an honest absence, not a
    zero)."""
    rows = await pool.fetch("""
        WITH scoped AS (
            SELECT id, type, created_at, date_trunc('week', created_at) AS cohort_week
            FROM objects WHERE type = ANY($1) AND status='active'
        ),
        per_object AS (
            SELECT s.id, s.type, s.cohort_week, s.created_at,
                count(*) FILTER (WHERE l.created_at <= s.created_at)
                    AS links_at_birth,
                count(*) FILTER (WHERE l.created_at <= s.created_at + interval '7 days')
                    AS links_at_7d,
                count(*) FILTER (WHERE l.created_at <= s.created_at + interval '30 days')
                    AS links_at_30d
            FROM scoped s
            LEFT JOIN links l
                ON (l.from_id = s.id OR l.to_id = s.id)
                AND (l.valid_until IS NULL OR l.valid_until > now())
            GROUP BY s.id, s.type, s.cohort_week, s.created_at
        )
        SELECT type, cohort_week, count(*) AS n,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY links_at_birth) AS median_at_birth,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY links_at_7d) AS median_at_7d,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY links_at_30d) AS median_at_30d
        FROM per_object
        GROUP BY type, cohort_week
        -- Eligibility filtered here, not in Python: a cohort only counts once it has
        -- genuinely reached 30 days of age. A fresher cohort's numbers would keep changing
        -- on every re-query (more links can still land within its own window), which is
        -- exactly the "live snapshot of an ever-growing present" hazard this metric was
        -- built to avoid.
        --
        -- Measured from the cohort's youngest member, not its week start (corrected after
        -- an earlier bug: this read `cohort_week + interval '30 days'`, which is the
        -- week's opening instant, so an object born on the Sunday of that week was
        -- declared 30-day-eligible at 24 days old, and its links_at_30d was still moving.
        -- That is precisely the hazard the comment above says this gate exists to prevent:
        -- the gate as written admitted the very cohorts whose numbers had not finished
        -- changing. max(created_at) states the invariant exactly, no member is younger
        -- than its own full window, rather than the week-start approximation, and it needs
        -- no separate "+7 for the week to complete" fudge.
        HAVING max(created_at) + interval '30 days' <= now()
        ORDER BY type, cohort_week DESC
    """, list(SCOPED_TYPES))

    by_type: dict[str, list[Any]] = {t: [] for t in SCOPED_TYPES}
    for r in rows:
        by_type[r["type"]].append(r)

    out: dict[str, dict[str, Any]] = {}
    for t, cohort_rows in by_type.items():
        if not cohort_rows:  # no 30-day-eligible cohort yet for this type
            continue
        latest = cohort_rows[0]  # NEWEST eligible cohort (cohort_week DESC)
        entry: dict[str, Any] = {
            "week": latest["cohort_week"].date().isoformat(),
            "n": latest["n"],
            "median_at_birth": float(latest["median_at_birth"] or 0),
            "median_at_7d": float(latest["median_at_7d"] or 0),
            "median_at_30d": float(latest["median_at_30d"] or 0),
        }
        if len(cohort_rows) > 1:
            prev = cohort_rows[1]
            entry["prev_week"] = prev["cohort_week"].date().isoformat()
            entry["prev_median_at_30d"] = float(prev["median_at_30d"] or 0)
            entry["trend_30d_delta"] = (
                entry["median_at_30d"] - entry["prev_median_at_30d"])
        out[t] = entry
    return out


async def _orphan_birth_rate(pool: asyncpg.Pool) -> dict[str, dict[str, Any]]:
    """Prevention, not healing: does the fraction of each birth-week cohort that never
    acquired a single live link within 7 days of its own birth fall over time. This is the
    question a fallback shipped elsewhere in this codebase (for lineage-root resolution) was
    meant to move, and the one instrument this house had nothing to answer it with before.

    A fixed historical fact, same discipline as `_cohort_connectivity` above and for the
    identical reason: the link check is bounded to `created_at + 7 days`, so a later backfill
    can never revise an already-eligible week's number. Not a hypothetical risk: re-deriving
    the original protocol's literal approach (an unbounded "still orphan right now" check, no
    window) live in one session found Thread's own weekly rate for a several-week span moved
    by up to 12 points, because an unrelated backfill happened to land mid-session and
    retroactively linked roughly 127 previously-orphaned Threads spanning those very weeks.
    An unbounded check is exactly the "live snapshot of an ever-growing present" trap this
    file's own cohort metric was already built to avoid (see this module's top docstring);
    the original protocol predates this file and inherited the trap. The 7-day bound closes
    it: generous against the underlying finding that most objects get every link they will
    ever declare within 60 seconds of birth (92.8%/92.3% of Decisions/Threads), so it costs
    this metric nothing a stricter window would also catch, while making the number
    un-revisable by a later healing pass, a property `_cohort_connectivity` already has and
    an unbounded read does not.

    SCOPE/ELIGIBILITY: same SCOPED_TYPES, `status='active'`, and
    `cohort_week + 7 days <= now()` eligibility gate as `_cohort_connectivity`; a week is not
    reported until every member of it has actually had its own full 7-day window. Newest
    eligible week headlines; the prior eligible week rides along for trend, same shape as
    `_cohort_connectivity`'s own return."""
    rows = await pool.fetch("""
        WITH scoped AS (
            SELECT id, type, created_at, date_trunc('week', created_at) AS cohort_week
            FROM objects WHERE type = ANY($1) AND status='active'
        ),
        flagged AS (
            SELECT s.id, s.type, s.cohort_week, s.created_at,
                NOT EXISTS (
                    SELECT 1 FROM links l
                    WHERE (l.from_id = s.id OR l.to_id = s.id)
                    AND l.created_at <= s.created_at + interval '7 days'
                ) AS born_orphan
            FROM scoped s
        )
        SELECT type, cohort_week, count(*) AS n,
            count(*) FILTER (WHERE born_orphan) AS n_orphan
        FROM flagged
        GROUP BY type, cohort_week
        -- Measured from the cohort's youngest member, not its week start (corrected after
        -- an earlier bug: this read `cohort_week + interval '7 days'`, the week's opening
        -- instant, while this function's own docstring promises "a week is not reported
        -- until every member of it has actually had its own full 7-day window." Those
        -- differ by up to six days: an object born on the Sunday of a week became eligible
        -- at one day old, its born_orphan flag still able to flip. The docstring was right
        -- and the SQL was wrong. max(created_at) states the documented invariant exactly.
        HAVING max(created_at) + interval '7 days' <= now()
        ORDER BY type, cohort_week DESC
    """, list(SCOPED_TYPES))

    by_type: dict[str, list[Any]] = {t: [] for t in SCOPED_TYPES}
    for r in rows:
        by_type[r["type"]].append(r)

    out: dict[str, dict[str, Any]] = {}
    for t, cohort_rows in by_type.items():
        if not cohort_rows:  # no 7-day-eligible cohort yet for this type
            continue
        latest = cohort_rows[0]
        rate = (latest["n_orphan"] / latest["n"]) if latest["n"] else 0.0
        entry: dict[str, Any] = {
            "week": latest["cohort_week"].date().isoformat(),
            "n": latest["n"],
            "n_orphan": latest["n_orphan"],
            "rate": rate,
        }
        if len(cohort_rows) > 1:
            prev = cohort_rows[1]
            prev_rate = (prev["n_orphan"] / prev["n"]) if prev["n"] else 0.0
            entry["prev_week"] = prev["cohort_week"].date().isoformat()
            entry["prev_rate"] = prev_rate
            entry["trend_delta"] = rate - prev_rate
        out[t] = entry
    return out


_LEGACY_EXTENSION_REASON_STRINGS = frozenset({
    # Every wording `_EXTENSION_LINK_PENDING_REASON` (src/mcp_server.py) is known to have
    # held before `unlinked_because_kind` existed as a structural discriminator. Found via
    # `git log --all --oneline -G "rediscovers=" -- src/mcp_server.py` (three separate
    # commits each edited this line as narrows=/cites= joined the param list) and confirmed
    # against the live graph. Closed: a row written from this point on always carries
    # unlinked_because_kind structurally, so this set never needs a new entry for a future
    # wording change.
    "extension-link-pending (task #189 condition 2, decision 7ea187b9) — machine-set: "
    "this write's only requested connectivity is obsoletes=/confirms=/refutes=/"
    "implements=/rediscovers=/bears_on=, which mint after this transaction and cannot "
    "satisfy the gate at its own commit point",
    "extension-link-pending (task #189 condition 2, decision 7ea187b9) — machine-set: "
    "this write's only requested connectivity is obsoletes=/confirms=/refutes=/"
    "implements=/rediscovers=/bears_on=/narrows=, which mint after this transaction and "
    "cannot satisfy the gate at its own commit point",
})


_HATCH_CAVEAT = (
    "a 0 here is NOT proof the gate is broken: the gate only fires on "
    "types that declare required_link_kinds, and none do yet, that content pass "
    "is separate and has not landed. ALL-TIME CUMULATIVE, NEVER A WINDOW OR A RATE: "
    "unlinked_because is asserted once per object at write "
    "time and never retracted, so total/split only ever grow, and two readings taken weeks "
    "apart are not a before/after comparison of the SAME thing, they are two cumulative "
    "totals at different elapsed times. THE EXACT-STRING CLASSIFICATION BUG IS FIXED: "
    "the split now reads a separate, non-prose "
    "`unlinked_because_kind` property asserted at write time, never a re-parse of "
    "`unlinked_because`'s own text, which drifted three times as this reason constant's "
    "enumerated param list grew (b7fee6c/57c9a0b/6fb6ba5) and silently miscounted 15 "
    "historical rows as standalone. Rows written before this fix carry no "
    "`unlinked_because_kind` at all and fall back to `_LEGACY_EXTENSION_REASON_STRINGS`, "
    "a closed, git-verified enumeration of every wording the constant is known to have "
    "held, so historical rows read correctly at query time too, no backfill needed"
)


async def _hatch_counts(pool: asyncpg.Pool) -> dict[str, Any]:
    """The `unlinked_because` hatch: an ordinary property assertion, not a column.
    `current_assertions` has existed since migration 0001, so this read is always
    structurally live; an empty result is a real zero, never a missing instrument, which is
    why this reports raw counts unconditionally rather than an `available` flag gating on
    schema. The split (an extension-link-only write must never be summed into the same
    bucket as a genuinely standalone one) needs the `_EXTENSION_LINK_PENDING_REASON`
    constant, imported live from `src.mcp_server` at census time rather than copied, because
    it is still liable to move. The `try` is not dead now that the gate has merged: it is
    the guard for a build where that constant has been renamed or removed out from under
    this reader, and `split=None` then degrades to raw per-value counts rather than silently
    reporting a zero that would read as "the gate refuses nothing".

    NEITHER COUNT IS A RATE OR A WINDOW (see `_HATCH_CAVEAT` for the full finding):
    `current_assertions` here is every `unlinked_because` ever written, all-time,
    monotonically growing, and the extension/standalone split is additionally sensitive to
    `_EXTENSION_LINK_PENDING_REASON`'s own current exact text; an older-worded write
    silently reclassifies as standalone the moment that constant's wording moves, with no
    change to the underlying object. This function's own numbers are correct census,
    unchanged by this finding; only the label was wrong. Fixing the exact-match itself is
    done: the split now reads `unlinked_because_kind`, a separate non-prose property
    `capture._enforce_required_links` asserts alongside `unlinked_because` in the same
    transaction (record_decision's MCP wrapper passes the exact boolean it already computes,
    never a later re-derivation from text). A row written before this fix carries no
    `unlinked_because_kind` at all; `_LEGACY_EXTENSION_REASON_STRINGS` is the closed, frozen
    enumeration of every wording `_EXTENSION_LINK_PENDING_REASON` is known to have ever held
    (found by reading git history, not guessed), so those rows still classify correctly at
    query time, no backfill or repair step needed: historical rows read correctly by
    re-evaluating at query time. This list is closed going forward too: every future write
    gets `unlinked_because_kind` structurally, so the constant's own prose is never
    load-bearing for classification again and this frozenset needs no further entries."""
    rows = await pool.fetch(
        "SELECT ub.object_id, (ub.value #>> '{}') AS reason, "
        "(k.value #>> '{}') AS kind "
        "FROM current_assertions ub "
        "LEFT JOIN current_assertions k "
        "  ON k.object_id = ub.object_id AND k.name = 'unlinked_because_kind' "
        "WHERE ub.name = 'unlinked_because'")
    by_reason_raw: dict[str, int] = {}
    for r in rows:
        by_reason_raw[r["reason"]] = by_reason_raw.get(r["reason"], 0) + 1
    total = len(rows)

    try:
        from src.mcp_server import _EXTENSION_LINK_PENDING_REASON
    except (ImportError, AttributeError):
        split = None
    else:
        pending = sum(
            1 for r in rows
            if (r["kind"] == "extension_link_pending")
            or (r["kind"] is None
                and (r["reason"] == _EXTENSION_LINK_PENDING_REASON
                     or r["reason"] in _LEGACY_EXTENSION_REASON_STRINGS)))
        split = {"extension_link_pending": pending, "standalone_other": total - pending}

    return {
        "total": total, "by_reason_raw": by_reason_raw, "split": split,
        "note": _HATCH_CAVEAT,
    }


async def adoption_meter(pool: asyncpg.Pool) -> dict[str, Any]:
    """The whole instrument: cohort-aged connectivity per SCOPED_TYPES, orphan birth rate
    per SCOPED_TYPES (prevention, not healing), plus the hatch split. Zero writes anywhere,
    read-only in full, including against `watermarks` (the retired metric's one write; see
    this module's own docstring for why cohort connectivity needs no persisted baseline)."""
    cohorts = await _cohort_connectivity(pool)
    orphan_birth_rate = await _orphan_birth_rate(pool)
    hatch = await _hatch_counts(pool)
    return {
        "measured_at": datetime.now(UTC).isoformat(),
        "cohorts": cohorts,
        "orphan_birth_rate": orphan_birth_rate,
        "hatch": hatch,
    }


def render_adoption_line(meter: dict[str, Any]) -> str:
    """One terse line, the same discipline `cmd_deploy`'s own other checks already use
    (`chaos replay: ...`, `full suite: green ...`), printed on every deploy, not only when
    something moved. A conditional "only print on change" line was considered and rejected:
    it recreates exactly the failure this instrument exists to prevent (something that
    quietly stops being seen), and a deploy is not so frequent in this house that one more
    honest, terse line is real noise; the existing lines already accept that trade. Only the
    headlined type's cohort renders here (`HEADLINE_TYPE`); the full per-type detail lives
    in the returned dict for a caller who wants it."""
    headline = meter["cohorts"].get(HEADLINE_TYPE)
    ob_headline = meter.get("orphan_birth_rate", {}).get(HEADLINE_TYPE)
    hatch = meter["hatch"]
    # All-time cumulative, never a window: a number printed on every deploy with no window
    # will be misread as a per-deploy or per-period figure. See `_HATCH_CAVEAT` for the full
    # finding, including the exact-string classification fragility this label does not
    # attempt to fix.
    if hatch["split"] is not None:
        hatch_str = (f"extension={hatch['split']['extension_link_pending']} "
                     f"standalone={hatch['split']['standalone_other']} (all-time total, "
                     f"not a window, do not diff against a prior deploy's line)")
    else:
        hatch_str = f"{hatch['total']} total (unsplit, reason constant not on this build)"
    if headline is None:
        cohort_str = "no 30-day-aged cohort yet"
    else:
        cohort_str = (
            f"cohort {headline['week']} (n={headline['n']}): "
            f"birth={headline['median_at_birth']:.1f} -> "
            f"30d={headline['median_at_30d']:.1f}"
            f" (Δ{headline['median_at_30d'] - headline['median_at_birth']:+.1f})"
        )
    if ob_headline is None:
        ob_str = "no 7d-aged cohort yet"
    else:
        ob_str = f"{ob_headline['week']} {ob_headline['rate'] * 100:.1f}%"
        if "trend_delta" in ob_headline:
            ob_str += f" (Δ{ob_headline['trend_delta'] * 100:+.1f})"
    return (
        f"adoption189: {HEADLINE_TYPE} {cohort_str} | "
        f"orphan_birth: {HEADLINE_TYPE} {ob_str} | unlinked_because: {hatch_str}"
    )
