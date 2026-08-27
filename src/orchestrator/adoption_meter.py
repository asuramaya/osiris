"""THE #189 ADOPTION METER — an instrument, not a fix (Thoth msg 5825, ruling d68c57e5,
obligation 8d510875). The risk this answers is stated in the ruling itself: "a gate that
exists in code and refuses nothing in production is the same artifact as a confession
nobody acts on." Today the acceptance test was a `triage(mode='census')` call somebody had
to REMEMBER to run and compare by hand against a baseline held in a decision's own prose —
exactly the shape that decays (5169686b diagnosed this identically on 2026-08-02 and the
population it named grew ~1,800 in the 24 days the record sat unbuilt).

BUILDS NOTHING NEW WHERE A SURFACE ALREADY CARRIES IT: the two headline numbers
(`median_links` moving off 1, true zero-link orphan counts for Decision/Thread/Reference)
are read straight off `triage(mode='census')` (compositions.py's own `_fn_triage`) — that
census already computes exactly this per type+status; this module filters it to the three
in-scope types, never re-derives the SQL. The baseline is a ROW, not a sentence: seeded
once from obligation 8d510875's own already-published numbers (2026-08-27T15:03Z,
decision:a621a95676c1) into `watermarks` via `monitor.get_cursor`/`set_cursor` — the SAME
generic cursor store `record_deploy`'s own devhead watermark and the chaos-replay gate's
own last-report snapshot already use (`set_cursor(pool, "chaos-replay:last", ...)`,
src/cli.py) — never a bespoke table.

THE THIRD NUMBER, THE ADOPTION METRIC ITSELF (Imhotep's hatch, `unlinked_because`, msg
5828): NOT a dedicated column — an ordinary property assertion, the same mechanism as
every other graded fact in this house, read off `current_assertions WHERE name=
'unlinked_because'`. This is why `_hatch_counts` never checks `information_schema` for a
column that will never exist: the read is structurally always "available" (the table has
existed since migration 0001), so an empty result is a REAL zero, never a missing
instrument — Imhotep's own caveat (msg 5828) is that a real zero can ALSO mean the gate's
own `required_link_kinds` isn't declared on any type yet (Khnum's content pass, separate
and not yet landed), so this module reports the raw split PLUS that caveat rather than
inventing a false "not live" flag it cannot actually verify. THE SPLIT (Thoth's
requirement #2) compares each assertion's value against
`src.mcp_server._EXTENSION_LINK_PENDING_REASON`, imported live at census time rather than
copied — Imhotep named this exact string as still liable to move, so hardcoding it here
would silently go stale the moment he touches it. When the symbol isn't importable yet
(pre-merge, exactly today's state), the split is reported as unavailable and the raw
per-value counts are returned instead — never a guess at which value means what.

SCOPE, MATCHING THE OBLIGATION'S OWN EXCLUSIONS: File (#120 proved single-link Files
benign — in_repo only, zero (repo,relpath) collisions) and Type (does not participate in
`links` like an ordinary object, per `triage`'s own contract) are never counted here.

NEVER A GATE: this module makes zero writes to `objects`/`links`/assertions and refuses
nothing — the one write it ever makes is the baseline watermark row, seeded at most once."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import asyncpg

ADOPTION_BASELINE_KEY = "adoption189:baseline"

SCOPED_TYPES = ("Decision", "Thread", "Reference")
"""The obligation's own scope (8d510875): every other object type is either explicitly
carved out (File, Type — see this module's own docstring) or was never part of #189's
original diagnosis (5169686b/d68c57e5 named Decision/Thread/Reference specifically)."""

_FIXED_BASELINE: dict[str, Any] = {
    "taken_at": "2026-08-27T15:03:00+00:00",
    "source": "decision:a621a95676c1 (ruling d68c57e5) / thread:cc495f1987e0 (obligation 8d510875)",
    "types": {
        "Decision": {"n": 5478, "orphans": 274, "thin": 4463, "median_links": 1.0},
        "Thread": {"n": 3465, "orphans": 232, "thin": 3019, "median_links": 1.0},
        "Reference": {"n": 300, "orphans": 112, "thin": 102, "median_links": None},
    },
}
"""THE NUMBER TO BEAT, transcribed ONCE from the obligation that named it (never re-derived
from a fresh census — a re-derivation now would silently absorb whatever drifted between
2026-08-27T15:03Z and whenever this first runs, defeating the whole point of a fixed
comparison point). `Reference.median_links` is `None` because the obligation's own prose
never quoted it (only orphans/thin) — reported honestly as missing rather than invented."""


async def _current_census(pool: asyncpg.Pool) -> dict[str, dict[str, Any]]:
    """The two headline numbers, filtered from the SAME census `_fn_triage(mode='census')`
    already computes — never a second query reimplementing its SQL. `status='active'` only,
    matching the baseline's own scope."""
    from src.orchestrator.compositions import _fn_triage

    rows = await _fn_triage(pool, None, {"mode": "census"})
    return {
        r["type"]: {
            "n": r["n"], "orphans": r["orphans"], "thin": r["thin"],
            "median_links": r["median_links"],
        }
        for r in rows
        if r["type"] in SCOPED_TYPES and r["status"] == "active"
    }


_HATCH_CAVEAT = (
    "a 0 here is NOT proof the gate is broken (Imhotep msg 5828): the gate only fires on "
    "types that declare required_link_kinds, and none do yet — Khnum's content pass for "
    "that is separate and has not landed"
)


async def _hatch_counts(pool: asyncpg.Pool) -> dict[str, Any]:
    """Imhotep's `unlinked_because` hatch (msg 5828): an ordinary property assertion, not a
    column — `current_assertions` has existed since migration 0001, so this read is always
    structurally live; an empty result is a REAL zero, never a missing instrument, which is
    why this reports raw counts unconditionally rather than an `available` flag gating on
    schema. THE SPLIT (Thoth's requirement #2 — an extension-link-only write must never be
    summed into the same bucket as a genuinely standalone one) needs Imhotep's own
    `_EXTENSION_LINK_PENDING_REASON` constant, imported live from `src.mcp_server` at
    census time (never copied — he named it as still liable to move before he commits).
    The `try` is NOT dead now that the gate has merged (main 45b42cd): it is the guard
    for a build where that constant has been renamed or removed out from under this
    reader, and `split=None` then degrades to raw per-value counts rather than
    silently reporting a zero that would read as "the gate refuses nothing"."""
    rows = await pool.fetch(
        "SELECT (a.value #>> '{}') AS reason, count(*) AS n FROM current_assertions a "
        "WHERE a.name = 'unlinked_because' GROUP BY reason ORDER BY n DESC")
    by_reason_raw = {r["reason"]: r["n"] for r in rows}
    total = sum(by_reason_raw.values())

    try:
        from src.mcp_server import _EXTENSION_LINK_PENDING_REASON
    except (ImportError, AttributeError):
        split = None
    else:
        pending = by_reason_raw.get(_EXTENSION_LINK_PENDING_REASON, 0)
        split = {"extension_link_pending": pending, "standalone_other": total - pending}

    return {
        "total": total, "by_reason_raw": by_reason_raw, "split": split,
        "note": _HATCH_CAVEAT,
    }


async def adoption_meter(
    pool: asyncpg.Pool, *, seed_baseline_if_missing: bool = True,
) -> dict[str, Any]:
    """THE WHOLE INSTRUMENT: current numbers, the durable baseline row, the delta between
    them, and Imhotep's hatch split. Never writes to the graph itself — the one write is
    the baseline watermark, and only on its first-ever call (or when explicitly reseeded by
    a caller who passes `seed_baseline_if_missing=True` after intentionally clearing it)."""
    from src.orchestrator.monitor import get_cursor, set_cursor

    current = await _current_census(pool)

    raw_baseline = await get_cursor(pool, ADOPTION_BASELINE_KEY)
    if raw_baseline is None and seed_baseline_if_missing:
        raw_baseline = json.dumps(_FIXED_BASELINE)
        await set_cursor(pool, ADOPTION_BASELINE_KEY, raw_baseline)
    baseline = json.loads(raw_baseline) if raw_baseline else None

    delta: dict[str, dict[str, Any]] = {}
    if baseline:
        for t in SCOPED_TYPES:
            b = baseline["types"].get(t)
            c = current.get(t)
            if not b or not c:
                continue
            delta[t] = {
                "orphans_delta": c["orphans"] - b["orphans"],
                "median_links_delta": (
                    None if b.get("median_links") is None
                    else c["median_links"] - b["median_links"]
                ),
            }

    hatch = await _hatch_counts(pool)

    return {
        "measured_at": datetime.now(UTC).isoformat(),
        "current": current,
        "baseline": baseline,
        "delta": delta,
        "hatch": hatch,
    }


def render_adoption_line(meter: dict[str, Any]) -> str:
    """One terse line, the same discipline `cmd_deploy`'s own other checks already use
    (`chaos replay: ...`, `full suite: green ...`) — printed on EVERY deploy, not only when
    something moved. A conditional "only print on change" line was considered and rejected:
    it recreates exactly the failure this instrument exists to prevent (something that
    quietly stops being seen), and a deploy is not so frequent in this house that one more
    honest, terse line is real noise — the existing lines already accept that trade."""
    d = meter["current"].get("Decision", {})
    t = meter["current"].get("Thread", {})
    r = meter["current"].get("Reference", {})
    bd = (meter["baseline"] or {}).get("types", {}).get("Decision", {})
    hatch = meter["hatch"]
    if hatch["split"] is not None:
        hatch_str = (f"extension={hatch['split']['extension_link_pending']} "
                     f"standalone={hatch['split']['standalone_other']}")
    else:
        hatch_str = f"{hatch['total']} total (unsplit — reason constant not on this build)"
    return (
        f"adoption189: Decision median_links={d.get('median_links')} "
        f"(baseline {bd.get('median_links')}) | orphans D/T/R="
        f"{d.get('orphans')}/{t.get('orphans')}/{r.get('orphans')} | "
        f"unlinked_because: {hatch_str}"
    )
