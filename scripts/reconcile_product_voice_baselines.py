#!/usr/bin/env python3
"""Git merge driver for the product-voice/taxonomy-drift baseline JSON files
(tests/product_voice_baseline.json, tests/product_voice_tier2_baseline.json,
tests/taxonomy_drift_baseline.json).

THE COLLISION THIS FIXES: every one of these three ratchets is regenerated fresh, in
full, by each lowering tip right before it lands (both because the lint is growth-only
and because a tip that lowers a bucket is required to regenerate in the same commit).
With several product-voice tips landing back to back, every tip's own regenerated
baseline conflicts with whatever the tip immediately ahead of it already changed, even
though the two tips never touched the same source lines: one lowered `tests`, another
lowered `src/orchestrator`, and git has no way to know a JSON object's two keys are
independent. Each of those conflicts has cost a full rebase-regenerate-reverify round
trip.

WHY PER-KEY MINIMUM IS ALWAYS SAFE: both ratchets are growth-only (a count going up
fails the gate; a count going down only prints a regen notice, never a hard failure -- see
tests/test_product_voice.py and tests/test_taxonomy_drift.py's own docstrings). Taking
the smaller of two candidate baseline numbers for a key can only make the gate STRICTER,
never more permissive: a merged tree whose live count for that key is above the smaller
number still fails exactly as it should, and a merged tree whose live count is between
the two candidate numbers is caught as growth here where a naive "take ours" or "take
theirs" pick might have missed it. The rare case where BOTH sides independently lowered
the SAME key (so neither candidate is the merged tree's own true fresh count) is not a
correctness problem, only a staleness one: the next tip that regenerates the baseline
(every tip does, right before it lands, per house convention) supersedes whatever this
driver wrote. This driver's job is narrower than producing the exact right number; it is
to never block an otherwise-clean merge and to never let a real regrowth hide behind a
stale-but-larger surviving value.

MECHANISM: unlike scripts/reconcile_tool_contract_ceiling.py, this driver never calls
`git merge-file` first -- there is no surrounding prose to preserve here, only a flat
{bucket: int} object, so `%A` and `%B` are read directly as the two sides' own
pre-conflict JSON, untouched by any textual merge attempt. Keys present on both sides
resolve to the smaller value; a key present on only one side keeps that side's own
value as-is (never invented, never dropped) since there is no informed way to compare
against a value the other side never touched. Output is written back to `%A`
(`args.ours`, the file git expects the driver to leave resolved in) as sorted, 2-space
indented JSON, matching this repo's own `json.dumps(..., indent=2)` convention so a
human diff of the resolved commit reads the same as every other regen.

FAILS LOUD, NEVER GUESSES: a side that fails to parse as a JSON object (malformed JSON,
or valid JSON that isn't a flat `{str: int}` mapping) declines the merge entirely,
leaving git's own unassisted conflict markers in place -- a real conflict for a human,
never a silently wrong resolution.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _load_flat_int_map(path: Path) -> dict[str, int] | None:
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None
    if not isinstance(data, dict):
        return None
    if not all(isinstance(k, str) and isinstance(v, int) and not isinstance(v, bool)
               for k, v in data.items()):
        return None
    return data


def _merge(ours: dict[str, int], theirs: dict[str, int]) -> dict[str, int]:
    merged: dict[str, int] = {}
    for key in sorted(set(ours) | set(theirs)):
        if key in ours and key in theirs:
            merged[key] = min(ours[key], theirs[key])
        elif key in ours:
            merged[key] = ours[key]
        else:
            merged[key] = theirs[key]
    return merged


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ancestor")  # %O, unused: the per-key-minimum rule needs no base
    ap.add_argument("ours")
    ap.add_argument("theirs")
    ap.add_argument("path")  # %P, git's own convention; used only for logging
    args = ap.parse_args(argv)

    ours = _load_flat_int_map(Path(args.ours))
    theirs = _load_flat_int_map(Path(args.theirs))
    if ours is None or theirs is None:
        print(f"reconcile_product_voice_baselines: DECLINED -- {args.path} is not a flat "
              "{str: int} JSON object on one or both sides of this merge; leaving git's "
              "own conflict markers in place for a human to resolve by hand.",
              file=sys.stderr)
        return 1

    merged = _merge(ours, theirs)
    Path(args.ours).write_text(json.dumps(merged, indent=2) + "\n")
    print(f"reconcile_product_voice_baselines: {args.path} merged, {len(merged)} keys "
          "(per-key minimum of the two sides) -- regenerate on top of this to get the "
          "exact fresh count.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
