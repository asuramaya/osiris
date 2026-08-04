#!/usr/bin/env python3
"""Git merge driver for TOOL_CONTRACT_CEILING_CHARS (dispatch 26686b77, Thoth msg 3658):
THE COLLISION THIS FIXES, measured not hypothetical — Imhotep and Sekhmet both raised the
ratchet constant from the same base (135_077) on parallel branches, to 135_292 (+215) and
135_189 (+112). Each branch's own dry-run against composer reports clean (of course — the
other branch doesn't exist yet from either one's perspective), so nothing warns either
author; the real conflict exists only once the second branch lands, and "take the larger
of the two" (the intuitive fix) is provably wrong whenever both changes are purely
additive: the true combined total is base + both deltas, always strictly greater than
either branch's own number alone.

WHY ARITHMETIC (base + ours-delta + theirs-delta), NOT A LIVE RE-MEASUREMENT AT MERGE
TIME — an earlier version of this driver shelled out to `_measure_tool_contract()` (or an
equivalent live measurement) against the working tree at the moment git invoked it for
this file. THAT WAS WRONG, found empirically while proving this against a real merge, not
assumed: git does NOT guarantee every OTHER (non-conflicting) file has already been merged
into the working tree — or even the index — by the time it invokes a custom driver for
THIS one conflicting path. Measuring at that moment silently read a stale, still-pre-merge
version of an unrelated file, undercounting by exactly the other branch's own delta — the
identical failure shape this whole driver exists to prevent, just moved one level down.
`ours` and `theirs` are each already a correct, honestly self-measured total for a tree
that DOESN'T yet contain the other branch's work (the same number each author committed).
Since git guarantees %O/%A/%B (the three text versions of THIS ONE file) are always fully
available — no working-tree timing dependency at all — `ours + theirs - base` is instant,
deterministic, and, for the documented and universal historical pattern of this ratchet
(every past raise is a disjoint addition to a DIFFERENT tool's docstring, never two
branches overlapping the same content), mathematically EXACT: matches the real
135_077+215+112=135_404 Thoth measured by hand (commit 46da176) precisely.

THE HONEST LIMIT: this formula is exact only when the two branches' changes are disjoint.
If they ever genuinely overlap (both edit the SAME tool's docstring, in non-conflicting
ways git still auto-merges), the arithmetic could be wrong — but never silently: the
EXISTING ratchet test (test_tool_contract_stays_under_the_ceiling) re-measures the real,
now-fully-merged tree the next time pytest runs (a gate before every commit, per
CLAUDE.md), and will fail loudly if this driver's number and reality ever disagree. This
driver's job is narrower than "prove correctness unconditionally" — it is "never worse
than a human guessing, and provably exact for the failure mode that has actually
happened" — scripts/measure_tool_contract.py remains the tool for a human (or CI) to get
the true live number directly, any time, including as a check after a merge like this one.

WHY A MERGE DRIVER, NOT A COMPUTED FLOOR: the ratchet file's own docstring already rules
out deriving the ceiling from the live tree at TEST time — "a ratchet that derives its own
ceiling from the tree is not a ratchet, it is a thermometer" (same law as
test_render_hygiene.py's _ALLOWLIST, Thoth msg 1921). Growth must stay a deliberate,
hand-authored, justified act. This driver doesn't touch that: it fires ONLY at the merge
collision point, and even then only replaces the one line neither author could have gotten
right alone (each measured correctly for a tree that didn't yet contain the other's work) —
never at ordinary test time, never for a single-lane raise.

MECHANISM: run git's own `git merge-file` first, unmodified — this is exactly what would
have happened with no custom driver at all, so the fallback path (see FAILS LOUD below) is
never worse than the status quo. Its output already carries standard conflict markers
around anything it couldn't reconcile — normally the changelog paragraph both branches
append PLUS the constant reassignment, all one hunk since they're textually adjacent. Any
conflict block that contains this constant on BOTH sides gets rewritten: each side's own
prose is kept (both authors' narratives survive, concatenated ours-then-theirs), the raw
constant lines are dropped from each side, and one arithmetically resolved line replaces
them. Everything else in the file — including any OTHER conflict this driver doesn't
recognize — is left exactly as git's own merge produced it.

FAILS LOUD, NEVER GUESSES: if the constant is missing from any of %O/%A/%B, or doesn't
appear on both sides of exactly the shape this driver expects, this leaves git's own
unassisted merge result in place and exits nonzero — a normal conflict, not a silent wrong
number. If unrelated hunks elsewhere in the file still conflict after the constant is
fixed, the constant is still corrected (one less thing for the human resolving the rest by
hand to get wrong) but the exit stays nonzero — real conflicts still get a real human.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

_BLOCK_RE = re.compile(
    r"<<<<<<<[^\n]*\n(?P<ours>.*?)\n=======\n(?P<theirs>.*?)\n>>>>>>>[^\n]*\n",
    re.DOTALL,
)


def _const_re(name: str) -> re.Pattern[str]:
    return re.compile(rf"^[ \t]*{re.escape(name)}[ \t]*=[ \t]*[\d_]+[ \t]*$", re.MULTILINE)


def _const_value(text: str, name: str) -> int | None:
    m = _const_re(name).search(text)
    if m is None:
        return None
    digits = re.search(r"[\d_]+", m.group(0))
    assert digits is not None  # _const_re's own match guarantees this group exists
    return int(digits.group(0).replace("_", ""))


def _reconcile(text: str, name: str, resolved: int) -> tuple[str, bool]:
    """Returns (new_text, constant_fixed). `constant_fixed` is True iff the constant's
    own conflict (or, if unconflicted, its single existing line) was rewritten to
    `resolved` — independent of whether some OTHER, unrelated hunk in the file still
    conflicts (the caller checks that separately: a fixed constant plus a leftover
    unrelated conflict is a different, better-labeled outcome than this driver simply not
    recognizing the constant's own shape at all)."""
    const_re = _const_re(name)
    resolved_line = f"{name} = {resolved}"
    fixed = False

    def _rewrite_block(m: re.Match[str]) -> str:
        nonlocal fixed
        ours, theirs = m.group("ours"), m.group("theirs")
        if not (const_re.search(ours) and const_re.search(theirs)):
            return m.group(0)  # not this collision's shape — leave untouched
        ours_prose = const_re.sub("", ours).strip("\n")
        theirs_prose = const_re.sub("", theirs).strip("\n")
        parts = [p for p in (ours_prose, theirs_prose) if p]
        fixed = True
        return "\n".join([*parts, resolved_line]) + "\n"

    new_text = _BLOCK_RE.sub(_rewrite_block, text)

    if not fixed:
        # no conflicting block contained the constant — either it's already a single
        # clean (possibly stale) line, or it's missing entirely. Force-correct the
        # former unconditionally (never trust a textual number, even an unconflicted
        # one); never invent the latter.
        new_text, n = const_re.subn(resolved_line, new_text)
        fixed = n == 1

    return new_text, fixed


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ancestor")
    ap.add_argument("ours")
    ap.add_argument("theirs")
    ap.add_argument("path")  # %P — git's own convention; unused directly, kept for logging
    ap.add_argument("--constant-name", default="TOOL_CONTRACT_CEILING_CHARS")
    args = ap.parse_args(argv)

    # capture %A's PRE-merge content first — `git merge-file` below overwrites it in
    # place, so this is the only chance to read ours' own original constant value.
    ours_original = Path(args.ours).read_text()

    baseline = subprocess.run(
        ["git", "merge-file", "-L", "ours", "-L", "base", "-L", "theirs",
         args.ours, args.ancestor, args.theirs],
        check=False,
    )
    text = Path(args.ours).read_text()

    base_v = _const_value(Path(args.ancestor).read_text(), args.constant_name)
    ours_v = _const_value(ours_original, args.constant_name)
    theirs_v = _const_value(Path(args.theirs).read_text(), args.constant_name)

    if base_v is None or ours_v is None or theirs_v is None:
        print(f"reconcile_tool_contract_ceiling: {args.constant_name} missing from one of "
              f"ancestor/ours/theirs — not this collision's shape, leaving git's own merge "
              f"of {args.path} as-is", file=sys.stderr)
        return baseline.returncode or 1

    resolved = ours_v + theirs_v - base_v
    new_text, fixed = _reconcile(text, args.constant_name, resolved)
    Path(args.ours).write_text(new_text)
    still_conflicted = "<<<<<<<" in new_text

    if fixed and not still_conflicted:
        print(f"reconcile_tool_contract_ceiling: {args.path} merged clean, "
              f"{args.constant_name} = {resolved} ({ours_v} + {theirs_v} - {base_v}, "
              f"never the larger of the two)")
        return 0
    if fixed:
        print(f"reconcile_tool_contract_ceiling: {args.constant_name} corrected to "
              f"{resolved}, but other hunks in {args.path} still conflict — resolve those "
              f"by hand, the number is already right.", file=sys.stderr)
    else:
        print(f"reconcile_tool_contract_ceiling: {args.constant_name} did not match the "
              f"shape this driver expects in {args.path} — leaving a real conflict, "
              f"resolve by hand (never take the larger of two values; run "
              f"scripts/measure_tool_contract.py on the merged tree instead).",
              file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
