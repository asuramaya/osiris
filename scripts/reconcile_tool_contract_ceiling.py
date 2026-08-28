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

FIRST REAL COLLISION, TWO LATENT BUGS FOUND BY IT (2026-08-28, thread 197164ae — this
driver had never actually fired against a real merge before): (1) `_const_value` used to
re-search `[\\d_]+` over the WHOLE matched line rather than read a captured group, so on a
name containing an underscore (every name this has ever been pointed at) it grabbed the
bare `_` inside the NAME itself and `int("")` raised straight past this file's own
`main()` — git folded that into an ordinary content conflict, indistinguishable from two
humans editing the same line, and the traceback would have scrolled past unread had Thoth
not been reading closely. (2) the line regex required nothing but whitespace after the
digits, so a value trailing a "# MEASURED..." changelog comment (this ratchet's own
actual convention on every recent raise) silently read back as "missing", not present —
the exact one-sided version of the same "value is really there, this driver just can't
see it" failure. Both fixed at the root (a captured group; an optional trailing `#.*`).
`main()`'s own parsing calls are ALSO now wrapped in a `try`/`except` that DECLINES with
a named reason on any OTHER future parse failure this driver has not yet met, rather than
raising past the merge machinery again — the belt, not just the one buckle that broke.
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
    # A TRAILING `# comment` IS THE COMMON CASE, NOT AN EDGE CASE (thread 197164ae's own
    # real specimen): every raise this ratchet has actually landed with this session
    # carries a "# MEASURED against the merged tree: N tools." trailing note — the exact
    # shape the merged commit 1893116 needed a HUMAN to write by hand because this regex,
    # before this fix, only matched a bare `NAME = NUMBER` line with nothing after it.
    # `ours`'s own real value (196_635  # MEASURED...) silently read back as "missing"
    # against the OLD regex — not the crash Thoth hit (that was `theirs`, whose line
    # happened to be bare), but the identical class of gap: a value that IS present,
    # parsed as absent, purely because of what trails it on the line.
    return re.compile(
        rf"^[ \t]*{re.escape(name)}[ \t]*=[ \t]*(?P<value>[\d_]+)[ \t]*(#.*)?$", re.MULTILINE)


def _const_value(text: str, name: str) -> int | None:
    """THE BUG THIS REPLACES, first live specimen 2026-08-28 (thread 197164ae) rather than
    a hypothesis: the old version re-searched `[\\d_]+` over the WHOLE matched line
    (`m.group(0)`, e.g. "TOOL_CONTRACT_CEILING_CHARS = 197_348") instead of reading a
    captured group — and `re.search` finds the FIRST such run left-to-right, which for
    any constant name containing an underscore (every name this driver has ever been
    pointed at) is the bare `_` inside the NAME itself ("TOOL_CONTRACT..."), not the
    number. `"_".replace("_", "")` is `""`, and `int("")` raised past every caller
    straight out of the merge driver. This was not intermittent — it fired on every
    single invocation this constant's own name shape has ever produced; nothing caught
    it earlier only because this driver had never actually collided in a real merge
    before. Fixed by reading `(?P<value>...)` directly off `_const_re`'s own match — the
    number was always right there in the regex that already located it; there was never
    a reason to re-derive it with a second, unanchored search."""
    m = _const_re(name).search(text)
    if m is None:
        return None
    return int(m.group("value").replace("_", ""))


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


_DEFAULT_CONSTANTS = ("TOOL_CONTRACT_CEILING_CHARS", "TOOL_CONTRACT_EXPECTED_COUNT")
# TWO NUMBERS, ONE FILE, ONE DRIVER (thread 5999 — Thoth's own live proof of the char fix,
# ten minutes after it landed, ALSO surfaced this: a wave of four merges left the char
# ceiling correctly reconciled at every step, but `assert len(per_tool) == N` sat nine
# lines below it, touched by only ONE branch at a time — no conflict at all, so git took
# that branch's side with nothing for this driver to even see, and main read 143 while the
# tree actually carried 144. A ratchet with two numbers and one merge driver only
# protected one of them. Fixed by (1) hoisting the bare inline assert into a NAMED
# constant, `TOOL_CONTRACT_EXPECTED_COUNT`, the exact same shape as the char ceiling, and
# (2) reconciling BOTH constants in one driver invocation — git calls this script once per
# conflicting file, never once per constant, so multiple names must be walked in the same
# pass. THE UNCONFLICTED CASE IS NOT A GAP HERE: `_reconcile`'s own fallback already
# force-corrects an unconflicted single line to the arithmetically resolved value (never
# trusts a textual number, even one no conflict marker ever touched) — this is exactly
# the property the count number was missing, and it falls out of the existing mechanism
# for free once the count has a name this driver can find.


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ancestor")
    ap.add_argument("ours")
    ap.add_argument("theirs")
    ap.add_argument("path")  # %P — git's own convention; unused directly, kept for logging
    ap.add_argument("--constant-name", action="append", default=None,
                    help="repeatable; defaults to both TOOL_CONTRACT_CEILING_CHARS and "
                         "TOOL_CONTRACT_EXPECTED_COUNT")
    args = ap.parse_args(argv)
    names = args.constant_name or list(_DEFAULT_CONSTANTS)

    # capture %A's PRE-merge content first — `git merge-file` below overwrites it in
    # place, so this is the only chance to read ours' own original constant value(s).
    ours_original = Path(args.ours).read_text()

    # Its own exit code is not consulted below — a leftover conflict is detected by
    # scanning for markers after every constant name has had its own turn, which is the
    # only way to know once this loop may have handled more than one name's own hunk.
    subprocess.run(
        ["git", "merge-file", "-L", "ours", "-L", "base", "-L", "theirs",
         args.ours, args.ancestor, args.theirs],
        check=False,
    )

    any_fixed = False
    any_unresolved = False
    for name in names:
        # DECLINES, NEVER RAISES (thread 197164ae — the first live specimen, not a
        # hypothesis: `_const_value`'s own digit-extraction bug once raised an unhandled
        # ValueError here, which git's merge machinery folded into an ordinary content
        # conflict indistinguishable from two humans editing the same line — the
        # traceback scrolled past and nothing else recorded that the automation had
        # abdicated. That ONE bug is fixed at its root in `_const_value` above; this
        # `try` is the belt for any OTHER parsing failure this driver has not yet met —
        # same family as `alarm_withheld_deploy_record`'s own confession: a correct
        # refusal must leave a named reason, never vanish into a generic failure path a
        # human has to excavate from a traceback to even notice happened.
        try:
            base_v = _const_value(Path(args.ancestor).read_text(), name)
            ours_v = _const_value(ours_original, name)
            theirs_v = _const_value(Path(args.theirs).read_text(), name)
        except (ValueError, OSError) as exc:
            print(f"reconcile_tool_contract_ceiling: DECLINED — could not parse {name} on "
                  f"one side of the merge ({exc!r}); not this collision's shape, or a bug "
                  f"in this driver — leaving it as git's own merge left it.", file=sys.stderr)
            any_unresolved = True
            continue

        if base_v is None or ours_v is None or theirs_v is None:
            print(f"reconcile_tool_contract_ceiling: {name} missing from one of "
                  f"ancestor/ours/theirs — not this collision's shape for this constant, "
                  f"leaving it as git's own merge left it.", file=sys.stderr)
            continue  # absent from this file entirely is not this driver's problem

        resolved = ours_v + theirs_v - base_v
        # re-read %A fresh each iteration: a prior name's own rewrite already changed it.
        text = Path(args.ours).read_text()
        new_text, fixed = _reconcile(text, name, resolved)
        Path(args.ours).write_text(new_text)
        if fixed:
            any_fixed = True
            print(f"reconcile_tool_contract_ceiling: {name} = {resolved} "
                  f"({ours_v} + {theirs_v} - {base_v}, never the larger of the two)")
        else:
            any_unresolved = True
            print(f"reconcile_tool_contract_ceiling: {name} did not match the shape this "
                  f"driver expects in {args.path} — leaving a real conflict, resolve by "
                  f"hand (never take the larger of two values; run "
                  f"scripts/measure_tool_contract.py on the merged tree instead).",
                  file=sys.stderr)

    still_conflicted = "<<<<<<<" in Path(args.ours).read_text()
    if any_fixed and not any_unresolved and not still_conflicted:
        print(f"reconcile_tool_contract_ceiling: {args.path} merged clean.")
        return 0
    if any_fixed:
        print(f"reconcile_tool_contract_ceiling: some constant(s) corrected, but "
              f"{args.path} still needs hand resolution — the number(s) already right "
              f"are one less thing to get wrong.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
