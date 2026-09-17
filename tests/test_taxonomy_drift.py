"""THE TAXONOMY DRIFT RATCHET (WAVE 28, decision 70c001ec / thread a47a0c7f; Thoth mail
12011): the six retired hierarchy words (house, office, district, cluster, neighbo(u)rhood,
room, hub, landmark, region, swarm) are being swept out of every reader-facing surface --
UI strings, CLI/MCP help text and docstrings, and docs/slash-docs -- one owner-piece at a
time (Khnum: house/office; Sekhmet: room/hub/landmark/cluster-in-docstrings/neighborhood-
in-docstrings; Seshat: UI strings; Imhotep: docs). A single end-of-wave audit would let any
one piece silently regress while another lands. This is a RATCHET instead, live from wave
28's first tip, not its last: a per-file baseline COUNT of today's retired-word mentions
across the reader-facing surfaces (same shape and same law as test_unbounded_wait.py's own
`_SUBPROCESS_BASELINE` -- drift in EITHER direction fails the gate, so a file that quietly
gains OR loses a mention must touch `tests/taxonomy_drift_baseline.json` on purpose).
As each owner's rename lands, their files' counts go down and the baseline shrinks with
them; when every count reaches zero the baseline (and this ratchet's reason to scan
source, as opposed to a flat "must be empty" assertion) can be deleted outright.

SCOPE, deliberately narrower than the full census (976 rows): surfaces 1-6 only --
UI strings/labels, MCP tool docstrings, CLI subcommand help, slash docs, docs/*.md, and
server-rendered receipt/exception text. Surface 7 (schema/ontology data names -- the
house->project migration, tracked by its own 147 data-name tests) and surface 8 (tests
that already assert on retired words, tracked by the WAVE 28 prose-assertion-test pass)
are NOT this ratchet's job -- decision 70c001ec's own drift-test description is "any
retired word in a user-facing string, help text, tool/command/parameter name or doc line",
which is exactly this file's five surface groups, not a data name and not a test's own
assertion string.

THE SCANNER IS A COARSE BUT SUFFICIENT PROXY (same disclosed-tradeoff language as
test_unbounded_wait.py's own docstring), not a perfect classifier:
  - .py surfaces: every `ast.Constant` string (covers docstrings, argparse help=/
    description=/epilog=, print()/receipt literals, and f-string literal segments via
    JoinedStr) -- this naturally excludes comments and bare identifiers (a function or
    parameter literally named `resync_house` is code, not a string, and never counted),
    which is exactly the comment/identifier exclusion the census itself used to separate
    surfaces 1-6 from the "second pass" (307 comments+identifiers, decision 70c001ec).
  - .js/.html surfaces: `/* */` block comments (CSS and JS both use them) are stripped
    from the whole file first, then `//` line comments are stripped per remaining line;
    no attempt to tell a JS string literal from a bare identifier on the same line, so
    this is looser than the .py scan -- acceptable here since src/ui/static's own
    retired-word footprint is small and mostly comments already (Surface 1 census note:
    "one user-visible string carries a retired word"). Found missing its first day live
    (Seshat's 12088, Thoth mail 12189 item 1): index.html:32's CSS `/* ... cluster ... */`
    comment was counted as a live hit because only `//` was stripped.
  - docs/*.md and commands/*.md: every line counts -- there is no comment/identifier
    concept in prose, and the census's own Surface 4/5 rows confirm every hit there was
    already classified "doc prose".

Per-LINE tracking (matching the census's own file:line rows one-to-one) was tried first
and abandoned: `ast.Constant.lineno` is the string literal's OPENING line, not each
wrapped physical line inside it, and adjacent string-literal concatenation (a common style
in this file's own argparse help= text) collapses several physical source lines into one
Constant with no embedded newlines to recover the offsets from -- a per-line diff came out
~340 false "new" hits and ~190 false "stale" entries purely from that line-numbering noise,
not from any real content drift. A per-file COUNT sidesteps all of it, at the cost of not
naming exactly which line moved -- an owner shrinking their own file's count already knows
which line they just fixed.
"""
from __future__ import annotations

import ast
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BASELINE_PATH = ROOT / "tests" / "taxonomy_drift_baseline.json"
# THE FLEET RULE (Thoth mail 12026): a tip that changes these counts regenerates the
# baseline in the SAME commit -- the gate refuses a baseline that grew. This is the one
# command that does it, named here so both failure messages below can print it verbatim.
REGEN_CMD = "uv run python tests/test_taxonomy_drift.py --write"

_WORDS = ["house", "houses", "housed", "housing", "office", "offices", "district",
          "districts", "cluster", "clusters", "clustering", "clustered",
          "neighbourhood", "neighbourhoods", "neighborhood", "neighborhoods",
          "room", "rooms", "hub", "hubs", "landmark", "landmarks", "region",
          "regions", "swarm", "swarms"]
# same regex shape as the census this baseline was seeded from (taxonomy-census.md,
# 2026-09-17): \b-bounded, case-insensitive, British and American spellings.
_RETIRED_RE = re.compile(r"\b(" + "|".join(_WORDS) + r")\b", re.IGNORECASE)

_PY_SURFACES = [
    "src/mcp_server.py", "src/cli.py", "src/orchestrator/textrender.py",
    "src/orchestrator/graph_physics.py", "src/orchestrator/graph_stream.py",
    "src/orchestrator/graph_layout.py", "src/orchestrator/graph_migrations.py",
]


def _scan_py(relpath: str) -> int:
    text = (ROOT / relpath).read_text()
    tree = ast.parse(text, filename=relpath)
    count = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            count += len(_RETIRED_RE.findall(node.value))
    return count


_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)


def _scan_js_html(path: Path) -> int:
    text = _BLOCK_COMMENT_RE.sub("", path.read_text())
    count = 0
    for raw in text.splitlines():
        cidx = raw.find("//")
        line = raw if cidx == -1 else raw[:cidx]
        count += len(_RETIRED_RE.findall(line))
    return count


def _scan_doc(path: Path) -> int:
    count = len(_RETIRED_RE.findall(path.read_text()))
    return count


def _live_counts() -> dict[str, int]:
    counts: dict[str, int] = {}
    for rel in _PY_SURFACES:
        n = _scan_py(rel)
        if n:
            counts[rel] = n
    for path in sorted((ROOT / "src" / "ui" / "static").glob("*.js")) + \
            sorted((ROOT / "src" / "ui" / "static").glob("*.html")):
        n = _scan_js_html(path)
        if n:
            counts[str(path.relative_to(ROOT))] = n
    for path in sorted((ROOT / "docs").glob("*.md")) + sorted((ROOT / "commands").glob("*.md")):
        n = _scan_doc(path)
        if n:
            counts[str(path.relative_to(ROOT))] = n
    return counts


def test_scan_js_html_strips_block_comments_not_just_line_comments(tmp_path: Path) -> None:
    """THE SPECIMEN (Seshat 12088, index.html:32): a CSS `/* ... */` block comment
    mentioning a retired word must not count -- only `//` was stripped before this fix,
    so a block comment (CSS's only comment form, and legal in JS too) sailed through."""
    fixture = tmp_path / "fixture.html"
    fixture.write_text(
        "<style>\n"
        "/* this whole cluster of rules styles the district fill, a room, a hub */\n"
        ".foo { color: red; } // trailing line comment mentions office too\n"
        "</style>\n"
        '<div class="house">real hit, not a comment</div>\n'
    )
    assert _scan_js_html(fixture) == 1


def test_taxonomy_drift_baseline_file_is_valid_json() -> None:
    baseline = json.loads(BASELINE_PATH.read_text())
    assert isinstance(baseline, dict)
    assert baseline, "an empty baseline means the sweep is done -- delete this ratchet " \
        "and this test file instead of leaving a baseline with nothing to check"
    assert all(isinstance(v, int) and v > 0 for v in baseline.values()), (
        "every baseline entry is a positive count -- a file with zero retired-word "
        "mentions has no reason to appear in the baseline at all")


def test_taxonomy_drift_matches_the_committed_baseline_exactly() -> None:
    """THE RATCHET ITSELF: today's live per-file retired-word count must equal the
    committed baseline exactly, in EITHER direction. A NEW mention (a file gaining a
    count, or a fresh file appearing with retired words) means someone reintroduced
    retired vocabulary into a reader-facing surface. A file's count going DOWN or a file
    disappearing entirely means an owner's rename landed and the baseline is now stale in
    the good direction -- still a failure, on purpose, so nobody's rename silently ships
    without this ratchet being told about it (`tests/taxonomy_drift_baseline.json`
    regenerates from `_live_counts()`; lower the matching entries, or delete them once a
    file reaches zero)."""
    baseline = json.loads(BASELINE_PATH.read_text())
    live = _live_counts()

    new_or_grown = {f: (baseline.get(f, 0), n) for f, n in live.items()
                     if n != baseline.get(f, 0) and n > baseline.get(f, 0)}
    shrunk_or_gone = {f: (baseline.get(f, 0), live.get(f, 0)) for f in baseline
                       if live.get(f, 0) != baseline[f] and live.get(f, 0) < baseline[f]}

    assert not new_or_grown, (
        "retired-word mentions INCREASED in these files versus the committed baseline "
        f"(was -> now): {new_or_grown} -- a retired word (house/office/district/cluster/"
        "neighbo(u)rhood/room/hub/landmark/region/swarm) was newly introduced into a "
        "reader-facing surface; use the current taxonomy instead, or if this really is "
        "content (not hierarchy -- e.g. the analyst ontology's own IntrusionSet/cluster: "
        "id-prefix use, ruled to stay by decision 70c001ec), raise the baseline here with "
        f"a comment saying why. Regenerate with: {REGEN_CMD}")
    assert not shrunk_or_gone, (
        "retired-word mentions DECREASED in these files versus the committed baseline "
        f"(was -> now): {shrunk_or_gone} -- good news, but the baseline in "
        "tests/taxonomy_drift_baseline.json is now stale and must be lowered (or "
        "the file's entry removed once it reaches zero) in the SAME tip as the rename "
        f"that did this. Regenerate with: {REGEN_CMD}")


if __name__ == "__main__":
    import sys

    if "--write" not in sys.argv:
        print(f"usage: {REGEN_CMD}", file=sys.stderr)
        raise SystemExit(1)
    before = json.loads(BASELINE_PATH.read_text()) if BASELINE_PATH.exists() else {}
    after = _live_counts()
    BASELINE_PATH.write_text(json.dumps(dict(sorted(after.items())), indent=2) + "\n")
    changed = {f: (before.get(f, 0), n) for f, n in after.items() if before.get(f, 0) != n}
    changed |= {f: (before[f], after.get(f, 0)) for f in before
                if f not in after and before[f] != 0}
    print(f"wrote {BASELINE_PATH.relative_to(ROOT)} -- {len(after)} files, "
          f"{sum(after.values())} total mentions")
    if changed:
        print("changed (was -> now):")
        for f, (b, n) in sorted(changed.items()):
            print(f"  {f}: {b} -> {n}")
    else:
        print("no change")
