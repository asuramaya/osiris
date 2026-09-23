"""THE PRODUCT-VOICE RATCHET (ruling 6c510acbd80b): osiris is a product for a stranger,
not a private log of who built what. The live Settings pane review found internal
working-agent language leaking straight into the interface ("over Khnum's soul-key
door") and the same voice runs through CLI help, MCP docstrings and the docs -- fleet
worker names, mail/DM ids, ruling/decision/thread ids, wave and gate numbers, and
quotations attributed to the human operator have no meaning to someone who has never
seen this project's own internal coordination. This is a RATCHET, same law and same
shape as tests/test_taxonomy_drift.py's own baseline: a per-file baseline COUNT of
today's violations across the reader-facing surfaces, so existing debt stays visible
without blocking the gate, but any NEW occurrence fails it. Provenance citations belong
in code comments and commit messages -- the repo's own history -- which stay OUT of
this ratchet's scope by the ruling's own words.

GROWTH-ONLY (decision 3a858fd3, fixing gate w398's failure): the ratchet fails on
growth only, never on shrinkage. An exact-match baseline cannot survive a merge with any
concurrent cleanup tip, since a baseline generated on one branch is stale the instant
another branch's tip touches the same file or bucket first. A count going down prints a
notice recommending a regen, never a hard failure -- so concurrent product-voice tips
stay independently mergeable, each regenerating on top of whatever landed before it.

SIX PATTERN CLASSES, matched independently and summed per file:
  - fleet worker names -- every handle a Seat object has ever carried while actively
    held (tests/product_voice_names.json, regenerated from the live graph rather than
    hand-typed here -- see `_write_names` below)
  - "mail NNNN" / "DM NNNN" / "msg NNNN"
  - an 8-character lowercase hex id (a ruling/decision/thread short id)
  - "wave NN" / "wNNN" / "gate wNNN"
  - an operator quotation ("operator's word(s)", "operator said")
  - the em dash character (U+2014) -- replaced with a sentence, a comma, or a colon

SURFACES, the same five reader-facing groups ruling 6c510acbd80b names: console UI
strings (src/ui/static/*.js, *.html), CLI help/receipt/error strings (src/cli.py),
@mcp.tool docstrings and receipts (src/mcp_server.py), REST route descriptions
(src/api/app.py), and docs/*.md + README/INSTALL. Reuses test_taxonomy_drift.py's own
scanning shapes verbatim (AST string-constant walk for .py, block-then-line-comment
stripping for .js/.html, whole-line scan for .md) -- the same coarse-but-sufficient
tradeoff, not a perfect classifier, disclosed there and not repeated here.

TIER 2 (scope widened to the whole codebase, decision a4aa0ba4, amending 1e2ef5c3's own
code-comment exemption): comments, docstrings, and test names are product code too, not
just the reader-facing surfaces above. Tier 2 runs the same six-pattern count as a plain
whole-file text scan (matching the census that sized this problem) over ten directory
buckets: src/ui/static, src/cli.py, src/mcp_server.py, src/api, src/orchestrator,
src/ingest, src/config, scripts, tests, docs -- baselined PER DIRECTORY, not per file,
because the cleanup lands module by module and a per-directory number is what a single
module's cleanup commit can honestly claim to lower. Tier 2 only ratchets against
regrowth, same growth-only shape as tier 1. A separate coinage count (in-session
phrases like "the ... door", "hot path", "the ladder") is measured and printed on regen
but asserts nothing -- reported, not yet
enforced, per the scope-widening ruling's own words.
"""
from __future__ import annotations

import ast
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BASELINE_PATH = ROOT / "tests" / "product_voice_baseline.json"
TIER2_BASELINE_PATH = ROOT / "tests" / "product_voice_tier2_baseline.json"
NAMES_PATH = ROOT / "tests" / "product_voice_names.json"
REGEN_CMD = "uv run python tests/test_product_voice.py --write"
REGEN_NAMES_CMD = "uv run python tests/test_product_voice.py --write-names"

_ID_RE = re.compile(r"\b[0-9a-f]{8}\b")
_MAIL_RE = re.compile(r"\b(?:mail|dm|msg)\s+#?\d{3,}\b", re.IGNORECASE)
_WAVE_RE = re.compile(r"\bwave\s+\d+\b|\bgate\s+w\d+\b|\bw\d{2,4}\b", re.IGNORECASE)
_OPERATOR_QUOTE_RE = re.compile(
    r"\boperator[’']s (?:own )?words?\b|\boperator said\b", re.IGNORECASE)
_EM_DASH_RE = re.compile("—")
_COINAGE_RE = re.compile(
    r"\bthe [\w-]+ door\b|\bfirst breath\b|\bthe ladder\b|\bthe law\b|\bthe box\b|"
    r"\bhot path\b|\bstranger\b|\btwin\b|\bwhisper\b|\bceremony\b|\bestate\b",
    re.IGNORECASE)

_TIER2_BUCKETS: list[tuple[str, list[str] | None]] = [
    ("src/ui/static", ["*.js", "*.html"]),
    ("src/cli.py", None),
    ("src/mcp_server.py", None),
    ("src/api", ["*.py"]),
    ("src/orchestrator", ["*.py"]),
    ("src/ingest", ["*.py"]),
    ("src/config", ["*.py"]),
    ("scripts", ["*.py", "*.sh"]),
    ("tests", ["*.py"]),
    ("docs", ["*.md"]),
]


def _tier2_bucket_files(rel: str, patterns: list[str] | None) -> list[Path]:
    base = ROOT / rel
    if patterns is None:
        return [base]
    files: list[Path] = []
    for pattern in patterns:
        files.extend(sorted(base.rglob(pattern)))
    return files


def _names_re() -> re.Pattern[str]:
    names = json.loads(NAMES_PATH.read_text())
    escaped = [re.escape(n) for n in names]
    return re.compile(r"\b(" + "|".join(escaped) + r")\b", re.IGNORECASE)


def _count_violations(text: str, names_re: re.Pattern[str]) -> int:
    return (
        len(names_re.findall(text))
        + len(_MAIL_RE.findall(text))
        + len(_ID_RE.findall(text))
        + len(_WAVE_RE.findall(text))
        + len(_OPERATOR_QUOTE_RE.findall(text))
        + len(_EM_DASH_RE.findall(text))
    )


_PY_SURFACES = ["src/cli.py", "src/mcp_server.py", "src/api/app.py"]


def _scan_py(relpath: str, names_re: re.Pattern[str]) -> int:
    text = (ROOT / relpath).read_text()
    tree = ast.parse(text, filename=relpath)
    count = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            count += _count_violations(node.value, names_re)
    return count


_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)


def _scan_js_html(path: Path, names_re: re.Pattern[str]) -> int:
    text = _BLOCK_COMMENT_RE.sub("", path.read_text())
    count = 0
    for raw in text.splitlines():
        cidx = raw.find("//")
        line = raw if cidx == -1 else raw[:cidx]
        count += _count_violations(line, names_re)
    return count


def _scan_doc(path: Path, names_re: re.Pattern[str]) -> int:
    return _count_violations(path.read_text(), names_re)


def _live_counts() -> dict[str, int]:
    names_re = _names_re()
    counts: dict[str, int] = {}
    for rel in _PY_SURFACES:
        n = _scan_py(rel, names_re)
        if n:
            counts[rel] = n
    for path in sorted((ROOT / "src" / "ui" / "static").glob("*.js")) + \
            sorted((ROOT / "src" / "ui" / "static").glob("*.html")):
        n = _scan_js_html(path, names_re)
        if n:
            counts[str(path.relative_to(ROOT))] = n
    for path in sorted((ROOT / "docs").glob("*.md")) + \
            sorted((ROOT).glob("README*.md")) + \
            sorted((ROOT / "docs").glob("INSTALL*.md")):
        n = _scan_doc(path, names_re)
        if n:
            counts[str(path.relative_to(ROOT))] = n
    return counts


def _tier2_live_counts() -> dict[str, int]:
    names_re = _names_re()
    counts: dict[str, int] = {}
    for rel, patterns in _TIER2_BUCKETS:
        total = 0
        for path in _tier2_bucket_files(rel, patterns):
            total += _count_violations(path.read_text(), names_re)
        if total:
            counts[rel] = total
    return counts


def _tier2_coinage_counts() -> dict[str, int]:
    counts: dict[str, int] = {}
    for rel, patterns in _TIER2_BUCKETS:
        total = 0
        for path in _tier2_bucket_files(rel, patterns):
            total += len(_COINAGE_RE.findall(path.read_text()))
        if total:
            counts[rel] = total
    return counts


def test_product_voice_tier2_baseline_file_is_valid_json() -> None:
    baseline = json.loads(TIER2_BASELINE_PATH.read_text())
    assert isinstance(baseline, dict)
    assert all(isinstance(v, int) and v > 0 for v in baseline.values())


def test_product_voice_tier2_matches_the_committed_baseline_exactly() -> None:
    """THE WHOLE-TREE RATCHET (decision a4aa0ba4): a per-directory count over comments,
    docstrings and test names too, not just the reader-facing surfaces tier 1 covers.
    GROWTH-ONLY (decision 3a858fd3, fixing gate w398): an exact-match baseline can never
    survive a merge with any concurrent cleanup tip, since a baseline generated on one
    branch is stale the instant another branch's tip changes the same bucket. A
    directory's count going up fails, meaning new working-agent language crept in
    somewhere in that directory. A count going down PASSES, with a printed notice that
    the baseline is stale in the good direction and the next commit that touches that
    bucket should lower it -- never a hard failure, so concurrent tips stay mergeable."""
    baseline = json.loads(TIER2_BASELINE_PATH.read_text())
    live = _tier2_live_counts()

    grown = {d: (baseline.get(d, 0), n) for d, n in live.items()
             if n > baseline.get(d, 0)}
    shrunk = {d: (baseline.get(d, 0), live.get(d, 0)) for d in baseline
              if live.get(d, 0) < baseline[d]}

    if shrunk:
        print(f"tier-2 product-voice violations DECREASED in these directories "
              f"(was -> now): {shrunk} -- not a failure, but regenerate to lower the "
              f"baseline: {REGEN_CMD}")
    assert not grown, (
        "tier-2 product-voice violations INCREASED in these directories versus the "
        f"committed baseline (was -> now): {grown}. Regenerate with: {REGEN_CMD}")


def test_scan_js_html_strips_block_comments_not_just_line_comments(tmp_path: Path) -> None:
    """Same specimen shape test_taxonomy_drift.py's own sibling test proves against: a
    CSS block comment must not count, only a real string literal does."""
    names_re = _names_re()
    fixture = tmp_path / "fixture.html"
    fixture.write_text(
        "<style>\n"
        "/* this comment mentions Thoth and wave 12 and a1b2c3d4, none of it real */\n"
        ".foo { color: red; } // line comment mentions mail 12345 too\n"
        "</style>\n"
        '<div>real hit: mail 99999</div>\n'
    )
    assert _scan_js_html(fixture, names_re) == 1


def test_names_regex_is_whole_word_only() -> None:
    """A name embedded inside a longer word must never match -- 'imhotep' inside
    'imhotep-binding-probe' style test-seat slugs would otherwise flood the count with
    throwaway test-fixture noise the live-roster query already filters out at the
    source (see `_write_names`'s own active-holder filter)."""
    names_re = _names_re()
    assert names_re.search("anubisknight") is None
    assert names_re.search("Anubis") is not None


def test_product_voice_baseline_file_is_valid_json() -> None:
    baseline = json.loads(BASELINE_PATH.read_text())
    assert isinstance(baseline, dict)
    assert all(isinstance(v, int) and v > 0 for v in baseline.values())


def test_product_voice_matches_the_committed_baseline_exactly() -> None:
    """THE RATCHET ITSELF, GROWTH-ONLY (decision 3a858fd3, fixing gate w398): an
    exact-match baseline can never survive a merge with any concurrent cleanup tip, since
    a baseline generated on one branch is stale the instant another branch's tip changes
    the same file. A file gaining a mention FAILS: someone reintroduced working-agent
    language into a reader-facing surface. A file's count going down PASSES, with a
    printed notice that a voice cleanup landed and the baseline should be lowered in a
    follow-up commit -- never a hard failure, so concurrent tips stay mergeable."""
    baseline = json.loads(BASELINE_PATH.read_text())
    live = _live_counts()

    grown = {f: (baseline.get(f, 0), n) for f, n in live.items()
             if n > baseline.get(f, 0)}
    shrunk = {f: (baseline.get(f, 0), live.get(f, 0)) for f in baseline
              if live.get(f, 0) < baseline[f]}

    if shrunk:
        print(f"product-voice violations DECREASED in these files (was -> now): "
              f"{shrunk} -- not a failure, but regenerate to lower the baseline: "
              f"{REGEN_CMD}")
    assert not grown, (
        "product-voice violations INCREASED in these files versus the committed "
        f"baseline (was -> now): {grown} -- a fleet worker name, a mail/DM/msg "
        "id, an 8-hex ruling/decision/thread id, a wave/gate number, an operator "
        "quotation, or an em dash was newly introduced into a reader-facing surface. "
        f"Regenerate with: {REGEN_CMD}")


if __name__ == "__main__":
    import sys

    if "--write-names" in sys.argv:
        import asyncio
        import os

        async def _write_names() -> None:
            import asyncpg

            pool = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=1, max_size=1)
            try:
                rows = await pool.fetch(
                    "SELECT DISTINCT a.value #>> '{}' AS handle "
                    "FROM current_assertions a JOIN objects o ON o.id = a.object_id "
                    "WHERE o.type='Seat' AND a.name='handle' AND EXISTS ("
                    "  SELECT 1 FROM links l WHERE l.to_id = o.id AND l.type='holds' "
                    "  AND l.valid_until IS NULL) "
                    "ORDER BY 1")
            finally:
                await pool.close()
            names = sorted({r["handle"] for r in rows})
            NAMES_PATH.write_text(json.dumps(names, indent=2) + "\n")
            print(f"wrote {NAMES_PATH.relative_to(ROOT)} -- {len(names)} active-holder "
                  "seat handles")

        asyncio.run(_write_names())
        raise SystemExit(0)

    if "--write" not in sys.argv:
        print(f"usage: {REGEN_CMD}\n   or: {REGEN_NAMES_CMD}", file=sys.stderr)
        raise SystemExit(1)
    before = json.loads(BASELINE_PATH.read_text()) if BASELINE_PATH.exists() else {}
    after = _live_counts()
    BASELINE_PATH.write_text(json.dumps(dict(sorted(after.items())), indent=2) + "\n")
    changed = {f: (before.get(f, 0), n) for f, n in after.items() if before.get(f, 0) != n}
    changed |= {f: (before[f], after.get(f, 0)) for f in before
                if f not in after and before[f] != 0}
    print(f"wrote {BASELINE_PATH.relative_to(ROOT)} -- {len(after)} files, "
          f"{sum(after.values())} total violations")
    if changed:
        print("changed (was -> now):")
        for f, (b, n) in sorted(changed.items()):
            print(f"  {f}: {b} -> {n}")
    else:
        print("no change")

    t2_before = json.loads(TIER2_BASELINE_PATH.read_text()) if TIER2_BASELINE_PATH.exists() else {}
    t2_after = _tier2_live_counts()
    TIER2_BASELINE_PATH.write_text(json.dumps(dict(sorted(t2_after.items())), indent=2) + "\n")
    t2_changed = {d: (t2_before.get(d, 0), n) for d, n in t2_after.items()
                  if t2_before.get(d, 0) != n}
    t2_changed |= {d: (t2_before[d], t2_after.get(d, 0)) for d in t2_before
                   if d not in t2_after and t2_before[d] != 0}
    print(f"wrote {TIER2_BASELINE_PATH.relative_to(ROOT)} -- {len(t2_after)} directories, "
          f"{sum(t2_after.values())} total violations")
    if t2_changed:
        print("tier-2 changed (was -> now):")
        for d, (b, n) in sorted(t2_changed.items()):
            print(f"  {d}: {b} -> {n}")
    else:
        print("tier-2: no change")

    coinage = _tier2_coinage_counts()
    print(f"coinage report (informational, not gated) -- {sum(coinage.values())} total:")
    for d, n in sorted(coinage.items()):
        print(f"  {d}: {n}")
