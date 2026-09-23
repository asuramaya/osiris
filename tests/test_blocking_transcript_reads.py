"""THE BLOCKING TRANSCRIPT READ GUARD (THE OSIRIS-MCP MAIN-THREAD STALL): the confirmed
root cause was a backfill dry run doing `path.read_text().splitlines()` on a 200-470MB
harness session transcript ON THE MCP LOOP THREAD. A single blocking synchronous read
call froze the entire single-threaded asyncio event loop, which is why every concurrent
MCP tool call (inbox, mount, dossier, object_events, search, backlog, candidates)
crawled then stopped entirely for 19 minutes. This is the standing guard against that
exact shape recurring: every module whose real job is reading a harness SESSION
TRANSCRIPT (a `.jsonl` file, potentially hundreds of MB) must do so off the loop
thread, `asyncio.to_thread(path.read_text, ...)`, the SAME call reference shape
provenance_backfill.py's own already-fixed call site uses, never a direct
`path.read_text()`/`.read_bytes()`/`.readlines()`/`.read()` call, unless the code runs
inside a WORKER JOB (`src/workers/*.py`), which has no event loop shared with
osiris-mcp to freeze.

DETECTION SHAPE: a genuinely wrapped call, `asyncio.to_thread(path.read_text, ...)`,
never appears as an `ast.Call` node with `attr='read_text'` at all, the method is
passed as a bare, uncalled attribute reference (the argument to `to_thread`), not
invoked inline. So this scanner needs no separate "is this inside to_thread" check: ANY
direct `ast.Call` to one of the four blocking read methods, found by walking a scoped
module's AST, is unwrapped by construction.

SCOPE, DELIBERATELY NARROW (same "coarse but sufficient proxy" discipline
test_unbounded_wait.py's own module docstring already states and defends for this
codebase): rather than flag every `read_text`/`read_bytes`/`readlines`/`read` call in
the whole tree, the overwhelming majority are small config/pin/state file reads with
no realistic size risk (offices.py's own git HEAD read, agents.py's own pin.toml, and
dozens more), flagging those would be pure noise with no signal, the exact failure
test_unbounded_wait.py's own subprocess-ratchet docstring already warns against for a
structurally identical reason. This scans only `_TRANSCRIPT_MODULES` below: modules
whose STATED job is reading harness session transcripts. Adding a module to that set is
a real scope decision, not automatic, name why in a comment when you do.

THE RATCHET (same law as test_render_hygiene.py's `_ALLOWLIST` / test_unbounded_wait.py's
`_SUBPROCESS_BASELINE`): a per-file EXACT count of currently-unwrapped calls, drift in
EITHER direction fails, so a module that quietly gains OR loses one of these call sites
must touch this baseline, on purpose, never silently. The non-zero entries below are
KNOWN, PRE-EXISTING gaps this guard's own FIRST run found, named here rather than
silently grandfathered as "fine" or quietly fixed without a record. None has yet been
measured to reproduce the incident's own 95%-CPU shape (the confirmed culprit was
provenance_backfill.py, already fixed, baseline 0, so THIS module regressing is what
the ratchet actually protects going forward); these are the same SHAPE of risk,
flagged for whoever owns each module next, not fixed by this guard's own author (out
of this dispatch's stated scope: "scope narrows to the instrumentation").
"""
from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_SELF = Path(__file__).resolve()

_BLOCKING_READ_METHODS = frozenset({"read_text", "read_bytes", "readlines", "read"})

# Modules whose real job is reading harness session transcripts (.jsonl files,
# potentially hundreds of MB), see the module docstring for why this list, not a
# whole-tree sweep, is the right scope. `src/workers/*.py` is deliberately NEVER a
# member: a worker job has no event loop shared with osiris-mcp to freeze.
_TRANSCRIPT_MODULES: dict[str, int] = {
    # CLOSED: all 13 pre-existing gaps this guard's own first run found are fixed,
    # lineage.py's three sub-agent-transcript readers made async, wrapped via
    # asyncio.to_thread; sessions.py's two genuinely UNBOUNDED whole-transcript reads
    # (774/793) made bounded AND streaming, not just off-loop ("memory is the second
    # hazard"), its bounded tail/chunk readers and CLI stdin read wrapped for
    # ratchet-cleanliness; claude_jsonl.py's two small file reads switched to a
    # file-object line iterator / json.load(f) (streams from the file object, never a
    # literal .read_text()/.read() in this source at all, no async/Protocol change
    # needed since HarnessAdapter.enumerate() stays sync).
    # Every entry below reads 0, a regression here is exactly what this ratchet exists
    # to catch first, same law "src/orchestrator/provenance_backfill.py": 0 already set.
    "src/orchestrator/lineage.py": 0,
    "src/orchestrator/provenance_backfill.py": 0,
    "src/ingest/sessions.py": 0,
    "src/ingest/harness/claude_jsonl.py": 0,
}


def _scan(path: Path) -> list[tuple[int, str]]:
    tree = ast.parse(path.read_text(), filename=str(path))
    violations: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute) or func.attr not in _BLOCKING_READ_METHODS:
            continue
        violations.append((node.lineno, f"{ast.unparse(func)}(...)"))
    return violations


def test_no_new_unwrapped_transcript_reads_outside_the_baseline() -> None:
    problems: list[str] = []
    for rel, expected in sorted(_TRANSCRIPT_MODULES.items()):
        path = ROOT / rel
        assert path.is_file(), f"{rel} named in _TRANSCRIPT_MODULES no longer exists"
        hits = _scan(path)
        actual = len(hits)
        if actual != expected:
            direction = "gained" if actual > expected else "fixed/removed"
            where = "; ".join(f"{ln}: {src}" for ln, src in hits)
            problems.append(
                f"{rel}: {direction} unwrapped transcript read(s), ratchet says "
                f"{expected}, found {actual} ({where}). If you fixed one, lower the "
                "baseline to match. If you added a genuinely necessary one, wrap it in "
                "asyncio.to_thread (preferred, e.g. `await asyncio.to_thread(path."
                "read_text, ...)`) or move the call into a worker job "
                "(src/workers/*.py), never raise this baseline without one of those.")
    assert not problems, "\n".join(problems)


def test_the_detector_finds_a_synthetic_unwrapped_read(tmp_path: Path) -> None:
    """PROVE THE MECHANISM before trusting it against real files (the same discipline
    test_cli_mcp_parity.py's/test_unbounded_wait.py's own proof tests hold to)."""
    sample = tmp_path / "sample.py"
    sample.write_text(
        "def f(transcript):\n"
        "    text = transcript.read_text('utf-8', errors='replace')\n"
        "    return text.splitlines()\n")
    hits = _scan(sample)
    assert hits == [(2, "transcript.read_text(...)")]


def test_the_detector_does_not_flag_an_asyncio_to_thread_wrapped_read(tmp_path: Path) -> None:
    """The exact safe shape provenance_backfill.py's own fixed call site uses: the
    method is a bare, uncalled attribute reference, never an ast.Call at all."""
    sample = tmp_path / "sample.py"
    sample.write_text(
        "import asyncio\n"
        "async def f(path):\n"
        "    text = await asyncio.to_thread(path.read_text, errors='replace')\n"
        "    return text.splitlines()\n")
    assert _scan(sample) == []


def test_the_detector_ignores_splitlines_itself() -> None:
    """`.splitlines()` on an already-in-hand string is not a blocking read, only the
    four FILE-READ methods themselves are in scope (module docstring, DETECTION SHAPE)."""
    import inspect

    src = inspect.getsource(_scan)
    assert "splitlines" not in src
