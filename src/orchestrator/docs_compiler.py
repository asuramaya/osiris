"""THE DOCS COMPILER (task #111, thread 26694d10) — a doc's compiled section regenerates
from what it actually snapshots, instead of being a hand-maintained artifact someone must
keep true by hand and a test must assert byte-for-byte (the mechanism this replaces:
tests/test_schema.py's retired REFERENCE.md == render_reference() assertion).

Reuses boot_compiler.py's marker mechanism VERBATIM — wrap_managed / locate_managed_section /
MarkerError, imported, never reimplemented — because a second marker vocabulary would be
exactly the "two records of one truth" shape (decision 38c71544) this house already treats
as a defect. The NEVER-CLOBBER BOUNDARY and REFUSE-ON-MANGLED-MARKER rule are load-bearing
here for the identical reason they are in boot_compiler.py: hand prose living in the SAME
file as a compiled section (REFERENCE.md's evidence-classes/sources/MCP-tools/repo-map
prose, surrounding the compiled data-model block) must survive a recompile byte-for-byte.

REFERENCE.md IS DELIBERATELY POOL-FREE (Thoth's ruling, msg 2099, correcting an earlier
mis-stated acceptance criterion that would have wired it to catalog.py's LIVE, accretive
catalog instead): it compiles from schema.py's STATIC declared manifest, via the EXISTING
schema.render_reference() — not reimplemented here, so there is exactly one function that
knows how to render the type catalog as markdown — so it is regenerable from an empty
checkout with no database at all, and an agent minting a type through ensure_type's live
accretion path changes NOTHING in the shipped doc. `reference_catalog` is ALSO registered
as a composition function in compositions.py (queryable via run_composition("reference"),
UI/API parity with every other named lens) — but that async path is for LIVE inspection
only; the FILE-COMPILE path below never touches a pool, on purpose.

Generic enough for more than REFERENCE.md: `compile_markdown_section` takes an
already-rendered body string and a target path — nothing in it is REFERENCE.md-specific.
A future roadmap/canon/changelog doc reuses the same primitive with its own composition-
backed body, exactly as `compile_reference_doc` does below for this one.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from src.orchestrator.boot_compiler import MarkerError, locate_managed_section, wrap_managed

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
REFERENCE_MD = _REPO_ROOT / "docs" / "REFERENCE.md"

_REFERENCE_VERSION = "schema-v1"  # bump only if render_reference()'s own shape changes


def compile_markdown_section(
    target_path: Path, body: str, *, version: str, because: str,
) -> dict[str, Any]:
    """Recompile `target_path`'s managed section to `body`, reusing boot_compiler's exact
    marker mechanism: a malformed or missing managed section REFUSES LOUDLY rather than
    guessing which span to replace; hand prose outside the markers survives untouched.
    Generic — takes an already-rendered body, no doc-specific logic lives here."""
    if not because.strip():
        return {"error": "because is required — a recompile is testimony, same as a reissue"}
    if not target_path.exists():
        return {"error": f"no such file: {target_path} — this compiles an EXISTING doc's "
                         "managed section, never scaffolds a new file"}
    text = target_path.read_text()
    try:
        b_start, _b_end, _e_start, e_end, _old_version = locate_managed_section(text)
    except MarkerError as exc:
        return {"error": f"{target_path.name}: {exc} — refusing to guess; fix the marker "
                         "by hand"}
    wrapped = wrap_managed(body, version)
    # wrap_managed's OWN output already ends in exactly one "\n" after the END marker — but
    # `e_end` (boot_compiler's own locate_managed_section) only spans the marker TEXT, never
    # that trailing newline, so it survives untouched in `text[e_end:]` on every splice. Left
    # alone, that means each recompile ADDS a fresh blank line on top of the last one's
    # (accumulating forever) instead of being idempotent — absorb exactly the one newline
    # wrap_managed itself already accounts for, so a no-op recompile stays a true no-op.
    tail = text[e_end:]
    if tail.startswith("\n"):
        tail = tail[1:]
    new_text = text[:b_start] + wrapped + tail
    if new_text == text:
        return {"path": str(target_path), "version": version, "because": because,
                "changed": False, "note": "no change — the compiled section already matches"}
    target_path.write_text(new_text)
    return {"path": str(target_path), "version": version, "because": because, "changed": True,
            "note": "managed section recompiled"}


def compile_reference_doc(*, because: str) -> dict[str, Any]:
    """Recompile docs/REFERENCE.md's data-model section from schema.py's static manifest —
    pool-free, regenerable from an empty checkout. Reuses schema.render_reference() verbatim
    (never reimplemented here)."""
    from src.ontology.schema import render_reference

    body = render_reference().rstrip("\n")
    return compile_markdown_section(REFERENCE_MD, body, version=_REFERENCE_VERSION,
                                    because=because)
