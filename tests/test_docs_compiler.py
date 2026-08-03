"""THE DOCS COMPILER (task #111, thread 26694d10) — `compile_markdown_section` is generic
and pool-free: pure marker-splice tests, no Actions/DB needed. `compile_reference_doc` is
tested against a monkeypatched target path, never the real docs/REFERENCE.md — a test run
must never mutate the repo's own doc as a side effect.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from src.ontology.schema import render_reference
from src.orchestrator import docs_compiler
from src.orchestrator.boot_compiler import wrap_managed
from src.orchestrator.docs_compiler import compile_markdown_section, compile_reference_doc


def _doc(managed_body: str = "old content", version: str = "v1") -> str:
    return ("# A Doc\n\nSome hand-written prose above.\n\n"
            + wrap_managed(managed_body, version)
            + "\n## Below\n\nMore hand-written prose below, unrelated to the managed section.\n")


def test_compile_markdown_section_replaces_only_the_managed_span(tmp_path: Path) -> None:
    p = tmp_path / "doc.md"
    p.write_text(_doc())
    out = compile_markdown_section(p, "new content", version="v2", because="test")
    assert out["changed"] is True
    text = p.read_text()
    assert "new content" in text and "old content" not in text
    assert "Some hand-written prose above." in text
    assert "More hand-written prose below, unrelated to the managed section." in text


def test_compile_markdown_section_is_idempotent(tmp_path: Path) -> None:
    p = tmp_path / "doc.md"
    p.write_text(_doc("old content", "v1"))
    first = compile_markdown_section(p, "same content", version="v1", because="test")
    assert first["changed"] is True
    second = compile_markdown_section(p, "same content", version="v1", because="test")
    assert second == {"path": str(p), "version": "v1", "because": "test", "changed": False,
                      "note": "no change — the compiled section already matches"}


def test_compile_markdown_section_refuses_on_mangled_marker_and_touches_nothing(
        tmp_path: Path) -> None:
    p = tmp_path / "doc.md"
    mangled = _doc().replace("<!-- osiris:compiled:end -->", "")  # END marker deleted
    p.write_text(mangled)
    out = compile_markdown_section(p, "new content", version="v2", because="test")
    assert "error" in out
    assert p.read_text() == mangled  # untouched — never guess which span was meant


def test_compile_markdown_section_refuses_missing_file(tmp_path: Path) -> None:
    out = compile_markdown_section(tmp_path / "nope.md", "x", version="v1", because="test")
    assert "error" in out and "no such file" in out["error"]


def test_compile_markdown_section_requires_because(tmp_path: Path) -> None:
    p = tmp_path / "doc.md"
    p.write_text(_doc())
    out = compile_markdown_section(p, "new content", version="v2", because="  ")
    assert "error" in out and "because is required" in out["error"]


def test_compile_reference_doc_regenerates_from_schema_pool_free(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The doc-specific wiring: body comes from schema.render_reference() verbatim, hand
    prose survives, and none of this touches a database (compile_reference_doc takes no
    pool/Actions argument at all — regenerable from an empty checkout)."""
    p = tmp_path / "REFERENCE.md"
    p.write_text(_doc("stale data-model content", "schema-v0"))
    monkeypatch.setattr(docs_compiler, "REFERENCE_MD", p)
    out = compile_reference_doc(because="test recompile")
    assert out["changed"] is True
    text = p.read_text()
    assert render_reference().rstrip("\n") in text
    assert "stale data-model content" not in text
    assert "Some hand-written prose above." in text
    assert "More hand-written prose below, unrelated to the managed section." in text


def test_compile_reference_doc_is_idempotent_when_already_current(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = tmp_path / "REFERENCE.md"
    p.write_text(_doc("stale", "schema-v0"))
    monkeypatch.setattr(docs_compiler, "REFERENCE_MD", p)
    compile_reference_doc(because="first")
    again = compile_reference_doc(because="second")
    assert again["changed"] is False
