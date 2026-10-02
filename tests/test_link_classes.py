"""NAVIGABLE SPACE, THE READING LAYER: every link type this
codebase's own write paths can actually mint must be deliberately classified
structural or semantic -- "no unclassified" is the ruling's own acceptance line, held
here to a stricter bar than `link_class()`'s own safe default by scanning src/ for the
real, live population rather than trusting a hand-typed list to stay current."""
from __future__ import annotations

import re
from pathlib import Path

from src.ontology.link_classes import (
    CONTAINER_LINK_TYPES,
    SEMANTIC_LINK_TYPES_KNOWN,
    STRUCTURAL_LINK_TYPES,
    link_class,
)
from src.ontology.schema import LINK_TYPES

ROOT = Path(__file__).resolve().parent.parent
_SRC = ROOT / "src"

# one level of nested parens tolerated (e.g. `datetime.now(UTC)` inside a call's own
# argument list) -- matches this codebase's actual call shapes: `create_link(from, to,
# "type", ...)`, and the lineage/agents helpers' own thin wrappers `_link_once(actions,
# a, b, "type", ...)` / `_link(a, b, "type", ...)`.
_CALL_RE = re.compile(
    r"\b(?:create_link|_link_once|_link)\(([^()]*(?:\([^()]*\)[^()]*)*)\)")
_STRING_RE = re.compile(r'"([a-z_]+)"')


def _link_types_actually_written() -> set[str]:
    """Every quoted snake_case string appearing as an argument to a link-creation call
    site, filtered to ones that are themselves a DECLARED link type (schema.py's own
    LINK_TYPES) -- a coincidental match (some unrelated string that happens to also be
    a declared type name) is vanishingly unlikely and would be a harmless over-count
    in any case, never a missed one."""
    found: set[str] = set()
    for path in _SRC.rglob("*.py"):
        text = path.read_text()
        for call in _CALL_RE.finditer(text):
            for tok in _STRING_RE.findall(call.group(1)):
                if tok in LINK_TYPES:
                    found.add(tok)
    return found


def test_every_actually_written_link_type_is_classified() -> None:
    written = _link_types_actually_written()
    assert written, "the scan itself found nothing -- the regex has drifted from " \
        "this codebase's real call shapes, fix the scan before trusting this test"
    classified = STRUCTURAL_LINK_TYPES | SEMANTIC_LINK_TYPES_KNOWN
    unclassified = sorted(written - classified)
    assert unclassified == [], (
        f"these link types are actually written by this codebase but not in either "
        f"explicit set (falling through to link_class()'s own default instead of a "
        f"deliberate choice): {unclassified}")


def test_link_class_is_total_and_defaults_safely_for_unknown_types() -> None:
    assert link_class("in_repo") == "container"
    assert link_class("managed_by") == "structural"
    assert link_class("cites") == "semantic"
    assert link_class("some-extension-type-nobody-declared") == "semantic"


def test_structural_and_semantic_sets_never_overlap() -> None:
    assert STRUCTURAL_LINK_TYPES & SEMANTIC_LINK_TYPES_KNOWN == set()


def test_container_types_are_a_subset_of_structural() -> None:
    """THE PHYSICS LAYOUT: container is a flag NESTED inside the
    structural class, never an independent third bucket -- every existing caller
    checking membership in STRUCTURAL_LINK_TYPES directly (graph_layout.py's own
    relax-exclusion) still catches every container type without any change."""
    assert CONTAINER_LINK_TYPES <= STRUCTURAL_LINK_TYPES
    assert CONTAINER_LINK_TYPES & SEMANTIC_LINK_TYPES_KNOWN == set()


def test_link_class_container_types_all_read_container() -> None:
    for lt in CONTAINER_LINK_TYPES:
        assert link_class(lt) == "container"
