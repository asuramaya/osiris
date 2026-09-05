"""THE PRICE-MINIMIZER #1 GATE, GENERALIZED (task #202/#204, Thoth msg 7039/7040/7059):
`seat` was the first object-type dispatcher and got its own hand-written proof
(tests/test_seat_dispatcher_schema.py) — real, but seat-specific, so a SECOND
dispatcher (a future `project`/`agent` fold) could ship with a malformed or leaky
oneOf schema and nothing fleet-wide would catch it until a real client broke on it.

This iterates `_HAND_BUILT_SCHEMAS` (src/mcp_server.py's own override dict, the exact
seam `BoundedMCP.list_tools()` substitutes into every dispatcher's advertised
inputSchema) — whatever dispatchers exist TODAY, automatically, never a hardcoded
name. Any tool added to that dict without a real `oneOf`-per-action shape, a unique
action per branch, `action` required, or `additionalProperties: False` per branch
fails here — the contract price-minimizer #1 promised, enforced fleet-wide rather
than reproven by hand for every dispatcher that ever ships."""
from __future__ import annotations

from typing import Any

import jsonschema
from src import mcp_server as srv


def test_every_hand_built_schema_is_a_well_formed_discriminated_union() -> None:
    assert srv._HAND_BUILT_SCHEMAS, (
        "no hand-built dispatcher schemas registered — if this fires after the seat "
        "dispatcher's own removal, drop this test too; if it fires because the dict "
        "genuinely emptied out from under a still-live dispatcher, that is the bug"
    )
    for name, schema in srv._HAND_BUILT_SCHEMAS.items():
        jsonschema.Draft7Validator.check_schema(schema)
        assert schema.get("type") == "object", (
            f"{name!r}'s hand-built schema must be type=object at the top level")
        branches = schema.get("oneOf")
        assert branches, (
            f"{name!r} is in _HAND_BUILT_SCHEMAS but carries no oneOf — a dispatcher "
            "schema IS the discriminated union; a flat schema belongs to ordinary "
            "signature-driven generation instead, not this override seam")
        _assert_discriminated_union(name, branches)


def _assert_discriminated_union(name: str, branches: list[dict[str, Any]]) -> None:
    actions: set[str] = set()
    for branch in branches:
        assert branch.get("type") == "object", (
            f"{name!r}: every oneOf branch must itself be type=object")
        assert branch.get("additionalProperties") is False, (
            f"{name!r}: every branch must set additionalProperties=False — a real "
            "client's typo or a cross-action param leaking onto this branch's own "
            "const must be rejected client-side, not silently accepted (price-"
            "minimizer #1's own requirement)")
        props = branch.get("properties", {})
        action_prop = props.get("action")
        assert action_prop is not None and "const" in action_prop, (
            f"{name!r}: every branch must pin `action` to a const — that is what "
            "makes this a DISCRIMINATED union rather than an ambiguous oneOf a real "
            "client cannot pick a branch from")
        required = branch.get("required", [])
        assert "action" in required, (
            f"{name!r}: every branch must require `action` — a branch where it's "
            "merely present-but-optional lets a caller omit the one field that "
            "actually selects which shape they mean")
        actions.add(action_prop["const"])
    assert len(actions) == len(branches), (
        f"{name!r}: two branches sharing one action const make the union ambiguous — "
        "a real client (and _seat_impl-style pre-dispatch validation) cannot tell "
        "which branch a call meant. Every action must be unique.")


def test_the_detector_itself_catches_a_missing_additional_properties_false() -> None:
    """PROVE THE MECHANISM — same discipline every other gate in this house's parity/
    contract suite runs on, not just exercised live against whatever seat happens to
    look like today."""
    import pytest

    broken = {
        "type": "object",
        "oneOf": [
            {"type": "object", "properties": {"action": {"const": "a"}},
             "required": ["action"]},  # missing additionalProperties: False
        ],
    }
    with pytest.raises(AssertionError, match="additionalProperties"):
        _assert_discriminated_union("broken_dispatcher", broken["oneOf"])


def test_the_detector_itself_catches_a_duplicate_action_const() -> None:
    import pytest

    branches = [
        {"type": "object", "properties": {"action": {"const": "dup"}},
         "required": ["action"], "additionalProperties": False},
        {"type": "object", "properties": {"action": {"const": "dup"}},
         "required": ["action"], "additionalProperties": False},
    ]
    with pytest.raises(AssertionError, match="ambiguous"):
        _assert_discriminated_union("broken_dispatcher", branches)


def test_the_detector_itself_catches_action_missing_from_required() -> None:
    import pytest

    branches = [
        {"type": "object", "properties": {"action": {"const": "a"}},
         "required": [], "additionalProperties": False},
    ]
    with pytest.raises(AssertionError, match="require `action`"):
        _assert_discriminated_union("broken_dispatcher", branches)
