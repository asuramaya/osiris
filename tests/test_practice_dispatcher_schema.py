"""practice()'s hand-built discriminated-union inputSchema (task #202, Thoth dispatch
7162, proposal decision 07395004, price-minimizer #1) — the sixth and FINAL object-type
dispatcher of #202's own fold arc. tests/test_dispatcher_schema_contract.py already
proves every _HAND_BUILT_SCHEMAS entry is a well-formed discriminated union,
generically; this file is the per-action REAL-CLIENT proof, the same discipline every
sibling dispatcher's own schema test already established — an independent
VALID_PAYLOADS table, checked against jsonschema.validate (the exact call the mcp SDK's
own server/client make internally), not just asserted to agree with
_PRACTICE_ACTION_PARAMS by construction.
"""
from __future__ import annotations

from typing import Any

import jsonschema
import pytest
from src import mcp_server as srv


async def _schema(tool_name: str) -> dict[str, Any]:
    live = await srv.mcp.list_tools()
    tool = next(t for t in live if t.name == tool_name)
    return tool.inputSchema


async def test_practice_is_live_and_its_aliases_are_not() -> None:
    live_names = {t.name for t in await srv.mcp.list_tools()}
    assert "practice" in live_names
    for hidden in ("record_practice", "amend_practice"):
        assert hidden not in live_names, f"{hidden} should be hidden, still live"
    # explicitly declined from this fold, per decision 07395004 — stay live/named
    for stays in ("record_decision", "amend_decision", "consult_canon",
                 "handoff_briefing", "dismiss_brief", "ack_handoff", "practices"):
        assert stays in live_names, f"{stays} should still be live-named"


# (action, valid_payload_without_action) — one genuinely valid call per action,
# deliberately independent of _PRACTICE_ACTION_PARAMS (src/mcp_server.py) — proving the
# SCHEMA's own idea of "valid" against a real validator, not just asserting the two
# tables agree by construction.
PRACTICE_VALID_PAYLOADS: dict[str, dict[str, Any]] = {
    "record": {"statement": "always read the full function body before editing it"},
    "amend": {"ref": "371fbbac", "amendment": "narrowed to only apply when X"},
}


async def test_practice_schema_is_a_well_formed_discriminated_union() -> None:
    schema = await _schema("practice")
    jsonschema.Draft7Validator.check_schema(schema)
    assert schema["type"] == "object"
    branches = schema["oneOf"]
    assert len(branches) == 2, "one branch per practice action — update this count " \
        "and the ACTION TABLE docstring together if the action set changes"
    actions = {b["properties"]["action"]["const"] for b in branches}
    assert actions == set(PRACTICE_VALID_PAYLOADS), (
        "PRACTICE_VALID_PAYLOADS has drifted from the schema's own action set")


@pytest.mark.parametrize("action", sorted(PRACTICE_VALID_PAYLOADS))
async def test_a_real_client_validates_the_valid_practice_call_for_every_action(
    action: str,
) -> None:
    schema = await _schema("practice")
    instance = {"action": action, **PRACTICE_VALID_PAYLOADS[action]}
    jsonschema.validate(instance=instance, schema=schema)


@pytest.mark.parametrize("action", sorted(PRACTICE_VALID_PAYLOADS))
async def test_a_real_client_rejects_that_practice_action_missing_its_required_params(
    action: str,
) -> None:
    schema = await _schema("practice")
    branch = next(b for b in schema["oneOf"]
                 if b["properties"]["action"]["const"] == action)
    required_beyond_action = [r for r in branch["required"] if r != "action"]
    if not required_beyond_action:
        pytest.skip(f"{action} has no required params beyond action — nothing to omit")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance={"action": action}, schema=schema)


async def test_a_real_client_rejects_an_unknown_practice_action() -> None:
    schema = await _schema("practice")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance={"action": "not_a_real_action"}, schema=schema)


async def test_a_real_client_rejects_an_unexpected_param_for_a_known_practice_action() -> None:
    schema = await _schema("practice")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            instance={"action": "amend", "ref": "371fbbac", "amendment": "x",
                     "this_param_does_not_exist": 1},
            schema=schema)


async def test_a_real_client_rejects_one_practice_actions_params_on_anothers_const() -> None:
    """oneOf discriminates on the `action` const alone — record's own `statement` must
    not leak into `amend`'s branch just because both are objects."""
    schema = await _schema("practice")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            instance={"action": "amend", "statement": "should-not-be-here"},
            schema=schema)
