"""agent()'s hand-built discriminated-union inputSchema (task #202, Thoth dispatch
7162, proposal decision 65a6eb73, price-minimizer #1) — the fifth object-type
dispatcher. tests/test_dispatcher_schema_contract.py already proves every
_HAND_BUILT_SCHEMAS entry is a well-formed discriminated union, generically; this file
is the per-action REAL-CLIENT proof, the same discipline test_seat_dispatcher_schema.py
and its siblings already established — a genuinely independent VALID_PAYLOADS table,
checked against jsonschema.validate (the exact call the mcp SDK's own server/client
make internally), not just asserted to agree with _AGENT_ACTION_PARAMS by construction.
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


async def test_agent_is_live_and_its_aliases_are_not() -> None:
    live_names = {t.name for t in await srv.mcp.list_tools()}
    assert "agent" in live_names
    for hidden in (
        "claim_name", "correct_agent_house", "retire_agent", "fleet_reconcile",
        "file_subagent", "file_subagents",
    ):
        assert hidden not in live_names, f"{hidden} should be hidden, still live"
    # explicitly declined from this fold — stay live/named or already hidden elsewhere
    for stays in ("retire", "identify_agent", "succession_chain"):
        assert stays in live_names, f"{stays} should still be live-named"


# (action, valid_payload_without_action) — one genuinely valid call per action,
# deliberately independent of _AGENT_ACTION_PARAMS (src/mcp_server.py) — proving the
# SCHEMA's own idea of "valid" against a real validator, not just asserting the two
# tables agree by construction.
AGENT_VALID_PAYLOADS: dict[str, dict[str, Any]] = {
    "claim_name": {"name": "distinctive-handle"},
    "correct_house": {"agent_id": "agent:abc123", "project": "widget"},
    "retire": {"agent_id": "agent:abc123", "because": "third-party retirement"},
    "fleet_reconcile": {},
    "file_subagent": {"subagent_id": "agent:abc123.1"},
    "file_subagents": {"project": "widget"},
}


async def test_agent_schema_is_a_well_formed_discriminated_union() -> None:
    schema = await _schema("agent")
    jsonschema.Draft7Validator.check_schema(schema)
    assert schema["type"] == "object"
    branches = schema["oneOf"]
    assert len(branches) == 6, "one branch per agent action — update this count and " \
        "the ACTION TABLE docstring together if the action set changes"
    actions = {b["properties"]["action"]["const"] for b in branches}
    assert actions == set(AGENT_VALID_PAYLOADS), (
        "AGENT_VALID_PAYLOADS has drifted from the schema's own action set")


@pytest.mark.parametrize("action", sorted(AGENT_VALID_PAYLOADS))
async def test_a_real_client_validates_the_valid_agent_call_for_every_action(
    action: str,
) -> None:
    schema = await _schema("agent")
    instance = {"action": action, **AGENT_VALID_PAYLOADS[action]}
    jsonschema.validate(instance=instance, schema=schema)


@pytest.mark.parametrize("action", sorted(AGENT_VALID_PAYLOADS))
async def test_a_real_client_rejects_that_agent_action_missing_its_required_params(
    action: str,
) -> None:
    schema = await _schema("agent")
    branch = next(b for b in schema["oneOf"]
                 if b["properties"]["action"]["const"] == action)
    required_beyond_action = [r for r in branch["required"] if r != "action"]
    if not required_beyond_action:
        pytest.skip(f"{action} has no required params beyond action — nothing to omit")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance={"action": action}, schema=schema)


async def test_a_real_client_rejects_an_unknown_agent_action() -> None:
    schema = await _schema("agent")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance={"action": "not_a_real_action"}, schema=schema)


async def test_a_real_client_rejects_an_unexpected_param_for_a_known_agent_action() -> None:
    schema = await _schema("agent")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            instance={"action": "fleet_reconcile", "this_param_does_not_exist": 1},
            schema=schema)


async def test_a_real_client_rejects_one_agent_actions_params_on_anothers_const() -> None:
    """oneOf discriminates on the `action` const alone — claim_name's own `name` must
    not leak into `retire`'s branch just because both are objects."""
    schema = await _schema("agent")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            instance={"action": "retire", "name": "should-not-be-here"}, schema=schema)
