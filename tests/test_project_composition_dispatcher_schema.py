"""project()'s and composition()'s hand-built discriminated-union inputSchemas (task
#202, Thoth dispatch 7095, price-minimizer #1) — the second and third object-type
dispatchers after seat. tests/test_dispatcher_schema_contract.py already proves every
_HAND_BUILT_SCHEMAS entry is a well-formed discriminated union, generically; this file
is the per-action REAL-CLIENT proof Thoth's dispatch asked for by name, the same
discipline test_seat_dispatcher_schema.py already established for seat — a genuinely
independent VALID_PAYLOADS table per dispatcher, checked against jsonschema.validate
(the exact call the mcp SDK's own server/client make internally), not just asserted to
agree with _PROJECT_ACTION_PARAMS/_COMPOSITION_ACTION_PARAMS by construction.
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


async def test_project_and_composition_are_live_and_their_aliases_are_not() -> None:
    live_names = {t.name for t in await srv.mcp.list_tools()}
    assert "project" in live_names
    assert "composition" in live_names
    for hidden in (
        "create_project", "ingest_project", "ingest_project_third_party",
        "rename_project", "fork_project", "unfork_project", "retire_project",
        "project_identity_evidence", "assert_project_property",
        "save_composition", "run_composition", "list_compositions",
    ):
        assert hidden not in live_names, f"{hidden} should be hidden, still live"


# ---------------------------------------------------------------------------------
# project()
# ---------------------------------------------------------------------------------

# (action, valid_payload_without_action) — one genuinely valid call per action,
# deliberately independent of _PROJECT_ACTION_PARAMS (src/mcp_server.py) — proving the
# SCHEMA's own idea of "valid" against a real validator, not just asserting the two
# tables agree by construction.
PROJECT_VALID_PAYLOADS: dict[str, dict[str, Any]] = {
    "create": {"name": "widget", "because": "a genuinely new tree"},
    "ingest": {},
    "rename": {"project": "repo:widget", "new_name": "gadget", "because": "renamed upstream"},
    "fork": {"project": "repo:widget", "fork_into": "repo:widget2", "because": "splitting"},
    "unfork": {"project": "repo:widget", "fork_into": "repo:widget2", "because": "undoing"},
    "retire": {"project": "repo:widget", "because": "dead tree"},
    "identity_evidence": {"seat_id": "seat:abc"},
    "assert_property": {"project": "repo:widget", "name": "some_flag", "value": "true"},
}


async def test_project_schema_is_a_well_formed_discriminated_union() -> None:
    schema = await _schema("project")
    jsonschema.Draft7Validator.check_schema(schema)
    assert schema["type"] == "object"
    branches = schema["oneOf"]
    assert len(branches) == 8, "one branch per project action — update this count and " \
        "the ACTION TABLE docstring together if the action set changes"
    actions = {b["properties"]["action"]["const"] for b in branches}
    assert actions == set(PROJECT_VALID_PAYLOADS), (
        "PROJECT_VALID_PAYLOADS has drifted from the schema's own action set")


@pytest.mark.parametrize("action", sorted(PROJECT_VALID_PAYLOADS))
async def test_a_real_client_validates_the_valid_project_call_for_every_action(
    action: str,
) -> None:
    schema = await _schema("project")
    instance = {"action": action, **PROJECT_VALID_PAYLOADS[action]}
    jsonschema.validate(instance=instance, schema=schema)


@pytest.mark.parametrize("action", sorted(PROJECT_VALID_PAYLOADS))
async def test_a_real_client_rejects_that_project_action_missing_its_required_params(
    action: str,
) -> None:
    schema = await _schema("project")
    branch = next(b for b in schema["oneOf"]
                 if b["properties"]["action"]["const"] == action)
    required_beyond_action = [r for r in branch["required"] if r != "action"]
    if not required_beyond_action:
        pytest.skip(f"{action} has no required params beyond action — nothing to omit")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance={"action": action}, schema=schema)


async def test_a_real_client_rejects_an_unknown_project_action() -> None:
    schema = await _schema("project")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance={"action": "not_a_real_action"}, schema=schema)


async def test_a_real_client_rejects_an_unexpected_param_for_a_known_project_action() -> None:
    schema = await _schema("project")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            instance={"action": "retire", "project": "repo:widget", "because": "dead",
                     "this_param_does_not_exist": 1},
            schema=schema)


async def test_a_real_client_rejects_one_project_actions_params_on_another_actions_const() -> None:
    """oneOf discriminates on the `action` const alone — create's own `name` must not
    leak into, say, retire's branch just because both are objects."""
    schema = await _schema("project")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            instance={"action": "retire", "name": "should-not-be-here"}, schema=schema)


# ---------------------------------------------------------------------------------
# composition()
# ---------------------------------------------------------------------------------

COMPOSITION_VALID_PAYLOADS: dict[str, dict[str, Any]] = {
    "save": {"name": "my-lens", "spec": {"subject": "x"}},
    "run": {"name": "my-lens"},
    "list": {},
}


async def test_composition_schema_is_a_well_formed_discriminated_union() -> None:
    schema = await _schema("composition")
    jsonschema.Draft7Validator.check_schema(schema)
    assert schema["type"] == "object"
    branches = schema["oneOf"]
    assert len(branches) == 3, "one branch per composition action — update this count " \
        "and the ACTION TABLE docstring together if the action set changes"
    actions = {b["properties"]["action"]["const"] for b in branches}
    assert actions == set(COMPOSITION_VALID_PAYLOADS), (
        "COMPOSITION_VALID_PAYLOADS has drifted from the schema's own action set")


@pytest.mark.parametrize("action", sorted(COMPOSITION_VALID_PAYLOADS))
async def test_a_real_client_validates_the_valid_composition_call_for_every_action(
    action: str,
) -> None:
    schema = await _schema("composition")
    instance = {"action": action, **COMPOSITION_VALID_PAYLOADS[action]}
    jsonschema.validate(instance=instance, schema=schema)


@pytest.mark.parametrize("action", sorted(COMPOSITION_VALID_PAYLOADS))
async def test_a_real_client_rejects_that_composition_action_missing_its_required_params(
    action: str,
) -> None:
    schema = await _schema("composition")
    branch = next(b for b in schema["oneOf"]
                 if b["properties"]["action"]["const"] == action)
    required_beyond_action = [r for r in branch["required"] if r != "action"]
    if not required_beyond_action:
        pytest.skip(f"{action} has no required params beyond action — nothing to omit")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance={"action": action}, schema=schema)


async def test_a_real_client_rejects_an_unknown_composition_action() -> None:
    schema = await _schema("composition")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance={"action": "not_a_real_action"}, schema=schema)


async def test_a_real_client_rejects_an_unexpected_param_for_a_known_composition_action() -> None:
    schema = await _schema("composition")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            instance={"action": "list", "this_param_does_not_exist": 1}, schema=schema)


async def test_a_real_client_rejects_one_composition_actions_params_on_anothers_const() -> None:
    """oneOf discriminates on the `action` const alone — save's own `spec` must not leak
    into `list`'s branch just because both are objects."""
    schema = await _schema("composition")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            instance={"action": "list", "spec": {"subject": "x"}}, schema=schema)
