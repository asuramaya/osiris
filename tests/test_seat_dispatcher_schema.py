"""seat()'s hand-built discriminated-union inputSchema (task #202, operator ruling
f9182ad7, price-minimizer #1): FastMCP cannot generate a oneOf-per-action schema from a
flat Python signature, so SEAT_INPUT_SCHEMA is authored by hand in src/mcp_server.py and
substituted in at BoundedMCP.list_tools()'s own override seam. The requirement was
explicit: "build the schema by hand and test that a real client validates per action."

`jsonschema.validate` is not a stand-in for a real client — it is the EXACT call the mcp
SDK's own server (mcp/server/lowlevel/server.py) and client (mcp/client/session.py) make
internally against a tool's inputSchema before/after a real call. Testing against it here
is testing the real validation path this server already runs on, not a reimplementation.
"""
from __future__ import annotations

from typing import Any

import jsonschema
import pytest
from src import mcp_server as srv


async def _seat_schema() -> dict[str, Any]:
    live = await srv.mcp.list_tools()
    seat_tool = next(t for t in live if t.name == "seat")
    return seat_tool.inputSchema


async def test_seat_is_live_and_hidden_aliases_are_not() -> None:
    live_names = {t.name for t in await srv.mcp.list_tools()}
    assert "seat" in live_names
    for hidden in (
        "mint_seat", "stop", "walk_in", "pause_seat", "vacate_seat", "rebind_seat",
        "bind_seat_tree", "seat_edge", "charter", "charter_for", "heal_seat_anchor",
        "heal_seat_transcript", "transition_seat_project", "resync_seat_house",
        "sweep_seat_disk", "rename_seat", "set_seat_attended", "reissue_office",
        "establish_office", "invalidate_works_in", "reconcile_seat_identity",
        "correct_house", "correct_pin_value", "revert_own_pin_write",
    ):
        assert hidden not in live_names, f"{hidden} should be hidden, still live"
    # launch/resume/wake/wake_preflight stay named AND dispatch through seat — neither
    # hidden nor removed.
    for stays in ("launch", "resume", "wake", "wake_preflight", "retire_object"):
        assert stays in live_names, f"{stays} should still be live-named"


async def test_seat_schema_is_a_well_formed_discriminated_union() -> None:
    schema = await _seat_schema()
    jsonschema.Draft7Validator.check_schema(schema)  # raises SchemaError if malformed
    assert schema["type"] == "object"
    branches = schema["oneOf"]
    assert len(branches) == 32, "one branch per seat action — update this count and " \
        "the ACTION TABLE docstring together if the action set changes"
    actions = {b["properties"]["action"]["const"] for b in branches}
    assert len(actions) == len(branches), "two branches sharing one action const would " \
        "make oneOf ambiguous — every action must be unique"


# (action, valid_payload_without_action) — one genuinely valid call per action, matching
# what _seat_impl's own pre-dispatch validation would also accept (see
# _SEAT_ACTION_PARAMS in src/mcp_server.py — this table is deliberately independent,
# proving the SCHEMA'S own idea of "valid" against a real validator, not just asserting
# the two tables agree by construction).
VALID_PAYLOADS: dict[str, dict[str, Any]] = {
    "mint": {"handle": "worker-1"},
    "stop": {},
    "walk_in": {"handle": "visitor-1", "wants_office": True},
    "pause": {},
    "vacate": {"target": "seat:abc", "because": "dead holder"},
    "retire": {"target": "seat:abc"},
    "rebind": {"target": "seat:abc", "new_cwd": "/tmp/x"},
    "bind_tree": {"target": "seat:abc", "tree_cwd": "/tmp/x", "because": "reason"},
    "attach": {"target": "seat:worker", "manager": "seat:mgr", "because": "reason"},
    "detach": {"target": "seat:worker", "because": "reason"},
    "charter": {},
    "charter_for": {"target": "seat:abc", "repos": ["osiris"], "because": "reason"},
    "heal_anchor": {},
    "heal_transcript": {"target": "handle", "source_paths": ["/tmp/a.jsonl"]},
    "transition_project": {},
    "resync_house": {"target": "seat:abc", "reason": "reason"},
    "sweep_disk": {"target": "handle"},
    "rename": {"target": "seat:abc", "new_handle": "new", "because": "reason"},
    "set_attended": {"target": "seat:abc", "attended": "human", "because": "reason"},
    "reissue_office": {"target": "seat:abc", "because": "reason"},
    "establish_office": {"target": "seat:abc"},
    "invalidate_works_in": {"stale_project": "old", "because": "reason"},
    "reconcile_identity": {},
    "rehold": {"target": "seat:abc", "agent_id": "agent:xyz", "because": "reason"},
    "correct_house": {"new_house": "newhouse"},
    "correct_pin": {"key": "k", "reason": "reason"},
    "resync_pin": {"target": "seat:abc", "key": "k"},
    "revert_pin": {},
    "launch": {"target": "seat:abc"},
    "resume": {"target": "seat:abc"},
    "wake": {"target": "seat:abc", "message": "hi"},
    "wake_preflight": {"target": "seat:abc"},
}


@pytest.mark.parametrize("action", sorted(VALID_PAYLOADS))
async def test_a_real_client_validates_the_valid_call_for_every_action(action: str) -> None:
    schema = await _seat_schema()
    instance = {"action": action, **VALID_PAYLOADS[action]}
    jsonschema.validate(instance=instance, schema=schema)  # raises on failure


@pytest.mark.parametrize("action", sorted(VALID_PAYLOADS))
async def test_a_real_client_rejects_that_same_action_missing_its_required_params(
    action: str,
) -> None:
    schema = await _seat_schema()
    branch = next(b for b in schema["oneOf"]
                 if b["properties"]["action"]["const"] == action)
    required_beyond_action = [r for r in branch["required"] if r != "action"]
    if not required_beyond_action:
        pytest.skip(f"{action} has no required params beyond action — nothing to omit")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance={"action": action}, schema=schema)


async def test_a_real_client_rejects_an_unknown_action() -> None:
    schema = await _seat_schema()
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance={"action": "not_a_real_action"}, schema=schema)


async def test_a_real_client_rejects_an_unexpected_param_for_a_known_action() -> None:
    schema = await _seat_schema()
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            instance={"action": "vacate", "target": "seat:abc", "because": "reason",
                     "this_param_does_not_exist": 1},
            schema=schema)


async def test_a_real_client_rejects_one_actions_params_on_another_actions_const() -> None:
    """oneOf discriminates on the `action` const alone — mint's own params (handle,
    project) must not leak into, say, vacate's branch just because both are objects."""
    schema = await _seat_schema()
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            instance={"action": "vacate", "handle": "should-not-be-here"}, schema=schema)
