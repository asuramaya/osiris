"""thread()'s hand-built discriminated-union inputSchema (task #202, Thoth dispatch
7162, price-minimizer #1) — the fourth object-type dispatcher, absorbing
`thread_action` ITSELF (already a wave-3 action-dispatcher) into the object-type-
dispatcher naming convention. tests/test_dispatcher_schema_contract.py already proves
every _HAND_BUILT_SCHEMAS entry is a well-formed discriminated union, generically; this
file is the per-action REAL-CLIENT proof, the same discipline test_seat_dispatcher_
schema.py and test_project_composition_dispatcher_schema.py already established — a
genuinely independent VALID_PAYLOADS table, checked against jsonschema.validate (the
exact call the mcp SDK's own server/client make internally), not just asserted to agree
with _THREAD_ACTION_PARAMS by construction.
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


async def test_thread_is_live_and_thread_action_is_not() -> None:
    live_names = {t.name for t in await srv.mcp.list_tools()}
    assert "thread" in live_names
    for hidden in (
        "thread_action", "resolve_thread", "annotate_thread",
        "correct_thread_summary", "reclassify_thread",
    ):
        assert hidden not in live_names, f"{hidden} should be hidden, still live"
    # open_thread deliberately stays OUT of this fold and separately named (it MINTS).
    assert "open_thread" in live_names


# (action, valid_payload_without_action) — one genuinely valid call per action,
# deliberately independent of _THREAD_ACTION_PARAMS (src/mcp_server.py) — proving the
# SCHEMA's own idea of "valid" against a real validator, not just asserting the two
# tables agree by construction.
THREAD_VALID_PAYLOADS: dict[str, dict[str, Any]] = {
    "resolve": {"ref": "371fbbac", "because": "done"},
    "annotate": {"ref": "371fbbac", "note": "a progress note"},
    "correct_summary": {"ref": "371fbbac", "corrected_summary": "a better headline"},
    "reclassify": {"ref": "371fbbac", "kind": "obligation"},
}


async def test_thread_schema_is_a_well_formed_discriminated_union() -> None:
    schema = await _schema("thread")
    jsonschema.Draft7Validator.check_schema(schema)
    assert schema["type"] == "object"
    branches = schema["oneOf"]
    assert len(branches) == 4, "one branch per thread action — update this count and " \
        "the ACTION TABLE docstring together if the action set changes"
    actions = {b["properties"]["action"]["const"] for b in branches}
    assert actions == set(THREAD_VALID_PAYLOADS), (
        "THREAD_VALID_PAYLOADS has drifted from the schema's own action set")


async def test_thread_resolve_branch_accepts_a_list_ref_for_batch_mode() -> None:
    """resolve is the ONE action whose `ref` also accepts a list (#203, decision
    880ffe79's batch mode) — every other action's ref stays a plain string."""
    schema = await _schema("thread")
    jsonschema.validate(
        instance={"action": "resolve", "ref": ["371fbbac", "a49d2730"], "because": "batch"},
        schema=schema)


@pytest.mark.parametrize("action", sorted(THREAD_VALID_PAYLOADS))
async def test_a_real_client_validates_the_valid_thread_call_for_every_action(
    action: str,
) -> None:
    schema = await _schema("thread")
    instance = {"action": action, **THREAD_VALID_PAYLOADS[action]}
    jsonschema.validate(instance=instance, schema=schema)


@pytest.mark.parametrize("action", sorted(THREAD_VALID_PAYLOADS))
async def test_a_real_client_rejects_that_thread_action_missing_its_required_params(
    action: str,
) -> None:
    schema = await _schema("thread")
    branch = next(b for b in schema["oneOf"]
                 if b["properties"]["action"]["const"] == action)
    required_beyond_action = [r for r in branch["required"] if r != "action"]
    if not required_beyond_action:
        pytest.skip(f"{action} has no required params beyond action — nothing to omit")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance={"action": action}, schema=schema)


async def test_a_real_client_rejects_an_unknown_thread_action() -> None:
    schema = await _schema("thread")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance={"action": "not_a_real_action"}, schema=schema)


async def test_a_real_client_rejects_an_unexpected_param_for_a_known_thread_action() -> None:
    schema = await _schema("thread")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            instance={"action": "annotate", "ref": "371fbbac", "note": "x",
                     "this_param_does_not_exist": 1},
            schema=schema)


async def test_a_real_client_rejects_one_thread_actions_params_on_anothers_const() -> None:
    """oneOf discriminates on the `action` const alone — reclassify's own `kind` must
    not leak into `annotate`'s branch just because both are objects."""
    schema = await _schema("thread")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            instance={"action": "annotate", "ref": "371fbbac", "kind": "obligation"},
            schema=schema)
