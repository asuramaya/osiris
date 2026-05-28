from __future__ import annotations

from pathlib import Path

from src.actions.core import Actions
from src.orchestrator.manifests import load_manifests, project_triggers
from src.orchestrator.triggers import matching_helpers

HELPERS_DIR = Path(__file__).parent.parent / "helpers"


def test_load_manifests_validates() -> None:
    manifests = load_manifests(HELPERS_DIR)
    assert "threatfox_malware_iocs" in manifests
    m = manifests["threatfox_malware_iocs"]
    assert m.consumes.type == "Malware"
    assert m.tier == "open"
    assert m.parser == "threatfox_malware_iocs"


async def test_project_triggers_from_manifests(actions: Actions) -> None:
    manifests = load_manifests(HELPERS_DIR)
    n = await project_triggers(actions.pool, manifests)
    assert n == len(manifests)
    row = await actions.pool.fetchrow(
        "SELECT on_event, match, enabled FROM triggers WHERE helper_id='threatfox_malware_iocs'"
    )
    assert row["on_event"] == "object_created"
    assert row["match"]["type"] == "Malware"
    assert row["enabled"] is True


async def test_project_triggers_preserves_enabled_flag(actions: Actions) -> None:
    manifests = load_manifests(HELPERS_DIR)
    await project_triggers(actions.pool, manifests)
    # analyst disables the trigger; re-projection must not re-enable it
    await actions.pool.execute(
        "UPDATE triggers SET enabled=false WHERE helper_id='threatfox_malware_iocs'"
    )
    await project_triggers(actions.pool, manifests)
    assert await actions.pool.fetchval(
        "SELECT enabled FROM triggers WHERE helper_id='threatfox_malware_iocs'"
    ) is False


async def test_matching_helpers(actions: Actions) -> None:
    manifests = load_manifests(HELPERS_DIR)
    await project_triggers(actions.pool, manifests)
    # a Malware object_created matches the ThreatFox helper
    assert "threatfox_malware_iocs" in await matching_helpers(
        actions.pool, "object_created", "Malware"
    )
    # a Domain doesn't
    assert await matching_helpers(actions.pool, "object_created", "Domain") == []
    # disabled triggers don't match
    await actions.pool.execute("UPDATE triggers SET enabled=false")
    assert await matching_helpers(actions.pool, "object_created", "Malware") == []
