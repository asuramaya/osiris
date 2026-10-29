"""A project rename receipt names governing workspaces the harness does not trust."""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.orchestrator import workspace_trust
from src.orchestrator.seats import bind_holder, ensure_seat
from src.orchestrator.workspace_trust import hint


async def test_a_project_rename_receipt_names_untrusted_governing_workspaces(
    actions: Actions, tmp_path: Path, monkeypatch: Any,
) -> None:
    from src.orchestrator.project_identity import rename_project

    tree = tmp_path / "newtree"
    tree.mkdir()
    office = tmp_path / "office"
    office.mkdir()
    seat = await ensure_seat(actions, house="osiris", handle="Renametrust",
                             anchor_cwd=str(office), source="test")
    agent = await actions.create_or_find_object("Agent", "agent:7a57ee01", "test")
    assert agent
    await bind_holder(actions, seat_id=seat["seat_id"], agent_id="agent:7a57ee01")
    proj = await actions.create_or_find_object("SoftwareProject", "repo:oldtrustproj", "test")
    await actions.assert_property(proj, "name", "oldtrustproj", "test", datetime.now(UTC),
                                  0.9, evidence_class="self_declared")
    seat_oid = await actions.pool.fetchval(
        "SELECT id FROM objects WHERE canonical=$1", seat["seat_id"])
    await actions.create_link(seat_oid, proj, "governs", "test", datetime.now(UTC), 0.9)
    monkeypatch.setattr(workspace_trust, "load_projects", lambda home=None: {})

    out = await rename_project(actions, project="oldtrustproj", new_name="newtrustproj",
                               because="test", actor="agent:test", dry_run=False)
    note = out["workspace_trust"]
    assert str(office) in note["untrusted"]
    assert hint(str(office)) in note["hints"]
    dry = await rename_project(actions, project="newtrustproj", new_name="newtrustproj2",
                               because="test", actor="agent:test", dry_run=True)
    assert "workspace_trust" in dry
