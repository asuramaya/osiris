"""The roster shows which seats' workspaces the harness does not trust."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from src.actions.core import Actions
from src.orchestrator import workspace_trust
from src.orchestrator.seats import ensure_seat


async def test_the_roster_marks_a_seat_whose_workspace_is_untrusted(
    actions: Actions, tmp_path: Path, monkeypatch: Any,
) -> None:
    from src.orchestrator.seats import roster

    office = tmp_path / "office"
    office.mkdir()
    await ensure_seat(actions, house="osiris", handle="Untrustedseat",
                      anchor_cwd=str(office), source="test")
    monkeypatch.setattr(workspace_trust, "load_projects", lambda home=None: {})
    row = next(r for r in (await roster(actions.pool))["seats"]
               if r["handle"] == "Untrustedseat")
    assert str(office) in row["workspace_trust"]["untrusted"]
    monkeypatch.setattr(workspace_trust, "load_projects", lambda home=None: {
        str(tmp_path): {"hasTrustDialogAccepted": True}})
    row = next(r for r in (await roster(actions.pool))["seats"]
               if r["handle"] == "Untrustedseat")
    assert row["workspace_trust"]["untrusted"] == []
