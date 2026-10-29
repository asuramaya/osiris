"""The /readiness route feeds the workspace-trust step from the live roster."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
from src.actions.core import Actions
from src.api.app import create_app
from src.orchestrator import workspace_trust
from src.orchestrator.manifests import load_manifests
from src.orchestrator.seats import ensure_seat


async def test_readiness_route_lists_the_untrusted_seat_workspace(
    actions: Actions, tmp_path: Path, monkeypatch: Any,
) -> None:
    office = tmp_path / "office"
    office.mkdir()
    await ensure_seat(actions, house="osiris", handle="Readytrust", anchor_cwd=str(office),
                      source="test")
    monkeypatch.setattr(workspace_trust, "load_projects", lambda home=None: {})
    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = load_manifests(Path(__file__).parent.parent / "helpers")

    async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        body = (await client.get("/readiness")).json()

    step = next(s for s in body["steps"] if s["key"] == "workspace_trust")
    assert step["status"] == "needs_attention"
    assert f"workspace not trusted: run claude once in {office}" in step["reason"]
