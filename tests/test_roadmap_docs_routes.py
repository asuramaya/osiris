"""/roadmap /canon — the routes feeding chrome's pure renderers off the live graph
(thread 521ae613a6f4). One end-to-end test per route: the pure-renderer fixture tests in
test_chrome.py cover layout; this covers the wiring (Query aliasing, pool injection,
project defaulting) those can't. The "docs" nav tab routes at /canon, not /docs — FastAPI
reserves /docs for its own Swagger UI (a second route at the same path is silently
shadowed by it — this file's own first draft caught that live, hitting the swagger page
instead of the renderer)."""
from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app
from src.ingest.reference import ingest_reference_doc
from src.orchestrator.capture import open_thread

NOW = datetime(2026, 7, 25, tzinfo=UTC)


@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(actions.pool)
    app.state.pool = actions.pool
    app.state.manifests = {}
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_roadmap_route_defaults_to_osiris_and_switches_on_p(
    actions: Actions, client: httpx.AsyncClient,
) -> None:
    proj = await actions.create_or_find_object("SoftwareProject", "repo:osiris", "test")
    await actions.assert_property(proj, "name", "osiris", "test", NOW, 0.9,
                                  evidence_class="self_declared")
    await open_thread(actions, "a real duty on the default project", repo="osiris",
                      kind="obligation", source="agent:me")

    default = await client.get("/roadmap")
    assert default.status_code == 200
    assert "a real duty on the default project" in default.text
    assert "roadmap" in default.text

    unknown = await client.get("/roadmap?p=nobody-here")
    assert unknown.status_code == 200
    assert "no such project" in unknown.text

    partial = await client.get("/roadmap?partial=1")
    assert "<!doctype" not in partial.text.lower()  # partial: content div only, no shell


async def test_docs_route_serves_the_ingested_canon(
    actions: Actions, client: httpx.AsyncClient, tmp_path: object,
) -> None:
    from pathlib import Path

    doc = Path(str(tmp_path)) / "SOME-DOC.md"
    doc.write_text("<!-- topic: concepts -->\n\n# Some Doc\n\nBody.\n")
    await ingest_reference_doc(actions, str(doc))

    r = await client.get("/canon?p=osiris")
    assert r.status_code == 200
    assert "Some Doc" in r.text
    assert "concepts" in r.text


async def test_docs_route_is_not_shadowed_by_fastapis_own_swagger_docs(
    client: httpx.AsyncClient,
) -> None:
    """The regression this whole file exists to guard: FastAPI auto-registers /docs for its
    own Swagger UI, and a second app route at that exact path is silently shadowed by it —
    caught live when the first draft of this test hit the swagger page instead of the
    renderer. The "docs" nav tab must never move back to /docs without also disabling or
    relocating FastAPI's default docs_url."""
    swagger = await client.get("/docs")
    assert "swagger-ui" in swagger.text.lower()
    canon = await client.get("/canon")
    assert "swagger-ui" not in canon.text.lower()
