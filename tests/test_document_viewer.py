"""The document-viewer primitive — a node's renderable CONTENT. A Reference/doc returns its
markdown body; an object with no body returns kind=none; a missing object 404s. (The Commit
git-show DIFF path needs a real repo, so it's proven live, not here.)
"""
from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import pytest_asyncio
from src.actions.core import Actions
from src.api.app import create_app

NOW = datetime(2026, 6, 28, tzinfo=UTC)


@pytest_asyncio.fixture
async def client(actions: Actions) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(actions.pool)
    app.state.pool = actions.pool
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_content_renders_a_docs_markdown(actions: Actions, client: httpx.AsyncClient) -> None:
    ref = await actions.create_or_find_object("Reference", "ref:demo", "ref:osiris")
    await actions.assert_property(ref, "name", "Demo Doc", "ref:osiris", NOW, 0.6)
    await actions.assert_property(ref, "body", "# Title\n\nHello **world**.",
                                  "ref:osiris", NOW, 0.6)
    d = (await client.get(f"/objects/{ref}/content")).json()
    assert d["kind"] == "markdown"
    assert d["title"] == "Demo Doc" and "Hello" in d["content"]


async def test_content_is_none_without_body(actions: Actions, client: httpx.AsyncClient) -> None:
    obj = await actions.create_or_find_object("Organization", "cik:99", "edgar")
    d = (await client.get(f"/objects/{obj}/content")).json()
    assert d["kind"] == "none"


async def test_content_404_for_missing(client: httpx.AsyncClient) -> None:
    assert (await client.get(f"/objects/{uuid.uuid4()}/content")).status_code == 404
