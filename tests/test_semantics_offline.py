"""A search never touches the network, and the semantic leg's numpy work never runs on the
event loop thread. Sockets are blocked outright for the whole request in these tests."""
from __future__ import annotations

import socket
import sys
import threading
import types
from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator import compositions, semantics


@pytest.fixture
def no_network(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Any attempt to open a connection fails the test."""
    attempts: list[str] = []

    def _blocked(self: Any, address: Any) -> None:
        attempts.append(str(address))
        raise AssertionError(f"network call attempted: {address}")

    monkeypatch.setattr(socket.socket, "connect", _blocked)
    return attempts


def _fake_model2vec(monkeypatch: pytest.MonkeyPatch, loads: list[tuple[str, dict[str, Any]]]
                    ) -> None:
    class _Static:
        @classmethod
        def from_pretrained(cls, path: str, **kw: Any) -> _Static:
            loads.append((path, kw))
            return cls()

        def encode(self, texts: list[str]) -> list[Any]:
            import numpy as np

            return [np.ones(4, dtype=np.float32) for _ in texts]

    monkeypatch.setitem(sys.modules, "model2vec", types.SimpleNamespace(StaticModel=_Static))


async def test_the_model_loads_from_the_local_cache_and_never_the_hub(
    monkeypatch: pytest.MonkeyPatch, no_network: list[str], tmp_path: Path,
) -> None:
    loads: list[tuple[str, dict[str, Any]]] = []
    _fake_model2vec(monkeypatch, loads)
    monkeypatch.setattr(semantics, "local_model_dir", lambda model: str(tmp_path))
    emb = semantics.Model2VecEmbedder("acme/tiny")
    assert len(await emb.embed(["one", "two"])) == 2
    await emb.embed(["three"])
    assert loads == [(str(tmp_path), {"force_download": False})]   # once, from the local dir
    assert no_network == []


async def test_a_model_that_is_not_cached_closes_the_route_without_a_network_call(
    monkeypatch: pytest.MonkeyPatch, no_network: list[str],
) -> None:
    loads: list[tuple[str, dict[str, Any]]] = []
    _fake_model2vec(monkeypatch, loads)
    monkeypatch.setattr(semantics, "local_model_dir", lambda model: None)
    emb = semantics.Model2VecEmbedder("acme/tiny")
    with pytest.raises(RuntimeError, match="not in the local cache"):
        await emb.embed(["x"])
    assert loads == [] and no_network == []
    assert "local cache" in (emb.last_error or "")


async def test_a_search_with_an_uncached_model_falls_back_to_the_lexical_legs(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, no_network: list[str],
) -> None:
    monkeypatch.setattr(semantics, "local_model_dir", lambda model: None)
    semantics.set_embedder_for_tests(semantics.Model2VecEmbedder("acme/tiny"))
    try:
        assert await compositions._semantic_hits(actions.pool, "anything at all", 5) == []
    finally:
        semantics.set_embedder_for_tests(None)
    assert no_network == []


async def test_the_semantic_leg_runs_its_numpy_off_the_event_loop_thread(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, no_network: list[str],
) -> None:
    from tests.test_semantics import FakeEmbedder, _decision

    await _decision(actions, "decision:offlineleg1", "wakes reach an idle session by mail")
    fake = FakeEmbedder()
    await semantics.embed_backfill(actions.pool, fake)
    semantics._matrix_cache.clear()
    seen: list[int] = []
    real = semantics._top_k

    def _spy(*a: Any, **kw: Any) -> Any:
        seen.append(threading.get_ident())
        return real(*a, **kw)

    monkeypatch.setattr(semantics, "_top_k", _spy)
    hits = await semantics.semantic_candidates(
        actions.pool, fake, "wakes reach an idle session by mail", floor=-1.0)
    assert hits and seen
    assert threading.get_ident() not in seen      # not on the loop's own thread
    assert no_network == []


async def test_warm_reports_whether_the_route_is_ready_and_never_raises(
    actions: Actions, monkeypatch: pytest.MonkeyPatch, no_network: list[str],
) -> None:
    from tests.test_semantics import FakeEmbedder, _decision

    semantics.set_embedder_for_tests(None)
    assert await semantics.warm(actions.pool) is False          # no embedder: closed
    await _decision(actions, "decision:offlineleg2", "wakes reach an idle session by mail")
    fake = FakeEmbedder()
    await semantics.embed_backfill(actions.pool, fake)
    semantics.set_embedder_for_tests(fake)
    try:
        assert await semantics.warm(actions.pool) is True
        assert "v" in semantics._matrix_cache                    # built before any search
    finally:
        semantics.set_embedder_for_tests(None)
    assert no_network == []


async def test_only_the_background_pass_may_download_the_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """embed_pass fetches a missing Model2Vec model (the one allowed network step); a
    request-path embed never does."""
    from src.workers import arq_worker

    fetched: list[str] = []
    monkeypatch.setattr(semantics, "local_model_dir", lambda model: None)
    monkeypatch.setattr(semantics, "fetch_model", lambda model: fetched.append(model) or "/x")
    semantics.set_embedder_for_tests(semantics.Model2VecEmbedder("acme/tiny"))

    class _Cascade:
        actions = types.SimpleNamespace(pool=None)

    try:
        await arq_worker.embed_pass({"cascade": _Cascade()})
    finally:
        semantics.set_embedder_for_tests(None)
    assert fetched == ["acme/tiny"]
