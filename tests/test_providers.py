"""Inference providers — the GPU-as-an-API-key abstraction.

The engine runs no GPU: the text LLM and the vision/OCR model sit behind seams whose
backend is chosen by config (a hosted key, or a local model). Tests cover the factory
resolution + the document→text normalization (incl. the OCR route); live HTTP to a
model is deferred (needs a key), exactly like the extractor.
"""
from __future__ import annotations

import pytest
from src.config.settings import Settings
from src.ingest.providers import (
    AnthropicClient,
    AnthropicVisionClient,
    document_to_text,
    llm_provider,
    vision_provider,
)


def test_llm_provider_resolves_from_config() -> None:
    assert llm_provider(Settings(anthropic_api_key="")) is None  # no key → no provider
    p = llm_provider(Settings(anthropic_api_key="sk-test", osiris_extract_provider="anthropic"))
    assert isinstance(p, AnthropicClient) and p.api_key == "sk-test"
    # a self-hoster who turns the provider off (e.g. local-only) gets None, not a crash
    assert llm_provider(Settings(anthropic_api_key="sk", osiris_extract_provider="none")) is None


def test_vision_provider_resolves_from_config() -> None:
    assert vision_provider(Settings(anthropic_api_key="")) is None
    assert isinstance(vision_provider(Settings(anthropic_api_key="k")), AnthropicVisionClient)


async def test_document_to_text_passes_text_through() -> None:
    assert await document_to_text("already text") == "already text"
    assert await document_to_text(b"bytes text", media_type="text/plain") == "bytes text"
    assert await document_to_text(b'{"a":1}', media_type="application/json") == '{"a":1}'


async def test_scanned_image_routes_through_the_vision_seam() -> None:
    """A scanned notice (image/pdf) is OCR'd via the vision provider before extraction."""

    class _FakeVision:
        def __init__(self) -> None:
            self.seen: tuple[bytes, str] | None = None

        async def to_text(self, *, image: bytes, media_type: str, model: str,
                           instruction: str = "") -> str:
            self.seen = (image, media_type)
            return "OCR'd: 18330 Olive Leaf Dr"

    fv = _FakeVision()
    out = await document_to_text(b"\x89PNG...", media_type="image/png", vision=fv, model="m")
    assert out == "OCR'd: 18330 Olive Leaf Dr"
    assert fv.seen == (b"\x89PNG...", "image/png")


async def test_image_without_a_vision_provider_raises(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.ingest.providers.vision_provider", lambda: None)
    with pytest.raises(RuntimeError, match="vision provider"):
        await document_to_text(b"scan", media_type="image/png")
