"""Inference providers — the GPU-as-an-API-key abstraction.

The engine NEVER runs a GPU. Document extraction needs two model capabilities:
  * a text **LLM** — entities/relationships out of text;
  * a **vision** model — OCR a scanned notice / PDF page into text first (county
    foreclosure notices are images, not clean text).

Both sit behind an injected seam (`LLMClient`, `VisionClient`). Config picks the
backend: a hosted API (an API key, no ops) for the managed broker deployment, or a
local model for a self-hoster with their own GPU — same code. "Whose GPU / which
model" becomes a deployment switch, not a rewrite. Live providers use httpx directly
(no SDK dependency); tests inject fakes.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Protocol

import httpx

from src.config.settings import Settings, get_settings

_ANTHROPIC = "https://api.anthropic.com"
_VERSION = "2023-06-01"


class LLMClient(Protocol):
    """A document-grounded text completion returning JSON/text."""

    async def complete(
        self, *, system: str, prompt: str, model: str, max_tokens: int = 2048
    ) -> str: ...


class VisionClient(Protocol):
    """Turn a document page IMAGE into text (OCR / VLM) before extraction."""

    async def to_text(
        self, *, image: bytes, media_type: str, model: str, instruction: str = ...
    ) -> str: ...


@dataclass
class AnthropicClient:
    """Live text LLM over the Anthropic Messages API via httpx (no SDK)."""

    api_key: str
    base_url: str = _ANTHROPIC

    async def complete(
        self, *, system: str, prompt: str, model: str, max_tokens: int = 2048
    ) -> str:
        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.post(
                f"{self.base_url}/v1/messages",
                headers={"x-api-key": self.api_key, "anthropic-version": _VERSION,
                         "content-type": "application/json"},
                json={"model": model, "max_tokens": max_tokens, "system": system,
                      "messages": [{"role": "user", "content": prompt}]},
            )
            r.raise_for_status()
            data = r.json()
            return "".join(
                b.get("text", "") for b in data.get("content", []) if b.get("type") == "text"
            )


_OCR_INSTRUCTION = (
    "Transcribe ALL text in this document image exactly, preserving names, addresses, "
    "dates, and amounts. Output only the transcribed text — no commentary."
)


@dataclass
class AnthropicVisionClient:
    """Live vision/OCR over the Anthropic Messages API (an image content block)."""

    api_key: str
    base_url: str = _ANTHROPIC

    async def to_text(
        self, *, image: bytes, media_type: str, model: str, instruction: str = _OCR_INSTRUCTION
    ) -> str:
        b64 = base64.standard_b64encode(image).decode("ascii")
        async with httpx.AsyncClient(timeout=120.0) as client:
            r = await client.post(
                f"{self.base_url}/v1/messages",
                headers={"x-api-key": self.api_key, "anthropic-version": _VERSION,
                         "content-type": "application/json"},
                json={"model": model, "max_tokens": 4096, "messages": [{"role": "user",
                      "content": [
                          {"type": "image", "source": {"type": "base64",
                           "media_type": media_type, "data": b64}},
                          {"type": "text", "text": instruction}]}]},
            )
            r.raise_for_status()
            data = r.json()
            return "".join(
                b.get("text", "") for b in data.get("content", []) if b.get("type") == "text"
            )


# --- factories: resolve the backend from config (the deployment switch) -----

def llm_provider(settings: Settings | None = None) -> LLMClient | None:
    """The configured text LLM, or None if no backend is wired (extraction then needs an
    explicitly-injected client, or is skipped). Keeps a keyless run from crashing."""
    s = settings or get_settings()
    if s.osiris_extract_provider == "anthropic" and s.anthropic_api_key:
        return AnthropicClient(s.anthropic_api_key)
    return None


def vision_provider(settings: Settings | None = None) -> VisionClient | None:
    """The configured vision/OCR backend, or None."""
    s = settings or get_settings()
    if s.osiris_extract_provider == "anthropic" and s.anthropic_api_key:
        return AnthropicVisionClient(s.anthropic_api_key)
    return None


_TEXTUAL = ("text/", "application/json")


async def document_to_text(
    content: bytes | str,
    *,
    media_type: str = "text/plain",
    vision: VisionClient | None = None,
    model: str | None = None,
) -> str:
    """Normalize a fetched document to text for the extractor. Plain text passes
    through; a scanned page (image/*, application/pdf) is OCR'd via the vision provider.
    The OCR seam is the same key-as-GPU abstraction — no local inference."""
    if isinstance(content, str):
        return content
    if any(media_type.startswith(t) for t in _TEXTUAL):
        return content.decode("utf-8", "replace")
    v = vision or vision_provider()
    if v is None:
        raise RuntimeError(
            f"no vision provider for {media_type!r} — set ANTHROPIC_API_KEY (the OCR seam)"
        )
    return await v.to_text(
        image=content, media_type=media_type, model=model or get_settings().osiris_vision_model
    )
