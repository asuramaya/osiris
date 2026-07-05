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

import asyncio
import base64
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from src.config.settings import Settings, get_settings

_ANTHROPIC = "https://api.anthropic.com"
_VERSION = "2023-06-01"


@dataclass
class Usage:
    """What one completion cost. `cost_usd` comes free from the CLI envelope; None on the API
    path (tokens only). cache_* split cheap/discounted input from fresh input."""

    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_creation_tokens: int = 0
    cost_usd: float | None = None
    duration_ms: int | None = None


def _usage(data: dict[str, Any], model: str, *, with_cost: bool) -> Usage:
    """Read usage from an Anthropic/CLI response envelope (both nest it under 'usage' with the
    same token keys). cost_usd + duration_ms are the CLI envelope's extras (with_cost=True)."""
    u = data.get("usage")
    u = u if isinstance(u, dict) else {}
    cost = data.get("total_cost_usd") if with_cost else None
    dur = data.get("duration_ms") if with_cost else None
    return Usage(
        model=model,
        input_tokens=int(u.get("input_tokens") or 0),
        output_tokens=int(u.get("output_tokens") or 0),
        cache_read_tokens=int(u.get("cache_read_input_tokens") or 0),
        cache_creation_tokens=int(u.get("cache_creation_input_tokens") or 0),
        cost_usd=float(cost) if isinstance(cost, (int, float)) else None,
        duration_ms=int(dur) if isinstance(dur, (int, float)) else None,
    )


class LLMClient(Protocol):
    """A document-grounded text completion returning JSON/text."""

    async def complete(
        self, *, system: str, prompt: str, model: str, max_tokens: int = 2048,
        usage_out: list[Usage] | None = None,
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
        self, *, system: str, prompt: str, model: str, max_tokens: int = 2048,
        usage_out: list[Usage] | None = None,
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
            if usage_out is not None:
                usage_out.append(_usage(data, model, with_cost=False))
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


def _cli_result(stdout: bytes) -> str:
    """Pull the completion text out of `claude -p --output-format json`'s envelope."""
    data = json.loads(stdout.decode() or "{}")
    if data.get("is_error"):
        raise RuntimeError(f"claude CLI error: {str(data.get('result', ''))[:200]}")
    return str(data.get("result", ""))


@dataclass
class ClaudeCliClient:
    """Local text LLM via the installed `claude` CLI (Claude Code) in headless -p/--print mode.

    Uses the box's OWN Claude Code auth (subscription / OAuth) — NO API key embedded. This is
    the right backend for the CORE box where Claude Code is installed: the always-on worker
    borrows the local Claude instance for per-document extraction, and API keys are reserved
    for SATELLITES / remote deployments that have no CLI. `--system-prompt` replaces the heavy
    default Code prompt, so each extraction call stays lean.
    """

    binary: str = "claude"

    async def complete(
        self, *, system: str, prompt: str, model: str, max_tokens: int = 2048,
        usage_out: list[Usage] | None = None,
    ) -> str:
        # Context isolation: without it, a `claude -p` run inside a repo inherits the
        # project's CLAUDE.md + settings — ~52k tokens of the project's own opinions
        # pre-loaded into what is supposed to be a NEUTRAL extraction call (measured live:
        # --setting-sources "" cuts cache-creation 51,900 → 7,362 tokens). The neutral cwd
        # is belt-and-braces for the same leak. An extractor must read its input with
        # nothing but its instructions.
        # The cwd is a DEDICATED dir, not bare /tmp: each -p call writes its own session
        # transcript under ~/.claude/projects/<cwd-slug>/, and the session-miner senses
        # that tree — a recognizable slug lets it exclude the extractor's own transcripts
        # (an instrument reading itself is the loop-pathology class).
        workdir = os.path.join(tempfile.gettempdir(), "osiris-extract")
        os.makedirs(workdir, exist_ok=True)
        proc = await asyncio.create_subprocess_exec(
            self.binary, "-p", prompt, "--model", model, "--system-prompt", system,
            "--output-format", "json", "--setting-sources", "",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            cwd=workdir,
        )
        out, err = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"claude CLI exit {proc.returncode}: {err.decode()[:200]}")
        text = _cli_result(out)
        if usage_out is not None:  # the CLI envelope carries usage AND the real cost_usd
            usage_out.append(_usage(json.loads(out.decode() or "{}"), model, with_cost=True))
        return text


# --- factories: resolve the backend from config (the deployment switch) -----

def llm_provider(settings: Settings | None = None) -> LLMClient | None:
    """The configured text LLM, or None if no backend is wired (extraction then needs an
    explicitly-injected client, or is skipped). Keeps a keyless run from crashing.

    `auto` (default) = prefer the local Claude CLI if installed, else an API key; `claude-cli`
    = force the local install (no key, core box); `anthropic` = force a key (satellites/remote)."""
    s = settings or get_settings()
    p = s.osiris_extract_provider
    if p in ("claude-cli", "auto") and shutil.which(s.osiris_claude_binary):
        return ClaudeCliClient(s.osiris_claude_binary)
    if p in ("anthropic", "auto") and s.anthropic_api_key:
        return AnthropicClient(s.anthropic_api_key)
    return None


def vision_provider(settings: Settings | None = None) -> VisionClient | None:
    """The configured vision/OCR backend, or None. OCR (scanned pages) is the keyed path —
    the local CLI doesn't do image input here — so 'auto'/'anthropic' both need a key."""
    s = settings or get_settings()
    if s.osiris_extract_provider in ("anthropic", "auto") and s.anthropic_api_key:
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
