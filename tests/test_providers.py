"""Inference providers — the GPU-as-an-API-key abstraction.

The engine runs no GPU: the text LLM and the vision/OCR model sit behind seams whose
backend is chosen by config (a hosted key, or a local model). Tests cover the factory
resolution + the document→text normalization (incl. the OCR route); live HTTP to a
model is deferred (needs a key), exactly like the extractor.
"""
from __future__ import annotations

import asyncio
import stat
from pathlib import Path
from typing import Any

import pytest
from src.config.settings import Settings
from src.ingest.providers import (
    AnthropicClient,
    AnthropicVisionClient,
    ClaudeCliClient,
    document_to_text,
    llm_provider,
    vision_provider,
)


def test_llm_provider_resolves_from_config(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.ingest.providers as prov
    monkeypatch.setattr(prov.shutil, "which", lambda _: None)  # no local claude CLI here
    assert llm_provider(Settings(anthropic_api_key="",
                                 osiris_extract_provider="anthropic")) is None  # no key
    p = llm_provider(Settings(anthropic_api_key="sk-test", osiris_extract_provider="anthropic"))
    assert isinstance(p, AnthropicClient) and p.api_key == "sk-test"
    # turned off (local-only with no backend wired) → None, not a crash
    assert llm_provider(Settings(anthropic_api_key="sk", osiris_extract_provider="none")) is None
    # explicit claude-cli backend → the local CLI when it's installed (subscription, no key)
    monkeypatch.setattr(prov.shutil, "which", lambda _: "/usr/bin/cc")
    cli = llm_provider(Settings(osiris_extract_provider="claude-cli", osiris_claude_binary="cc"))
    assert isinstance(cli, ClaudeCliClient) and cli.binary == "cc"


def test_auto_prefers_local_cli_then_the_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """'auto' (the default) is the deployment story: the core box has the CLI → keyless local
    Claude; a satellite has no CLI but a key → the API; neither → None (no crash)."""
    import src.ingest.providers as prov
    monkeypatch.setattr(prov.shutil, "which", lambda _: "/usr/bin/claude")
    assert isinstance(llm_provider(Settings(osiris_extract_provider="auto")), ClaudeCliClient)
    monkeypatch.setattr(prov.shutil, "which", lambda _: None)
    assert isinstance(llm_provider(Settings(osiris_extract_provider="auto",
                                            anthropic_api_key="k")), AnthropicClient)
    assert llm_provider(Settings(osiris_extract_provider="auto")) is None


async def test_claude_cli_client_spawns_and_parses(tmp_path: Path) -> None:
    """The CLI backend shells out to `claude -p … --output-format json` and pulls `.result`
    out of the envelope — proven against a fake binary so CI needs no real claude install."""
    fake = tmp_path / "claude"
    fake.write_text('#!/bin/sh\necho \'{"result":"Acme Corp","is_error":false}\'\n')
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    client = ClaudeCliClient(binary=str(fake))
    out = await client.complete(system="extract", prompt="a doc", model="haiku")
    assert out == "Acme Corp"


async def test_claude_cli_client_raises_on_error(tmp_path: Path) -> None:
    fake = tmp_path / "claude"
    fake.write_text('#!/bin/sh\necho \'{"result":"rate limited","is_error":true}\'\n')
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    with pytest.raises(RuntimeError, match="claude CLI error"):
        await ClaudeCliClient(binary=str(fake)).complete(system="s", prompt="p", model="m")


def _hangs(tmp_path: Path) -> Path:
    """A `claude` that never returns — the shape of a wedged CLI call."""
    fake = tmp_path / "claude"
    fake.write_text("#!/bin/sh\nsleep 30\n")
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    return fake


def _spy(monkeypatch: pytest.MonkeyPatch) -> tuple[list[Any], asyncio.Event]:
    """Capture the Process objects the client spawns, so a test can ask: did it DIE?

    The Event fires the moment a child is out the door — a test waits on THAT, never on a
    sleep, so it cancels at the exact instant the hand is extended."""
    import src.ingest.providers as prov
    spawned: list[Any] = []
    out_the_door = asyncio.Event()
    real = prov.asyncio.create_subprocess_exec

    async def watched(*a: Any, **k: Any) -> Any:
        proc = await real(*a, **k)
        spawned.append(proc)
        out_the_door.set()
        return proc

    monkeypatch.setattr(prov.asyncio, "create_subprocess_exec", watched)
    return spawned, out_the_door


async def test_a_timed_out_extractor_is_KILLED_not_abandoned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE LEAK THAT WEDGED THE WORKER. `await proc.communicate()` had no timeout: a hung CLI
    call held an arq slot AND 290MB forever. Now the call dies — and takes its child with it."""
    spawned, _ = _spy(monkeypatch)
    client = ClaudeCliClient(binary=str(_hangs(tmp_path)), timeout=0.3)
    with pytest.raises(TimeoutError):
        await client.complete(system="s", prompt="p", model="m")
    assert len(spawned) == 1
    assert spawned[0].returncode is not None, "the extractor outlived the call that spawned it"


async def test_a_CANCELLED_extractor_is_KILLED_not_abandoned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The one that actually happened: arq's timeout CANCELS the tick, and CancelledError is not
    an Exception. The old code caught nothing, so the `claude -p` kept running — and kept
    BILLING — long after the job that owned it was dead. Osiris must be able to close its hand."""
    spawned, out_the_door = _spy(monkeypatch)
    client = ClaudeCliClient(binary=str(_hangs(tmp_path)), timeout=30)
    task = asyncio.create_task(client.complete(system="s", prompt="p", model="m"))
    await out_the_door.wait()               # the child is running; NOW pull the rug
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert spawned[0].returncode is not None, "a cancelled call abandoned a live, billing child"


async def test_only_ONE_extractor_is_ever_alive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """10 × 290MB into a 2G cgroup is not a race condition, it is arithmetic. arq will happily
    run ten jobs at once; the gate is what stops ten subprocesses from existing at once."""
    import src.ingest.providers as prov
    monkeypatch.setattr(prov, "_CLI_GATE", asyncio.Semaphore(1))  # fresh gate per test
    log = tmp_path / "log"
    fake = tmp_path / "claude"
    fake.write_text(f'#!/bin/sh\necho + >> {log}\nsleep 0.15\necho - >> {log}\n'
                    f'echo \'{{"result":"ok","is_error":false}}\'\n')
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    client = ClaudeCliClient(binary=str(fake))
    await asyncio.gather(*(client.complete(system="s", prompt=str(i), model="m")
                           for i in range(3)))
    marks = log.read_text().split()
    assert marks == ["+", "-", "+", "-", "+", "-"], f"extractors overlapped: {marks}"


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
