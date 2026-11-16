"""The console's two reads and runs over the NAS first offload (src/ui/static/athome.js).

`stream_nas` runs `nas_first_offload.command` and turns each line it says into one JSON object on
its own line (newline-delimited JSON), then a closing {"done": true, "ok": ..., "text": ...}. The
page shows them as they arrive, so the six steps appear one by one the way they do in a terminal.
`restore_test_summary` turns the restore-test receipt for the NAS target into what the page
shows."""
from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

_STEP = re.compile(r"^\[(\d+)/(\d+)\]\s+(STOPPED:\s*)?(.*)$")


def parse_step_line(line: str) -> dict[str, Any]:
    """One line the first offload said, as an object. `[3/6] text` is step 3 done; `[3/6] STOPPED:
    why` is step 3 failed; an indented `next:` line is the advice that follows a stop; anything
    else (the header, the closing `done:` sentence) is plain information."""
    text = line.rstrip("\n")
    m = _STEP.match(text.strip())
    if m:
        return {"n": int(m.group(1)), "of": int(m.group(2)),
                "status": "failed" if m.group(3) else "ok", "text": m.group(4).strip()}
    return {"status": "info", "text": text.strip()}


async def stream_nas(
    runner: Callable[[Callable[[str], None]], Awaitable[int]],
) -> AsyncIterator[str]:
    """Run `runner(say)` and yield one JSON line per thing it says, then the closing line. A
    failure inside the runner is reported as a closing line, never a traceback."""
    queue: asyncio.Queue[str | None] = asyncio.Queue()

    def say(text: str) -> None:
        queue.put_nowait(text)

    async def go() -> int:
        try:
            return await runner(say)
        except Exception as exc:  # noqa: BLE001 - the page gets a sentence, not a stack
            say(f"This stopped before it finished: {type(exc).__name__}.")
            return 1
        finally:
            queue.put_nowait(None)

    task = asyncio.create_task(go())
    last = ""
    while True:
        item = await queue.get()
        if item is None:
            break
        obj = parse_step_line(item)
        if obj["text"]:
            last = obj["text"]
        yield json.dumps(obj) + "\n"
    rc = await task
    yield json.dumps({"done": True, "ok": rc == 0, "text": last}) + "\n"


def restore_test_summary(
    target: dict[str, Any] | None, receipt: dict[str, Any],
) -> dict[str, Any]:
    """The restore-test result for the NAS target: `ok` is True when the last attempt passed,
    False when it failed, None when it has never been run (or there is no NAS target)."""
    if target is None:
        return {"ok": None, "when": None, "text": "No NAS target is set up yet.", "steps": []}
    attempted, passed = receipt.get("last_attempt_at"), receipt.get("last_passed_at")
    error = receipt.get("last_error")
    if not attempted and not passed:
        return {"ok": None, "when": None, "steps": [],
                "text": "The restore test has not been run against the NAS yet."}
    ok = bool(passed) and not error
    when = passed if ok else attempted
    if ok:
        text = "A copy was restored from the NAS and checked."
    else:
        text = f"The restore test failed: {error}" if error else "The restore test did not finish."
    note = receipt.get("last_note")
    return {"ok": ok, "when": when, "text": text, "snapshot": None, "files_checked": None,
            "steps": ([{"text": str(note), "ok": True}] if note else [])}
