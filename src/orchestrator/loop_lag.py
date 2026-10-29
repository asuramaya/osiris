"""Where the event loop stalls: a sampler thread beside the loop.

The loop writes a heartbeat timestamp every `interval_ms`. A plain thread (the only kind of
observer a blocked loop cannot freeze) watches it; when the heartbeat is more than
`threshold_ms` late, the loop is blocked in synchronous code, and the thread samples the
loop thread's own stack every `sample_ms` until it moves again. Samples are aggregated by the
innermost frames, so a stall reads as "N ms blocked in X" rather than a wall of stacks.

Nothing here writes or reads the database. `snapshot()` is what tool_traffic hands back."""
from __future__ import annotations

import asyncio
import sys
import threading
import time
from typing import Any

THRESHOLD_MS = 150
_INTERVAL_MS = 50
_SAMPLE_MS = 20
_FRAMES = 4
_KEEP = 40  # distinct stall sites kept; the rest fold into "other"


def _site(frame: Any) -> str:
    """The innermost `_FRAMES` frames of a stack, leaf first, as file:function:line. Walks
    f_back directly: traceback.extract_stack reads source lines, and that file I/O inside the
    sampler would starve it of the very samples it exists to take."""
    out: list[str] = []
    while frame is not None and len(out) < _FRAMES:
        code = frame.f_code
        name = code.co_filename.split("/site-packages/")[-1].split("/src/")[-1]
        out.append(f"{name}:{code.co_name}:{frame.f_lineno}")
        frame = frame.f_back
    return " < ".join(out)


class LoopLag:
    def __init__(self, threshold_ms: int = THRESHOLD_MS, interval_ms: int = _INTERVAL_MS,
                 sample_ms: int = _SAMPLE_MS) -> None:
        self.threshold = threshold_ms / 1000
        self.interval = interval_ms / 1000
        self.sample = sample_ms / 1000
        self._beat = time.monotonic()
        self._loop_thread: int | None = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        # site -> [samples, blocked_ms (samples * sample_ms)]
        self._sites: dict[str, list[float]] = {}
        self.stalls = 0
        self.stalled_ms = 0.0
        self.worst_ms = 0.0
        self._thread: threading.Thread | None = None
        self._ticker: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Called on the loop's own thread. Idempotent."""
        if self._thread is not None:
            return
        self._loop_thread = threading.get_ident()
        self._beat = time.monotonic()
        self._ticker = asyncio.get_running_loop().create_task(self._tick())
        self._thread = threading.Thread(target=self._watch, name="loop-lag", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._ticker is not None:
            self._ticker.cancel()

    async def _tick(self) -> None:
        while True:
            self._beat = time.monotonic()
            await asyncio.sleep(self.interval)

    def _watch(self) -> None:
        stall_started: float | None = None
        last_sample = 0.0
        while not self._stop.wait(self.sample if stall_started else self.interval / 2):
            late = time.monotonic() - self._beat - self.interval
            if late > self.threshold:
                now = time.monotonic()
                if stall_started is None:
                    stall_started = self._beat + self.interval
                    last_sample = stall_started
                # the sampler thread competes for the GIL with the blocked code, so the
                # real gap between samples, not the nominal one, is the time each stands for
                dt_ms = (now - last_sample) * 1000
                last_sample = now
                frame = sys._current_frames().get(self._loop_thread or -1)
                if frame is not None:
                    site = _site(frame)
                    with self._lock:
                        row = self._sites.setdefault(site, [0, 0.0])
                        row[0] += 1
                        row[1] += dt_ms
            elif stall_started is not None:
                blocked = (time.monotonic() - stall_started) * 1000
                with self._lock:
                    self.stalls += 1
                    self.stalled_ms += blocked
                    self.worst_ms = max(self.worst_ms, blocked)
                    if len(self._sites) > _KEEP:
                        for k in sorted(self._sites, key=lambda s: self._sites[s][1])[:-_KEEP]:
                            del self._sites[k]
                stall_started = None

    def snapshot(self, top: int = 10) -> dict[str, Any]:
        with self._lock:
            ranked = sorted(self._sites.items(), key=lambda kv: -kv[1][1])[:top]
            return {
                "threshold_ms": round(self.threshold * 1000),
                "stalls": self.stalls,
                "stalled_ms": round(self.stalled_ms),
                "worst_ms": round(self.worst_ms),
                "top_sites": [{"blocked_ms": round(v[1]), "samples": int(v[0]), "site": k}
                              for k, v in ranked],
            }


_lag: LoopLag | None = None


def ensure_started() -> LoopLag:
    """The process-wide sampler, started on first use from the loop's own thread."""
    global _lag
    if _lag is None:
        _lag = LoopLag()
        _lag.start()
    return _lag


def snapshot() -> dict[str, Any] | None:
    return _lag.snapshot() if _lag is not None else None
