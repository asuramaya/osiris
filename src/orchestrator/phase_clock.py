"""Wall time per named phase of one call, for finding where a slow tool spends it.

    clock = PhaseClock()
    ...first phase...
    clock.mark("identity")        # time since the start (or the previous mark)
    ...second phase...
    clock.mark("seat_binding")
    clock.phases_ms()             # {"identity": 41.2, "seat_binding": 3.1}

A mark only reads a monotonic clock and appends to a list, so it is safe on a hot path.
"""
from __future__ import annotations

import time


class PhaseClock:
    def __init__(self) -> None:
        self._start = self._last = time.perf_counter()
        self._marks: list[tuple[str, float]] = []

    def mark(self, name: str) -> None:
        """Close the phase that ran since the previous mark, naming it `name`."""
        now = time.perf_counter()
        self._marks.append((name, now - self._last))
        self._last = now

    def phases_ms(self) -> dict[str, float]:
        """Wall milliseconds per phase, in order; a name marked twice adds up."""
        out: dict[str, float] = {}
        for name, secs in self._marks:
            out[name] = round(out.get(name, 0.0) + secs * 1000.0, 1)
        return out

    def total_ms(self) -> float:
        return round((self._last - self._start) * 1000.0, 1)

    def line(self) -> str:
        """`total=123.4ms identity=41.2 seat_binding=3.1`, slowest phase first."""
        ranked = sorted(self.phases_ms().items(), key=lambda kv: -kv[1])
        return f"total={self.total_ms()}ms " + " ".join(f"{k}={v}" for k, v in ranked)
