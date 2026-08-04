#!/usr/bin/env python3
"""Prints the exact current tool-contract char total (task #129's ratchet) — the one
correct source for TOOL_CONTRACT_CEILING_CHARS in tests/test_tool_contract_diet.py.
Never guess a raise (never take the larger of two conflicting branch values, never do
arithmetic by hand): run this against the tree you're about to commit and paste its
output. Reused by scripts/reconcile_tool_contract_ceiling.py's own default
--measure-cmd (dispatch 26686b77, Thoth msg 3658) — one measurement, two callers,
never two implementations that could silently diverge.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


async def _main() -> int:
    from tests.test_tool_contract_diet import _measure_tool_contract

    total, _ = await _measure_tool_contract()
    print(total)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
