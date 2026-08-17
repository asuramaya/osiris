"""Thread e6fd3772 piece 2 (measured, live): `phonenumbers.geocoder`/`carrier` each bundle
their own multi-megabyte geodata tables — importing them at src/connectors/phone.py's own
module top-level meant every process that ever imports src.connectors.registry (which
imports phone.py eagerly to populate its CONNECTORS dict) paid that cost just to REGISTER
the connector, never mind call it. Traced live: osiris-mcp's `orient()` -> organ_health ->
monitor.scheduled_jobs()'s lazy `from src.workers.arq_worker import WorkerSettings` drags
in the whole connector registry, so a fresh mcp-server process's first orient() call blocked
15s+ and grew RSS by several hundred MB entirely inside this one import chain.

Needs a SUBPROCESS: this test suite's own earlier tests may have already imported
phonenumbers.geocoder for unrelated reasons, and sys.modules is process-global — only a
fresh interpreter can prove phone.py's own import doesn't drag the heavy submodules in.
"""
from __future__ import annotations

import subprocess
import sys


def test_importing_the_connector_registry_does_not_load_phonenumbers_geodata() -> None:
    out = subprocess.run(
        [sys.executable, "-c",
         "import src.connectors.registry\n"
         "import sys\n"
         "print('geocoder' in sys.modules.get('phonenumbers', object()).__dict__ "
         "if 'phonenumbers' in sys.modules else 'phonenumbers-not-imported')\n"
         "print('phonenumbers.geocoder' in sys.modules)\n"
         "print('phonenumbers.carrier' in sys.modules)\n"],
        capture_output=True, text=True, timeout=30, cwd=".",
    )
    assert out.returncode == 0, out.stderr
    lines = out.stdout.strip().splitlines()
    assert lines[-2] == "False", (
        f"phonenumbers.geocoder loaded merely by importing the registry: {out.stdout}")
    assert lines[-1] == "False", (
        f"phonenumbers.carrier loaded merely by importing the registry: {out.stdout}")


def test_calling_fetch_phone_meta_still_loads_and_works(tmp_path) -> None:
    """The lazy import must still fire, correctly, on an actual call — this is a
    reversibility check, not just an absence check."""
    out = subprocess.run(
        [sys.executable, "-c",
         "import asyncio, uuid\n"
         "from src.connectors.phone import fetch_phone_meta\n"
         "from src.parsers.base import InputObject\n"
         "io = InputObject(id=str(uuid.uuid4()), type='Phone', canonical='+14155552671')\n"
         "meta = asyncio.run(fetch_phone_meta(io))\n"
         "print(meta['valid'])\n"
         "print(meta['country'])\n"],
        capture_output=True, text=True, timeout=30, cwd=".",
    )
    assert out.returncode == 0, out.stderr
    lines = out.stdout.strip().splitlines()
    assert lines[0] == "True"
    assert lines[1] == "US"
