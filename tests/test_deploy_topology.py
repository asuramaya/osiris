"""Phase 7 — the hosting cut, as a verifiable topology.

These aren't integration tests (the live multi-process bring-up is deploy/up.sh); they
guard the deployment MANIFEST from rotting: the full compose must declare the rings as
separate units, the surfaces must share one PG+Redis bus, and migrations must gate the
app start. A drift here is how a deploy silently breaks.
"""
from __future__ import annotations

from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parent.parent
_FULL = _ROOT / "deploy" / "docker-compose.full.yml"


def _load() -> dict:
    return yaml.safe_load(_FULL.read_text())


def test_full_topology_declares_the_rings() -> None:
    services = _load()["services"]
    # infra + one-shot migrate + the two long-running surfaces, all separate units
    assert {"postgres", "redis", "migrate", "api", "worker"} <= set(services)


def test_surfaces_share_one_pg_and_redis_bus() -> None:
    services = _load()["services"]
    for name in ("api", "worker", "migrate"):
        env = services[name]["environment"]
        assert env["DATABASE_URL"].endswith("@postgres:5432/osiris")
        assert env["REDIS_URL"] == "redis://redis:6379/0"


def test_app_start_is_gated_on_migrations() -> None:
    services = _load()["services"]
    assert services["migrate"]["command"][:3] == ["uv", "run", "alembic"]
    for name in ("api", "worker"):
        dep = services[name]["depends_on"]["migrate"]
        assert dep["condition"] == "service_completed_successfully"


def test_worker_and_api_are_distinct_processes() -> None:
    services = _load()["services"]
    # the API uses the image default (uvicorn); the worker overrides command to arq
    assert "command" not in services["api"]  # image CMD = uvicorn
    assert services["worker"]["command"][:3] == ["uv", "run", "arq"]


def test_satellite_is_opt_in_profile() -> None:
    # the placeful satellite is not part of the default up (it runs at a vantage)
    assert _load()["services"]["satellite"]["profiles"] == ["satellite"]


def test_bringup_scripts_are_executable_and_present() -> None:
    for name in ("up.sh", "down.sh"):
        assert (_ROOT / "deploy" / name).exists()
