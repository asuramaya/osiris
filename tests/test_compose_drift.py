"""COMPOSE DRIFT (Thoth mail 9122 item 2, wave 16): deploy/up.sh, docker-compose.yml,
deploy/docker-compose.full.yml must match the live osiris-pg — same image, the SAME
named volume the real container actually runs on (osiris-pg-data, confirmed live via
`docker inspect osiris-pg` — not a compose-project-prefixed anonymous one, which
`docker volume ls` shows sitting as dead weight today, osiris_pgdata/osiris_redisdata,
from exactly this mistake once already made), and WAL archiving (archive_mode/
archive_command) — the structural fact nothing else rederives, so a recreate that drops
it fails silently until a restore is actually attempted.

TWO LAYERS: static checks (no docker needed, run every gate) confirm the three deploy
artifacts declare the right structural facts and never re-hardcode the tuning flags
scripts/osiris_pg_autotune.py now owns; a live check (skipped when docker or a real
osiris-pg container isn't present) diffs those static declarations against
`docker inspect`/postgresql.auto.conf on the ACTUAL running container — the real "does
the file match the world" proof, not just internal self-consistency.
"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
UP_SH = (REPO_ROOT / "deploy" / "up.sh").read_text()
# comments (this file's own explanatory prose freely NAMES the flags it removed, e.g.
# "shared_buffers" and "--rm", as part of documenting the fix) must never satisfy a
# presence/absence check meant to look at the actual docker-run invocation.
UP_SH_CODE = "\n".join(
    line for line in UP_SH.splitlines() if not line.strip().startswith("#"))
COMPOSE_YML = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())
COMPOSE_FULL_YML = yaml.safe_load((REPO_ROOT / "deploy" / "docker-compose.full.yml").read_text())

_REAL_PG_VOLUME = "osiris-pg-data"
_REAL_REDIS_VOLUME = "osiris-redis-data"
_STALE_TUNING_FLAGS = (
    "shared_buffers", "effective_cache_size", "work_mem", "maintenance_work_mem",
)


def _pg_service(compose: dict) -> dict:
    return compose["services"]["postgres"]


def _pg_command_text(compose: dict) -> str:
    cmd = _pg_service(compose).get("command") or []
    return " ".join(cmd)


# --- static: the two compose files ------------------------------------------------


@pytest.mark.parametrize("compose", [COMPOSE_YML, COMPOSE_FULL_YML], ids=["yml", "full_yml"])
def test_postgres_volume_is_pinned_to_the_real_live_volume_name(compose: dict) -> None:
    volumes = compose["volumes"]
    assert volumes["pgdata"]["name"] == _REAL_PG_VOLUME
    assert volumes["redisdata"]["name"] == _REAL_REDIS_VOLUME


@pytest.mark.parametrize("compose", [COMPOSE_YML, COMPOSE_FULL_YML], ids=["yml", "full_yml"])
def test_postgres_archives_wal(compose: dict) -> None:
    cmd = _pg_command_text(compose)
    assert "archive_mode=on" in cmd
    assert "archive_command=" in cmd
    assert "osiris_archive_wal.sh" in cmd
    assert "wal_level=replica" in cmd


@pytest.mark.parametrize("compose", [COMPOSE_YML, COMPOSE_FULL_YML], ids=["yml", "full_yml"])
def test_the_archive_script_is_bind_mounted_from_the_repo_not_baked_in(compose: dict) -> None:
    volumes = _pg_service(compose)["volumes"]
    mounts = [v if isinstance(v, str) else f"{v}" for v in volumes]
    assert any("osiris_archive_wal.sh" in m and m.endswith(":ro") for m in mounts), (
        "the archive script must be a read-only bind mount from THIS repo checkout, "
        "never a stale manually-copied file nobody remembers placing")


@pytest.mark.parametrize("compose", [COMPOSE_YML, COMPOSE_FULL_YML], ids=["yml", "full_yml"])
def test_postgres_never_hardcodes_the_tuning_flags_autotune_owns(compose: dict) -> None:
    """scripts/osiris_pg_autotune.py derives shared_buffers/work_mem/effective_cache_
    size/max_connections from THIS box's own RAM/CPU daily — a hardcoded value here
    would silently down-tune a recreated box back to whatever number was checked in
    years ago (measured live: shared_buffers is 7.7GB, the old checked-in value here
    was 4GB). Compose declares NONE of these; the image's stock defaults are the day-0
    floor and autotune corrects them within 24h."""
    cmd = _pg_command_text(compose)
    for flag in _STALE_TUNING_FLAGS:
        assert flag not in cmd, f"{flag} is hardcoded — autotune should own this value"


@pytest.mark.parametrize("compose", [COMPOSE_YML, COMPOSE_FULL_YML], ids=["yml", "full_yml"])
def test_postgres_and_redis_restart_unless_stopped(compose: dict) -> None:
    assert _pg_service(compose)["restart"] == "unless-stopped"
    assert compose["services"]["redis"]["restart"] == "unless-stopped"


def test_postgres_image_is_16_in_both_compose_files() -> None:
    assert _pg_service(COMPOSE_YML)["image"] == "postgres:16"
    assert _pg_service(COMPOSE_FULL_YML)["image"] == "postgres:16"


# --- static: deploy/up.sh (bash, not yaml — text checks) --------------------------


def test_up_sh_uses_the_real_named_volumes_not_rm() -> None:
    assert "--rm" not in UP_SH_CODE, "a real box's postgres/redis must survive a restart cycle"
    assert "-v osiris-pg-data:/var/lib/postgresql/data" in UP_SH_CODE
    assert "-v osiris-redis-data:/data" in UP_SH_CODE
    assert "--restart unless-stopped" in UP_SH_CODE


def test_up_sh_archives_wal() -> None:
    assert "archive_mode=on" in UP_SH_CODE
    assert "archive_command=" in UP_SH_CODE
    assert "osiris_archive_wal.sh" in UP_SH_CODE
    assert "wal_level=replica" in UP_SH_CODE


def test_up_sh_bind_mounts_the_archive_script_read_only() -> None:
    assert ("osiris_archive_wal.sh:/var/lib/postgresql/data/osiris_archive_wal.sh:ro"
            in UP_SH_CODE)


def test_up_sh_never_hardcodes_the_tuning_flags_autotune_owns() -> None:
    for flag in _STALE_TUNING_FLAGS:
        assert flag not in UP_SH_CODE, f"{flag} is hardcoded — autotune should own this value"


# --- live: diff the static declarations against the actual running container ------

_DOCKER = shutil.which("docker")


def _osiris_pg_is_running() -> bool:
    if _DOCKER is None:
        return False
    try:
        out = subprocess.run(
            ["docker", "inspect", "osiris-pg", "--format", "{{.State.Running}}"],
            capture_output=True, text=True, timeout=10)
    except (subprocess.SubprocessError, OSError):
        return False
    return out.returncode == 0 and out.stdout.strip() == "true"


pytestmark_live = pytest.mark.skipif(
    not _osiris_pg_is_running(),
    reason="no running osiris-pg container on this box — nothing to diff against")


@pytestmark_live
def test_live_container_image_matches_compose() -> None:
    out = subprocess.run(
        ["docker", "inspect", "osiris-pg", "--format", "{{.Config.Image}}"],
        capture_output=True, text=True, check=True)
    assert out.stdout.strip() == _pg_service(COMPOSE_YML)["image"]


@pytestmark_live
def test_live_container_volume_matches_the_compose_declared_name() -> None:
    out = subprocess.run(
        ["docker", "inspect", "osiris-pg", "--format", "{{range .Mounts}}{{.Name}}{{end}}"],
        capture_output=True, text=True, check=True)
    live_volume = out.stdout.strip()
    assert live_volume == _REAL_PG_VOLUME, (
        f"live container mounts volume {live_volume!r}, compose declares "
        f"{_REAL_PG_VOLUME!r} — a recreate from compose today would NOT reattach to "
        "the real data")


@pytestmark_live
def test_live_container_archives_wal_per_postgresql_auto_conf() -> None:
    """The literal 'prove decryption, not presence' shape applied to WAL archiving: not
    just checking the compose FILE says archive_mode=on, but that the box currently
    running actually has it live, in the config file that survives a restart."""
    out = subprocess.run(
        ["docker", "exec", "osiris-pg", "cat",
         "/var/lib/postgresql/data/postgresql.auto.conf"],
        capture_output=True, text=True, check=True)
    conf = out.stdout
    assert "archive_mode = 'on'" in conf
    assert "osiris_archive_wal.sh" in conf
