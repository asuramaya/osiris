"""Canonical dev-box env resolution (task #69, ruling 45b074bf) — ONE place for the fallback
values every dev-facing systemd USER unit already inlines by hand (deploy/osiris-manager.
service, deploy/osiris-console.service, deploy/osiris-pulse.service: DATABASE_URL=...:5601/
osiris, REDIS_URL=...:6396/0) and half a dozen scripts re-hardcode ad hoc (scripts/
osiris_fleet_glance.py, osiris_statusline.py, osiris_stophook.py, backfill_thread_arc.py,
backfill_generations.py, backfill_seat_bindings.py all carry the identical literal
independently — flagged as its own duplication debt, not fixed here, since none of them are
this build's territory).

The bug this closes (3e96c10e's cousin, found diagnosing task #69): Settings.database_url's
class default is postgresql://...@127.0.0.1:5432/osiris — the PROD shape (real deploys set
DATABASE_URL via /etc/osiris/osiris.env, an EnvironmentFile systemd always sources first). A
bare `osiris` invocation on THIS dev box has no such file and this repo's own .env carries no
DATABASE_URL/REDIS_URL at all (confirmed empirically) — so today it silently targets 5432,
which is not the dev instance at all. An explicit env var (a real prod deploy, or an operator
who exported their own) always wins; this only fills the gap a bare CLI call would otherwise
leave, and only for `osiris` CLI processes — get_settings() itself is untouched."""
from __future__ import annotations

import os

DEV_DATABASE_URL = "postgresql://osiris:osiris@127.0.0.1:5601/osiris"
DEV_REDIS_URL = "redis://127.0.0.1:6396/0"


def apply_dev_fallback() -> None:
    """Fill DATABASE_URL/REDIS_URL from this dev box's known values IFF unset. Call this once,
    before the first get_settings()/create_pool() — pydantic-settings and os.environ.get both
    read the environment at call time, so anything already exported (prod's osiris.env, or an
    operator's own override) is never touched. On a genuinely separate host where neither the
    env nor these dev values apply, the result is the SAME loud connection failure a missing
    DATABASE_URL already produces today — never a silent wrong success."""
    os.environ.setdefault("DATABASE_URL", DEV_DATABASE_URL)
    os.environ.setdefault("REDIS_URL", DEV_REDIS_URL)
