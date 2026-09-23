"""Canonical dev-box env resolution: one place for the fallback values every dev-facing
systemd user unit already inlines by hand (deploy/osiris-manager.service, deploy/user/
osiris-console.service, deploy/user/osiris-pulse.service: DATABASE_URL=...:5601/osiris,
REDIS_URL=...:6396/0), which half a dozen scripts also re-hardcode independently. That
duplication is known debt, not fixed here, since none of those scripts are this module's
territory.

The bug this closes: Settings.database_url's class default is
postgresql://...@127.0.0.1:5432/osiris, the production shape (a real deploy sets
DATABASE_URL via /etc/osiris/osiris.env, an EnvironmentFile systemd always sources
first). A bare `osiris` invocation on this dev box has no such file, and this repo's own
.env carries no DATABASE_URL/REDIS_URL at all (confirmed empirically), so without this
fallback it would silently target 5432, which is not the dev instance at all. An explicit
env var (a real production deploy, or a developer's own override) always wins; this only
fills the gap a bare CLI call would otherwise leave, and only for `osiris` CLI
processes -- get_settings() itself is untouched."""
from __future__ import annotations

import os

DEV_DATABASE_URL = "postgresql://osiris:osiris@127.0.0.1:5601/osiris"
DEV_REDIS_URL = "redis://127.0.0.1:6396/0"


def apply_dev_fallback() -> None:
    """Fill DATABASE_URL/REDIS_URL from this dev box's known values if unset. Call this once,
    before the first get_settings()/create_pool() -- pydantic-settings and os.environ.get both
    read the environment at call time, so anything already exported (production's osiris.env,
    or an operator's own override) is never touched. On a genuinely separate host where
    neither the environment nor these dev values apply, the result is the same loud
    connection failure a missing DATABASE_URL already produces today, never a silent wrong
    success."""
    os.environ.setdefault("DATABASE_URL", DEV_DATABASE_URL)
    os.environ.setdefault("REDIS_URL", DEV_REDIS_URL)


def refuse_silent_live_db(caller: str) -> str | None:
    """A shared guard for one-off, human-run scripts: on this box there is no isolated
    dev instance -- `DEV_DATABASE_URL` above (5601) is the same database every deployed
    service's own DATABASE_URL points at (see `apply_dev_fallback`'s docstring: a real
    production deploy sets DATABASE_URL via /etc/osiris/osiris.env, which does not exist
    here). A script that neither the caller nor any deployed unit's environment already
    set DATABASE_URL for is about to hit that same live graph by accident,
    indistinguishable from a real confirmed run. Returns the refusal message (never
    prints or exits itself -- the caller decides its own exit convention) when
    DATABASE_URL is unset and `OSIRIS_ALLOW_LIVE` is not "1"; returns None when the
    caller may proceed (an explicit DATABASE_URL, including one a deployed unit's own
    environment already carries, always wins and is never blocked).

    Never call this from a script that is itself the deployed automation -- those exist
    to run against the real graph on every ordinary invocation with nobody manually
    confirming; guarding them would break routine operation across the fleet. Reserve
    this for exploratory or scratch-run tools, never a deliberate, already-authorized
    process's own entry point."""
    if os.environ.get("DATABASE_URL") or os.environ.get("OSIRIS_ALLOW_LIVE") == "1":
        return None
    return (
        f"{caller}: refusing, no DATABASE_URL is set, and this box's own dev fallback "
        "points at the same database every deployed service uses (no isolated dev "
        "instance exists here). Set DATABASE_URL to a scratch instance, or "
        "OSIRIS_ALLOW_LIVE=1 to confirm you mean the real graph."
    )
