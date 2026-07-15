"""Alembic environment.

Migrations are tooling, so they use a *sync* psycopg driver (the "no sync drivers"
rule applies to application runtime, which uses asyncpg). The async DATABASE_URL is
transparently converted to a sync psycopg URL here.
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config

if config.config_file_name is not None:
    # disable_existing_loggers=False: fileConfig's default silently DISABLES every logger
    # already created in this process — in tests (conftest runs migrations in-process at
    # session start) that killed the app's own loggers, and any assertion observing a log
    # record went permanently dark (the parked PTY flood test, thread bbca30a9: force-detach
    # FIRED, its warning was swallowed, three producer-side fixes chased the wrong layer).
    fileConfig(config.config_file_name, disable_existing_loggers=False)

# No ORM models — schema is raw SQL DDL in the migration scripts.
target_metadata = None


def _sync_url() -> str:
    url = os.environ.get("DATABASE_URL", "postgresql://osiris:osiris@127.0.0.1:5432/osiris")
    # asyncpg/plain scheme -> sync psycopg scheme for migrations
    if url.startswith("postgresql+asyncpg://"):
        url = url.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)
    elif url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


def run_migrations_offline() -> None:
    context.configure(url=_sync_url(), target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    section = config.get_section(config.config_ini_section, {})
    section["sqlalchemy.url"] = _sync_url()
    connectable = engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
