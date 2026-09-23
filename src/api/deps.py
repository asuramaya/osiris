"""FastAPI dependencies shared between src.api.app and src.api.inbox.app.

Kept in its own module, importing neither of them, specifically to break a circular
import: app.py lazily imports inbox.app's router inside create_app() (called at module
scope as `app = create_app()`), and inbox.app needs get_pool. When inbox.app imported it
straight from src.api.app, importing src.api.inbox.app first would re-enter src.api.app's
module execution, reach create_app() at module scope, and try to import
src.api.inbox.app back while it was still mid-import (only partially initialized, before
`router` was defined) -- raising an ImportError. This module depends on neither, so
either side can import it first with no cycle.
"""
from __future__ import annotations

import asyncpg
from fastapi import Request


def get_pool(request: Request) -> asyncpg.Pool:
    pool: asyncpg.Pool = request.app.state.pool
    return pool
