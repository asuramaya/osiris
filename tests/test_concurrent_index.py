"""The shared helper for concurrent-index migrations: outside a transaction, and an INVALID
leftover from a failed build is dropped and rebuilt instead of kept."""
from __future__ import annotations

from typing import Any

from src.db.concurrent_index import create_index_concurrently, drop_index_concurrently


class _FakeOp:
    def __init__(self, leftover: bool) -> None:
        self.leftover = leftover
        self.seen: list[str] = []
        self.params: list[Any] = []
        self._auto = False

    def get_bind(self) -> Any:
        outer = self

        class _Result:
            @staticmethod
            def scalar() -> int | None:
                return 1 if outer.leftover else None

        class _Bind:
            @staticmethod
            def execute(stmt: object, params: Any = None) -> _Result:
                outer.params.append(params)
                return _Result()

        return _Bind()

    def get_context(self) -> Any:
        outer = self

        class _Block:
            def __enter__(self) -> None:
                outer._auto = True

            def __exit__(self, *exc: object) -> None:
                outer._auto = False

        class _Ctx:
            @staticmethod
            def autocommit_block() -> _Block:
                return _Block()

        return _Ctx()

    def execute(self, sql: str) -> None:
        self.seen.append(("auto: " if self._auto else "plain: ") + sql)


def test_a_clean_build_is_one_concurrent_create_outside_a_transaction() -> None:
    op = _FakeOp(leftover=False)

    create_index_concurrently(op, "ix_a", "t", "(a, b DESC) WHERE c IS NOT NULL")

    assert op.seen == [
        "auto: CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_a ON t (a, b DESC) WHERE c IS NOT NULL"]
    assert op.params == [{"name": "ix_a"}]


def test_an_invalid_leftover_is_dropped_first_and_unique_is_honoured() -> None:
    op = _FakeOp(leftover=True)

    create_index_concurrently(op, "ix_u", "t", "(a)", unique=True)

    assert op.seen == [
        "auto: DROP INDEX CONCURRENTLY IF EXISTS ix_u",
        "auto: CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ix_u ON t (a)"]


def test_drop_is_concurrent_and_outside_a_transaction() -> None:
    op = _FakeOp(leftover=False)

    drop_index_concurrently(op, "ix_a")

    assert op.seen == ["auto: DROP INDEX CONCURRENTLY IF EXISTS ix_a"]
