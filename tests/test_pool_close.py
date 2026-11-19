"""
Closing a pool while connections are still checked out (sync pool).

`close_pool` ends a pool's life. A connection that was checked out when the pool
closed is closed when it is checked in, not put back into the closed pool, and
`checkedout()` keeps counting the connections still out until they come back.
`connect()` on a closed pool raises `PoolClosedError`.

Each case is its own test so each one is seen failing on its own.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from sqlalchemy import event

from adbc_poolhouse import (
    DuckDBConfig,
    PoolClosedError,
    PoolhouseError,
    close_pool,
    create_pool,
    managed_pool,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    import sqlalchemy.pool


@pytest.fixture
def pool(tmp_path: Path) -> Iterator[sqlalchemy.pool.QueuePool]:
    """A file-backed DuckDB pool, closed on teardown (closing twice is allowed)."""
    p = create_pool(DuckDBConfig(database=str(tmp_path / "close.db")), pool_size=2)
    try:
        yield p
    finally:
        close_pool(p)


def _opened(pool: sqlalchemy.pool.QueuePool) -> list[Any]:
    """Record every DBAPI connection the pool opens from now on."""
    opened: list[Any] = []

    def _record(dbapi_conn: Any, _rec: object) -> None:
        opened.append(dbapi_conn)

    event.listen(pool, "connect", _record)
    return opened


def test_pool_closed_error_is_a_poolhouse_error() -> None:
    assert issubclass(PoolClosedError, PoolhouseError)


def test_connection_checked_in_after_close_is_closed(pool: sqlalchemy.pool.QueuePool) -> None:
    opened = _opened(pool)
    conn = pool.connect()
    close_pool(pool)
    conn.close()
    assert len(opened) == 1
    assert opened[0]._closed, "the connection went back into the closed pool, still open"


def test_closed_pool_keeps_no_idle_connections(pool: sqlalchemy.pool.QueuePool) -> None:
    conn = pool.connect()
    close_pool(pool)
    conn.close()
    assert pool.checkedin() == 0


def test_checkedout_counts_connections_still_out_after_close(
    pool: sqlalchemy.pool.QueuePool,
) -> None:
    first, second = pool.connect(), pool.connect()
    close_pool(pool)
    assert pool.checkedout() == 2
    first.close()
    assert pool.checkedout() == 1
    second.close()
    assert pool.checkedout() == 0


def test_invalidated_connection_after_close_leaves_count_at_zero(
    pool: sqlalchemy.pool.QueuePool,
) -> None:
    conn = pool.connect()
    close_pool(pool)
    conn.invalidate()
    conn.close()
    assert pool.checkedout() == 0


def test_connect_after_close_raises(pool: sqlalchemy.pool.QueuePool) -> None:
    close_pool(pool)
    with pytest.raises(PoolClosedError):
        pool.connect()


def test_connect_after_close_opens_no_connection(pool: sqlalchemy.pool.QueuePool) -> None:
    opened = _opened(pool)
    close_pool(pool)
    with pytest.raises(PoolhouseError):
        pool.connect()
    assert opened == []


def test_connect_after_managed_pool_exit_raises(tmp_path: Path) -> None:
    with managed_pool(DuckDBConfig(database=str(tmp_path / "managed.db"))) as p:
        pass
    with pytest.raises(PoolClosedError):
        p.connect()


def test_close_pool_twice_is_a_no_op(pool: sqlalchemy.pool.QueuePool) -> None:
    conn = pool.connect()
    close_pool(pool)
    close_pool(pool)
    assert pool.checkedout() == 1
    conn.close()
    assert pool.checkedout() == 0


def test_dispose_alone_leaves_the_pool_usable(pool: sqlalchemy.pool.QueuePool) -> None:
    """`pool.dispose()` is SQLAlchemy's reset, not a close: the pool still serves."""
    pool.connect().close()
    pool.dispose()
    conn = pool.connect()
    cur = conn.cursor()
    cur.execute("SELECT 1")
    assert cur.fetchone() == (1,)
    cur.close()
    conn.close()
    assert pool.checkedin() == 1


class _FakeConnection:
    """The DBAPI surface QueuePool touches on check-out, check-in and close."""

    def __init__(self) -> None:
        self.closed = False

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


class _StatelessBackend:
    """A non-ADBC `ConnectionBackend` with no shared source, like Databricks Python."""

    def __init__(self) -> None:
        self.opened: list[_FakeConnection] = []

    def creator(self) -> Any:
        def _open() -> _FakeConnection:
            conn = _FakeConnection()
            self.opened.append(conn)
            return conn

        return _open

    def close(self) -> None:
        pass

    def on_reset(self, dbapi_conn: object) -> None:
        pass


def _native_pool(backend: _StatelessBackend) -> sqlalchemy.pool.QueuePool:
    from adbc_poolhouse._pool_factory import _create_native_pool  # noqa: PLC0415

    return _create_native_pool(
        backend, pool_size=2, max_overflow=0, timeout=1, recycle=3600, pre_ping=False
    )


def test_native_pool_connect_after_close_raises() -> None:
    """A stateless backend could still open connections; a closed pool must not."""
    backend = _StatelessBackend()
    pool = _native_pool(backend)
    close_pool(pool)
    with pytest.raises(PoolClosedError):
        pool.connect()
    assert backend.opened == []


def test_native_pool_connection_checked_in_after_close_is_closed() -> None:
    backend = _StatelessBackend()
    pool = _native_pool(backend)
    conn = pool.connect()
    close_pool(pool)
    conn.close()
    assert backend.opened[0].closed
    assert pool.checkedout() == 0
