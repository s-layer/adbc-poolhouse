"""
Closing a pool while connections are still checked out (sync pool).

`close_pool` ends a pool's life. A connection that was checked out when the pool
closed is closed when it is checked in, not put back into the closed pool, and
`checkedout()` keeps counting the connections still out until they come back.
`connect()` on a closed pool raises `PoolClosedError`.

Each case is its own test so each one is seen failing on its own.
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Any, cast

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


def _connect_in_thread(pool: sqlalchemy.pool.QueuePool) -> tuple[threading.Thread, list[object]]:
    """Start `pool.connect()` on a thread; the list receives what it returned or raised."""
    outcome: list[object] = []

    def _run() -> None:
        try:
            outcome.append(pool.connect())
        except BaseException as exc:  # noqa: BLE001
            outcome.append(exc)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return thread, outcome


def _wait_until_blocked(pool: sqlalchemy.pool.QueuePool) -> None:
    """Wait until a checkout thread is blocked on the pool's empty queue."""
    queue = cast("Any", pool)._pool
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        # A waiter sits in `not_empty.wait`, which registers it on the condition.
        if queue.not_empty._waiters:
            return
        time.sleep(0.01)
    pytest.fail("the checkout never blocked on the pool")


def test_checkout_waiting_when_the_pool_closes_raises_promptly(tmp_path: Path) -> None:
    """A checkout already blocked on a full pool fails at close, not after `timeout`."""
    pool = create_pool(
        DuckDBConfig(database=str(tmp_path / "waiter.db")),
        pool_size=1,
        max_overflow=0,
        timeout=20,
    )
    held = pool.connect()
    thread, outcome = _connect_in_thread(pool)
    _wait_until_blocked(pool)
    close_pool(pool)
    held.close()
    thread.join(timeout=5)
    assert not thread.is_alive(), "the waiting checkout was still blocked 5 s after close"
    assert len(outcome) == 1
    assert isinstance(outcome[0], PoolClosedError), outcome[0]


def test_native_checkout_waiting_when_the_pool_closes_opens_nothing() -> None:
    """On a stateless backend the waiter must not open a connection after close."""
    backend = _StatelessBackend()
    pool = _native_pool(backend)
    first, second = pool.connect(), pool.connect()
    thread, outcome = _connect_in_thread(pool)
    _wait_until_blocked(pool)
    close_pool(pool)
    first.close()
    second.close()
    thread.join(timeout=5)
    assert not thread.is_alive(), "the waiting checkout was still blocked 5 s after close"
    assert isinstance(outcome[0], PoolClosedError), outcome[0]
    assert len(backend.opened) == 2


class _FailingCloseBackend(_StatelessBackend):
    """A backend whose teardown fails the first time."""

    def __init__(self) -> None:
        super().__init__()
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1
        if self.close_calls == 1:
            raise RuntimeError("teardown failed")


def test_close_pool_retries_a_failed_source_close() -> None:
    backend = _FailingCloseBackend()
    pool = _native_pool(backend)
    with pytest.raises(RuntimeError, match="teardown failed"):
        close_pool(pool)
    close_pool(pool)
    assert backend.close_calls == 2
    close_pool(pool)
    assert backend.close_calls == 2, "a third close tore the backend down again"


def test_dispose_on_a_closed_pool_is_a_no_op(pool: sqlalchemy.pool.QueuePool) -> None:
    close_pool(pool)
    pool.dispose()
    with pytest.raises(PoolClosedError):
        pool.connect()
