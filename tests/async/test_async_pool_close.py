"""
Closing an async pool while connections are still checked out.

The async pool wraps the sync one, so it inherits `close_pool`'s rules: a
connection checked in after `close_async_pool` is closed rather than pooled, and
`connect()` on a closed pool raises `PoolClosedError`. Real DuckDB, both anyio
backends.
"""

from __future__ import annotations

import importlib
import tempfile
from pathlib import Path
from typing import Any, cast

import anyio
import pytest
from sqlalchemy import event

from adbc_poolhouse import (
    DuckDBConfig,
    PoolClosedError,
    close_async_pool,
    create_async_pool,
    managed_async_pool,
)

pytestmark = importlib.import_module("tests.async._edge_helpers").concurrency_marks


def _config() -> DuckDBConfig:
    return DuckDBConfig(database=str(Path(tempfile.mkdtemp()) / "async_close.db"))


@pytest.mark.anyio
async def test_connect_after_close_raises(anyio_backend_name: str) -> None:
    pool = create_async_pool(_config())
    await close_async_pool(pool)
    with pytest.raises(PoolClosedError):
        await pool.connect()


@pytest.mark.anyio
async def test_connect_after_managed_exit_raises(anyio_backend_name: str) -> None:
    async with managed_async_pool(_config()) as pool:
        pass
    with pytest.raises(PoolClosedError):
        await pool.connect()


@pytest.mark.anyio
async def test_connection_checked_in_after_close_is_closed(anyio_backend_name: str) -> None:
    pool = create_async_pool(_config())
    opened: list[Any] = []

    def _record(dbapi_conn: Any, _rec: object) -> None:
        opened.append(dbapi_conn)

    event.listen(pool._pool, "connect", _record)
    conn = await pool.connect()
    await close_async_pool(pool)
    assert pool._pool.checkedout() == 1
    await conn.close()
    assert pool._pool.checkedout() == 0
    assert pool._pool.checkedin() == 0
    assert len(opened) == 1
    assert opened[0]._closed, "the connection went back into the closed pool, still open"


@pytest.mark.anyio
async def test_checkout_waiting_when_the_pool_closes_raises_promptly(
    anyio_backend_name: str,
) -> None:
    """A checkout already blocked on a full pool fails at close, not after `timeout`."""
    pool = create_async_pool(_config(), pool_size=1, max_overflow=0, timeout=20)
    queue = cast("Any", pool._pool)._pool
    held = await pool.connect()
    outcome: list[BaseException] = []

    async def _wait_for_connection() -> None:
        try:
            await pool.connect()
        except PoolClosedError as exc:
            outcome.append(exc)

    with anyio.fail_after(5):
        async with anyio.create_task_group() as tg:
            tg.start_soon(_wait_for_connection)
            while not queue.not_empty._waiters:
                await anyio.sleep(0)
            await close_async_pool(pool)
            await held.close()
    assert len(outcome) == 1
