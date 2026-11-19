"""
Pool invariants under random orderings: a Hypothesis state machine over a real pool.

The rest of `tests/async` pins hand-picked orderings of checkout, check-in,
cancellation and invalidation. This module lets Hypothesis pick the orderings: each
example builds a real DuckDB `AsyncPool` with a drawn `pool_size` and
`max_overflow`, then runs a random sequence of these operations against it:

- check out a connection (expected to time out exactly when the pool is at its
  `pool_size + max_overflow` ceiling, since `timeout=0`)
- check one in, normally or from inside an already-cancelled scope (the check-in is
  shielded, so it must still happen)
- invalidate one, then close the dead handle (a documented no-op)
- run a query on one, normally, from inside an already-cancelled scope (the call
  never starts, so the connection must stay usable), or cancelled while it runs
  (the connection is either still fine, if the query won the race, or dropped)
- check out from inside an already-cancelled scope (nothing may be checked out)

After every step it checks the pool against a model that only knows which
connections the test is holding:

- `checkedout()` equals the number of connections held, and never exceeds
  `pool_size + max_overflow`
- every DBAPI connection the pool ever opened is either checked out, idle in the
  pool, or closed (no leaked connections)
- neither of the pool's capacity limiters has a token borrowed between calls
- every held connection still answers a query (no poisoned connection is handed
  out or kept)

At the end it checks everything in, closes the pool and requires every DBAPI
connection to be closed.

Closing the pool while connections are still checked out is not one of the
operations: the pool-lifecycle guide says close neither waits for nor closes
checked-out connections, so the teardown drains first, as the guide tells users to.

The machine runs on a blocking portal (one event loop in a side thread for the
whole example), on both anyio backends. Each step is driven from the test thread,
so a step that hangs is caught by the pytest-timeout mark.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any, cast

import anyio
import anyio.from_thread
import pytest
import sqlalchemy.exc
from hypothesis import HealthCheck, settings
from hypothesis import event as count_outcome
from hypothesis import strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    initialize,
    invariant,
    precondition,
    rule,
    run_state_machine_as_test,
)
from sqlalchemy import event

from adbc_poolhouse import DuckDBConfig, create_async_pool

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    from sqlalchemy.pool import ConnectionPoolEntry

    from adbc_poolhouse._async._connection import AsyncConnection
    from adbc_poolhouse._async._pool import AsyncPool

# Runs for a few hundred milliseconds when not interrupted, so a cancel fired a
# moment after it starts usually lands mid-query. When the query wins the race
# instead, the model accepts that too: the connection is then simply still held.
_SLOW_QUERY = "SELECT count(*) FROM range(10000) a, range(10000) b WHERE a.range % 7 = b.range % 7"
_CANCEL_AFTER_S = 0.02

pytestmark = pytest.mark.timeout(300)


class PoolMachine(RuleBasedStateMachine):
    """Random checkout / check-in / cancel / invalidate orderings against a real pool."""

    backend = "asyncio"
    database = ""

    def __init__(self) -> None:
        super().__init__()
        self._exit = contextlib.ExitStack()
        self.portal = self._exit.enter_context(
            anyio.from_thread.start_blocking_portal(self.backend)
        )
        self.pool: AsyncPool | None = None
        self.ceiling = 0
        # The model: the connections the test holds, and the invalidated handles.
        self.held: list[AsyncConnection] = []
        self.dead: list[AsyncConnection] = []
        self.dbapi_connections: list[Any] = []

    @initialize(pool_size=st.integers(1, 3), max_overflow=st.integers(0, 2))
    def make_pool(self, pool_size: int, max_overflow: int) -> None:
        async def _make() -> AsyncPool:
            return create_async_pool(
                DuckDBConfig(database=self.database),
                pool_size=pool_size,
                max_overflow=max_overflow,
                timeout=0,
            )

        self.pool = self.portal.call(_make)
        self.ceiling = pool_size + max_overflow
        event.listen(self.pool._pool, "connect", self._record_connect)  # noqa: SLF001

    def _record_connect(self, dbapi_conn: Any, _record: object) -> None:
        self.dbapi_connections.append(dbapi_conn)

    def _pool(self) -> AsyncPool:
        assert self.pool is not None
        return self.pool

    def _take(self, data: st.DataObject) -> AsyncConnection:
        conn = data.draw(st.sampled_from(self.held))
        self.held.remove(conn)
        return conn

    def _select_one(self, conn: AsyncConnection) -> object:
        async def _run() -> object:
            cur = conn.cursor()
            try:
                await cur.execute("SELECT 1")
                return await cur.fetchone()
            finally:
                await cur.close()

        return self.portal.call(_run)

    # --- checkout ------------------------------------------------------------

    @rule()
    def checkout(self) -> None:
        pool = self._pool()
        if len(self.held) == self.ceiling:
            with pytest.raises(sqlalchemy.exc.TimeoutError):
                self.portal.call(pool.connect)
            return
        conn = self.portal.call(pool.connect)
        # A connection fresh out of the pool must work, even one a cancelled or
        # invalidated predecessor left behind.
        assert self._select_one(conn) == (1,)
        self.held.append(conn)

    @rule()
    def checkout_in_cancelled_scope(self) -> None:
        pool = self._pool()

        async def _run() -> AsyncConnection | None:
            with anyio.CancelScope() as scope:
                scope.cancel()
                return await pool.connect()
            return None

        got = self.portal.call(_run)
        assert got is None, "a checkout cancelled before it started returned a connection"

    # --- check-in ------------------------------------------------------------

    @precondition(lambda self: self.held)
    @rule(data=st.data())
    def checkin(self, data: st.DataObject) -> None:
        self.portal.call(self._take(data).close)

    @precondition(lambda self: self.held)
    @rule(data=st.data())
    def checkin_in_cancelled_scope(self, data: st.DataObject) -> None:
        conn = self._take(data)

        async def _run() -> None:
            with anyio.CancelScope() as scope:
                scope.cancel()
                await conn.close()

        self.portal.call(_run)

    # --- invalidation ----------------------------------------------------------

    @precondition(lambda self: self.held)
    @rule(data=st.data())
    def invalidate(self, data: st.DataObject) -> None:
        conn = self._take(data)
        self.portal.call(conn.invalidate)
        self.dead.append(conn)

    @precondition(lambda self: self.dead)
    @rule(data=st.data())
    def close_dead_handle(self, data: st.DataObject) -> None:
        handle = data.draw(st.sampled_from(self.dead))
        self.dead.remove(handle)
        # The documented safe no-op: an invalidated handle returns nothing.
        self.portal.call(handle.close)

    # --- queries -------------------------------------------------------------

    @precondition(lambda self: self.held)
    @rule(data=st.data())
    def query(self, data: st.DataObject) -> None:
        conn = data.draw(st.sampled_from(self.held))
        assert self._select_one(conn) == (1,)

    @precondition(lambda self: self.held)
    @rule(data=st.data())
    def query_in_cancelled_scope(self, data: st.DataObject) -> None:
        conn = data.draw(st.sampled_from(self.held))

        async def _run() -> bool:
            with anyio.CancelScope() as scope:
                scope.cancel()
                await conn.cursor().execute("SELECT 1")
                return True
            return False

        assert self.portal.call(_run) is False, "a cancelled execute ran"
        # The call never started, so nothing was poisoned: the connection stays.
        assert conn._fairy.is_valid  # noqa: SLF001

    @precondition(lambda self: self.held)
    @rule(data=st.data())
    def query_cancelled_while_running(self, data: st.DataObject) -> None:
        conn = self._take(data)

        async def _run() -> None:
            with anyio.move_on_after(_CANCEL_AFTER_S):
                cur = conn.cursor()
                await cur.execute(_SLOW_QUERY)
                await cur.fetchone()

        self.portal.call(_run)
        count_outcome("slow query: " + ("finished first" if conn._fairy.is_valid else "cancelled"))  # noqa: SLF001
        if conn._fairy.is_valid:  # noqa: SLF001
            # The query finished before the cancel landed, so the connection is
            # still checked out, and must still work.
            assert self._select_one(conn) == (1,)
            self.held.append(conn)
        else:
            # The cancel landed: the poisoned connection left the pool.
            self.dead.append(conn)

    # --- invariants ----------------------------------------------------------

    @invariant()
    def accounting_matches_model(self) -> None:
        if self.pool is None:
            return
        sync_pool = self.pool._pool  # noqa: SLF001
        assert sync_pool.checkedout() == len(self.held)
        assert len(self.held) <= self.ceiling
        # Every open DBAPI connection is accounted for: either behind a connection the
        # test holds, or idle in the pool's queue. (An invalidated connection's record
        # goes back to the queue empty and reconnects on its next checkout.)
        idle: Iterable[ConnectionPoolEntry] = cast("Any", sync_pool)._pool.queue
        accounted: list[object] = [conn._fairy.dbapi_connection for conn in self.held]  # noqa: SLF001
        accounted += [r.dbapi_connection for r in idle if r.dbapi_connection is not None]
        open_conns = [c for c in self.dbapi_connections if not c._closed]  # noqa: SLF001
        assert sorted(map(id, open_conns)) == sorted(map(id, accounted)), (
            "a DBAPI connection is open but neither held nor idle in the pool"
        )
        assert self.pool._limiter.borrowed_tokens == 0  # noqa: SLF001
        assert self.pool._checkout_limiter.borrowed_tokens == 0  # noqa: SLF001

    def teardown(self) -> None:
        try:
            if self.pool is not None:
                for conn in self.held:
                    self.portal.call(conn.close)
                self.held.clear()
                assert self.pool._pool.checkedout() == 0  # noqa: SLF001
                self.portal.call(self.pool.close)
                still_open = [c for c in self.dbapi_connections if not c._closed]  # noqa: SLF001
                assert not still_open, f"{len(still_open)} DBAPI connection(s) leaked"
        finally:
            self._exit.close()


@pytest.mark.parametrize("backend", ["asyncio", "trio"])
def test_pool_invariants_hold_under_random_orderings(backend: str, tmp_path: Path) -> None:
    """Random orderings of pool operations keep accounting, leaks and limiters right."""
    machine = type(
        f"PoolMachine_{backend}",
        (PoolMachine,),
        {"backend": backend, "database": str(tmp_path / "state_machine.db")},
    )
    run_state_machine_as_test(
        machine,
        settings=settings(
            max_examples=30,
            stateful_step_count=30,
            deadline=None,
            suppress_health_check=[HealthCheck.too_slow],
        ),
    )
