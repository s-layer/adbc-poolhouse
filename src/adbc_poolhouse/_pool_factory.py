"""
Pool factory: public create_pool() entry point.

This module wires together config methods and the ADBC source+clone
pattern to create a SQLAlchemy QueuePool.  Each config instance is
self-describing -- it carries its own driver path, kwargs translation,
entrypoint, and optional DBAPI module -- so no registry or dispatch
layer is needed.

Three call patterns are supported:

1. ``create_pool(config)`` -- config-based (original path)
2. ``create_pool(driver_path=..., db_kwargs=...)`` -- native ADBC driver
3. ``create_pool(dbapi_module=..., db_kwargs=...)`` -- Python dbapi module

No module-level pool or connection objects exist here (POOL-05).

Internal helpers are prefixed with _ and not exported.
"""

from __future__ import annotations

import contextlib
import threading
from typing import TYPE_CHECKING, Any, cast, overload

import sqlalchemy.pool
from sqlalchemy import event
from sqlalchemy.util import queue as sqla_queue

from adbc_poolhouse._driver_api import create_adbc_connection
from adbc_poolhouse._exceptions import PoolClosedError

if TYPE_CHECKING:
    import collections.abc

    from sqlalchemy.pool import ConnectionPoolEntry

    from adbc_poolhouse._backend import ConnectionBackend
    from adbc_poolhouse._base_config import WarehouseConfig


_TUNING_DEFAULTS: dict[str, int | bool] = {
    "pool_size": 5,
    "max_overflow": 3,
    "timeout": 30,
    "recycle": 3600,
    "pre_ping": False,
}


class _ClosableQueue(sqla_queue.Queue[Any]):
    """
    The pool's queue of idle connections, with a closed state.

    Once closed, a `get` that finds the queue empty raises `PoolClosedError`
    instead of reporting it empty or waiting, and `close` wakes every `get`
    already waiting. That is how a checkout blocked on a full pool learns the
    pool closed, rather than waiting out the pool's `timeout`.
    """

    closed = False

    def close(self) -> None:
        """Mark the queue closed and wake every waiting `get`."""
        with self.mutex:
            self.closed = True
            self.not_empty.notify_all()

    def _empty(self) -> bool:
        # A closed queue never reads as empty, so `get` neither raises `Empty`
        # nor waits: it goes on to `_get`, which raises `PoolClosedError`.
        return not self.queue and not self.closed

    def _get(self) -> Any:
        if not self.queue:
            raise PoolClosedError
        return super()._get()


class _PoolhouseQueuePool(sqlalchemy.pool.QueuePool):
    """
    A `QueuePool` that `close_pool` can close for good.

    SQLAlchemy's `dispose()` is a reset: the pool keeps serving afterwards, and a
    connection checked out across it is put back into the queue on check-in. A
    pool whose ADBC source has been closed cannot serve again, so `close_pool`
    calls `_close()` instead, after which:

    - `connect()` raises `PoolClosedError` rather than trying to open a
      connection (on the ADBC path, a clone of the closed source), and so does a
      `connect()` that was already waiting for a free connection;
    - a connection checked in is closed, not queued, so nothing is left open in a
      pool no one will close again;
    - `checkedout()` keeps counting the connections still out until they return.

    `dispose()` keeps SQLAlchemy's meaning on an open pool and does nothing on a
    closed one.
    """

    _queue_class = _ClosableQueue

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._poolhouse_closed = False
        # Set by `close_pool` once the ADBC source or native backend has closed,
        # so a failed source close is retried by the next `close_pool`.
        self._poolhouse_source_closed = False
        # Held across the closed check and the queue put on check-in, so a
        # connection returned while `_close()` runs is either drained by it or
        # closed on return, never left in the queue. Reentrant because a check-in
        # can also run from SQLAlchemy's weakref finalizer, which garbage
        # collection may fire on a thread that already holds the lock.
        self._poolhouse_close_lock = threading.RLock()

    def _close(self) -> None:
        """
        Mark the pool closed, close its idle connections and wake waiting checkouts.

        Safe to call again: a later call closes anything a failed earlier one left
        in the queue.
        """
        queue = cast("_ClosableQueue", self._pool)
        with self._poolhouse_close_lock:
            first = not self._poolhouse_closed
            self._poolhouse_closed = True
            try:
                while True:
                    with queue.mutex:
                        if not queue.queue:
                            break
                        record = queue.queue.popleft()
                    try:
                        record.close()
                    finally:
                        # The record leaves the pool for good: drop it from the
                        # count, as a check-in that overflows the queue does.
                        self._dec_overflow()
            finally:
                # Wake waiting checkouts even if closing an idle connection raised;
                # a later `close_pool` drains whatever is left.
                queue.close()
        if first:
            self.logger.info("Pool closed. %s", self.status())

    def dispose(self) -> None:
        if self._poolhouse_closed:
            return
        super().dispose()

    def _do_get(self) -> ConnectionPoolEntry:
        if self._poolhouse_closed:
            raise PoolClosedError
        return super()._do_get()

    def _create_connection(self) -> ConnectionPoolEntry:
        # A checkout that passed the closed check before `_close()` ran can reach
        # here with the pool closed. `QueuePool._do_get` undoes its overflow
        # increment when this raises.
        if self._poolhouse_closed:
            raise PoolClosedError
        return super()._create_connection()

    def _do_return_conn(self, record: ConnectionPoolEntry) -> None:
        with self._poolhouse_close_lock:
            if not self._poolhouse_closed:
                super()._do_return_conn(record)
                return
        try:
            record.close()
        finally:
            self._dec_overflow()


def _resolve_tuning(
    config: WarehouseConfig | None,
    pool_size: int | None,
    max_overflow: int | None,
    timeout: int | None,
    recycle: int | None,
    pre_ping: bool | None,
) -> tuple[int, int, int, int, bool]:
    """
    Resolve pool-tuning values with precedence: explicit arg > config field > default.

    An explicit `create_pool` keyword (anything other than `None`) always wins. When
    it is omitted, the value comes from the config's own field, which loads from
    keyword arguments and environment variables (e.g. `SNOWFLAKE_POOL_SIZE`). The
    hardcoded defaults are the final fallback for the raw driver paths, which have no
    config object.
    """

    def pick(explicit: int | bool | None, field: str) -> int | bool:
        if explicit is not None:
            return explicit
        if config is not None:
            return getattr(config, field, _TUNING_DEFAULTS[field])
        return _TUNING_DEFAULTS[field]

    return (
        int(pick(pool_size, "pool_size")),
        int(pick(max_overflow, "max_overflow")),
        int(pick(timeout, "timeout")),
        int(pick(recycle, "recycle")),
        bool(pick(pre_ping, "pre_ping")),
    )


def _create_pool_impl(
    config: WarehouseConfig | None,
    driver_path: str | None,
    db_kwargs: dict[str, str] | None,
    entrypoint: str | None,
    dbapi_module: str | None,
    pool_size: int | None,
    max_overflow: int | None,
    timeout: int | None,
    recycle: int | None,
    pre_ping: bool | None,
) -> sqlalchemy.pool.QueuePool:
    """Internal: create pool from either config or raw driver args."""
    if driver_path is not None and dbapi_module is not None:
        raise TypeError("create_pool() accepts driver_path or dbapi_module, not both")

    pool_size, max_overflow, timeout, recycle, pre_ping = _resolve_tuning(
        config, pool_size, max_overflow, timeout, recycle, pre_ping
    )

    if config is not None:
        # Non-ADBC backend path -- a config may declare its own ConnectionBackend
        # (e.g. the Databricks Python connector) via the optional _make_backend
        # hook. Accessed via getattr so third-party configs implementing only the
        # documented WarehouseConfig protocol (no _make_backend) still work and
        # take the ADBC path.
        make_backend = getattr(config, "_make_backend", None)
        native_backend = make_backend() if make_backend is not None else None
        if native_backend is not None:
            return _create_native_pool(
                native_backend, pool_size, max_overflow, timeout, recycle, pre_ping
            )

        # Config path -- extract driver info from config methods
        cfg_driver_path = config._driver_path()
        cfg_dbapi_module = config._dbapi_module()
        if cfg_driver_path is None and cfg_dbapi_module is None:
            raise TypeError(
                f"{type(config).__name__} must provide _driver_path() or "
                "_dbapi_module() (both returned None)"
            )
        resolved_driver_path = cfg_driver_path if cfg_driver_path is not None else ""
        resolved_kwargs = config.to_adbc_kwargs()
        resolved_entrypoint = config._adbc_entrypoint()
        resolved_dbapi_module = cfg_dbapi_module
    elif driver_path is not None:
        # Native ADBC driver path
        if db_kwargs is None:
            raise TypeError("db_kwargs is required when using driver_path")
        resolved_driver_path = driver_path
        resolved_kwargs = db_kwargs
        resolved_entrypoint = entrypoint
        resolved_dbapi_module = None
    elif dbapi_module is not None:
        # Python dbapi module path -- driver_path unused by this branch
        if db_kwargs is None:
            raise TypeError("db_kwargs is required when using dbapi_module")
        resolved_driver_path = ""
        resolved_kwargs = db_kwargs
        resolved_entrypoint = None
        resolved_dbapi_module = dbapi_module
    else:
        raise TypeError(
            "create_pool() requires one of: a config object (positional), "
            "driver_path=..., or dbapi_module=..."
        )

    source = create_adbc_connection(
        resolved_driver_path,
        resolved_kwargs,
        entrypoint=resolved_entrypoint,
        dbapi_module=resolved_dbapi_module,
    )

    pool = _PoolhouseQueuePool(
        source.adbc_clone,  # type: ignore[arg-type]
        pool_size=pool_size,
        max_overflow=max_overflow,
        timeout=timeout,
        recycle=recycle,
        pre_ping=pre_ping,
    )

    pool._adbc_source = source  # type: ignore[attr-defined]

    event.listen(pool, "reset", _release_arrow_allocators)

    return pool


def _create_native_pool(
    backend: ConnectionBackend,
    pool_size: int,
    max_overflow: int,
    timeout: int,
    recycle: int,
    pre_ping: bool,
) -> sqlalchemy.pool.QueuePool:
    """
    Build a QueuePool from a non-ADBC `ConnectionBackend`.

    Unlike the ADBC path there is no shared source connection cloned per
    checkout: the backend's creator opens an independent connection each time.
    The backend is stashed on ``pool._native_backend`` so `close_pool` can tear
    it down, and its ``on_reset`` runs on the pool ``reset`` event to release
    Arrow buffers on check-in.
    """
    pool = _PoolhouseQueuePool(
        backend.creator(),
        pool_size=pool_size,
        max_overflow=max_overflow,
        timeout=timeout,
        recycle=recycle,
        pre_ping=pre_ping,
    )

    pool._native_backend = backend  # type: ignore[attr-defined]

    def _on_reset(dbapi_conn: object, connection_record: object, reset_state: object) -> None:
        # The reset event fires with dbapi_conn=None on invalidation paths (same
        # as the ADBC hook's guard); skip so backends need not handle None.
        if dbapi_conn is None:
            return
        backend.on_reset(dbapi_conn)

    event.listen(pool, "reset", _on_reset)

    return pool


@overload
def create_pool(
    config: WarehouseConfig,
    *,
    pool_size: int = 5,
    max_overflow: int = 3,
    timeout: int = 30,
    recycle: int = 3600,
    pre_ping: bool = False,
) -> sqlalchemy.pool.QueuePool: ...


@overload
def create_pool(
    *,
    driver_path: str,
    db_kwargs: dict[str, str],
    entrypoint: str | None = None,
    pool_size: int = 5,
    max_overflow: int = 3,
    timeout: int = 30,
    recycle: int = 3600,
    pre_ping: bool = False,
) -> sqlalchemy.pool.QueuePool: ...


@overload
def create_pool(
    *,
    dbapi_module: str,
    db_kwargs: dict[str, str],
    pool_size: int = 5,
    max_overflow: int = 3,
    timeout: int = 30,
    recycle: int = 3600,
    pre_ping: bool = False,
) -> sqlalchemy.pool.QueuePool: ...


def create_pool(
    config: WarehouseConfig | None = None,
    *,
    driver_path: str | None = None,
    db_kwargs: dict[str, str] | None = None,
    entrypoint: str | None = None,
    dbapi_module: str | None = None,
    pool_size: int | None = None,
    max_overflow: int | None = None,
    timeout: int | None = None,
    recycle: int | None = None,
    pre_ping: bool | None = None,
) -> sqlalchemy.pool.QueuePool:
    """
    Create a SQLAlchemy QueuePool backed by an ADBC driver.

    Three call patterns are supported:

        pool = create_pool(DuckDBConfig(...))           # from a config object
        pool = create_pool(driver_path="...", ...)       # native ADBC driver
        pool = create_pool(dbapi_module="...", ...)      # Python dbapi module

    The config path extracts driver information from the config object's
    methods. The two raw paths accept driver arguments directly, bypassing
    config objects entirely.

    Args:
        config: A warehouse config model instance (e.g. ``DuckDBConfig``).
            Mutually exclusive with ``driver_path`` and ``dbapi_module``.
        driver_path: Path to a native ADBC driver shared library, or a
            short driver name for manifest-based resolution. Requires
            ``db_kwargs``. Mutually exclusive with ``config`` and
            ``dbapi_module``.
        db_kwargs: ADBC connection keyword arguments as ``dict[str, str]``.
            Required when using ``driver_path`` or ``dbapi_module``.
        entrypoint: ADBC entry-point symbol. Only used with ``driver_path``
            (e.g. ``"duckdb_adbc_init"`` for DuckDB). Default: ``None``.
        dbapi_module: Dotted module name for a Python package implementing
            the ADBC dbapi interface (e.g. ``"adbc_driver_snowflake.dbapi"``
            or a custom ``"my_driver.dbapi"``). Requires ``db_kwargs``.
            Mutually exclusive with ``config`` and ``driver_path``.
        pool_size: Number of connections to keep in the pool. When omitted, the
            config's own ``pool_size`` field is used (which loads from keyword
            arguments and environment variables such as ``DUCKDB_POOL_SIZE``);
            passing a value here overrides the config. The raw driver paths, which
            have no config, fall back to 5.
        max_overflow: Extra connections allowed above pool_size. Same precedence as
            ``pool_size`` (config field, then 5's counterpart 3 for raw paths).
        timeout: Seconds to wait for a connection before raising. Config field, or
            30 for the raw paths.
        recycle: Seconds before a connection is recycled. Config field, or 3600 for
            the raw paths.
        pre_ping: Whether to ping connections before checkout. Config field, or
            False for the raw paths. Pre-ping does not function on a standalone
            QueuePool without a SQLAlchemy dialect; recycle is the preferred health
            mechanism.

    Returns:
        A configured ``sqlalchemy.pool.QueuePool`` ready for use.

    Raises:
        TypeError: If none of ``config``, ``driver_path``, or ``dbapi_module``
            is provided, or if both ``driver_path`` and ``dbapi_module`` are
            provided.
        ImportError: If the required ADBC driver is not installed.

    Example:
        Config path:

        ```python
        from adbc_poolhouse import create_pool, close_pool
        from adbc_poolhouse import DuckDBConfig

        pool = create_pool(DuckDBConfig(database="/tmp/my.db"))
        close_pool(pool)
        ```

        Raw native driver path:

        ```python
        pool = create_pool(
            driver_path="/path/to/libduckdb.dylib",
            db_kwargs={"path": "/tmp/my.db"},
            entrypoint="duckdb_adbc_init",
        )
        close_pool(pool)
        ```
    """
    return _create_pool_impl(
        config,
        driver_path,
        db_kwargs,
        entrypoint,
        dbapi_module,
        pool_size,
        max_overflow,
        timeout,
        recycle,
        pre_ping,
    )


def close_pool(pool: sqlalchemy.pool.QueuePool) -> None:
    """
    Close a pool for good, with its idle connections and its ADBC source connection.

    Always call this rather than ``pool.dispose()``: SQLAlchemy's ``dispose()``
    only resets the pool and leaves the ADBC source connection open.

    Closing does not wait for connections that are still checked out. Each one is
    closed when it is checked in, instead of going back into the pool, and
    ``pool.checkedout()`` counts them until then. After closing,
    ``pool.connect()`` raises `PoolClosedError`, and so does a ``connect()`` that
    was already waiting for a free connection. Calling ``close_pool`` again does
    nothing, unless closing the source failed the first time, in which case it
    tries again.

    Args:
        pool: A pool returned by `create_pool`.

    Example:
        ```python
        from adbc_poolhouse import DuckDBConfig, create_pool, close_pool

        pool = create_pool(DuckDBConfig(database="/tmp/wh.db"))
        close_pool(pool)
        ```
    """
    if isinstance(pool, _PoolhouseQueuePool):
        pool._close()  # noqa: SLF001
        if pool._poolhouse_source_closed:  # noqa: SLF001
            return
    else:
        pool.dispose()
    native_backend = getattr(pool, "_native_backend", None)
    if native_backend is not None:
        native_backend.close()
    else:
        pool._adbc_source.close()  # type: ignore[attr-defined]
    if isinstance(pool, _PoolhouseQueuePool):
        pool._poolhouse_source_closed = True  # noqa: SLF001


@overload
def managed_pool(
    config: WarehouseConfig,
    *,
    pool_size: int = 5,
    max_overflow: int = 3,
    timeout: int = 30,
    recycle: int = 3600,
    pre_ping: bool = False,
) -> contextlib.AbstractContextManager[sqlalchemy.pool.QueuePool]: ...


@overload
def managed_pool(
    *,
    driver_path: str,
    db_kwargs: dict[str, str],
    entrypoint: str | None = None,
    pool_size: int = 5,
    max_overflow: int = 3,
    timeout: int = 30,
    recycle: int = 3600,
    pre_ping: bool = False,
) -> contextlib.AbstractContextManager[sqlalchemy.pool.QueuePool]: ...


@overload
def managed_pool(
    *,
    dbapi_module: str,
    db_kwargs: dict[str, str],
    pool_size: int = 5,
    max_overflow: int = 3,
    timeout: int = 30,
    recycle: int = 3600,
    pre_ping: bool = False,
) -> contextlib.AbstractContextManager[sqlalchemy.pool.QueuePool]: ...


@contextlib.contextmanager
def managed_pool(
    config: WarehouseConfig | None = None,
    *,
    driver_path: str | None = None,
    db_kwargs: dict[str, str] | None = None,
    entrypoint: str | None = None,
    dbapi_module: str | None = None,
    pool_size: int | None = None,
    max_overflow: int | None = None,
    timeout: int | None = None,
    recycle: int | None = None,
    pre_ping: bool | None = None,
) -> collections.abc.Generator[sqlalchemy.pool.QueuePool, None, None]:
    """
    Context manager that creates a pool and closes it on exit.

    The pool is created when the ``with`` block is entered and disposed
    (via `close_pool`) when the block exits, whether normally or by
    exception.

    Three call patterns are supported:

        with managed_pool(DuckDBConfig(...)) as pool: ...          # config
        with managed_pool(driver_path="...", ...) as pool: ...     # native
        with managed_pool(dbapi_module="...", ...) as pool: ...    # dbapi

    Args:
        config: A warehouse config model instance (e.g. ``DuckDBConfig``).
            Mutually exclusive with ``driver_path`` and ``dbapi_module``.
        driver_path: Path to a native ADBC driver shared library, or a
            short driver name for manifest-based resolution. Requires
            ``db_kwargs``. Mutually exclusive with ``config`` and
            ``dbapi_module``.
        db_kwargs: ADBC connection keyword arguments as ``dict[str, str]``.
            Required when using ``driver_path`` or ``dbapi_module``.
        entrypoint: ADBC entry-point symbol. Only used with ``driver_path``
            (e.g. ``"duckdb_adbc_init"`` for DuckDB). Default: ``None``.
        dbapi_module: Dotted module name for a Python package implementing
            the ADBC dbapi interface (e.g. ``"adbc_driver_snowflake.dbapi"``
            or a custom ``"my_driver.dbapi"``). Requires ``db_kwargs``.
            Mutually exclusive with ``config`` and ``driver_path``.
        pool_size: Number of connections to keep in the pool. When omitted, the
            config's ``pool_size`` field is used (loaded from keyword arguments or
            environment variables); passing a value overrides it. The raw driver
            paths fall back to 5.
        max_overflow: Extra connections above pool_size. Config field, or 3.
        timeout: Seconds to wait for a connection before raising. Config field, or 30.
        recycle: Seconds before a connection is recycled. Config field, or 3600.
        pre_ping: Whether to ping connections before checkout. Config field, or False.

    Yields:
        A configured ``sqlalchemy.pool.QueuePool``. The pool is automatically
        closed when the ``with`` block exits.

    Raises:
        TypeError: If none of ``config``, ``driver_path``, or ``dbapi_module``
            is provided, or if both ``driver_path`` and ``dbapi_module`` are
            provided.
        ImportError: If the required ADBC driver is not installed.

    Example:
        Config path:

        ```python
        from adbc_poolhouse import DuckDBConfig, managed_pool

        with managed_pool(DuckDBConfig(database="/tmp/wh.db")) as pool:
            with pool.connect() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
        ```

        Raw native driver path:

        ```python
        with managed_pool(
            driver_path="/path/to/libduckdb.dylib",
            db_kwargs={"path": "/tmp/my.db"},
            entrypoint="duckdb_adbc_init",
        ) as pool:
            with pool.connect() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 42")
        ```
    """
    pool = _create_pool_impl(
        config,
        driver_path,
        db_kwargs,
        entrypoint,
        dbapi_module,
        pool_size,
        max_overflow,
        timeout,
        recycle,
        pre_ping,
    )
    try:
        yield pool
    finally:
        close_pool(pool)


def _release_arrow_allocators(
    dbapi_conn: object,
    connection_record: object,
    reset_state: object,
) -> None:
    """
    Close any open cursors to release Arrow record batch readers.

    Registered on the pool ``reset`` event, which fires on all connection
    return paths: normal checkin, invalidation, and error. The ``reset``
    event is preferred over ``checkin`` because ``checkin`` receives
    ``None`` as ``dbapi_conn`` when the connection is invalidated.

    ADBC connections store open cursors in ``_cursors: weakref.WeakSet``.
    Iterating and closing them releases Arrow streaming memory.
    """
    if dbapi_conn is None:
        return
    for cur in list(getattr(dbapi_conn, "_cursors", [])):
        if not getattr(cur, "_closed", True):
            with contextlib.suppress(Exception):
                cur.close()
