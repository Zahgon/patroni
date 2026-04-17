import logging

from contextlib import contextmanager
from threading import Lock
from typing import Any, Dict, Iterator, List, Optional, Tuple, TYPE_CHECKING, Union

if TYPE_CHECKING:  # pragma: no cover
    from psycopg import Connection, Cursor
    from psycopg2 import connection, cursor

from .. import psycopg
from ..exceptions import PostgresConnectionException

logger = logging.getLogger(__name__)


class NamedConnection:
    """Helper class to manage ``psycopg`` connections from Patroni to PostgreSQL.

    :ivar server_version: PostgreSQL version in integer format where we are connected to.
    """

    server_version: int

    def __init__(self, pool: 'ConnectionPool', name: str, kwargs_override: Optional[Dict[str, Any]]) -> None:
        """Create an instance of :class:`NamedConnection` class.

        :param pool: reference to a :class:`ConnectionPool` object.
        :param name: name of the connection.
        :param kwargs_override: :class:`dict` object with connection parameters that should be
                                different from default values provided by connection *pool*.
        """
        self._pool = pool
        self._name = name
        self._kwargs_override = kwargs_override or {}
        self._lock = Lock()  # used to make sure that only one connection to postgres is established
        self._connection = None

    @property
    def _conn_kwargs(self) -> Dict[str, Any]:
        """Connection parameters for this :class:`NamedConnection`."""
        pass

    def get(self) -> Union['connection', 'Connection[Any]']:
        """Get ``psycopg``/``psycopg2`` connection object.

        .. note::
            Opens a new connection if necessary.

        :returns: ``psycopg`` or ``psycopg2`` connection object.
        """
        with self._lock:
            if not self._connection or self._connection.closed != 0:
                logger.info("establishing a new patroni %s connection to postgres", self._name)
                self._connection = psycopg.connect(**self._conn_kwargs)
                self.server_version = getattr(self._connection, 'server_version', 0)
        return self._connection

    def query(self, sql: str, *params: Any) -> List[Tuple[Any, ...]]:
        """Execute a query with parameters and optionally returns a response.

        :param sql: SQL statement to execute.
        :param params: parameters to pass.

        :returns: a query response as a list of tuples if there is any.
        :raises:
            :exc:`~psycopg.Error` if had issues while executing *sql*.

            :exc:`~patroni.exceptions.PostgresConnectionException`: if had issues while connecting to the database.
        """
        pass

    def close(self, silent: bool = False) -> bool:
        """Close the psycopg connection to postgres.

        :param silent: whether the method should not write logs.

        :returns: ``True`` if ``psycopg`` connection was closed, ``False`` otherwise.``
        """
        pass


class ConnectionPool:
    """Helper class to manage named connections from Patroni to PostgreSQL.

    The instance keeps named :class:`NamedConnection` objects and parameters that must be used for new connections.
    """

    def __init__(self) -> None:
        """Create an instance of :class:`ConnectionPool` class."""
        self._lock = Lock()
        self._connections: Dict[str, NamedConnection] = {}
        self._conn_kwargs: Dict[str, Any] = {}

    @property
    def conn_kwargs(self) -> Dict[str, Any]:
        """Connection parameters that must be used for new ``psycopg`` connections."""
        pass

    @conn_kwargs.setter
    def conn_kwargs(self, value: Dict[str, Any]) -> None:
        """Set new connection parameters.

        :param value: :class:`dict` object with connection parameters.
        """
        pass

    def get(self, name: str, kwargs_override: Optional[Dict[str, Any]] = None) -> NamedConnection:
        """Get a new named :class:`NamedConnection` object from the pool.

        .. note::
            Creates a new :class:`NamedConnection` object if it doesn't yet exist in the pool.

        :param name: name of the connection.
        :param kwargs_override: :class:`dict` object with connection parameters that should be
                                different from default values provided by :attr:`conn_kwargs`.

        :returns: :class:`NamedConnection` object.
        """
        with self._lock:
            if name not in self._connections:
                self._connections[name] = NamedConnection(self, name, kwargs_override)
        return self._connections[name]

    def close(self) -> None:
        """Close all named connections from Patroni to PostgreSQL registered in the pool."""
        pass


@contextmanager
def get_connection_cursor(**kwargs: Any) -> Iterator[Union['cursor', 'Cursor[Any]']]:
    pass
