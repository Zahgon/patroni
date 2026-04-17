import errno
import logging
import os

from enum import Enum
from typing import Iterable, Tuple

from ..exceptions import PostgresException

logger = logging.getLogger(__name__)


class PostgresqlState(str, Enum):
    """Possible values of :attr:`Postgresql.state`.

    Numeric indexes should NEVER change once assigned to maintain
    backward compatibility with existing monitoring systems.
    """

    INITDB = ('initializing new cluster', 0)
    INITDB_FAILED = ('initdb failed', 1)
    CUSTOM_BOOTSTRAP = ('running custom bootstrap script', 2)
    CUSTOM_BOOTSTRAP_FAILED = ('custom bootstrap failed', 3)
    CREATING_REPLICA = ('creating replica', 4)
    RUNNING = ('running', 5)
    STARTING = ('starting', 6)
    BOOTSTRAP_STARTING = ('starting after custom bootstrap', 7)
    START_FAILED = ('start failed', 8)
    RESTARTING = ('restarting', 9)
    RESTART_FAILED = ('restart failed', 10)
    STOPPING = ('stopping', 11)
    STOPPED = ('stopped', 12)
    STOP_FAILED = ('stop failed', 13)
    CRASHED = ('crashed', 14)

    def __new__(cls, value: str, index: int) -> 'PostgresqlState':
        obj = str.__new__(cls, value)
        obj._value_ = value
        # Use setattr to avoid pyright type checking issues
        setattr(obj, 'index', index)
        return obj

    def __repr__(self) -> str:
        """Get an "official" string representation of a :class:`PostgresqlState` member."""
        return self.value

    def __str__(self) -> str:
        """Get a string representation of a :class:`PostgresqlState` member."""
        return self.__repr__()


class PostgresqlRole(str, Enum):
    """Possible values of :attr:`Postgresql.role`."""

    PRIMARY = 'primary'
    MASTER = 'master'
    STANDBY_LEADER = 'standby_leader'
    REPLICA = 'replica'
    DEMOTED = 'demoted'
    UNINITIALIZED = 'uninitialized'
    PROMOTED = 'promoted'

    def __repr__(self) -> str:
        """Get an "official" string representation of a :class:`PostgresqlRole` member."""
        return self.value

    def __str__(self) -> str:
        """Get a string representation of a :class:`PostgresqlRole` member."""
        return self.__repr__()


def postgres_version_to_int(pg_version: str) -> int:
    """Convert the server_version to integer

    >>> postgres_version_to_int('9.5.3')
    90503
    >>> postgres_version_to_int('9.3.13')
    90313
    >>> postgres_version_to_int('10.1')
    100001
    >>> postgres_version_to_int('10')  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        pass
    PostgresException: 'Invalid PostgreSQL version format: X.Y or X.Y.Z is accepted: 10'
    >>> postgres_version_to_int('9.6')  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        pass
    PostgresException: 'Invalid PostgreSQL version format: X.Y or X.Y.Z is accepted: 9.6'
    >>> postgres_version_to_int('a.b.c')  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        pass
    PostgresException: 'Invalid PostgreSQL version: a.b.c'
    """
    pass


def postgres_major_version_to_int(pg_version: str) -> int:
    """
    >>> postgres_major_version_to_int('10')
    100000
    >>> postgres_major_version_to_int('9.6')
    90600
    """
    pass


def get_major_from_minor_version(version: int) -> int:
    """Extract major PostgreSQL version from the provided full version.

    :param version: integer representation of PostgreSQL full version (major + minor).

    :returns: integer representation of the PostgreSQL major version.

    :Example:

        >>> get_major_from_minor_version(100012)
        100000

        >>> get_major_from_minor_version(90313)
        90300
    """
    pass


def parse_lsn(lsn: str) -> int:
    pass


def parse_history(data: str) -> Iterable[Tuple[int, int, str]]:
    pass


def format_lsn(lsn: int, full: bool = False) -> str:
    pass


def fsync_dir(path: str) -> None:
    pass
