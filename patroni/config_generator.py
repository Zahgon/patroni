"""patroni ``--generate-config`` machinery."""
import abc
import logging
import os
import socket
import sys

from contextlib import contextmanager
from getpass import getpass, getuser
from typing import Any, Dict, Iterator, List, Optional, TextIO, Tuple, TYPE_CHECKING, Union

import psutil
import yaml

if TYPE_CHECKING:  # pragma: no cover
    from psycopg import Cursor
    from psycopg2 import cursor

from . import psycopg
from .collections import EMPTY_DICT
from .config import Config
from .exceptions import PatroniException
from .log import PatroniLogger
from .postgresql.config import ConfigHandler, parse_dsn
from .postgresql.misc import postgres_major_version_to_int
from .utils import get_major_version, parse_bool, patch_config, read_stripped

# Mapping between the libpq connection parameters and the environment variables.
# This dict should be kept in sync with `patroni.utils._AUTH_ALLOWED_PARAMETERS`
# (we use "username" in the Patroni config for some reason, other parameter names are the same).
_AUTH_ALLOWED_PARAMETERS_MAPPING = {
    'user': 'PGUSER',
    'password': 'PGPASSWORD',
    'sslmode': 'PGSSLMODE',
    'sslcert': 'PGSSLCERT',
    'sslkey': 'PGSSLKEY',
    'sslpassword': '',
    'sslrootcert': 'PGSSLROOTCERT',
    'sslcrl': 'PGSSLCRL',
    'sslcrldir': 'PGSSLCRLDIR',
    'gssencmode': 'PGGSSENCMODE',
    'channel_binding': 'PGCHANNELBINDING',
    'sslnegotiation': 'PGSSLNEGOTIATION'
}
NO_VALUE_MSG = '#FIXME'


def get_address() -> Tuple[str, str]:
    """Try to get hostname and the ip address for it returned by :func:`~socket.gethostname`.

    .. note::
        Can also return local ip.

    :returns: tuple consisting of the hostname returned by :func:`~socket.gethostname`
        and the first element in the sorted list of the addresses returned by :func:`~socket.getaddrinfo`.
        Sorting guarantees it will prefer IPv4.
        If an exception occurred, hostname and ip values are equal to :data:`~patroni.config_generator.NO_VALUE_MSG`.
    """
    pass


class AbstractConfigGenerator(abc.ABC):
    """Object representing the generated Patroni config.

    :ivar output_file: full path to the output file to be used.
    :ivar pg_major: integer representation of the major PostgreSQL version.
    :ivar config: dictionary used for the generated configuration storage.
    """

    def __init__(self, output_file: Optional[str]) -> None:
        """Set up the output file (if passed), helper vars and the minimal config structure.

        :param output_file: full path to the output file to be used.
        """
        self.output_file = output_file
        self.pg_major = 0
        self.config = self.get_template_config()

        self.generate()

    @classmethod
    def get_template_config(cls) -> Dict[str, Any]:
        """Generate a template config for further extension (e.g. in the inherited classes).

        :returns: dictionary with the values gathered from Patroni env, hopefully defined hostname and ip address
                  (otherwise set to :data:`~patroni.config_generator.NO_VALUE_MSG`), and some sane defaults.
        """
        pass

    @abc.abstractmethod
    def generate(self) -> None:
        """Generate config and store in :attr:`~AbstractConfigGenerator.config`."""

    @staticmethod
    def _format_block(block: Any, line_prefix: str = '') -> str:
        """Format a single YAML block.

        .. note::
            Optionally the formatted block could be indented with the *line_prefix*

        :param block: the object that should be formatted to YAML.
        :param line_prefix: is used for indentation.

        :returns: a formatted and indented *block*.
        """
        pass

    def _format_config_section(self, section_name: str) -> Iterator[str]:
        """Format and yield as single section of the current :attr:`~AbstractConfigGenerator.config`.

        .. note::
            If the section is a :class:`dict` object we put an empty line before it.

        :param section_name: a section name in the :attr:`~AbstractConfigGenerator.config`.

        :yields: a formatted section in case if it exists in the :attr:`~AbstractConfigGenerator.config`.
        """
        pass

    def _format_config(self) -> Iterator[str]:
        """Format current :attr:`~AbstractConfigGenerator.config` and enrich it with some comments.

        :yields: formatted lines or blocks that represent a text output of the YAML document.
        """
        pass

    def _write_config_to_fd(self, fd: TextIO) -> None:
        """Format and write current :attr:`~AbstractConfigGenerator.config` to provided file descriptor.

        :param fd: where to write the config file. Could be ``sys.stdout`` or the real file.
        """
        pass

    def write_config(self) -> None:
        """Write current :attr:`~AbstractConfigGenerator.config` to the output file if provided, to stdout otherwise."""
        pass


class SampleConfigGenerator(AbstractConfigGenerator):
    """Object representing the generated sample Patroni config.

    Sane defaults are used based on the gathered PG version.
    """

    @property
    def get_auth_method(self) -> str:
        """Return the preferred authentication method for a specific PG version if provided or the default ``md5``.

        :returns: :class:`str` value for the preferred authentication method.
        """
        pass

    def _get_int_major_version(self) -> int:
        """Get major PostgreSQL version from the binary as an integer.

        :returns: an integer PostgreSQL major version representation gathered from the PostgreSQL binary.
                  See :func:`~patroni.postgresql.misc.postgres_major_version_to_int` and
                  :func:`~patroni.utils.get_major_version`.
        """
        pass

    def generate(self) -> None:
        """Generate sample config using some sane defaults and update :attr:`~AbstractConfigGenerator.config`."""
        pass


class RunningClusterConfigGenerator(AbstractConfigGenerator):
    """Object representing the Patroni config generated using information gathered from the running instance.

    :ivar dsn: DSN string for the local instance to get GUC values from (if provided).
    :ivar parsed_dsn: DSN string parsed into a dictionary (see :func:`~patroni.postgresql.config.parse_dsn`).
    """

    def __init__(self, output_file: Optional[str] = None, dsn: Optional[str] = None) -> None:
        """Additionally store the passed dsn (if any) in both original and parsed version and run config generation.

        :param output_file: full path to the output file to be used.
        :param dsn: DSN string for the local instance to get GUC values from.

        :raises:
            :exc:`~patroni.exceptions.PatroniException`: if DSN parsing failed.
        """
        self.dsn = dsn
        self.parsed_dsn = {}

        super().__init__(output_file)

    @property
    def _get_hba_conn_types(self) -> Tuple[str, ...]:
        """Return the connection types allowed.

        If :attr:`~RunningClusterConfigGenerator.pg_major` is defined, adds additional parameters
        for PostgreSQL version >=16.

        :returns: tuple of the connection methods allowed.
        """
        pass

    @property
    def _required_pg_params(self) -> List[str]:
        """PG configuration parameters that have to be always present in the generated config.

        :returns: list of the parameter names.
        """
        pass

    def _get_bin_dir_from_running_instance(self) -> str:
        """Define the directory postgres binaries reside using postmaster's pid executable.

        :returns: path to the PostgreSQL binaries directory.

        :raises:
            :exc:`~patroni.exceptions.PatroniException`: if:

                * pid could not be obtained from the ``postmaster.pid`` file; or
                * :exc:`OSError` occurred during ``postmaster.pid`` file handling; or
                * the obtained postmaster pid doesn't exist.
        """
        pass

    @contextmanager
    def _get_connection_cursor(self) -> Iterator[Union['cursor', 'Cursor[Any]']]:
        """Get cursor for the PG connection established based on the stored information.

        :raises:
            :exc:`~patroni.exceptions.PatroniException`: if :exc:`psycopg.Error` occurred.
        """
        pass

    def _set_pg_params(self, cur: Union['cursor', 'Cursor[Any]']) -> None:
        """Extend :attr:`~RunningClusterConfigGenerator.config` with the actual PG GUCs values.

        THe following GUC values are set:

            * Non-internal having configuration file, postmaster command line or environment variable
              as a source.

            * List of the always required parameters (see :meth:`~RunningClusterConfigGenerator._required_pg_params`).

        :param cur: connection cursor to use.
        """
        pass

    def _set_su_params(self) -> None:
        """Extend :attr:`~RunningClusterConfigGenerator.config` with the superuser auth information.

        Information set is based on the options used for connection.
        """
        pass

    def _set_conf_files(self) -> None:
        """Extend :attr:`~RunningClusterConfigGenerator.config` with ``pg_hba.conf`` and ``pg_ident.conf`` content.

        .. note::
            This function only defines ``postgresql.pg_hba`` and ``postgresql.pg_ident`` when
            ``hba_file`` and ``ident_file`` are set to the defaults. It may happen these files
            are located outside of ``PGDATA`` and Patroni doesn't have write permissions for them.

        :raises:
            :exc:`~patroni.exceptions.PatroniException`: if :exc:`OSError` occurred during the conf files handling.
        """
        pass

    def _enrich_config_from_running_instance(self) -> None:
        """Extend :attr:`~RunningClusterConfigGenerator.config` with the values gathered from the running instance.

        Retrieve the following information from the running PostgreSQL instance:

        * superuser auth parameters (see :meth:`~RunningClusterConfigGenerator._set_su_params`);
        * some GUC values (see :meth:`~RunningClusterConfigGenerator._set_pg_params`);
        * ``postgresql.connect_address``, ``postgresql.listen``;
        * ``postgresql.pg_hba`` and ``postgresql.pg_ident`` (see :meth:`~RunningClusterConfigGenerator._set_conf_files`)

        And redefine ``scope`` with the ``cluster_name`` GUC value if set.

        :raises:
            :exc:`~patroni.exceptions.PatroniException`: if the provided user doesn't have superuser privileges.
        """
        pass

    def generate(self) -> None:
        """Generate config using the info gathered from the specified running PG instance.

        Result is written to :attr:`~RunningClusterConfigGenerator.config`.
        """
        pass


def generate_config(output_file: str, sample: bool, dsn: Optional[str]) -> None:
    """Generate Patroni configuration file.

    :param output_file: Full path to the configuration file to be used. If not provided, result is sent to ``stdout``.
    :param sample: Optional flag. If set, no source instance will be used - generate config with some sane defaults.
    :param dsn: Optional DSN string for the local instance to get GUC values from.
    """
    pass
