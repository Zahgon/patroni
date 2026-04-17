import logging
import os
import re
import shutil
import socket
import stat
import time

from contextlib import contextmanager
from types import TracebackType
from typing import Any, Callable, Collection, Dict, Iterator, List, Optional, Tuple, Type, TYPE_CHECKING, Union
from urllib.parse import parse_qsl, unquote, urlparse

from .. import global_config
from ..collections import CaseInsensitiveDict, CaseInsensitiveSet, EMPTY_DICT
from ..dcs import Leader, Member, RemoteMember, slot_name_from_member_name
from ..exceptions import PatroniFatalException, PostgresConnectionException
from ..file_perm import pg_perm
from ..psycopg import parse_conninfo
from ..utils import compare_values, get_postgres_version, is_subpath, \
    maybe_convert_from_base_unit, parse_bool, parse_int, split_host_port, uri, validate_directory
from ..validator import EnumValidator, IntValidator
from .misc import get_major_from_minor_version, postgres_version_to_int, PostgresqlRole, PostgresqlState
from .validator import recovery_parameters, transform_postgresql_parameter_value, transform_recovery_parameter_value

if TYPE_CHECKING:  # pragma: no cover
    from . import Postgresql

logger = logging.getLogger(__name__)

PARAMETER_RE = re.compile(r'([a-z_]+)\s*=\s*')


def _conninfo_uri_parse(dsn: str) -> Dict[str, str]:
    """
    >>> r = _conninfo_uri_parse('postgresql://u%2Fse:pass@:%2f123/db%2Fsdf?application_name=mya%2Fpp&ssl=true')
    >>> r == {'application_name': 'mya/pp', 'dbname': 'db/sdf', 'sslmode': 'require',\
              'password': 'pass', 'port': '/123', 'user': 'u/se'}
    True
    >>> r = _conninfo_uri_parse('postgresql://u%2Fse:pass@[::1]/db%2Fsdf?application_name=mya%2Fpp&ssl=true')
    >>> r == {'application_name': 'mya/pp', 'dbname': 'db/sdf', 'host': '::1', 'sslmode': 'require',\
              'password': 'pass', 'user': 'u/se'}
    True
    """
    pass


def read_param_value(value: str) -> Union[Tuple[None, None], Tuple[str, int]]:
    pass


def _conninfo_dsn_parse(dsn: str) -> Optional[Dict[str, str]]:
    """
    >>> r = _conninfo_dsn_parse(" host = 'host' dbname = db\\\\ name requiressl=1 ")
    >>> r == {'dbname': 'db name', 'host': 'host', 'requiressl': '1'}
    True
    >>> _conninfo_dsn_parse('requiressl = 0\\\\') == {'requiressl': '0'}
    True
    >>> _conninfo_dsn_parse("host=a foo = '") is None
    True
    >>> _conninfo_dsn_parse("host=a foo = ") is None
    True
    >>> _conninfo_dsn_parse("1") is None
    True
    """
    pass


def _conninfo_parse(value: str) -> Optional[Dict[str, str]]:
    """
    Very simple equivalent of `psycopg2.extensions.parse_dsn` introduced in 2.7.0.
    Exists just for compatibility with 2.5.4+.

    >>> r = _conninfo_parse('postgresql://foo/postgres')
    >>> r == {'dbname': 'postgres', 'host': 'foo'}
    True
    >>> r = _conninfo_parse(" host = 'host' dbname = db\\\\ name requiressl=1 ")
    >>> r == {'dbname': 'db name', 'host': 'host', 'sslmode': 'require'}
    True
    >>> _conninfo_parse('requiressl = 0\\\\') == {'sslmode': 'prefer'}
    True
    """
    pass


def parse_dsn(value: str) -> Optional[Dict[str, str]]:
    """
    Compatibility layer on top of function from psycopg2/psycopg3, which parses connection strings.
    In this function sets the `sslmode`, 'gssencmode', and `channel_binding` to `prefer`
    and `sslnegotiation` to `postgres` if they are not present in the connection string.
    This is necessary to simplify comparison of the old and the new values.

    >>> r = parse_dsn('postgresql://foo/postgres')
    >>> r == {'dbname': 'postgres', 'host': 'foo', 'sslmode': 'prefer', 'gssencmode': 'prefer',\
              'channel_binding': 'prefer', 'sslnegotiation': 'postgres'}
    True
    >>> r = parse_dsn(" host = 'host' dbname = db\\\\ name requiressl=1 ")
    >>> r == {'dbname': 'db name', 'host': 'host', 'sslmode': 'require',\
              'gssencmode': 'prefer', 'channel_binding': 'prefer', 'sslnegotiation': 'postgres'}
    True
    >>> parse_dsn('requiressl = 0\\\\') == {'sslmode': 'prefer', 'gssencmode': 'prefer',\
                                            'channel_binding': 'prefer', 'sslnegotiation': 'postgres'}
    True
    >>> parse_dsn('foo=bar') == {'foo': 'bar', 'sslmode': 'prefer', 'gssencmode': 'prefer',\
                                 'channel_binding': 'prefer', 'sslnegotiation': 'postgres'}
    True
    """
    pass


def strip_comment(value: str) -> str:
    pass


def read_recovery_param_value(value: str) -> Optional[str]:
    """
    >>> read_recovery_param_value('') is None
    True
    >>> read_recovery_param_value("'") is None
    True
    >>> read_recovery_param_value("''a") is None
    True
    >>> read_recovery_param_value('a b') is None
    True
    >>> read_recovery_param_value("'''") is None
    True
    >>> read_recovery_param_value("'\\\\") is None
    True
    >>> read_recovery_param_value("'a' s#") is None
    True
    >>> read_recovery_param_value("'\\\\'''' #a")
    "''"
    >>> read_recovery_param_value('asd')
    'asd'
    """
    pass


def mtime(filename: str) -> Optional[float]:
    pass


class ConfigWriter(object):

    def __init__(self, filename: str) -> None:
        self._filename = filename
        self._fd = None

    def __enter__(self) -> 'ConfigWriter':
        self._fd = open(self._filename, 'w')
        self.writeline('# Do not edit this file manually!\n# It will be overwritten by Patroni!')
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]) -> None:
        if self._fd:
            self._fd.close()

    def writeline(self, line: str) -> None:
        pass

    def writelines(self, lines: List[Optional[str]]) -> None:
        pass

    @staticmethod
    def escape(value: Any) -> str:  # Escape (by doubling) any single quotes or backslashes in given string
        pass

    def write_param(self, param: str, value: Any) -> None:
        pass


def _false_validator(value: Any) -> bool:
    pass


def _bool_validator(value: Any) -> bool:
    pass


def _bool_is_true_validator(value: Any) -> bool:
    pass


def get_param_diff(old_value: Any, new_value: Any,
                   vartype: Optional[str] = None, unit: Optional[str] = None) -> Dict[str, str]:
    """Get a dictionary representing a single PG parameter's value diff.

    :param old_value: current :class:`str` parameter value.
    :param new_value: :class:`str` value of the parameter after a restart.
    :param vartype: the target type to parse old/new_value. See ``vartype`` argument of
        :func:`~patroni.utils.maybe_convert_from_base_unit`.
    :param unit: unit of *old/new_value*. See ``base_unit`` argument of
        :func:`~patroni.utils.maybe_convert_from_base_unit`.

    :returns: a :class:`dict` object that contains two keys: ``old_value`` and ``new_value``
        with their values casted to :class:`str` and converted from base units (if possible).
    """
    pass


class ConfigHandler(object):

    # List of parameters which must be always passed to postmaster as command line options
    # to make it not possible to change them with 'ALTER SYSTEM'.
    # Some of these parameters have sane default value assigned and Patroni doesn't allow
    # to decrease this value. E.g. 'wal_level' can't be lower then 'hot_standby' and so on.
    # These parameters could be changed only globally, i.e. via DCS.
    # P.S. 'listen_addresses' and 'port' are added here just for convenience, to mark them
    # as a parameters which should always be passed through command line.
    #
    # Format:
    #  key - parameter name
    #  value - tuple(default_value, check_function, min_version)
    #    default_value -- some sane default value
    #    check_function -- if the new value is not correct must return `!False`
    #    min_version -- major version of PostgreSQL when parameter was introduced
    CMDLINE_OPTIONS = CaseInsensitiveDict({
        'listen_addresses': (None, _false_validator, 90100),
        'port': (None, _false_validator, 90100),
        'cluster_name': (None, _false_validator, 90500),
        'wal_level': ('hot_standby', EnumValidator(('hot_standby', 'replica', 'logical')), 90100),
        'hot_standby': ('on', _bool_is_true_validator, 90100),
        'max_connections': (100, IntValidator(min=25), 90100),
        'max_wal_senders': (10, IntValidator(min=3), 90100),
        'wal_keep_segments': (8, IntValidator(min=1), 90100),
        'wal_keep_size': ('128MB', IntValidator(min=16, base_unit='MB'), 130000),
        'max_prepared_transactions': (0, IntValidator(min=0), 90100),
        'max_locks_per_transaction': (64, IntValidator(min=32), 90100),
        'track_commit_timestamp': ('off', _bool_validator, 90500),
        'max_replication_slots': (10, IntValidator(min=4), 90400),
        'max_worker_processes': (8, IntValidator(min=2), 90400),
        'wal_log_hints': ('on', _bool_validator, 90400)
    })

    _RECOVERY_PARAMETERS = CaseInsensitiveSet(recovery_parameters.keys())

    def __init__(self, postgresql: 'Postgresql', config: Dict[str, Any]) -> None:
        self._postgresql = postgresql
        self._config_dir = os.path.abspath(config.get('config_dir', '') or postgresql.data_dir)
        config_base_name = config.get('config_base_name', 'postgresql')
        self._postgresql_conf = os.path.join(self._config_dir, config_base_name + '.conf')
        self._postgresql_conf_mtime = None
        self._postgresql_base_conf_name = config_base_name + '.base.conf'
        self._postgresql_base_conf = os.path.join(self._config_dir, self._postgresql_base_conf_name)
        self._pg_hba_conf = os.path.join(self._config_dir, 'pg_hba.conf')
        self._pg_ident_conf = os.path.join(self._config_dir, 'pg_ident.conf')
        self._recovery_conf = os.path.join(postgresql.data_dir, 'recovery.conf')
        self._recovery_conf_mtime = None
        self._recovery_signal = os.path.join(postgresql.data_dir, 'recovery.signal')
        self._standby_signal = os.path.join(postgresql.data_dir, 'standby.signal')
        self._auto_conf = os.path.join(postgresql.data_dir, 'postgresql.auto.conf')
        self._auto_conf_mtime = None
        self._pgpass = os.path.abspath(config.get('pgpass') or os.path.join(os.path.expanduser('~'), 'pgpass'))
        if os.path.exists(self._pgpass) and not os.path.isfile(self._pgpass):
            raise PatroniFatalException("'{0}' exists and it's not a file, check your `postgresql.pgpass` configuration"
                                        .format(self._pgpass))
        self._passfile = None
        self._passfile_mtime = None
        self._postmaster_ctime = None
        self._current_recovery_params: Optional[CaseInsensitiveDict] = None
        self._config = {}
        self._recovery_params = CaseInsensitiveDict()
        self._server_parameters: CaseInsensitiveDict = CaseInsensitiveDict()
        self.reload_config(config)

    def load_current_server_parameters(self) -> None:
        """Read GUC's values from ``pg_settings`` when Patroni is joining the the postgres that is already running."""
        pass

    def setup_server_parameters(self) -> None:
        pass

    def try_to_create_dir(self, d: str, msg: str) -> None:
        pass

    def check_directories(self) -> None:
        pass

    @property
    def config_dir(self) -> str:
        pass

    @property
    def pg_version(self) -> int:
        """Current full postgres version if instance is running, major version otherwise.

        We can only use ``postgres --version`` output if major version there equals to the one
        in data directory. If it is not the case, we should use major version from the ``PG_VERSION``
        file.
        """
        pass

    @property
    def _configuration_to_save(self) -> List[str]:
        pass

    def set_file_permissions(self, filename: str) -> None:
        """Set permissions of file *filename* according to the expected permissions.

        .. note::
            Use original umask if the file is not under PGDATA, use PGDATA
            permissions otherwise.

        :param filename: path to a file which permissions might need to be adjusted.
        """
        pass

    @contextmanager
    def config_writer(self, filename: str) -> Iterator[ConfigWriter]:
        """Create :class:`ConfigWriter` object and set permissions on a *filename*.

        :param filename: path to a config file.

        :yields: :class:`ConfigWriter` object.
        """
        pass

    def save_configuration_files(self, check_custom_bootstrap: bool = False) -> bool:
        """
            copy postgresql.conf to postgresql.conf.backup to be able to retrieve configuration files
            - originally stored as symlinks, those are normally skipped by pg_basebackup
            - in case of WAL-E basebackup (see http://comments.gmane.org/gmane.comp.db.postgresql.wal-e/239)
        """
        pass

    def restore_configuration_files(self) -> None:
        """ restore a previously saved postgresql.conf """
        pass

    def write_postgresql_conf(self, configuration: Optional[CaseInsensitiveDict] = None) -> None:
        # rename the original configuration if it is necessary
        pass

    def append_pg_hba(self, config: List[str]) -> bool:
        pass

    def replace_pg_hba(self) -> Optional[bool]:
        """
        Replace pg_hba.conf content in the PGDATA if hba_file is not defined in the
        `postgresql.parameters` and pg_hba is defined in `postgresql` configuration section.

        :returns: True if pg_hba.conf was rewritten.
        """
        pass

    def replace_pg_ident(self) -> Optional[bool]:
        """
        Replace pg_ident.conf content in the PGDATA if ident_file is not defined in the
        `postgresql.parameters` and pg_ident is defined in the `postgresql` section.

        :returns: True if pg_ident.conf was rewritten.
        """
        pass

    def primary_conninfo_params(self, member: Union[Leader, Member, None]) -> Optional[Dict[str, Any]]:
        pass

    def format_dsn(self, params: Dict[str, Any]) -> str:
        """Format connection string from connection parameters.

        .. note::
            only parameters from the below list are considered and values are escaped.

        :param params: :class:`dict` object with connection parameters.

        :returns: a connection string in a format "key1=value2 key2=value2"
        """
        pass

    def _write_recovery_params(self, fd: ConfigWriter, recovery_params: CaseInsensitiveDict) -> None:
        pass

    def build_recovery_params(self, member: Union[Leader, Member, None]) -> CaseInsensitiveDict:
        pass

    def recovery_conf_exists(self) -> bool:
        pass

    @property
    def triggerfile_good_name(self) -> str:
        pass

    @property
    def _triggerfile_wrong_name(self) -> str:
        pass

    @property
    def _recovery_parameters_to_compare(self) -> CaseInsensitiveSet:
        pass

    def _read_recovery_params(self) -> Tuple[Optional[CaseInsensitiveDict], bool]:
        """Read current recovery parameters values.

        .. note::
            We query Postgres only if we detected that Postgresql was restarted
            or when at least one of the following files was updated:

                * ``postgresql.conf``;
                * ``postgresql.auto.conf``;
                * ``passfile`` that is used in the ``primary_conninfo``.

        :returns: a tuple with two elements:

            * :class:`CaseInsensitiveDict` object with current values of recovery parameters,
              or ``None`` if no configuration files were updated;

            * ``True`` if new values of recovery parameters were queried, ``False`` otherwise.
        """
        pass

    def _read_recovery_params_pre_v12(self) -> Tuple[Optional[CaseInsensitiveDict], bool]:
        pass

    def _check_passfile(self, passfile: str, wanted_primary_conninfo: Dict[str, Any]) -> bool:
        # If there is a passfile in the primary_conninfo try to figure out that
        # the passfile contains the line(s) allowing connection to the given node.
        # We assume that the passfile was created by Patroni and therefore doing
        # the full match and not covering cases when host, port or user are set to '*'
        pass

    def _check_primary_conninfo(self, primary_conninfo: Dict[str, Any],
                                wanted_primary_conninfo: Dict[str, Any]) -> bool:
        # first we will cover corner cases, when we are replicating from somewhere while shouldn't
        # or there is no primary_conninfo but we should replicate from some specific node.
        pass

    def check_recovery_conf(self, member: Union[Leader, Member, None]) -> Tuple[bool, bool]:
        """Returns a tuple. The first boolean element indicates that recovery params don't match
           and the second is set to `True` if the restart is required in order to apply new values"""
        pass

    @staticmethod
    def _remove_file_if_exists(name: str) -> None:
        pass

    @staticmethod
    def _pgpass_content(record: Dict[str, Any]) -> Optional[str]:
        """Generate content of `pgpassfile` based on connection parameters.

        .. note::
            In case if ``host`` is a comma separated string we generate one line per host.

        :param record: :class:`dict` object with connection parameters.
        :returns: a string with generated content of pgpassfile or ``None`` if there is no ``password``.
        """
        pass

    def write_pgpass(self, record: Dict[str, Any]) -> Dict[str, str]:
        """Maybe creates :attr:`_passfile` based on connection parameters.

        :param record: :class:`dict` object with connection parameters.

        :returns: a copy of environment variables, that will include ``PGPASSFILE`` in case if the file was written.
        """
        pass

    def write_recovery_conf(self, recovery_params: CaseInsensitiveDict) -> None:
        pass

    def remove_recovery_conf(self) -> None:
        pass

    def _sanitize_auto_conf(self) -> None:
        pass

    def _adjust_recovery_parameters(self) -> None:
        # It is not strictly necessary, but we can make patroni configs crossi-compatible with all postgres versions.
        pass

    def get_server_parameters(self, config: Dict[str, Any]) -> CaseInsensitiveDict:
        pass

    @staticmethod
    def _get_unix_local_address(unix_socket_directories: str) -> str:
        pass

    def _get_tcp_local_address(self) -> str:
        pass

    def resolve_connection_addresses(self) -> None:
        """Calculates and sets local and remote connection urls and options.

        This method sets:
            * :attr:`Postgresql.connection_string <patroni.postgresql.Postgresql.connection_string>` attribute, which
              is later written to the member key in DCS as ``conn_url``.
            * :attr:`ConfigHandler.local_replication_address` attribute, which is used for replication connections to
              local postgres.
            * :attr:`ConnectionPool.conn_kwargs <patroni.postgresql.connection.ConnectionPool.conn_kwargs>` attribute,
              which is used for superuser connections to local postgres.

        .. note::
            If there is a valid directory in ``postgresql.parameters.unix_socket_directories`` in the Patroni
            configuration and ``postgresql.use_unix_socket`` and/or ``postgresql.use_unix_socket_repl``
            are set to ``True``, we respectively use unix sockets for superuser and replication connections
            to local postgres.

            If there is a requirement to use unix sockets, but nothing is set in the
            ``postgresql.parameters.unix_socket_directories``, we omit a ``host`` in connection parameters relying
            on the ability of ``libpq`` to connect via some default unix socket directory.

            If unix sockets are not requested we "switch" to TCP, preferring to use ``localhost`` if it is possible
            to deduce that Postgres is listening on a local interface address.

            Otherwise we just used the first address specified in the ``listen_addresses`` GUC.
        """
        pass

    def _get_pg_settings(self, names: Collection[str]) -> Dict[Any, Tuple[Any, ...]]:
        pass

    @staticmethod
    def _handle_wal_buffers(old_values: Dict[Any, Tuple[Any, ...]], changes: CaseInsensitiveDict) -> None:
        pass

    def reload_config(self, config: Dict[str, Any], sighup: bool = False) -> None:
        pass

    def set_synchronous_standby_names(self, value: Optional[str]) -> Optional[bool]:
        """Updates synchronous_standby_names and reloads if necessary.
        :returns: True if value was updated."""
        pass

    @property
    def effective_configuration(self) -> CaseInsensitiveDict:
        """It might happen that the current value of one (or more) below parameters stored in
        the controldata is higher than the value stored in the global cluster configuration.

        Example: max_connections in global configuration is 100, but in controldata
        `Current max_connections setting: 200`. If we try to start postgres with
        max_connections=100, it will immediately exit.
        As a workaround we will start it with the values from controldata and set `pending_restart`
        to true as an indicator that current values of parameters are not matching expectations."""
        pass

    @property
    def replication(self) -> Dict[str, Any]:
        pass

    @property
    def superuser(self) -> Dict[str, Any]:
        pass

    @property
    def rewind_credentials(self) -> Dict[str, Any]:
        pass

    @property
    def ident_file(self) -> Optional[str]:
        pass

    @property
    def hba_file(self) -> Optional[str]:
        pass

    @property
    def pg_hba_conf(self) -> str:
        pass

    @property
    def postgresql_conf(self) -> str:
        pass

    def get(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        return self._config.get(key, default)

    def restore_command(self) -> Optional[str]:
        pass

    @property
    def synchronous_standby_names(self) -> Optional[str]:
        """Get ``synchronous_standby_names`` value configured by the user.

        :returns: value of ``synchronous_standby_names`` in the Patroni configuration,
            if any, otherwise ``None``.
        """
        pass
