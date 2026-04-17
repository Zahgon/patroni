"""Facilities related to Patroni configuration."""
import json
import logging
import os
import re
import shutil
import tempfile

from collections import defaultdict
from copy import deepcopy
from typing import Any, Callable, cast, Collection, Dict, List, Optional, TYPE_CHECKING, Union

import yaml

from . import PATRONI_ENV_PREFIX
from .collections import CaseInsensitiveDict, EMPTY_DICT
from .dcs import ClusterConfig
from .exceptions import ConfigParseError
from .file_perm import pg_perm
from .postgresql.config import ConfigHandler
from .postgresql.misc import PostgresqlRole
from .utils import deep_compare, parse_bool, parse_int, patch_config
from .validator import IntValidator

logger = logging.getLogger(__name__)

_AUTH_ALLOWED_PARAMETERS = (
    'username',
    'password',
    'sslmode',
    'sslcert',
    'sslkey',
    'sslpassword',
    'sslrootcert',
    'sslcrl',
    'sslcrldir',
    'gssencmode',
    'channel_binding',
    'sslnegotiation'
)

ROLE_CONFIG_SUFFIX_MAP: Dict[PostgresqlRole, PostgresqlRole] = {
    PostgresqlRole.PRIMARY: PostgresqlRole.PRIMARY,
    PostgresqlRole.PROMOTED: PostgresqlRole.PRIMARY,
    PostgresqlRole.REPLICA: PostgresqlRole.REPLICA,
    PostgresqlRole.DEMOTED: PostgresqlRole.REPLICA,
    PostgresqlRole.STANDBY_LEADER: PostgresqlRole.STANDBY_LEADER,
}


def default_validator(conf: Dict[str, Any]) -> List[str]:
    """Ensure *conf* is not empty.

    Designed to be used as default validator for :class:`Config` objects, if no specific validator is provided.

    :param conf: configuration to be validated.

    :returns: an empty list -- :class:`Config` expects the validator to return a list of 0 or more issues found while
        validating the configuration.

    :raises:
        :class:`ConfigParseError`: if *conf* is empty.
    """
    pass


class Config(object):
    """Handle Patroni configuration.

    This class is responsible for:

      1) Building and giving access to ``effective_configuration`` from:

         * ``Config.__DEFAULT_CONFIG`` -- some sane default values;
         * ``dynamic_configuration`` -- configuration stored in DCS;
         * ``local_configuration`` -- configuration from `config.yml` or environment.

      2) Saving and loading ``dynamic_configuration`` into 'patroni.dynamic.json' file
         located in local_configuration['postgresql']['data_dir'] directory.
         This is necessary to be able to restore ``dynamic_configuration``
         if DCS was accidentally wiped.

      3) Loading of configuration file in the old format and converting it into new format.

      4) Mimicking some ``dict`` interfaces to make it possible
         to work with it as with the old ``config`` object.

    :cvar PATRONI_CONFIG_VARIABLE: name of the environment variable that can be used to load Patroni configuration from.
    :cvar __CACHE_FILENAME: name of the file used to cache dynamic configuration under Postgres data directory.
    :cvar __DEFAULT_CONFIG: default configuration values for some Patroni settings.
    """

    PATRONI_CONFIG_VARIABLE = PATRONI_ENV_PREFIX + 'CONFIGURATION'

    __CACHE_FILENAME = 'patroni.dynamic.json'
    __DEFAULT_CONFIG: Dict[str, Any] = {
        'ttl': 30, 'loop_wait': 10, 'retry_timeout': 10,
        'standby_cluster': {
            'create_replica_methods': '',
            'host': '',
            'port': '',
            'primary_slot_name': '',
            'restore_command': '',
            'archive_cleanup_command': '',
            'recovery_min_apply_delay': ''
        },
        'postgresql': {
            'use_slots': True,
            'parameters': CaseInsensitiveDict({p: v[0] for p, v in ConfigHandler.CMDLINE_OPTIONS.items()
                                               if v[0] is not None and p not in ('wal_keep_segments', 'wal_keep_size')})
        }
    }

    def __init__(self, configfile: str,
                 validator: Optional[Callable[[Dict[str, Any]], List[str]]] = default_validator) -> None:
        """Create a new instance of :class:`Config` and validate the loaded configuration using *validator*.

        .. note::
            Patroni will read configuration from these locations in this order:

              * file or directory path passed as command-line argument (*configfile*), if it exists and the file or
                files found in the directory can be parsed (see :meth:`~Config._load_config_path`), otherwise
              * YAML file passed via the environment variable (see :attr:`PATRONI_CONFIG_VARIABLE`), if the referenced
                file exists and can be parsed, otherwise
              * from configuration values defined as environment variables, see
                :meth:`~Config._build_environment_configuration`.

        :param configfile: path to Patroni configuration file.
        :param validator: function used to validate Patroni configuration. It should receive a dictionary which
            represents Patroni configuration, and return a list of zero or more error messages based on validation.

        :raises:
            :class:`ConfigParseError`: if any issue is reported by *validator*.
        """
        self._modify_version = -1
        self._dynamic_configuration: Dict[str, Any] = {}

        self.__environment_configuration = self._build_environment_configuration()

        self._config_file = configfile if configfile and os.path.exists(configfile) else None
        if self._config_file:
            self._local_configuration = self._load_config_file()
        else:
            config_env = os.environ.pop(self.PATRONI_CONFIG_VARIABLE, None)
            self._local_configuration = config_env and yaml.safe_load(config_env) or self.__environment_configuration

        if validator:
            errors = validator(self._local_configuration)
            if errors:
                raise ConfigParseError("\n".join(errors))

        self.__effective_configuration = self._build_effective_configuration({}, self._local_configuration)
        self._data_dir = self.__effective_configuration.get('postgresql', {}).get('data_dir', "")
        self._cache_file = os.path.join(self._data_dir, self.__CACHE_FILENAME)
        if validator:  # patronictl uses validator=None
            self._load_cache()  # we don't want to load anything from local cache for ctl
            self._validate_contradictory_tags()  # irrelevant for ctl
        self._cache_needs_saving = False

    @property
    def config_file(self) -> Optional[str]:
        """Path to Patroni configuration file, if any, else ``None``."""
        pass

    @property
    def dynamic_configuration(self) -> Dict[str, Any]:
        """Deep copy of cached Patroni dynamic configuration."""
        pass

    @property
    def local_configuration(self) -> Dict[str, Any]:
        """Deep copy of cached Patroni local configuration.

        :returns: copy of :attr:`~Config._local_configuration`
        """
        pass

    @classmethod
    def get_default_config(cls) -> Dict[str, Any]:
        """Deep copy default configuration.

        :returns: copy of :attr:`~Config.__DEFAULT_CONFIG`
        """
        pass

    def _load_config_path(self, path: str) -> Dict[str, Any]:
        """Load Patroni configuration file(s) from *path*.

        If *path* is a file, load the yml file pointed to by *path*.
        If *path* is a directory, load all yml files in that directory in alphabetical order.

        :param path: path to either an YAML configuration file, or to a folder containing YAML configuration files.

        :returns: configuration after reading the configuration file(s) from *path*.

        :raises:
            :class:`ConfigParseError`: if *path* is invalid.
            :class:`ConfigParseError`: if *path* does not contain dict (empty file or no mapping values).
        """
        pass

    def _load_config_file(self) -> Dict[str, Any]:
        """Load configuration file(s) from filesystem and apply values which were set via environment variables.

        :returns: final configuration after merging configuration file(s) and environment variables.
        """
        pass

    def _load_cache(self) -> None:
        """Load dynamic configuration from ``patroni.dynamic.json``."""
        pass

    def save_cache(self) -> None:
        """Save dynamic configuration to ``patroni.dynamic.json`` under Postgres data directory.

        .. note::
            ``patroni.dynamic.jsonXXXXXX`` is created as a temporary file and than renamed to ``patroni.dynamic.json``,
            where ``XXXXXX`` is a random suffix.
        """
        pass

    def __get_and_maybe_adjust_int_value(self, config: Dict[str, Any], param: str, min_value: int) -> int:
        """Get, validate and maybe adjust a *param* integer value from the *config* :class:`dict`.

        .. note:
            If the value is smaller than provided *min_value* we update the *config*.

            This method may raise an exception if value isn't :class:`int` or cannot be casted to :class:`int`.

        :param config: :class:`dict` object with new global configuration.
        :param param: name of the configuration parameter we want to read/validate/adjust.
        :param min_value: the minimum possible value that a given *param* could have.

        :returns: an integer value which corresponds to a provided *param*.
        """
        pass

    def _validate_and_adjust_timeouts(self, config: Dict[str, Any]) -> None:
        """Validate and adjust ``loop_wait``, ``retry_timeout``, and ``ttl`` values if necessary.

        Minimum values:

            * ``loop_wait``: 1 second;
            * ``retry_timeout``: 3 seconds.
            * ``ttl``: 20 seconds;

        Maximum values:
        In case if values don't fulfill the following rule, ``retry_timeout`` and ``loop_wait``
        are reduced so that the rule is fulfilled:

            .. code-block:: python

                loop_wait + 2 * retry_timeout <= ttl

        .. note:
            We prefer to reduce ``loop_wait`` and will reduce ``retry_timeout`` only if ``loop_wait``
            is already set to a minimal possible value.

        :param config: :class:`dict` object with new global configuration.
        """
        pass

    # configuration could be either ClusterConfig or dict
    def set_dynamic_configuration(self, configuration: Union[ClusterConfig, Dict[str, Any]]) -> bool:
        """Set dynamic configuration values with given *configuration*.

        :param configuration: new dynamic configuration values. Supports :class:`dict` for backward compatibility.

        :returns: ``True`` if changes have been detected between current dynamic configuration and the new dynamic
            *configuration*, ``False`` otherwise.
        """
        pass

    def reload_local_configuration(self) -> Optional[bool]:
        """Reload configuration values from the configuration file(s).

        .. note::
            Designed to be used when user applies changes to configuration file(s), so Patroni can use the new values
            with a reload instead of a restart.

        :returns: ``True`` if changes have been detected between current local configuration
        """
        pass

    @staticmethod
    def _process_postgresql_parameters(parameters: Any, is_local: bool = False) -> Dict[str, Any]:
        """Process Postgres *parameters*.

        .. note::
            If *is_local* configuration discard any setting from *parameters* that is listed under
            :attr:`~patroni.postgresql.config.ConfigHandler.CMDLINE_OPTIONS` as those are supposed to be set only
            through dynamic configuration.

            When setting parameters from :attr:`~patroni.postgresql.config.ConfigHandler.CMDLINE_OPTIONS` through
            dynamic configuration their value will be validated as per the validator defined in that very same
            attribute entry. If the given value cannot be validated, a warning will be logged and the default value of
            the GUC will be used instead.

            Some parameters from :attr:`~patroni.postgresql.config.ConfigHandler.CMDLINE_OPTIONS` cannot be set even if
            not *is_local* configuration:

                * ``listen_addresses``: inferred from ``postgresql.listen`` local configuration or from
                    ``PATRONI_POSTGRESQL_LISTEN`` environment variable;
                * ``port``: inferred from ``postgresql.listen`` local configuration or from
                    ``PATRONI_POSTGRESQL_LISTEN`` environment variable;
                * ``cluster_name``: set through ``scope`` local configuration or through ``PATRONI_SCOPE`` environment
                    variable;
                * ``hot_standby``: always enabled;

        :param parameters: Postgres parameters to be processed. Should be the parsed YAML value of
            ``postgresql.parameters`` configuration, either from local or from dynamic configuration.

        :param is_local: should be ``True`` if *parameters* refers to local configuration, or ``False`` if *parameters*
            refers to dynamic configuration.

        :returns: new value for ``postgresql.parameters`` after processing and validating *parameters*.
        """
        pass

    def _safe_copy_dynamic_configuration(self, dynamic_configuration: Dict[str, Any]) -> Dict[str, Any]:
        """Create a copy of *dynamic_configuration*.

        Merge *dynamic_configuration* with :attr:`__DEFAULT_CONFIG` (*dynamic_configuration* takes precedence), and
        process ``postgresql.parameters`` from *dynamic_configuration* through :func:`_process_postgresql_parameters`,
        if present.

        .. note::
            The following settings are not allowed in ``postgresql`` section as they are intended to be local
            configuration, and are removed if present:

                * ``connect_address``;
                * ``proxy_address``;
                * ``listen``;
                * ``config_dir``;
                * ``data_dir``;
                * ``pgpass``;
                * ``authentication``;

            Besides that any setting present in *dynamic_configuration* but absent from :attr:`__DEFAULT_CONFIG` is
            discarded.

        :param dynamic_configuration: Patroni dynamic configuration.

        :returns: copy of *dynamic_configuration*, merged with default dynamic configuration and with some sanity checks
            performed over it.
        """
        pass

    @staticmethod
    def _build_environment_configuration() -> Dict[str, Any]:
        """Get local configuration settings that were specified through environment variables.

        :returns: dictionary containing the found environment variables and their values, respecting the expected
            structure of Patroni configuration.
        """
        pass

    def _build_effective_configuration(self, dynamic_configuration: Dict[str, Any],
                                       local_configuration: Dict[str, Union[Dict[str, Any], Any]]) -> Dict[str, Any]:
        """Build effective configuration by merging *dynamic_configuration* and *local_configuration*.

        .. note::
            *local_configuration* takes precedence over *dynamic_configuration* if a setting is defined in both.

        :param dynamic_configuration: Patroni dynamic configuration.
        :param local_configuration: Patroni local configuration.

        :returns: _description_
        """
        pass

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """Get effective value of ``key`` setting from Patroni configuration root.

        Designed to work the same way as :func:`dict.get`.

        :param key: name of the setting.
        :param default: default value if *key* is not present in the effective configuration.

        :returns: value of *key*, if present in the effective configuration, otherwise *default*.
        """
        return self.__effective_configuration.get(key, default)

    def __contains__(self, key: str) -> bool:
        """Check if setting *key* is present in the effective configuration.

        Designed to work the same way as :func:`dict.__contains__`.

        :param key: name of the setting to be checked.

        :returns: ``True`` if setting *key* exists in effective configuration, else ``False``.
        """
        return key in self.__effective_configuration

    def __getitem__(self, key: str) -> Any:
        """Get value of setting *key* from effective configuration.

        Designed to work the same way as :func:`dict.__getitem__`.

        :param key: name of the setting.

        :returns: value of setting *key*.

        :raises:
            :class:`KeyError`: if *key* is not present in effective configuration.
        """
        return self.__effective_configuration[key]

    def copy(self) -> Dict[str, Any]:
        """Get a deep copy of effective Patroni configuration.

        :returns: a deep copy of the Patroni configuration.
        """
        pass

    def build_effective_postgresql_configuration(self, role: PostgresqlRole) -> Dict[str, Any]:
        """Build effective postgresql configuration with role-based overrides applied.

        This method assembles the complete postgresql configuration by:

        1. Starting with the base postgresql configuration from effective_configuration
        2. Applying role-specific parameter overrides (parameters_primary, etc.) - merged with base
        3. Applying role-specific pg_hba overrides (pg_hba_primary, etc.) - full replacement
        4. Applying role-specific pg_ident overrides (pg_ident_primary, etc.) - full replacement

        .. note::
            Protected parameters from ConfigHandler.CMDLINE_OPTIONS are never overridden.

        :param role: Current PostgresqlRole to determine which overrides to apply.
        :returns: Deep copy of postgresql configuration with role-specific overrides applied.
        """
        pass

    def _validate_contradictory_tags(self) -> None:
        """Check boolean/priority tags' config and warn user if it's contradictory.

        .. note::
          To preserve sanity (and backwards compatibility) the ``nofailover``/``nosync`` tag will still exist.
          A contradictory configuration is one where ``nofailover``/``nosync`` is ``True`` but
          ``failover_priority > 0``/``sync_priority > 0``, or where ``nofailover``/``nosync`` is ``False``,
          but ``failover_priority <= 0``/``sync_priority <= 0``. Essentially, ``nofailover``/``nosync`` and
          ``failover_priority``/``sync_priority`` are communicating different things.
          This checks for this edge case (which is a misconfiguration on the part of the user) and warns them.
          The behaviour is as if ``failover_priority``/``sync_priority`` were not provided
          (i.e ``nofailover``/``nosync`` is the bedrock source of truth).
        """
        pass
