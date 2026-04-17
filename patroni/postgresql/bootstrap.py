import logging
import os
import shlex
import tempfile
import time

from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union

from ..async_executor import CriticalTask
from ..collections import EMPTY_DICT
from ..dcs import Leader, Member, RemoteMember
from ..psycopg import quote_ident, quote_literal
from ..utils import deep_compare, process_user_options
from .misc import PostgresqlState

if TYPE_CHECKING:  # pragma: no cover
    from . import Postgresql

logger = logging.getLogger(__name__)


class Bootstrap(object):

    def __init__(self, postgresql: 'Postgresql') -> None:
        self._postgresql = postgresql
        self._running_custom_bootstrap = False

    @property
    def running_custom_bootstrap(self) -> bool:
        pass

    @property
    def keep_existing_recovery_conf(self) -> bool:
        pass

    def _initdb(self, config: Any) -> bool:
        pass

    def _post_restore(self) -> None:
        pass

    def _custom_bootstrap(self, config: Any) -> bool:
        """Bootstrap a fresh Patroni cluster using a custom method provided by the user.

        :param config: configuration used for running a custom bootstrap method. It comes from the Patroni YAML file,
            so it is expected to be a :class:`dict`.

        .. note::
            *config* must contain a ``command`` key, which value is the command or script to perform the custom
            bootstrap procedure. The exit code of the ``command`` dictates if the bootstrap succeeded or failed.

            When calling ``command``, Patroni will pass the following arguments to the ``command`` call:

                * ``--scope``: contains the value of ``scope`` configuration;
                * ``--data_dir``: contains the value of the ``postgresql.data_dir`` configuration.

            You can avoid that behavior by filling the optional key ``no_params`` with the value ``False`` in the
            configuration file, which will instruct Patroni to not pass these parameters to the ``command`` call.

            Besides that, a couple more keys are supported in *config*, but optional:

                * ``keep_existing_recovery_conf``: if ``True``, instruct Patroni to not remove the existing
                  ``recovery.conf`` (PostgreSQL <= 11), to not discard recovery parameters from the configuration
                  (PostgreSQL >= 12), and to not remove the files ``recovery.signal`` or ``standby.signal``
                  (PostgreSQL >= 12). This is specially useful when you are restoring backups through tools like
                  pgBackRest and Barman, in which case they generated the appropriate recovery settings for you;
                * ``recovery_conf``: a section containing a map, where each key is the name of a recovery related
                  setting, and the value is the value of the corresponding setting.

            Any key/value other than the ones that were described above will be interpreted as additional arguments for
            the ``command`` call. They will all be added to the call in the format ``--key=value``.

        :returns: ``True`` if the bootstrap was successful, i.e. the execution of the custom ``command`` from *config*
            exited with code ``0``, ``False`` otherwise.
        """
        pass

    def call_post_bootstrap(self, config: Dict[str, Any]) -> bool:
        """
        runs a script after initdb or custom bootstrap script is called and waits until completion.
        """
        pass

    def create_replica(self, clone_member: Union[Leader, Member, None],
                       clone_from_leader: bool = False) -> Optional[int]:
        """
            create the replica according to the replica_method
            defined by the user.  this is a list, so we need to
            loop through all methods the user supplies
        """
        pass

    def basebackup(self, conn_url: str, env: Dict[str, str], options: Dict[str, Any]) -> Optional[int]:
        # creates a replica data dir using pg_basebackup.
        # this is the default, built-in create_replica_methods
        # tries twice, then returns failure (as 1)
        # uses "stream" as the xlog-method to avoid sync issues
        # supports additional user-supplied options, those are not validated
        pass

    def clone(self, clone_member: Union[Leader, Member, None], clone_from_leader: bool = False) -> bool:
        """
             - initialize the replica from an existing member (primary or replica)
             - initialize the replica using the replica creation method that
               works without the replication connection (i.e. restore from on-disk
               base backup)
        """
        pass

    def bootstrap(self, config: Dict[str, Any]) -> bool:
        """ Initialize a new node from scratch and start it. """
        pass

    def create_or_update_role(self, name: str, password: Optional[str], options: List[str]) -> None:
        pass

    def post_bootstrap(self, config: Dict[str, Any], task: CriticalTask) -> Optional[bool]:
        pass
