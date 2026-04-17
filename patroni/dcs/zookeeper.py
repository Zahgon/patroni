import json
import logging
import select
import socket
import time

from typing import Any, Callable, cast, Dict, List, Optional, Tuple, TYPE_CHECKING, Union

from kazoo.client import KazooClient, KazooRetry, KazooState
from kazoo.exceptions import ConnectionClosedError, NodeExistsError, NoNodeError, SessionExpiredError
from kazoo.handlers.threading import AsyncResult, SequentialThreadingHandler
from kazoo.protocol.states import KeeperState, WatchedEvent, ZnodeStat
from kazoo.retry import RetryFailedError
from kazoo.security import ACL, make_acl

from ..exceptions import DCSError
from ..postgresql.mpp import AbstractMPP
from ..utils import deep_compare
from . import AbstractDCS, Cluster, ClusterConfig, Failover, Leader, Member, Status, SyncState, TimelineHistory

if TYPE_CHECKING:  # pragma: no cover
    from ..config import Config

logger = logging.getLogger(__name__)


class ZooKeeperError(DCSError):
    pass


class PatroniSequentialThreadingHandler(SequentialThreadingHandler):

    def __init__(self, connect_timeout: Union[int, float]) -> None:
        super(PatroniSequentialThreadingHandler, self).__init__()
        self.set_connect_timeout(connect_timeout)

    def set_connect_timeout(self, connect_timeout: Union[int, float]) -> None:
        pass

    def create_connection(self, *args: Any, **kwargs: Any) -> socket.socket:
        """This method is trying to establish connection with one of the zookeeper nodes.
           Somehow strategy "fail earlier and retry more often" works way better comparing to
           the original strategy "try to connect with specified timeout".
           Since we want to try connect to zookeeper more often (with the smaller connect_timeout),
           he have to override `create_connection` method in the `SequentialThreadingHandler`
           class (which is used by `kazoo.Client`).

        :param args: always contains `tuple(host, port)` as the first element and could contain
                     `connect_timeout` (negotiated session timeout) as the second element."""
        pass

    def select(self, *args: Any, **kwargs: Any) -> Any:
        """
        Python 3.XY may raise following exceptions if select/poll are called with an invalid socket:
        - `ValueError`: because fd == -1
        - `TypeError`: Invalid file descriptor: -1 (starting from kazoo 2.9)
        Python 2.7 may raise the `IOError` instead of `socket.error` (starting from kazoo 2.9)

        When it is appropriate we map these exceptions to `socket.error`.
        """
        pass


class PatroniKazooClient(KazooClient):

    def _call(self, request: Tuple[Any], async_object: AsyncResult) -> Optional[bool]:
        # Before kazoo==2.7.0 it wasn't possible to send requests to zookeeper if
        # the connection is in the SUSPENDED state and Patroni was strongly relying on it.
        # The https://github.com/python-zk/kazoo/pull/588 changed it, and now such requests are queued.
        # We override the `_call()` method in order to keep the old behavior.

        pass


class ZooKeeper(AbstractDCS):

    def __init__(self, config: Dict[str, Any], mpp: AbstractMPP) -> None:
        super(ZooKeeper, self).__init__(config, mpp)

        hosts: Union[str, List[str]] = config.get('hosts', [])
        if isinstance(hosts, list):
            hosts = ','.join(hosts)

        mapping = {'use_ssl': 'use_ssl', 'verify': 'verify_certs', 'cacert': 'ca',
                   'cert': 'certfile', 'key': 'keyfile', 'key_password': 'keyfile_password'}
        kwargs = {v: config[k] for k, v in mapping.items() if k in config}

        if 'set_acls' in config:
            default_acl: List[ACL] = []
            for principal, permissions in config['set_acls'].items():
                normalizedPermissions = [p.upper() for p in permissions]

                if ':' in principal:
                    scheme, credential = principal.split(':', 1)
                else:
                    scheme, credential = 'x509', principal

                default_acl.append(make_acl(scheme=scheme,
                                            credential=credential,
                                            read='READ' in normalizedPermissions,
                                            write='WRITE' in normalizedPermissions,
                                            create='CREATE' in normalizedPermissions,
                                            delete='DELETE' in normalizedPermissions,
                                            admin='ADMIN' in normalizedPermissions,
                                            all='ALL' in normalizedPermissions))
            kwargs['default_acl'] = default_acl

        self._client = PatroniKazooClient(hosts, handler=PatroniSequentialThreadingHandler(config['retry_timeout']),
                                          timeout=config['ttl'], connection_retry=KazooRetry(max_delay=1, max_tries=-1,
                                          sleep_func=time.sleep), command_retry=KazooRetry(max_delay=1, max_tries=-1,
                                          deadline=config['retry_timeout'], sleep_func=time.sleep),
                                          auth_data=list(config.get('auth_data', {}).items()), **kwargs)

        self.__last_member_data: Optional[Dict[str, Any]] = None

        self._orig_kazoo_connect = self._client._connection._connect
        self._client._connection._connect = self._kazoo_connect

        self._client.start()

    def _kazoo_connect(self, *args: Any) -> Tuple[Union[int, float], Union[int, float]]:
        """Kazoo is using Ping's to determine health of connection to zookeeper. If there is no
        response on Ping after Ping interval (1/2 from read_timeout) it will consider current
        connection dead and try to connect to another node. Without this "magic" it was taking
        up to 2/3 from session timeout (ttl) to figure out that connection was dead and we had
        only small time for reconnect and retry.

        This method is needed to return different value of read_timeout, which is not calculated
        from negotiated session timeout but from value of `loop_wait`. And it is 2 sec smaller
        than loop_wait, because we can spend up to 2 seconds when calling `touch_member()` and
        `write_leader_optime()` methods, which also may hang..."""
        pass

    def _watcher(self, event: WatchedEvent) -> None:
        pass

    def reload_config(self, config: Union['Config', Dict[str, Any]]) -> None:
        pass

    def set_ttl(self, ttl: int) -> Optional[bool]:
        """It is not possible to change ttl (session_timeout) in zookeeper without
        destroying old session and creating the new one. This method returns `!True`
        if session_timeout has been changed (`restart()` has been called)."""
        pass

    @property
    def ttl(self) -> int:
        pass

    def set_retry_timeout(self, retry_timeout: int) -> None:
        pass

    def get_node(
            self, key: str, watch: Optional[Callable[[WatchedEvent], None]] = None
    ) -> Optional[Tuple[str, ZnodeStat]]:
        pass

    def get_status(self, path: str, leader: Optional[Leader]) -> Status:
        pass

    @staticmethod
    def member(name: str, value: str, znode: ZnodeStat) -> Member:
        pass

    def get_children(self, key: str) -> List[str]:
        pass

    def load_members(self, path: str) -> List[Member]:
        pass

    def _postgresql_cluster_loader(self, path: str) -> Cluster:
        """Load and build the :class:`Cluster` object from DCS, which represents a single PostgreSQL cluster.

        :param path: the path in DCS where to load :class:`Cluster` from.

        :returns: :class:`Cluster` instance.
        """
        pass

    def _mpp_cluster_loader(self, path: str) -> Dict[int, Cluster]:
        """Load and build all PostgreSQL clusters from a single MPP cluster.

        :param path: the path in DCS where to load Cluster(s) from.

        :returns: all MPP groups as :class:`dict`, with group IDs as keys and :class:`Cluster` objects as values.
        """
        pass

    def _load_cluster(
            self, path: str, loader: Callable[[str], Union[Cluster, Dict[int, Cluster]]]
    ) -> Union[Cluster, Dict[int, Cluster]]:
        pass

    def _create(self, path: str, value: bytes, retry: bool = False, ephemeral: bool = False) -> bool:
        pass

    def attempt_to_acquire_leader(self) -> bool:
        pass

    def _set_or_create(self, key: str, value: str, version: Optional[int] = None,
                       retry: bool = False, do_not_create_empty: bool = False) -> Union[int, bool]:
        pass

    def set_failover_value(self, value: str, version: Optional[int] = None) -> bool:
        pass

    def set_config_value(self, value: str, version: Optional[int] = None) -> bool:
        pass

    def initialize(self, create_new: bool = True, sysid: str = "") -> bool:
        pass

    def touch_member(self, data: Dict[str, Any]) -> bool:
        pass

    def take_leader(self) -> bool:
        pass

    def _write_leader_optime(self, last_lsn: str) -> bool:
        pass

    def _write_status(self, value: str) -> bool:
        pass

    def _write_failsafe(self, value: str) -> bool:
        pass

    def _update_leader(self, leader: Leader) -> bool:
        pass

    def _delete_leader(self, leader: Leader) -> bool:
        pass

    def _cancel_initialization(self) -> None:
        pass

    def cancel_initialization(self) -> bool:
        pass

    def delete_cluster(self) -> bool:
        pass

    def set_history_value(self, value: str) -> bool:
        pass

    def set_sync_state_value(self, value: str, version: Optional[int] = None) -> Union[int, bool]:
        pass

    def delete_sync_state(self, version: Optional[int] = None) -> bool:
        pass

    def watch(self, leader_version: Optional[int], timeout: float) -> bool:
        pass
