import abc
import json
import logging
import os
import random
import socket
import time

from collections import defaultdict
from copy import deepcopy
from http.client import HTTPException
from queue import Queue
from threading import Thread
from typing import Any, Callable, Collection, Dict, List, Optional, Tuple, Type, TYPE_CHECKING, Union
from urllib.parse import urlparse

import etcd
import urllib3.util.connection

from dns import resolver
from dns.exception import DNSException
from urllib3 import Timeout
from urllib3.exceptions import HTTPError, ProtocolError, ReadTimeoutError

from ..exceptions import DCSError
from ..postgresql.mpp import AbstractMPP
from ..request import get as requests_get
from ..utils import Retry, RetryFailedError, split_host_port, uri, USER_AGENT
from . import AbstractDCS, catch_return_false_exception, Cluster, ClusterConfig, \
    Failover, Leader, Member, ReturnFalseException, Status, SyncState, TimelineHistory

if TYPE_CHECKING:  # pragma: no cover
    from ..config import Config

logger = logging.getLogger(__name__)


class EtcdRaftInternal(etcd.EtcdException):
    """Raft Internal Error"""


class StaleEtcdNode(Exception):
    """Node is stale (raft term is older than previous known)."""


class EtcdError(DCSError):
    pass


_AddrInfo = Tuple[socket.AddressFamily, socket.SocketKind, int, str,
                  Union[Tuple[str, int], Tuple[str, int, int, int], Tuple[int, bytes]]]


class DnsCachingResolver(Thread):

    def __init__(self, cache_time: float = 600.0, cache_fail_time: float = 30.0) -> None:
        super(DnsCachingResolver, self).__init__()
        self._cache: Dict[Tuple[str, int], Tuple[float, List[_AddrInfo]]] = {}
        self._cache_time = cache_time
        self._cache_fail_time = cache_fail_time
        self._resolve_queue: Queue[Tuple[Tuple[str, int], int]] = Queue()
        self.daemon = True
        self.start()

    def run(self) -> None:
        pass

    def resolve(self, host: str, port: int) -> List[_AddrInfo]:
        pass

    def resolve_async(self, host: str, port: int, attempt: int = 0) -> None:
        pass

    def remove(self, host: str, port: int) -> None:
        pass

    @staticmethod
    def _do_resolve(host: str, port: int) -> List[_AddrInfo]:
        pass


class StaleEtcdNodeGuard(object):

    def __init__(self) -> None:
        self._reset_cluster_raft_term()

    def _reset_cluster_raft_term(self) -> None:
        pass

    def _check_cluster_raft_term(self, cluster_id: Optional[str], value: Union[None, str, int]) -> None:
        """Check that observed Raft Term in Etcd cluster is increasing.

        :param cluster_id: last observed Etcd Cluster ID
        :param raft_term: last observed Raft Term

        :raises:
            :exc::`StaleEtcdNode` if last observed *raft_term* is smaller than previously known *raft_term*.
        """
        pass


class AbstractEtcdClientWithFailover(abc.ABC, etcd.Client, StaleEtcdNodeGuard):

    ERROR_CLS: Type[Exception]

    def __init__(self, config: Dict[str, Any], dns_resolver: DnsCachingResolver, cache_ttl: int = 300) -> None:
        StaleEtcdNodeGuard.__init__(self)
        self._dns_resolver = dns_resolver
        self.set_machines_cache_ttl(cache_ttl)
        self._machines_cache_updated = 0
        kwargs = {p: config.get(p) for p in ('host', 'port', 'protocol', 'use_proxies', 'version_prefix',
                                             'username', 'password', 'cert', 'ca_cert') if config.get(p)}
        super(AbstractEtcdClientWithFailover, self).__init__(read_timeout=config['retry_timeout'], **kwargs)
        # For some reason python3-etcd on debian and ubuntu are not based on the latest version
        # Workaround for the case when https://github.com/jplana/python-etcd/pull/196 is not applied
        self.http.connection_pool_kw.pop('ssl_version', None)
        self._config = config
        self._load_machines_cache()
        self._allow_reconnect = True
        # allow passing retry argument to api_execute in params
        self._comparison_conditions.add('retry')
        self._read_options.add('retry')
        self._del_conditions.add('retry')

    def _calculate_timeouts(self, etcd_nodes: int, timeout: Optional[float] = None) -> Tuple[int, float, int]:
        """Calculate a request timeout and number of retries per single etcd node.
        In case if the timeout per node is too small (less than one second) we will reduce the number of nodes.
        For the cluster with only one node we will try to do 2 retries.
        For clusters with 2 nodes we will try to do 1 retry for every node.
        No retries for clusters with 3 or more nodes. We better rely on switching to a different node."""
        pass

    def reload_config(self, config: Dict[str, Any]) -> None:
        pass

    def _get_headers(self) -> Dict[str, str]:
        pass

    def _prepare_common_parameters(self, etcd_nodes: int, timeout: Optional[float] = None) -> Dict[str, Any]:
        pass

    def set_machines_cache_ttl(self, cache_ttl: int) -> None:
        pass

    @abc.abstractmethod
    def _prepare_get_members(self, etcd_nodes: int) -> Dict[str, Any]:
        """returns: request parameters"""

    @abc.abstractmethod
    def _get_members(self, base_uri: str, **kwargs: Any) -> List[str]:
        """returns: list of clientURLs"""

    @property
    def machines_cache(self) -> List[str]:
        pass

    def _get_machines_list(self, machines_cache: List[str]) -> List[str]:
        """Gets list of members from Etcd cluster using API

        :param machines_cache: initial list of Etcd members
        :returns: list of clientURLs retrieved from Etcd cluster
        :raises EtcdConnectionFailed: if failed"""
        pass

    @property
    def machines(self) -> List[str]:
        """Original `machines` method(property) of `etcd.Client` class raise exception
        when it failed to get list of etcd cluster members. This method is being called
        only when request failed on one of the etcd members during `api_execute` call.
        For us it's more important to execute original request rather then get new topology
        of etcd cluster. So we will catch this exception and return empty list of machines.
        Later, during next `api_execute` call we will forcefully update machines_cache.

        Also this method implements the same timeout-retry logic as `api_execute`, because
        the original method was retrying 2 times with the `read_timeout` on each node.

        After the next refactoring the whole logic was moved to the _get_machines_list() method."""
        pass

    def set_read_timeout(self, timeout: float) -> None:
        pass

    def _do_http_request(self, retry: Optional[Retry], machines_cache: List[str],
                         request_executor: Callable[..., urllib3.response.HTTPResponse],
                         method: str, path: str, fields: Optional[Dict[str, Any]] = None,
                         **kwargs: Any) -> Any:
        pass

    @abc.abstractmethod
    def _prepare_request(self, kwargs: Dict[str, Any], params: Optional[Dict[str, Any]] = None,
                         method: Optional[str] = None) -> Callable[..., urllib3.response.HTTPResponse]:
        """returns: request_executor"""

    def api_execute(self, path: str, method: str, params: Optional[Dict[str, Any]] = None,
                    timeout: Optional[float] = None) -> Any:
        pass

    @staticmethod
    def get_srv_record(host: str) -> List[Tuple[str, int]]:
        pass

    def _get_machines_cache_from_srv(self, srv: str, srv_suffix: Optional[str] = None) -> List[str]:
        """Fetch list of etcd-cluster member by resolving _etcd-server._tcp. SRV record.
        This record should contain list of host and peer ports which could be used to run
        'GET http://{host}:{port}/members' request (peer protocol)"""
        pass

    def _get_machines_cache_from_dns(self, host: str, port: int) -> List[str]:
        """One host might be resolved into multiple ip addresses. We will make list out of it"""
        pass

    def _get_machines_cache_from_config(self) -> List[str]:
        pass

    @staticmethod
    def _update_dns_cache(func: Callable[[str, int], None], machines: List[str]) -> None:
        pass

    def _load_machines_cache(self) -> bool:
        """This method should fill up `_machines_cache` from scratch.
        It could happen only in two cases:
        1. During class initialization
        2. When all etcd members failed"""
        pass

    def _refresh_machines_cache(self, machines_cache: Optional[List[str]] = None) -> bool:
        """Get etcd cluster topology using Etcd API and put it to self._machines_cache

        :param machines_cache: the list of nodes we want to run through executing API request
                               in addition to values stored in the self._machines_cache
        :returns: `True` if self._machines_cache was updated with new values
        :raises EtcdException: if failed to get topology and `machines_cache` was specified.

        The self._machines_cache will not be updated if nodes from the list are
        not accessible or if they are not returning correct results."""
        pass

    def set_base_uri(self, value: str) -> None:
        pass


class EtcdClient(AbstractEtcdClientWithFailover):

    ERROR_CLS = EtcdError

    def __init__(self, config: Dict[str, Any], dns_resolver: DnsCachingResolver, cache_ttl: int = 300) -> None:
        super(EtcdClient, self).__init__({**config, 'version_prefix': None}, dns_resolver, cache_ttl)

    def __del__(self) -> None:
        try:
            self.http.clear()
        except (ReferenceError, TypeError, AttributeError):
            pass

    def _prepare_get_members(self, etcd_nodes: int) -> Dict[str, Any]:
        pass

    def _handle_server_response(self, response: urllib3.response.HTTPResponse) -> Any:
        pass

    def _get_members(self, base_uri: str, **kwargs: Any) -> List[str]:
        pass

    def _prepare_request(self, kwargs: Dict[str, Any], params: Optional[Dict[str, Any]] = None,
                         method: Optional[str] = None) -> Callable[..., urllib3.response.HTTPResponse]:
        pass


class AbstractEtcd(AbstractDCS):

    def __init__(self, config: Dict[str, Any], mpp: AbstractMPP, client_cls: Type[AbstractEtcdClientWithFailover],
                 retry_errors_cls: Union[Type[Exception], Tuple[Type[Exception], ...]]) -> None:
        super(AbstractEtcd, self).__init__(config, mpp)
        self._retry = Retry(deadline=config['retry_timeout'], max_delay=1, max_tries=-1,
                            retry_exceptions=retry_errors_cls)
        self._ttl = int(config.get('ttl') or 30)
        self._abstract_client = self.get_etcd_client(config, client_cls)
        self.__do_not_watch = False
        self._has_failed = False

    @property
    @abc.abstractmethod
    def _client(self) -> AbstractEtcdClientWithFailover:
        """return correct type of etcd client"""

    def reload_config(self, config: Union['Config', Dict[str, Any]]) -> None:
        pass

    def retry(self, method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        retry = self._retry.copy()
        kwargs['retry'] = retry
        return retry(method, *args, **kwargs)

    def _handle_exception(self, e: Exception, name: str = '', do_sleep: bool = False,
                          raise_ex: Optional[Exception] = None) -> None:
        pass

    def handle_etcd_exceptions(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        pass

    def _run_and_handle_exceptions(self, method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        pass

    def set_socket_options(self, sock: socket.socket,
                           socket_options: Optional[Collection[Tuple[int, int, int]]]) -> None:
        pass

    def get_etcd_client(self, config: Dict[str, Any],
                        client_cls: Type[AbstractEtcdClientWithFailover]) -> AbstractEtcdClientWithFailover:
        pass

    def set_ttl(self, ttl: int) -> Optional[bool]:
        pass

    @property
    def ttl(self) -> int:
        pass

    def set_retry_timeout(self, retry_timeout: int) -> None:
        pass


def catch_etcd_errors(func: Callable[..., Any]) -> Any:
    pass


class Etcd(AbstractEtcd):

    def __init__(self, config: Dict[str, Any], mpp: AbstractMPP) -> None:
        super(Etcd, self).__init__(config, mpp, EtcdClient, (etcd.EtcdLeaderElectionInProgress, EtcdRaftInternal))
        self.__do_not_watch = False

    @property
    def _client(self) -> EtcdClient:
        pass

    def set_ttl(self, ttl: int) -> Optional[bool]:
        pass

    @staticmethod
    def member(node: etcd.EtcdResult) -> Member:
        pass

    def _cluster_from_nodes(self, etcd_index: int, nodes: Dict[str, etcd.EtcdResult]) -> Cluster:
        # get initialize flag
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

    @catch_etcd_errors
    def touch_member(self, data: Dict[str, Any]) -> bool:
        pass

    @catch_etcd_errors
    def take_leader(self) -> bool:
        pass

    def _do_attempt_to_acquire_leader(self) -> bool:
        pass

    @catch_return_false_exception
    def attempt_to_acquire_leader(self) -> bool:
        pass

    @catch_etcd_errors
    def set_failover_value(self, value: str, version: Optional[int] = None) -> bool:
        pass

    @catch_etcd_errors
    def set_config_value(self, value: str, version: Optional[int] = None) -> bool:
        pass

    @catch_etcd_errors
    def _write_leader_optime(self, last_lsn: str) -> bool:
        pass

    @catch_etcd_errors
    def _write_status(self, value: str) -> bool:
        pass

    def _do_update_leader(self) -> bool:
        pass

    @catch_etcd_errors
    def _write_failsafe(self, value: str) -> bool:
        pass

    @catch_return_false_exception
    def _update_leader(self, leader: Leader) -> bool:
        pass

    @catch_etcd_errors
    def initialize(self, create_new: bool = True, sysid: str = "") -> bool:
        pass

    @catch_etcd_errors
    def _delete_leader(self, leader: Leader) -> bool:
        pass

    @catch_etcd_errors
    def cancel_initialization(self) -> bool:
        pass

    @catch_etcd_errors
    def delete_cluster(self) -> bool:
        pass

    @catch_etcd_errors
    def set_history_value(self, value: str) -> bool:
        pass

    @catch_etcd_errors
    def set_sync_state_value(self, value: str, version: Optional[int] = None) -> Union[int, bool]:
        pass

    @catch_etcd_errors
    def delete_sync_state(self, version: Optional[int] = None) -> bool:
        pass

    def watch(self, leader_version: Optional[int], timeout: float) -> bool:
        pass


etcd.EtcdError.error_exceptions[300] = EtcdRaftInternal
