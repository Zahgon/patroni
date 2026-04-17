import base64
import functools
import json
import logging
import os
import socket
import sys
import time

from collections import defaultdict
from enum import IntEnum
from threading import Condition, Lock, Thread
from typing import Any, Callable, Collection, Dict, Iterator, List, Optional, Tuple, Type, TYPE_CHECKING, Union

import etcd
import urllib3

from urllib3.exceptions import ProtocolError, ReadTimeoutError

from ..collections import EMPTY_DICT
from ..exceptions import DCSError, PatroniException
from ..postgresql.mpp import AbstractMPP
from ..utils import deep_compare, enable_keepalive, iter_response_objects, \
    parse_bool, RetryFailedError, USER_AGENT, WHITESPACE_RE
from . import catch_return_false_exception, Cluster, ClusterConfig, \
    Failover, Leader, Member, Status, SyncState, TimelineHistory
from .etcd import AbstractEtcd, AbstractEtcdClientWithFailover, catch_etcd_errors, \
    DnsCachingResolver, Retry, StaleEtcdNode, StaleEtcdNodeGuard

logger = logging.getLogger(__name__)


class Etcd3Error(DCSError):
    pass


class UnsupportedEtcdVersion(PatroniException):
    pass


# google.golang.org/grpc/codes
class GRPCCode(IntEnum):
    OK = 0
    Canceled = 1
    Unknown = 2
    InvalidArgument = 3
    DeadlineExceeded = 4
    NotFound = 5
    AlreadyExists = 6
    PermissionDenied = 7
    ResourceExhausted = 8
    FailedPrecondition = 9
    Aborted = 10
    OutOfRange = 11
    Unimplemented = 12
    Internal = 13
    Unavailable = 14
    DataLoss = 15
    Unauthenticated = 16


GRPCcodeToText: Dict[int, str] = {v: k for k, v in GRPCCode.__dict__['_member_map_'].items()}


class Etcd3Exception(etcd.EtcdException):
    pass


class Etcd3WatchCanceled(Etcd3Exception):
    pass


class Etcd3ClientError(Etcd3Exception):

    def __init__(self, code: Optional[int] = None, error: Optional[str] = None, status: Optional[int] = None) -> None:
        if not hasattr(self, 'code'):
            self.code = code
        if not hasattr(self, 'error'):
            self.error = error and error.strip()
        self.codeText = GRPCcodeToText.get(code) if code is not None else None
        self.status = status

    def __repr__(self) -> str:
        return "<{0} error: '{1}', code: {2}>"\
            .format(self.codeText, getattr(self, 'error', None), getattr(self, 'code', None))

    __str__ = __repr__

    def as_dict(self) -> Dict[str, Any]:
        pass

    @classmethod
    def get_subclasses(cls) -> Iterator[Type['Etcd3ClientError']]:
        for subclass in cls.__subclasses__():
            for subsubclass in subclass.get_subclasses():
                yield subsubclass
            yield subclass


class Unknown(Etcd3ClientError):
    code = GRPCCode.Unknown


class InvalidArgument(Etcd3ClientError):
    code = GRPCCode.InvalidArgument


class DeadlineExceeded(Etcd3ClientError):
    code = GRPCCode.DeadlineExceeded
    error = "context deadline exceeded"


class NotFound(Etcd3ClientError):
    code = GRPCCode.NotFound


class FailedPrecondition(Etcd3ClientError):
    code = GRPCCode.FailedPrecondition


class Unavailable(Etcd3ClientError):
    code = GRPCCode.Unavailable


# https://github.com/etcd-io/etcd/commits/main/api/v3rpc/rpctypes/error.go
class LeaseNotFound(NotFound):
    error = "etcdserver: requested lease not found"


class UserEmpty(InvalidArgument):
    error = "etcdserver: user name is empty"


class AuthFailed(InvalidArgument):
    error = "etcdserver: authentication failed, invalid user ID or password"


class AuthOldRevision(InvalidArgument):
    error = "etcdserver: revision of auth store is old"


class PermissionDenied(Etcd3ClientError):
    code = GRPCCode.PermissionDenied
    error = "etcdserver: permission denied"


class AuthNotEnabled(FailedPrecondition):
    error = "etcdserver: authentication is not enabled"


class InvalidAuthToken(Etcd3ClientError):
    code = GRPCCode.Unauthenticated
    error = "etcdserver: invalid auth token"


errStringToClientError = {getattr(s, 'error'): s for s in Etcd3ClientError.get_subclasses() if hasattr(s, 'error')}
errCodeToClientError = {getattr(s, 'code'): s for s in Etcd3ClientError.__subclasses__()}


def _raise_for_data(data: Union[bytes, str, Dict[str, Any]], status_code: Optional[int] = None) -> Etcd3ClientError:
    pass


def to_bytes(v: Union[str, bytes]) -> bytes:
    pass


def prefix_range_end(v: str) -> bytes:
    pass


def base64_encode(v: Union[str, bytes]) -> str:
    pass


def base64_decode(v: str) -> str:
    pass


def build_range_request(key: str, range_end: Union[bytes, str, None] = None) -> Dict[str, Any]:
    pass


def _handle_auth_errors(func: Callable[..., Any]) -> Any:
    pass


class Etcd3Client(AbstractEtcdClientWithFailover):

    ERROR_CLS = Etcd3Error

    def __init__(self, config: Dict[str, Any], dns_resolver: DnsCachingResolver, cache_ttl: int = 300) -> None:
        self._decoder = json.JSONDecoder()
        self._reauthenticate = False
        self._token = None
        self._cluster_version: Tuple[int, ...] = tuple()
        super(Etcd3Client, self).__init__({**config, 'version_prefix': '/v3beta'}, dns_resolver, cache_ttl)
        if self._use_proxies and not self._cluster_version:
            kwargs = self._prepare_common_parameters(1)
            self._ensure_version_prefix(self._base_uri, **kwargs)
            self.authenticate_on_start()

    def authenticate_on_start(self, auth_request_func: Optional[Callable[..., Dict[str, Any]]] = None):
        """Authenticate with Etcd v3 at startup and exit on invalid credentials.

        :param auth_request_func: optional custom authentication request function,
                                  if not provided, :meth:`call_rpc` will be used.
        """
        pass

    def _get_headers(self) -> Dict[str, str]:
        pass

    def _prepare_request(self, kwargs: Dict[str, Any], params: Optional[Dict[str, Any]] = None,
                         method: Optional[str] = None) -> Callable[..., urllib3.response.HTTPResponse]:
        pass

    def _handle_server_response(self, response: urllib3.response.HTTPResponse) -> Dict[str, Any]:
        pass

    def _ensure_version_prefix(self, base_uri: str, **kwargs: Any) -> None:
        pass

    def _prepare_get_members(self, etcd_nodes: int) -> Dict[str, Any]:
        pass

    def _do_auth_request(self, base_uri: str, kwargs: Dict[str, Any],
                         method: str, fields: Dict[str, Any], retry: Optional[Retry] = None) -> Dict[str, Any]:
        """Special method for handling authentication when discovering cluster members.

        We can't use `call_rpc()` method for this purpose because it may cause infinite recursion.

        :param base_uri: base url for authentication request, e.g. `http://etcd:2379/v3`
        :param kwargs: common request parameters, e.g. headers.
        :param method: `/auth/authenticate`
        :param fields: authentication fields, e.g. `{'name': 'user', 'password': 'pass'}`.
        :param retry: optional retry configuration, ignored.
        """
        pass

    def _do_member_list_request(self, url: str, retry: Optional[Retry] = None, **kwargs: Any) -> Any:
        """Special method for handling member list requests.

        :param url: base url for member list request, e.g. `http://etcd:2379/v3/cluster/member/list`
        :param kwargs: common request parameters, e.g. headers.
        :param retry: optional retry configuration, ignored.
        """
        pass

    def _get_members(self, base_uri: str, **kwargs: Any) -> List[str]:
        pass

    def call_rpc(self, method: str, fields: Dict[str, Any], retry: Optional[Retry] = None) -> Dict[str, Any]:
        pass

    def authenticate(self, *, retry: Optional[Retry] = None,
                     auth_request_func: Optional[Callable[..., Dict[str, Any]]] = None) -> bool:
        """Authenticate with the Etcd v3 cluster.

        :param retry: optional retry configuration.
        :param auth_request_func: optional custom authentication request function,
                                  if not provided, `call_rpc()` method will be used.
        """
        pass

    def handle_auth_errors(self: 'Etcd3Client', func: Callable[..., Any], *args: Any,
                           auth_request_func: Optional[Callable[..., Dict[str, Any]]] = None,
                           retry: Optional[Retry] = None, **kwargs: Any) -> Any:
        """Handle authentication errors for the given function.

        :param func: function to call.
        :param args: positional arguments for the function.
        :param auth_request_func: optional custom authentication request function,
                                  if not provided, `call_rpc()` method will be used.
        :param retry: optional retry configuration.
        :param kwargs: keyword arguments for the function.
        """
        pass

    @_handle_auth_errors
    def range(self, key: str, range_end: Union[bytes, str, None] = None, serializable: bool = True,
              *, retry: Optional[Retry] = None) -> Dict[str, Any]:
        pass

    def prefix(self, key: str, serializable: bool = True, *, retry: Optional[Retry] = None) -> Dict[str, Any]:
        pass

    @_handle_auth_errors
    def lease_grant(self, ttl: int, *, retry: Optional[Retry] = None) -> str:
        pass

    @_handle_auth_errors
    def lease_keepalive(self, ID: str, *, retry: Optional[Retry] = None) -> Optional[str]:
        pass

    @_handle_auth_errors
    def txn(self, compare: Dict[str, Any], success: Dict[str, Any],
            failure: Optional[Dict[str, Any]] = None, *, retry: Optional[Retry] = None) -> Dict[str, Any]:
        pass

    @_handle_auth_errors
    def put(self, key: str, value: str, lease: Optional[str] = None, create_revision: Optional[str] = None,
            mod_revision: Optional[str] = None, *, retry: Optional[Retry] = None) -> Dict[str, Any]:
        pass

    @_handle_auth_errors
    def deleterange(self, key: str, range_end: Union[bytes, str, None] = None,
                    mod_revision: Optional[str] = None, *, retry: Optional[Retry] = None) -> Dict[str, Any]:
        pass

    def deleteprefix(self, key: str, *, retry: Optional[Retry] = None) -> Dict[str, Any]:
        pass

    def watchrange(self, key: str, range_end: Union[bytes, str, None] = None,
                   start_revision: Optional[str] = None, filters: Optional[List[Dict[str, Any]]] = None,
                   read_timeout: Optional[float] = None) -> urllib3.response.HTTPResponse:
        """returns: response object"""
        pass

    def watchprefix(self, key: str, start_revision: Optional[str] = None,
                    filters: Optional[List[Dict[str, Any]]] = None,
                    read_timeout: Optional[float] = None) -> urllib3.response.HTTPResponse:
        pass


class KVCache(StaleEtcdNodeGuard, Thread):

    def __init__(self, dcs: 'Etcd3', client: 'PatroniEtcd3Client') -> None:
        Thread.__init__(self)
        StaleEtcdNodeGuard.__init__(self)
        self.daemon = True
        self._dcs = dcs
        self._client = client
        self.condition = Condition()
        self._config_key = base64_encode(dcs.config_path)
        self._leader_key = base64_encode(dcs.leader_path)
        self._optime_key = base64_encode(dcs.leader_optime_path)
        self._status_key = base64_encode(dcs.status_path)
        self._name = base64_encode(getattr(dcs, '_name'))  # pyright
        self._is_ready = False
        self._response = None
        self._response_lock = Lock()
        self._object_cache = {}
        self._object_cache_lock = Lock()
        self.start()

    def set(self, value: Dict[str, Any], overwrite: bool = False) -> Tuple[bool, Optional[Dict[str, Any]]]:
        pass

    def delete(self, name: str, mod_revision: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
        pass

    def copy(self) -> List[Dict[str, Any]]:
        pass

    def get(self, name: str) -> Optional[Dict[str, Any]]:
        with self._object_cache_lock:
            return self._object_cache.get(name)

    def _process_event(self, event: Dict[str, Any]) -> None:
        pass

    def _process_message(self, message: Dict[str, Any]) -> None:
        pass

    @staticmethod
    def _finish_response(response: urllib3.response.HTTPResponse) -> None:
        pass

    def _do_watch(self, revision: str) -> None:
        pass

    def _build_cache(self) -> None:
        pass

    def run(self) -> None:
        pass

    def kill_stream(self) -> None:
        pass

    def is_ready(self) -> bool:
        """Must be called only when holding the lock on `condition`"""
        pass


class PatroniEtcd3Client(Etcd3Client):

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._kv_cache = None
        super(PatroniEtcd3Client, self).__init__(*args, **kwargs)

    def configure(self, etcd3: 'Etcd3') -> None:
        pass

    def start_watcher(self) -> None:
        pass

    def _restart_watcher(self) -> None:
        pass

    def set_base_uri(self, value: str) -> None:
        pass

    def _wait_cache(self, timeout: float) -> None:
        pass

    def get_cluster(self, path: str) -> List[Dict[str, Any]]:
        pass

    def call_rpc(self, method: str, fields: Dict[str, Any], retry: Optional[Retry] = None) -> Dict[str, Any]:
        pass

    def txn(self, compare: Dict[str, Any], success: Dict[str, Any],
            failure: Optional[Dict[str, Any]] = None, *, retry: Optional[Retry] = None) -> Dict[str, Any]:
        pass


class Etcd3(AbstractEtcd):

    def __init__(self, config: Dict[str, Any], mpp: AbstractMPP) -> None:
        super(Etcd3, self).__init__(config, mpp, PatroniEtcd3Client, (DeadlineExceeded, FailedPrecondition))
        self.__do_not_watch = False
        self._lease = None
        self._last_lease_refresh = 0

        self._client.configure(self)
        if not self._ctl:
            self._client.start_watcher()
            self.create_lease()

    @property
    def _client(self) -> PatroniEtcd3Client:
        pass

    def set_socket_options(self, sock: socket.socket,
                           socket_options: Optional[Collection[Tuple[int, int, int]]]) -> None:
        pass

    def set_ttl(self, ttl: int) -> Optional[bool]:
        pass

    def _do_refresh_lease(self, force: bool = False, retry: Optional[Retry] = None) -> bool:
        pass

    def refresh_lease(self) -> bool:
        pass

    def create_lease(self) -> None:
        pass

    @property
    def cluster_prefix(self) -> str:
        """Construct the cluster prefix for the cluster.

        :returns: path in the DCS under which we store information about this Patroni cluster.
        """
        pass

    @staticmethod
    def member(node: Dict[str, str]) -> Member:
        pass

    def _cluster_from_nodes(self, nodes: Dict[str, Any]) -> Cluster:
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

    def _do_attempt_to_acquire_leader(self, retry: Retry) -> bool:
        pass

    @catch_return_false_exception
    def attempt_to_acquire_leader(self) -> bool:
        pass

    @catch_etcd_errors
    def set_failover_value(self, value: str, version: Optional[str] = None) -> bool:
        pass

    @catch_etcd_errors
    def set_config_value(self, value: str, version: Optional[str] = None) -> bool:
        pass

    @catch_etcd_errors
    def _write_leader_optime(self, last_lsn: str) -> bool:
        pass

    @catch_etcd_errors
    def _write_status(self, value: str) -> bool:
        pass

    @catch_etcd_errors
    def _write_failsafe(self, value: str) -> bool:
        pass

    @catch_return_false_exception
    def _update_leader(self, leader: Leader) -> bool:
        pass

    @catch_etcd_errors
    def initialize(self, create_new: bool = True, sysid: str = ""):
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
    def set_sync_state_value(self, value: str, version: Optional[str] = None) -> Union[str, bool]:
        pass

    @catch_etcd_errors
    def delete_sync_state(self, version: Optional[str] = None) -> bool:
        pass

    def watch(self, leader_version: Optional[str], timeout: float) -> bool:
        pass
