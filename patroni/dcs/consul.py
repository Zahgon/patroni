import json
import logging
import os
import re
import socket
import ssl
import time

from collections import defaultdict
from http.client import HTTPException
from typing import Any, Callable, Dict, List, Mapping, NamedTuple, Optional, Tuple, TYPE_CHECKING, Union
from urllib.parse import quote, urlencode, urlparse

import urllib3

from consul import base, Check, ConsulException, NotFound
from urllib3.exceptions import HTTPError

from ..exceptions import DCSError
from ..postgresql.misc import PostgresqlRole, PostgresqlState
from ..postgresql.mpp import AbstractMPP
from ..utils import deep_compare, parse_bool, Retry, RetryFailedError, split_host_port, uri, USER_AGENT
from . import AbstractDCS, catch_return_false_exception, Cluster, ClusterConfig, \
    Failover, Leader, Member, ReturnFalseException, Status, SyncState, TimelineHistory

if TYPE_CHECKING:  # pragma: no cover
    from ..config import Config

logger = logging.getLogger(__name__)


class ConsulError(DCSError):
    pass


class ConsulInternalError(ConsulException):
    """An internal Consul server error occurred"""


class InvalidSessionTTL(ConsulException):
    """Session TTL is too small or too big"""


class InvalidSession(ConsulException):
    """invalid session"""


class Response(NamedTuple):
    code: int
    headers: Union[Mapping[str, str], Mapping[bytes, bytes], None]
    body: str
    content: bytes


class HTTPClient(object):

    def __init__(self, host: str = '127.0.0.1', port: int = 8500, token: Optional[str] = None, scheme: str = 'http',
                 verify: bool = True, cert: Optional[str] = None, ca_cert: Optional[str] = None) -> None:
        self.token = token
        self._read_timeout = 10
        self.base_uri = uri(scheme, (host, port))
        kwargs = {}
        if cert:
            if isinstance(cert, tuple):
                # Key and cert are separate
                kwargs['cert_file'] = cert[0]
                kwargs['key_file'] = cert[1]
            else:
                # combined certificate
                kwargs['cert_file'] = cert
        if ca_cert:
            kwargs['ca_certs'] = ca_cert
        kwargs['cert_reqs'] = ssl.CERT_REQUIRED if verify or ca_cert else ssl.CERT_NONE
        self.http = urllib3.PoolManager(num_pools=10, maxsize=10, headers={}, **kwargs)
        self._ttl = 30

    def set_read_timeout(self, timeout: float) -> None:
        pass

    @property
    def ttl(self) -> int:
        pass

    def set_ttl(self, ttl: int) -> bool:
        pass

    @staticmethod
    def response(response: urllib3.response.HTTPResponse) -> Response:
        pass

    def uri(self, path: str,
            params: Union[None, Dict[str, Any], List[Tuple[str, Any]], Tuple[Tuple[str, Any], ...]] = None) -> str:
        pass

    def __getattr__(self, method: str) -> Callable[[Callable[[Response], Union[bool, Any, Tuple[str, Any]]],
                                                    str, Union[None, Dict[str, Any], List[Tuple[str, Any]]],
                                                    str, Optional[Dict[str, str]]], Union[bool, Any, Tuple[str, Any]]]:
        if method not in ('get', 'post', 'put', 'delete'):
            raise AttributeError("HTTPClient instance has no attribute '{0}'".format(method))

        def wrapper(callback: Callable[[Response], Union[bool, Any, Tuple[str, Any]]], path: str,
                    params: Union[None, Dict[str, Any], List[Tuple[str, Any]]] = None, data: str = '',
                    headers: Optional[Dict[str, str]] = None) -> Union[bool, Any, Tuple[str, Any]]:
            # python-consul doesn't allow to specify ttl smaller then 10 seconds
            # because session_ttl_min defaults to 10s, so we have to do this ugly dirty hack...
            pass
        return wrapper


class ConsulClient(base.Consul):

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Consul client with Patroni customisations.

        .. note::

            Parameters, *token*, *cert* and *ca_cert* are not passed to the parent class :class:`consul.base.Consul`.

        Original class documentation,

            *token* is an optional ``ACL token``. If supplied it will be used by
            default for all requests made with this client session. It's still
            possible to override this token by passing a token explicitly for a
            request.

            *consistency* sets the consistency mode to use by default for all reads
            that support the consistency option. It's still possible to override
            this by passing explicitly for a given request. *consistency* can be
            either 'default', 'consistent' or 'stale'.

            *dc* is the datacenter that this agent will communicate with.
            By default, the datacenter of the host is used.

            *verify* is whether to verify the SSL certificate for HTTPS requests

            *cert* client side certificates for HTTPS requests

        :param args: positional arguments to pass to :class:`consul.base.Consul`
        :param kwargs: keyword arguments, with *cert*, *ca_cert* and *token* removed, passed to
                       :class:`consul.base.Consul`
        """
        self._cert = kwargs.pop('cert', None)
        self._ca_cert = kwargs.pop('ca_cert', None)
        self.token = kwargs.get('token')
        super(ConsulClient, self).__init__(*args, **kwargs)

    def http_connect(self, *args: Any, **kwargs: Any) -> HTTPClient:
        pass

    def connect(self, *args: Any, **kwargs: Any) -> HTTPClient:
        pass

    def reload_config(self, config: Dict[str, Any]) -> None:
        pass


def catch_consul_errors(func: Callable[..., Any]) -> Callable[..., Any]:
    pass


def force_if_last_failed(func: Callable[..., Any]) -> Callable[..., Any]:
    pass


def service_name_from_scope_name(scope_name: str) -> str:
    """Translate scope name to service name which can be used in dns.

    230 = 253 - len('replica.') - len('.service.consul')
    """
    pass


class Consul(AbstractDCS):

    def __init__(self, config: Dict[str, Any], mpp: AbstractMPP) -> None:
        super(Consul, self).__init__(config, mpp)
        self._base_path = self._base_path[1:]
        self._scope = config['scope']
        self._session = None
        self.__do_not_watch = False
        self._retry = Retry(deadline=config['retry_timeout'], max_delay=1, max_tries=-1,
                            retry_exceptions=(ConsulInternalError, HTTPException,
                                              HTTPError, socket.error, socket.timeout))

        if 'url' in config:
            url: str = config['url']
            r = urlparse(url)
            config.update({'scheme': r.scheme, 'host': r.hostname, 'port': r.port or 8500})
        elif 'host' in config:
            host, port = split_host_port(config.get('host', '127.0.0.1:8500'), 8500)
            config['host'] = host
            if 'port' not in config:
                config['port'] = int(port)

        if config.get('cacert'):
            config['ca_cert'] = config.pop('cacert')

        if config.get('key') and config.get('cert'):
            config['cert'] = (config['cert'], config['key'])

        config_keys = ('host', 'port', 'token', 'scheme', 'cert', 'ca_cert', 'dc', 'consistency')
        kwargs: Dict[str, Any] = {p: config.get(p) for p in config_keys if config.get(p)}

        verify = config.get('verify')
        if not isinstance(verify, bool):
            verify = parse_bool(verify)
        if isinstance(verify, bool):
            kwargs['verify'] = verify

        self._client = ConsulClient(**kwargs)
        self.set_retry_timeout(config['retry_timeout'])
        self.set_ttl(config.get('ttl') or 30)
        self._last_session_refresh = 0
        self.__session_checks = config.get('checks', [])
        self._register_service = config.get('register_service', False)
        self._previous_loop_register_service = self._register_service
        self._service_tags = sorted(config.get('service_tags', []))
        self._previous_loop_service_tags = self._service_tags
        if self._register_service:
            self._set_service_name()
        self._service_check_interval = config.get('service_check_interval', '5s')
        self._service_check_tls_server_name = config.get('service_check_tls_server_name', None)
        if not self._ctl:
            self.create_session()
        self._previous_loop_token = self._client.token

    def retry(self, method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        return self._retry.copy()(method, *args, **kwargs)

    def create_session(self) -> None:
        pass

    def reload_config(self, config: Union['Config', Dict[str, Any]]) -> None:
        pass

    def set_ttl(self, ttl: int) -> Optional[bool]:
        pass

    @property
    def ttl(self) -> int:
        pass

    def set_retry_timeout(self, retry_timeout: int) -> None:
        pass

    def adjust_ttl(self) -> None:
        pass

    def _do_refresh_session(self, force: bool = False) -> bool:
        """:returns: `!True` if it had to create new session"""
        pass

    def refresh_session(self) -> bool:
        pass

    @staticmethod
    def member(node: Dict[str, str]) -> Member:
        pass

    def _cluster_from_nodes(self, nodes: Dict[str, Any]) -> Cluster:
        # get initialize flag
        pass

    @property
    def _consistency(self) -> str:
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

    @catch_consul_errors
    def touch_member(self, data: Dict[str, Any]) -> bool:
        pass

    def _set_service_name(self) -> None:
        pass

    @catch_consul_errors
    def register_service(self, service_name: str, **kwargs: Any) -> bool:
        pass

    @catch_consul_errors
    def deregister_service(self, service_id: str) -> bool:
        pass

    def _update_service(self, data: Dict[str, Any]) -> Optional[bool]:
        pass

    @force_if_last_failed
    def update_service(self, old_data: Dict[str, Any], new_data: Dict[str, Any], force: bool = False) -> Optional[bool]:
        pass

    def _do_attempt_to_acquire_leader(self, retry: Retry) -> bool:
        pass

    @catch_return_false_exception
    def attempt_to_acquire_leader(self) -> bool:
        pass

    def take_leader(self) -> bool:
        pass

    @catch_consul_errors
    def set_failover_value(self, value: str, version: Optional[int] = None) -> bool:
        pass

    @catch_consul_errors
    def set_config_value(self, value: str, version: Optional[int] = None) -> bool:
        pass

    @catch_consul_errors
    def _write_leader_optime(self, last_lsn: str) -> bool:
        pass

    @catch_consul_errors
    def _write_status(self, value: str) -> bool:
        pass

    @catch_consul_errors
    def _write_failsafe(self, value: str) -> bool:
        pass

    @staticmethod
    def _run_and_handle_exceptions(method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        pass

    @catch_return_false_exception
    def _update_leader(self, leader: Leader) -> bool:
        pass

    @catch_consul_errors
    def initialize(self, create_new: bool = True, sysid: str = '') -> bool:
        pass

    @catch_consul_errors
    def cancel_initialization(self) -> bool:
        pass

    @catch_consul_errors
    def delete_cluster(self) -> bool:
        pass

    @catch_consul_errors
    def set_history_value(self, value: str) -> bool:
        pass

    @catch_consul_errors
    def _delete_leader(self, leader: Leader) -> bool:
        pass

    @catch_consul_errors
    def set_sync_state_value(self, value: str, version: Optional[int] = None) -> Union[int, bool]:
        pass

    @catch_consul_errors
    def delete_sync_state(self, version: Optional[int] = None) -> bool:
        pass

    def watch(self, leader_version: Optional[int], timeout: float) -> bool:
        pass
