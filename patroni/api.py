"""Implement Patroni's REST API.

Exposes a REST API of patroni operations functions, such as status, performance and management to web clients.

Much of what can be achieved with the command line tool patronictl can be done via the API. Patroni CLI and daemon
utilises the API to perform these functions.
"""

import base64
import datetime
import hmac
import json
import logging
import os
import socket
import sys
import time
import traceback

from http.server import BaseHTTPRequestHandler, HTTPServer
from ipaddress import ip_address, ip_network, IPv4Network, IPv6Network
from socketserver import ThreadingMixIn
from typing import Any, Callable, cast, Dict, Iterator, List, Optional, Tuple, TYPE_CHECKING, Union
from urllib.parse import parse_qs, urlparse

import dateutil.parser

from . import global_config, psycopg
from .__main__ import Patroni
from .dcs import Cluster
from .exceptions import PostgresConnectionException, PostgresException
from .postgresql.misc import postgres_version_to_int, PostgresqlRole, PostgresqlState
from .thread_pool import PatroniThreadPoolExecutor
from .utils import cluster_as_json, deep_compare, enable_keepalive, parse_bool, \
    parse_int, patch_config, Retry, RetryFailedError, split_host_port, tzutc, uri

logger = logging.getLogger(__name__)


def check_access(*args: Any, **kwargs: Any) -> Callable[..., Any]:
    """Check the source ip, authorization header, or client certificates.

    .. note::
        The actual logic to check access is implemented through :func:`RestApiServer.check_access`.

        Optionally it is possible to skip source ip check by specifying ``allowlist_check_members=False``.

    :returns: a decorator that executes *func* only if :func:`RestApiServer.check_access` returns ``True``.

    :Example:

        >>> class FooServer:
        ...   def check_access(self, *args, **kwargs):
        ...     print(f'In FooServer: {args[0].__class__.__name__}')
        ...     return True
        pass

        >>> class Foo:
        ...   server = FooServer()
        ...   @check_access
        ...   def do_PUT_foo(self):
        ...      print('In do_PUT_foo')
        ...   @check_access(allowlist_check_members=False)
        ...   def do_POST_bar(self):
        ...      print('In do_POST_bar')

        >>> f = Foo()
        >>> f.do_PUT_foo()
        In FooServer: Foo
        In do_PUT_foo
    """
    allowlist_check_members = kwargs.get('allowlist_check_members', True)

    def inner_decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        pass

    # A hacky way to have decorators that work with and without parameters.
    if len(args) == 1 and callable(args[0]):
        # The first parameter is a function, it means decorator is used as "@check_access"
        return inner_decorator(args[0])
    else:
        # @check_access(allowlist_check_members=False) case
        return inner_decorator


class RestApiHandler(BaseHTTPRequestHandler):
    """Define how to handle each of the requests that are made against the REST API server."""

    # Comment from pyi stub file. These unions can cause typing errors with IDEs, e.g. PyCharm
    #
    # Those are technically of types, respectively:
    # * _RequestType = Union[socket.socket, Tuple[bytes, socket.socket]]
    # * _AddressType = Tuple[str, int]
    # But there are some concerns that having unions here would cause
    # too much inconvenience to people using it (see
    # https://github.com/python/typeshed/pull/384#issuecomment-234649696)

    def __init__(self, request: Any,
                 client_address: Any,
                 server: Union['RestApiServer', HTTPServer]) -> None:
        """Create a :class:`RestApiHandler` instance.

        .. note::
            Currently not different from its superclass :func:`__init__`, and only used so ``pyright`` can understand
            the type of ``server`` attribute.

        :param request: client request to be processed.
        :param client_address: address of the client connection.
        :param server: HTTP server that received the request.
        """
        if TYPE_CHECKING:  # pragma: no cover
            assert isinstance(server, RestApiServer)
        super(RestApiHandler, self).__init__(request, client_address, server)
        self.server: 'RestApiServer' = server  # pyright: ignore [reportIncompatibleVariableOverride]
        self.__start_time: float = 0.0
        self.path_query: Dict[str, List[str]] = {}

    def version_string(self) -> str:
        """Override the default version string to return the server header as specified in the configuration.

        If the server header is not set, then it returns the default version string of the HTTP server.

        :return: ``Server`` version string, which is either the server header or the default version string
            from the :class:`BaseHTTPRequestHandler`.
        """
        pass

    def _write_status_code_only(self, status_code: int) -> None:
        """Write a response that is composed only of the HTTP status.

        The response is written with these values separated by space:

            * HTTP protocol version;
            * *status_code*;
            * description of *status_code*.

        .. note::
            This is usually useful for replying to requests from software like HAProxy.

        :param status_code: HTTP status code.

        :Example:

            * ``_write_status_code_only(200)`` would write a response like ``HTTP/1.0 200 OK``.
        """
        pass

    def write_response(self, status_code: int, body: str, content_type: str = 'text/html',
                       headers: Optional[Dict[str, str]] = None) -> None:
        """Write an HTTP response.

        .. note::
            Besides ``Content-Type`` header, and the HTTP headers passed through *headers*, this function will also
            write the HTTP headers defined through ``restapi.http_extra_headers`` and ``restapi.https_extra_headers``
            from Patroni configuration.

        :param status_code: response HTTP status code.
        :param body: response body.
        :param content_type: value for ``Content-Type`` HTTP header.
        :param headers: dictionary of additional HTTP headers to set for the response. Each key is the header name, and
            the corresponding value is the value for the header in the response.
        """
        pass

    def _write_json_response(self, status_code: int, response: Any) -> None:
        """Write an HTTP response with a JSON content type.

        Call :func:`write_response` with ``content_type`` as ``application/json``.

        :param status_code: response HTTP status code.
        :param response: value to be dumped as a JSON string and to be used as the response body.
        """
        pass

    def _write_status_response(self, status_code: int, response: Dict[str, Any]) -> None:
        """Write an HTTP response with Patroni/Postgres status in JSON format.

        Modifies *response* before sending it to the client. Defines the ``patroni`` key, which is a
        dictionary that contains the mandatory keys:

            * ``version``: Patroni version, e.g. ``3.0.2``;
            * ``scope``: value of ``scope`` setting from Patroni configuration.

        May also add the following optional keys, depending on the status of this Patroni/PostgreSQL node:

            * ``tags``: tags that were set through Patroni configuration merged with dynamically applied tags;
            * ``database_system_identifier``: ``Database system identifier`` from ``pg_controldata`` output;
            * ``pending_restart``: ``True`` if PostgreSQL is pending to be restarted;
            * ``pending_restart_reason``: dictionary where each key is the parameter that caused "pending restart" flag
                to be set and the value is a dictionary with the old and the new value.
            * ``scheduled_restart``: a dictionary with a single key ``schedule``, which is the timestamp for the
                scheduled restart;
            * ``watchdog_failed``: ``True`` if watchdog device is unhealthy;
            * ``logger_queue_size``: log queue length if it is longer than expected;
            * ``logger_records_lost``: number of log records that have been lost while the log queue was full.

        :param status_code: response HTTP status code.
        :param response: represents the status of the PostgreSQL node, and is used as a basis for the HTTP response.
            This dictionary is built through :func:`get_postgresql_status`.
        """
        pass

    def do_GET(self, write_status_code_only: bool = False) -> None:
        """Process all GET requests which can not be routed to other methods.

        Is used for handling all health-checks requests. E.g. "GET /(primary|replica|sync|async|etc...)".

        The (optional) query parameters and the HTTP response status depend on the requested path:

            * ``/``, ``primary``, or ``read-write``:

                * HTTP status ``200``: if a primary with the leader lock.

            * ``/standby-leader``:

                * HTTP status ``200``: if holds the leader lock in a standby cluster.

            * ``/leader``:

                * HTTP status ``200``: if holds the leader lock.

            * ``/replica``:

                * Query parameters:

                    * ``lag``: only accept replication lag up to ``lag``. Accepts either an :class:`int`, which
                        represents lag in bytes, or a :class:`str` representing lag in human-readable format (e.g.
                        ``10MB``).
                    * ``replication_state``: only return HTTP status ``200`` if the node's state matches the
                        requested one (e.g. "streaming")
                    * Any custom parameter: will attempt to match them against node tags.

                * HTTP status ``200``: if up and running as a standby and without ``noloadbalance`` tag.

            * ``/read-only``:

                * HTTP status ``200``: if up and running and without ``noloadbalance`` tag.

            * ``/quorum``:

                * HTTP status ``200``: if up and running as a quorum synchronous standby.

            * ``/read-only-quorum``:

                * HTTP status ``200``: if up and running as a quorum synchronous standby or primary.

            * ``/synchronous`` or ``/sync``:

                * HTTP status ``200``: if up and running as a synchronous standby.

            * ``/read-only-sync``:

                * HTTP status ``200``: if up and running as a synchronous standby or primary.

            * ``/asynchronous``:

                * Query parameters:

                    * ``lag``: only accept replication lag up to ``lag``. Accepts either an :class:`int`, which
                        represents lag in bytes, or a :class:`str` representing lag in human-readable format (e.g.
                        ``10MB``).

                * HTTP status ``200``: if up and running as an asynchronous standby.

            * ``/health``:

                * HTTP status ``200``: if up and running.

        .. note::
            If not able to honor the query parameter, or not able to match the condition described for HTTP status
            ``200`` in each path above, then HTTP status will be ``503``.

        .. note::
            Independently of the requested path, if *write_status_code_only* is ``False``, then it always write an HTTP
            response through :func:`_write_status_response`, with the node status.

        :param write_status_code_only: indicates that instead of a normal HTTP response we should
                                       send only the HTTP Status Code and close the connection.
                                       Useful when health-checks are executed by HAProxy.
        """
        pass

    def do_OPTIONS(self) -> None:
        """Handle an ``OPTIONS`` request.

        Write a simple HTTP response that represents the current PostgreSQL status. Send only ``200 OK`` or
        ``503 Service Unavailable`` as a response and nothing more, particularly no headers.
        """
        pass

    def do_HEAD(self) -> None:
        """Handle a ``HEAD`` request.

        Write a simple HTTP response that represents the current PostgreSQL status. Send only ``200 OK`` or
        ``503 Service Unavailable`` as a response and nothing more, particularly no headers.
        """
        pass

    def do_GET_liveness(self) -> None:
        """Handle a ``GET`` request to ``/liveness`` path.

        Write a simple HTTP response with HTTP status:

            * ``200``:

                * If the cluster is in maintenance mode; or
                * If Patroni heartbeat loop is properly running;

            * ``503``:

                * if Patroni heartbeat loop last run was more than ``ttl`` setting ago on the primary (or twice the
                    value of ``ttl`` on a replica).

        """
        pass

    def _readiness(self) -> Optional[str]:
        """Check if readiness conditions are met.

        :returns: None if node can be considered ready or diagnostic message if not."""
        pass

    def do_GET_readiness(self) -> None:
        """Handle a ``GET`` request to ``/readiness`` path.

            * Query parameters:

                * ``lag``: only accept replication lag up to ``lag``. Accepts either an :class:`int`, which
                    represents lag in bytes, or a :class:`str` representing lag in human-readable format (e.g.
                    ``10MB``).
                * ``mode``: allowed values ``write``, ``apply``. Base replication lag off of received WAL or
                    replayed WAL. Defaults to ``apply``.

        Write a simple HTTP response which HTTP status can be:

            * ``200``:

                * If this Patroni node considers itself the leader; or
                * If PostgreSQL is running, replicating and not lagging;

            * ``503``: if none of the previous conditions apply.

        """
        pass

    def do_GET_patroni(self) -> None:
        """Handle a ``GET`` request to ``/patroni`` path.

        Write an HTTP response through :func:`_write_status_response`, with HTTP status ``200`` and the status of
        Postgres.
        """
        pass

    def do_GET_cluster(self) -> None:
        """Handle a ``GET`` request to ``/cluster`` path.

        Write an HTTP response with JSON content based on the output of :func:`~patroni.utils.cluster_as_json`, with
        HTTP status ``200`` and the JSON representation of the cluster topology.
        """
        pass

    def do_GET_history(self) -> None:
        """Handle a ``GET`` request to ``/history`` path.

        Write an HTTP response with a JSON content representing the history of events in the cluster, with HTTP status
        ``200``.

        The response contains a :class:`list` of failover/switchover events. Each item is a :class:`list` with the
        following items:

            * Timeline when the event occurred (class:`int`);
            * LSN at which the event occurred (class:`int`);
            * The reason for the event (class:`str`);
            * Timestamp when the new timeline was created (class:`str`);
            * Name of the involved Patroni node (class:`str`).

        """
        pass

    def do_GET_config(self) -> None:
        """Handle a ``GET`` request to ``/config`` path.

        Write an HTTP response with a JSON content representing the Patroni configuration that is stored in the DCS,
        with HTTP status ``200``.

        If the cluster information is not available in the DCS, then it will respond with no body and HTTP status
        ``502`` instead.
        """
        pass

    def do_GET_metrics(self) -> None:
        """Handle a ``GET`` request to ``/metrics`` path.

        Write an HTTP response with plain text content in the format used by Prometheus, with HTTP status ``200``.

        The response contains the following items:

            * ``patroni_version``: Patroni version without periods, e.g. ``030002`` for Patroni ``3.0.2``;
            * ``patroni_postgres_running``: ``1`` if PostgreSQL is running, else ``0``;
            * ``patroni_postmaster_start_time``: epoch timestamp since Postmaster was started;
            * ``patroni_primary``: ``1`` if this node holds the leader lock, else ``0``;
            * ``patroni_xlog_location``: ``pg_wal_lsn_diff(pg_current_wal_flush_lsn(), '0/0')`` if leader, else ``0``;
            * ``patroni_standby_leader``: ``1`` if standby leader node, else ``0``;
            * ``patroni_replica``: ``1`` if a replica, else ``0``;
            * ``patroni_sync_standby``: ``1`` if a sync replica, else ``0``;
            * ``patroni_quorum_standby``: ``1`` if a quorum sync replica, else ``0``;
            * ``patroni_xlog_received_location``: ``pg_wal_lsn_diff(pg_last_wal_receive_lsn(), '0/0')``;
            * ``patroni_xlog_replayed_location``: ``pg_wal_lsn_diff(pg_last_wal_replay_lsn(), '0/0)``;
            * ``patroni_xlog_replayed_timestamp``: ``pg_last_xact_replay_timestamp``;
            * ``patroni_xlog_paused``: ``pg_is_wal_replay_paused()``;
            * ``patroni_postgres_server_version``: Postgres version without periods, e.g. ``150002`` for Postgres
              ``15.2``;
            * ``patroni_cluster_unlocked``: ``1`` if no one holds the leader lock, else ``0``;
            * ``patroni_failsafe_mode_is_active``: ``1`` if ``failsafe_mode`` is currently active, else ``0``;
            * ``patroni_failsafe_mode_enabled``: ``1`` if ``failsafe_mode`` is enabled in configuration, else ``0``;
            * ``patroni_failsafe_member``: ``1`` if this node is a member of failsafe topology, else ``0``;
            * ``patroni_postgres_timeline``: PostgreSQL timeline based on current WAL file name;
            * ``patroni_dcs_last_seen``: epoch timestamp when DCS was last contacted successfully;
            * ``patroni_pending_restart``: ``1`` if this PostgreSQL node is pending a restart, else ``0``;
            * ``patroni_is_paused``: ``1`` if Patroni is in maintenance node, else ``0``.

        For PostgreSQL v9.6+ the response will also have the following:

            * ``patroni_postgres_streaming``: 1 if Postgres is streaming from another node, else ``0``;
            * ``patroni_postgres_in_archive_recovery``: ``1`` if Postgres isn't streaming and
              there is ``restore_command`` available, else ``0``.
        """
        pass

    def _read_json_content(self, body_is_optional: bool = False) -> Optional[Dict[Any, Any]]:
        """Read JSON from HTTP request body.

        .. note::
            Retrieves the request body based on `content-length` HTTP header. The body is expected to be a JSON
            string with that length.

            If request body is expected but `content-length` HTTP header is absent, then write an HTTP response
            with HTTP status ``411``.

            If request body is expected but contains nothing, or if an exception is faced, then write an HTTP
            response with HTTP status ``400``.

        :param body_is_optional: if ``False`` then the request must contain a body. If ``True``, then the request may or
            may not contain a body.

        :returns: deserialized JSON string from request body, if present. If body is absent, but *body_is_optional* is
            ``True``, then return an empty dictionary. Returns ``None`` otherwise.
        """
        pass

    @check_access
    def do_PATCH_config(self) -> None:
        """Handle a ``PATCH`` request to ``/config`` path.

        Updates the Patroni configuration based on the JSON request body, then writes a response with the new
        configuration, with HTTP status ``200``.

        .. note::
            If the configuration has been previously wiped out from DCS, then write a response with
            HTTP status ``503``.

            If applying a configuration value fails, then write a response with HTTP status ``409``.
        """
        pass

    @check_access
    def do_PUT_config(self) -> None:
        """Handle a ``PUT`` request to ``/config`` path.

        Overwrites the Patroni configuration based on the JSON request body, then writes a response with the new
        configuration, with HTTP status ``200``.

        .. note::
            If applying the new configuration fails, then write a response with HTTP status ``502``.
        """
        pass

    @check_access
    def do_POST_reload(self) -> None:
        """Handle a ``POST`` request to ``/reload`` path.

        Schedules a reload to Patroni and writes a response with HTTP status ``202``.
        """
        pass

    def do_GET_failsafe(self) -> None:
        """Handle a ``GET`` request to ``/failsafe`` path.

        Writes a response with a JSON string body containing all nodes that are known to Patroni at a given point
        in time, with HTTP status ``200``. The JSON contains a dictionary, each key is the name of the Patroni node,
        and the corresponding value is the URI to access `/patroni` path of its REST API.

        .. note::
            If ``failsafe_mode`` is not enabled, then write a response with HTTP status ``502``.
        """
        pass

    @check_access(allowlist_check_members=False)
    def do_POST_failsafe(self) -> None:
        """Handle a ``POST`` request to ``/failsafe`` path.

        Writes a response with HTTP status ``200`` if this node is a Standby, or with HTTP status ``500`` if this is
        the primary. In addition to that it returns absolute value of received/replayed LSN in the ``lsn`` header.

        .. note::
            If ``failsafe_mode`` is not enabled, then write a response with HTTP status ``502``.
        """
        pass

    @check_access
    def do_POST_sigterm(self) -> None:
        """Handle a ``POST`` request to ``/sigterm`` path.

        Schedule a shutdown and write a response with HTTP status ``202``.

        .. note::
            Only for behave testing on Windows.
        """
        pass

    @staticmethod
    def parse_schedule(schedule: str,
                       action: str) -> Tuple[Union[int, None], Union[str, None], Union[datetime.datetime, None]]:
        """Parse the given *schedule* and validate it.

        :param schedule: a string representing a timestamp, e.g. ``2023-04-14T20:27:00+00:00``.
        :param action: the action to be scheduled (``restart``, ``switchover``, or ``failover``).

        :returns: a tuple composed of 3 items:

            * Suggested HTTP status code for a response:

                * ``None``: if no issue was faced while parsing, leaving it up to the caller to decide the status; or
                * ``400``: if no timezone information could be found in *schedule*; or
                * ``422``: if *schedule* is invalid -- in the past or not parsable.

            * An error message, if any error is faced, otherwise ``None``;
            * Parsed *schedule*, if able to parse, otherwise ``None``.

        """
        pass

    @check_access
    def do_POST_restart(self) -> None:
        """Handle a ``POST`` request to ``/restart`` path.

        Used to restart postgres (or schedule a restart), mainly by ``patronictl restart``.

        The request body should be a JSON dictionary, and it can contain the following keys:

            * ``schedule``: timestamp at which the restart should occur;
            * ``role``: restart only nodes which role is ``role``. Can be either:

                * ``primary`; or
                * ``replica``.

            * ``postgres_version``: restart only nodes which PostgreSQL version is less than ``postgres_version``, e.g.
                ``15.2``;
            * ``timeout``: if restart takes longer than ``timeout`` return an error and fail over to a replica;
            * ``restart_pending``: if we should restart only when have ``pending restart`` flag;

        Response HTTP status codes:

            * ``200``: if successfully performed an immediate restart; or
            * ``202``: if successfully scheduled a restart for later; or
            * ``500``: if the cluster is in maintenance mode; or
            * ``400``: if

                * ``role`` value is invalid; or
                * ``postgres_version`` value is invalid; or
                * ``timeout`` is not a number, or lesser than ``0``; or
                * request contains an unknown key; or
                * exception is faced while performing an immediate restart.

            * ``409``: if another restart was already previously scheduled; or
            * ``503``: if any issue was found while performing an immediate restart; or
            * HTTP status returned by :func:`parse_schedule`, if any error was observed while parsing the schedule.

        .. note::
            If it's not able to parse the request body, then the request is silently discarded.
        """
        pass

    @check_access
    def do_DELETE_restart(self) -> None:
        """Handle a ``DELETE`` request to ``/restart`` path.

        Used to remove a scheduled restart of PostgreSQL.

        Response HTTP status codes:

            * ``200``: if a scheduled restart was removed; or
            * ``404``: if no scheduled restart could be found.
        """
        pass

    @check_access
    def do_DELETE_switchover(self) -> None:
        """Handle a ``DELETE`` request to ``/switchover`` path.

        Used to remove a scheduled switchover in the cluster.

        It writes a response, and the HTTP status code can be:

            * ``200``: if a scheduled switchover was removed; or
            * ``404``: if no scheduled switchover could be found; or
            * ``409``: if not able to update the switchover info in the DCS.
        """
        pass

    @check_access
    def do_POST_reinitialize(self) -> None:
        """Handle a ``POST`` request to ``/reinitialize`` path.

        The request body may contain a JSON dictionary with the following key:

            * ``force``: ``True`` if we want to cancel an already running task in order to reinit a replica.
            * ``from_leader``: ``True`` if we want to reinit a replica and get basebackup from the leader node.

        Response HTTP status codes:

            * ``200``: if the reinit operation has started; or
            * ``503``: if any error is returned by :func:`~patroni.ha.Ha.reinitialize`.
        """
        pass

    def poll_failover_result(self, leader: Optional[str], candidate: Optional[str], action: str) -> Tuple[int, str]:
        """Poll failover/switchover operation until it finishes or times out.

        :param leader: name of the current Patroni leader.
        :param candidate: name of the Patroni node to be promoted.
        :param action: the action that is ongoing (``switchover`` or ``failover``).

        :returns: a tuple composed of 2 items:

            * Response HTTP status codes:

                * ``200``: if the operation succeeded; or
                * ``503``: if the operation failed or timed out.

            * A status message about the operation.

        """
        pass

    def is_failover_possible(self, cluster: Cluster, leader: Optional[str], candidate: Optional[str],
                             action: str) -> Optional[str]:
        """Checks whether there are nodes that could take over after demoting the primary.

        :param cluster: the Patroni cluster.
        :param leader: name of the current Patroni leader.
        :param candidate: name of the Patroni node to be promoted.
        :param action: the action to be performed (``switchover`` or ``failover``).

        :returns: a string with the error message or ``None`` if good nodes are found.
        """
        pass

    @check_access
    def do_POST_failover(self, action: str = 'failover') -> None:
        """Handle a ``POST`` request to ``/failover`` path.

        Handles manual failovers/switchovers, mainly from ``patronictl``.

        The request body should be a JSON dictionary, and it can contain the following keys:

            * ``leader``: name of the current leader in the cluster;
            * ``candidate``: name of the Patroni node to be promoted;
            * ``scheduled_at``: a string representing the timestamp when to execute the switchover/failover, e.g.
                ``2023-04-14T20:27:00+00:00``.

        Response HTTP status codes:

            * ``202``: if operation has been scheduled;
            * ``412``: if operation is not possible;
            * ``503``: if unable to register the operation to the DCS;
            * HTTP status returned by :func:`parse_schedule`, if any error was observed while parsing the schedule;
            * HTTP status returned by :func:`poll_failover_result` if the operation has been processed immediately;
            * ``400``: if none of the above applies.

        .. note::
            If unable to parse the request body, then the request is silently discarded.

        :param action: the action to be performed (``switchover`` or ``failover``).
        """
        pass

    def do_POST_switchover(self) -> None:
        """Handle a ``POST`` request to ``/switchover`` path.

        Calls :func:`do_POST_failover` with ``switchover`` option.
        """
        pass

    @check_access
    def do_POST_citus(self) -> None:
        """Handle a ``POST`` request to ``/citus`` path.

        .. note::
            We keep this entrypoint for backward compatibility and simply dispatch the request to :meth:`do_POST_mpp`.
        """
        pass

    def do_POST_mpp(self) -> None:
        """Handle a ``POST`` request to ``/mpp`` path.

        Call :func:`~patroni.postgresql.mpp.AbstractMPPHandler.handle_event` to handle the request,
        then write a response with HTTP status code ``200``.

        .. note::
            If unable to parse the request body, then the request is silently discarded.
        """
        pass

    def parse_request(self) -> bool:
        """Override :func:`parse_request` to enrich basic functionality of :class:`~http.server.BaseHTTPRequestHandler`.

        Original class can only invoke :func:`do_GET`, :func:`do_POST`, :func:`do_PUT`, etc method implementations if
        they are defined.

        But we would like to have at least some simple routing mechanism, i.e.:

            * ``GET /uri1/part2`` request should invoke :func:`do_GET_uri1()`
            * ``POST /other`` should invoke :func:`do_POST_other()`

        If the :func:`do_<REQUEST_METHOD>_<first_part_url>` method does not exist we'll fall back to original behavior.

        :returns: ``True`` for success, ``False`` for failure; on failure, any relevant error response has already been
                  sent back.

        """
        pass

    def query(self, sql: str, *params: Any, retry: bool = False) -> List[Tuple[Any, ...]]:
        """Execute *sql* query with *params* and optionally return results.

        :param sql: the SQL statement to be run.
        :param params: positional arguments to call :func:`RestApiServer.query` with.
        :param retry: whether the query should be retried upon failure or given up immediately.

        :returns: a list of rows that were fetched from the database.
        """
        pass

    def get_postgresql_status(self, retry: bool = False) -> Dict[str, Any]:
        """Builds an object representing a status of "postgres".

        Some of the values are collected by executing a query and other are taken from the state stored in memory.

        :param retry: whether the query should be retried if failed or give up immediately

        :returns: a dict with the status of Postgres/Patroni. The keys are:

            * ``state``: one of :class:`~patroni.postgresql.misc.PostgresqlState` or ``unknown``;
            * ``postmaster_start_time``: ``pg_postmaster_start_time()``;
            * ``role``: :class:`~patroni.postgresql.misc.PostgresqlRole.REPLICA` or
                :class:`~patroni.postgresql.misc.PostgresqlRole.PRIMARY` based on ``pg_is_in_recovery()`` output;
            * ``server_version``: Postgres version without periods, e.g. ``150002`` for Postgres ``15.2``;
            * ``latest_end_lsn``: latest_end_lsn value from ``pg_stat_get_wal_receiver()``, only on replica nodes;
            * ``xlog``: dictionary. Its structure depends on ``role``:

                * If :class:`~patroni.postgresql.misc.PostgresqlRole.PRIMARY`:

                    * ``location``: ``pg_current_wal_flush_lsn()``

                * If :class:`~patroni.postgresql.misc.PostgresqlRole.REPLICA`:

                    * ``received_location``: ``pg_wal_lsn_diff(pg_last_wal_receive_lsn(), '0/0')``;
                    * ``replayed_location``: ``pg_wal_lsn_diff(pg_last_wal_replay_lsn(), '0/0)``;
                    * ``replayed_timestamp``: ``pg_last_xact_replay_timestamp``;
                    * ``paused``: ``pg_is_wal_replay_paused()``;

            * ``sync_standby``: ``True`` if replication mode is synchronous and this is a sync standby;
            * ``quorum_standby``: ``True`` if replication mode is quorum and this is a quorum standby;
            * ``timeline``: PostgreSQL primary node timeline;
            * ``replication``: :class:`list` of :class:`dict` entries, one for each replication connection. Each entry
                contains the following keys:

                * ``application_name``: ``pg_stat_activity.application_name``;
                * ``client_addr``: ``pg_stat_activity.client_addr``;
                * ``state``: ``pg_stat_replication.state``;
                * ``sync_priority``: ``pg_stat_replication.sync_priority``;
                * ``sync_state``: ``pg_stat_replication.sync_state``;
                * ``usename``: ``pg_stat_activity.usename``.

            * ``pause``: ``True`` if cluster is in maintenance mode;
            * ``cluster_unlocked``: ``True`` if cluster has no node holding the leader lock;
            * ``failsafe_mode_is_active``: ``True`` if DCS failsafe mode is currently active;
            * ``dcs_last_seen``: epoch timestamp DCS was last reached by Patroni.

        """
        pass

    def handle_one_request(self) -> None:
        """Parse and dispatch a request to the appropriate ``do_*`` method.

        .. note::
            This is only used to keep track of latency when logging messages through :func:`log_message`.
        """
        pass

    def log_message(self, format: str, *args: Any) -> None:
        """Log a custom ``debug`` message.

        Additionally, to *format*, the log entry contains the client IP address and the current latency of the request.

        :param format: printf-style format string message to be logged.
        :param args: arguments to be applied as inputs to *format*.
        """
        pass


class RestApiServer(ThreadingMixIn, HTTPServer):
    """Patroni REST API server.

    An asynchronous thread-pool-based HTTP server.
    """

    def __init__(self, patroni: Patroni, config: Dict[str, Any]) -> None:
        """Establish patroni configuration for the REST API daemon.

        Create a :class:`RestApiServer` instance.

        :param patroni: Patroni daemon process.
        :param config: ``restapi`` section of Patroni configuration.
        """
        self.connection_string: str
        self.__auth_key = None
        self.__allowlist_include_members: Optional[bool] = None
        self.__allowlist: Tuple[Union[IPv4Network, IPv6Network], ...] = ()
        self.http_extra_headers: Dict[str, str] = {}
        self.patroni = patroni
        self.__listen = None
        self.request_queue_size = int(config.get('request_queue_size', 5))
        try:
            thread_pool_size = max(5, int(config.get('thread_pool_size', 5)))
        except Exception as e:
            logger.warning('Failed to parse restapi.thread_pool_size value "%s": %r', config.get('thread_pool_size'), e)
            thread_pool_size = 5
        logger.info('REST API thread_pool_size = %d', thread_pool_size)
        self._executor = PatroniThreadPoolExecutor(max_workers=thread_pool_size + 1, thread_name_prefix='RestAPI')
        self.__ssl_options: Dict[str, Any] = {}
        self.__ssl_serial_number = None
        self._received_new_cert = False
        self.reload_config(config)
        self.daemon = True

    def construct_server_tokens(self, token_config: str) -> str:
        """Construct the value for the ``Server`` HTTP header based on *server_tokens*.

        :param server_tokens: the value of ``restapi.server_tokens`` configuration option.

        :returns: a string to be used as the value of ``Server`` HTTP header.
        """
        pass

    def query(self, sql: str, *params: Any) -> List[Tuple[Any, ...]]:
        """Execute *sql* query with *params* and optionally return results.

        .. note::
            Prefer to use own connection to postgres and fallback to ``heartbeat`` when own isn't available.

        :param sql: the SQL statement to be run.
        :param params: positional arguments to be used as parameters for *sql*.

        :returns: a list of rows that were fetched from the database.

        :raises:
            :class:`psycopg.Error`: if had issues while executing *sql*.
            :class:`~patroni.exceptions.PostgresConnectionException`: if had issues while connecting to the database.
        """
        pass

    @staticmethod
    def _set_fd_cloexec(fd: socket.socket) -> None:
        """Set ``FD_CLOEXEC`` for *fd*.

        It is used to avoid inheriting the REST API port when forking its process.

        .. note::
            Only takes effect on non-Windows environments.

        :param fd: socket file descriptor.
        """
        pass

    def check_basic_auth_key(self, key: str) -> bool:
        """Check if *key* matches the password configured for the REST API.

        :param key: the password received through the Basic authorization header of an HTTP request.

        :returns: ``True`` if *key* matches the password configured for the REST API.
        """
        pass

    def check_auth_header(self, auth_header: Optional[str]) -> Optional[str]:
        """Validate HTTP Basic authorization header, if present.

        :param auth_header: value of ``Authorization`` HTTP header, if present, else ``None``.

        :returns: an error message if any issue is found, ``None`` otherwise.
        """
        pass

    @staticmethod
    def __resolve_ips(host: str, port: int) -> Iterator[Union[IPv4Network, IPv6Network]]:
        """Resolve *host* + *port* to one or more IP networks.

        :param host: hostname to be checked.
        :param port: port to be checked.

        :yields: *host* + *port* resolved to IP networks.
        """
        pass

    def __members_ips(self) -> Iterator[Union[IPv4Network, IPv6Network]]:
        """Resolve each Patroni node ``restapi.connect_address`` to IP networks.

        .. note::
            Only yields object if ``restapi.allowlist_include_members`` setting is enabled.

        :yields: each node ``restapi.connect_address`` resolved to an IP network.
        """
        pass

    def check_access(self, rh: RestApiHandler, allowlist_check_members: bool = True) -> Optional[bool]:
        """Ensure client has enough privileges to perform a given request.

        Write a response back to the client if any issue is observed, and the HTTP status may be:

            * ``401``: if ``Authorization`` header is missing or contain an invalid password;
            * ``403``: if:

                * ``restapi.allowlist`` was configured, but client IP is not in the allowed list; or
                * ``restapi.allowlist_include_members`` is enabled, but client IP is not in the members list; or
                * a client certificate is expected by the server, but is missing in the request.

        :param rh: the request which access should be checked.
        :param allowlist_check_members: whether we should check the source ip against existing cluster members.

        :returns: ``True`` if client access verification succeeded, otherwise ``None``.
        """
        allowlist_check_members = allowlist_check_members and bool(self.__allowlist_include_members)
        if self.__allowlist or allowlist_check_members:
            incoming_ip = ip_address(rh.client_address[0])

            members_ips = tuple(self.__members_ips()) if allowlist_check_members else ()

            if not any(incoming_ip in net for net in self.__allowlist + members_ips):
                return rh.write_response(403, 'Access is denied')

        if not hasattr(rh.request, 'getpeercert') or not rh.request.getpeercert():  # valid client cert isn't present
            if self.__protocol == 'https' and self.__ssl_options.get('verify_client') in ('required', 'optional'):
                return rh.write_response(403, 'client certificate required')

        reason = self.check_auth_header(rh.headers.get('Authorization'))
        if reason:
            headers = {'WWW-Authenticate': 'Basic realm="' + self.patroni.__class__.__name__ + '"'}
            return rh.write_response(401, reason, headers=headers)
        return True

    @staticmethod
    def __has_dual_stack() -> bool:
        """Check if the system has support for dual stack sockets.

        :returns: ``True`` if it has support for dual stack sockets.
        """
        pass

    def __httpserver_init(self, host: str, port: int) -> None:
        """Start REST API HTTP server.

        .. note::
            If system has no support for dual stack sockets, then IPv4 is preferred over IPv6.

        :param host: host to bind REST API to.
        :param port: port to bind REST API to.
        """
        pass

    def __initialize(self, listen: str, ssl_options: Dict[str, Any]) -> None:
        """Configure and start REST API HTTP server.

        .. note::
            This method can be called upon first initialization, and also when reloading Patroni. When reloading
            Patroni, it restarts the HTTP server thread.

        :param listen: IP and port to bind REST API to. It should be a string in the format ``host:port``, where
            ``host`` can be a hostname or IP address. It is the value of ``restapi.listen`` setting.
        :param ssl_options: dictionary that may contain the following keys, depending on what has been configured in
            ``restapi`` section:

            * ``certfile``: path to PEM certificate. If given, will start in HTTPS mode;
            * ``keyfile``: path to key of ``certfile``;
            * ``keyfile_password``: password for decrypting ``keyfile``;
            * ``cafile``: path to CA file to validate client certificates;
            * ``ciphers``: permitted cipher suites;
            * ``verify_client``: value can be one among:

                * ``none``: do not check client certificates;
                * ``optional``: check client certificate only for unsafe REST API endpoints;
                * ``required``: check client certificate for all REST API endpoints.

        :raises:
            :class:`ValueError`: if any issue is faced while parsing *listen*.
        """
        pass

    def start(self) -> None:
        pass

    def process_request_thread(self, request: Union[socket.socket, Tuple[bytes, socket.socket]],
                               client_address: Tuple[str, int]) -> None:
        """Process a request to the REST API.

        Wrapper for :func:`~socketserver.ThreadingMixIn.process_request_thread` that additionally:

            * Enable TCP keepalive
            * Perform SSL handshake (if an SSL socket).

        :param request: socket to handle the client request.
        :param client_address: tuple containing the client IP and port.
        """
        pass

    def process_request(self, request: Union[socket.socket, Tuple[bytes, socket.socket]],
                        client_address: Tuple[str, int]) -> None:
        pass

    def shutdown(self) -> None:
        pass

    def shutdown_request(self, request: Union[socket.socket, Tuple[bytes, socket.socket]]) -> None:
        """Shut down a request to the REST API.

        Wrapper for :func:`http.server.HTTPServer.shutdown_request` that additionally:

            * Perform SSL shutdown handshake (if a SSL socket).

        :param request: socket to handle the client request.
        """
        pass

    def get_certificate_serial_number(self) -> Optional[str]:
        """Get serial number of the certificate used by the REST API.

        :returns: serial number of the certificate configured through ``restapi.certfile`` setting.
        """
        pass

    def reload_local_certificate(self) -> Optional[bool]:
        """Reload the SSL certificate used by the REST API.

        :return: ``True`` if a different certificate has been configured through ``restapi.certfile` setting, ``None``
            otherwise.
        """
        pass

    def _build_allowlist(self, value: Optional[List[str]]) -> Iterator[Union[IPv4Network, IPv6Network]]:
        """Resolve each entry in *value* to an IP network object.

        :param value: list of IPs and/or networks contained in ``restapi.allowlist`` setting. Each item can be a host,
            an IP, or a network in CIDR format.

        :yields: *host* + *port* resolved to IP networks.
        """
        pass

    def reload_config(self, config: Dict[str, Any]) -> None:
        """Reload REST API configuration.

        :param config: dictionary representing values under the ``restapi`` configuration section.

        :raises:
            :class:`ValueError`: if ``listen`` key is not present in *config*.
        """
        pass

    def handle_error(self, request: Union[socket.socket, Tuple[bytes, socket.socket]],
                     client_address: Tuple[str, int]) -> None:
        """Handle any exception that is thrown while handling a request to the REST API.

        Logs ``WARNING`` messages with the client information, and the stack trace of the faced exception.

        :param request: the request that faced an exception.
        :param client_address: a tuple composed of the IP and port of the client connection.
        """
        pass
