import logging
import re
import time

from threading import Condition, Event, Thread
from typing import Any, cast, Collection, Dict, Iterator, List, Optional, Set, Tuple, TYPE_CHECKING, Union
from urllib.parse import urlparse

from ...dcs import Cluster
from ...psycopg import connect, ProgrammingError, quote_ident
from ...utils import parse_int
from ..misc import PostgresqlRole, PostgresqlState
from . import AbstractMPP, AbstractMPPHandler

if TYPE_CHECKING:  # pragma: no cover
    from .. import Postgresql

CITUS_COORDINATOR_GROUP_ID = 0
CITUS_SLOT_NAME_RE = re.compile(r'^citus_shard_(move|split)_slot(_[1-9][0-9]*){2,3}$')
logger = logging.getLogger(__name__)


class PgDistNode:
    """Represents a single row in "pg_dist_node" table.

    .. note::

        Unlike "noderole" possible values of ``role`` are ``primary``, ``secondary``, and ``demoted``.
        The last one is used to pause client connections on the coordinator to the worker by
        appending ``-demoted`` suffix to the "nodename". The actual "noderole" in DB remains ``primary``.

    :ivar host: "nodename" value
    :ivar port: "nodeport" value
    :ivar role: "noderole" value
    :ivar nodeid: "nodeid" value
    """

    def __init__(self, host: str, port: int, role: str, nodeid: Optional[int] = None) -> None:
        """Create a :class:`PgDistNode` object based on given arguments.

        :param host: "nodename" of the Citus coordinator or worker.
        :param port: "nodeport" of the Citus coordinator or worker.
        :param role: "noderole" value.
        :param nodeid: id of the row in the "pg_dist_node".
        """
        self.host = host
        self.port = port
        self.role = role
        self.nodeid = nodeid

    def __hash__(self) -> int:
        """Defines a hash function to put :class:`PgDistNode` objects to :class:`PgDistGroup` set-like object.

        .. note::
            We use (:attr:`host`, :attr:`port`) tuple here because it is one of the UNIQUE constraints on the
            "pg_dist_node" table. The :attr:`role` value is irrelevant here because nodes may change their roles.
        """
        return hash((self.host, self.port))

    def __eq__(self, other: Any) -> bool:
        """Defines a comparison function.

        :returns: ``True`` if :attr:`host` and :attr:`port` between two instances are the same.
        """
        return isinstance(other, PgDistNode) and self.host == other.host and self.port == other.port

    def __str__(self) -> str:
        return ('PgDistNode(nodeid={0},host={1},port={2},role={3})'
                .format(self.nodeid, self.host, self.port, self.role))

    def __repr__(self) -> str:
        return str(self)

    def is_primary(self) -> bool:
        """Checks whether this object represents "primary" in a corresponding group.

        :returns: ``True`` if this object represents the ``primary``.
        """
        pass

    def as_tuple(self, include_nodeid: bool = False) -> Tuple[str, int, str, Optional[int]]:
        """Helper method to compare two :class:`PgDistGroup` objects.

        .. note::

            *include_nodeid* is set to ``True`` only in unit-tests.

        :param include_nodeid: whether :attr:`nodeid` should be taken into account when comparison is performed.

        :returns: :class:`tuple` object with :attr:`host`, :attr:`port`, :attr:`role`, and optionally :attr:`nodeid`.
        """
        pass


class PgDistGroup(Set[PgDistNode]):
    """A :class:`set`-like object that represents a Citus group in "pg_dist_node" table.

    This class implements a set of methods to compare topology and if it is necessary
    to transition from the old to the new topology in a "safe" manner:

        * register new primary/secondaries
        * replace gone secondaries with added secondaries
        * failover and switchover

    Typically there will be at least one :class:`PgDistNode` object registered (``primary``).
    In addition to that there could be one or more ``secondary`` nodes.

    :ivar failover: whether the ``primary`` row should be updated as a result of :func:`transition` method call.
    :ivar groupid: the "groupid" from "pg_dist_node".
    """

    def __init__(self, groupid: int, nodes: Optional[Collection[PgDistNode]] = None) -> None:
        """Creates a :class:`PgDistGroup` object based on given arguments.

        :param groupid: the groupid from "pg_dist_node".
        :param nodes: a collection of :class:`PgDistNode` objects that belong to a *groupid*.
        """
        self.failover = False
        self.groupid = groupid

        if nodes:
            self.update(nodes)

    def equals(self, other: 'PgDistGroup', check_nodeid: bool = False) -> bool:
        """Compares two :class:`PgDistGroup` objects.

        :param other: what we want to compare with.
        :param check_nodeid: whether :attr:`PgDistNode.nodeid` should be compared in addition to
                             :attr:`PgDistNode.host`, :attr:`PgDistNode.port`, and :attr:`PgDistNode.role`.

        :returns: ``True`` if two :class:`PgDistGroup` objects are fully identical.
        """
        pass

    def primary(self) -> Optional[PgDistNode]:
        """Finds and returns :class:`PgDistNode` object that represents the "primary".

        :returns: :class:`PgDistNode` object which represents the "primary" or ``None`` if not found.
        """
        pass

    def get(self, value: PgDistNode) -> Optional[PgDistNode]:
        """Performs a lookup of the actual value in a set.

        .. note::
            It is necessary because :func:`__hash__` and :func:`__eq__` methods in :class:`PgDistNode` are
            redefined and effectively they check only :attr:`PgDistNode.host` and :attr:`PgDistNode.port` attributes.

        :param value: the key we search for.
        :returns: the actual :class:`PgDistNode` value from this :class:`PgDistGroup` object or ``None`` if not found.
        """
        return next(iter(v for v in self if v == value), None)

    def transition(self, old: 'PgDistGroup') -> Iterator[PgDistNode]:
        """Compares this topology with the old one and yields transitions that transform the old to the new one.

        .. note::
            The actual yielded object is :class:`PgDistNode` that will be passed to
            the :meth:`CitusHandler.update_node` to execute all transitions in a transaction.

            In addition to the yielding transactions this method fills up :attr:`PgDistNode.nodeid`
            attribute for nodes that are presented in the old and in the new topology.

            There are a few simple rules/constraints that are imposed by Citus and must be followed:
            - adding/removing nodes is only possible when metadata is synced to all registered "priorities".

            - the "primary" row in "pg_dist_node" always keeps the nodeid (unless it is
              removed, but it is not supported by Patroni).

            - "nodename", "nodeport" must be unique across all rows in the "pg_dist_node". This means that
              every time we want to change the nodeid of an existing node (i.e. to change it from secondary
              to primary), we should first write some other "nodename"/"nodeport" to the row it's currently in.

            - updating "broken" nodes always works and metadata is synced asynchnonously after the commit.

        Following these rules below is an example of the switchover between node1 (primary, nodeid=4)
        and node2 (secondary, nodeid=5).

        .. code-block:: SQL

            BEGIN;
                SELECT citus_update_node(4, 'node1-demoted', 5432);
                SELECT citus_update_node(5, 'node1', 5432);
                SELECT citus_update_node(4, 'node2', 5432);
            COMMIT;

        :param old: the last known topology registered in "pg_dist_node" for a given :attr:`groupid`.

        :yields: :class:`PgDistNode` objects that must be updated/added/removed in "pg_dist_node".
        """
        pass


class PgDistTask(PgDistGroup):
    """A "task" that represents the current or desired state of "pg_dist_node" for a provided *groupid*.

    :ivar group: the "groupid" in "pg_dist_node".
    :ivar event: an "event" that resulted in creating this task.
                 possible values: "before_demote", "before_promote", "after_promote".
    :ivar timeout: a transaction timeout if the task resulted in starting a transaction.
    :ivar cooldown: the cooldown value for ``citus_update_node()`` UDF call.
    :ivar deadline: the time in unix seconds when the transaction is allowed to be rolled back.
    """

    def __init__(self, groupid: int, nodes: Optional[Collection[PgDistNode]], event: str,
                 timeout: Optional[float] = None, cooldown: Optional[float] = None) -> None:
        """Create a :class:`PgDistTask` object based on given arguments.

        :param groupid: the groupid from "pg_dist_node".
        :param nodes: a collection of :class:`PgDistNode` objects that belong to a *groupid*.
        :param event: an "event" that resulted in creating this task.
        :param timeout: a transaction timeout if the task resulted in starting a transaction.
        :param cooldown: the cooldown value for ``citus_update_node()`` UDF call.
        """
        super(PgDistTask, self).__init__(groupid, nodes)

        # Event that is trying to change or changed the given row.
        # Possible values: before_demote, before_promote, after_promote.
        self.event = event

        # If transaction was started, we need to COMMIT/ROLLBACK before the deadline
        self.timeout = timeout
        self.cooldown = cooldown or 10000  # 10s by default
        self.deadline: float = 0

        # All changes in the pg_dist_node are serialized on the Patroni
        # side by performing them from a thread. The thread, that is
        # requested a change, sometimes needs to wait for a result.
        # For example, we want to pause client connections before demoting
        # the worker, and once it is done notify the calling thread.
        self._event = Event()

    def wait(self) -> None:
        """Wait until this task is processed by a dedicated thread."""
        pass

    def wakeup(self) -> None:
        """Notify a thread that created a task that it was processed."""
        pass

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, PgDistTask) and self.event == other.event\
            and super(PgDistTask, self).equals(other)

    def __ne__(self, other: Any) -> bool:
        return not self == other


class Citus(AbstractMPP):

    group_re = re.compile('^(0|[1-9][0-9]*)$')

    @staticmethod
    def validate_config(config: Any) -> bool:
        """Check whether provided config is good for a given MPP.

        :param config: configuration of ``citus`` MPP section.

        :returns: ``True`` is config passes validation, otherwise ``False``.
        """
        pass

    @property
    def group(self) -> int:
        """The group of this Citus node."""
        return int(self._config['group'])

    @property
    def coordinator_group_id(self) -> int:
        """The group id of the Citus coordinator PostgreSQL cluster."""
        pass


class CitusHandler(Citus, AbstractMPPHandler, Thread):
    """Define the interfaces for handling an underlying Citus cluster."""

    def __init__(self, postgresql: 'Postgresql', config: Dict[str, Union[str, int]]) -> None:
        """"Initialize a new instance of :class:`CitusHandler`.

        :param postgresql: the Postgres node.
        :param config: the ``citus`` MPP config section.
        """
        Thread.__init__(self)
        AbstractMPPHandler.__init__(self, postgresql, config)
        self.daemon = True
        if config:
            self._connection = postgresql.connection_pool.get(
                'citus', {'dbname': config['database'],
                          'options': '-c statement_timeout=0 -c idle_in_transaction_session_timeout=0'})
        self._pg_dist_group: Dict[int, PgDistTask] = {}  # Cache of pg_dist_node: {groupid: PgDistTask()}
        self._tasks: List[PgDistTask] = []  # Requests to change pg_dist_group, every task is a `PgDistTask`
        self._in_flight: Optional[PgDistTask] = None  # Reference to the `PgDistTask` being changed in a transaction
        self._schedule_load_pg_dist_group = True  # Flag that "pg_dist_group" should be queried from the database
        self._condition = Condition()  # protects _pg_dist_group, _tasks, _in_flight, and _schedule_load_pg_dist_group
        self._ready_to_run = Event()
        self.schedule_cache_rebuild()
        if self.is_coordinator():
            self.start()

    def schedule_cache_rebuild(self) -> None:
        """Cache rebuild handler.

        Is called to notify handler that it has to refresh its metadata cache from the database.
        """
        pass

    def on_demote(self) -> None:
        pass

    def query(self, sql: str, *params: Any) -> List[Tuple[Any, ...]]:
        pass

    def load_pg_dist_group(self) -> bool:
        """Read from the `pg_dist_node` table and put it into the local cache"""
        pass

    def sync_meta_data(self, cluster: Cluster) -> None:
        """Maintain the ``pg_dist_node`` from the coordinator leader every heartbeat loop.

        We can't always rely on REST API calls from worker nodes in order
        to maintain `pg_dist_node`, therefore at least once per heartbeat
        loop we make sure that works registered in `self._pg_dist_group`
        cache are matching the cluster view from DCS by creating tasks
        the same way as it is done from the REST API."""
        pass

    def find_task_by_groupid(self, groupid: int) -> Optional[int]:
        pass

    def pick_task(self) -> Tuple[Optional[int], Optional[PgDistTask]]:
        """Returns the tuple(i, task), where `i` - is the task index in the self._tasks list

        Tasks are picked by following priorities:

        1. If there is already a transaction in progress, pick a task
           that that will change already affected worker primary.
        2. If the coordinator address should be changed - pick a task
           with groupid=0 (coordinators are always in groupid 0).
        3. Pick a task that is the oldest (first from the self._tasks)
        """
        pass

    def update_node(self, groupid: int, node: PgDistNode, cooldown: float = 10000) -> None:
        pass

    def update_group(self, task: PgDistTask, transaction: bool) -> None:
        pass

    def process_task(self, task: PgDistTask) -> bool:
        """Updates a single row in `pg_dist_group` table, optionally in a transaction.

        The transaction is started if we do a demote of the worker node or before promoting the other worker if
        there is no transaction in progress. And, the transaction is committed when the switchover/failover completed.

        .. note:
            The maximum lifetime of the transaction in progress is controlled outside of this method.

        .. note:
            Read access to `self._in_flight` isn't protected because we know it can't be changed outside of our thread.

        :param task: reference to a :class:`PgDistTask` object that represents a row to be updated/created.
        :returns: ``True`` if the row was successfully created/updated or transaction in progress
            was committed as an indicator that the `self._pg_dist_group` cache should be updated,
            or, if the new transaction was opened, this method returns `False`.
        """
        pass

    def process_tasks(self) -> None:
        pass

    def run(self) -> None:
        # we want to postpone "start" until first attempt to sync_meta_data
        pass

    def _add_task(self, task: PgDistTask) -> bool:
        pass

    @staticmethod
    def _pg_dist_node(role: str, conn_url: str) -> Optional[PgDistNode]:
        pass

    def add_task(self, event: str, groupid: int, cluster: Cluster, leader_name: str, leader_url: str,
                 timeout: Optional[float] = None, cooldown: Optional[float] = None) -> Optional[PgDistTask]:
        pass

    def handle_event(self, cluster: Cluster, event: Dict[str, Any]) -> None:
        pass

    def bootstrap(self) -> None:
        """Bootstrap handler.

        Is called when the new cluster is initialized (through ``initdb`` or a custom bootstrap method).
        """
        pass

    def adjust_postgres_gucs(self, parameters: Dict[str, Any]) -> None:
        """Adjust GUCs in the current PostgreSQL configuration.

        :param parameters: dictionary of GUCs, with key as GUC name and the corresponding value as current GUC value.
        """
        pass

    def ignore_replication_slot(self, slot: Dict[str, str]) -> bool:
        """Check whether provided replication *slot* existing in the database should not be removed.

        .. note::
            MPP database may create replication slots for its own use, for example to migrate data between workers
            using logical replication, and we don't want to suddenly drop them.

        :param slot: dictionary containing the replication slot settings, like ``name``, ``database``, ``type``, and
                     ``plugin``.

        :returns: ``True`` if the replication slots should not be removed, otherwise ``False``.
        """
        pass
