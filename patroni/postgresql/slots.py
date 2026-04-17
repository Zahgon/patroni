"""Replication slot handling.

Provides classes for the creation, monitoring, management and synchronisation of PostgreSQL replication slots.
"""

import logging
import os
import shutil

from collections import defaultdict
from contextlib import contextmanager
from threading import Condition, Thread
from typing import Any, Collection, Dict, Iterator, List, Optional, Tuple, TYPE_CHECKING, Union

from .. import global_config
from ..dcs import Cluster, Leader
from ..file_perm import pg_perm
from ..psycopg import OperationalError
from ..tags import Tags
from .connection import get_connection_cursor
from .misc import format_lsn, fsync_dir, PostgresqlRole

if TYPE_CHECKING:  # pragma: no cover
    from psycopg import Cursor
    from psycopg2 import cursor

    from . import Postgresql

logger = logging.getLogger(__name__)


def compare_slots(s1: Dict[str, Any], s2: Dict[str, Any], dbid: str = 'database') -> bool:
    """Compare 2 replication slot objects for equality.

    ..note ::
        If the first argument is a ``physical`` replication slot then only the `type` of the second slot is compared.
        If the first argument is another ``type`` (e.g. ``logical``) then *dbid* and ``plugin`` are compared.

    :param s1: First slot dictionary to be compared.
    :param s2: Second slot dictionary to be compared.
    :param dbid: Optional attribute to be compared when comparing ``logical`` replication slots.

    :return: ``True`` if the slot ``type`` of *s1* and *s2* is matches, and the ``type`` of *s1* is ``physical``,
             OR the ``types`` match AND the *dbid* and ``plugin`` attributes are equal.

    """
    pass


class SlotsAdvanceThread(Thread):
    """Daemon process :class:``Thread`` object for advancing logical replication slots on replicas.

    This ensures that slot advancing queries sent to postgres do not block the main loop.
    """

    def __init__(self, slots_handler: 'SlotsHandler') -> None:
        """Create and start a new thread for handling slot advance queries.

        :param slots_handler: The calling class instance for reference to slot information attributes.
        """
        super().__init__()
        self.daemon = True
        self._slots_handler = slots_handler

        # _copy_slots and _failed are used to asynchronously give some feedback to the main thread
        self._copy_slots: List[str] = []
        self._failed = False
        # {'dbname1': {'slot1': 100, 'slot2': 100}, 'dbname2': {'slot3': 100}}
        self._scheduled: Dict[str, Dict[str, int]] = defaultdict(dict)
        self._condition = Condition()  # protect self._scheduled from concurrent access and to wakeup the run() method

        self.start()

    def sync_slot(self, cur: Union['cursor', 'Cursor[Any]'], database: str, slot: str, lsn: int) -> None:
        """Execute a ``pg_replication_slot_advance`` query and store success for scheduled synchronisation task.

        :param cur: database connection cursor.
        :param database: name of the database associated with the slot.
        :param slot: name of the slot to be synchronised.
        :param lsn: last known LSN position
        """
        pass

    def sync_slots_in_database(self, database: str, slots: List[str]) -> None:
        """Synchronise slots for a single database.

        :param database: name of the database.
        :param slots: list of slot names to synchronise.
        """
        pass

    def sync_slots(self) -> None:
        """Synchronise slots for all scheduled databases."""
        pass

    def run(self) -> None:
        """Thread main loop entrypoint.

        .. note::
            Thread will wait until a sync is scheduled from outside, normally triggered during the HA loop or a wakeup
            call.
        """
        pass

    def schedule(self, advance_slots: Dict[str, Dict[str, int]]) -> Tuple[bool, List[str]]:
        """Trigger a synchronisation of slots.

        This is the main entrypoint for Patroni HA loop wakeup call.

        :param advance_slots: dictionary containing slots that need to be advanced

        :return: tuple of failure status and a list of slots to be copied
        """
        pass

    def clean(self) -> None:
        """Reset state of the daemon."""
        pass


class SlotsHandler:
    """Handler for managing and storing information on replication slots in PostgreSQL.

    :ivar pg_replslot_dir: system location path of the PostgreSQL replication slots.
    :ivar _logical_slots_processing_queue: yet to be processed logical replication slots on the primary
    """

    def __init__(self, postgresql: 'Postgresql') -> None:
        """Create an instance with storage attributes for replication slots and schedule the first synchronisation.

        :param postgresql: Calling class instance providing interface to PostgreSQL.
        """
        self._force_readiness_check = False
        self._schedule_load_slots = False
        self._postgresql = postgresql
        self._advance = SlotsAdvanceThread(self)
        self._replication_slots: Dict[str, Dict[str, Any]] = {}  # already existing replication slots
        self._logical_slots_processing_queue: Dict[str, Optional[int]] = {}
        self.pg_replslot_dir = os.path.join(self._postgresql.data_dir, 'pg_replslot')
        self.schedule()

    def _query(self, sql: str, *params: Any) -> List[Tuple[Any, ...]]:
        """Helper method for :meth:`Postgresql.query`.

        :param sql: SQL statement to execute.
        :param params: parameters to pass through to :meth:`Postgresql.query`.

        :returns: query response.
        """
        pass

    @staticmethod
    def _copy_items(src: Dict[str, Any], dst: Dict[str, Any], keys: Optional[Collection[str]] = None) -> None:
        """Select values from *src* dictionary to update in *dst* dictionary for optional supplied *keys*.

        :param src: source dictionary that *keys* will be looked up from.
        :param dst: destination dictionary to be updated.
        :param keys: optional list of keys to be looked up in the source dictionary.
        """
        pass

    def process_permanent_slots(self, slots: List[Dict[str, Any]]) -> Dict[str, int]:
        """Process replication slot information from the host and prepare information used in subsequent cluster tasks.

        .. note::
            This methods solves three problems.

            The ``cluster_info_query`` from :class:``Postgresql`` is executed every HA loop and returns information
            about all replication slots that exists on the current host.

            Based on this information perform the following actions:

            1. For the primary we want to expose to DCS permanent logical slots, therefore build (and return) a dict
               that maps permanent logical slot names to ``confirmed_flush_lsn``.
            2. detect if one of the previously known permanent slots is missing and schedule resync.
            3. Update the local cache with the fresh ``catalog_xmin`` and ``confirmed_flush_lsn`` for every known slot.

           This info is used when performing the check of logical slot readiness on standbys.

        :param slots: replication slot information that exists on the current host.

        :return: dictionary of logical slot names to ``confirmed_flush_lsn``.
        """
        pass

    def load_replication_slots(self) -> None:
        """Query replication slot information from the database and store it for processing by other tasks.

        .. note::
            Only supported from PostgreSQL version 9.4 onwards.

        Store replication slot ``name``, ``type``, ``plugin``, ``database`` and ``datoid``.
        If PostgreSQL version is 10 or newer also store ``catalog_xmin`` and ``confirmed_flush_lsn``.
        If PostgreSQL version is 17 or above also fetch ``failover`` and ``synced``.

        When using logical slots, store information separately for slot synchronisation  on replica nodes.
        """
        pass

    def ignore_replication_slot(self, cluster: Cluster, name: str) -> bool:
        """Check if slot *name* should not be managed by Patroni.

        :param cluster: cluster state information object.
        :param name: name of the slot to ignore

        :returns: ``True`` if slot *name* matches any slot specified in ``ignore_slots`` configuration,
                 otherwise will pass through and return result of :meth:`AbstractMPPHandler.ignore_replication_slot`.
        """
        pass

    def drop_replication_slot(self, name: str) -> Tuple[bool, bool]:
        """Drop a named slot from Postgres.

        :param name: name of the slot to be dropped.

        :returns: a tuple of ``active`` and ``dropped``. ``active`` is ``True`` if the slot is active,
                  ``dropped`` is ``True`` if the slot was successfully dropped. If the slot was not found return
                  ``False`` for both.
        """
        pass

    def _drop_replication_slot(self, name: str) -> None:
        """Drop replication slot by name.

        .. note::
            If not able to drop the slot, it will log a message and set the flag to reload slots.

        :param name: name of the slot to be dropped.
        """
        pass

    def _drop_incorrect_failover_synced_slots(self) -> None:
        """Drop logical failover slots with synced=false on standby.

        Try to workaround nasty bug in Postgres when it refuses to sync logical
        failover slots after switchover on the former primary node.
        """
        pass

    def _drop_incorrect_slots(self, cluster: Cluster, slots: Dict[str, Any]) -> None:
        """Compare required slots and configured as permanent slots with those found, dropping extraneous ones.

        .. note::
            Slots that are not contained in *slots* will be dropped.
            Slots can be filtered out with ``ignore_slots`` configuration.

            Slots that have matching names but do not match attributes in *slots* will also be dropped.

        :param cluster: cluster state information object.
        :param slots: dictionary of desired slot names as keys with slot attributes as a dictionary value, if known.
        """
        pass

    def _ensure_physical_slots(self, slots: Dict[str, Any], clean_inactive_physical_slots: bool) -> None:
        """Create or advance physical replication *slots*.

        Any failures are logged and do not interrupt creation of all *slots*.

        :param slots: A dictionary mapping slot name to slot attributes. This method only considers a slot
                      if the value is a dictionary with the key ``type`` and a value of ``physical``.
        :param clean_inactive_physical_slots: whether replication slots with ``xmin`` and not expected
                                              to be active should be dropped.
        """
        pass

    @contextmanager
    def get_local_connection_cursor(self, **kwargs: Any) -> Iterator[Union['cursor', 'Cursor[Any]']]:
        """Create a new database connection to local server.

        Create a non-blocking connection cursor to avoid the situation where an execution of the query of
        ``pg_replication_slot_advance`` takes longer than the timeout on a HA loop, which could cause a false
        failure state.

        :param kwargs: Any keyword arguments to pass to :func:`psycopg.connect`.

        :yields: connection cursor object, note implementation varies depending on version of :mod:`psycopg`.
        """
        pass

    def _ensure_logical_slots_primary(self, slots: Dict[str, Any]) -> None:
        """Create any missing logical replication *slots* on the primary.

        If the logical slot already exists, copy state information into the replication slots structure stored in the
        class instance.

        :param slots: Slots that should exist are supplied in a dictionary, mapping slot name to any attributes.
                      The method will only consider slots that have a value that is a dictionary with a key ``type``
                      with a value that is ``logical``.

        """
        pass

    def _ensure_logical_slots_replica(self, slots: Dict[str, Any]) -> List[str]:
        """Update logical *slots* on replicas.

        If the logical slot already exists, copy state information into the replication slots structure stored in the
        class instance. Slots that exist are also advanced if their ``confirmed_flush_lsn`` is smaller than the state
        of the slot stored in DCS or at least the ``replay_lsn`` of this replica.

        As logical slots can only be created when the primary is available, pass the list of slots that need to be
        copied back to the caller. They will be created on replicas with :meth:`SlotsHandler.copy_logical_slots`.

        :param slots: A dictionary mapping slot name to slot attributes. This method only considers a slot
                      if the value is a dictionary with the key ``type`` and a value of ``logical``.

        :returns: list of slots to be copied from the primary.
        """
        pass

    def sync_replication_slots(self, cluster: Cluster, tags: Tags) -> List[str]:
        """During the HA loop read, check and alter replication slots found in the cluster.

        Read physical and logical slots from ``pg_replication_slots``, then compare to those configured in the DCS.
        Drop any slots that do not match those required by configuration and are not configured as permanent.
        Create any missing physical slots, or advance their position according to feedback stored in DCS.
        If we are the primary then create logical slots, otherwise if logical slots are known and active create
        them on replica nodes by copying slot files from the primary.

        :param cluster: object containing stateful information for the cluster.
        :param tags: reference to an object implementing :class:`Tags` interface.

        :returns: list of logical replication slots names that should be copied from the primary.
        """
        pass

    @contextmanager
    def _get_leader_connection_cursor(self, leader: Leader) -> Iterator[Union['cursor', 'Cursor[Any]']]:
        """Create a new database connection to the leader.

        .. note::
            Uses rewind user credentials because it has enough permissions to read files from PGDATA.
            Sets the options ``connect_timeout`` to ``3`` and ``statement_timeout`` to ``2000``.

        :param leader: object with information on the leader

        :yields: connection cursor object, note implementation varies depending on version of ``psycopg``.
        """
        pass

    def check_logical_slots_readiness(self, cluster: Cluster, tags: Tags) -> bool:
        """Determine whether all known logical slots are synchronised from the leader.

        1) Retrieve the current ``catalog_xmin`` value for the physical slot from the cluster leader, and
        2) using previously stored list of "unready" logical slots, those which have yet to be checked hence have no
           stored slot attributes,
        3) store logical slot ``catalog_xmin`` when the physical slot ``catalog_xmin`` becomes valid.

        :param cluster: object containing stateful information for the cluster.
        :param tags: reference to an object implementing :class:`Tags` interface.

        :returns: ``False`` if any issue while checking logical slots readiness, ``True`` otherwise.
        """
        pass

    def _update_pending_logical_slot_primary(self, slots: Dict[str, Any], catalog_xmin: Optional[int] = None) -> bool:
        """Store pending logical slot information for ``catalog_xmin`` on the primary.

        Remember ``catalog_xmin`` of logical slots on the primary when ``catalog_xmin`` of the physical slot became
        valid. Logical slots on replica will be safe to use after promote when ``catalog_xmin`` of the physical slot
        overtakes these values.

        :param slots: dictionary of slot information from the primary
        :param catalog_xmin: ``catalog_xmin`` of the physical slot used by this replica to stream changes from primary.

        :returns: ``False`` if any issue was faced while processing, ``True`` otherwise.
        """
        pass

    def _ready_logical_slots(self, primary_physical_catalog_xmin: Optional[int] = None) -> None:
        """Ready logical slots by comparing primary physical slot ``catalog_xmin`` to logical ``catalog_xmin``.

        The logical slot on a replica is safe to use when the physical replica slot on the primary:

            1. has a nonzero/non-null ``catalog_xmin`` represented by ``primary_physical_xmin``.
            2. has a ``catalog_xmin`` that is not newer (greater) than the ``catalog_xmin`` of any slot on the standby
            3. overtook the ``catalog_xmin`` of remembered values of logical slots on the primary.

        :param primary_physical_catalog_xmin: is the value retrieved from ``pg_catalog.pg_get_replication_slots()`` for
                                              the physical replication slot on the primary.
        """
        pass

    def copy_logical_slots(self, cluster: Cluster, tags: Tags, create_slots: List[str]) -> None:
        """Create logical replication slots on standby nodes.

        :param cluster: object containing stateful information for the cluster.
        :param tags: reference to an object implementing :class:`Tags` interface.
        :param create_slots: list of slot names to copy from the primary.
        """
        pass

    def schedule(self, value: Optional[bool] = None) -> None:
        """Schedule the loading of slot information from the database.

        :param value: the optional value can be used to unschedule if set to ``False`` or force it to be ``True``.
                      If it is omitted the value will be ``True`` if this PostgreSQL node supports slot replication.
        """
        pass

    def on_promote(self) -> None:
        """Entry point from HA cycle used when a standby node is to be promoted to primary.

        .. note::
            If logical replication slot synchronisation is enabled then slot advancement will be triggered.
            If any logical slots that were copied are yet to be confirmed as ready a warning message will be logged.

        """
        pass
