import concurrent.futures
import datetime
import functools
import json
import logging
import sys
import time
import uuid

from threading import RLock
from typing import Any, Callable, cast, Collection, Dict, List, NamedTuple, Optional, Tuple, TYPE_CHECKING, Union

from . import global_config, psycopg, thread_pool
from .__main__ import Patroni
from .async_executor import AsyncExecutor, CriticalTask
from .collections import CaseInsensitiveSet
from .dcs import AbstractDCS, Cluster, Leader, Member, RemoteMember, Status, SyncState
from .exceptions import DCSError, PatroniFatalException, PostgresConnectionException
from .postgresql.callback_executor import CallbackAction
from .postgresql.misc import postgres_version_to_int, PostgresqlRole, PostgresqlState
from .postgresql.postmaster import PostmasterProcess
from .postgresql.rewind import Rewind
from .quorum import QuorumStateResolver
from .tags import Tags
from .utils import parse_int, polling_loop, tzutc

logger = logging.getLogger(__name__)


class _MemberStatus(Tags, NamedTuple('_MemberStatus',
                                     [('member', Member),
                                      ('reachable', bool),
                                      ('in_recovery', Optional[bool]),
                                      ('wal_position', int),
                                      ('data', Dict[str, Any])])):
    """Node status distilled from API response.

    Consists of the following fields:

    :ivar member: :class:`~patroni.dcs.Member` object of the node.
    :ivar reachable: ``False`` if the node is not reachable or is not responding with correct JSON.
    :ivar in_recovery: ``False`` if the node is running as a primary (`if pg_is_in_recovery() == true`).
    :ivar wal_position: maximum value of ``replayed_location`` or ``received_location`` from JSON.
    :ivar data: the whole JSON response for future usage.
    """

    @classmethod
    def from_api_response(cls, member: Member, json: Dict[str, Any]) -> '_MemberStatus':
        """
        :param member: dcs.Member object
        :param json: RestApiHandler.get_postgresql_status() result
        :returns: _MemberStatus object
        """
        pass

    @property
    def tags(self) -> Dict[str, Any]:
        """Dictionary with values of different tags (i.e. nofailover)."""
        pass

    @property
    def timeline(self) -> int:
        """Timeline value from JSON."""
        pass

    @property
    def watchdog_failed(self) -> bool:
        """Indicates that watchdog is required by configuration but not available or failed."""
        pass

    @classmethod
    def unknown(cls, member: Member) -> '_MemberStatus':
        """Create a new class instance with empty or null values."""
        pass

    def failover_limitation(self) -> Optional[str]:
        """Returns reason why this node can't promote or None if everything is ok."""
        pass


class _FailsafeResponse(NamedTuple):
    """Response on POST ``/failsafe`` API request.

    Consists of the following fields:

    :ivar member_name: member name.
    :ivar accepted: ``True`` if the member agrees that the current primary will continue running, ``False`` otherwise.
    :ivar lsn: absolute position of received/replayed location in bytes.
    """

    member_name: str
    accepted: bool
    lsn: Optional[int]


class Failsafe(object):
    """Object that represents failsafe state of the cluster."""

    def __init__(self, dcs: AbstractDCS) -> None:
        """Initialize the :class:`Failsafe` object.

        :param dcs: current DCS object, is used only to get current value of ``ttl``.
        """
        self._lock = RLock()
        self._dcs = dcs
        self._reset_state()

    def update_slots(self, slots: Dict[str, int]) -> None:
        """Assign value to :attr:`_slots`.

        .. note:: This method is only called on the primary node.

        :param slots: a :class:`dict` object with member names as keys and received/replayed LSNs as values.
        """
        pass

    def update(self, data: Dict[str, Any]) -> None:
        """Update the :class:`Failsafe` object state.

        The last update time is stored and object will be invalidated after ``ttl`` seconds.

        .. note::
            This method is only called as a result of `POST /failsafe` REST API call.

        :param data: deserialized JSON document from REST API call that contains information about current leader.
        """
        pass

    def _reset_state(self) -> None:
        """Reset state of the :class:`Failsafe` object."""
        pass

    @property
    def leader(self) -> Optional[Leader]:
        """Return information about current cluster leader if the failsafe mode is active."""
        pass

    def update_cluster(self, cluster: Cluster) -> Cluster:
        """Update and return provided :class:`Cluster` object with fresh values.

        .. note::
            This method is called when failsafe mode is active and is used to update cluster state
            with fresh values of replication ``slots`` status and ``xlog_location`` on member nodes.

        :returns: :class:`Cluster` object, either unchanged or updated.
        """
        pass

    def is_active(self) -> bool:
        """Check whether the failsafe mode is active.

        .. note:
            This method is called from the REST API to report whether the failsafe mode was activated.

            On primary the :attr:`_last_update` is updated from the :func:`set_is_active` method and always
            returns the correct value.

            On replicas the :attr:`_last_update` is updated at the moment when the primary performs
            ``POST /failsafe`` REST API calls.

            The side-effect - it is possible that replicas will show ``failsafe_is_active``
            values different from the primary.

        :returns: ``True`` if failsafe mode is active, ``False`` otherwise.
        """
        pass

    def set_is_active(self, value: float) -> None:
        """Update :attr:`_last_update` value.

        .. note::
            This method is only called on the primary.
            Effectively it sets expiration time of failsafe mode.
            If the provided value is ``0``, it disables failsafe mode.

        :param value: time of the last update.
        """
        pass


class Ha(object):

    def __init__(self, patroni: Patroni):
        self.patroni = patroni
        self.state_handler = patroni.postgresql
        self._rewind = Rewind(self.state_handler)
        self.dcs = patroni.dcs
        self.cluster = Cluster.empty()
        self.old_cluster = Cluster.empty()
        self._leader_expiry = 0
        self._leader_expiry_lock = RLock()
        self._failsafe = Failsafe(patroni.dcs)
        self._was_paused = False
        self._promote_timestamp = 0
        self._leader_timeline = None
        self.recovering = False
        self._async_response = CriticalTask()
        self._crash_recovery_started = 0
        self._start_timeout = None
        self._async_executor = AsyncExecutor(self.state_handler.cancellable, self.wakeup)
        self.watchdog = patroni.watchdog

        # Each member publishes various pieces of information to the DCS using touch_member. This lock protects
        # the state and publishing procedure to have consistent ordering and avoid publishing stale values.
        self._member_state_lock = RLock()

        # The last know value of current receive/flush/replay LSN.
        # We update this value from update_lock() and touch_member() methods, because they fetch it anyway.
        # This value is used to notify the leader when the failsafe_mode is active without performing any queries.
        self._last_wal_lsn = None
        # The last known value of current timeline on this standby node.
        # We update this value from touch_member() and _is_healthiest_node() methods, because they fetch it anyway.
        # This value is used to detect cases of timeline bump with actual leader remaining on the same node
        # and trigger pg_rewind state machine.
        self._last_timeline = None

        # receive/flush/replay LSN from last cycle, is used to detect false positives of dead primary
        self._prev_wal_lsn: Optional[int] = None
        # timestamp when primary_race_backoff was triggered
        self._primary_race_backoff_timestamp = 0

        # Count of concurrent sync disabling requests. Value above zero means that we don't want to be synchronous
        # standby. Changes protected by _member_state_lock.
        self._disable_sync = 0
        # Remember the last known member role and state written to the DCS in order to notify MPP coordinator
        self._last_state = None

        # We need following property to avoid shutdown of postgres when join of Patroni to the postgres
        # already running as replica was aborted due to cluster not being initialized in DCS.
        self._join_aborted = False

        # used only in backoff after failing a pre_promote script
        self._released_leader_key_timestamp = 0

    def primary_stop_timeout(self) -> Union[int, None]:
        """:returns: "primary_stop_timeout" from the global configuration or `None` when not in synchronous mode."""
        pass

    def is_paused(self) -> bool:
        """:returns: `True` if in maintenance mode."""
        pass

    def check_timeline(self) -> bool:
        """:returns: `True` if should check whether the timeline is latest during the leader race."""
        pass

    def is_standby_cluster(self) -> bool:
        """:returns: `True` if global configuration has a valid "standby_cluster" section."""
        pass

    def is_leader(self) -> bool:
        """:returns: `True` if the current node is the leader, based on expiration set when it last held the key."""
        pass

    def set_is_leader(self, value: bool) -> None:
        """Update the current node's view of it's own leadership status.

        Will update the expiry timestamp to match the dcs ttl if setting leadership to true,
        otherwise will set the expiry to the past to immediately invalidate.

        :param value: is the current node the leader.
        """
        pass

    def sync_mode_is_active(self) -> bool:
        """Check whether synchronous replication is requested and already active.

        :returns: ``True`` if the primary already put its name into the ``/sync`` in DCS.
        """
        pass

    def quorum_commit_mode_is_active(self) -> bool:
        """Checks whether quorum replication is requested and already active.

        :returns: ``True`` if the primary already put its name into the ``/sync`` in DCS.
        """
        pass

    def _get_failover_action_name(self) -> str:
        """Return the currently requested manual failover action name or the default ``failover``.

        :returns: :class:`str` representing the manually requested action (``manual failover`` if no leader
            is specified in the ``/failover`` in DCS, ``switchover`` otherwise) or ``failover`` if
            ``/failover`` is empty.
        """
        pass

    def load_cluster_from_dcs(self) -> None:
        pass

    def acquire_lock(self) -> bool:
        pass

    def _failsafe_config(self) -> Optional[Dict[str, str]]:
        pass

    def update_lock(self, update_status: bool = False) -> bool:
        """Update the leader lock in DCS.

        .. note::
            After successful update of the leader key the :meth:`AbstractDCS.update_leader` method could also
            optionally update the ``/status`` and ``/failsafe`` keys.

            The ``/status`` key contains the last known LSN on the leader node and the last known state
            of permanent replication slots including permanent physical replication slot for the leader.

            Last, but not least, this method calls a :meth:`Watchdog.keepalive` method after the leader key
            was successfully updated.

        :param update_status: ``True`` if we also need to update the ``/status`` key in DCS, otherwise ``False``.

        :returns: ``True`` if the leader key was successfully updated and we can continue to run postgres
                  as a ``primary`` or as a ``standby_leader``, otherwise ``False``.
        """
        pass

    def has_lock(self, info: bool = True) -> bool:
        pass

    def get_effective_tags(self) -> Dict[str, Any]:
        """Return configuration tags merged with dynamically applied tags."""
        pass

    def notify_mpp_coordinator(self, event: str) -> None:
        """Send an event to the MPP coordinator.

        :param event: the type of event for coordinator to parse.
        """
        pass

    def touch_member(self) -> bool:
        pass

    def clone(self, clone_member: Union[Leader, Member, None] = None, msg: str = '(without leader)',
              clone_from_leader: bool = False) -> Optional[bool]:
        pass

    def bootstrap(self) -> str:
        # no initialize key and node is allowed to be primary and has 'bootstrap' section in a configuration file
        pass

    def bootstrap_standby_leader(self) -> Optional[bool]:
        """ If we found 'standby' key in the configuration, we need to bootstrap
            not a real primary, but a 'standby leader', that will take base backup
            from a remote member and start follow it.
        """
        pass

    def _handle_crash_recovery(self) -> Optional[str]:
        pass

    def _handle_rewind_or_reinitialize(self) -> Optional[str]:
        pass

    def recover(self) -> str:
        """Handle the case when postgres isn't running.

        Depending on the state of Patroni, DCS cluster view, and pg_controldata the following could happen:

          - if ``primary_start_timeout`` is 0 and this node owns the leader lock, the lock
            will be voluntarily released if there are healthy replicas to take it over.

          - if postgres was running as a ``primary`` and this node owns the leader lock, postgres is started as primary.

          - crash recover in a single-user mode is executed in the following cases:

            - postgres was running as ``primary`` wasn't ``shut down`` cleanly and there is no leader in DCS

            - postgres was running as ``replica`` wasn't ``shut down in recovery`` (cleanly)
              and we need to run ``pg_rewind`` to join back to the cluster.

          - ``pg_rewind`` is executed if it is necessary, or optionally, the data directory could
             be removed if it is allowed by configuration.

          - after ``crash recovery`` and/or ``pg_rewind`` are executed, postgres is started in recovery.

        :returns: action message, describing what was performed.
        """
        pass

    def _get_node_to_follow(self, cluster: Cluster) -> Union[Leader, Member, None]:
        """Determine the node to follow.

        :param cluster: the currently known cluster state from DCS.

        :returns: the node which we should be replicating from.
        """
        pass

    def follow(self, demote_reason: str, follow_reason: str, refresh: bool = True) -> str:
        pass

    def is_synchronous_mode(self) -> bool:
        """:returns: `True` if synchronous replication is requested."""
        pass

    def is_quorum_commit_mode(self) -> bool:
        """``True`` if quorum commit replication is requested and "supported"."""
        pass

    def is_failsafe_mode(self) -> bool:
        """:returns: `True` if failsafe_mode is enabled in global configuration."""
        pass

    def _maybe_enable_synchronous_mode(self) -> Optional[SyncState]:
        """Explicitly enable synchronous mode if not yet enabled.

        We are trying to solve a corner case: synchronous mode needs to be explicitly enabled
        by updating the ``/sync`` key with the current leader name and empty members. In opposite
        case it will never be automatically enabled if there are no eligible candidates.

        :returns: the latest version of :class:`~patroni.dcs.SyncState` object.
        """
        pass

    def disable_synchronous_replication(self) -> None:
        """Cleans up ``/sync`` key in DCS and updates ``synchronous_standby_names``.

        .. note::
            We fall back to using the value configured by the user for ``synchronous_standby_names``, if any.
        """
        pass

    def _process_quorum_replication(self) -> None:
        """Process synchronous replication state when quorum commit is requested.

        Synchronous standbys are registered in two places: ``postgresql.conf`` and DCS. The order of updating them must
        keep the invariant that ``quorum + sync >= len(set(quorum pool)|set(sync pool))``. This is done using
        :class:`QuorumStateResolver` that given a current state and set of desired synchronous nodes and replication
        level outputs changes to DCS and synchronous replication in correct order to reach the desired state.
        In case any of those steps causes an error we can just bail out and let next iteration rediscover the state
        and retry necessary transitions.
        """
        pass

    def _process_multisync_replication(self) -> None:
        """Process synchronous replication state with one or more sync standbys.

        Synchronous standbys are registered in two places postgresql.conf and DCS. The order of updating them must
        be right. The invariant that should be kept is that if a node is primary and sync_standby is set in DCS,
        then that node must have synchronous_standby set to that value. Or more simple, first set in postgresql.conf
        and then in DCS. When removing, first remove in DCS, then in postgresql.conf. This is so we only consider
        promoting standbys that were guaranteed to be replicating synchronously.
        """
        pass

    def process_sync_replication(self) -> None:
        """Process synchronous replication behavior on the primary."""
        pass

    def process_sync_replication_prepromote(self) -> bool:
        """Handle sync replication state before promote.

        If quorum replication is requested, and we can keep syncing to enough nodes satisfying the quorum invariant
        we can promote immediately and let normal quorum resolver process handle any membership changes later.
        Otherwise, we will just reset DCS state to ourselves and add replicas as they connect.

        :returns: ``True`` if on success or ``False`` if failed to update /sync key in DCS.
        """
        pass

    def is_sync_standby(self, cluster: Cluster) -> bool:
        """:returns: `True` if the current node is a synchronous standby."""
        pass

    def while_not_sync_standby(self, func: Callable[..., Any]) -> Any:
        """Runs specified action while trying to make sure that the node is not assigned synchronous standby status.

        When running in ``synchronous_mode`` with ``synchronous_node_count = 2``, shutdown or restart of a
        synchronous standby may cause a write downtime. Therefore we need to signal a primary that we don't want
        to by synchronous anymore and wait until it will replace our name from ``synchronous_standby_names``
        and ``/sync`` key in DCS with some other node. Once current node is not synchronous we will run the *func*.

        .. note::
            If the connection to DCS fails we run the *func* anyway, as this is only a hint.

            There is a small race window where this function runs between a primary picking us the sync standby
            and publishing it to the DCS. As the window is rather tiny consequences are holding up commits for
            one cycle period we don't worry about it here.

        :param func: the function to be executed.

        :returns: a return value of the *func*.
        """
        pass

    def update_cluster_history(self) -> None:
        pass

    def enforce_follow_remote_member(self, message: str) -> str:
        pass

    def enforce_primary_role(self, message: str, promote_message: str) -> str:
        """
        Ensure the node that has won the race for the leader key meets criteria
        for promoting its PG server to the 'primary' role.
        """
        pass

    def fetch_node_status(self, member: Member) -> _MemberStatus:
        """Perform http get request on member.api_url to fetch its status.

        Usually this happens during the leader race and we can't afford to wait an indefinite time
        for a response, therefore the request timeout is hardcoded to 2 seconds, which seems to be a
        good compromise. The node which is slow to respond is most likely unhealthy.

        :returns: :class:`_MemberStatus` object
        """
        pass

    def fetch_nodes_statuses(self, members: List[Member]) -> List[_MemberStatus]:
        pass

    def update_failsafe(self, data: Dict[str, Any]) -> Union[int, str, None]:
        """Update failsafe state.

        :param data: deserialized JSON document from REST API call that contains information about current leader.

        :returns: the reason why caller shouldn't continue as a primary or the current value of received/replayed LSN.
        """
        pass

    def failsafe_is_active(self) -> bool:
        pass

    def call_failsafe_member(self, data: Dict[str, Any], member: Member) -> _FailsafeResponse:
        """Call ``POST /failsafe`` REST API request on provided member.

        :param data: data to be send in the POST request.

        :returns: a :class:`_FailsafeResponse` object.
        """
        pass

    def check_failsafe_topology(self) -> bool:
        """Check whether we could continue to run as a primary by calling all members from the failsafe topology.

        .. note::
            If the ``/failsafe`` key contains invalid data or if the ``name`` of our node is missing in
            the ``/failsafe`` key, we immediately give up and return ``False``.

            We send the JSON document in the POST request with the following fields:

            * ``name`` - the name of our node;
            * ``conn_url`` - connection URL to the postgres, which is reachable from other nodes;
            * ``api_url`` - connection URL to Patroni REST API on this node reachable from other nodes;
            * ``slots`` - a :class:`dict` with replication slots that exist on the leader node, including the primary
              itself with the last known LSN, because there could be a permanent physical slot on standby nodes.

            Standby nodes are using information from the ``slots`` dict to advance position of permanent
            replication slots while DCS is not accessible in order to avoid indefinite growth of ``pg_wal``.

            Standby nodes are returning their received/replayed location in the ``lsn`` header, which later are
            used by the primary to advance position of replication slots that for nodes that are doing cascading
            replication from other nodes. It is required to avoid indefinite growth of ``pg_wal``.

        :returns: ``True`` if all members from the ``/failsafe`` topology agree that this node could continue to
                  run as a ``primary``, or ``False`` if some of standby nodes are not accessible or don't agree.
        """
        pass

    def is_lagging(self, wal_position: int) -> bool:
        """Check if node should consider itself unhealthy to be promoted due to replication lag.

        :param wal_position: Current wal position.

        :returns: ``True`` when node is lagging
        """
        pass

    def _is_healthiest_node(self, members: Collection[Member],
                            check_replication_lag: bool = True,
                            leader: Optional[Leader] = None) -> bool:
        """Determine whether the current node is healthy enough to become a new leader candidate.

        :param members: the list of nodes to check against
        :param check_replication_lag: whether to take the replication lag into account.
                                      If the lag exceeds configured threshold the node disqualifies itself.
        :param leader: the old cluster leader, it will be used to ignore its ``failover_priority`` value.
        :returns: ``True`` if the node is eligible to become the new leader. Since this method is executed
                  on multiple nodes independently it is possible that multiple nodes could count
                  themselves as the healthiest because they received/replayed up to the same LSN,
                  but this is totally fine.
        """
        pass

    def is_failover_possible(self, *, cluster_lsn: int = 0, exclude_failover_candidate: bool = False) -> bool:
        """Checks whether any of the cluster members is allowed to promote and is healthy enough for that.

        :param cluster_lsn: to calculate replication lag and exclude member if it is lagging.
        :param exclude_failover_candidate: if ``True``, exclude :attr:`failover.candidate` from the members
                                           list against which the failover possibility checks are run.
        :returns: `True` if there are members eligible to become the new leader.
        """
        pass

    def manual_failover_process_no_leader(self) -> Optional[bool]:
        """Handles manual failover/switchover when the old leader already stepped down.

        :returns: - `True` if the current node is the best candidate to become the new leader
                  - `None` if the current node is running as a primary and requested candidate doesn't exist
        """
        pass

    def is_healthiest_node(self) -> bool:
        """Performs a series of checks to determine that the current node is the best candidate.

        In case if manual failover/switchover is requested it calls :func:`manual_failover_process_no_leader` method.

        :returns: `True` if the current node is among the best candidates to become the new leader.
        """
        pass

    def _delete_leader(self, last_lsn: Optional[int] = None) -> None:
        pass

    def release_leader_key_voluntarily(self, last_lsn: Optional[int] = None) -> None:
        pass

    def demote(self, mode: str) -> Optional[bool]:
        """Demote PostgreSQL running as primary.

        :param mode: One of offline, graceful, immediate or immediate-nolock.
                     ``offline`` is used when connection to DCS is not available.
                     ``graceful`` is used when failing over to another node due to user request. May only be called
                     running async.
                     ``immediate`` is used when we determine that we are not suitable for primary and want to failover
                     quickly without regard for data durability. May only be called synchronously.
                     ``immediate-nolock`` is used when find out that we have lost the lock to be primary. Need to bring
                     down PostgreSQL as quickly as possible without regard for data durability. May only be called
                     synchronously.
        """
        pass

    def should_run_scheduled_action(self, action_name: str, scheduled_at: Optional[datetime.datetime],
                                    cleanup_fn: Callable[..., Any]) -> bool:
        pass

    def process_manual_failover_from_leader(self) -> Optional[str]:
        """Checks if manual failover is requested and takes action if appropriate.

        Cleans up failover key if failover conditions are not matched.

        :returns: action message if demote was initiated, None if no action was taken"""
        pass

    def process_unhealthy_cluster(self) -> str:
        """Cluster has no leader key"""
        pass

    def process_healthy_cluster(self) -> str:
        pass

    def evaluate_scheduled_restart(self) -> Optional[str]:
        pass

    def restart_matches(self, role: Optional[str], postgres_version: Optional[str], pending_restart: bool) -> bool:
        pass

    def schedule_future_restart(self, restart_data: Dict[str, Any]) -> bool:
        pass

    def delete_future_restart(self) -> bool:
        pass

    def future_restart_scheduled(self) -> Dict[str, Any]:
        pass

    def restart_scheduled(self) -> bool:
        pass

    def restart(self, restart_data: Dict[str, Any], run_async: bool = False) -> Tuple[bool, str]:
        """ conditional and unconditional restart """
        pass

    def _do_reinitialize(self, cluster: Cluster, from_leader: bool = False) -> Optional[bool]:
        pass

    def reinitialize(self, force: bool = False, from_leader: bool = False) -> Optional[str]:
        pass

    def handle_long_action_in_progress(self) -> str:
        """Figure out what to do with the task AsyncExecutor is performing."""
        pass

    @staticmethod
    def sysid_valid(sysid: Optional[str]) -> bool:
        # sysid does tv_sec << 32, where tv_sec is the number of seconds sine 1970,
        # so even 1 << 32 would have 10 digits.
        pass

    def post_recover(self) -> Optional[str]:
        pass

    def cancel_initialization(self) -> None:
        pass

    def post_bootstrap(self) -> str:
        pass

    def handle_starting_instance(self) -> Optional[str]:
        """Starting up PostgreSQL may take a long time. In case we are the leader we may want to fail over to."""
        pass

    def set_start_timeout(self, value: Optional[int]) -> None:
        """Sets timeout for starting as primary before eligible for failover.

        Must be called when async_executor is busy or in the main thread.
        """
        pass

    def _run_cycle(self) -> str:
        pass

    def _handle_dcs_error(self) -> str:
        pass

    def _sync_replication_slots(self, dcs_failed: bool) -> List[str]:
        """Handles replication slots.

        :param dcs_failed: bool, indicates that communication with DCS failed (get_cluster() or update_leader())

        :returns: list[str], replication slots names that should be copied from the primary
        """
        pass

    def run_cycle(self) -> str:
        pass

    def shutdown(self) -> None:
        pass

    def watch(self, timeout: float) -> bool:
        # watch on leader key changes if the postgres is running and leader is known and current node is not lock owner
        pass

    def wakeup(self) -> None:
        """Trigger the next run of HA loop if there is no "active" leader watch request in progress.

        This usually happens on the leader or if the node is running async action"""
        pass

    def get_remote_member(self, member: Union[Leader, Member, None] = None) -> RemoteMember:
        """Get remote member node to stream from.

        In case of standby cluster this will tell us from which remote member to stream. Config can be both patroni
        config or cluster.config.data.
        """
        pass

    def get_failover_candidates(self, exclude_failover_candidate: bool) -> List[Member]:
        """Return a list of candidates for either manual or automatic failover.

        Exclude non-sync members when in synchronous mode, the current node (its checks are always performed earlier)
        and the candidate if required. If failover candidate exclusion is not requested and a candidate is specified
        in the /failover key, return the candidate only.
        The result is further evaluated in the caller :func:`Ha.is_failover_possible` to check if any member is actually
        healthy enough and is allowed to poromote.

        :param exclude_failover_candidate: if ``True``, exclude :attr:`failover.candidate` from the candidates.

        :returns: a list of :class:`Member` objects or an empty list if there is no candidate available.
        """
        pass
