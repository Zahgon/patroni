"""Implement state machine to manage ``synchronous_standby_names`` GUC and ``/sync`` key in DCS."""
import logging

from typing import Collection, Iterator, NamedTuple, Optional

from .collections import CaseInsensitiveSet
from .exceptions import PatroniException

logger = logging.getLogger(__name__)


class Transition(NamedTuple):
    """Object describing transition of ``/sync`` or ``synchronous_standby_names`` to the new state.

    .. note::
        Object attributes represent the new state.

    :ivar transition_type: possible values:

        * ``sync`` - indicates that we needed to update ``synchronous_standby_names``.
        * ``quorum`` - indicates that we need to update ``/sync`` key in DCS.
        * ``restart`` - caller should stop iterating over transitions and restart :class:`QuorumStateResolver`.
    :ivar leader: the new value of the ``leader`` field in the ``/sync`` key.
    :ivar num: the new value of the synchronous nodes count in ``synchronous_standby_names`` or value of the ``quorum``
               field in the ``/sync`` key for :attr:`transition_type` values ``sync`` and ``quorum`` respectively.
    :ivar names: the new value of node names listed in ``synchronous_standby_names`` or value of ``voters``
                 field in the ``/sync`` key  for :attr:`transition_type` values ``sync`` and ``quorum`` respectively.
    """

    transition_type: str
    leader: str
    num: int
    names: CaseInsensitiveSet


class QuorumError(PatroniException):
    """Exception indicating that the quorum state is broken."""


class QuorumStateResolver:
    """Calculates a list of state transitions and yields them as :class:`Transition` named tuples.

    Synchronous replication state is set in two places:

    * PostgreSQL configuration sets how many and which nodes are needed for a commit to succeed, abbreviated as
      ``numsync`` and ``sync`` set here;
    * DCS contains information about how many and which nodes need to be interrogated to be sure to see an wal position
      containing latest confirmed commit, abbreviated as ``quorum`` and ``voters`` set.

    .. note::
        Both of above pairs have the meaning "ANY n OF set".

        The number of nodes needed for commit to succeed, ``numsync``, is also called the replication factor.

    To guarantee zero transaction loss on failover we need to keep the invariant that at all times any subset of
    nodes that can acknowledge a commit overlaps with any subset of nodes that can achieve quorum to promote a new
    leader. Given a desired replication factor and a set of nodes able to participate in sync replication there
    is one optimal state satisfying this condition. Given the node set ``active``, the optimal state is::

        sync = voters = active

        numsync = min(sync_wanted, len(active))

        quorum = len(active) - numsync

    We need to be able to produce a series of state changes that take the system to this desired state from any
    other arbitrary state given arbitrary changes is node availability, configuration and interrupted transitions.

    To keep the invariant the rule to follow is that when increasing ``numsync`` or ``quorum``, we need to perform the
    increasing operation first. When decreasing either, the decreasing operation needs to be performed later. In other
    words:

    * If a user increases ``synchronous_node_count`` configuration, first we increase ``synchronous_standby_names``
      (``numsync``), then we decrease ``quorum`` field in the ``/sync`` key;
    * If a user decreases ``synchronous_node_count`` configuration, first we increase ``quorum`` field in the ``/sync``
      key, then we decrease ``synchronous_standby_names`` (``numsync``).

    Order of adding or removing nodes from ``sync`` and ``voters`` depends on the state of
    ``synchronous_standby_names``.

    When adding new nodes::

        if ``sync`` (``synchronous_standby_names``) is empty:
            add new nodes first to ``sync`` and then to ``voters`` when ``numsync_confirmed`` > ``0``.
        else:
            add new nodes first to ``voters`` and then to ``sync``.

    When removing nodes::

        if ``sync`` (``synchronous_standby_names``) will become empty after removal:
            first remove nodes from ``voters`` and then from ``sync``.
        else:
            first remove nodes from ``sync`` and then from ``voters``.
            Make ``voters`` empty if ``numsync_confirmed`` == ``0``.

    :ivar leader: name of the leader, according to the ``/sync`` key.
    :ivar quorum: ``quorum`` value from the ``/sync`` key, the minimal number of nodes we need see
                  when doing the leader race.
    :ivar voters: ``sync_standby`` value from the ``/sync`` key, set of node names we will be
                  running the leader race against.
    :ivar numsync: the number of synchronous nodes from the ``synchronous_standby_names``.
    :ivar sync: set of node names listed in the ``synchronous_standby_names``.
    :ivar numsync_confirmed: the number of nodes that are confirmed to reach "safe" LSN after they were added to the
                  ``synchronous_standby_names``.
    :ivar active: set of node names that are replicating from the primary (according to ``pg_stat_replication``)
                  and are eligible to be listed in ``synchronous_standby_names``.
    :ivar sync_wanted: desired number of synchronous nodes (``synchronous_node_count`` from the global configuration).
    :ivar leader_wanted: the desired leader (could be different from the :attr:`leader` right after a failover).
    """

    def __init__(self, leader: str, quorum: int, voters: Collection[str],
                 numsync: int, sync: Collection[str], numsync_confirmed: int,
                 active: Collection[str], sync_wanted: int, leader_wanted: str) -> None:
        """Instantiate :class:``QuorumStateResolver`` based on input parameters.

        :param leader: name of the leader, according to the ``/sync`` key.
        :param quorum: ``quorum`` value from the ``/sync`` key, the minimal number of nodes we need see
                        when doing the leader race.
        :param voters: ``sync_standby`` value from the ``/sync`` key, set of node names we will be
                       running the leader race against.
        :param numsync: the number of synchronous nodes from the ``synchronous_standby_names``.
        :param sync: Set of node names listed in the ``synchronous_standby_names``.
        :param numsync_confirmed: the number of nodes that are confirmed to reach "safe" LSN after
                                  they were added to the ``synchronous_standby_names``.
        :param active: set of node names that are replicating from the primary (according to ``pg_stat_replication``)
                       and are eligible to be listed in ``synchronous_standby_names``.
        :param sync_wanted: desired number of synchronous nodes
                            (``synchronous_node_count`` from the global configuration).
        :param leader_wanted: the desired leader (could be different from the *leader* right after a failover).

        """
        self.leader = leader
        self.quorum = quorum
        self.voters = CaseInsensitiveSet(voters)
        self.numsync = min(numsync, len(sync))  # numsync can't be bigger than number of listed synchronous nodes.
        self.sync = CaseInsensitiveSet(sync)
        self.numsync_confirmed = numsync_confirmed
        self.active = CaseInsensitiveSet(active)
        self.sync_wanted = sync_wanted
        self.leader_wanted = leader_wanted

    def check_invariants(self) -> None:
        """Checks invariant of ``synchronous_standby_names`` and ``/sync`` key in DCS.

        .. seealso::
            Check :class:`QuorumStateResolver`'s docstring for more information.

        :raises:
            :exc:`QuorumError`: in case of broken state"""
        pass

    def quorum_update(self, quorum: int, voters: CaseInsensitiveSet, leader: Optional[str] = None,
                      adjust_quorum: Optional[bool] = True) -> Iterator[Transition]:
        """Updates :attr:`quorum`, :attr:`voters` and optionally :attr:`leader` fields.

        :param quorum: the new value for :attr:`quorum`, could be adjusted depending
                       on values of :attr:`numsync_confirmed` and *adjust_quorum*.
        :param voters: the new value for :attr:`voters`, could be adjusted if :attr:`numsync_confirmed` == ``0``.
        :param leader: the new value for :attr:`leader`, optional.
        :param adjust_quorum: if set to ``True`` the quorum requirement will be increased by the
                              difference between :attr:`numsync` and :attr:`numsync_confirmed`.

        :yields: the new state of the ``/sync`` key as a :class:`Transition` object.

        :raises:
            :exc:`QuorumError` in case of invalid data or if the invariant after transition could not be satisfied.
        """
        pass

    def sync_update(self, numsync: int, sync: CaseInsensitiveSet) -> Iterator[Transition]:
        """Updates :attr:`numsync` and :attr:`sync` fields.

        :param numsync: the new value for :attr:`numsync`.
        :param sync: the new value for :attr:`sync`:

        :yields: the new state of ``synchronous_standby_names`` as a :class:`Transition` object.

        :raises:
            :exc:`QuorumError` in case of invalid data or if invariant after transition could not be satisfied
        """
        pass

    def __iter__(self) -> Iterator[Transition]:
        """Iterate over the transitions produced by :meth:`_generate_transitions`.

        .. note::
            Merge two transitions of the same type to a single one.

            This is always safe because skipping the first transition is equivalent
            to no one observing the intermediate state.

        :yields: transitions as :class:`Transition` objects.
        """
        transitions = list(self._generate_transitions())
        for cur_transition, next_transition in zip(transitions, transitions[1:] + [None]):
            if isinstance(next_transition, Transition) \
                    and cur_transition.transition_type == next_transition.transition_type:
                continue
            yield cur_transition
            if cur_transition.transition_type == 'restart':
                break

    def __handle_non_steady_cases(self) -> Iterator[Transition]:
        """Handle cases when set of transitions produced on previous run was interrupted.

        :yields: transitions as :class:`Transition` objects.
        """
        pass

    def __remove_gone_nodes(self) -> Iterator[Transition]:
        """Remove inactive nodes from ``synchronous_standby_names`` and from ``/sync`` key.

        :yields: transitions as :class:`Transition` objects.
        """
        pass

    def __add_new_nodes(self) -> Iterator[Transition]:
        """Add new active nodes to ``synchronous_standby_names`` and to ``/sync`` key.

        :yields: transitions as :class:`Transition` objects.
        """
        pass

    def __handle_replication_factor_change(self) -> Iterator[Transition]:
        """Handle change of the replication factor (:attr:`sync_wanted`, aka ``synchronous_node_count``).

        :yields: transitions as :class:`Transition` objects.
        """
        pass

    def _generate_transitions(self) -> Iterator[Transition]:
        """Produce a set of changes to safely transition from the current state to the desired.

        :yields: transitions as :class:`Transition` objects.
        """
        pass
