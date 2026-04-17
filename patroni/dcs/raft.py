import json
import logging
import os
import threading
import time

from collections import defaultdict
from typing import Any, Callable, Collection, Dict, List, Optional, Set, TYPE_CHECKING, Union

from pysyncobj import FAIL_REASON, replicated, SyncObj, SyncObjConf
from pysyncobj.dns_resolver import globalDnsResolver
from pysyncobj.node import TCPNode
from pysyncobj.transport import CONNECTION_STATE, TCPTransport
from pysyncobj.utility import TcpUtility

from ..exceptions import DCSError
from ..postgresql.mpp import AbstractMPP
from ..utils import validate_directory
from . import AbstractDCS, Cluster, ClusterConfig, Failover, Leader, Member, Status, SyncState, TimelineHistory

if TYPE_CHECKING:  # pragma: no cover
    from ..config import Config

logger = logging.getLogger(__name__)


class RaftError(DCSError):
    pass


class _TCPTransport(TCPTransport):

    def __init__(self, syncObj: 'DynMemberSyncObj', selfNode: Optional[TCPNode],
                 otherNodes: Collection[TCPNode]) -> None:
        super(_TCPTransport, self).__init__(syncObj, selfNode, otherNodes)
        self.setOnUtilityMessageCallback('members', syncObj.getMembers)

    def _connectIfNecessarySingle(self, node: TCPNode) -> bool:
        pass


def resolve_host(self: TCPNode) -> Optional[str]:
    pass


setattr(TCPNode, 'ip', property(resolve_host))


class SyncObjUtility(object):

    def __init__(self, otherNodes: Collection[Union[str, TCPNode]], conf: SyncObjConf, retry_timeout: int = 10) -> None:
        self._nodes = otherNodes
        self._utility = TcpUtility(conf.password, retry_timeout / max(1, len(otherNodes)))
        self.__node = next(iter(otherNodes), None)

    def executeCommand(self, command: List[Any]) -> Any:
        pass

    def getMembers(self) -> Optional[List[str]]:
        pass


class DynMemberSyncObj(SyncObj):

    def __init__(self, selfAddress: Optional[str], partnerAddrs: Collection[str],
                 conf: SyncObjConf, retry_timeout: int = 10) -> None:
        self.__early_apply_local_log = selfAddress is not None
        self.applied_local_log = False

        utility = SyncObjUtility(partnerAddrs, conf, retry_timeout)
        members = utility.getMembers()
        add_self = members and selfAddress not in members

        partnerAddrs = [member for member in (members or partnerAddrs) if member != selfAddress]

        super(DynMemberSyncObj, self).__init__(selfAddress, partnerAddrs, conf, transportClass=_TCPTransport)

        if add_self:
            thread = threading.Thread(target=utility.executeCommand, args=(['add', selfAddress],))
            thread.daemon = True
            thread.start()

    def getMembers(self, args: Any, callback: Callable[[Any, Any], Any]) -> None:
        pass

    def _onTick(self, timeToWait: float = 0.0):
        pass


class KVStoreTTL(DynMemberSyncObj):

    def __init__(self, on_ready: Optional[Callable[..., Any]], on_set: Optional[Callable[[str, Dict[str, Any]], None]],
                 on_delete: Optional[Callable[[str], None]], **config: Any) -> None:
        self.__thread = None
        self.__on_set = on_set
        self.__on_delete = on_delete
        self.__limb: Dict[str, Dict[str, Any]] = {}
        self.set_retry_timeout(int(config.get('retry_timeout') or 10))

        self_addr = config.get('self_addr')
        partner_addrs: Set[str] = set(config.get('partner_addrs', []))
        if config.get('patronictl'):
            if self_addr:
                partner_addrs.add(self_addr)
            self_addr = None

        # Create raft data_dir if necessary
        raft_data_dir = config.get('data_dir', '')
        if raft_data_dir != '':
            validate_directory(raft_data_dir)

        file_template = (self_addr or '')
        file_template = file_template.replace(':', '_') if os.name == 'nt' else file_template
        file_template = os.path.join(raft_data_dir, file_template)
        conf = SyncObjConf(password=config.get('password'), autoTick=False, appendEntriesUseBatch=False,
                           bindAddress=config.get('bind_addr'), dnsFailCacheTime=(config.get('loop_wait') or 10),
                           dnsCacheTime=(config.get('ttl') or 30), commandsWaitLeader=config.get('commandsWaitLeader'),
                           fullDumpFile=(file_template + '.dump' if self_addr else None),
                           journalFile=(file_template + '.journal' if self_addr else None),
                           onReady=on_ready, dynamicMembershipChange=True)

        super(KVStoreTTL, self).__init__(self_addr, partner_addrs, conf, self.__retry_timeout)
        self.__data: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def __check_requirements(old_value: Dict[str, Any], **kwargs: Any) -> bool:
        pass

    def set_retry_timeout(self, retry_timeout: int) -> None:
        pass

    def retry(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        event = threading.Event()
        ret = {'result': None, 'error': -1}

        def callback(result: Any, error: Any) -> None:
            pass

        kwargs['callback'] = callback
        timeout = kwargs.pop('timeout', None) or self.__retry_timeout
        deadline = timeout and time.time() + timeout

        while True:
            event.clear()
            func(*args, **kwargs)
            event.wait(timeout)
            if ret['error'] == FAIL_REASON.SUCCESS:
                return ret['result']
            elif ret['error'] == FAIL_REASON.REQUEST_DENIED:
                break
            elif deadline:
                timeout = deadline - time.time()
                if timeout <= 0:
                    raise RaftError('timeout')
            time.sleep(1)
        return False

    @replicated
    def _set(self, key: str, value: Dict[str, Any], **kwargs: Any) -> Union[bool, Dict[str, Any]]:
        pass

    def set(self, key: str, value: str, ttl: Optional[int] = None,
            handle_raft_error: bool = True, **kwargs: Any) -> Union[bool, Dict[str, Any]]:
        pass

    def __pop(self, key: str) -> None:
        pass

    @replicated
    def _delete(self, key: str, recursive: bool = False, **kwargs: Any) -> bool:
        pass

    def delete(self, key: str, recursive: bool = False, **kwargs: Any) -> bool:
        pass

    @staticmethod
    def __values_match(old: Dict[str, Any], new: Dict[str, Any]) -> bool:
        pass

    @replicated
    def _expire(self, key: str, value: Dict[str, Any], callback: Optional[Callable[..., Any]] = None) -> None:
        pass

    def __expire_keys(self) -> None:
        pass

    def get(self, key: str, recursive: bool = False) -> Optional[Dict[str, Any]]:
        if not recursive:
            return self.__data.get(key)
        return {k: v for k, v in self.__data.items() if k.startswith(key)}

    def _onTick(self, timeToWait: float = 0.0) -> None:
        pass

    def _autoTickThread(self) -> None:
        pass

    def startAutoTick(self) -> None:
        pass

    def destroy(self) -> None:
        pass


class Raft(AbstractDCS):

    def __init__(self, config: Dict[str, Any], mpp: AbstractMPP) -> None:
        super(Raft, self).__init__(config, mpp)
        self._ttl = int(config.get('ttl') or 30)

        ready_event = threading.Event()
        self._sync_obj = KVStoreTTL(ready_event.set, self._on_set, self._on_delete, commandsWaitLeader=False, **config)
        self._sync_obj.startAutoTick()

        while True:
            ready_event.wait(5)
            if ready_event.is_set() or self._sync_obj.applied_local_log:
                break
            else:
                logger.info('waiting on raft')

    def _on_set(self, key: str, value: Dict[str, Any]) -> None:
        pass

    def _on_delete(self, key: str) -> None:
        pass

    def set_ttl(self, ttl: int) -> Optional[bool]:
        pass

    @property
    def ttl(self) -> int:
        pass

    def set_retry_timeout(self, retry_timeout: int) -> None:
        pass

    def reload_config(self, config: Union['Config', Dict[str, Any]]) -> None:
        pass

    @staticmethod
    def member(key: str, value: Dict[str, Any]) -> Member:
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

    def _write_leader_optime(self, last_lsn: str) -> bool:
        pass

    def _write_status(self, value: str) -> bool:
        pass

    def _write_failsafe(self, value: str) -> bool:
        pass

    def _update_leader(self, leader: Leader) -> bool:
        pass

    def attempt_to_acquire_leader(self) -> bool:
        pass

    def set_failover_value(self, value: str, version: Optional[int] = None) -> bool:
        pass

    def set_config_value(self, value: str, version: Optional[int] = None) -> bool:
        pass

    def touch_member(self, data: Dict[str, Any]) -> bool:
        pass

    def take_leader(self) -> bool:
        pass

    def initialize(self, create_new: bool = True, sysid: str = '') -> bool:
        pass

    def _delete_leader(self, leader: Leader) -> bool:
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
