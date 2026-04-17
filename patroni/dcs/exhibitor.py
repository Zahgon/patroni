import json
import logging
import random
import time

from typing import Any, Callable, cast, Dict, List, Union

from ..postgresql.mpp import AbstractMPP
from ..request import get as requests_get
from ..utils import uri
from . import Cluster
from .zookeeper import ZooKeeper

logger = logging.getLogger(__name__)


class ExhibitorEnsembleProvider(object):

    TIMEOUT = 3.1

    def __init__(self, hosts: List[str], port: int,
                 uri_path: str = '/exhibitor/v1/cluster/list', poll_interval: int = 300) -> None:
        self._exhibitor_port = port
        self._uri_path = uri_path
        self._poll_interval = poll_interval
        self._exhibitors: List[str] = hosts
        self._boot_exhibitors = hosts
        self._zookeeper_hosts = ''
        self._next_poll = None
        while not self.poll():
            logger.info('waiting on exhibitor')
            time.sleep(5)

    def poll(self) -> bool:
        pass

    def _query_exhibitors(self, exhibitors: List[str]) -> Any:
        pass

    @property
    def zookeeper_hosts(self) -> str:
        pass


class Exhibitor(ZooKeeper):

    def __init__(self, config: Dict[str, Any], mpp: AbstractMPP) -> None:
        interval = config.get('poll_interval', 300)
        self._ensemble_provider = ExhibitorEnsembleProvider(config['hosts'], config['port'], poll_interval=interval)
        super(Exhibitor, self).__init__({**config, 'hosts': self._ensemble_provider.zookeeper_hosts}, mpp)

    def _load_cluster(
            self, path: str, loader: Callable[[str], Union[Cluster, Dict[int, Cluster]]]
    ) -> Union[Cluster, Dict[int, Cluster]]:
        pass
