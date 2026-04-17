import logging
import subprocess

from threading import Lock
from typing import Any, Dict, List, Optional

import psutil

from patroni.exceptions import PostgresException
from patroni.utils import polling_loop

logger = logging.getLogger(__name__)


class CancellableExecutor(object):

    """
    There must be only one such process so that AsyncExecutor can easily cancel it.
    """

    def __init__(self) -> None:
        self._process = None
        self._process_cmd = None
        self._process_children: List[psutil.Process] = []
        self._lock = Lock()

    def _start_process(self, cmd: List[str], *args: Any, **kwargs: Any) -> Optional[bool]:
        """This method must be executed only when the `_lock` is acquired"""
        pass

    def _kill_process(self) -> None:
        pass

    def _kill_children(self) -> None:
        pass


class CancellableSubprocess(CancellableExecutor):

    def __init__(self) -> None:
        super(CancellableSubprocess, self).__init__()
        self._is_cancelled = False

    def call(self, *args: Any, **kwargs: Any) -> Optional[int]:
        pass

    def reset_is_cancelled(self) -> None:
        pass

    @property
    def is_cancelled(self) -> bool:
        pass

    def cancel(self, kill: bool = False) -> None:
        pass
