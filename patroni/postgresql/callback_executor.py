import logging
import sys

from enum import Enum
from threading import Condition, Thread
from typing import Any, Dict, List

from .. import thread_pool
from .cancellable import CancellableExecutor, CancellableSubprocess

logger = logging.getLogger(__name__)


class CallbackAction(str, Enum):
    NOOP = "noop"
    ON_START = "on_start"
    ON_STOP = "on_stop"
    ON_RESTART = "on_restart"
    ON_RELOAD = "on_reload"
    ON_ROLE_CHANGE = "on_role_change"

    def __repr__(self) -> str:
        return self.value


class OnReloadExecutor(CancellableSubprocess):

    def call_nowait(self, cmd: List[str]) -> None:
        """Run one `on_reload` callback at most.

        To achieve it we always kill already running command including child processes."""
        pass


class CallbackExecutor(CancellableExecutor, Thread):

    def __init__(self):
        CancellableExecutor.__init__(self)
        Thread.__init__(self)
        self.daemon = True
        self._on_reload_executor = OnReloadExecutor()
        self._cmd = None
        self._condition = Condition()
        self.start()

    def call(self, cmd: List[str]) -> None:
        """Executes one callback at a time.

        Already running command is killed (including child processes).
        If it couldn't be killed we wait until it finishes.

        :param cmd: command to be executed"""
        pass

    def run(self) -> None:
        pass
