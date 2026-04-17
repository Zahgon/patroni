import logging
import multiprocessing
import os
import pathlib
import re
import signal
import subprocess
import sys

from multiprocessing.connection import Connection
from typing import Dict, List, Optional

import psutil

from patroni import KUBERNETES_ENV_PREFIX, PATRONI_ENV_PREFIX

# avoid spawning the resource tracker process
if sys.version_info >= (3, 8):  # pragma: no cover
    import multiprocessing.resource_tracker
    multiprocessing.resource_tracker.getfd = lambda: 0
elif sys.version_info >= (3, 4):  # pragma: no cover
    import multiprocessing.semaphore_tracker
    multiprocessing.semaphore_tracker.getfd = lambda: 0

logger = logging.getLogger(__name__)

STOP_SIGNALS = {
    'smart': 'TERM',
    'fast': 'INT',
    'immediate': 'QUIT',
}


def pg_ctl_start(conn: Connection, cmdline: List[str], env: Dict[str, str]) -> None:
    pass


class PostmasterProcess(psutil.Process):

    def __init__(self, pid: int) -> None:
        self._postmaster_pid: Dict[str, str]
        self.is_single_user = False
        if pid < 0:
            pid = -pid
            self.is_single_user = True
        super(PostmasterProcess, self).__init__(pid)

    @staticmethod
    def _read_postmaster_pidfile(data_dir: str) -> Dict[str, str]:
        """Reads and parses postmaster.pid from the data directory

        :returns dictionary of values if successful, empty dictionary otherwise
        """
        pass

    def _is_postmaster_process(self, pgcommand: str, data_dir: str) -> bool:
        """Determine whether this process is the postmaster.

        This method applies several heuristics to decide if the PID read from ``postmaster.pid``
        corresponds to the PostgreSQL postmaster:

            * Excludes the Patroni process itself, its parent, and its direct children.
            * Compares the process start time with the value stored in ``postmaster.pid``,
              treating a small time difference as a positive match.
            * Checks that the executable name matches the expected binary derived from
              ``pgcommand`` (``postgres`` by default) or ``postmaster``.
            * When possible, verifies that the process current working directory matches the given data directory.

        :param pgcommand: name of the postgres/postmaster executable that should be running.
        :param data_dir: PostgreSQL data directory that contains.
        :returns: ``True`` if the process is likely the correct postmaster, ``False`` otherwise.
        """
        pass

    @classmethod
    def _from_pidfile(cls, data_dir: str) -> Optional['PostmasterProcess']:
        pass

    @staticmethod
    def from_pidfile(pgcommand: str, data_dir: str) -> Optional['PostmasterProcess']:
        pass

    @classmethod
    def from_pid(cls, pid: int) -> Optional['PostmasterProcess']:
        pass

    def signal_kill(self) -> bool:
        """to suspend and kill postmaster and all children

        :returns True if postmaster and children are killed, False if error
        """
        pass

    def signal_stop(self, mode: str, pg_ctl: str = 'pg_ctl') -> Optional[bool]:
        """Signal postmaster process to stop

        :returns None if signaled, True if process is already gone, False if error
        """
        pass

    def pg_ctl_kill(self, mode: str, pg_ctl: str) -> Optional[bool]:
        pass

    def wait_for_user_backends_to_close(self, stop_timeout: Optional[float]) -> None:
        # These regexps are cross checked against versions PostgreSQL 9.1 .. 18
        pass

    @staticmethod
    def start(pgcommand: str, data_dir: str, conf: str, options: List[str]) -> Optional['PostmasterProcess']:
        # Unfortunately `pg_ctl start` does not return postmaster pid to us. Without this information
        # it is hard to know the current state of postgres startup, so we had to reimplement pg_ctl start
        # in python. It will start postgres, wait for port to be open and wait until postgres will start
        # accepting connections.
        # Important!!! We can't just start postgres using subprocess.Popen, because in this case it
        # will be our child for the rest of our live and we will have to take care of it (`waitpid`).
        # So we will use the same approach as pg_ctl uses: start a new process, which will start postgres.
        # This process will write postmaster pid to stdout and exit immediately. Now it's responsibility
        # of init process to take care about postmaster.
        # In order to make everything portable we can't use fork&exec approach here, so  we will call
        # ourselves and pass list of arguments which must be used to start postgres.
        # On Windows, in order to run a side-by-side assembly the specified env must include a valid SYSTEMROOT.
        pass
