import logging
import os
import re
import shlex
import shutil
import subprocess

from enum import IntEnum
from threading import Lock
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from .. import thread_pool
from ..async_executor import CriticalTask
from ..collections import EMPTY_DICT
from ..dcs import Leader, RemoteMember
from ..utils import process_user_options
from . import Postgresql
from .connection import get_connection_cursor
from .misc import format_lsn, fsync_dir, parse_history, parse_lsn, PostgresqlRole

logger = logging.getLogger(__name__)


class REWIND_STATUS(IntEnum):
    INITIAL = 0
    CHECKPOINT = 1
    CHECK = 2
    NEED = 3
    NOT_NEED = 4
    SUCCESS = 5
    FAILED = 6


class Rewind(object):

    def __init__(self, postgresql: Postgresql) -> None:
        self._postgresql = postgresql
        self._checkpoint_task_lock = Lock()
        self.reset_state()

    @staticmethod
    def configuration_allows_rewind(data: Dict[str, str]) -> bool:
        pass

    @property
    def enabled(self) -> bool:
        pass

    @property
    def can_rewind(self) -> bool:
        """ check if pg_rewind executable is there and that pg_controldata indicates
            we have either wal_log_hints or checksums turned on
        """
        pass

    @property
    def should_remove_data_directory_on_diverged_timelines(self) -> bool:
        pass

    @property
    def can_rewind_or_reinitialize_allowed(self) -> bool:
        pass

    def trigger_check_diverged_lsn(self) -> None:
        pass

    @staticmethod
    def check_leader_is_not_in_recovery(conn_kwargs: Dict[str, Any]) -> Optional[bool]:
        pass

    @staticmethod
    def check_leader_has_run_checkpoint(conn_kwargs: Dict[str, Any]) -> Optional[str]:
        pass

    def _get_checkpoint_end(self, timeline: int, lsn: int) -> int:
        """Get the end of checkpoint record from WAL.

        .. note::
            The checkpoint record size in WAL depends on postgres major version and platform (memory alignment).
            Hence, the only reliable way to figure out where it ends, is to read the record from file with the
            help of ``pg_waldump`` and parse the output.

            We are trying to read two records, and expect that it will fail to read the second record with message:

                fatal: error in WAL record at 0/182E220: invalid record length at 0/182E298: wanted 24, got 0; or

                fatal: error in WAL record at 0/182E220: invalid record length at 0/182E298: expected at least 24, got 0

            The error message contains information about LSN of the next record, which is exactly where checkpoint ends.

        :param timeline: the checkpoint *timeline* from ``pg_controldata``.
        :param lsn: the checkpoint *location* as :class:`int` from ``pg_controldata``.

        :returns: the end of checkpoint record as :class:`int` or ``0`` if failed to parse ``pg_waldump`` output.
        """
        pass

    def _get_local_timeline_lsn_from_controldata(self) -> Tuple[Optional[bool], Optional[int], Optional[int]]:
        pass

    def _get_local_timeline_lsn(self) -> Tuple[Optional[bool], Optional[int], Optional[int]]:
        pass

    @staticmethod
    def _log_primary_history(history: List[Tuple[int, int, str]], i: int) -> None:
        pass

    def _conn_kwargs(self, member: Union[Leader, RemoteMember], auth: Dict[str, Any]) -> Dict[str, Any]:
        pass

    def _check_timeline_and_lsn(self, leader: Union[Leader, RemoteMember]) -> None:
        pass

    def rewind_or_reinitialize_needed_and_possible(self, leader: Union[Leader, RemoteMember, None]) -> bool:
        pass

    def __checkpoint(self, task: CriticalTask, wakeup: Callable[..., Any]) -> None:
        pass

    def ensure_checkpoint_after_promote(self, wakeup: Callable[..., Any]) -> None:
        """After promote issue a CHECKPOINT from a new thread and asynchronously check the result.
        In case if CHECKPOINT failed, just check that timeline in pg_control was updated."""
        pass

    def checkpoint_after_promote(self) -> bool:
        pass

    def get_archive_command(self) -> Optional[str]:
        """Get ``archive_command`` GUC value if defined and archiving is enabled.

        :returns: ``archive_command`` defined in the Postgres configuration or None.
        """
        pass

    def _build_archiver_command(self, command: str, wal_filename: str) -> str:
        """Replace placeholders in the given archiver command's template.
        Applicable for archive_command and restore_command.
        Can also be used for archive_cleanup_command and recovery_end_command,
        however %r value is always set to 000000010000000000000001."""
        pass

    def _fetch_missing_wal(self, restore_command: str, wal_filename: str) -> bool:
        pass

    def _find_missing_wal(self, data: bytes) -> Optional[str]:
        # could not open file "$PGDATA/pg_wal/0000000A00006AA100000068": No such file or directory
        pass

    def _archive_ready_wals(self) -> None:
        """Try to archive WALs that have .ready files just in case
        archive_mode was not set to 'always' before promote, while
        after it the WALs were recycled on the promoted replica.
        With this we prevent the entire loss of such WALs and the
        consequent old leader's start failure."""
        pass

    def _maybe_clean_pg_replslot(self) -> None:
        """Clean pg_replslot directory if pg version is less then 11
        (pg_rewind deletes $PGDATA/pg_replslot content only since pg11)."""
        pass

    def pg_rewind(self, conn_kwargs: Dict[str, Any]) -> bool:
        """Do pg_rewind.

        .. note::
            If ``pg_rewind`` doesn't support ``--restore-target-wal`` parameter and exited with non zero code,
            Patroni will parse stderr/stdout to figure out if it failed due to a missing WAL file and will
            repeat an attempt after downloading the missing file using ``restore_command``.

        :param conn_kwargs: :class:`dict` object with connection parameters.

        :returns: ``True`` if ``pg_rewind`` finished successfully, ``False`` otherwise.
        """
        pass

    def execute(self, leader: Union[Leader, RemoteMember]) -> Optional[bool]:
        pass

    def reset_state(self) -> None:
        pass

    @property
    def is_needed(self) -> bool:
        pass

    @property
    def executed(self) -> bool:
        pass

    @property
    def failed(self) -> bool:
        pass

    def read_postmaster_opts(self) -> Dict[str, str]:
        """returns the list of option names/values from postgres.opts, Empty dict if read failed or no file"""
        pass

    def single_user_mode(self, communicate: Optional[Dict[str, Any]] = None,
                         options: Optional[Dict[str, str]] = None) -> Optional[int]:
        """run a given command in a single-user mode. If the command is empty - then just start and stop"""
        pass

    def cleanup_archive_status(self) -> None:
        pass

    def ensure_clean_shutdown(self) -> Optional[bool]:
        pass

    def archive_shutdown_checkpoint_wal(self, archive_cmd: str) -> None:
        """Archive WAL file with the shutdown checkpoint.

        :param archive_cmd: archiver command to use
        """
        pass
