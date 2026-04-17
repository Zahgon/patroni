#!/usr/bin/env python

# sample script to clone new replicas using WAL-E restore
# falls back to pg_basebackup if WAL-E restore fails, or if
# WAL-E backup is too far behind
# note that pg_basebackup still expects to use restore from
# WAL-E for transaction logs

# theoretically should work with SWIFT, but not tested on it

# arguments are:
#   - cluster scope
#   - cluster role
#   - leader connection string
#   - number of retries
#   - envdir for the WALE env
# - WALE_BACKUP_THRESHOLD_MEGABYTES if WAL amount is above that - use pg_basebackup
# - WALE_BACKUP_THRESHOLD_PERCENTAGE if WAL size exceeds a certain percentage of the

# this script depends on an envdir defining the S3 bucket (or SWIFT dir),and login
# credentials per WALE Documentation.

# currently also requires that you configure the restore_command to use wal_e, example:
#       recovery_conf:
#               restore_command: envdir /etc/wal-e.d/env wal-e wal-fetch "%f" "%p" -p 1
import argparse
import csv
import logging
import os
import subprocess
import sys
import time

from enum import IntEnum
from typing import Any, List, NamedTuple, Optional, Tuple, TYPE_CHECKING

from .. import psycopg

logger = logging.getLogger(__name__)

RETRY_SLEEP_INTERVAL = 1
si_prefixes = ['K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']


# Meaningful names to the exit codes used by WALERestore
class ExitCode(IntEnum):
    SUCCESS = 0  #: Succeeded
    RETRY_LATER = 1  #: External issue, retry later
    FAIL = 2  #: Don't try again unless configuration changes


# We need to know the current PG version in order to figure out the correct WAL directory name
def get_major_version(data_dir: str) -> float:
    pass


def repr_size(n_bytes: float) -> str:
    """
    >>> repr_size(1000)
    '1000 Bytes'
    >>> repr_size(8257332324597)
    '7.5 TiB'
    """
    pass


def size_as_bytes(size: float, prefix: str) -> int:
    """
    >>> size_as_bytes(7.5, 'T')
    8246337208320
    """
    pass


class WALEConfig(NamedTuple):
    env_dir: str
    threshold_mb: int
    threshold_pct: int
    cmd: List[str]


class WALERestore(object):
    def __init__(self, scope: str, datadir: str, connstring: str, env_dir: str, threshold_mb: int,
                 threshold_pct: int, use_iam: int, no_leader: bool, retries: int) -> None:
        self.scope = scope
        self.leader_connection = connstring
        self.data_dir = datadir
        self.no_leader = no_leader

        wale_cmd = [
            'envdir',
            env_dir,
            'wal-e',
        ]

        if use_iam == 1:
            wale_cmd += ['--aws-instance-profile']

        self.wal_e = WALEConfig(
            env_dir=env_dir,
            threshold_mb=threshold_mb,
            threshold_pct=threshold_pct,
            cmd=wale_cmd,
        )

        self.init_error = (not os.path.exists(self.wal_e.env_dir))
        self.retries = retries

    def run(self) -> int:
        """
        Creates a new replica using WAL-E

        Returns
        -------
        ExitCode
            0 = Success
            1 = Error, try again
            2 = Error, don't try again

        """
        pass

    def should_use_s3_to_create_replica(self) -> Optional[bool]:
        """ determine whether it makes sense to use S3 and not pg_basebackup """
        pass

    def fix_subdirectory_path_if_broken(self, dirname: str) -> bool:
        # in case it is a symlink pointing to a non-existing location, remove it and create the actual directory
        pass

    def create_replica_with_s3(self) -> int:
        # if we're set up, restore the replica using fetch latest
        pass


def main() -> int:
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)
    parser = argparse.ArgumentParser(description='Script to image replicas using WAL-E')
    parser.add_argument('--scope', required=True)
    parser.add_argument('--role', required=False)
    parser.add_argument('--datadir', required=True)
    parser.add_argument('--connstring', required=True)
    parser.add_argument('--retries', type=int, default=1)
    parser.add_argument('--envdir', required=True)
    parser.add_argument('--threshold_megabytes', type=int, default=10240)
    parser.add_argument('--threshold_backup_size_percentage', type=int, default=30)
    parser.add_argument('--use_iam', type=int, default=0)
    parser.add_argument('--no_leader', type=int, default=0)
    args = parser.parse_args()

    exit_code = None
    assert args.retries >= 0

    # Retry cloning in a loop. We do separate retries for the leader
    # connection attempt inside should_use_s3_to_create_replica,
    # because we need to differentiate between the last attempt and
    # the rest and make a decision when the last attempt fails on
    # whether to use WAL-E or not depending on the no_leader flag.
    for _ in range(0, args.retries + 1):
        restore = WALERestore(scope=args.scope, datadir=args.datadir, connstring=args.connstring,
                              env_dir=args.envdir, threshold_mb=args.threshold_megabytes,
                              threshold_pct=args.threshold_backup_size_percentage, use_iam=args.use_iam,
                              no_leader=args.no_leader, retries=args.retries)
        exit_code = restore.run()
        if exit_code != ExitCode.RETRY_LATER:  # only WAL-E failures lead to the retry
            logger.debug('exit_code is %r, not retrying', exit_code)
            break
        time.sleep(RETRY_SLEEP_INTERVAL)

    if TYPE_CHECKING:  # pragma: no cover
        assert exit_code is not None
    return exit_code


if __name__ == '__main__':
    sys.exit(main())
