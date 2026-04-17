#!/usr/bin/env python

"""Implements ``patroni_barman config-switch`` sub-command.

Apply a Barman configuration model through ``pg-backup-api``.

This sub-command is specially useful as a ``on_role_change`` callback to change
Barman configuration in response to failovers and switchovers. Check the output
of ``--help`` to understand the parameters supported by the sub-command.

It requires that you have previously configured a Barman server and Barman
config models, and that you have ``pg-backup-api`` configured and running in
the same host as Barman.

Refer to :class:`ExitCode` for possible exit codes of this sub-command.
"""
import logging
import time

from argparse import Namespace
from enum import IntEnum
from typing import Optional, TYPE_CHECKING

from .utils import OperationStatus, RetriesExceeded

if TYPE_CHECKING:  # pragma: no cover
    from .utils import PgBackupApi


class ExitCode(IntEnum):
    """Possible exit codes of this script.

    :cvar CONFIG_SWITCH_DONE: config switch was successfully performed.
    :cvar CONFIG_SWITCH_SKIPPED: if the execution was skipped because of not
        matching user expectations.
    :cvar CONFIG_SWITCH_FAILED: config switch faced an issue.
    :cvar HTTP_ERROR: an error has occurred while communicating with
        ``pg-backup-api``
    :cvar INVALID_ARGS: an invalid set of arguments has been given to the
        operation.
    """

    CONFIG_SWITCH_DONE = 0
    CONFIG_SWITCH_SKIPPED = 1
    CONFIG_SWITCH_FAILED = 2
    HTTP_ERROR = 3
    INVALID_ARGS = 4


def _should_skip_switch(args: Namespace) -> bool:
    """Check if we should skip the config switch operation.

    :param args: arguments received from the command-line of
        ``patroni_barman config-switch`` command.

    :returns: if the operation should be skipped.
    """
    pass


def _switch_config(api: "PgBackupApi", barman_server: str,
                   barman_model: Optional[str], reset: Optional[bool]) -> int:
    """Switch configuration of Barman server through ``pg-backup-api``.

    .. note::
        If requests to ``pg-backup-api`` fail recurrently or we face HTTP
        errors, then exit with :attr:`ExitCode.HTTP_ERROR`.

    :param api: a :class:`PgBackupApi` instance to handle communication with
        the API.
    :param barman_server: name of the Barman server which config is to be
        switched.
    :param barman_model: name of the Barman model to be applied to the server,
        if any.
    :param reset: ``True`` if you would like to unapply the currently active
        model for the server, if any.

    :returns: the return code to be used when exiting the ``patroni_barman``
        application. Refer to :class:`ExitCode`.
    """
    pass


def run_barman_config_switch(api: "PgBackupApi", args: Namespace) -> int:
    """Run a remote ``barman config-switch`` through the ``pg-backup-api``.

    :param api: a :class:`PgBackupApi` instance to handle communication with
        the API.
    :param args: arguments received from the command-line of
        ``patroni_barman config-switch`` command.

    :returns: the return code to be used when exiting the ``patroni_barman``
        application. Refer to :class:`ExitCode`.
    """
    pass
