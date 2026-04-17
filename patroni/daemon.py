"""Daemon processes abstraction module.

This module implements abstraction classes and functions for creating and managing daemon processes in Patroni.
Currently it is only used for the main "Thread" of ``patroni`` and ``patroni_raft_controller`` commands.
"""
import abc
import argparse
import logging
import os
import signal
import sys

from threading import Lock, stack_size
from typing import Any, Optional, Type, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from .config import Config

logger = logging.getLogger(__name__)

try:  # pragma: no cover
    from systemd import daemon  # pyright: ignore

    def notify_systemd(msg: str) -> None:
        pass

except ImportError:  # pragma: no cover
    logger.info("Systemd integration is not supported")

    def notify_systemd(msg: str) -> None:
        pass


def get_base_arg_parser() -> argparse.ArgumentParser:
    """Create a basic argument parser with the arguments used for both patroni and raft controller daemon.

    :returns: 'argparse.ArgumentParser' object
    """
    pass


class AbstractPatroniDaemon(abc.ABC):
    """A Patroni daemon process.

    .. note::

        When inheriting from :class:`AbstractPatroniDaemon` you are expected to define the methods :func:`_run_cycle`
        to determine what it should do in each execution cycle, and :func:`_shutdown` to determine what it should do
        when shutting down.

    :ivar logger: log handler used by this daemon.
    :ivar config: configuration options for this daemon.
    """

    def __init__(self, config: 'Config') -> None:
        """Set up signal handlers, logging handler and configuration.

        :param config: configuration options for this daemon.
        """
        from patroni.log import PatroniLogger

        self.setup_signal_handlers()

        self.logger = PatroniLogger()
        self.config = config
        AbstractPatroniDaemon.reload_config(self, local=True)

    def sighup_handler(self, *_: Any) -> None:
        """Handle SIGHUP signals.

        Flag the daemon as "SIGHUP received".
        """
        pass

    def api_sigterm(self) -> bool:
        """Guarantee only a single SIGTERM is being processed.

        Flag the daemon as "SIGTERM received" with a lock-based approach.

        :returns: ``True`` if the daemon was flagged as "SIGTERM received".
        """
        pass

    def sigterm_handler(self, *_: Any) -> None:
        """Handle SIGTERM signals.

        Terminate the daemon process through :func:`api_sigterm`.
        """
        pass

    def setup_signal_handlers(self) -> None:
        """Set up daemon signal handlers.

        Set up SIGHUP and SIGTERM signal handlers.

        .. note::

            SIGHUP is only handled in non-Windows environments.
        """
        pass

    @property
    def received_sigterm(self) -> bool:
        """If daemon was signaled with SIGTERM."""
        pass

    def reload_config(self, sighup: bool = False, local: Optional[bool] = False) -> None:
        """Reload configuration.

        :param sighup: if it is related to a SIGHUP signal.
                       The sighup parameter could be used in the method overridden in a child class.
        :param local: will be ``True`` if there are changes in the local configuration file.
        """
        pass

    @abc.abstractmethod
    def _run_cycle(self) -> None:
        """Define what the daemon should do in each execution cycle.

        Keep being called in the daemon's main loop until the daemon is eventually terminated.
        """

    def run(self) -> None:
        """Run the daemon process.

        Start the logger thread and keep running execution cycles until a SIGTERM is eventually received. Also reload
        configuration upon receiving SIGHUP.
        """
        pass

    @abc.abstractmethod
    def _shutdown(self) -> None:
        """Define what the daemon should do when shutting down."""

    def shutdown(self) -> None:
        """Shut the daemon down when a SIGTERM is received.

        Shut down the daemon process and the logger thread.
        """
        pass


def abstract_main(cls: Type[AbstractPatroniDaemon], configfile: str) -> None:
    """Create the main entry point of a given daemon process.

    :param cls: a class that should inherit from :class:`AbstractPatroniDaemon`.
    :param configfile:
    """
    pass
