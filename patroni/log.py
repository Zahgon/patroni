"""Patroni logging facilities.

Daemon processes will use a 2-step logging handler. Whenever a log message is issued it is initially enqueued in-memory
and is later asynchronously flushed by a thread to the final destination.
"""
import logging
import os
import sys

from copy import deepcopy
from io import TextIOWrapper
from logging.handlers import RotatingFileHandler
from queue import Full, Queue
from threading import Lock, Thread
from typing import Any, cast, Dict, List, Optional, TYPE_CHECKING, Union

from .file_perm import pg_perm
from .utils import deep_compare, parse_int

type_logformat = Union[List[Union[str, Dict[str, Any], Any]], str, Any]

_LOGGER = logging.getLogger(__name__)


class PatroniFileHandler(RotatingFileHandler):
    """Wrapper of :class:`RotatingFileHandler` to handle permissions of log files. """

    def __init__(self, filename: str, mode: Optional[int]) -> None:
        """Create a new :class:`PatroniFileHandler` instance.

        :param filename: basename for log files.
        :param mode: permissions for log files.
        """
        self.set_log_file_mode(mode)
        super(PatroniFileHandler, self).__init__(filename)

    def set_log_file_mode(self, mode: Optional[int]) -> None:
        """Set mode for Patroni log files.

        :param mode: permissions for log files.

        .. note::
            If *mode* is not specified, we calculate it from the `umask` value.
        """
        pass

    def _open(self) -> TextIOWrapper:
        """Open a new log file and assign permissions.

        :returns: the resulting stream.
        """
        pass


def debug_exception(self: logging.Logger, msg: object, *args: Any, **kwargs: Any) -> None:
    """Add full stack trace info to debug log messages and partial to others.

    Handle :func:`~self.exception` calls for *self*.

    .. note::
        * If *self* log level is set to ``DEBUG``, then issue a ``DEBUG`` message with the complete stack trace;
        * If *self* log level is ``INFO`` or higher, then issue an ``ERROR`` message with only the last line of
            the stack trace.

    :param self: logger for which :func:`~self.exception` will be processed.
    :param msg: the message related to the exception to be logged.
    :param args: positional arguments to be passed to :func:`~self.debug` or :func:`~self.error`.
    :param kwargs: keyword arguments to be passed to :func:`~self.debug` or :func:`~self.error`.
    """
    pass


def error_exception(self: logging.Logger, msg: object, *args: Any, **kwargs: Any) -> None:
    """Add full stack trace info to error messages.

    Handle :func:`~self.exception` calls for *self*.

    .. note::
        * By default issue an ``ERROR`` message with the complete stack trace. If you do not want to show the complete
          stack trace, call with ``exc_info=False``.

    :param self: logger for which :func:`~self.exception` will be processed.
    :param msg: the message related to the exception to be logged.
    :param args: positional arguments to be passed to :func:`~self.error`.
    :param kwargs: keyword arguments to be passed to :func:`~self.error`.
    """
    pass


def _type(value: Any) -> str:
    """Get type of the *value*.

    :param value: any arbitrary value.
    :returns: a string with a type name.
    """
    pass


class QueueHandler(logging.Handler):
    """Queue-based logging handler.

    :ivar queue: queue to hold log messages that are pending to be flushed to the final destination.
    """

    def __init__(self) -> None:
        """Queue initialised and initial records_lost established."""
        super().__init__()
        self.queue: Queue[Union[logging.LogRecord, None]] = Queue()
        self._records_lost = 0

    def _put_record(self, record: logging.LogRecord) -> None:
        """Asynchronously enqueue a log record.

        :param record: the record to be logged.
        """
        pass

    def _try_to_report_lost_records(self) -> None:
        """Report the number of log messages that have been lost and reset the counter.

        .. note::
            It will issue an ``WARNING`` message in the logs with the number of lost log messages.
        """
        pass

    def emit(self, record: logging.LogRecord) -> None:
        """Handle each log record that is emitted.

        Call :func:`_put_record` to enqueue the emitted log record.

        Also check if we have previously lost any log record, and if so, log a ``WARNING`` message.

        :param record: the record that was emitted.
        """
        pass

    @property
    def records_lost(self) -> int:
        """Number of log messages that have been lost while the queue was full."""
        pass


class ProxyHandler(logging.Handler):
    """Handle log records in place of pending log handlers.

    .. note::
        This is used to handle log messages while the logger thread has not started yet, in which case the queue-based
        handler is not yet started.

    :ivar patroni_logger: the logger thread.
    """

    def __init__(self, patroni_logger: 'PatroniLogger') -> None:
        """Create a new :class:`ProxyHandler` instance.

        :param patroni_logger: the logger thread.
        """
        super().__init__()
        self.patroni_logger = patroni_logger

    def emit(self, record: logging.LogRecord) -> None:
        """Emit each log record that is handled.

        Will push the log record down to :func:`~logging.Handler.handle` method of the currently configured log handler.

        :param record: the record that was emitted.
        """
        pass


class PatroniLogger(Thread):
    """Logging thread for the Patroni daemon process.

    It is a 2-step logging approach. Any time a log message is issued it is initially enqueued in-memory, and then
    asynchronously flushed to the final destination by the logging thread.

    .. seealso::
        :class:`QueueHandler`: object used for enqueueing messages in-memory.

    :cvar DEFAULT_TYPE: default type of log format (``plain``).
    :cvar DEFAULT_LEVEL: default logging level (``INFO``).
    :cvar DEFAULT_TRACEBACK_LEVEL: default traceback logging level (``ERROR``).
    :cvar DEFAULT_FORMAT: default format of log messages (``%(asctime)s %(levelname)s: %(message)s``).
    :cvar NORMAL_LOG_QUEUE_SIZE: expected number of log messages per HA loop when operating under a normal situation.
    :cvar DEFAULT_MAX_QUEUE_SIZE: default maximum queue size for holding a backlog of log messages that are pending
        to be flushed.
    :cvar LOGGING_BROKEN_EXIT_CODE: exit code to be used if it detects(``5``).

    :ivar log_handler: log handler that is currently being used by the thread.
    :ivar log_handler_lock: lock used to modify ``log_handler``.
    """

    DEFAULT_TYPE = 'plain'
    DEFAULT_LEVEL = 'INFO'
    DEFAULT_TRACEBACK_LEVEL = 'ERROR'
    DEFAULT_FORMAT = '%(asctime)s %(levelname)s: %(message)s'

    NORMAL_LOG_QUEUE_SIZE = 2  # When everything goes normal Patroni writes only 2 messages per HA loop
    DEFAULT_MAX_QUEUE_SIZE = 1000
    LOGGING_BROKEN_EXIT_CODE = 5

    def __init__(self) -> None:
        """Prepare logging queue and proxy handlers as they become ready during daemon startup.

        .. note::
            While Patroni is starting up it keeps ``DEBUG`` log level, and writes log messages through a proxy handler.
            Once the logger thread is finally started, it switches from that proxy handler to the queue based logger,
            and applies the configured log settings. The switching is used to avoid that the logger thread prevents
            Patroni from shutting down if any issue occurs in the meantime until the thread is properly started.
        """
        super(PatroniLogger, self).__init__()
        self._queue_handler = QueueHandler()
        self._root_logger = logging.getLogger()
        self._config: Optional[Dict[str, Any]] = None
        self.log_handler = None
        self.log_handler_lock = Lock()
        self._old_handlers: List[logging.Handler] = []
        # initially set log level to ``DEBUG`` while the logger thread has not started running yet. The daemon process
        # will later adjust all log related settings with what was provided through the user configuration file.
        self.reload_config({'level': 'DEBUG'})
        # We will switch to the QueueHandler only when thread was started.
        # This is necessary to protect from the cases when Patroni constructor
        # failed and PatroniLogger thread remain running and prevent shutdown.
        self._proxy_handler = ProxyHandler(self)
        self._root_logger.addHandler(self._proxy_handler)

    def update_loggers(self, config: Dict[str, Any]) -> None:
        """Configure custom loggers' log levels.

        .. note::
            It creates logger objects that are not defined yet in the log manager.

        :param config: :class:`dict` object with custom loggers configuration, is set either from:

                       * ``log.loggers`` section of Patroni configuration; or

                       * from the method that is trying to make sure that the node name
                         isn't duplicated (to silence annoying ``urllib3`` WARNING's).

        :Example:

            .. code-block:: python

                update_loggers({'urllib3.connectionpool': 'WARNING'})
        """
        pass

    def _is_config_changed(self, config: Dict[str, Any]) -> bool:
        """Checks if the given config is different from the current one.

        :param config: ``log`` section from Patroni configuration.

        :returns: ``True`` if the config is changed, ``False`` otherwise.
        """
        pass

    def _get_plain_formatter(self, logformat: type_logformat, dateformat: Optional[str]) -> logging.Formatter:
        """Returns a logging formatter with the specified format and date format.

        .. note::
            If the log format isn't a string, prints a warning message and uses the default log format instead.

        :param logformat: The format of the log messages.
        :param dateformat: The format of the timestamp in the log messages.

        :returns: A logging formatter object that can be used to format log records.
        """
        pass

    def _get_json_formatter(self, logformat: type_logformat, dateformat: Optional[str],
                            static_fields: Dict[str, Any]) -> logging.Formatter:
        """Returns a logging formatter that outputs JSON formatted messages.

        .. note::
            If :mod:`pythonjsonlogger` library is not installed, prints an error message and returns
            a plain log formatter instead.

        :param logformat: Specifies the log fields and their key names in the JSON log message.
        :param dateformat: The format of the timestamp in the log messages.
        :param static_fields: A dictionary of static fields that are added to every log message.

        :returns: A logging formatter object that can be used to format log records as JSON strings.
        """
        pass

    def _get_formatter(self, config: Dict[str, Any]) -> logging.Formatter:
        """Returns a logging formatter based on the type of logger in the given configuration.

        :param config: ``log`` section from Patroni configuration.

        :returns: A :class:`logging.Formatter` object that can be used to format log records.
        """
        pass

    def reload_config(self, config: Dict[str, Any]) -> None:
        """Apply log related configuration.

        .. note::
            It is also able to deal with runtime configuration changes.

        :param config: ``log`` section from Patroni configuration.
        """
        pass

    def _close_old_handlers(self) -> None:
        """Close old log handlers.

        .. note::
            It is used to remove different handlers that were configured previous to a reload in the configuration,
            e.g. if we are switching from :class:`PatroniFileHandler` to
            class:`~logging.StreamHandler` and vice-versa.
        """
        pass

    def run(self) -> None:
        """Run logger's thread main loop.

        Keep consuming log queue until requested to quit through ``None`` special log record.
        """
        pass

    @staticmethod
    def _is_heartbeat_msg(record: logging.LogRecord) -> bool:
        """Checks if the given record contains a heartbeat message.

        :param record: the record to check.

        :returns: ``True`` if the record contains a heartbeat message, ``False`` otherwise.
        """
        pass

    def shutdown(self) -> None:
        """Shut down the logger thread."""
        pass

    @property
    def queue_size(self) -> int:
        """Number of log records in the queue."""
        pass

    @property
    def records_lost(self) -> int:
        """Number of logging records that have been lost while the queue was full."""
        pass
