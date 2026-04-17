import abc
import logging
import platform
import sys

from threading import RLock
from typing import Any, Callable, Dict, Optional, Union

from ..config import Config
from ..exceptions import WatchdogError

__all__ = ['WatchdogError', 'Watchdog']

logger = logging.getLogger(__name__)

MODE_REQUIRED = 'required'    # Will not run if a watchdog is not available
MODE_AUTOMATIC = 'automatic'  # Will use a watchdog if one is available
MODE_OFF = 'off'              # Will not try to use a watchdog


def parse_mode(mode: Union[bool, str]) -> str:
    pass


def synchronized(func: Callable[..., Any]) -> Callable[..., Any]:
    pass


class WatchdogConfig(object):
    """Helper to contain a snapshot of configuration"""
    def __init__(self, config: Config) -> None:
        watchdog_config = config.get("watchdog") or {'mode': 'automatic'}

        self.mode = parse_mode(watchdog_config.get('mode', 'automatic'))
        self.ttl = config['ttl']
        self.loop_wait = config['loop_wait']
        self.safety_margin = watchdog_config.get('safety_margin', 5)
        self.driver = watchdog_config.get('driver', 'default')
        self.driver_config = dict((k, v) for k, v in watchdog_config.items()
                                  if k not in ['mode', 'safety_margin', 'driver'])

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, WatchdogConfig) and \
            all(getattr(self, attr) == getattr(other, attr) for attr in
                ['mode', 'ttl', 'loop_wait', 'safety_margin', 'driver', 'driver_config'])

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def get_impl(self) -> 'WatchdogBase':
        pass

    @property
    def timeout(self) -> int:
        pass

    @property
    def timing_slack(self) -> int:
        pass


class Watchdog(object):
    """Facade to dynamically manage watchdog implementations and handle config changes.

    When activation fails underlying implementation will be switched to a Null implementation. To avoid log spam
    activation will only be retried when watchdog configuration is changed."""
    def __init__(self, config: Config) -> None:
        self.config = WatchdogConfig(config)
        self.active_config: WatchdogConfig = self.config
        self.lock = RLock()
        self.active = False

        if self.config.mode == MODE_OFF:
            self.impl = NullWatchdog()
        else:
            self.impl = self.config.get_impl()
            if self.config.mode == MODE_REQUIRED and self.impl.is_null:
                logger.error("Configuration requires a watchdog, but watchdog is not supported on this platform.")
                sys.exit(1)

    @synchronized
    def reload_config(self, config: Config) -> None:
        pass

    @synchronized
    def activate(self) -> bool:
        """Activates the watchdog device with suitable timeouts. While watchdog is active keepalive needs
        to be called every time loop_wait expires.

        :returns False if a safe watchdog could not be configured, but is required.
        """
        pass

    def _activate(self) -> bool:
        pass

    def _set_timeout(self) -> Optional[int]:
        pass

    @synchronized
    def disable(self) -> None:
        pass

    def _disable(self) -> None:
        pass

    @synchronized
    def keepalive(self) -> None:
        pass

    @property
    @synchronized
    def is_running(self) -> bool:
        pass

    @property
    @synchronized
    def is_healthy(self) -> bool:
        pass


class WatchdogBase(abc.ABC):
    """A watchdog object when opened requires periodic calls to keepalive.
    When keepalive is not called within a timeout the system will be terminated."""
    is_null = False

    @property
    def is_running(self) -> bool:
        """Returns True when watchdog is activated and capable of performing it's task."""
        pass

    @property
    def is_healthy(self) -> bool:
        """Returns False when calling open() is known to fail."""
        pass

    @property
    def can_be_disabled(self) -> bool:
        """Returns True when watchdog will be disabled by calling close(). Some watchdog devices
        will keep running no matter what once activated. May raise WatchdogError if called without
        calling open() first."""
        pass

    @abc.abstractmethod
    def open(self) -> None:
        """Open watchdog device.

        When watchdog is opened keepalive must be called. Returns nothing on success
        or raises WatchdogError if the device could not be opened."""

    @abc.abstractmethod
    def close(self) -> None:
        """Gracefully close watchdog device."""

    @abc.abstractmethod
    def keepalive(self) -> None:
        """Resets the watchdog timer.

        Watchdog must be open when keepalive is called."""

    @abc.abstractmethod
    def get_timeout(self) -> int:
        """Returns the current keepalive timeout in effect."""

    def has_set_timeout(self) -> bool:
        """Returns True if setting a timeout is supported."""
        pass

    def set_timeout(self, timeout: int) -> None:
        """Set the watchdog timer timeout.

        :param timeout: watchdog timeout in seconds"""
        pass

    def describe(self) -> str:
        """Human readable name for this device"""
        pass

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> 'WatchdogBase':
        pass


class NullWatchdog(WatchdogBase):
    """Null implementation when watchdog is not supported."""
    is_null = True

    def open(self) -> None:
        pass

    def close(self) -> None:
        pass

    def keepalive(self) -> None:
        pass

    def get_timeout(self) -> int:
        # A big enough number to not matter
        pass
