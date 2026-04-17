# pyright: reportConstantRedefinition=false
import ctypes
import os
import platform

from typing import Any, Dict, NamedTuple

from .base import WatchdogBase, WatchdogError

# Pythonification of linux/ioctl.h
IOC_NONE = 0
IOC_WRITE = 1
IOC_READ = 2

IOC_NRBITS = 8
IOC_TYPEBITS = 8
IOC_SIZEBITS = 14
IOC_DIRBITS = 2

# Non-generic platform special cases
machine = platform.machine()
if machine in ['mips', 'sparc', 'powerpc', 'ppc64', 'ppc64le']:  # pragma: no cover
    IOC_SIZEBITS = 13
    IOC_DIRBITS = 3
    IOC_NONE, IOC_WRITE = 1, 4
elif machine == 'parisc':  # pragma: no cover
    IOC_WRITE, IOC_READ = 2, 1

IOC_NRSHIFT = 0
IOC_TYPESHIFT = IOC_NRSHIFT + IOC_NRBITS
IOC_SIZESHIFT = IOC_TYPESHIFT + IOC_TYPEBITS
IOC_DIRSHIFT = IOC_SIZESHIFT + IOC_SIZEBITS


def IOW(type_: str, nr: int, size: int) -> int:
    pass


def IOR(type_: str, nr: int, size: int) -> int:
    return IOC(IOC_READ, type_, nr, size)


def IOWR(type_: str, nr: int, size: int) -> int:
    return IOC(IOC_READ | IOC_WRITE, type_, nr, size)


def IOC(dir_: int, type_: str, nr: int, size: int) -> int:
    pass


# Pythonification of linux/watchdog.h

WATCHDOG_IOCTL_BASE = 'W'


class watchdog_info(ctypes.Structure):
    _fields_ = [
        ('options', ctypes.c_uint32),           # Options the card/driver supports
        ('firmware_version', ctypes.c_uint32),  # Firmware version of the card
        ('identity', ctypes.c_uint8 * 32),      # Identity of the board
    ]


struct_watchdog_info_size = ctypes.sizeof(watchdog_info)
int_size = ctypes.sizeof(ctypes.c_int)

WDIOC_GETSUPPORT = IOR(WATCHDOG_IOCTL_BASE, 0, struct_watchdog_info_size)
WDIOC_GETSTATUS = IOR(WATCHDOG_IOCTL_BASE, 1, int_size)
WDIOC_GETBOOTSTATUS = IOR(WATCHDOG_IOCTL_BASE, 2, int_size)
WDIOC_GETTEMP = IOR(WATCHDOG_IOCTL_BASE, 3, int_size)
WDIOC_SETOPTIONS = IOR(WATCHDOG_IOCTL_BASE, 4, int_size)
WDIOC_KEEPALIVE = IOR(WATCHDOG_IOCTL_BASE, 5, int_size)
WDIOC_SETTIMEOUT = IOWR(WATCHDOG_IOCTL_BASE, 6, int_size)
WDIOC_GETTIMEOUT = IOR(WATCHDOG_IOCTL_BASE, 7, int_size)
WDIOC_SETPRETIMEOUT = IOWR(WATCHDOG_IOCTL_BASE, 8, int_size)
WDIOC_GETPRETIMEOUT = IOR(WATCHDOG_IOCTL_BASE, 9, int_size)
WDIOC_GETTIMELEFT = IOR(WATCHDOG_IOCTL_BASE, 10, int_size)


WDIOF_UNKNOWN = -1  # Unknown flag error
WDIOS_UNKNOWN = -1  # Unknown status error

WDIOF = {
    "OVERHEAT": 0x0001,       # Reset due to CPU overheat
    "FANFAULT": 0x0002,       # Fan failed
    "EXTERN1": 0x0004,        # External relay 1
    "EXTERN2": 0x0008,        # External relay 2
    "POWERUNDER": 0x0010,     # Power bad/power fault
    "CARDRESET": 0x0020,      # Card previously reset the CPU
    "POWEROVER": 0x0040,      # Power over voltage
    "SETTIMEOUT": 0x0080,     # Set timeout (in seconds)
    "MAGICCLOSE": 0x0100,     # Supports magic close char
    "PRETIMEOUT": 0x0200,     # Pretimeout (in seconds), get/set
    "ALARMONLY": 0x0400,      # Watchdog triggers a management or other external alarm not a reboot
    "KEEPALIVEPING": 0x8000,  # Keep alive ping reply
}

WDIOS = {
    "DISABLECARD": 0x0001,    # Turn off the watchdog timer
    "ENABLECARD": 0x0002,     # Turn on the watchdog timer
    "TEMPPANIC": 0x0004,      # Kernel panic on temperature trip
}

# Implementation


class WatchdogInfo(NamedTuple):
    """Watchdog descriptor from the kernel"""
    options: int
    version: int
    identity: str

    def __getattr__(self, name: str) -> bool:
        """Convenience has_XYZ attributes for checking WDIOF bits in options"""
        if name.startswith('has_') and name[4:] in WDIOF:
            return bool(self.options & WDIOF[name[4:]])

        raise AttributeError("WatchdogInfo instance has no attribute '{0}'".format(name))


class LinuxWatchdogDevice(WatchdogBase):
    DEFAULT_DEVICE = '/dev/watchdog'

    def __init__(self, device: str) -> None:
        self.device = device
        self._support_cache = None
        self._fd = None

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> 'LinuxWatchdogDevice':
        pass

    @property
    def is_running(self) -> bool:
        pass

    @property
    def is_healthy(self) -> bool:
        pass

    def open(self) -> None:
        pass

    def close(self) -> None:
        pass

    @property
    def can_be_disabled(self) -> bool:
        pass

    def _ioctl(self, func: int, arg: Any) -> None:
        """Runs the specified ioctl on the underlying fd.

        Raises WatchdogError if the device is closed.
        Raises OSError or IOError (Python 2) when the ioctl fails."""
        pass

    def get_support(self) -> WatchdogInfo:
        pass

    def describe(self) -> str:
        pass

    def keepalive(self) -> None:
        pass

    def has_set_timeout(self) -> bool:
        """Returns True if setting a timeout is supported."""
        pass

    def set_timeout(self, timeout: int) -> None:
        pass

    def get_timeout(self) -> int:
        pass


class TestingWatchdogDevice(LinuxWatchdogDevice):  # pragma: no cover
    """Converts timeout ioctls to regular writes that can be intercepted from a named pipe."""
    timeout = 60

    def get_support(self) -> WatchdogInfo:
        pass

    def set_timeout(self, timeout: int) -> None:
        pass

    def get_timeout(self) -> int:
        pass
