"""Helper functions to search for implementations of specific abstract interface in a package."""
import importlib
import inspect
import logging
import os
import pkgutil
import sys

from types import ModuleType
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Type, TYPE_CHECKING, TypeVar, Union

if TYPE_CHECKING:  # pragma: no cover
    from .config import Config

logger = logging.getLogger(__name__)


def iter_modules(package: str) -> List[str]:
    """Get names of modules from *package*, depending on execution environment.

    .. note::
        If being packaged with PyInstaller, modules aren't discoverable dynamically by scanning source directory because
        :class:`importlib.machinery.FrozenImporter` doesn't implement :func:`iter_modules`. But it is still possible to
        find all potential modules by iterating through ``toc``, which contains list of all "frozen" resources.

    :param package: a package name to search modules in, e.g. ``patroni.dcs``.

    :returns: list of known module names with absolute python module path namespace, e.g. ``patroni.dcs.etcd``.
    """
    pass


ClassType = TypeVar("ClassType")


def find_class_in_module(module: ModuleType, cls_type: Type[ClassType]) -> Optional[Type[ClassType]]:
    """Try to find the implementation of *cls_type* class interface in *module* matching the *module* name.

    :param module: imported module.
    :param cls_type: a class type we are looking for.

    :returns: class with a name matching the name of *module* that implements *cls_type* or ``None`` if not found.
    """
    pass


def iter_classes(
        package: str, cls_type: Type[ClassType],
        config: Optional[Union['Config', Dict[str, Any]]] = None
) -> Iterator[Tuple[str, Type[ClassType]]]:
    """Attempt to import modules and find implementations of *cls_type* that are present in the given configuration.

    .. note::
            If a module successfully imports we can assume that all its requirements are installed.

    :param package: a package name to search modules in, e.g. ``patroni.dcs``.
    :param cls_type: a class type we are looking for.
    :param config: configuration information with possible module names as keys. If given, only attempt to import
                   modules defined in the configuration. Else, if ``None``, attempt to import any supported module.

    :yields: a tuple containing the module ``name`` and the imported class object.
    """
    pass
