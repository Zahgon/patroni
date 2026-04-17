"""Tags handling."""
import abc

from typing import Any, Dict, Optional

from patroni.utils import parse_bool, parse_int


class Tags(abc.ABC):
    """An abstract class that encapsulates all the ``tags`` logic.

    Child classes that want to use provided facilities must implement ``tags`` abstract property.

    .. note::
        Due to backward-compatibility reasons, old tags may have a less strict type conversion than new ones.
    """

    @staticmethod
    def _filter_tags(tags: Dict[str, Any]) -> Dict[str, Any]:
        """Get tags configured for this node, if any.

        Handle both predefined Patroni tags and custom defined tags.

        .. note::
            A custom tag is any tag added to the configuration ``tags`` section that is not one of ``clonefrom``,
            ``nofailover``, ``noloadbalance``,``nosync`` or ``nostream``.

            For most of the Patroni predefined tags, the returning object will only contain them if they are enabled as
            they all are boolean values that default to disabled.
            However ``nofailover`` tag is always returned if ``failover_priority`` tag is defined. In this case, we need
            both values to see if they are contradictory and the ``nofailover`` value should be used.
            The same rule applies for ``nosync`` and ``sync_priority`` tags.

        :returns: a dictionary of tags set for this node. The key is the tag name, and the value is the corresponding
            tag value.
        """
        pass

    @property
    @abc.abstractmethod
    def tags(self) -> Dict[str, Any]:
        """Configured tags.

        Must be implemented in a child class.
        """
        pass

    @property
    def clonefrom(self) -> bool:
        """``True`` if ``clonefrom`` tag is ``True``, else ``False``."""
        pass

    def _priority_tag(self, bool_name: str, priority_name: str) -> int:
        """Common logic for obtaining the value of a priority tag from ``tags`` if defined.

        If boolean tag is defined as ``True``, this will return ``0``. Otherwise, it will return the value of
        the respective priority tag, defaulting to ``1`` if it's not defined or invalid.

        :param bool_name: name of the boolean tag (``nofailover``. ``nosync``).
        :param priority_name: name of the priority tag (``failover_priority``, ``sync_priority``).

        :returns: integer value based on the defined tags.
        """
        pass

    def _bool_tag(self, bool_name: str, priority_name: str) -> bool:
        """Common logic for obtaining the value of a boolean tag from ``tags`` if defined.

        If boolean tag is not defined, this methods returns ``True`` if priority tag is non-positive,
        ``False`` otherwise.

        :param bool_name: name of the boolean tag (``nofailover``. ``nosync``).
        :param priority_name: name of the priority tag (``failover_priority``, ``sync_priority``).

        :returns: boolean value based on the defined tags.
        """
        pass

    @property
    def nofailover(self) -> bool:
        """``True`` if node configuration doesn't allow it to become primary, ``False`` otherwise."""
        pass

    @property
    def failover_priority(self) -> int:
        """Value of ``failover_priority`` from ``tags`` if defined, otherwise derived from ``nofailover``."""
        pass

    @property
    def noloadbalance(self) -> bool:
        """``True`` if ``noloadbalance`` is ``True``, else ``False``."""
        pass

    @property
    def nosync(self) -> bool:
        """``True`` if node configuration doesn't allow it to become synchronous, ``False`` otherwise."""
        pass

    @property
    def sync_priority(self) -> int:
        """Value of ``sync_priority`` from ``tags`` if defined, otherwise derived from ``nosync``."""
        pass

    @property
    def replicatefrom(self) -> Optional[str]:
        """Value of ``replicatefrom`` tag, if any."""
        pass

    @property
    def nostream(self) -> bool:
        """``True`` if ``nostream`` is ``True``, else ``False``."""
        pass
