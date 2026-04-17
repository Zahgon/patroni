import abc
import logging

from copy import deepcopy
from typing import Any, cast, Dict, Iterator, List, MutableMapping, Optional, Tuple, Type, Union

import yaml

from ..collections import CaseInsensitiveDict, CaseInsensitiveSet
from ..exceptions import PatroniException
from ..utils import parse_bool, parse_int, parse_real
from .available_parameters import get_validator_files, PathLikeObj

logger = logging.getLogger(__name__)


class _Transformable(abc.ABC):

    def __init__(self, version_from: int, version_till: Optional[int] = None) -> None:
        self.__version_from = version_from
        self.__version_till = version_till

    @classmethod
    def get_subclasses(cls) -> Iterator[Type['_Transformable']]:
        """Recursively get all subclasses of :class:`_Transformable`.

        :yields: each subclass of :class:`_Transformable`.
        """
        for subclass in cls.__subclasses__():
            yield from subclass.get_subclasses()
            yield subclass

    @property
    def version_from(self) -> int:
        pass

    @property
    def version_till(self) -> Optional[int]:
        pass

    @abc.abstractmethod
    def transform(self, name: str, value: Any) -> Optional[Any]:
        """Verify that provided value is valid.

        :param name: GUC's name
        :param value: GUC's value
        :returns: the value (sometimes clamped) or ``None`` if the value isn't valid
        """


class Bool(_Transformable):

    def transform(self, name: str, value: Any) -> Optional[Any]:
        pass


class Number(_Transformable):

    def __init__(self, *, version_from: int, version_till: Optional[int] = None, min_val: Union[int, float],
                 max_val: Union[int, float], unit: Optional[str] = None) -> None:
        super(Number, self).__init__(version_from, version_till)
        self.__min_val = min_val
        self.__max_val = max_val
        self.__unit = unit

    @property
    def min_val(self) -> Union[int, float]:
        pass

    @property
    def max_val(self) -> Union[int, float]:
        pass

    @property
    def unit(self) -> Optional[str]:
        pass

    @staticmethod
    @abc.abstractmethod
    def parse(value: Any, unit: Optional[str]) -> Optional[Any]:
        """Convert provided value to unit."""

    def transform(self, name: str, value: Any) -> Union[int, float, None]:
        pass


class Integer(Number):

    @staticmethod
    def parse(value: Any, unit: Optional[str]) -> Optional[int]:
        pass


class Real(Number):

    @staticmethod
    def parse(value: Any, unit: Optional[str]) -> Optional[float]:
        pass


class Enum(_Transformable):

    def __init__(self, *, version_from: int, version_till: Optional[int] = None,
                 possible_values: Tuple[str, ...]) -> None:
        super(Enum, self).__init__(version_from, version_till)
        self.__possible_values = possible_values

    @property
    def possible_values(self) -> Tuple[str, ...]:
        pass

    def transform(self, name: str, value: Optional[Any]) -> Optional[Any]:
        pass


class EnumBool(Enum):

    def transform(self, name: str, value: Optional[Any]) -> Optional[Any]:
        pass


class String(_Transformable):

    def transform(self, name: str, value: Optional[Any]) -> Optional[Any]:
        pass


# Format:
#  key  - parameter name
#  value - variable length tuple of `_Transformable` objects. Each object in the tuple represents a different
#          validation of the GUC across postgres versions. If a GUC validation has never changed over time, then it will
#          have a single object in the tuple. For example, `password_encryption` used to be a boolean GUC up to Postgres
#          10, at which point it started being an enum. In that case the value of `password_encryption` would be a tuple
#          of 2 `_Transformable` objects (`Bool` and `Enum`, respectively), each one reprensenting a different
#          validation rule.
parameters = CaseInsensitiveDict()
recovery_parameters = CaseInsensitiveDict()


class ValidatorFactoryNoType(PatroniException):
    """Raised when a validator spec misses a type."""


class ValidatorFactoryInvalidType(PatroniException):
    """Raised when a validator spec contains an invalid type."""


class ValidatorFactoryInvalidSpec(PatroniException):
    """Raised when a validator spec contains an invalid set of attributes."""


class ValidatorFactory:
    """Factory class used to build Patroni validator objects based on the given specs."""

    TYPES: Dict[str, Type[_Transformable]] = {cls.__name__: cls for cls in _Transformable.get_subclasses()}

    def __new__(cls, validator: Dict[str, Any]) -> _Transformable:
        """Parse a given Postgres GUC *validator* into the corresponding Patroni validator object.

        :param validator: a validator spec for a given parameter. It usually comes from a parsed YAML file.

        :returns: the Patroni validator object that corresponds to the specification found in *validator*.

        :raises:
            :class:`ValidatorFactoryNoType`: if *validator* contains no ``type`` key.
            :class:`ValidatorFactoryInvalidType`: if ``type`` key from *validator* contains an invalid value.
            :class:`ValidatorFactoryInvalidSpec`: if *validator* contains an invalid set of attributes for the given
                ``type``.

        :Example:

            If a given validator was defined as follows in the YAML file:

            ```yaml
            - type: String
            version_from: 90300
            version_till: null
            ```

            Then this method would receive *validator* as:

            ```python
            {
                'type': 'String',
                'version_from': 90300,
                'version_till': None
            }
            ```

            And this method would return a :class:`String`:

            ```python
            String(90300, None)
            ```
        """
        validator = deepcopy(validator)
        try:
            type_ = validator.pop('type')
        except KeyError as exc:
            raise ValidatorFactoryNoType('Validator contains no type.') from exc

        if type_ not in cls.TYPES:
            raise ValidatorFactoryInvalidType(f'Unexpected validator type: `{type_}`.')

        for key, value in validator.items():
            # :func:`_transform_parameter_value` expects :class:`tuple` instead of :class:`list`
            if isinstance(value, list):
                validator[key] = tuple(cast(List[Any], value))

        try:
            return cls.TYPES[type_](**validator)
        except Exception as exc:
            raise ValidatorFactoryInvalidSpec(
                f'Failed to parse `{type_}` validator (`{validator}`): `{str(exc)}`.') from exc


def _get_postgres_guc_validators(config: Dict[str, Any], parameter: str) -> Tuple[_Transformable, ...]:
    """Get all validators of *parameter* from *config*.

    Loop over all validators specs of *parameter* and return them parsed as Patroni validators.

    :param config: Python object corresponding to an YAML file, with values of either ``parameters`` or
        ``recovery_parameters`` key.
    :param parameter: name of the parameter found under *config* which validators should be parsed and returned.

    :rtype: yields any exception that is faced while parsing a validator spec into a Patroni validator object.
    """
    pass


class InvalidGucValidatorsFile(PatroniException):
    """Raised when reading or parsing of a YAML file faces an issue."""


def _read_postgres_gucs_validators_file(file: PathLikeObj) -> Dict[str, Any]:
    """Read an YAML file and return the corresponding Python object.

    :param file: path-like object to read from. It is expected to be encoded with ``UTF-8``, and to be a YAML document.

    :returns: the YAML content parsed into a Python object. If any issue is faced while reading/parsing the file, then
        return ``None``.

    :raises:
        :class:`InvalidGucValidatorsFile`: if faces an issue while reading or parsing *file*.
    """
    pass


def _load_postgres_gucs_validators() -> None:
    """Load all Postgres GUC validators from YAML files.

    Recursively walk through ``available_parameters`` directory and load validators of each found YAML file into
    ``parameters`` and/or ``recovery_parameters`` variables.

    Walk through directories in top-down fashion and for each of them:
        * Sort files by name;
        * Load validators from YAML files that were found.

    Any problem faced while reading or parsing files will be logged as a ``WARNING`` by the child function, and the
    corresponding file or validator will be ignored.

    By default, Patroni only ships the file ``0_postgres.yml``, which contains Community Postgres GUCs validators, but
    that behavior can be extended. For example: if a vendor wants to add GUC validators to Patroni for covering a custom
    Postgres build, then they can create their custom YAML files under ``available_parameters`` directory.

    Each YAML file may contain either or both of these root attributes, here called sections:
        * ``parameters``: general GUCs that would be written to ``postgresql.conf``;
        * ``recovery_parameters``: recovery related GUCs that would be written to ``recovery.conf`` (Patroni later
            writes them to ``postgresql.conf`` if running PG 12 and above).

    Then, each of these sections, if specified, may contain one or more attributes with the following structure:

        * key: the name of a GUC;
        * value: a list of validators. Each item in the list must contain a ``type`` attribute, which must be one among:

            * ``Bool``; or
            * ``Integer``; or
            * ``Real``; or
            * ``Enum``; or
            * ``EnumBool``; or
            * ``String``.

            Besides the ``type`` attribute, it should also contain all the required attributes as per the corresponding
            class in this module.

    .. seealso::

        * :class:`Bool`;
        * :class:`Integer`;
        * :class:`Real`;
        * :class:`Enum`;
        * :class:`EnumBool`;
        * :class:`String`.

    :Example:

        This is a sample content for an YAML file based on Postgres GUCs, showing each of the supported types and
        sections:

            .. code-block:: yaml

                parameters:
                  archive_command:
                  - type: String
                    version_from: 90300
                    version_till: null
                  archive_mode:
                  - type: Bool
                    version_from: 90300
                    version_till: 90500
                  - type: EnumBool
                    version_from: 90500
                    version_till: null
                    possible_values:
                    - always
                  archive_timeout:
                  - type: Integer
                    version_from: 90300
                    version_till: null
                    min_val: 0
                    max_val: 1073741823
                    unit: s
                  autovacuum_vacuum_cost_delay:
                  - type: Integer
                    version_from: 90300
                    version_till: 120000
                    min_val: -1
                    max_val: 100
                    unit: ms
                  - type: Real
                    version_from: 120000
                    version_till: null
                    min_val: -1
                    max_val: 100
                    unit: ms
                  client_min_messages:
                  - type: Enum
                    version_from: 90300
                    version_till: null
                    possible_values:
                    - debug5
                    - debug4
                    - debug3
                    - debug2
                    - debug1
                    - log
                    - notice
                    - warning
                    - error
                recovery_parameters:
                  archive_cleanup_command:
                  - type: String
                    version_from: 90300
                    version_till: null

    """
    for file in get_validator_files():
        try:
            config: Dict[str, Any] = _read_postgres_gucs_validators_file(file)
        except InvalidGucValidatorsFile as exc:
            logger.warning(str(exc))
            continue

        logger.debug('Parsing validators from file `%s`.', file)

        mapping = {
            'parameters': parameters,
            'recovery_parameters': recovery_parameters,
        }

        for section in ['parameters', 'recovery_parameters']:
            section_var = mapping[section]

            config_section = config.get(section, {})
            for parameter in config_section.keys():
                section_var[parameter] = _get_postgres_guc_validators(config_section, parameter)


_load_postgres_gucs_validators()


def _transform_parameter_value(validators: MutableMapping[str, Tuple[_Transformable, ...]],
                               version: int, name: str, value: Any,
                               available_gucs: CaseInsensitiveSet) -> Optional[Any]:
    """Validate *value* of GUC *name* for Postgres *version* using defined *validators* and *available_gucs*.

    :param validators: a dictionary of all GUCs across all Postgres versions. Each key is the name of a Postgres GUC,
        and the corresponding value is a variable length tuple of :class:`_Transformable`. Each item is a validation
        rule for the GUC for a given range of Postgres versions. Should either contain recovery GUCs or general GUCs,
        not both.
    :param version: Postgres version to validate the GUC against.
    :param name: name of the Postgres GUC.
    :param value: value of the Postgres GUC.
    :param available_gucs: a set of all GUCs available in Postgres *version*. Each item is the name of a Postgres
        GUC. Used to avoid ignoring GUC *name* if it does not have a validator in *validators*, but is a valid GUC
        in Postgres *version*.

    :returns: the return value may be one among:

        * *value* transformed to the expected format for GUC *name* in Postgres *version*, if *name* has a validator
          in *validators* for the corresponding Postgres *version*; or
        * ``None`` if *name* does not have a validator in *validators* and is not present in *available_gucs*.
    """
    pass


def transform_postgresql_parameter_value(version: int, name: str, value: Any,
                                         available_gucs: CaseInsensitiveSet) -> Optional[Any]:
    """Validate *value* of GUC *name* for Postgres *version* using ``parameters`` and *available_gucs*.

    :param version: Postgres version to validate the GUC against.
    :param name: name of the Postgres GUC.
    :param value: value of the Postgres GUC.
    :param available_gucs: a set of all GUCs available in Postgres *version*. Each item is the name of a Postgres
        GUC. Used to avoid ignoring GUC *name* if it does not have a validator in ``parameters``, but is a valid GUC in
        Postgres *version*.

    :returns: The return value may be one among:

        * The original *value* if *name* seems to be an extension GUC (contains a period '.'); or
        * ``None`` if **name** is a recovery GUC; or
        * *value* transformed to the expected format for GUC *name* in Postgres *version* using validators defined in
            ``parameters``. Can also return ``None``. See :func:`_transform_parameter_value`.
    """
    pass


def transform_recovery_parameter_value(version: int, name: str, value: Any,
                                       available_gucs: CaseInsensitiveSet) -> Optional[Any]:
    """Validate *value* of GUC *name* for Postgres *version* using ``recovery_parameters`` and *available_gucs*.

    :param version: Postgres version to validate the recovery GUC against.
    :param name: name of the Postgres recovery GUC.
    :param value: value of the Postgres recovery GUC.
    :param available_gucs: a set of all GUCs available in Postgres *version*. Each item is the name of a Postgres
        GUC. Used to avoid ignoring GUC *name* if it does not have a validator in ``parameters``, but is a valid GUC in
        Postgres *version*.

    :returns: *value* transformed to the expected format for recovery GUC *name* in Postgres *version* using validators
        defined in ``recovery_parameters``. It can also return ``None``. See :func:`_transform_parameter_value`.
    """
    pass
