"""
Defines structures used to provide application commands arguments required for functionality
"""
import typing
import argparse
import pathlib
import dataclasses
import logging

from pg2pq.utilities import settings, ConflictResolution
from pg2pq.utilities import constants
from pg2pq.models import DatabaseSpecification

LOGGER: logging.Logger = logging.getLogger(pathlib.Path(__file__).stem)


@typing.runtime_checkable
class ApplicationArgsProtocol(typing.Protocol):
    """
    Mimics a class that provides concrete arguments to application commands
    """
    @classmethod
    def add_argument_type(cls, command: str, argument_type: typing.Type["BaseArguments"]):
        """Add argument classes to the application arguments"""


@dataclasses.dataclass
class BaseArguments:
    """
    A generalized base class for operational arguments with the capacity to help build an argument parser
    """
    debug: bool = dataclasses.field(
        default=False,
        kw_only=True,
        metadata={
            "flags": [
                "--debug"
            ],
            "action": "store_true",
            "description": "Run in debug mode",
        }
    )
    """Run in debug mode"""

    def __post_init__(self):
        fields_with_fallbacks: typing.Iterable[dataclasses.Field] = filter(
            lambda argument_field: "fallback_default" in argument_field.metadata,
            dataclasses.fields(self.__class__)
        )
        for field_with_fallback in fields_with_fallbacks:
            field_value = getattr(self, field_with_fallback.name, constants.SENTINEL)
            fallback = field_with_fallback.metadata['fallback_default']

            if field_value == constants.SENTINEL:
                raise KeyError(
                    f"Cannot access the '{field_with_fallback.name}' attribute on {self.__class__.__qualname__}"
                )

            unset_value = field_with_fallback.metadata.get("unset_value", None)
            if field_value == unset_value:
                if callable(fallback):
                    setattr(self, field_with_fallback.name, fallback())
                else:
                    setattr(self, field_with_fallback.name, fallback)

        if self.debug:
            logging.warning(
                "Running in debug mode - important information may get leaked into logs. "
                "Disable immediately if not intended."
            )
            logging.getLogger().setLevel(logging.DEBUG)
            settings.debug = True

    @classmethod
    def get_help(cls) -> typing.Optional[str]:
        """Get the help string that will appear next to this set of arguments in the parser"""
        return None

    @classmethod
    def get_description(cls) -> typing.Optional[str]:
        """Get a description of what this set of arguments does"""
        return None

    @classmethod
    def get_command(cls) -> typing.Optional[str]:
        """Get the command used to invoke these arguments"""
        return None

    @classmethod
    def add_to_parser(cls, parser: typing.Union[argparse.ArgumentParser, argparse._SubParsersAction]) -> None:
        """
        Attach this set of arguments to the argument parser

        Args:
            parser (): The argument parser to attach these arguments to
        """
        if cls.get_command() and isinstance(parser, argparse._SubParsersAction):
            parser_to_add_to = parser.add_parser(
                name=cls.get_command(),
                help=cls.get_help(),
                description=cls.get_description(),
            )
        elif isinstance(parser, argparse._SubParsersAction):
            raise TypeError(
                f"Cannot add a {cls.__qualname__} to a sub parser collection - it does not have a specialized command"
            )
        else:
            parser_to_add_to = parser

        arguments_to_add = dataclasses.fields(cls)

        for argument_to_add in arguments_to_add:  # type: dataclasses.Field
            field_metadata: typing.Mapping[str, typing.Any] = argument_to_add.metadata

            argument_args: typing.Dict[str, typing.Any] = {
                "help": field_metadata.get("description"),
            }

            if 'choices' in field_metadata:
                argument_args['choices'] = field_metadata['choices']

            if "action" in field_metadata:
                argument_args['action'] = field_metadata['action']
            elif "type" in field_metadata:
                argument_args['type'] = field_metadata['type']
            else:
                field_types = list(typing.get_args(argument_to_add.type))
                if type(None) in field_types:
                    field_types.remove(type(None))

                field_type = field_types[0] if field_types else argument_to_add.type

                if isinstance(field_type, str):
                    type_path: typing.List[str] = field_type.split(".")
                    search_domain = {**globals()}
                    builtins = search_domain['__builtins__']
                    search_domain.update(vars(builtins) if hasattr(builtins, '__dict__') else builtins)
                    for path in type_path:
                        found_element = (
                            search_domain if isinstance(search_domain, typing.Mapping) else vars(search_domain)).get(
                            path
                            )
                        if found_element is None:
                            search_domain = None
                            break
                        search_domain = found_element
                    field_type = search_domain

                if field_type:
                    argument_args['type'] = field_type

            default_value = dataclasses.MISSING

            if argument_to_add.default != dataclasses.MISSING:
                default_value = argument_to_add.default
            elif argument_to_add.default_factory != dataclasses.MISSING:
                default_value = argument_to_add.default_factory()
            elif argument_args.get('action'):
                if argument_args['action'] == "store_true":
                    default_value = False
                elif argument_args['action'] == "store_false":
                    default_value = True
                elif argument_args['action'] in ("append", "extend"):
                    default_value = []
                elif argument_args['action'] == "count":
                    default_value = 0
                elif argument_args['action'] != "store":
                    raise ValueError(f"The argument action of '{argument_args['action']}' is not supported yet")

            supports_multiple_values: bool = issubclass(
                typing.get_origin(argument_args.get("type")) or type(None),
                typing.Iterable
            ) or field_metadata.get("is_list", False) or field_metadata.get("nargs", None) in ("+", "*")

            if default_value == dataclasses.MISSING:
                if supports_multiple_values:
                    argument_args['nargs'] = "+"
                parser_to_add_to.add_argument(
                    argument_to_add.name,
                    **argument_args
                )
            else:
                if supports_multiple_values:
                    argument_args['nargs'] = "*"

                if 'flags' in field_metadata:
                    flags = field_metadata['flags']
                else:
                    flags = [f"--{argument_to_add.name.replace('-', '_')}"]

                argument_args['dest'] = argument_to_add.name
                argument_args['default'] = default_value
                try:
                    parser_to_add_to.add_argument(
                        *flags,
                        **argument_args
                    )
                except Exception as e:
                    logging.error(f"Could not add '{argument_args}' to '{parser_to_add_to}': {e}")
                    raise

ArgumentType = typing.TypeVar("ArgumentType", bound=BaseArguments, covariant=True)
"""A generic stand-in for different types of arguments that will drive operation"""


@dataclasses.dataclass(kw_only=True)
class ArgumentsForDatabase(BaseArguments):
    """
    A base class that defines parameters required for database access
    """
    database_host: typing.Optional[str] = dataclasses.field(
        default_factory=lambda: settings.default_database_host,
        metadata={
            "flags": [
                "--database-host"
            ],
            "description": "The location of the database",
            "type": str
        }
    )
    """The location of the database. Access this information through the attached `database_specification` instead."""
    database_port: typing.Optional[int] = dataclasses.field(
        default_factory=lambda: settings.default_database_port,
        metadata={
            "flags": [
                "--database-port"
            ],
            "description": "The port the database is being served on on the host",
            "type": int
        }
    )
    """
    The port the database is being served on on the host. 
    Access this information through the attached `database_specification` instead.
    """
    database_name: typing.Optional[str] = dataclasses.field(
        default_factory=lambda: settings.default_database_name,
        metadata={
            "flags": [
                "--database-name"
            ],
            "description": "The name of the database within the database server to use",
            "type": str
        }
    )
    """
    The name of the database within the database server to use.
    Access this information through the attached `database_specification` instead.
    """
    database_user: str = dataclasses.field(
        default_factory=lambda: settings.default_database_user,
        metadata={
            "flags": [
                "--database-user"
            ],
            "description": "The name of the user to log into the database as",
            "type": str
        }
    )
    """
    The name of the user to log into the database as
    Access this information through the attached `database_specification` instead.
    """
    database_driver: str = dataclasses.field(
        default_factory=lambda: settings.default_database_driver,
        metadata={
            "flags": [
                "--database-driver"
            ],
            "description": "The name of the database driver to use",
            "type": str,
        }
    )
    """
    The name of the database driver to use
    Access this information through the attached `database_specification` instead.
    """
    database_password: typing.Optional[str] = dataclasses.field(
        default=None,
        metadata={
            "flags": [
                "--database-password"
            ],
            "description": "The password used to enter the database",
            "optional": True,
            "type": str,
            "fallback_default": lambda: settings.default_database_password
        }
    )
    """
    The password used to enter the database
    Access this information through the attached `database_specification` instead.
    """

    @property
    def database_specification(self) -> DatabaseSpecification:
        """
        Details on how to reach to specified database
        """
        return DatabaseSpecification(
            host=self.database_host,
            port=self.database_port,
            name=self.database_name,
            username=self.database_user,
            password=self.database_password,
            driver=self.database_driver
        )


@dataclasses.dataclass
class DatabaseDumpArguments(ArgumentsForDatabase):
    """
    Dump a table from the database
    """
    @classmethod
    def get_command(cls) -> str:
        return "dump"

    table_name: str = dataclasses.field(
        metadata={
            "description": "The name of the table to dump"
        }
    )

    output_path: pathlib.Path = dataclasses.field(
        metadata={
            "description": "Where to put the dumped data"
        }
    )

    schema_name: str = dataclasses.field(
        default="public",
        metadata={
            "flags": [
                "-s",
                "--schema-name"
            ],
            "type": str,
            "description": "The schema that contains the table"
        }
    )

    buffer_size: int = dataclasses.field(
        default=settings.buffer_size,
        metadata={
            "flags": [
                "-b",
                "--buffer-size"
            ],
            "type": int,
            "description": "The number of rows to load at once when reading from the table"
        }
    )

    conflict_resolution: ConflictResolution = dataclasses.field(
        default=ConflictResolution.ERROR,
        metadata={
            "flags": [
                "-c",
                "--on-conflict"
            ],
            "type": ConflictResolution,
            "description": "What to do if there is preexisting data",
            "choices": [member.value for member in ConflictResolution]
        }
    )

@dataclasses.dataclass
class AlterSourceArguments(ArgumentsForDatabase):
    """
    Alter details about the source table
    """
    @classmethod
    def get_command(cls) -> str:
        return "alter-source"

    schema_name: str = dataclasses.field(
        metadata={
            "description": "The name of the schema that contains the table"
        }
    )

    table_name: str = dataclasses.field(
        metadata={
            "description": "The name of the table to alter"
        }
    )

    new_type: str = dataclasses.field(
        metadata={
            "description": "The name of the new type for the column"
        }
    )

    column_names: typing.List[str] = dataclasses.field(
        metadata={
            "description": "The names of the columns to update",
            "nargs": "+"
        }
    )


@dataclasses.dataclass
class GenerateEnvironmentArgs(BaseArguments):
    """
    Generate environment variables
    """
    @classmethod
    def get_command(cls) -> str:
        return "generate-env"

    output_path: pathlib.Path = dataclasses.field(
        default=constants.ROOT_DIRECTORY / ".env",
        metadata={
            "description": "Where to put the generated environment file",
            "type": pathlib.Path,
            "flags": [
                "-o",
                "--output-path"
            ]
        }
    )


@dataclasses.dataclass
class MergeArgs(BaseArguments):
    """
    Merge parquet files
    """
    @classmethod
    def get_command(cls) -> str:
        return "merge"

    output_path: pathlib.Path = dataclasses.field(
        metadata={
            "type": pathlib.Path,
            "description": "Where to save the generated data"
        }
    )

    input_files: typing.List[pathlib.Path] = dataclasses.field(
        metadata={
            "type": pathlib.Path,
            "nargs": "+",
            "description": "The files to include"
        }
    )

    compression_algorithm: str = dataclasses.field(
        default="zstd",
        metadata={
            "type": str,
            "description": "The compression algorithm to use when saving data",
        }
    )

    compression_level: int = dataclasses.field(
        default=5,
        metadata={
            "type": int,
            "description": (
                "How compressed the output should be - the higher the number, the more compressed with reduced "
                "returns after ~10"
            )
        }
    )

    enforce_unique: bool = dataclasses.field(
        default=False,
        metadata={
            "flags": [
                "-u",
                "--enforce-unique",
            ],
            "action": "store_true",
            "description": "Ensure that all combined rows are unique"
        }
    )

    keys: typing.List[str] = dataclasses.field(
        default_factory=list,
        metadata={
            "flags": [
                "-k",
                "--keys",
            ],
            "description": (
                "Keys to partition on while enforcing unique keys. "
                "Failure to provide this will result is a massive memory spike"
            ),
            "nargs": "*"
        }
    )


@dataclasses.dataclass
class ToNetcdfArgs(BaseArguments):
    """
    Arguments that instruct the system to convert parquet data into netcdf
    """
    @classmethod
    def get_command(cls) -> str:
        return "to-netcdf"

    dimensions: typing.List[str] = dataclasses.field(
        metadata={
            "nargs": "+",
            "type": str,
            "description": "What columns to use as dimensions"
        }
    )

    target_parquet: pathlib.Path = dataclasses.field(
        metadata={
            "type": pathlib.Path,
            "description": "The path to the parquet file to convert"
        }
    )

    output_path: pathlib.Path = dataclasses.field(
        metadata={
            "type": pathlib.Path,
            "description": "Where to save the resultant netcdf data"
        }
    )

    exclude: typing.List[str] = dataclasses.field(
        default_factory=list,
        metadata={
            "flags": [
                "-e",
                "--exclude"
            ],
            "type": str,
            "nargs": "*",
            "description": "Columns to ignore when converting to netcdf"
        }
    )

def register_argument(argument_class: typing.Type[BaseArguments], command: typing.Optional[str] = None):
    """
    Attach the given argument to an ApplicationArgsProtocol implementation

    Args:
        argument_class (): The argument class to add
        command (): The command that will be associated with the arguments

    Returns:
        A decorated version of the ApplicationArgsProtocol implementation that will consider the given argument class
    """
    if not isinstance(argument_class, type):
        raise TypeError(f"Only types may be registered as arguments - received: {argument_class}")

    if not issubclass(argument_class, BaseArguments):
        raise TypeError(f"Only action arguments may be added as arguments. Received: {argument_class.__qualname__}")

    if command is None:
        command = argument_class.get_command()

    if not command:
        raise ValueError(
            f"Arguments may only be registered as subcommands, but '{argument_class.__qualname__}' "
            f"didn't have an associated command"
        )

    def decorator(cls: typing.Type[ApplicationArgsProtocol]) -> typing.Type[ApplicationArgsProtocol]:
        """
        Add the argument type to this ApplicationArgsProtocol implementation

        Args:
            cls (): The ApplicationArgsProtocol object to modify

        Returns:
            The updated class
        """
        if not isinstance(cls, type):
            raise TypeError(f"'register_argument' may only modify classes - received a {type(cls)}")

        if not issubclass(cls, ApplicationArgsProtocol):
            if hasattr(cls, "__mro__"):
                import inspect
                mro_entries: typing.List[str] = list(map(
                    lambda mro_entry: mro_entry.__name__,
                    inspect.getmro(cls)
                ))
                cls_definition = f"{mro_entries[0]}({', '.join(mro_entries[1:])})"
            else:
                cls_definition = repr(cls)

            raise TypeError(
                f"'register_argument' may only modify Application Arguments - "
                f"attempting to modify manipulate a {cls_definition}"
            )

        cls.add_argument_type(command=command, argument_type=argument_class)
        return cls

    return decorator
