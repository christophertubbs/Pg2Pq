"""
Global system of record for important applications settings

Similar to pg2pq.utilities.constants, but the values may change per environment
"""
import logging
import typing
import os
import pathlib

from pg2pq.utilities import constants
from pg2pq.utilities.constants import PREFIX


DEFAULT_DATABASE_HOST: str = os.environ.get(
    f"{PREFIX}_DATABASE_HOST",
    "localhost"
)
"""The default location of the database"""

DEFAULT_DATABASE_PORT: typing.Optional[int] = int(os.environ[f"{PREFIX}_DATABASE_PORT"]) if f"{PREFIX}_DATABASE_PORT" in os.environ else None
"""The default port number on which the database is being hosted"""

DEFAULT_DATABASE_DRIVER: str = os.environ.get(
    f"{PREFIX}_DATABASE_DRIVER",
    "psycopg"
)
"""The default setting for what type of database to use"""

DEFAULT_DATABASE_USER: typing.Optional[str] = os.environ.get(
    f"{PREFIX}_DATABASE_USER"
)
"""The default user for the database"""

DEFAULT_DATABASE_PASSWORD: typing.Optional[str] = os.environ.get(
    f"{PREFIX}_DATABASE_PASSWORD"
)
"""The default password for the database"""

DEFAULT_DATABASE_NAME: typing.Optional[str] = os.environ.get(
    f"{PREFIX}_DATABASE_NAME"
)
"""The default name of the database"""


class _Settings:
    """
    Application wide accessors for important settings

    Values may be set for this process and this process only - useful for testing
    """
    @property
    def default_database_password(self) -> typing.Optional[str]:
        """The default password for the database"""
        return os.environ.get(f"{PREFIX}_DATABASE_PASSWORD")

    @default_database_password.setter
    def default_database_password(self, value: str):
        os.environ[f"{PREFIX}_DATABASE_PASSWORD"] = value

    @property
    def default_database_name(self) -> typing.Optional[str]:
        """The default name of the database"""
        return os.environ.get(f'{PREFIX}_DATABASE_NAME', 'postgres')

    @default_database_name.setter
    def default_database_name(self, value: str):
        os.environ[f"{PREFIX}_DATABASE_NAME"] = value

    @property
    def default_database_user(self) -> typing.Optional[str]:
        """The default user for the database"""
        return os.environ.get(f"{PREFIX}_DATABASE_USER")

    @default_database_user.setter
    def default_database_user(self, value: str):
        os.environ[f'{PREFIX}_DATABASE_USER'] = value

    @property
    def default_database_driver(self) -> str:
        """The default setting for what type of database to use"""
        if f"{PREFIX}_DATABASE_DRIVER" in os.environ:
            driver: str = os.environ.get(f"{PREFIX}_DATABASE_DRIVER", "postgresql+psycopg")
            return driver
        return "psycopg"

    @default_database_driver.setter
    def default_database_driver(self, value: str):
        os.environ[f"{PREFIX}_DATABASE_DRIVER"] = value

    @property
    def default_database_host(self) -> str:
        """The default location of the database"""
        return os.environ.get(
            f"{PREFIX}_DATABASE_HOST",
            "localhost"
        )

    @default_database_host.setter
    def default_database_host(self, value: typing.Union[pathlib.Path, str, None]):
        os.environ[f"{PREFIX}_DATABASE_HOST"] = None if value is None else str(value)

    @property
    def default_database_port(self) -> int:
        """The default port number that the server should be accessible from"""
        return int(os.environ[f"{PREFIX}_DATABASE_PORT"]) if f"{PREFIX}_DATABASE_PORT" in os.environ else None

    @default_database_port.setter
    def default_database_port(self, value: typing.Union[int, str, None]):
        if isinstance(value, str) and not constants.INTEGER_PATTERN.match(value):
            raise TypeError(f"'{value}' is an invalid port number - it must be an integer.")
        os.environ[f"{PREFIX}_DATABASE_PORT"] = None if value is None else str(value)

    @property
    def default_log_level(self) -> int:
        """The default level at which log messages should be considered"""
        import re
        log_level: typing.Optional[str] = os.environ.get(f"{PREFIX}_LOG_LEVEL")

        if log_level is None:
            return logging.INFO

        matching_number: re.match = constants.NUMBER_PATTERN.match(string=log_level)
        if matching_number:
            return int(float(matching_number.groupdict()['number']))

        return logging.getLevelName(log_level)

    @default_log_level.setter
    def default_log_level(self, value: typing.Union[int, str]):
        if isinstance(value, int):
            value = logging.getLevelName(level=value)
        os.environ[f"{PREFIX}_LOG_LEVEL"] = value

    @property
    def buffer_size(self) -> int:
        return int(os.environ.get(f"{PREFIX}_BUFFER_SIZE", constants.DEFAULT_BUFFER_SIZE))

    @buffer_size.setter
    def buffer_size(self, value: typing.Union[int, str]):
        os.environ[f"{PREFIX}_BUFFER_SIZE"] = value

    @property
    def debug(self) -> bool:
        """
        Whether the application should run in debug mode

        May show a lot of data you do NOT want users to see. Use sparingly
        """
        return os.environ.get(f"{PREFIX}__DEBUG", "False").lower() in ["true", "t", "y", "yes", "o", "on", "1"]

    @debug.setter
    def debug(self, value: bool):
        os.environ[f"{PREFIX}__DEBUG"] = str(value)

    def to_dict(self) -> typing.Dict[str, typing.Any]:
        """
        Convert all the values into a dictionary
        """
        import inspect
        properties: typing.Dict[str, property] = dict(inspect.getmembers(
            self,
            predicate=lambda member: isinstance(member, property))
        )
        values: typing.Dict[str, typing.Any] = {
            property_name: prop.fget(self)
            for property_name, prop in properties.items()
        }
        return values


settings: _Settings = _Settings()
