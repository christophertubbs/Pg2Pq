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
    def __init__(self):
        self.__setter_handlers: typing.Dict[str, typing.List[typing.Callable[[typing.Any], typing.Any]]] = {}

    def add_setting_handler(self, setting_name: str, handler: typing.Callable[[typing.Any], typing.Any]) -> None:
        self.__setter_handlers.setdefault(setting_name, []).append(handler)

    def _handle_setting_change(self, setting_name: str, new_value: typing.Any) -> None:
        exceptions: typing.List[Exception] = []

        for handler in self.__setter_handlers.get(setting_name, []):
            try:
                handler(new_value)
            except Exception as e:
                exceptions.append(e)

        if exceptions:
            raise ExceptionGroup(f"Errors were encountered when updating the '{setting_name}' setting", exceptions)

    @property
    def default_database_password(self) -> typing.Optional[str]:
        """The default password for the database"""
        return os.environ.get(f"{PREFIX}_DATABASE_PASSWORD")

    @default_database_password.setter
    def default_database_password(self, value: str):
        os.environ[f"{PREFIX}_DATABASE_PASSWORD"] = value

        self._handle_setting_change("default_database_password", value)

    @property
    def default_database_name(self) -> typing.Optional[str]:
        """The default name of the database"""
        return os.environ.get(f'{PREFIX}_DATABASE_NAME', 'postgres')

    @default_database_name.setter
    def default_database_name(self, value: str):
        os.environ[f"{PREFIX}_DATABASE_NAME"] = value
        self._handle_setting_change("default_database_name", value)

    @property
    def default_database_user(self) -> typing.Optional[str]:
        """The default user for the database"""
        return os.environ.get(f"{PREFIX}_DATABASE_USER")

    @default_database_user.setter
    def default_database_user(self, value: str):
        os.environ[f'{PREFIX}_DATABASE_USER'] = value
        self._handle_setting_change("default_database_user", value)

    @property
    def default_database_driver(self) -> str:
        """The default setting for what type of database to use"""
        if f"{PREFIX}_DATABASE_DRIVER" not in os.environ:
            try:
                import psycopg
                os.environ[f"{PREFIX}_DATABASE_DRIVER"] = "postgresql+psycopg"
            except ImportError:
                try:
                    import psycopg2
                    os.environ[f"{PREFIX}_DATABASE_DRIVER"] = "postgresql+psycopg2"
                except ImportError:
                    try:
                        import pg8000
                        os.environ[f"{PREFIX}_DATABASE_DRIVER"] = "postgresql+pg8000"
                    except ImportError as import_error:
                        raise ImportError(
                            "Could not find a postgres driver. Please install `psycopg`",
                            name=import_error.name,
                            path=import_error.path
                        ) from import_error

        driver: str = os.environ.get(f"{PREFIX}_DATABASE_DRIVER")
        return driver

    @default_database_driver.setter
    def default_database_driver(self, value: str):
        os.environ[f"{PREFIX}_DATABASE_DRIVER"] = value
        self._handle_setting_change("default_database_driver", value)

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
        self._handle_setting_change("default_database_host", value)

    @property
    def default_database_port(self) -> int:
        """The default port number that the server should be accessible from"""
        return int(os.environ[f"{PREFIX}_DATABASE_PORT"]) if f"{PREFIX}_DATABASE_PORT" in os.environ else None

    @default_database_port.setter
    def default_database_port(self, value: typing.Union[int, str, None]):
        if isinstance(value, str) and not constants.INTEGER_PATTERN.match(value):
            raise TypeError(f"'{value}' is an invalid port number - it must be an integer.")
        os.environ[f"{PREFIX}_DATABASE_PORT"] = None if value is None else str(value)
        self._handle_setting_change("default_database_port", value)

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
        self._handle_setting_change("default_log_level", value)

    @property
    def buffer_size(self) -> int:
        return int(os.environ.get(f"{PREFIX}_BUFFER_SIZE", constants.DEFAULT_BUFFER_SIZE))

    @buffer_size.setter
    def buffer_size(self, value: typing.Union[int, str]):
        os.environ[f"{PREFIX}_BUFFER_SIZE"] = value
        self._handle_setting_change("buffer_size", value)

    @property
    def timezone(self) -> str:
        from tzlocal import get_localzone_name
        return os.environ.get(f"{PREFIX}_TIMEZONE", get_localzone_name())

    @timezone.setter
    def timezone(self, value: str):
        os.environ[f"{PREFIX}_TIMEZONE"] = value
        self._handle_setting_change("timezone", value)

    @property
    def debug(self) -> bool:
        """
        Whether the application should run in debug mode

        May show a lot of data you do NOT want users to see. Use sparingly
        """
        return os.environ.get(f"{PREFIX}__DEBUG", "False").lower() in [True, "true", "t", "y", "yes", "o", "on", "1"]

    @debug.setter
    def debug(self, value: bool):
        os.environ[f"{PREFIX}__DEBUG"] = str(value)
        self._handle_setting_change("debug", value)

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
