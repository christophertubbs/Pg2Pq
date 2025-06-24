"""
Functions and objects used to represent entities within the database of choice
"""
import typing
import logging
import pathlib
import warnings

import pydantic
import sqlalchemy
import sqlalchemy.exc

from pg2pq.utilities import constants
from pg2pq.utilities import settings

LOGGER: logging.Logger = logging.getLogger(pathlib.Path(__file__).stem)

class DatabaseSpecification(pydantic.BaseModel):
    """
    Details on how to reach a database

    Defaults to sqlite, but supports postgres
    """
    host: str = pydantic.Field(
        default=settings.default_database_host,
    )
    """The address to the database to connect to"""
    driver: str = pydantic.Field(
        default=settings.default_database_driver,
    )
    """The driver to use to connect to the database"""
    port: typing.Optional[int] = pydantic.Field(
        default=settings.default_database_port,
    )
    """The port on the database host to connect to"""
    username: typing.Optional[str] = pydantic.Field(default=settings.default_database_user)
    """The username to connect as"""
    password: typing.Optional[str] = pydantic.Field(default=settings.default_database_password)
    """The password for the database server - intentionally not kept in-app"""
    name: typing.Optional[str] = pydantic.Field(default=settings.default_database_name)
    """The name of the database to connect to"""
    _engine: sqlalchemy.engine.Engine = pydantic.PrivateAttr(default=None)

    @pydantic.field_validator("driver", mode="before")
    @classmethod
    def _set_driver(cls, driver: str = None) -> str:
        if isinstance(driver, str):
            return driver

        if driver is None:
            return settings.default_database_driver

        raise ValueError(f"'{driver}' is not a valid database driver")

    @property
    def url(self) -> sqlalchemy.URL:
        """
        Get a SQLAlchemy URL for specification
        """
        url: sqlalchemy.URL = sqlalchemy.URL.create(
            drivername=self.driver,
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.name
        )

        return url

    @property
    def engine(self) -> sqlalchemy.Engine:
        """
        Get a sqlalchemy database engine
        """
        url = self.url
        if self._engine is None:
            try:
                self._engine = sqlalchemy.create_engine(url)
            except BaseException as e:
                LOGGER.error("A database engine could not be created")
                raise
        return self._engine

    @property
    def metadata(self) -> sqlalchemy.MetaData:
        """
        Get a SQLAlchemy database metadata object
        """
        with warnings.catch_warnings(action="ignore", category=sqlalchemy.exc.SAWarning):
            metadata: sqlalchemy.MetaData = sqlalchemy.MetaData()

            from time import sleep

            creation_attempts: int = 0
            reflected: bool = False

            while not reflected and creation_attempts < constants.LONG_TERM_BACKOFF_ATTEMPTS:
                creation_attempts += 1
                try:
                    metadata.reflect(bind=self.engine)
                    reflected = True
                    break
                except sqlalchemy.exc.OperationalError as e:
                    if "server closed the connection unexpectedly" not in str(e):
                        raise
                    if creation_attempts >= constants.LONG_TERM_BACKOFF_ATTEMPTS:
                        raise

                    import logging
                    logging.error(
                        f"{e.orig if e.orig else e}: Waiting {constants.LONG_TERM_BACKOFF} and trying again..."
                    )

                    # Reset the connection pool to clear out any possibly dead/stale connections
                    self.engine.dispose()
                    sleep(constants.LONG_TERM_BACKOFF.total_seconds())

            if not reflected:
                raise Exception(f"Could not load data from {self}")

            return metadata

    @property
    def connection(self) -> sqlalchemy.Connection:
        """
        Get a connection to the database
        """
        return self.engine.connect()

    @property
    def raw_connection(self) -> sqlalchemy.PoolProxiedConnection:
        """
        A raw connection that may be used to connect to the database
        :return:
        """
        connection = self.connection
        return connection.connection


    def __str__(self):
        return f"{self.driver}://{self.host}{':' + str(self.port) if self.port else ''}{'/' + self.name if self.name else ''}"
