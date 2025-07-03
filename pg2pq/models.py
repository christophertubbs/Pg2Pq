"""
Functions and objects used to represent entities within the database of choice
"""
import typing
import logging
import pathlib
import warnings

import pydantic

if typing.TYPE_CHECKING:
    import sqlalchemy
    import pyarrow
    import psycopg

from pg2pq.utilities import constants
from pg2pq.utilities import settings

LOGGER: logging.Logger = logging.getLogger(pathlib.Path(__file__).stem)

VariableParameters = typing.ParamSpec("VariableParameters")
TypeConstructor = typing.Callable[[VariableParameters], "pyarrow.lib.DataType"]

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
    _engine: typing.Optional["sqlalchemy.engine.Engine"] = pydantic.PrivateAttr(default=None)
    _connection: typing.Optional["psycopg.Connection"] = pydantic.PrivateAttr(default=None)

    @pydantic.field_validator("driver", mode="before")
    @classmethod
    def _set_driver(cls, driver: str = None) -> str:
        if isinstance(driver, str):
            return driver

        if driver is None:
            return settings.default_database_driver

        raise ValueError(f"'{driver}' is not a valid database driver")

    @property
    def url(self) -> "sqlalchemy.URL":
        """
        Get a SQLAlchemy URL for specification
        """
        import sqlalchemy
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
    def engine(self) -> "sqlalchemy.Engine":
        """
        Get a sqlalchemy database engine
        """
        import sqlalchemy
        url = self.url
        if self._engine is None:
            try:
                self._engine = sqlalchemy.create_engine(url)
            except BaseException as e:
                LOGGER.error("A database engine could not be created")
                raise
        return self._engine

    @property
    def metadata(self) -> "sqlalchemy.MetaData":
        """
        Get a SQLAlchemy database metadata object
        """
        import sqlalchemy
        import sqlalchemy.exc
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
    def connection(self) -> "psycopg.Connection":
        """
        Get a connection to the database
        """
        import psycopg
        if self._connection is None:
            self._connection = psycopg.connect(**self.psycopg_kwargs)
        return self._connection

    @property
    def psycopg_kwargs(self) -> typing.Dict[str, typing.Any]:
        return {
            "dbname": self.name,
            "user": self.username,
            "password": self.password,
            "host": self.host,
            "port": self.port,
        }


    def __str__(self):
        return f"{self.driver}://{self.host}{':' + str(self.port) if self.port else ''}{'/' + self.name if self.name else ''}"

def get_type_mapping() -> typing.Dict[str, TypeConstructor]:
    import pyarrow
    return {
        "binary": pyarrow.binary,
        "bool": pyarrow.bool_,
        "boolean": pyarrow.bool_,
        "date32": pyarrow.date32,
        "date64": pyarrow.date64,
        "date": pyarrow.date32,
        "decimal32": pyarrow.decimal32,
        "decimal64": pyarrow.decimal64,
        "decimal128": pyarrow.decimal128,
        "decimal256": pyarrow.decimal256,
        "duration": pyarrow.duration,
        "float16": pyarrow.float16,
        "float32": pyarrow.float32,
        "float": pyarrow.float32,
        "float64": pyarrow.float64,
        "int8": pyarrow.int8,
        "byte": pyarrow.int8,
        "int16": pyarrow.int16,
        "short": pyarrow.int16,
        "int32": pyarrow.int32,
        "int": pyarrow.int32,
        "integer": pyarrow.int32,
        "int64": pyarrow.int64,
        "long": pyarrow.int64,
        "uint8": pyarrow.uint8,
        "uint16": pyarrow.uint16,
        "uint32": pyarrow.uint32,
        "uint64": pyarrow.uint64,
        "json": pyarrow.json_,
        "large_binary": pyarrow.large_binary,
        "large_string": pyarrow.large_string,
        "string": pyarrow.string,
        "time": pyarrow.time32,
        "time32": pyarrow.time32,
        "time64": pyarrow.time64,
        "timestamp": pyarrow.timestamp,
        "datetime": pyarrow.timestamp,
    }

def get_pyarrow_datatype(name: str, *args, **kwargs) -> "pyarrow.lib.DataType":
    key: str = name.lower()
    type_constructor: typing.Optional[TypeConstructor] = get_type_mapping().get(key, None)
    if type_constructor is None:
        raise KeyError(f"'{name}' is not a valid data type for a parquet schema")
    return type_constructor(*args, **kwargs)


class ParquetColumn(pydantic.BaseModel):
    """
    Represents the means to create a column for a pyarrow schema for a parquet file
    """
    def to_field(self) -> "pyarrow.Field":
        import pyarrow
        try:
            field: pyarrow.Field = pyarrow.field(
                name=self.name,
                type=get_pyarrow_datatype(self.datatype, **self.type_arguments),
                nullable=self.nullable,
                metadata=self.metadata
            )
        except Exception as e:
            LOGGER.error(f"Could not form the '{self.name}::{self.datatype}' field due to: {e}", exc_info=True)
            raise
        return field

    name: str
    datatype: str
    type_arguments: typing.Dict[str, typing.Any] = pydantic.Field(default_factory=dict)
    nullable: bool = pydantic.Field(default=True)
    metadata: typing.Optional[typing.Dict[str, typing.Any]] = pydantic.Field(default=None)

class ParquetSchema(pydantic.BaseModel):
    """
    Represents the means to create a pyarrow schema for a parquet file
    """
    @classmethod
    def read_file(cls, path: typing.Union[str, pathlib.Path]) -> "ParquetSchema":
        """
        Load the parquet schema from a json file

        :param path:
        :return:
        """
        if isinstance(path, bytes):
            path = path.decode()

        if isinstance(path, str):
            path = pathlib.Path(path)

        if not isinstance(path, pathlib.Path):
            raise TypeError(
                f"Cannot read parquet schema from {path} (type={type(type)}) - it must be a string or a pathlib.Path"
            )

        import json
        raw_data = json.loads(path.read_text())
        return cls(**raw_data)

    def to_schema(self) -> "pyarrow.Schema":
        import pyarrow
        schema: pyarrow.Schema = pyarrow.schema(
            fields=[field.to_field() for field in self.fields],
            metadata=self.metadata
        )
        return schema

    fields: typing.List[ParquetColumn]
    metadata: typing.Optional[typing.Dict[str, typing.Any]] = pydantic.Field(default=None)
