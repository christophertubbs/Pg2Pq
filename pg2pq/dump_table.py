"""
Objects and functions used to dump a Postgresql table into a parquet file
"""
import os
import typing
import pathlib
import logging

import sqlalchemy
import pyarrow

from pg2pq.models import DatabaseSpecification
from pg2pq.utilities import settings, ConflictResolution
from pg2pq.utilities.common import get_random_identifier
from pg2pq.utilities.constants import APPLICATION_NAME


LOGGER: logging.Logger = logging.getLogger(pathlib.Path(__file__).stem)


NOTIFICATION_FREQUENCY: int = 200


def map_sqlalchemy_type_to_arrow(dtype: sqlalchemy.types.TypeEngine) -> pyarrow.types.lib.DataType:
    from sqlalchemy.dialects import postgresql
    if isinstance(dtype, (postgresql.BOOLEAN, postgresql.BIT)):
        return pyarrow.bool_()

    if isinstance(dtype, postgresql.BIGINT):
        return pyarrow.int64()
    if isinstance(dtype, postgresql.INTEGER):
        return pyarrow.int32()
    if isinstance(dtype, postgresql.SMALLINT):
        return pyarrow.int16()

    if isinstance(dtype, (postgresql.FLOAT, postgresql.DOUBLE_PRECISION)):
        return pyarrow.float64()
    if isinstance(dtype, postgresql.REAL):
        return pyarrow.float32()
    if isinstance(dtype, postgresql.DATE):
        return pyarrow.date32()
    if isinstance(dtype, (postgresql.TIMESTAMP, sqlalchemy.types.DATETIME, postgresql.TIME)):
        return pyarrow.timestamp("us")

    if isinstance(dtype, postgresql.ARRAY):
        inner_type: pyarrow.types.lib.DataType = map_sqlalchemy_type_to_arrow(dtype.item_type)
        return pyarrow.list_(value_type=inner_type)

    if isinstance(dtype, (sqlalchemy.types.LargeBinary, postgresql.BYTEA)):
        return pyarrow.binary()

    if isinstance(dtype, postgresql.NUMERIC):
        return pyarrow.decimal128(int_precision=38, int_scale=10)

    string_types: typing.Tuple[typing.Type[sqlalchemy.types.TypeEngine], ...] = (
        postgresql.CHAR,
        postgresql.VARCHAR
    )

    if not isinstance(dtype, string_types):
        LOGGER.warning(
            f"Casting a {dtype} to a {pyarrow.string()} - there doesn't appear to be a cut and dry translation"
        )
    return pyarrow.string()


def dump_table(
    specification: DatabaseSpecification,
    schema_name: str,
    table_name: str,
    output_path: pathlib.Path,
    buffer_size: int = settings.buffer_size,
    conflict_resolution: ConflictResolution = ConflictResolution.ERROR
) -> None:
    """
    Dump a postgresql table to a parquet file

    :param specification:
    :param schema_name:
    :param table_name:
    :param output_path:
    :param buffer_size:
    :param conflict_resolution: How to handle preexisting data
    """
    import psycopg
    from pyarrow import parquet
    connection: typing.Optional[psycopg.connection] = None

    if output_path.is_dir():
        output_path = output_path / f"{schema_name}.{table_name}.parquet"

    if conflict_resolution == ConflictResolution.ERROR and output_path.exists():
        raise FileExistsError(
            f"{output_path} already exists - delete it or change conflict resolution options when running "
            f"{APPLICATION_NAME}."
        )
    elif conflict_resolution == ConflictResolution.OVERWRITE and output_path.exists():
        output_path.unlink()

    try:
        existing_keys: typing.Set[typing.Tuple] = set()
        key_columns: typing.List[typing.Sequence[str]] = []
        table_data: typing.Optional[sqlalchemy.Table] = specification.metadata.tables.get(table_name)

        if table_data is None:
            raise KeyError(f"No table named '{schema_name}.{table_name}' could be found in {specification}")

        for constraint in table_data.constraints:
            if isinstance(constraint, (sqlalchemy.UniqueConstraint, sqlalchemy.PrimaryKeyConstraint)) and len(constraint.columns) > 0:
                key_columns.append(tuple(key for key in constraint.columns.keys()))

        if output_path.exists() and key_columns:
            Now load up all unique values?
            pass

        schema_columns: typing.List[typing.Tuple[str, pyarrow.types.lib.DataType]] = []
        for column_name, column in table_data.columns.items():  # type: str, sqlalchemy.Column
            dtype: sqlalchemy.types.TypeEngine = column.type
            arrow_type: pyarrow.types.lib.DataType = map_sqlalchemy_type_to_arrow(dtype=dtype)
            schema_columns.append((column_name, arrow_type))

        schema: pyarrow.Schema = pyarrow.schema(fields=schema_columns)

        LOGGER.info(f"The schema for the data will be:{os.linesep}{schema}")

        from psycopg.rows import dict_row
        connection: psycopg.Connection = psycopg.connect(
            host=specification.host,
            port=specification.port,
            user=specification.username,
            password=specification.password,
            dbname=specification.name,
            row_factory=dict_row,
        )
        with connection.cursor(name=f"dump_{schema_name}.{table_name}_{get_random_identifier(4)}") as cursor:
            import psycopg.sql
            query = psycopg.sql.SQL(
                "SELECT * FROM {}"
            ).format(psycopg.sql.Identifier(schema_name, table_name))

            cursor.execute(query)

            retrieval_count: int = 0
            batch: typing.Sequence[typing.Dict[str, typing.Any]] = cursor.fetchmany(buffer_size)
            LOGGER.info(f"{len(batch)} rows fetched in the first retrieval")

            while batch:
                retrieval_count += 1
                arrow_table: pyarrow.Table = pyarrow.Table.from_pylist(batch, schema=schema)

                if not output_path.parent.exists():
                    output_path.parent.mkdir(parents=True, exist_ok=True)

                parquet.write_table(
                    arrow_table,
                    str(output_path)
                )

                if retrieval_count % NOTIFICATION_FREQUENCY == 0:
                    LOGGER.info(
                        f"Wrote {retrieval_count} records to {output_path}, for an estimated total of {retrieval_count * buffer_size} records"
                    )
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                LOGGER.error(f"Could not close the raw connection used to connect to {specification}", exc_info=True)
