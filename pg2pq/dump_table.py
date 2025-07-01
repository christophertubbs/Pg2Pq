"""
Objects and functions used to dump a Postgresql table into a parquet file
"""
import os
import queue
import threading
import typing
import pathlib
import logging
import weakref
import signal
import atexit

from threading import Event
from queue import Queue

import psycopg
import sqlalchemy
import pyarrow

import pyarrow.parquet

from psycopg.sql import Composed as PostgresQuery

from pg2pq.models import DatabaseSpecification
from pg2pq.utilities import settings, ConflictResolution
from pg2pq.utilities.common import get_random_identifier
from pg2pq.utilities.constants import APPLICATION_NAME


LOGGER: logging.Logger = logging.getLogger(pathlib.Path(__file__).stem)

WRITERS: weakref.WeakValueDictionary[pathlib.Path, pyarrow.parquet.ParquetWriter] = weakref.WeakValueDictionary()
"""A mapping of open parquet writers. A reference from this dictionary won't keep the writer open and in memory"""
NOTIFICATION_FREQUENCY: int = 150
"""The number of queries to buffer in before sending some sort of notification to the user and logs"""
MAXIMUM_EXECUTION_ATTEMPTS: int = 5
"""The maximum number of attempts that something has before we really consider it a failure"""
COMPRESSION_ALGORITHM: str = "zstd"
"""The default algorithm to use when compressing parquet data"""
COMPRESSION_LEVEL: int = 5
"""
How compressed the data should be. The count generally goes up to around 22, starts at 0, but starts at 1 for zstd.

Levels 3-6 covers around 90-95% of max compression, so expect diminishing returns after.

The higher the value, the longer it will take to write, BUT the smaller the file and easier to distribute
"""

def close_writers(signum = None, frame = None):
    """
    Close any writers that have been recorded and are open
    :param signum:
    :param frame:
    """
    for path, writer in WRITERS.items():
        if isinstance(writer, pyarrow.parquet.ParquetWriter) and writer.is_open:
            try:
                writer.close()
            except:
                LOGGER.error(f"Could not close the parquet writer for {path}")

signal.signal(signal.SIGTERM, close_writers)
signal.signal(signal.SIGINT, close_writers)
signal.signal(signal.SIGHUP, close_writers)
atexit.register(close_writers)


def get_parquet_writer(
    target: pathlib.Path,
    schema: pyarrow.Schema,
    compression_algorithm: str = COMPRESSION_ALGORITHM,
    compression_level: int = COMPRESSION_LEVEL
) -> pyarrow.parquet.ParquetWriter:
    """
    Get a writer for a parquet file.

    This ensures that parquet writers are properly closed

    :param target: The parquet file to open
    :param schema: How to treat each column
    :param compression_algorithm: How to compress the data
    :param compression_level: To what degree to compress the data
    :return: A writer that will apply parquet data to disk
    """
    # Properly close a previously existing writer if it is found
    if target in WRITERS:
        previous_writer: pyarrow.parquet.ParquetWriter = WRITERS.pop(target)
        if previous_writer.is_open:
            try:
                previous_writer.close()
            except:
                LOGGER.error(
                    f"Tried to close a writer that was previously writing to {target} but could not",
                    exc_info=True
                )

    writer: pyarrow.parquet.ParquetWriter = pyarrow.parquet.ParquetWriter(
        where=target,
        schema=schema,
        compression=compression_algorithm,
        compression_level=compression_level
    )

    WRITERS[target] = writer

    return writer


def map_postgresql_type_to_arrow(dtype: sqlalchemy.types.TypeEngine) -> pyarrow.types.lib.DataType:
    """
    Determine the appropriate pyarrow data type for the given sqlalchemy type

    :param dtype: The type of column from sqlalchemy
    :return: An appropriate pyarrow data type that may be used to reflect the sqlalchemy type
    """
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
        inner_type: pyarrow.types.lib.DataType = map_postgresql_type_to_arrow(dtype.item_type)
        return pyarrow.list_(value_type=inner_type)

    if isinstance(dtype, (sqlalchemy.types.LargeBinary, postgresql.BYTEA)):
        return pyarrow.binary()

    # TODO: Find a good means to avoid hard coding precision and scale
    if isinstance(dtype, postgresql.NUMERIC):
        return pyarrow.decimal128(int_precision=dtype.precision, int_scale=dtype.scale)

    if isinstance(dtype, postgresql.UUID):
        return pyarrow.uuid()

    if isinstance(dtype, postgresql.JSON):
        return pyarrow.json_(pyarrow.utf8())

    string_types: typing.Tuple[typing.Type[sqlalchemy.types.TypeEngine], ...] = (
        postgresql.CHAR,
        postgresql.VARCHAR
    )

    if not isinstance(dtype, string_types):
        raise TypeError(
            f"Casting a {dtype} to a {pyarrow.string()} - there doesn't appear to be a cut and dry translation"
        )
    return pyarrow.string()


def get_unique_table_keys(
    table: sqlalchemy.Table
) -> typing.Sequence[typing.Sequence[str]]:
    """
    Get all keys that describe uniqueness for the given table

    :param table: The table whose keys we want to find
    :return: A collection of a series of column names that should be considered unique
    """
    unique_column_constraints: typing.Union[typing.Iterable, typing.Iterator] = filter(
        lambda constraint: isinstance(constraint, sqlalchemy.schema.ColumnCollectionConstraint),
        table.constraints
    )
    key_sets: typing.List[typing.Sequence[str]] = [
        tuple(key for key in column_constraints.columns.keys())
        for column_constraints in unique_column_constraints
        if len(column_constraints) > 0
    ]
    return key_sets

def get_table_schema(table: sqlalchemy.Table) -> pyarrow.Schema:
    """
    Create a schema for future parquet files based on sqlalchemy table metadata

    :param table: The table whose schema needs to be reflected
    :return: A pyarrow table schema that should accurately reflect the schema of the database table
    """
    schema_columns: typing.List[typing.Tuple[str, pyarrow.types.lib.DataType]] = []

    for column_name, column in table.columns.items():  # type: str, sqlalchemy.Column
        dtype: sqlalchemy.types.TypeEngine = column.type
        arrow_type: pyarrow.types.lib.DataType = map_postgresql_type_to_arrow(dtype=dtype)
        schema_columns.append((column_name, arrow_type))

    schema: pyarrow.Schema = pyarrow.schema(schema_columns)
    return schema


def log_schema(schema: pyarrow.Schema):
    """
    Output the pyarrow schema to the logs for later investigation

    :param schema: The pyarrow schema to log
    """
    tab_over: str = '    - '
    column_descriptions: typing.Iterable[str] = (
        f"{column_name}: {column_type}"
        for column_name, column_type in zip(schema.names, schema.types)
    )
    LOGGER.info(
        f"The schema for the data will be:{os.linesep}"
        f"{tab_over}{(os.linesep + tab_over).join(column_descriptions)}"
    )


def form_data_retrieval_query(
    schema_name: str,
    table_name: str,
    longest_key: typing.Optional[typing.Sequence[str]],
    preexisting_values: typing.Optional[typing.Mapping[str, typing.Any]]
) -> PostgresQuery:
    """
    Create a specially formatted postgresql query that will retrieve data from the database

    :param schema_name: The name of the schema that the table of interest exists in
    :param table_name: The name of the table to query
    :param longest_key: The longest key to use for ordering
    :param preexisting_values: A mapping of the largest values for the key in preexisting data
    :return: A query that may be called to retrieve data
    """
    from psycopg import sql
    query: PostgresQuery = sql.SQL("SELECT * FROM {}").format(sql.Identifier(schema_name, table_name))

    if preexisting_values:
        where_clause: PostgresQuery = sql.SQL(" WHERE ({}) > ({})").format(
            sql.SQL(", ").join(sql.Identifier(key) for key in preexisting_values),
            sql.SQL(", ").join(sql.Placeholder(key) for key in preexisting_values)
        )
        query = query + where_clause

        LOGGER.info(
            f"There is preexisting data, so only data that comes AFTER {preexisting_values} will be selected"
        )

    if longest_key:
        order_clause: PostgresQuery = sql.SQL(" ORDER BY ") + sql.SQL(", ").join(sql.Identifier(key) for key in longest_key)
        query = query + order_clause

    return query

def stream_batches(
    connection: psycopg.Connection,
    schema_name: str,
    table_name: str,
    buffer_size: int,
    max_execution_attempts: int = MAXIMUM_EXECUTION_ATTEMPTS,
    longest_key: typing.Sequence[str] = None,
    previous_values: typing.Dict[str, typing.Any] = None,
) -> typing.Generator[typing.Sequence[typing.Dict[str, typing.Any]], None, None]:
    """
    Create a generator that will pull in a limited amount of data from a postres table

    :param connection: An open connection to a postgres database
    :param schema_name: The schema that contains the table of itnerest
    :param table_name: The name of the table to dump
    :param buffer_size: The number of rows to load into memory at once
    :param max_execution_attempts: The maximum number of times to attempt the query before outright failing
    :param longest_key: Columns names to use for tests against uniqueness
    :param previous_values: A mapping for the last encountered values for each key if there is preexisting data
    :return: A generator that streams batchs of rows of data
    """
    if previous_values is None:
        previous_values = {}

    cursor_name: str = f"{APPLICATION_NAME}_{schema_name}.{table_name}_{get_random_identifier(4)}"
    """A unique name for the cursor - helps prevent clashing server side operations"""

    with connection.cursor(name=cursor_name) as cursor:
        query: PostgresQuery = form_data_retrieval_query(
            schema_name=schema_name,
            table_name=table_name,
            longest_key=longest_key,
            preexisting_values=previous_values,
        )

        execution_attempt: int = 0
        last_execution_error: typing.Optional[Exception] = None

        while execution_attempt <= max_execution_attempts:
            execution_attempt += 1
            try:
                cursor.execute(query, params=previous_values or None)
                last_execution_error = None
                break
            except Exception as e:
                last_execution_error = e

        if last_execution_error is not None:
            raise last_execution_error

        batch: typing.Sequence[typing.Dict[str, typing.Any]] = cursor.fetchmany(buffer_size)

        if len(batch) == 0:
            raise ValueError(
                f"There was no data to retrieve from '{schema_name}.{table_name}' - there is nothing to dump"
            )

        LOGGER.info(f"{len(batch):,} rows fetched in the first retrieval")

        while batch:
            # Yield 'returns' the batch when `next` is called. When `next` is called again, the function goes to the
            # end of the while block and 'returns' the next batch
            yield batch
            batch: typing.Sequence[typing.Dict[str, typing.Any]] = cursor.fetchmany(buffer_size)


def get_unqualified_host(url: typing.Union[str, sqlalchemy.engine.url.URL]) -> str:
    """
    Get the name of the host for identification purposes

    Examples:
        https://www.example.com -> example.com
        www.example.com -> example.com
        example.com -> example.com
        example.com/path/to/resource?param1=value -> example.com
        www.example.com:9090 -> example.com
        https://www.example.net:9090/path/to/resource -> example.net

    :param url: The url whose host to extract
    :return: The name of the host
    """
    if isinstance(url, sqlalchemy.engine.url.URL):
        return url.host

    if not isinstance(url, str):
        LOGGER.warning(
            f"Attempting to get the host name from '{url}' (type={type(url)}). "
            f"Expected either a sqlalchemy url or a string"
        )
        url = str(url)

    import re
    address_pattern: re.Pattern = re.compile(
        r"^(?P<protocol>[-.+a-zA-Z\d]+://)?"
        r"(www\.)?"
        r"(?P<credentials>[\w.]+(?P<password>:[-+%&*()[\];\"'<>,?~`\\|{}\w_.!#]+)?@)"
        r"(?P<host>[-a-zA-Z0-9.]+)(\.[-a-zA-Z0-9]+)?/?"
    )

    address_match: typing.Optional[re.Match] = address_pattern.match(url)

    if address_match is None:
        raise ValueError(f"A host name could not be found within: {url}")

    return address_match.group('host')

def get_most_recent_data(
    path: pathlib.Path,
    keys: typing.Optional[typing.Sequence[str]]
) -> typing.Optional[typing.Dict[str, typing.Any]]:
    """
    Get the most recent data that was saved to disk based on a series of keys. No preexisting values are reported if
    there aren't any keys to respect.

    :param path: The path to preexisting data
    :param keys: A list of the columns to use as indicators for preexisting data. Order matters -
    ('day_nu', 'location_id', 'month_nu') can yield WILDLY different results than ('location_id', 'month_nu', 'day_nu')
    :return: The most recent values for the keys in the order given
    """
    if not path.exists():
        return None
    if not keys:
        return None

    import duckdb

    query: str = f"""SELECT {', '.join(keys)}
FROM read_parquet('{str(path)}')
ORDER BY {', '.join(keys)}
LIMIT 1"""

    try:
        query_result: duckdb.DuckDBPyRelation = duckdb.sql(query)
    except duckdb.InvalidInputException as e:
        if 'magic bytes' in str(e).lower():
            checkpoint_path = path.parent / f"{path.stem}.checkpoint.parquet"

            query: str = f"""SELECT {', '.join(keys)}
        FROM read_parquet('{str(checkpoint_path)}')
        ORDER BY {', '.join(keys)}
        LIMIT 1"""
            query_result = duckdb.sql(query)
        else:
            raise
    records: typing.Sequence[typing.Dict[str, typing.Any]] = query_result.to_df().to_dict(orient="records")

    if records:
        return records[-1]

    return None


def merge_checkpoints(
    main_path: pathlib.Path,
    checkpoint_queue: Queue[pathlib.Path],
    may_continue: Event,
    compression_algorithm: str,
    compression_level: int,
    timeout: int = 2,
    backoff: int = 2
) -> None:
    """
    Merge checkpoint files

    :param main_path: The path to the primary checkpoint file
    :param checkpoint_queue: A queue that indicates files that need to be merged
    :param may_continue: An event that states that this function may continue polling if set
    :param compression_algorithm: How to compress the merged data
    :param compression_level: To what degree to compress the data
    :param timeout: The number of seconds to wait for a new file to merge before timing out. Setting the value too
    high will keep the application from exiting in a timely fashion.
    :param backoff: The amount of time to wait if an error was encountered before trying again
    """
    from time import sleep

    if main_path.is_dir():
        message: str = f"Cannot merge data from {main_path} - it is a directory, not a file"
        LOGGER.error(message)
        raise IsADirectoryError(message)

    if not may_continue.is_set():
        message: str = f"The 'merge_checkpoints' function could not run - the 'may_continue' event was not set"
        LOGGER.error(message)
        raise ValueError(message)

    from pg2pq.merge import merge_parquet

    # Try to find data that has not been merged yet
    if main_path.is_file():
        from glob import glob
        similar_path_pattern: str = str(main_path.parent / f"{main_path.stem}*")
        similar_paths: typing.List[pathlib.Path] = [
            pathlib.Path(match) for match in glob(similar_path_pattern)
            if pathlib.Path(match) != main_path
        ]
        if len(similar_paths) > 0:
            LOGGER.info(
                f"Found {len(similar_paths)} files that look ready to merge. "
                f"Merging those before listening for new data to merge"
            )
            if len(similar_paths) == 1:
                similar_paths = [main_path, similar_paths[0]]

            try:
                merge_parquet(
                    files_to_merge=similar_paths,
                    enforce_unique=True,
                    target_path=main_path,
                    compression_algorithm=compression_algorithm,
                    compression_level=compression_level,
                )

                for path in similar_paths:
                    path.unlink(missing_ok=True)
            except Exception as error:
                LOGGER.error(
                    f"Error encountered while trying to merge preexisting checkpoints: {error}",
                    exc_info=error
                )

    while may_continue.is_set():
        new_checkpoint_path: typing.Optional[pathlib.Path] = None
        try:
            new_checkpoint_path = checkpoint_queue.get(timeout=timeout)
        except queue.Empty:
            pass
        except TimeoutError:
            pass
        except Exception as e:
            LOGGER.error(f"Retrieving a new checkpoint failed: {e}", exc_info=True)

        if new_checkpoint_path is None:
            if may_continue.is_set():
                sleep(backoff)
                continue
            else:
                break

        LOGGER.info(f"Received the indicator that data from {new_checkpoint_path} needs to be merged in {main_path}")
        if main_path.is_file():
            merge_parquet(
                files_to_merge=[main_path, new_checkpoint_path],
                enforce_unique=True,
                target_path=main_path,
                compression_algorithm=compression_algorithm,
                compression_level=compression_level,
            )
        else:
            import shutil
            shutil.move(new_checkpoint_path, main_path)

        new_checkpoint_path.unlink(missing_ok=True)

        if may_continue.is_set():
            sleep(backoff)
        else:
            break


def dump_table(
    specification: DatabaseSpecification,
    schema_name: str,
    table_name: str,
    output_path: pathlib.Path,
    buffer_size: int = settings.buffer_size,
    conflict_resolution: ConflictResolution = ConflictResolution.ERROR,
    compression_algorithm: str = COMPRESSION_ALGORITHM,
    compression_level: int = COMPRESSION_LEVEL,
) -> None:
    """
    Dump a postgresql table to a parquet file

    :param specification:
    :param schema_name:
    :param table_name:
    :param output_path:
    :param buffer_size:
    :param conflict_resolution: How to handle preexisting data
    :param compression_algorithm: How to compress resultant parquet data
    :param compression_level: How intensely to compress resultant parquet data. ~5 is a good choice, diminishing returns after ~7
    """
    import psycopg
    from pyarrow import parquet
    import shutil

    if output_path.is_dir():
        output_path = output_path / f"{schema_name}.{table_name}.parquet"

    # Create data at a working location - this data will be purely additive and may contain duplicates.
    # This will be post processed and stored in the intended location later
    host: str = get_unqualified_host(specification.url)
    working_path: pathlib.Path = output_path.parent / host / f"{schema_name}.{table_name}.parquet"

    if conflict_resolution == ConflictResolution.ERROR and output_path.exists():
        raise FileExistsError(
            f"{output_path} already exists - delete it or change conflict resolution options when running "
            f"{APPLICATION_NAME}."
        )
    elif conflict_resolution == ConflictResolution.OVERWRITE and output_path.exists():
        output_path.unlink()
    elif output_path.exists():
        LOGGER.info(f"{output_path} already exists. Data will be appended to it")

    if conflict_resolution == ConflictResolution.ERROR and working_path.exists():
        raise FileExistsError(
            f"Partial processing of {schema_name}.{table_name} from {specification} has been detected. "
            f"Data cannot be overwritten or appended to without explicit approval with the conflict resolution parameter"
        )
    elif conflict_resolution == ConflictResolution.OVERWRITE and working_path.exists():
        working_path.unlink()

    working_path.parent.mkdir(parents=True, exist_ok=True)

    table_data: typing.Optional[sqlalchemy.Table] = specification.metadata.tables.get(table_name)

    if table_data is None:
        raise KeyError(f"No table named '{schema_name}.{table_name}' could be found in {specification}")

    schema: pyarrow.Schema = get_table_schema(table=table_data)
    keys: typing.Sequence[typing.Sequence[str]] = get_unique_table_keys(table=table_data)

    if keys:
        longest_key: typing.Optional[typing.Sequence[str]] = max(keys, key=len)
    else:
        longest_key = None

    most_recent_values: typing.Optional[typing.Dict[str, typing.Any]] = get_most_recent_data(
        path=working_path,
        keys=longest_key,
    )

    # Open up the connection and set the row factory as a list of dictionaries rather than a list of tuples
    from psycopg.rows import dict_row
    connection_arguments: typing.Mapping[str, typing.Any] = {
        "host": specification.host,
        "port": specification.port,
        "user": specification.username,
        "password": specification.password,
        "dbname": specification.name,
        "row_factory": dict_row,
    }

    with psycopg.connect(**connection_arguments) as connection:
        if COMPRESSION_LEVEL >= 9:
            LOGGER.warning(f"The compression level is set at {COMPRESSION_LEVEL}. Writing may be very slow")

        writer: pyarrow.parquet.ParquetWriter = get_parquet_writer(
            target=working_path,
            schema=schema,
            compression_algorithm=COMPRESSION_ALGORITHM,
            compression_level=COMPRESSION_LEVEL,
        )
        batch_generator: typing.Generator[typing.Sequence[typing.Dict[str, typing.Any]], None, None] = stream_batches(
            connection=connection,
            schema_name=schema_name,
            table_name=table_name,
            buffer_size=buffer_size,
            max_execution_attempts=MAXIMUM_EXECUTION_ATTEMPTS,
            longest_key=longest_key,
            previous_values=most_recent_values,
        )

        try:
            primary_checkpoint_path = working_path.parent / f"{working_path.stem}.checkpoint.parquet"
            may_continue: Event = Event()
            may_continue.set()
            checkpoint_queue: Queue[pathlib.Path] = Queue()

            merge_thread: threading.Thread = threading.Thread(
                target=merge_checkpoints,
                name=f"{host}.{schema_name}.{table_name}",
                kwargs={
                    "main_path": primary_checkpoint_path,
                    "checkpoint_queue": checkpoint_queue,
                    "may_continue": may_continue,
                    "compression_algorithm": compression_algorithm,
                    "compression_level": compression_level,
                }
            )
            merge_thread.start()

            for batch_index, batch in enumerate(batch_generator):
                table: pyarrow.Table = pyarrow.Table.from_pylist(batch, schema=schema)
                writer.write_table(table=table)

                if batch_index > 0 and batch_index % NOTIFICATION_FREQUENCY == 0:
                    LOGGER.info(f"An estimated {table.num_rows:,} rows have now been written to {working_path}")
                    checkpoint_path = working_path.parent / f"{working_path.stem}.checkpoint_{batch_index}.parquet"
                    LOGGER.info(f"Closing the writer and saving off a checkpoint of the data to {checkpoint_path}")
                    writer.close()
                    shutil.move(working_path, checkpoint_path)
                    checkpoint_queue.put(checkpoint_path)

                    writer = get_parquet_writer(
                        target=working_path,
                        schema=schema,
                        compression_algorithm=COMPRESSION_ALGORITHM,
                        compression_level=COMPRESSION_LEVEL,
                    )
        finally:
            may_continue.clear()

            try:
                if writer.is_open:
                    writer.close()
            except:
                LOGGER.error("Received and interrupt and could not close the writer", exc_info=True)

            try:
                if merge_thread.is_alive():
                    merge_thread.join(timeout=10)
            except Exception as e:
                LOGGER.error(f"Error encountered when joining the merge thread: {e}", exc_info=True)

    LOGGER.info("Now Processing saved data")
    post_process_dumped_data(working_path=working_path, final_destination=output_path, keys=keys)
    LOGGER.info(f"Data from {specification}:{schema_name}.{table_name} written to {output_path}")


def ensure_data_is_unique(
    path: pathlib.Path,
    keys: typing.Sequence[typing.Sequence[str]],
    compression_algorithm: str = COMPRESSION_ALGORITHM,
    compression_level: int = COMPRESSION_LEVEL,
) -> None:
    """
    Ensure that the data at the given path is unique to the given keys
    :param path: The path to the data to enforce uniqueness on
    :param keys: A series of keys that help indicate unique patterns within the data
    :param compression_algorithm: The compression algorithm to use
    :param compression_level: To what degree to compress the data. The higher the value, the more compressed the data is
    """
    import duckdb
    parquet_options: str = ', '.join([
        "FORMAT PARQUET",
        "OVERWRITE TRUE",
        "USE_TMP_FILE TRUE",
        f"COMPRESSION {compression_algorithm.upper()}",
        f"COMPRESSION_LEVEL {compression_level}",
    ])
    for key_set in keys:
        query: str = f"""COPY (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY {', '.join(key_set)}) AS ROW_NUM
        FROM read_parquet('{str(path)}')
    ) AS UNIQUE_DATA
    WHERE ROW_NUM = 1
    ORDER BY {', '.join(key_set)}
) TO '{path}' ({parquet_options});"""
        duckdb.sql(query)


def combine_checkpoints(
    working_path: pathlib.Path,
    compression_algorithm: str = COMPRESSION_ALGORITHM,
    compression_level: int = COMPRESSION_LEVEL,
):
    checkpoint_glob: str = str(working_path.parent / f"{working_path.stem}.checkpoint*")
    copy_options: str = ', '.join([
        "FORMAT PARQUET",
        "OVERWRITE TRUE",
        "USE_TMP_FILE TRUE",
        f"COMPRESSION {compression_algorithm.upper()}",
        f"COMPRESSION_LEVEL {compression_level}",
    ])
    query: str = f"""COPY (
    SELECT * FROM read_parquet('{checkpoint_glob}')    
) TO '{working_path}' ({copy_options});"""

def post_process_dumped_data(
    working_path: pathlib.Path,
    final_destination: pathlib.Path,
    keys: typing.Sequence[typing.Sequence[str]] = None,
    compression_algorithm: str = COMPRESSION_ALGORITHM,
    compression_level: int = COMPRESSION_LEVEL,
) -> None:
    """
    Perform post-processing tasks like deduplication

    :param working_path:
    :param final_destination:
    :param keys: Keys to deduplicate
    :param compression_algorithm:
    :param compression_level:
    """
    if isinstance(keys, typing.Iterable):
        ensure_data_is_unique(
            path=working_path,
            keys=keys,
            compression_algorithm=compression_algorithm,
            compression_level=compression_level,
        )

    combine_checkpoints(
        working_path=working_path,
        compression_algorithm=compression_algorithm,
        compression_level=compression_level,
    )

    import shutil
    shutil.move(working_path, final_destination)

    general_data_glob: str = str(working_path.parent / f"{working_path.stem}*")

    LOGGER.info(f"The final output has been written to {final_destination}")

    # Clean up the working data if it isn't the expected end product
    for path in pathlib.Path.cwd().glob(general_data_glob):
        if path.is_file() and final_destination.is_file() and path != final_destination:
            path.unlink()

    # Remove the working directory if it no longer has content
    if len(list(working_path.parent.glob("*"))) == 0:
        from shutil import rmtree
        rmtree(working_path.parent)

