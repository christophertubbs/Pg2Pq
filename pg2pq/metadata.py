"""
Functions and objects used to get metadata about a Postgres Database or Table
"""
import typing
import logging
import warnings
import pathlib

import sqlalchemy
import sqlalchemy.exc

LOGGER: logging.Logger = logging.getLogger(pathlib.Path(__file__).stem)

def get_table(
    table_name: str,
    database_information: sqlalchemy.MetaData,
    schema: str = None
) -> sqlalchemy.Table:
    """
    Find an indicated table

    Args:
        table_name (): The name of the table
        database_information (): Information about the current database
        schema (): The schema to look within

    Returns:
        The found table
    """
    possible_tables: typing.List[sqlalchemy.Table] = [
        table
        for table in database_information.tables.values()
        if table.name == table_name
           and (
            schema is None or table.schema == schema
           )
    ]

    if len(possible_tables) == 0:
        full_table_name: str = f"{'' if schema is None else schema + '.'}{table_name}"
        raise KeyError(f"No '{full_table_name}' table was found in the database")

    table: sqlalchemy.Table = possible_tables[0]

    return table

def get_metadata(engine: sqlalchemy.engine.Engine, metadata: sqlalchemy.MetaData = None) -> sqlalchemy.MetaData:
    """
    Update the metadata about a database, suppressing false warnings, such as issues with Postgis types

    Args:
        engine (): The engine controlling access to the database
        metadata (): information about the database

    Returns:
        The updated metadata
    """
    if metadata is None:
        metadata = sqlalchemy.MetaData()

    with warnings.catch_warnings(action="ignore", category=sqlalchemy.exc.SAWarning):
        from pg2pq.utilities import constants
        from time import sleep

        creation_attempts: int = 0
        reflected: bool = False
        last_error: typing.Optional[Exception] = None

        while not reflected and creation_attempts < constants.LONG_TERM_BACKOFF_ATTEMPTS:
            creation_attempts += 1
            try:
                metadata.reflect(bind=engine)
                reflected = True
                break
            except sqlalchemy.exc.OperationalError as e:
                if "server closed the connection unexpectedly" not in str(e):
                    raise
                if creation_attempts >= constants.LONG_TERM_BACKOFF_ATTEMPTS:
                    raise
                last_error = e
                LOGGER.error(f"{e}: Waiting {constants.LONG_TERM_BACKOFF} and trying again...")

                # Reset the connection pool to clear out any possibly dead/stale connections
                engine.dispose()
                sleep(constants.LONG_TERM_BACKOFF.total_seconds())

        if not reflected:
            message = f"Could not load data from {engine}"

            if last_error:
                raise Exception(message) from last_error
            else:
                raise Exception(message)

        return metadata
