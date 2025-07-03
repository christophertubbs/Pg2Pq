#!/usr/bin/env python3
"""
Functions and objects used to alter the source postgres database
"""
import argparse
import os
import typing
import logging
import pathlib
import dataclasses

from pg2pq.models import DatabaseSpecification

LOGGER: logging.Logger = logging.getLogger(pathlib.Path(__file__).stem)

_MAXIMUM_RETRIES: int = 5


if __name__.endswith("__main__"):
    import dotenv
    dotenv.load_dotenv()

@dataclasses.dataclass
class AlterResult:
    """
    Information about the result of a table alteration
    """
    name: str
    """The table being altered"""
    column: str
    """The column being altered"""
    succeeded: bool
    """Whether the operation was a success"""
    attempts: int
    """The number of attempts made"""
    message: typing.Optional[str] = dataclasses.field(default=None)
    """An associated message describing the effort"""

    def __str__(self):
        if self.succeeded:
            return f"Altered {self.name}::{self.column} in {self.attempts} attempts{'. ' + self.message if self.message else ''}"
        return f"Unable to alter {self.name}::{self.column} after {self.attempts} attempts{'. ' + self.message if self.message else ''}"

def alter_table(
    database_specification: DatabaseSpecification,
    fully_qualified_name: str,
    column_name: str,
    new_type: str,
    maximum_retries: int = _MAXIMUM_RETRIES,
) -> AlterResult:
    """
    Update the table with the fully-qualified name

    :param database_specification:
    :param fully_qualified_name:
    :param column_name:
    :param new_type:
    :param maximum_retries: The maximum number of times to attempt to alter the table before quitting
    """
    LOGGER.info(f"Converting {fully_qualified_name}::{column_name} to a {new_type}")

    import psycopg
    from psycopg import sql

    query: sql.Composable = sql.SQL(
        "ALTER TABLE {table_name} ALTER COLUMN {column_name} SET DATA TYPE {data_type}"
    ).format(
        table_name=sql.SQL(fully_qualified_name),
        column_name=sql.SQL(column_name),
        data_type=sql.SQL(new_type)
    )

    attempts: int = 0
    succeeded: bool = False
    last_exception: typing.Optional[Exception] = None
    while attempts < _MAXIMUM_RETRIES and not succeeded:
        attempts += 1
        try:
            with psycopg.connect(**database_specification.psycopg_kwargs) as connection:  # type: psycopg.Connection
                connection.execute(query)
            succeeded = True
            break
        except Exception as e:
            if attempts < maximum_retries:
                LOGGER.warning(f"{e}. Trying again.")
            else:
                LOGGER.error(f"The following query could not be executed: {query.as_string()}")
            last_exception = e

    result: AlterResult = AlterResult(
        name=fully_qualified_name,
        column=column_name,
        succeeded=succeeded,
        attempts=attempts,
        message=str(last_exception) if isinstance(last_exception, Exception) else None
    )

    return result


def alter_source(
    database_specification: DatabaseSpecification,
    schema_name: str,
    table_name: str,
    new_type: str,
    column_names: typing.Sequence[str],
    max_workers: int = os.cpu_count(),
    show_progress: bool = True
) -> bool:
    from concurrent import futures

    alteration_arguments: typing.Sequence[typing.Dict[str, typing.Any]] = [
        {
            "database_specification": database_specification,
            "fully_qualified_name": f"{schema_name}.{table_name}",
            "column_name": column_name,
            "new_type": new_type,
        }
        for column_name in column_names
    ]

    errors: typing.List[AlterResult] = []
    successes: typing.List[AlterResult] = []

    with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_results: typing.Sequence[futures.Future[AlterResult]] = [
            executor.submit(alter_table, **kwargs)
            for kwargs in alteration_arguments
        ]

        if show_progress:
            from tqdm import tqdm
            progress_bar = tqdm(
                futures.as_completed(future_results), total=len(future_results), desc="Altering Columns"
            )
            for step in progress_bar:
                try:
                    result: AlterResult = step.result()
                    if result.succeeded:
                        successes.append(result)
                    else:
                        errors.append(result)
                except Exception as exception:
                    LOGGER.error(f"Table alteration failed: {exception}", exc_info=exception)
        else:
            completed, incomplete = futures.wait(future_results, return_when=futures.FIRST_EXCEPTION)  # type: typing.Sequence[AlterResult], typing.Sequence[futures.Future]

            successes.extend(
                completed_alteration
                for completed_alteration in completed
                if completed_alteration.succeeded
            )

            errors.extend(
                completed_alteration
                for completed_alteration in completed
                if not completed_alteration.succeeded
            )

            if incomplete:
                LOGGER.error(f"{len(incomplete)} tables were not altered")

    for alteration in (successes + errors):
        if alteration.succeeded:
            LOGGER.info(alteration)
        else:
            LOGGER.error(alteration)

    return len(errors) == 0 and len(successes) > 0

def get_parser() -> argparse.ArgumentParser:
    from pg2pq.command_arguments import AlterSourceArguments

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Alter Columns on a postgres table and/or its partitions"
    )

    AlterSourceArguments.add_to_parser(parser=parser)

    return parser

def main() -> int:
    from pg2pq.command_arguments import AlterSourceArguments

    parser: argparse.ArgumentParser = get_parser()
    raw_arguments: argparse.Namespace = parser.parse_args()

    arguments: AlterSourceArguments = AlterSourceArguments(**vars(raw_arguments))
    try:
        alter_source(
            database_specification=arguments.database_specification,
            schema_name=arguments.schema_name,
            table_name=arguments.table_name,
            new_type=arguments.new_type,
            column_names=arguments.column_names
        )
    except Exception as e:
        LOGGER.error(e, exc_info=e)
        return 1
    return 0

if __name__ == "__main__":
    from pg2pq.utilities import constants
    logging.basicConfig(
        level=logging.INFO,
        format=constants.LOG_FORMAT,
        datefmt=constants.DATE_FORMAT,
    )
    import sys
    sys.exit(main())
