"""
Generate Retrospective statistics
"""
from __future__ import annotations

import typing
import logging
import sys

from pg2pq.utilities import common

if __name__.endswith("__main__"):
    import dotenv
    dotenv.load_dotenv(verbose=True)
    common.prepare_logs()

LOGGER: logging.Logger = common.get_logger(__file__)

from pg2pq.arguments import GenerateEnvironmentArgs
from pg2pq.arguments import DatabaseDumpArguments
from pg2pq.arguments import ArgumentType
from pg2pq.arguments import ApplicationArguments
from pg2pq.arguments import BaseArguments

def dump_database_table(arguments: DatabaseDumpArguments):
    """
    Dump a table to a parquet file

    :param arguments:
    :return:
    """
    from pg2pq.dump_table import dump_table
    dump_table(
        specification=arguments.database_specification,
        schema_name=arguments.schema_name,
        table_name=arguments.table_name,
        output_path=arguments.output_path,
        buffer_size=arguments.buffer_size,
        conflict_resolution=arguments.conflict_resolution,
    )


def generate_env(arguments: GenerateEnvironmentArgs):
    """
    Generate a .env file

    :param arguments:
    :return:
    """
    from pg2pq import env_generator
    env_generator.generate_env_file(output_path=arguments.output_path)


def get_action_routing_table() -> typing.Mapping[str, typing.Callable[[ArgumentType], typing.Any]]:
    """
    Create a mapping of application commands to their handlers

    Returns:
        A mapping between application commands and their handlers
    """
    table: typing.Dict[str, typing.Callable[[ArgumentType], typing.Any]] = {
        DatabaseDumpArguments.get_command(): dump_database_table,
        GenerateEnvironmentArgs.get_command(): generate_env,
    }
    return table

def main() -> int:
    """The entrypoint function"""
    try:
        arguments: ApplicationArguments = ApplicationArguments()
    except Exception as e:
        print(e, file=sys.stderr)
        print()
        return 2

    routing_table: typing.Mapping[str, typing.Callable[[BaseArguments], typing.Any]] = get_action_routing_table()
    handler: typing.Callable[[BaseArguments], typing.Any] = routing_table.get(arguments.selected_command)

    if handler is None:
        LOGGER.error(f"There was no handler for the '{arguments.selected_command}' command")
        return 1

    try:
        handler(arguments.values)
    except Exception as e:
        LOGGER.error(f"Failed to perform '{arguments.selected_command}' due to: {e}", exc_info=True)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
