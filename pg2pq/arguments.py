"""
Arguments used to control direct Retrostat functionality
"""
from __future__ import annotations

import os
import typing
import argparse

from pg2pq.command_arguments import register_argument
from pg2pq.command_arguments import BaseArguments
from pg2pq.command_arguments import GenerateEnvironmentArgs
from pg2pq.command_arguments import MergeArgs
from pg2pq.command_arguments import DatabaseDumpArguments
from pg2pq.command_arguments import ArgumentType

@register_argument(DatabaseDumpArguments)
@register_argument(GenerateEnvironmentArgs)
@register_argument(MergeArgs)
class ApplicationArguments:
    """
    Provides a parser for all commands that may be performed via a CLI
    """
    _argument_types: typing.Dict[str, typing.Type[BaseArguments]] = {}
    @classmethod
    def add_argument_type(cls, command: str, argument_type: typing.Type[BaseArguments]):
        if not isinstance(argument_type, type):
            raise TypeError(f"Only class types may be added to a '{cls.__qualname__}. Received: {argument_type}")

        if not issubclass(argument_type, BaseArguments):
            raise TypeError(
                f"Cannot add an '{argument_type}' to a '{cls.__qualname__}' - it must be a descendent of "
                f"'{BaseArguments.__qualname__}'"
            )

        if argument_type != BaseArguments and command not in cls._argument_types:
            cls._argument_types[command] = argument_type

    def __init__(self, *args):
        self.__arguments: typing.Optional[BaseArguments] = None
        self.selected_command: str = ""
        self.__parse(args=args)

    @property
    def values(self) -> ArgumentType:
        """
        The typed parameters passed in from the CLI
        """
        return self.__arguments

    def __parse(self, args: typing.Sequence[str]):
        """
        Parse CLI parameters to direct operation
        Args:
            args (): Parameters that a user or script passed into this
        """
        parser: argparse.ArgumentParser = argparse.ArgumentParser(
            description="Prepare and Generate daily statistics on retrospective NWM data"
        )

        if self._argument_types:
            subcommands: argparse._SubParsersAction = parser.add_subparsers(dest="command")
            for command, argument_type in self._argument_types.items():
                subcommand: argparse.ArgumentParser = subcommands.add_parser(
                    name=command,
                    help=argument_type.get_help() or argument_type.__doc__,
                    description=argument_type.get_description() or argument_type.__doc__
                )
                argument_type.add_to_parser(parser=subcommand)

        parsed_values: typing.Dict[str, typing.Any] = vars(parser.parse_args(args=args) if args else parser.parse_args())

        command: str = parsed_values.pop("command")
        self.selected_command = command
        selected_argument_type: typing.Type[BaseArguments] = self._argument_types.get(command)

        if selected_argument_type is None:
            raise ValueError(f"No command was passed{os.linesep}{parser.format_help()}")

        arguments = selected_argument_type(**parsed_values)
        self.__arguments = arguments
