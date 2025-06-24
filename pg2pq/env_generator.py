#!/usr/bin/env python3
"""
A basic CLI program used to generate .env files
"""
import typing
import pathlib
import os
import dataclasses

from getpass import getpass

from pg2pq.utilities import constants
from pg2pq.utilities import settings


_DEFAULT_ENV_PATH: pathlib.Path = constants.ROOT_DIRECTORY / ".env"

@dataclasses.dataclass
class ConfigurableVariable:
    """
    Represents an environment variable that may be configured
    """
    name: str
    """The name of the variable"""
    default: typing.Any
    """The default value of the variable"""
    description: str
    """What the variable controls"""
    hide_input: bool = dataclasses.field(default=False)
    """Whether the user MUST supply a value"""

_ENVIRONMENT_VARIABLES: typing.Sequence[ConfigurableVariable] = [
    ConfigurableVariable(
        name=f"{constants.PREFIX}_DATABASE_HOST",
        default=settings.default_database_host,
        description="The URL of the Postgresql database to connect to"
    ),
    ConfigurableVariable(
        name=f"{constants.PREFIX}_DATABASE_PORT",
        default=settings.default_database_port,
        description="The port of the Postgresql database to connect to"
    ),
    ConfigurableVariable(
        name=f"{constants.PREFIX}_DATABASE_NAME",
        default=settings.default_database_name,
        description="The name of the Postgresql database to connect to"
    ),
    ConfigurableVariable(
        name=f"{constants.PREFIX}_DATABASE_USER",
        default=settings.default_database_user,
        description="The username of the Postgresql database to connect to"
    ),
    ConfigurableVariable(
        name=f"{constants.PREFIX}_DATABASE_PASSWORD",
        default=settings.default_database_password,
        description="The password of the Postgresql database to connect to",
        hide_input=True
    ),
    ConfigurableVariable(
        name=f"{constants.PREFIX}_DATABASE_DRIVER",
        default=settings.default_database_driver,
        description="The python driver used to connect tot he database"
    ),
    ConfigurableVariable(
        name=f"{constants.PREFIX}_LOG_LEVEL",
        default=settings.default_log_level,
        description="The logging level to use"
    ),
]


def format_prompt(variable: ConfigurableVariable) -> str:
    prompt = f"""{variable.name}:
{variable.description}
"""
    if variable.default:
        prompt += f"""Default: {variable.default}
"""
    return prompt


def prompt_for_environment_variables() -> typing.Dict[str, str]:
    """
    Prompt the user to input values for a list of environment variable keys.

    Returns:
        Dict[str, str]: A dictionary of key-value pairs entered by the user.
    """
    environment_variable_values: typing.Dict[str, str] = {}

    for configurable_variable in _ENVIRONMENT_VARIABLES:
        prompt: str = format_prompt(configurable_variable)

        if configurable_variable.hide_input:
            user_input: str = getpass(prompt=prompt).strip()
        else:
            user_input: str = input(prompt).strip()

        if not user_input and configurable_variable.default:
            user_input = configurable_variable.default
        elif not user_input:
            raise ValueError(f"A non-empty value must be provided for {configurable_variable.name}")

        environment_variable_values[configurable_variable.name] = user_input

    return environment_variable_values


def user_affirmed(user_input: str) -> typing.Optional[bool]:
    """
    Detects if what the user typed counts as 'yes' or 'no'. A null response means that user input was invalid

    Args:
        user_input: What the user typed

    Returns:
        if what the user typed counts as 'yes' or 'no'. A null response means that user input was invalid
    """
    if not user_input:
        return None

    user_input = user_input.lower().strip()

    if user_input in ["yes", "y", 't', 'true', '1']:
        return True

    if user_input in ["no", "n", 'f', 'false', '0']:
        return False

    return None


def write_env_file(
    environment_variable_dictionary: typing.Dict[str, str],
    env_file_path: typing.Union[str, pathlib.Path] = _DEFAULT_ENV_PATH
) -> None:
    """
    Write the given environment variable dictionary to a .env file.

    Args:
        environment_variable_dictionary (Dict[str, str]): Dictionary of environment variables.
        env_file_path (str): Path to the .env file. Defaults to ".env".
    """
    if isinstance(env_file_path, str):
        env_file_path: pathlib.Path = pathlib.Path(env_file_path)

    env_file_path = env_file_path.expanduser().absolute()

    env_file_path.parent.mkdir(parents=True, exist_ok=True)

    if env_file_path.exists():
        check_attempt: int = 0
        max_attempts: int = 5
        affirmation: typing.Optional[bool] = None
        while affirmation is None and check_attempt < max_attempts:
            check_attempt += 1
            answer: str = input(f"There is already an environment file at '{env_file_path}'. Overwrite it? [y/n]: ")
            affirmation = user_affirmed(answer)
            if affirmation is None and check_attempt < max_attempts:
                print(f"'{answer}' is not a valid answer - try again.")

        if affirmation is None:
            raise FileExistsError(
                f"The file at '{env_file_path}' already exists, but you did not give a clear answer as to "
                f"whether to overwrite it."
            )
        if not affirmation:
            print(f"Preexisting environment variables won't be overwritten")
            return

    values: str = os.linesep.join([
        f"{key}={value}"
        for key, value in environment_variable_dictionary.items()
    ])

    env_file_path.write_text(values)

    print(f"{os.linesep}✅  .env file generated at: {env_file_path}")


def generate_env_file(output_path: pathlib.Path) -> None:
    print(f"🔧 Environment Variable Generator{os.linesep}")
    environment_variable_data: typing.Dict[str, str] = prompt_for_environment_variables()
    write_env_file(environment_variable_data, env_file_path=output_path)

def main() -> None:
    """
    Entry point for the environment variable collection tool.
    """
    generate_env_file(output_path=_DEFAULT_ENV_PATH)


if __name__ == "__main__":
    main()
