"""
Common functions and structures that may be reused
"""
import typing
import logging
import logging.config
import re
import pathlib
import zoneinfo

from datetime import datetime

from pg2pq.utilities import constants


class InfoFilter(logging.Filter):
    def filter(self, record):
        return record.levelno < logging.ERROR

class ErrorFilter(logging.Filter):
    def filter(self, record):
        return record.levelno >= logging.ERROR

class TimeZoneFormatter(logging.Formatter):
    """
    A log formatter that outputs log times in whatever timezone has been configured
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from pg2pq.utilities import settings
        self.timezone: zoneinfo.ZoneInfo = zoneinfo.ZoneInfo(settings.timezone)
        settings.add_setting_handler(
            "timezone",
            self.set_timezone
        )

    def set_timezone(self, timezone_name: str) -> None:
        self.timezone = zoneinfo.ZoneInfo(timezone_name)

    def formatTime(self, record: logging.LogRecord, datefmt: str = None):
        converted_datetime: datetime = datetime.fromtimestamp(record.created, tz=self.timezone)

        if datefmt:
            return converted_datetime.strftime(datefmt)

        return converted_datetime.strftime(constants.PRECISE_DATE_FORMAT)


def now() -> datetime:
    """
    Get the current time with the intended timezone
    """
    from pg2pq.utilities import settings
    return datetime.now(tz=zoneinfo.ZoneInfo(settings.timezone))


def create_exception_group(
    message: str,
    exceptions: typing.Union[typing.Iterable[Exception], typing.Iterator[Exception]]
) -> typing.Union[ExceptionGroup, Exception]:
    """
    Create an exceptiongroup and deduplicate errors

    Args:
        message: The message to provide
        exceptions: The errors encountered

    Returns:
        An exception group with deduplicated errors or just an exception if there is only one
    """
    unique_errors: typing.Dict[typing.Tuple[type, str], Exception] = {}

    for exception in exceptions:
        key: typing.Tuple[type, str] = type(exception), str(exception)
        if key not in unique_errors:
            unique_errors[key] = exception

    error_list: typing.List[Exception] = list(unique_errors.values())
    if len(error_list) == 0:
        raise ValueError(f"Cannot create an exception group for when there are no errors")
    if len(error_list) == 1:
        return error_list[0]
    return ExceptionGroup(message, error_list)

def prepare_logs():
    """
    Prepare logs for recording

    Args:
        level: The logging level that the root logger will use
    """
    from pg2pq.utilities.constants import LOG_CONFIGURATION_PATH
    from pg2pq.utilities.templating import load_parse_and_replace_json
    log_configuration = load_parse_and_replace_json(path=LOG_CONFIGURATION_PATH)
    logging.config.dictConfig(config=log_configuration)

def prepare_short_term_logs(level: int = None):
    """
    Prepare logging that won't store things for a long period of time

    Args:
        level: The logging level that the root logger will use
    """
    from .constants import PRECISE_DATE_FORMAT
    from .constants import LOG_FORMAT

    if level is None:
        level = logging.INFO

    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        datefmt=PRECISE_DATE_FORMAT,
    )

def get_logger(filename: typing.Union[str, pathlib.Path], level: int = None) -> logging.Logger:
    """
    Get a logger with the correct log level

    Getting the logger this way allows us to provide loggers with levels that differ from dependent libraries

    Args:
        filename ():
        level ():

    Returns:

    """
    if level is None:
        from pg2pq.utilities import settings
        level = get_log_level(settings.default_log_level)

    logger: logging.Logger = logging.getLogger(pathlib.Path(filename).stem)
    logger.setLevel(level)

    return logger

def get_log_level(level: typing.Union[int, str, bytes]) -> int:
    """
    Get the level to log to

    Needed because the closest function within the logging module won't always return an int

    Args:
        level: A representation of the logger

    Returns:
        A valid integer matching the desired log level
    """
    if isinstance(level, int):
        return level

    if isinstance(level, bytes):
        level = level.decode()

    if not isinstance(level, str):
        raise TypeError(f"Cannot determine the log level - it must be a string or int")

    if re.match("^\d+$", level):
        return int(float(level))

    level = level.upper()
    return logging.getLevelName(level)


def get_random_identifier(length: int = 8) -> str:
    """
    Generate a random-enough identifier that doesn't need to guarantee offered by UUIDs

    Args:
        length (): The number of characters to have in the identifier

    Returns:
        A random-enough identifier that is fine for short-lived, human-friendly, discrete identifiers
    """
    import string
    import random
    character_set: str = string.ascii_letters + string.digits

    return ''.join(random.choices(population=character_set, k=length))
