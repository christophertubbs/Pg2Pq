"""
Constant values to be reused
"""
import pathlib
import re
import math

from datetime import timedelta


SENTINEL: object = object()
"""An object used to mean some version of 'not set' or 'not found' when `None` is a valid value"""

APPLICATION_NAME: str = "pg2pq"
"""The name of the application"""

PREFIX: str = "PG2PQ"
"""A prefix used to isolate Pg2Pq settings from other applications"""

LOG_FORMAT: str = "[%(asctime)s] (%(levelname)s) %(filename)s #%(lineno)d: %(message)s"
"""A common logging message format"""

PRECISE_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S%z"
"""A precise date format to use that goes down to the seconds"""

DATE_FORMAT: str = "%Y-%m-%d %H:%M%z"
"""A general date format that only goes down to the minute"""

ROOT_DIRECTORY: pathlib.Path = pathlib.Path(__file__).parent.parent.parent
"""What is considered the root of the application"""

ERROR_LOG_PATH: pathlib.Path = ROOT_DIRECTORY / f"{APPLICATION_NAME}_error.log"
"""The path to the error log"""

LOG_PATH: pathlib.Path = ROOT_DIRECTORY / f"{APPLICATION_NAME}.log"
"""The path to the log storing standard information"""

CONFIGURATION_DIRECTORY: pathlib.Path = ROOT_DIRECTORY / "configurations"
"""The path to general configurations"""

SCHEMA_DIRECTORY: pathlib.Path = CONFIGURATION_DIRECTORY / "schema"
"""Where to place parquet schema configurations"""

LOG_CONFIGURATION_PATH: pathlib.Path = CONFIGURATION_DIRECTORY / "logging.json"
"""The path to the logging config"""

LONG_TERM_BACKOFF: timedelta = timedelta(minutes=2)
"""
The number of seconds to wait when a long term operation outside of the scope of this application fails, 
such as an external server becoming unavailable
"""

LONG_TERM_BACKOFF_ATTEMPTS: int = math.ceil(timedelta(hours=1) / LONG_TERM_BACKOFF)
"""
The maximum number of times to wait for a long term operation outside of the scope of this application to start functioning
"""

INTEGER_PATTERN: re.Pattern = re.compile(r"^-?\d(_?\d)*$")
"""A pattern that detects if an entire string can be considered a python whole number integer"""
FLOAT_PATTERN: re.Pattern = re.compile(r"^-?\d(_\d)*\.\d*$")
"""A pattern that detects if an entire string can be considered a python floating point number"""
NUMBER_PATTERN: re.Pattern = re.compile(r"(?P<number>^(?P<float>-?\d(_?\d)*\.\d*)|(?P<integer>-?\d(_?\d)*)$)")
"""
A pattern that identifies either an integer or floating point whose value is stored in:

- `"number"`: The entire number, regardless of type
- `"float"`: A floating point number if it were detected
- `"integer"`: An integer value if it were detected
"""

DEFAULT_BUFFER_SIZE: int = 50_000
"""The default number of rows to buffer when reading data from the database"""
