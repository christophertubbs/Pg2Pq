"""
Provides a means of replacing text in large strings with a variety of stored values and expressions
"""
import typing
import re
import os
import json
import pathlib

from datetime import datetime

from pg2pq.utilities import settings
from pg2pq.utilities import constants

EXPRESSIONS: typing.Mapping[str, typing.Any] = {
    "NOW": lambda: datetime.now().isoformat()
}

def ready_value_for_replacement(value: typing.Any) -> str:
    if callable(value):
        return value()

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, bool):
        return str(value).lower()

    return str(value)

def create_template_variable_pattern(name: str) -> re.Pattern:
    return re.compile(
        rf'"%#\s*{name}\s*#%"|'
        rf"'%#\s*{name}\s*#%'|"
        rf'\\"%#\s*{name}\s*#%\\"'
    )

def get_patterns_and_replacements(**extra_replacements) -> typing.Sequence[typing.Tuple[str, re.Pattern, typing.Any]]:
    replacement_patterns: typing.List[typing.Tuple[str, re.Pattern, typing.Any]] = [
        (
            replacement_name,
            create_template_variable_pattern(name=replacement_name),
            ready_value_for_replacement(replacement)
        )
        for replacement_name, replacement in extra_replacements.items()
    ]

    replacement_patterns.extend([
        (
            expression_name,
            create_template_variable_pattern(name=expression_name),
            expression() if callable(expression) else expression
        )
        for expression_name, expression in EXPRESSIONS.items()
    ])

    replacement_patterns.extend([
        (
            setting_name,
            create_template_variable_pattern(name=setting_name.upper()),
            ready_value_for_replacement(setting_value)
        )
        for setting_name, setting_value in settings.to_dict().items()
    ])

    constant_values: typing.Mapping[str, typing.Any] = {
        key: value
        for key, value in vars(constants).items()
        if key == key.upper()
           and isinstance(value, (int, float, str, pathlib.Path, datetime, bool))
    }

    replacement_patterns.extend([
        (
            key,
            create_template_variable_pattern(name=key),
            ready_value_for_replacement(value=value)
        )
        for key, value in constant_values.items()
    ])

    replacement_patterns.extend([
        (
            variable_name,
            re.compile(rf'(\'|\\"|")?%#\s*{variable_name}\s*#%(\'|\\"|")?'),
            variable_value
        )
        for variable_name, variable_value in os.environ.items()
        if not any(pattern_and_value[0] == variable_name for pattern_and_value in replacement_patterns)
    ])
    return replacement_patterns

def parse_and_replace(text: str, **extra_replacements) -> str:
    """
    Read the text and replace all instances of recorded expressions, settings, and environment variables

    Args:
        text ():

    Returns:

    """
    replacement_patterns: typing.Sequence[typing.Tuple[str, re.Pattern, typing.Any]] = get_patterns_and_replacements(
        **extra_replacements
    )

    for replacement_name, replacement_pattern, replacement_value in replacement_patterns:
        if replacement_pattern.search(text):
            try:
                text = replacement_pattern.sub(
                    replacement_value.decode() if isinstance(replacement_value, bytes) else str(replacement_value),
                    text
                )
            except re.error as e:
                message: str = f"""Could not replace {replacement_name} with {replacement_value} with a pattern of {replacement_pattern.pattern} due to {e}:
{text}
"""
                raise re.error(message, replacement_pattern.pattern) from e

    return text

def load_parse_and_replace_json(path: typing.Union[str, pathlib.Path], **extra_replacements) -> typing.Any:
    if not isinstance(path, pathlib.Path):
        path = pathlib.Path(path)

    json_text = path.read_text()

    json_text = parse_and_replace(text=json_text, **extra_replacements)

    # TODO: Add something to fix rogue `True` and `False` values

    return json.loads(json_text)
