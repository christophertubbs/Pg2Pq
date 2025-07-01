#!/usr/bin/env python3
"""
Functions and objects used to merge parquet files
"""
import argparse
import os
import typing
import logging
import pathlib

from pg2pq.exceptions import NothingToMergeException


LOGGER: logging.Logger = logging.getLogger(pathlib.Path(__file__).stem)


def find_input_paths(raw_paths: typing.Sequence[typing.Union[str, pathlib.Path]]) -> typing.Sequence[pathlib.Path]:
    """
    Evaluate given paths and glob for paths if needed

    :param raw_paths: Paths or patterns to files
    :return: A list of paths pointing to discrete files on disk
    """
    import glob

    input_paths: typing.List[pathlib.Path] = []

    for raw_path in raw_paths:
        if isinstance(raw_path, (pathlib.Path, str)) and '*' in str(raw_path):
            input_paths.extend(map(pathlib.Path, glob.glob(str(raw_path))))
        elif isinstance(raw_path, pathlib.Path):
            if raw_path.is_file():
                input_paths.append(raw_path)
            else:
                LOGGER.warning(f"'{raw_path}' is not a file - it will not be merged with other data")
        elif isinstance(raw_path, str):
            raw_path = pathlib.Path(raw_path)
            if raw_path.is_file():
                input_paths.append(raw_path)
            else:
                LOGGER.warning(f"'{raw_path}' is not a file - it will not be merged with other data")
        else:
            LOGGER.error(
                f"Cannot merge data from '{raw_path}' (type={type(raw_path)}) - the path must be a string or path"
            )

    return input_paths


def merge_parquet(
    files_to_merge: typing.Sequence[typing.Union[str, pathlib.Path]],
    enforce_unique: bool,
    target_path: pathlib.Path,
    compression_algorithm: str = "zstd",
    compression_level: int = 5,
    keys: typing.Sequence[str] = None
) -> None:
    """
    Combine all the paths in files_to_merge into a single file at the target path. One or more of the files to merge
    may be a glob

    :param files_to_merge: A collection of strings or paths used to find input data
    :param enforce_unique: If true, enforce unique rows
    :param target_path: Where to save the merged data
    :param compression_algorithm: The compression algorithm to use
    :param compression_level: The compression level to use
    :param keys: A list of keys to partition uniqueness checks on. Failure to supply this may result in a massive
    memory spike
    """
    input_data: typing.Sequence[pathlib.Path] = find_input_paths(files_to_merge)

    if len(input_data) == 0:
        raise FileNotFoundError(
            f"No input data was found at the following paths - nothing may be merged: {files_to_merge}"
        )

    if len(input_data) == 1 and not target_path.is_file():
        raise NothingToMergeException(
            f"Only one input was found to merge ({input_data[0]}) at the following path - "
            f"two or more files are required when merging"
        )
    elif len(input_data) == 1:
        input_data = [target_path, *input_data]

    if not keys:
        LOGGER.warning(
            f"No keys to partition on have been provided. "
            f"Failure to supply this may result in a massive memory spike and an out of memory error"
        )

    import duckdb

    files_to_merge = list(map(str, input_data))

    copy_options = [
        "FORMAT PARQUET",
        f"COMPRESSION {compression_algorithm.upper()}",
        f"COMPRESSION_LEVEL {compression_level}",
        "USE_TMP_FILE TRUE",
        "OVERWRITE TRUE"
    ]

    copy_script: str = f"COPY ("

    if keys:
        copy_script += f"""
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY {', '.join(keys)}) AS ROW_NUM
        FROM read_parquet({files_to_merge}) AS parquet_data
    ) AS counted_parquet
    WHERE ROW_NUM = 1
    ORDER BY {', '.join(keys)}   
)"""
    elif enforce_unique:
        copy_script += f"""
    SELECT DISTINCT *
    FROM read_parquet({files_to_merge})
)"""
    else:
        copy_script += f"""
    SELECT *
    FROM read_parquet({files_to_merge})
)"""

    copy_script += f" TO '{target_path}' ({', '.join(copy_options)})"

    try:
        LOGGER.info(f"Merging data into '{target_path}' - this may be time consuming")
        duckdb.sql(copy_script)
        LOGGER.info(f"Data from {files_to_merge} have been merged to '{target_path}'")
    except:
        if keys:
            LOGGER.error(
                f"Could not merge data from the following parquet files on the keys "
                f"{', '.join(keys)}: {files_to_merge}",
                exc_info=True
            )
        else:
            LOGGER.error(f"Could not merge data from the following parquet files: {files_to_merge}", exc_info=True)
        LOGGER.debug(f"Failing Script:{os.linesep}{copy_script}")
        raise

def get_parser() -> argparse.ArgumentParser:
    from pg2pq.command_arguments import MergeArgs

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Merge multiple parquet files"
    )

    MergeArgs.add_to_parser(parser=parser)

    return parser

def main() -> int:
    from pg2pq.command_arguments import MergeArgs

    parser: argparse.ArgumentParser = get_parser()
    raw_arguments: argparse.Namespace = parser.parse_args()

    arguments: MergeArgs = MergeArgs(**vars(raw_arguments))
    try:
        merge_parquet(
            files_to_merge=arguments.input_files,
            target_path=arguments.output_path,
            compression_algorithm=arguments.compression_algorithm,
            compression_level=arguments.compression_level,
            enforce_unique=arguments.enforce_unique
        )
    except Exception as e:
        LOGGER.error(e)
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
