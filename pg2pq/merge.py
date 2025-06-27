#!/usr/bin/env python3
"""
Functions and objects used to merge parquet files
"""
import argparse
import typing
import logging
import pathlib


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
    compression_level: int = 5
) -> None:
    """
    Combine all the paths in files_to_merge into a single file at the target path. One or more of the files to merge
    may be a glob

    :param files_to_merge: A collection of strings or paths used to find input data
    :param enforce_unique: If true, enforce unique rows
    :param target_path: Where to save the merged data
    :param compression_algorithm: The compression algorithm to use
    :param compression_level: The compression level to use
    """
    input_data: typing.Sequence[pathlib.Path] = find_input_paths(files_to_merge)

    if len(input_data) == 0:
        raise FileNotFoundError(
            f"No input data was found at the following paths - nothing may be merged: {files_to_merge}"
        )
    if len(input_data) == 1:
        raise ValueError(
            f"Only one input was found to merge ({input_data[0]}) at the following path - "
            f"two or more files are required when merging"
        )

    import polars

    polars_friendly_paths: typing.Sequence[str] = list(map(str, input_data))

    LOGGER.info(f"{len(polars_friendly_paths)} Parquet files will be attempted to be merged")

    gathered_data: polars.LazyFrame = polars.scan_parquet(source=polars_friendly_paths)

    if enforce_unique:
        gathered_data = gathered_data.unique()

    target_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        import tempfile
        import shutil
        with tempfile.TemporaryDirectory() as temporary_directory:
            staging_path: pathlib.Path = pathlib.Path(temporary_directory) / target_path.name
            LOGGER.info(f"Now saving parquet data")
            gathered_data.sink_parquet(
                path=staging_path,
                compression=compression_algorithm,
                compression_level=compression_level,
            )
            shutil.move(staging_path, target_path)
        LOGGER.info(f"{', '.join(polars_friendly_paths)} have been merged into {target_path}")
    except Exception as e:
        if enforce_unique:
            message = f"Could not merge unique values from the following files: {', '.join(polars_friendly_paths)}. {e}"
        else:
            message = f"Could not merge values from the following files: {', '.join(polars_friendly_paths)}. {e}"

        LOGGER.error(message, exc_info=e)
        raise e

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
