#!/usr/bin/env python3
"""
Functions and objects used to convert parquet data to netcdf
"""
import argparse
import os
import typing
import logging
import pathlib
import sys

from pg2pq.utilities.common import now
from pg2pq.utilities import constants

LOGGER: logging.Logger = logging.getLogger(pathlib.Path(__file__).stem)


def get_scale_factor(precision: int = 2) -> float:
    if precision < 0:
        raise ValueError(f"Cannot calculate scale factor - the precision must be 0 or more")
    power: int = precision * -1
    return 10 ** power


def convert_to_netcdf(
    source: pathlib.Path,
    output_path: pathlib.Path,
    dimensions: typing.Sequence[str],
    variables_to_exclude: typing.Sequence[str] = None,
    precision: int = 2
) -> None:
    if len(dimensions) > 2:
        raise ValueError(f"More than 2 dimensions is not currently supported for converting parquet to netcdf")

    import xarray
    import dask.dataframe
    import dask.array
    from pandas.api.types import CategoricalDtype
    import polars
    import pyarrow

    if variables_to_exclude is None:
        variables_to_exclude = []

    LOGGER.info(f"Finding what columns to save")
    parquet_data: polars.DataFrame = polars.read_parquet(source)
    columns_to_include: typing.Sequence[str] = [
        column_name
        for column_name in parquet_data.columns
        if column_name not in variables_to_exclude
    ]
    schema: pyarrow.Schema = parquet_data.to_arrow().schema

    metadata: typing.Dict[str, typing.Dict[str, typing.Any]] = {
        column_name: (schema.field(schema.get_field_index(column_name)).metadata or {}).copy()
        for column_name in columns_to_include
    }
    global_metadata: typing.Dict[str, typing.Any] = (schema.metadata or {}).copy()
    del parquet_data

    global_metadata['source'] = str(source)
    global_metadata['generation_date'] = now().strftime(constants.DATE_FORMAT)
    global_metadata['history'] = ' '.join([pathlib.Path(sys.argv[0]).name, *sys.argv[1:]])
    global_metadata["generated_by"] = os.environ.get("USER", "unknown")

    category_types: typing.Dict[str, CategoricalDtype] = {}

    coordinates: typing.Dict[str, typing.Sequence[typing.Any]] = {}

    LOGGER.info(f"Loading in the source data from {source}")
    dask_data: dask.dataframe.DataFrame = dask.dataframe.read_parquet(str(source), columns=columns_to_include)

    LOGGER.info(f"Formatting the following dimensions to use as coordinates: {dimensions}")
    for dimension in dimensions:
        dimension_values = sorted(dask_data[dimension].unique().compute())
        dimension_category_type = CategoricalDtype(categories=dimension_values, ordered=True)
        dask_data[dimension] = dask_data[dimension].astype(dimension_category_type)
        category_types[dimension] = dimension_category_type
        coordinates[dimension] = dimension_values
        LOGGER.info(f"'{dimension}' has been categorized")

    columns: typing.Dict[str, xarray.DataArray] = {}

    for column in dask_data.columns:
        if column in dimensions:
            continue
        LOGGER.info(f"Preparing the '{column}' column for xarray")
        # TODO: Add alternate handling for when there is only 1 dimension
        pivoted_data: typing.Union[dask.dataframe.DataFrame, dask.array.Array] = dask_data.pivot_table(
            index=dimensions[0],
            columns=dimensions[1], # TODO: Look into 3+D stacking for when there are more than 2 dimensions
            values=column
        )
        pivoted_data = pivoted_data.to_dask_array(lengths=True).rechunk()
        columns[column] = xarray.DataArray(
            data=pivoted_data,
            name=column,
            dims=dimensions,
            coords=coordinates,
            attrs=metadata.get(column) or None
        )

    dataset: xarray.Dataset = xarray.Dataset(data_vars=columns, coords=coordinates, attrs=global_metadata or None)
    dataset = dataset.astype({
        column_name: "float32"
        for column_name, column in dataset.data_vars.items()
        if column.dtype.kind == 'f'
    })

    scale_factor: float = get_scale_factor(precision=precision)

    encodings: typing.Dict[str, typing.Dict[str, typing.Any]] = {
        column_name: {
            "dtype": "int32",
            "scale_factor": scale_factor,
            "add_offset": 0.0,
            "_FillValue": -999,
            "zlib": True,
            "complevel": 4,
            "chunksizes": (1, len(coordinates[dimensions[-1]])),
        }
        for column_name, column in dataset.data_vars.items()
        if column.dtype.kind == 'f'
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_netcdf(output_path, encoding=encodings)
    LOGGER.info(f"Netcdf data for {source} has been written to {output_path}")


def get_parser() -> argparse.ArgumentParser:
    try:
        from pg2pq.command_arguments import ToNetcdfArgs
    except ImportError as import_error:
        raise NotImplementedError(
            "Cannot perform the netcdf -> parquet conversion - required libraries are not installed"
        ) from import_error

    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Convert Parquet data to NetCDF"
    )

    ToNetcdfArgs.add_to_parser(parser=parser)

    return parser


def main() -> int:
    from pg2pq.command_arguments import ToNetcdfArgs

    parser: argparse.ArgumentParser = get_parser()
    raw_arguments: argparse.Namespace = parser.parse_args()

    arguments: ToNetcdfArgs = ToNetcdfArgs(**vars(raw_arguments))
    try:
        convert_to_netcdf(
            source=arguments.target_parquet,
            output_path=arguments.output_path,
            dimensions=arguments.dimensions,
            variables_to_exclude=arguments.exclude,
        )
    except Exception as e:
        LOGGER.error(e)
        return 1
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format=constants.LOG_FORMAT,
        datefmt=constants.DATE_FORMAT,
    )

    sys.exit(main())
