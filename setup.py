"""
The legacy setup script for the application
"""
from setuptools import setup, find_packages

setup(
    name="Pg2Pq",
    version="0.1.0",
    description="A suite used to dump postgresql tables to parquet files",
    author="Christopher Tubbs",
    author_email="christopherotubbs@gmail.gov",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "wheel",
        "pydantic",
        "polars",
        "pyarrow",
        "psycopg",
        "sqlalchemy",
        "dotenv",
        "asyncpg",
        "duckdb",
        "tzlocal",
        "tqdm"
    ],
    extras_require={
        "netcdf": ["netCDF4", "xarray"],
    },
    python_requires=">=3.11",
    entry_points={
        "console_scripts": [
            "pg2pq=pg2pq.__main__:main",
        ]
    }
)
