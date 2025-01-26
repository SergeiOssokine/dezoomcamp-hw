import gzip
import logging
import os

import pandas as pd
import pyarrow.parquet as pq
import requests
import typer
from rich.logging import RichHandler
from rich.traceback import install
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists
from typing_extensions import Annotated

# Sets up the logger to work with rich
logger = logging.getLogger(__name__)
logger.addHandler(RichHandler(rich_tracebacks=True, markup=True))
logger.setLevel("INFO")
# Setup rich to get nice tracebacks
install()


def load_csv_data(file_name: str, chunksize: int = 10000, **kwargs):
    """Load csv data into an iterator

    Args:
        file_name (str): The csv file to load
        chunksize (int, optional): The chunk size to use. Defaults to 10000.

    Returns:
        iterator: Iterator over the file
    """
    df_iter = pd.read_csv(file_name, iterator=True, chunksize=chunksize, **kwargs)
    return df_iter


def load_parquet_data(file_name: str, chunksize: int = 100000, **kwargs):
    """Load parquet data into an iterator

    Args:
        file_name (str): The name of the file to load
        chunksize (int, optional): The chunk size for the iterator.
            Defaults to 100000.

    Returns:
       iterator: Iterator over the file
    """
    file = pq.ParquetFile(file_name)
    df_iter = file.iter_batches(batch_size=chunksize, **kwargs)
    return df_iter


def download_data(url: str) -> str:
    """Download the file from the given url.
    Supports .csv, .parquet and .csv.gz files

    Args:
        url (str): The url to use

    Returns:
        str: The name of the downloaded file
    """
    logger.info("Downloading data")
    response = requests.get(url, stream=True)
    fname = os.path.basename(url)
    with open(fname, mode="wb") as fw:
        for chunk in response.iter_content(chunk_size=10 * 1024):
            fw.write(chunk)
    logger.info("Done")
    if fname.endswith(".gz"):
        trunc_name = fname[:-3]
        with gzip.open(fname, "rb") as fp:
            data = fp.read()
        with open(trunc_name, "wb") as fw:
            fw.write(data)
    return fname


def load_data_to_database(url: str, engine, table: str) -> None:
    """Download the data from the url and add it to the appropriate table

    Args:
        url (str): The url of the file to download
        engine (_type_): A SQLAlchemy engine
        table (str): The name of the table to create
    """
    # Download the file
    file_name = download_data(url)
    # Get the data from file
    logger.info("Loading data from file")
    if ".parquet" in file_name:
        df_iter = load_parquet_data(file_name)
        first = next(df_iter).to_pandas()

    else:
        df_iter = load_csv_data(file_name)
        first = next(df_iter)

    # Make the database if it doesn't exist
    if not database_exists(engine.url):
        create_database(engine.url)
    logger.info("Writing data to SQL database")

    first.to_sql(name=table, con=engine)
    for batch in df_iter:
        logger.info("Working on new chunk...")
        if ".parquet" in file_name:
            batch_df = batch.to_pandas()
        else:
            batch_df = batch
        batch_df.to_sql(name=table, con=engine, if_exists="append", index=False)


def main(
    url: Annotated[str, typer.Option(help="The url with the data")],
    database: Annotated[
        str, typer.Option(help="The name of the database to write the data to")
    ],
    table: Annotated[str, typer.Option(help="The name of the table to write to")],
    host: Annotated[str, typer.Option(help="The name of the host for the database")],
    port: Annotated[int, typer.Option(help="The port for the database")],
):
    """Download the data from given url and add it to the desired postgres database. Note that the
    database server must be running and the following env vars must be set:
    - POSTGRES_USER
    - POSTGRES_PASSWORD
    """
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    load_data_to_database(url, engine, table)


if __name__ == "__main__":
    typer.run(main)
