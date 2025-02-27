import gzip
import logging
import os

import pandas as pd
import pandas_gbq as pdq
import requests
from google.oauth2 import service_account
from rich.logging import RichHandler
from rich.traceback import install

# Sets up the logger to work with rich
logger = logging.getLogger(__name__)
logger.addHandler(RichHandler(rich_tracebacks=True, markup=True))
logger.setLevel("INFO")
# Setup rich to get nice tracebacks
install()

PROJECT_ID = os.environ["PROJECT_ID"]
GOOGLE_CREDENTIALS = os.environ["GOOGLE_CREDENTIALS"]


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


def write_to_bq(
    file_name: str,
    credentials=None,
    table_id: str = "dezoomcampnyc.fhv_data",
    project_id=PROJECT_ID,
):
    logger.info(f"Reading data from {file_name}")
    df = pd.read_csv(file_name)
    logger.info(f"Writing data from {file_name} to BQ table")
    pdq.to_gbq(
        df, table_id, project_id=project_id, if_exists="append", credentials=credentials
    )
    logger.info("Done writing data")


def main():
    credentials = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS,
    )
    for month in range(1, 13):
        fname = f"fhv_tripdata_2019-{str(month).zfill(2)}.csv.gz"
        logger.info(f"Working on {fname}")
        if not os.path.isfile(fname):
            url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{fname}"
            logger.info(f"Downloading data from {url}")
            download_data(url)
            logger.info("Done downloading")

        write_to_bq(fname[:-3], credentials=credentials)
    logger.info("All done")


if __name__ == "__main__":
    main()
