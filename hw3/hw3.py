import logging
import os

import numpy as np
from google.cloud import bigquery
from rich.logging import RichHandler

logger = logging.getLogger(__name__)
logger.addHandler(RichHandler(rich_tracebacks=True, markup=True))
logger.setLevel(logging.INFO)

CREDENTIALS_FILE = os.environ["CREDENTIALS_FILE"]
BUCKET = os.environ["BUCKET"]
PROJECT = os.environ["PROJECT"]
client = bigquery.Client.from_service_account_json(CREDENTIALS_FILE)


def get_estimated_usage(job):
    return np.round(job.total_bytes_processed / 1024 / 1024, 2)


def setup_bq():
    """
    Create the needed tables for the HW
    """
    client.query(f"create schema if not exists `{PROJECT}.nytaxi`;")
    create_external_table = f"""
create external table if not exists {PROJECT}.nytaxi.yellow_tripdata_2024_ext
options(
  format='parquet',
  uris=['gs://{BUCKET}/yellow_tripdata_2024-*.parquet']
);
"""
    client.query(create_external_table)
    create_regular_table = f"""
create table if not exists  {PROJECT}.nytaxi.yellow_tripdata_2024 as
select * from {PROJECT}.nytaxi.yellow_tripdata_2024_ext;
"""
    client.query(create_regular_table)


if __name__ == "__main__":
    logger.info("Beginning HW3")
    logger.info("Setting up the initial tables")
    setup_bq()
    logger.info("Done setup")

    job_config = bigquery.QueryJobConfig(dry_run=True)
    logger.info("[red] Question 1 [/red]")
    query = "select count(*) as nrows from `nytaxi.yellow_tripdata_2024`;"
    df = client.query(query).to_dataframe()
    logger.info(
        f"The total number of records for the 2024 Yellow Taxi Data is {df['nrows'].iloc[0]}"
    )

    logger.info("[red] Question 2 [/red]")
    # Get the estimate for the external table
    ext_query = (
        "select count(distinct PULocationID) from `nytaxi.yellow_tripdata_2024_ext`;"
    )
    job = client.query(ext_query, job_config=job_config)
    logger.info(
        f"The query with external table will process {get_estimated_usage(job)} MB"
    )

    query = "select count(distinct PULocationID) from `nytaxi.yellow_tripdata_2024`;"
    job = client.query(query, job_config=job_config)
    logger.info(
        f"The query with materialized table will process {get_estimated_usage(job)} MB"
    )

    logger.info("[red] Question 3 [/red]")
    query = "select PULocationID from `nytaxi.yellow_tripdata_2024`;"
    job = client.query(query, job_config=job_config)
    logger.info(
        f"The query with PULocationID will process {get_estimated_usage(job)} MB"
    )

    query = "select PULocationID, DOLocationID  from `nytaxi.yellow_tripdata_2024`;"
    job = client.query(query, job_config=job_config)
    logger.info(
        f"The query with both PULocationID and DOLocationID will process {get_estimated_usage(job)} MB"
    )
    logger.info(
        "The amount is different because BQ is a columnar database so in the second query it's scanning more data since it's scanning 2 columns"
    )
    logger.info("[red] Question 4 [/red]")
    query = """
select count(*) as no_fare from `nytaxi.yellow_tripdata_2024`
where fare_amount=0;
"""
    df = client.query(query).to_dataframe()
    logger.info(
        f"The number of records where fare_amount was 0 is {df['no_fare'].values[0]}"
    )

    logger.info("[red] Question 5 [/red]")
    logger.info(
        "The correct strategy is to partition on t_pep_dropoff_datetime and cluster by VendorID"
    )
    # Create a partitioned and clustered table
    query = """
create table if not exists `nytaxi.yellow_tripdata_2024_opt`
partition by date(tpep_dropoff_datetime)
cluster by VendorID as
(
  select * from `nytaxi.yellow_tripdata_2024`
);
"""
    job = client.query(query)

    logger.info("[red] Question 6 [/red]")
    # Run the query with the normal (unpartitioned table)
    query = """
select distinct VendorID
from `nytaxi.yellow_tripdata_2024`
where date(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15';
"""
    job = client.query(query, job_config=job_config)
    logger.info(
        f"The query on normal materialized table will process {get_estimated_usage(job)} MB"
    )

    query = """
select distinct VendorID
from `nytaxi.yellow_tripdata_2024_opt`
where date(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15';
"""
    job = client.query(query, job_config=job_config)
    logger.info(
        f"The query on the partitioned table will process {get_estimated_usage(job)} MB"
    )

    logger.info("[red] Question 7 [/red]")
    logger.info("The data for external tables is stored in the GCP bucket")

    logger.info("[red] Question 8 [/red]")
    logger.info(
        "In short, False - since sometimes other approaches like partitioning may be more appropriate"
    )

    logger.info("Done Hw3")
