
This folder contains scripts to solve homework for week 1 of the Data Engineering Zoomcamp for the 2025 cohort. For the actual questions, look [here]()

You will need both docker and docker-compose to do this. Don't forget to add yourself to the docker group so that you don't have to run things with sudo. You will also need to have the appropriate python packages. See the `setup` folder one level up.

NOTE: the instructions here are only for running homework and only on the localhost. These are explicitly NOT secure for anything else.

Before starting, launch `postgres` using `docker-compose`:

```
docker compose up -d
```


Next, run the following commands to download the necessary data and load it into the `nyc_taxi` database
```
python load_data_into_database.py --url https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz --database nyc_taxi --table green_trip_data --host localhost --port 5434

python load_data_into_database.py --url https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv --database nyc_taxi --table zones --host localhost --port 5434

```