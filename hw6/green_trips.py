import json
import time

import pandas as pd
from kafka import KafkaProducer


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


def load_data(file_name):
    df = pd.read_csv(file_name)
    df = df[
        [
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "passenger_count",
            "trip_distance",
            "tip_amount",
        ]
    ]
    return df


if __name__ == "__main__":
    server = "localhost:9092"

    producer = KafkaProducer(bootstrap_servers=[server])
    topic_name = "green-trips"
    data = load_data("green_tripdata_2019-10.csv")

    t0 = time.time()
    for index, row in data.iterrows():
        message = row.to_json()
        producer.send(topic_name, message.encode("utf-8"))
    producer.flush()
    t1 = time.time()
    print(f"took {(t1 - t0):.2f} seconds")
