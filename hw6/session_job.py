from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_events_aggregated_sink(t_env):
    table_name = "taxi_events_aggregated"
    sink_ddl = f"""
        CREATE OR REPLACE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3)
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_events_source_kafka(t_env):
    table_name = "taxi_events"
    pattern = "yyyy-MM-dd HH:mm:ss"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            dropoff_timestamp AS TO_TIMESTAMP(lpep_dropoff_datetime, '{pattern}'),
            WATERMARK FOR dropoff_timestamp AS dropoff_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def log_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(30 * 1000)
    env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)
        aggregated_table = create_events_aggregated_sink(t_env)
        # We interpret this question as follows:
        # 1. Consider all combinations of PU and DO locations in the data
        # 2. For every pair, group by the session based on the dropoff timestamp
        # 3. For every pair, compute the start and end of every session (there will be many such secessions)
        # 4. For every pair, compute the length of the session and then find the longest result. This is done with an outside query
        t_env.execute_sql(
            f"""
    INSERT INTO {aggregated_table}
    SELECT
        PULocationID,
        DOLocationID,
        MIN(dropoff_timestamp) as session_start,
        MAX(dropoff_timestamp) as session_end
    FROM {source_table}
    GROUP BY
        PULocationID,
        DOLocationID,
        SESSION(dropoff_timestamp, INTERVAL '5' MINUTE)
    """
        )

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == "__main__":
    log_aggregation()
