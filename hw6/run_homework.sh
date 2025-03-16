#!/bin/bash
# Function to print a nicer header
header() {
    echo $1
    printf '=%.0s' {1..80}
    echo
}

echo "Beginning HW 6"
cp session_job.py $DEZOOMCAMP_HOME/06-streaming/pyflink/src/job
orgidir=$(pwd)
cd $DEZOOMCAMP_HOME/06-streaming/pyflink
echo "Setting up the containers"
make up
echo "Done"
cd $orgidir
# Q1
header "Question 1"
version=$(docker exec redpanda-1 rpk version | grep Version | xargs | cut -d " " -f 2)
echo "The version of Red Panda is ${version}"

# Q2
header "Question 2"
docker exec redpanda-1  rpk topic create green-trips

# Q3
header "Question 3"
python test_connection.py

# Q4
header "Question 4"
python green_trips.py

# Q5
header "Question 5"
echo "Creating the sink table in the actual postgres database"
PGPASSWORD=postgres psql -U postgres -h localhost -p 5434 -d postgres -c "CREATE TABLE taxi_events_aggregated (
            PULocationID INTEGER,
            DOLocationID INTEGER,
			session_start TIMESTAMP(3),
			session_end TIMESTAMP(3)
            )"
echo "Starting the session job"
cd $DEZOOMCAMP_HOME/06-streaming/pyflink
docker compose exec jobmanager ./bin/flink run -py /opt/src/job/session_job.py --pyFiles /opt/src -d
echo "Waiting 30 seconds for the job to be executed"
sleep 30
echo "Running the final query"
PGPASSWORD=postgres psql -U postgres -h localhost -p 5434 -d postgres -c "select pulocationid, dolocationid, session_end -session_start as streak_length from taxi_events_aggregated order by streak_length desc limit 1"

echo "Cleaning up"
make down