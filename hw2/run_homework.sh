#!/bin/bash

# Function to print a nicer header
header() {
    echo $1
    printf '=%.0s' {1..80}
    echo
}

# Delete the _ext tables from all 2020 datasets
for k in $(seq -w 12); do
    echo "Working on month $k"
    bq rm -f -t dezoomcampnyc.green_tripdata_2020_${k}_ext
    bq rm -f -t dezoomcampnyc.yellow_tripdata_2020_${k}_ext
done

echo "Beginning HW 2"

# Q1
header "Question 1"
echo "The uncompressed size of yellow_tripdata_2020-12.csv is 128.3 MB"

# Q2
header "Question 2"
echo "The rendered value would be green_tripdata_2020-04.csv"

# Q3
header "Question 3"
echo "The total number of rows for the Yellow Taxi data for 2020 is"
bq query --use_legacy_sql=false --format prettyjson 'select count(*) as result from `dezoomcampnyc.yellow_tripdata_2020_*`;' | jq .[0].result

# Q4
header "Question 4"
echo "The total number of rows for the Green Tax data for 2020 is"
bq query --use_legacy_sql=false --format prettyjson 'select count(*) as result from `dezoomcampnyc.green_tripdata_2020_*`;' | jq .[0].result

# Q5
header "Question 5"
echo "The total number of rows for the Yellow Taxi data for 2021/03 is"
bq query --use_legacy_sql=false --format prettyjson 'select count(*) as result from `dezoomcampnyc.yellow_tripdata_2021_03`;' | jq .[0].result

# Q6
header "Question 6"
echo "To achieve this, simply add a timezone property set to America/New_York in the Schedule trigger configuration"
