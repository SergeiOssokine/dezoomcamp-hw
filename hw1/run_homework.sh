#!/bin/bash


# Function to print a nicer header
header() {
    echo $1
    printf '=%.0s' {1..80}
    echo
}

echo "Beginning HW 1"

# Q1
header "Question 1"
version=$(docker run  -it  python:3.12.8 bash -c "pip --version | cut -d ' '  -f 2")
echo "The version of pip inside the container is $version"

# Q2
header "Question 2"
echo "The correct way to connect is db:5432"

# Q3
header "Question 3"
echo "Running SQL"
PGPASSWORD=postgres psql  -h localhost -U postgres  -p 5434 -d nyc_taxi -a -f q3.sql

# Q4
header "Question 4"
echo "Running SQL"
PGPASSWORD=postgres psql  -h localhost -U postgres  -p 5434 -d nyc_taxi -a -f q4.sql

# Q5
header "Question 5"
echo "Running SQL"
PGPASSWORD=postgres psql  -h localhost -U postgres  -p 5434 -d nyc_taxi -a -f q5.sql

# Q6
header "Question 6"
echo "Running SQL"
PGPASSWORD=postgres psql  -h localhost -U postgres  -p 5434 -d nyc_taxi -a -f q6.sql

# Q6
header "Question 7"
echo "The correct commands are terraform init, terraform apply -auto-approve, terraform destroy"