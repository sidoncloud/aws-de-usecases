ALTER USER aws_kinesis_user SET RSA_PUBLIC_KEY='';

DESC USER aws_kinesis;

create database flights_db; 
create schema prod_schema; 

CREATE OR REPLACE TABLE flights (
    flight_id STRING,
    airline STRING,
    flight_number STRING,
    aircraft_type STRING,
    departure_airport STRING,
    arrival_airport STRING,
    scheduled_departure_time STRING,
    actual_departure_time STRING,
    scheduled_arrival_time STRING,
    actual_arrival_time STRING,
    gate STRING,
    terminal STRING,
    created_at STRING
);