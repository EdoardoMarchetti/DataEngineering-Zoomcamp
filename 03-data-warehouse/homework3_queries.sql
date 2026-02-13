-- SETUP

-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE  `project_id.dataset_name.yellow_taxi_trip_external`
OPTIONS(
  format = 'parquet',
  uris = ['gs://bucket_name/yellow_tripdata_2024-*.parquet']
);


-- Creating materialized table 
CREATE OR REPLACE TABLE  `project_id.dataset_name.yellow_taxi_trip` AS
SELECT * FROM `project_id.dataset_name.yellow_taxi_trip_external`;






-- QUESTION 2
-- Count on external table
SELECT COUNT(DISTINCT(PULocationID))
FROM `project_id.dataset_name.yellow_taxi_trip_external`;

-- Count on table
SELECT COUNT(DISTINCT(PULocationID))
FROM `project_id.dataset_name.yellow_taxi_trip`;




-- QUESTION 3
SELECT PULocationID
FROM `project_id.dataset_name.yellow_taxi_trip`;


SELECT PULocationID, DOLocationID
FROM `project_id.dataset_name.yellow_taxi_trip`;


-- QUESTION 4
SELECT count(*)
FROM `project_id.dataset_name.yellow_taxi_trip`
where fare_amount = 0;


-- QUESTION 5
CREATE OR REPLACE TABLE  `project_id.dataset_name.yellow_taxi_trip_partitioned`
PARTITION BY
  DATE(tpep_dropoff_datetime) 
CLUSTER BY 
  VendorID 
AS
SELECT * FROM `project_id.dataset_name.yellow_taxi_trip`;


-- QUESTION 6
SELECT DISTINCT(VendorID)
FROM `project_id.dataset_name.yellow_taxi_trip`
where tpep_dropoff_datetime >= '2024-03-01' AND tpep_dropoff_datetime <= '2024-03-15';


SELECT DISTINCT(VendorID)
FROM `project_id.dataset_name.yellow_taxi_trip_partitioned`
where tpep_dropoff_datetime >= '2024-03-01' AND tpep_dropoff_datetime <= '2024-03-15';


-- QUESTION 9
SELECT count(*)
FROM `project_id.dataset_name.yellow_taxi_trip`






