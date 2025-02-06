CREATE OR REPLACE EXTERNAL TABLE `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoom_camp_hw3_2025/yellow_tripdata_*.parquet']
);



CREATE OR REPLACE TABLE `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg`
AS SELECT * FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu`;


CREATE MATERIALIZED VIEW `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_mat` AS (
  SELECT * FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg`
);



SELECT count(1)  FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg` where fare_amount =0


SELECT DISTINCT VendorID
FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'



CREATE OR REPLACE TABLE `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_part_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
  SELECT * FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg`
);

--partioned 
CREATE OR REPLACE TABLE `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_part`
PARTITION BY DATE(tpep_dropoff_datetime) AS (
  SELECT * FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg`
);



SELECT DISTINCT VendorID
FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_mat`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'


SELECT DISTINCT VendorID
FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_part`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'



SELECT DISTINCT VendorID
FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_part_clustered`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'


SELECT count(*)
FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_mat`