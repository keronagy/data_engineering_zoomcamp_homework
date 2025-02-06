--creating tables

--creating external table
CREATE OR REPLACE EXTERNAL TABLE `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoom_camp_hw3_2025/yellow_tripdata_*.parquet']
);

--creating regular table
CREATE OR REPLACE TABLE `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg`
AS SELECT * FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu`;

--creating materialized view
CREATE MATERIALIZED VIEW `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_mat` AS (
  SELECT * FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg`
);


Question 1: What is count of records for the 2024 Yellow Taxi Data?

--using table details or using sql query
SELECT count(1) FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu`

--answer
20,332,093

Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?


--regular table
SELECT count (distinct PULocationID) FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg`
This query will process 155.12 MB when run.

--external table
SELECT count (distinct PULocationID) FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu`
This query will process 0 B when run.

--materialized view
SELECT count (distinct PULocationID) FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_mat`
This query will process 155.12 MB when run.

--answer
0 MB for the External Table and 155.12 MB for the Materialized Table


Question 3:
Write a query to retrieve the PULocationID form the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

SELECT PULocationID FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg`
This query will process 155.12 MB when run.

SELECT PULocationID,DOLocationID  FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg`
This query will process 310.24 MB when run.

--answer
BigQuery is a columnar database, and it only scans the specific columns requested in the query.
Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID),
leading to a higher estimated number of bytes processed.


Question 4:
How many records have a fare_amount of 0?

SELECT count(1)  FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg` where fare_amount =0

--answer
8,333



Question 5:
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_timedate and order the results by VendorID (Create a new table with this strategy)


--partioned and clustered
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


--answer
Partition by tpep_dropoff_timedate and Cluster on VendorID


Question 6:
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_timedate 03/01/2024 and 03/15/2024 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?


--materialized view
SELECT DISTINCT VendorID
FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_mat`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'
This query will process 310.24 MB when run.

SELECT DISTINCT VendorID
FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_part`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'
This query will process 26.84 MB when run.

SELECT DISTINCT VendorID
FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_part_clustered`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'
This query will process 26.84 MB when run.

--answer
310.24 MB for non-partitioned table and 26.84 MB for the partitioned table


Question 7:
Where is the data stored in the External Table you created?

--answer
GCP Bucket

Question 8:
It is best practice in Big Query to always cluster your data:

--answe
False

Question 8:
(Bonus: Not worth points) 
No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

SELECT count(*)
FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_mat`

--answer
This query will process 0 B when run.
because it is already stored in metadata of the table so no need to make any processing on the table
