
# Homework 3 - ZoomCamp Data Warehouse


## 1. Creating Tables

### Creating External Table
```sql
CREATE OR REPLACE EXTERNAL TABLE `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoom_camp_hw3_2025/yellow_tripdata_*.parquet']
);
```

### Creating Regular Table
```sql
CREATE OR REPLACE TABLE `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg`
AS SELECT * FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu`;
```

### Creating Materialized View
```sql
CREATE MATERIALIZED VIEW `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_mat` AS (
  SELECT * FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg`
);
```
### Creating Partitioned Table
```sql
CREATE OR REPLACE TABLE `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_part`
PARTITION BY DATE(tpep_dropoff_datetime) AS (
  SELECT * FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg`
);
```

### Creating Partitioned and Clustered Table
```sql
CREATE OR REPLACE TABLE `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_part_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
  SELECT * FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg`
);
```

---

## 2. Questions and Answers

### Question 1: What is the count of records for the 2024 Yellow Taxi Data?

#### Using table details or SQL query
```sql
SELECT count(1) FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu`
```

**Answer:**
```text
20,332,093
```

---

### Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

#### Regular Table Query
```sql
SELECT count(distinct PULocationID) 
FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg`
```
This query will process **155.12 MB** when run.

#### External Table Query
```sql
SELECT count(distinct PULocationID) 
FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu`
```
This query will process **0 B** when run.

#### Materialized View Query
```sql
SELECT count(distinct PULocationID) 
FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_mat`
```
This query will process **155.12 MB** when run.

**Answer:**
```text
0 MB for the External Table and 155.12 MB for the Materialized Table
```

---

### Question 3: Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

#### PULocationID Query
```sql
SELECT PULocationID 
FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg`
```
This query will process **155.12 MB** when run.

#### PULocationID and DOLocationID Query
```sql
SELECT PULocationID, DOLocationID  
FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg`
```
This query will process **310.24 MB** when run.

**Answer:**
```text
BigQuery is a columnar database, and it only scans the specific columns requested in the query.
Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID),
leading to a higher estimated number of bytes processed.
```

---

### Question 4: How many records have a fare_amount of 0?

```sql
SELECT count(1)  
FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg` 
WHERE fare_amount = 0
```

**Answer:**
```text
8,333
```

---

### Question 5: What is the best strategy to make an optimized table in BigQuery if your query will always filter based on `tpep_dropoff_timedate` and order the results by `VendorID`? (Create a new table with this strategy)

#### Partitioned and Clustered Table
```sql
CREATE OR REPLACE TABLE `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_part_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
  SELECT * FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg`
);
```

#### Partitioned Table Only
```sql
CREATE OR REPLACE TABLE `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_part`
PARTITION BY DATE(tpep_dropoff_datetime) AS (
  SELECT * FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_reg`
);
```

**Answer:**
```text
Partition by `tpep_dropoff_timedate` and Cluster on `VendorID`
```

---

### Question 6: Write a query to retrieve the distinct VendorIDs between `tpep_dropoff_timedate` 03/01/2024 and 03/15/2024 (inclusive).

Use the materialized table you created earlier in your `FROM` clause and note the estimated bytes. Now change the table in the `FROM` clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

#### Materialized View Query
```sql
SELECT DISTINCT VendorID
FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_mat`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'
```
This query will process **310.24 MB** when run.

#### Partitioned Table Query
```sql
SELECT DISTINCT VendorID
FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_part`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'
```
This query will process **26.84 MB** when run.

#### Partitioned and Clustered Table Query
```sql
SELECT DISTINCT VendorID
FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_part_clustered`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'
```
This query will process **26.84 MB** when run.

**Answer:**
```text
310.24 MB for the non-partitioned table and 26.84 MB for the partitioned table
```

---

### Question 7: Where is the data stored in the External Table you created?

**Answer:**
```text
GCP Bucket
```

---

### Question 8: It is best practice in BigQuery to always cluster your data:

**Answer:**
```text
False
```

---

### Question 9 (Bonus: Not Worth Points): Write a `SELECT count(*)` query from the materialized table you created. How many bytes will be read? Why?

```sql
SELECT count(*)
FROM `titanium-portal-449021-g8.zoomcamp.yellow_tripdata_2024_eu_mat`
```

**Answer:**
```text
This query will process 0 B when run because it is already stored in the metadata of the table, so no additional processing is needed.
```