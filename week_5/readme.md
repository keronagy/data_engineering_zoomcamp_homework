```python
!wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet
```

    --2025-03-08 20:57:25--  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet
    Resolving d37ci6vzurychx.cloudfront.net (d37ci6vzurychx.cloudfront.net)... 2600:9000:2795:1c00:b:20a5:b140:21, 2600:9000:2795:4800:b:20a5:b140:21, 2600:9000:2795:6c00:b:20a5:b140:21, ...
    Connecting to d37ci6vzurychx.cloudfront.net (d37ci6vzurychx.cloudfront.net)|2600:9000:2795:1c00:b:20a5:b140:21|:443... connected.
    HTTP request sent, awaiting response... 200 OK
    Length: 64346071 (61M) [binary/octet-stream]
    Saving to: 'yellow_tripdata_2024-10.parquet'
    
      
     62800K .......... .......... .......... .......              100% 47.7M=2.3s
    
    2025-03-08 20:57:28 (26.9 MB/s) - 'yellow_tripdata_2024-10.parquet' saved [64346071/64346071]
    
    


```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
```


```python
##Question 1: Install Spark and PySpark
spark.version
```




    '3.3.2'




```python
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
```


```python
df_yellow = spark.read.parquet('yellow_tripdata_2024-10.parquet')
```


```python
df_yellow.write.parquet('pq/')
```


```python
df_yellow.show()
```

    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+
    |VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|Airport_fee|
    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+
    |       1| 2024-10-07 19:40:43|  2024-10-07 21:10:56|              1|         14.8|        99|                 N|         127|         225|           1|       47.5|  0.0|    0.5|       0.0|        6.94|                  0.0|       54.94|                 0.0|        0.0|
    |       2| 2024-10-04 17:17:41|  2024-10-04 17:26:47|              1|          1.1|         1|                 N|         113|         211|           1|        9.3|  0.0|    0.5|      2.66|         0.0|                  1.0|       15.96|                 2.5|        0.0|
    |       2| 2024-10-01 14:17:28|  2024-10-01 14:32:18|              1|         4.63|         1|                 N|         231|         170|           1|       21.9|  0.0|    0.5|      5.18|         0.0|                  1.0|       31.08|                 2.5|        0.0|
    |       1| 2024-10-08 20:12:07|  2024-10-08 20:35:56|              1|          2.4|         1|                 N|         236|         100|           1|       19.8|  5.0|    0.5|      5.25|         0.0|                  1.0|       31.55|                 2.5|        0.0|
    |       1| 2024-10-01 18:37:08|  2024-10-01 18:54:23|              1|          2.1|         1|                 N|         237|          75|           1|       16.3|  2.5|    0.5|      4.05|         0.0|                  1.0|       24.35|                 2.5|        0.0|
    |       1| 2024-10-09 13:14:04|  2024-10-09 13:28:13|              1|          1.7|         1|                 N|         161|         234|           1|       12.8|  2.5|    0.5|      3.35|         0.0|                  1.0|       20.15|                 2.5|        0.0|
    |       2| 2024-10-04 20:08:38|  2024-10-04 21:03:10|              1|        17.42|         1|                 N|         132|          17|           1|       75.1|  2.5|    0.5|     19.77|         0.0|                  1.0|      100.62|                 0.0|       1.75|
    |       2| 2024-10-05 04:27:22|  2024-10-05 04:40:37|              1|         1.88|         1|                 N|         148|         137|           1|       13.5|  1.0|    0.5|       3.7|         0.0|                  1.0|        22.2|                 2.5|        0.0|
    |       2| 2024-10-06 02:50:45|  2024-10-06 03:00:32|              1|         1.06|         1|                 N|          79|         211|           1|       10.0|  1.0|    0.5|       2.0|         0.0|                  1.0|        17.0|                 2.5|        0.0|
    |       2| 2024-10-02 17:31:14|  2024-10-02 17:45:59|              1|         2.88|         1|                 N|         231|         246|           2|       17.0|  0.0|    0.5|       0.0|         0.0|                  1.0|        21.0|                 2.5|        0.0|
    |       2| 2024-10-10 11:25:53|  2024-10-10 11:27:59|              1|          0.4|         1|                 N|         237|         237|           1|        4.4|  0.0|    0.5|       2.5|         0.0|                  1.0|        10.9|                 2.5|        0.0|
    |       2| 2024-10-05 02:07:32|  2024-10-05 02:15:38|              1|         0.94|         1|                 N|         142|          48|           1|        9.3|  1.0|    0.5|      2.86|         0.0|                  1.0|       17.16|                 2.5|        0.0|
    |       2| 2024-10-04 20:00:04|  2024-10-04 20:07:54|              1|         0.85|         1|                 N|         170|         137|           2|        8.6|  2.5|    0.5|       0.0|         0.0|                  1.0|        15.1|                 2.5|        0.0|
    |       1| 2024-10-10 02:12:34|  2024-10-10 02:29:17|              1|          3.5|         1|                 N|         163|         211|           1|       17.7|  3.5|    0.5|       1.5|         0.0|                  1.0|        24.2|                 2.5|        0.0|
    |       2| 2024-10-10 00:19:16|  2024-10-10 00:53:56|              1|        12.33|         1|                 N|         237|         181|           1|       51.3|  1.0|    0.5|     15.81|        6.94|                  1.0|       79.05|                 2.5|        0.0|
    |       2| 2024-10-02 18:30:00|  2024-10-02 18:36:43|              1|         0.54|         1|                 N|          79|         113|           1|        7.2|  0.0|    0.5|       2.0|         0.0|                  1.0|        13.2|                 2.5|        0.0|
    |       2| 2024-10-01 23:12:23|  2024-10-01 23:23:32|              1|          2.1|         1|                 N|         161|         239|           1|       12.8|  1.0|    0.5|      3.56|         0.0|                  1.0|       21.36|                 2.5|        0.0|
    |       2| 2024-10-04 20:46:07|  2024-10-04 20:54:18|              1|         1.34|         1|                 N|         161|         141|           1|        9.3|  2.5|    0.5|      3.16|         0.0|                  1.0|       18.96|                 2.5|        0.0|
    |       2| 2024-10-05 12:16:58|  2024-10-05 12:20:40|              2|         0.91|         1|                 N|         141|         236|           1|        6.5|  0.0|    0.5|       2.1|         0.0|                  1.0|        12.6|                 2.5|        0.0|
    |       2| 2024-10-08 19:14:39|  2024-10-08 19:26:05|              1|          2.2|         1|                 N|         249|          13|           1|       13.5|  2.5|    0.5|       4.0|         0.0|                  1.0|        24.0|                 2.5|        0.0|
    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+
    only showing top 20 rows
    
    


```python
df_yellow = df_yellow.repartition(4)
```


```python
##What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.
!ls -lh pq/

#25
```

    total 97M
    -rw-r--r-- 1 Kyrillos Nagy 197609   0 Mar  8 21:03 _SUCCESS
    -rw-r--r-- 1 Kyrillos Nagy 197609 25M Mar  8 21:03 part-00000-72de6d9d-896f-4d13-ad30-9bcf40806c08-c000.snappy.parquet
    -rw-r--r-- 1 Kyrillos Nagy 197609 25M Mar  8 21:03 part-00001-72de6d9d-896f-4d13-ad30-9bcf40806c08-c000.snappy.parquet
    -rw-r--r-- 1 Kyrillos Nagy 197609 25M Mar  8 21:03 part-00002-72de6d9d-896f-4d13-ad30-9bcf40806c08-c000.snappy.parquet
    -rw-r--r-- 1 Kyrillos Nagy 197609 25M Mar  8 21:03 part-00003-72de6d9d-896f-4d13-ad30-9bcf40806c08-c000.snappy.parquet
    


```python
df_yellow.registerTempTable('yellow')
```


```python
df_yellow.columns
```




    ['VendorID',
     'tpep_pickup_datetime',
     'tpep_dropoff_datetime',
     'passenger_count',
     'trip_distance',
     'RatecodeID',
     'store_and_fwd_flag',
     'PULocationID',
     'DOLocationID',
     'payment_type',
     'fare_amount',
     'extra',
     'mta_tax',
     'tip_amount',
     'tolls_amount',
     'improvement_surcharge',
     'total_amount',
     'congestion_surcharge',
     'Airport_fee']




```python
spark.sql("""
SELECT
    count(1)
FROM
    yellow
WHERE 
--tpep_pickup_datetime >= ''
CAST(tpep_pickup_datetime AS DATE) = '2024-10-15'
""").show()
```

    +--------+
    |count(1)|
    +--------+
    |  122561|
    +--------+
    
    


```python
df_yellow.filter(F.to_date(F.col("tpep_pickup_datetime"))=="2024-10-15").count()
```




    122561




```python
spark.sql("""
SELECT
    count(1)
FROM
    yellow
WHERE 
tpep_pickup_datetime >= '2024-10-15 00:00:00'
and tpep_pickup_datetime < '2024-10-16 00:00:00'
""").show()
```

    +--------+
    |count(1)|
    +--------+
    |  122561|
    +--------+
    
    


```python
##Question 3: Count records = 122561
```


```python
spark.sql("""
SELECT
MAX(TIMESTAMPDIFF(HOUR,yellow.tpep_pickup_datetime,yellow.tpep_dropoff_datetime)) longest FROM yellow
""").show()
```

    +-------+
    |longest|
    +-------+
    |    162|
    +-------+
    
    


```python
##Question 4: Longest trip = 162
```


```python
##Question 5: User Interface = localhost:4040
```


```python
##Question 6: Least frequent pickup location zone
```


```python
!wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

    --2025-03-08 21:27:46--  https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
    Resolving d37ci6vzurychx.cloudfront.net (d37ci6vzurychx.cloudfront.net)... 2600:9000:2795:3200:b:20a5:b140:21, 2600:9000:2795:d200:b:20a5:b140:21, 2600:9000:2795:c200:b:20a5:b140:21, ...
    Connecting to d37ci6vzurychx.cloudfront.net (d37ci6vzurychx.cloudfront.net)|2600:9000:2795:3200:b:20a5:b140:21|:443... connected.
    HTTP request sent, awaiting response... 200 OK
    Length: 12331 (12K) [text/csv]
    Saving to: 'taxi_zone_lookup.csv'
    
         0K .......... ..                                         100% 3.10G=0s
    
    2025-03-08 21:27:47 (3.10 GB/s) - 'taxi_zone_lookup.csv' saved [12331/12331]
    
    


```python
df = spark.read \
    .option("header", "true") \
    .csv('taxi_zone_lookup.csv')
```


```python
df.write.parquet('zones')
```


```python
df_zones = spark.read.parquet('zones/')
```


```python
df_result = df_yellow.join(df_zones, df_yellow.PULocationID == df_zones.LocationID)
```


```python
df_result.show()
```

    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+----------+---------+--------------------+------------+
    |VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|Airport_fee|LocationID|  Borough|                Zone|service_zone|
    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+----------+---------+--------------------+------------+
    |       1| 2024-10-07 19:40:43|  2024-10-07 21:10:56|              1|         14.8|        99|                 N|         127|         225|           1|       47.5|  0.0|    0.5|       0.0|        6.94|                  0.0|       54.94|                 0.0|        0.0|       127|Manhattan|              Inwood|   Boro Zone|
    |       2| 2024-10-04 17:17:41|  2024-10-04 17:26:47|              1|          1.1|         1|                 N|         113|         211|           1|        9.3|  0.0|    0.5|      2.66|         0.0|                  1.0|       15.96|                 2.5|        0.0|       113|Manhattan|Greenwich Village...| Yellow Zone|
    |       2| 2024-10-01 14:17:28|  2024-10-01 14:32:18|              1|         4.63|         1|                 N|         231|         170|           1|       21.9|  0.0|    0.5|      5.18|         0.0|                  1.0|       31.08|                 2.5|        0.0|       231|Manhattan|TriBeCa/Civic Center| Yellow Zone|
    |       1| 2024-10-08 20:12:07|  2024-10-08 20:35:56|              1|          2.4|         1|                 N|         236|         100|           1|       19.8|  5.0|    0.5|      5.25|         0.0|                  1.0|       31.55|                 2.5|        0.0|       236|Manhattan|Upper East Side N...| Yellow Zone|
    |       1| 2024-10-01 18:37:08|  2024-10-01 18:54:23|              1|          2.1|         1|                 N|         237|          75|           1|       16.3|  2.5|    0.5|      4.05|         0.0|                  1.0|       24.35|                 2.5|        0.0|       237|Manhattan|Upper East Side S...| Yellow Zone|
    |       1| 2024-10-09 13:14:04|  2024-10-09 13:28:13|              1|          1.7|         1|                 N|         161|         234|           1|       12.8|  2.5|    0.5|      3.35|         0.0|                  1.0|       20.15|                 2.5|        0.0|       161|Manhattan|      Midtown Center| Yellow Zone|
    |       2| 2024-10-04 20:08:38|  2024-10-04 21:03:10|              1|        17.42|         1|                 N|         132|          17|           1|       75.1|  2.5|    0.5|     19.77|         0.0|                  1.0|      100.62|                 0.0|       1.75|       132|   Queens|         JFK Airport|    Airports|
    |       2| 2024-10-05 04:27:22|  2024-10-05 04:40:37|              1|         1.88|         1|                 N|         148|         137|           1|       13.5|  1.0|    0.5|       3.7|         0.0|                  1.0|        22.2|                 2.5|        0.0|       148|Manhattan|     Lower East Side| Yellow Zone|
    |       2| 2024-10-06 02:50:45|  2024-10-06 03:00:32|              1|         1.06|         1|                 N|          79|         211|           1|       10.0|  1.0|    0.5|       2.0|         0.0|                  1.0|        17.0|                 2.5|        0.0|        79|Manhattan|        East Village| Yellow Zone|
    |       2| 2024-10-02 17:31:14|  2024-10-02 17:45:59|              1|         2.88|         1|                 N|         231|         246|           2|       17.0|  0.0|    0.5|       0.0|         0.0|                  1.0|        21.0|                 2.5|        0.0|       231|Manhattan|TriBeCa/Civic Center| Yellow Zone|
    |       2| 2024-10-10 11:25:53|  2024-10-10 11:27:59|              1|          0.4|         1|                 N|         237|         237|           1|        4.4|  0.0|    0.5|       2.5|         0.0|                  1.0|        10.9|                 2.5|        0.0|       237|Manhattan|Upper East Side S...| Yellow Zone|
    |       2| 2024-10-05 02:07:32|  2024-10-05 02:15:38|              1|         0.94|         1|                 N|         142|          48|           1|        9.3|  1.0|    0.5|      2.86|         0.0|                  1.0|       17.16|                 2.5|        0.0|       142|Manhattan| Lincoln Square East| Yellow Zone|
    |       2| 2024-10-04 20:00:04|  2024-10-04 20:07:54|              1|         0.85|         1|                 N|         170|         137|           2|        8.6|  2.5|    0.5|       0.0|         0.0|                  1.0|        15.1|                 2.5|        0.0|       170|Manhattan|         Murray Hill| Yellow Zone|
    |       1| 2024-10-10 02:12:34|  2024-10-10 02:29:17|              1|          3.5|         1|                 N|         163|         211|           1|       17.7|  3.5|    0.5|       1.5|         0.0|                  1.0|        24.2|                 2.5|        0.0|       163|Manhattan|       Midtown North| Yellow Zone|
    |       2| 2024-10-10 00:19:16|  2024-10-10 00:53:56|              1|        12.33|         1|                 N|         237|         181|           1|       51.3|  1.0|    0.5|     15.81|        6.94|                  1.0|       79.05|                 2.5|        0.0|       237|Manhattan|Upper East Side S...| Yellow Zone|
    |       2| 2024-10-02 18:30:00|  2024-10-02 18:36:43|              1|         0.54|         1|                 N|          79|         113|           1|        7.2|  0.0|    0.5|       2.0|         0.0|                  1.0|        13.2|                 2.5|        0.0|        79|Manhattan|        East Village| Yellow Zone|
    |       2| 2024-10-01 23:12:23|  2024-10-01 23:23:32|              1|          2.1|         1|                 N|         161|         239|           1|       12.8|  1.0|    0.5|      3.56|         0.0|                  1.0|       21.36|                 2.5|        0.0|       161|Manhattan|      Midtown Center| Yellow Zone|
    |       2| 2024-10-04 20:46:07|  2024-10-04 20:54:18|              1|         1.34|         1|                 N|         161|         141|           1|        9.3|  2.5|    0.5|      3.16|         0.0|                  1.0|       18.96|                 2.5|        0.0|       161|Manhattan|      Midtown Center| Yellow Zone|
    |       2| 2024-10-05 12:16:58|  2024-10-05 12:20:40|              2|         0.91|         1|                 N|         141|         236|           1|        6.5|  0.0|    0.5|       2.1|         0.0|                  1.0|        12.6|                 2.5|        0.0|       141|Manhattan|     Lenox Hill West| Yellow Zone|
    |       2| 2024-10-08 19:14:39|  2024-10-08 19:26:05|              1|          2.2|         1|                 N|         249|          13|           1|       13.5|  2.5|    0.5|       4.0|         0.0|                  1.0|        24.0|                 2.5|        0.0|       249|Manhattan|        West Village| Yellow Zone|
    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+----------+---------+--------------------+------------+
    only showing top 20 rows
    
    


```python
df_result.columns
```




    ['VendorID',
     'tpep_pickup_datetime',
     'tpep_dropoff_datetime',
     'passenger_count',
     'trip_distance',
     'RatecodeID',
     'store_and_fwd_flag',
     'PULocationID',
     'DOLocationID',
     'payment_type',
     'fare_amount',
     'extra',
     'mta_tax',
     'tip_amount',
     'tolls_amount',
     'improvement_surcharge',
     'total_amount',
     'congestion_surcharge',
     'Airport_fee',
     'LocationID',
     'Borough',
     'Zone',
     'service_zone']




```python
df_result.registerTempTable('res')
```


```python
spark.sql("""
SELECT
    Zone,
    count(*)
FROM
    res
GROUP BY Zone
ORDER BY 2 ASC
LIMIT 1
""").show()
```

    +--------------------+--------+
    |                Zone|count(1)|
    +--------------------+--------+
    |Governor's Island...|       1|
    +--------------------+--------+
    
    


```python
#Q6 = Governor's Island/Ellis Island/Liberty Island
```
