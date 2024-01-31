TPC-DS Benchmark for MobilityDB
==================================

<img src="doc/images/mobilitydb-logo.svg" width="200" alt="MobilityDB Logo" />

[MobilityDB](https://github.com/ULB-CoDE-WIT/MobilityDB) is an open source software program that adds support for temporal and spatio-temporal objects to the [PostgreSQL](https://www.postgresql.org/) database and its spatial extension [PostGIS](http://postgis.net/).

This repository contains the code and the documentation for running an excerpt of the [TPC-DS bechmark](https://www.tpc.org/tpcds/) for analyzing alternative implementations of a temporal data warehouse to assess the performance of temporal algebra and temporal OLAP queries.

The three implementations are
*  The traditional Kimball's Slowly Changing Dimension (SCD) implementation
*  A temporal data warehouse (TDW) implementation proposed by Ahmed et al. in the following [article](https://www.igi-global.com/gateway/article/265260)
* A MobilityDB (MobDB) implementation 

Running the benchmark
---------------------

To run the benchmark you must first install the MobilityDB extension in your system. 
```
git clone https://github.com/MobilityDB/MobilityDB
```
We refer to the [MobilityDB](https://github.com/MobilityDB/MobilityDB) documentation for doing this. Please notice that MobilityDB requires PostgreSQL version 12 or higher and PostGIS version 3 or higher.

This benchmark uses data generated by the TPC-DS benchmark corresponding to four scale factors (SF): SF1, SF10, SF50, and SF100. The data for these scale factors are available in zip format in the files [tpcds_sf1.zip](https://docs.mobilitydb.com/pub/tpcds_sf1.zip) (135M), [tpcds_sf10.zip](https://docs.mobilitydb.com/pub/tpcds_sf10.zip) (1.16G), [tpcds_sf50.zip](https://docs.mobilitydb.com/pub/tpcds_sf50.zip) (5.7G), and [tpcds_sf100.zip](https://docs.mobilitydb.com/pub/tpcds_sf100.zip) (13.5G).

The CSV files extracted from the above zip files are expected to be in a subdirectory per scale factor under the current subdirectory, e.g.,
```bash
/home/user/src/MobilityDB-TPCS/sf1/
/home/user/src/MobilityDB-TPCS/sf10/
/home/user/src/MobilityDB-TPCS/sf50/
/home/user/src/MobilityDB-TPCS/sf100/
```

For loading the fact table `store_sales` in the larger scale factors it is recommended to increase the configuration parameter `max_wal_size` in the `postgresql.conf` file. As stated in [https://www.postgresql.org/docs/current/runtime-config-wal.html](https://www.postgresql.org/docs/current/runtime-config-wal.html), the default is 1GB, you may at least double this parameter. In our experiments we set this value to 8GB. Furthermore, as explained in
[https://www.enterprisedb.com/postgres-tutorials/how-tune-postgresql-memory](https://www.enterprisedb.com/postgres-tutorials/how-tune-postgresql-memory)
the `shared_buffers` parameter determines how much memory is dedicated to the server for caching data. It is recommended that the value should be set to 15% to 25% of the machine's total RAM. In our experiments we set this value to 8GB.
Please notice that a change of these parameters requires restarting PostgreSQL.

You can run the benchmark as follows (below the example for SF1).

```sql
$ createdb tpcds_sf1
$ psql tpcds_sf1
psql (13.10)
Type "help" for help.

-- Create the MobilityDB extension
tpcds_sf1=> create extension mobilitydb cascade;
NOTICE:  installing required extension "postgis"
CREATE EXTENSION
-- Create the function that loads the data
tpcds_sf1=> \i tdw_load.sql
psql:tdw_load.sql:52: NOTICE:  function tdw_load() does not exist, skipping
DROP FUNCTION
CREATE FUNCTION

-- Show the execution time for each command
tpcds_sf1=> \timing
Timing is on.

-- Load the data for scale factor 1 as stated by the argument.
-- You can use the values 10 or 100 for the other scale factors.
-- Please notice that loading the scale factor 100 takes considerable time.
-- The loading time for the three scale factors on a desktop machine with an
-- AMD Ryzen 9 3900X 12-Core Processor 3.79 GHz and 64 G of RAM are as follows
-- * sf1: Time: 10583.614 ms (00:10.584)
-- * sf10: Time: 204723.647 ms (03:24.724)
-- * sf50: Time: 2355244.455 ms (39:15.244)
-- * sf100: Time: 6158453.478 ms (01:42:38.453) <- XXX
tpcds_sf1=> SELECT tdw_load(SF:=1); -- Replace the argument with the other scale factors
NOTICE:  table "scd_item" does not exist, skipping
NOTICE:  table "mobdb_item" does not exist, skipping
NOTICE:  table "tdw_item" does not exist, skipping
NOTICE:  table "tdw_item_vt" does not exist, skipping
NOTICE:  table "mobdb_brand" does not exist, skipping
NOTICE:  table "tdw_brand" does not exist, skipping
NOTICE:  table "tdw_brand_vt" does not exist, skipping
NOTICE:  table "mobdb_category" does not exist, skipping
NOTICE:  table "tdw_category" does not exist, skipping
NOTICE:  table "tdw_category_vt" does not exist, skipping
NOTICE:  table "mobdb_item_category" does not exist, skipping
NOTICE:  table "tdw_item_category" does not exist, skipping
NOTICE:  table "mobdb_item_brand" does not exist, skipping
NOTICE:  table "tdw_item_brand" does not exist, skipping
NOTICE:  table "mobdb_item_price" does not exist, skipping
NOTICE:  table "tdw_item_price" does not exist, skipping
NOTICE:  table "date_dim" does not exist, skipping
NOTICE:  table "store_sales" does not exist, skipping
 tdw_load
----------
 The End
(1 row)

Time: 10583.614 ms (00:10.584)

-- Compute table statics to be used by the query optimizer.
tpcds_sf1=# analyze;
ANALYZE
Time: 190.260 ms

-- Create the views computing the temporal algebra operators.
-- The same views are used for computing the algebra and the OLAP queries
tpcds_sf1=> \i talgebra_views.sql
psql:talgebra_views.sql:52: NOTICE:  view "q1_mobdb" does not exist, skipping
DROP VIEW
Time: 0.606 ms
CREATE VIEW
Time: 5.296 ms
psql:talgebra_views.sql:64: NOTICE:  view "q1_tdw" does not exist, skipping
DROP VIEW
Time: 0.199 ms
CREATE VIEW
Time: 5.550 ms
psql:talgebra_views.sql:94: NOTICE:  view "q1_scd" does not exist, skipping
DROP VIEW
Time: 0.152 ms
CREATE VIEW
Time: 4.243 ms
psql:talgebra_views.sql:129: NOTICE:  view "q2_mobdb" does not exist, skipping
DROP VIEW
Time: 0.139 ms
CREATE VIEW
Time: 3.594 ms
psql:talgebra_views.sql:143: NOTICE:  view "q2_tdw" does not exist, skipping
DROP VIEW
Time: 0.144 ms
CREATE VIEW
Time: 4.047 ms
psql:talgebra_views.sql:171: NOTICE:  view "q2_scd" does not exist, skipping
DROP VIEW
Time: 0.166 ms
CREATE VIEW
Time: 4.083 ms
psql:talgebra_views.sql:204: NOTICE:  view "q3_mobdb" does not exist, skipping
DROP VIEW
Time: 0.181 ms
CREATE VIEW
Time: 3.879 ms
psql:talgebra_views.sql:216: NOTICE:  view "q3_tdw" does not exist, skipping
DROP VIEW
Time: 0.166 ms
CREATE VIEW
Time: 4.010 ms
psql:talgebra_views.sql:227: NOTICE:  view "q3_scd" does not exist, skipping
DROP VIEW
Time: 0.137 ms
CREATE VIEW
Time: 4.255 ms
psql:talgebra_views.sql:263: NOTICE:  view "q4_mobdb" does not exist, skipping
DROP VIEW
Time: 0.138 ms
CREATE VIEW
Time: 4.005 ms
psql:talgebra_views.sql:283: NOTICE:  view "q4_tdw" does not exist, skipping
DROP VIEW
Time: 0.142 ms
CREATE VIEW
Time: 4.202 ms
psql:talgebra_views.sql:317: NOTICE:  view "q4_scd" does not exist, skipping
DROP VIEW
Time: 0.176 ms
CREATE VIEW
Time: 3.887 ms
psql:talgebra_views.sql:352: NOTICE:  view "q5_mobdb" does not exist, skipping
DROP VIEW
Time: 0.145 ms
CREATE VIEW
Time: 3.668 ms
psql:talgebra_views.sql:368: NOTICE:  view "q5_tdw" does not exist, skipping
DROP VIEW
Time: 0.133 ms
CREATE VIEW
Time: 7.968 ms
psql:talgebra_views.sql:502: NOTICE:  view "q5_scd" does not exist, skipping
DROP VIEW
Time: 0.147 ms
CREATE VIEW
Time: 5.929 ms
psql:talgebra_views.sql:640: NOTICE:  view "q6_mobdb" does not exist, skipping
DROP VIEW
Time: 0.162 ms
CREATE VIEW
Time: 3.654 ms
psql:talgebra_views.sql:687: NOTICE:  view "q6_tdw" does not exist, skipping
DROP VIEW
Time: 0.154 ms
CREATE VIEW
Time: 4.531 ms
psql:talgebra_views.sql:735: NOTICE:  view "q6_scd" does not exist, skipping
DROP VIEW
Time: 0.130 ms
CREATE VIEW
Time: 6.254 ms
Time: 0.093 ms

-- Define the function that executes the benchmark of the algebra queries
tpcds_sf1=> \i talgebra_queries.sql
psql:talgebra_queries.sql:41: NOTICE:  function talgebra_queries() does not exist, skipping
DROP FUNCTION
Time: 0.333 ms
CREATE FUNCTION
Time: 1797.864 ms (00:01.798)
Time: 0.203 ms

-- Run the benchmark for the algebra queries only once as stated by argument.
-- You can run the queries several times by giving other values for the argument.
-- An example is given later in this document.
tpcds_sf1=> SELECT talgebra_queries(1); -- Replace the argument with the number of times each query is executed
NOTICE:  table "talgebra_queries" does not exist, skipping
INFO:  Query: Q1_MobDB, Run: 1, Total Duration: 00:00:13.465, Seconds: 13.465, Number of Rows: 4421
INFO:  Query: Q1_TDW, Run: 1, Total Duration: 00:08:56.308, Seconds: 536.308, Number of Rows: 4604
INFO:  Query: Q1_SCD, Run: 1, Total Duration: 00:09:38.939, Seconds: 578.939, Number of Rows: 4604
INFO:  Query: Q2_MobDB, Run: 1, Total Duration: 00:00:03.547, Seconds: 3.547, Number of Rows: 949
INFO:  Query: Q2_TDW, Run: 1, Total Duration: 00:24:47.164, Seconds: 1487.164, Number of Rows: 955
INFO:  Query: Q3_MobDB, Run: 1, Total Duration: 00:01:15.087, Seconds: 75.087, Number of Rows: 17951
INFO:  Query: Q3_TDW, Run: 1, Total Duration: 00:00:58.132, Seconds: 58.132, Number of Rows: 17951
INFO:  Query: Q3_SCD, Run: 1, Total Duration: 00:04:43.538, Seconds: 283.538, Number of Rows: 17951
INFO:  Query: Q4_MobDB, Run: 1, Total Duration: 00:00:13.331, Seconds: 13.331, Number of Rows: 1688
INFO:  Query: Q4_TDW, Run: 1, Total Duration: 00:00:54.532, Seconds: 54.532, Number of Rows: 1730
INFO:  Query: Q4_SCD, Run: 1, Total Duration: 00:01:13.91, Seconds: 73.91, Number of Rows: 1730
INFO:  Query: Q5_MobDB, Run: 1, Total Duration: 00:00:24.838, Seconds: 24.838, Number of Rows: 945
INFO:  Query: Q6_MobDB, Run: 1, Total Duration: 00:01:38.773, Seconds: 98.773, Number of Rows: 693
INFO:  Query: Q6_TDW, Run: 1, Total Duration: 00:02:58.494, Seconds: 178.494, Number of Rows: 750
INFO:  Query: Q6_SCD, Run: 1, Total Duration: 01:57:44.982, Seconds: 7064.982, Number of Rows: 750
INFO:  Execution Start: 2023-02-26 11:06:50.29803+01, Execution End: 2023-02-26 11:07:00.869392+01, Total Duration: 00:00:10.571362
 talgebra_queries
------------------
 The End
(1 row)

Time: 10495.866 ms (00:10.496)

-- Define the function that executes the benchmark of the OLAP queries
tpcds_sf1=> \i tolap_queries.sql
psql:tolap_queries.sql:41: NOTICE:  function tolap_queries() does not exist, skipping
DROP FUNCTION
Time: 0.453 ms
CREATE FUNCTION
Time: 20.734 ms
Time: 0.166 ms

-- Run the benchmark for the OLAP queries only once as stated by the argument.
-- You can run the queries several times by giving other values for the argument.
tpcds_sf1=> SELECT tolap_queries(1); -- Replace the argument with the number of times each query is executed
NOTICE:  table "tolap_queries" does not exist, skipping
INFO:  Query: Q1_MobDB, Run: 1, Total Duration: 00:21:14.718, Seconds: 1274.718, Number of Rows: 4421
INFO:  Query: Q1_TDW, Run: 1, Total Duration: 00:54:36.526, Seconds: 3276.526, Number of Rows: 4604
INFO:  Query: Q1_SCD, Run: 1, Total Duration: 00:49:31.08, Seconds: 2971.08, Number of Rows: 4604
INFO:  Query: Q2_MobDB, Run: 1, Total Duration: 00:54:58.816, Seconds: 3298.816, Number of Rows: 949
INFO:  Query: Q2_TDW, Run: 1, Total Duration: 01:55:36.957, Seconds: 6936.957, Number of Rows: 955
INFO:  Query: Q3_MobDB, Run: 1, Total Duration: 01:12:20.348, Seconds: 4340.348, Number of Rows: 17951
INFO:  Query: Q3_TDW, Run: 1, Total Duration: 01:55:46.427, Seconds: 6946.427, Number of Rows: 17951
INFO:  Query: Q3_SCD, Run: 1, Total Duration: 02:31:15.31, Seconds: 9075.31, Number of Rows: 17951
INFO:  Query: Q4_MobDB, Run: 1, Total Duration: 00:52:09.889, Seconds: 3129.889, Number of Rows: 1688
INFO:  Query: Q4_TDW, Run: 1, Total Duration: 00:36:23.757, Seconds: 2183.757, Number of Rows: 1730
INFO:  Query: Q4_SCD, Run: 1, Total Duration: 00:19:49.101, Seconds: 1189.101, Number of Rows: 1730
INFO:  Query: Q5_MobDB, Run: 1, Total Duration: 00:30:32.284, Seconds: 1832.284, Number of Rows: 945
INFO:  Query: Q6_MobDB, Run: 1, Total Duration: 01:02:22.289, Seconds: 22.289, Number of Rows: 693
INFO:  Query: Q6_TDW, Run: 1, Total Duration: 01:21:20.165, Seconds: 4880.165, Number of Rows: 750
INFO:  Query: Q6_SCD, Run: 1, Total Duration: 02:55:46.85, Seconds: 10546.85, Number of Rows: 750
INFO:  Execution Start: 2023-02-11 15:32:22.673339+01, Execution End: 2023-02-11 15:33:28.339794+01, Total Duration: 00:01:05.666455
 tolap_queries
---------------
 The End
(1 row)

Time: 65696.671 ms (01:05.697)

-- SQL query that fetches from the database the execution time of the queries
tpcds_sf1=> SELECT t1.QueryId, t1.Seconds AS Algebra, t2.Seconds AS OLAP
  FROM talgebra_queries t1, tolap_queries t2
  WHERE t1.QueryId = t2.QueryId
  ORDER BY t1.QueryId;

  queryid   | algebra  |   olap
------------+----------+----------
 Q1_MobDB   |    17.58 | 1274.718
 Q1_SCD     |  575.757 |  2971.08
 Q1_TDW     |  530.459 | 3276.526
 Q2_MobDB   |    4.665 | 3298.816
 Q2_TDW     | 1429.987 | 6936.957
 Q3_MobDB   |   71.595 | 4340.348
 Q3_SCD     |  279.679 |  9075.31
 Q3_TDW     |   56.766 | 6946.427
 Q4_MobDB   |   18.278 | 3129.889
 Q4_SCD     |   74.432 | 1189.101
 Q4_TDW     |   54.653 | 2183.757
 Q5_MobDB   |   19.896 | 1832.284
 Q6_MobDB   |   90.746 |   22.289
 Q6_SCD     |  7078.06 | 10546.85
 Q6_TDW     |   164.28 | 4880.165
(15 rows)

Time: 0.967 ms
```

To run the benchmark several times and obtain the average of the execution times not considering the first run you can do as follows
```sql
-- Run the benchmark for the algebra queries 6 times
tpcds_sf1=> SELECT talgebra_queries(6);
INFO:  Query: Q1_MobDB, Run: 1, Total Duration: 00:00:19.144, Seconds: 19.144, Number of Rows: 4421
...
INFO:  Query: Q6_SCD, Run: 6, Total Duration: 01:57:46.686, Seconds: 7066.686, Number of Rows: 750
INFO:  Execution Start: 2023-02-11 15:40:19.728245+01, Execution End: 2023-02-11 15:41:22.718604+01, Total Duration: 00:01:02.990359
 talgebra_queries
------------------
 The End
(1 row)

Time: 63025.500 ms (01:03.026)

tpcds_sf1=> SELECT tolap_queries(6); -- Replace the argument with the number of times each query is executed
INFO:  Query: Q1_MobDB, Run: 1, Total Duration: 00:21:07.541, Seconds: 1267.541, Number of Rows: 4421
...
INFO:  Query: Q6_SCD, Run: 6, Total Duration: 02:55:51.476, Seconds: 10551.476, Number of Rows: 750
INFO:  Execution Start: 2023-02-11 15:41:34.848755+01, Execution End: 2023-02-11 15:48:04.545894+01, Total Duration: 00:06:29.697139
 tolap_queries
---------------
 The End
(1 row)

Time: 389718.760 ms (06:29.719)

tpcds_sf1=> SELECT t1.QueryId, ROUND(AVG(t1.Seconds)::numeric,6) AS Algebra,
  ROUND(AVG(t2.Seconds)::numeric, 6) AS OLAP
FROM talgebra_queries t1, tolap_queries t2
WHERE t1.QueryId = t2.QueryId AND t1.RunId > 1 AND t2.RunId > 1
GROUP BY t1.QueryId
ORDER BY t1.QueryId;

  queryid   |   algebra   |     olap
------------+-------------+--------------
 Q1_MobDB   |   17.736400 |  1322.421800
 Q1_SCD     |  586.621600 |  3017.222000
 Q1_TDW     |  536.752000 |  3297.729600
 Q2_MobDB   |    4.785600 |  3347.211600
 Q2_TDW     | 1479.315400 |  6962.742600
 Q3_MobDB   |   72.306800 |  4417.048600
 Q3_SCD     |  279.820200 |  9157.247200
 Q3_TDW     |   57.201000 |  6622.169800
 Q4_MobDB   |   18.333200 |  3122.822400
 Q4_SCD     |   74.726200 |  1165.060200
 Q4_TDW     |   54.329600 |  2173.815000
 Q5_MobDB   |   19.958400 |  1716.511400
 Q6_MobDB   |   90.932600 |    24.189000
 Q6_SCD     | 7037.183600 | 10639.362600
 Q6_TDW     |  166.975000 |  4891.128200
(15 rows)

Time: 1.557 ms
```
License
-------

The documentation of this benchmark is licensed under a [Creative Commons Attribution-Share Alike 3.0 License](https://creativecommons.org/licenses/by-sa/3.0/)
