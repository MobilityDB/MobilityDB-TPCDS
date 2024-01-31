/*****************************************************************************
 *
 * This MobilityDB code is provided under The PostgreSQL License.
 * Copyright (c) 2016-2023, Université libre de Bruxelles and MobilityDB
 * contributors
 *
 * MobilityDB includes portions of PostGIS version 3 source code released
 * under the GNU General Public License (GPLv2 or later).
 * Copyright (c) 2001-2023, PostGIS contributors
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby granted, provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON
 * AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *****************************************************************************/

/**
 * @brief Scripts used to benchmark the temporal algebra queries on alternative
 * implementations of a temporal data warehouse using an excerpt of the
 * TPC-DS benchmark https://www.tpc.org/tpcds/.
 *
 * @note According to 
 * https://stackoverflow.com/questions/37873517/how-we-can-make-statement-timeout-work-inside-a-function
 * in PostgreSQL is not possible to set a timeout to stop long-running queries 
 * and continue executing a function. For this reason, the queries that exceed
 * the time out have been commented out in the code below.
 *
 * Execution at SF10 that motivated to comment out the queries that run more than 5 minutes
 *
 * @code
 * tpcds_sf10=# select talgebra_queries(1);
 * INFO:  Query: Q1_MobDB, Run: 1, Total Duration: 00:00:00.021344, Seconds: 0.021344, Number of Rows: 1093
 * INFO:  Query: Q1_TDW, Run: 1, Total Duration: 00:00:00.037376, Seconds: 0.037376, Number of Rows: 1094
 * INFO:  Query: Q1_SCD, Run: 1, Total Duration: 00:00:00.267792, Seconds: 0.267792, Number of Rows: 1094
 * INFO:  Query: Q2_MobDB, Run: 1, Total Duration: 00:00:00.259737, Seconds: 0.259737, Number of Rows: 50957
 * INFO:  Query: Q2_TDW, Run: 1, Total Duration: 00:05:57.801112, Seconds: 357.801112, Number of Rows: 50994
 * INFO:  Query: Q2_SCD, Run: 1, Total Duration: 00:10:12.473022, Seconds: 612.473022, Number of Rows: 50994
 * INFO:  Query: Q3_MobDB, Run: 1, Total Duration: 00:00:00.055781, Seconds: 0.055781, Number of Rows: 44
 * INFO:  Query: Q3_TDW, Run: 1, Total Duration: 00:00:00.053256, Seconds: 0.053256, Number of Rows: 44
 * INFO:  Query: Q3_SCD, Run: 1, Total Duration: 00:00:00.065464, Seconds: 0.065464, Number of Rows: 44
 * INFO:  Query: Q4_MobDB, Run: 1, Total Duration: 00:00:00.084666, Seconds: 0.084666, Number of Rows: 3225
 * INFO:  Query: Q4_TDW, Run: 1, Total Duration: 00:00:00.196392, Seconds: 0.196392, Number of Rows: 3256
 * INFO:  Query: Q4_SCD, Run: 1, Total Duration: 00:00:00.493055, Seconds: 0.493055, Number of Rows: 3256
 * INFO:  Query: Q5_MobDB, Run: 1, Total Duration: 00:00:05.967152, Seconds: 5.967152, Number of Rows: 1093
 * INFO:  Query: Q6_MobDB, Run: 1, Total Duration: 00:00:00.558739, Seconds: 0.558739, Number of Rows: 10
 * INFO:  Query: Q6_TDW, Run: 1, Total Duration: 00:00:01.340693, Seconds: 1.340693, Number of Rows: 10
 * INFO:  Execution Start: 2023-12-05 08:59:38.12409+01, Execution End: 2023-12-05 09:15:57.829497+01, Total Duration: 00:16:19.705407
 *  talgebra_queries
 * ------------------
 *  The End
 * (1 row)
 * tpcds_sf10=# select count(*) from q6_scd;
 * ^CCancel request sent
 * ERROR:  canceling statement due to user request
 * Time: 911757.784 ms (15:11.758)
 * @endcode
 */

DROP FUNCTION IF EXISTS get_sf;
CREATE OR REPLACE FUNCTION get_sf()
RETURNS integer AS $$
DECLARE
  dbname text;
  sf_text text;
BEGIN
  SELECT current_database() INTO dbname;
  SELECT replace(dbname, 'tpcds_sf', '') INTO sf_text;
  RETURN sf_text::integer;
END;
$$ LANGUAGE 'plpgsql';

DROP FUNCTION IF EXISTS talgebra_queries;
CREATE OR REPLACE FUNCTION talgebra_queries(times integer,
  newtable boolean DEFAULT true, detailed boolean DEFAULT false)
RETURNS text AS $$
DECLARE
  QueryId char(10);
  QueryText text;
  J json;
  InitialTime timestamptz; /* Initial time of the overall execution */
  FinalTime timestamptz;   /* Final time of the overall execution */
  StartTime timestamptz;
  PlanningTime float;
  ExecutionTime float;
  Duration interval;
  Seconds float;
  NumberRows bigint;
  RunId int;
  current_sf int;
BEGIN
IF newtable THEN
  DROP TABLE IF EXISTS talgebra_queries;
  CREATE TABLE talgebra_queries (
    QueryId char(10),
    RunId int,
    StartTime timestamptz,
    PlanningTime float,
    ExecutionTime float,
    Duration interval,
    Seconds float,
    NumberRows bigint,
    J json
  );
END IF;

-- Get the current scale factor
SELECT get_sf() INTO current_sf;

SELECT clock_timestamp() INTO InitialTime;

FOR RunId IN 1..times
LOOP
  SET log_error_verbosity to terse;

/******************************************************************************
 * Query 1: Temporal Selection
 * Time when an item is assigned to brand B
 *****************************************************************************/

-- Q1_MobDB ------------------------------------------

  QueryId = 'Q1_MobDB';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT * FROM Q1_MobDB
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Algebra Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Algebra Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO talgebra_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q1_TDW ------------------------------------------

  QueryId = 'Q1_TDW';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT * FROM Q1_TDW
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Algebra Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Algebra Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO talgebra_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q1_SCD ------------------------------------------

  QueryId = 'Q1_SCD';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT * FROM Q1_SCD
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Algebra Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Algebra Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO talgebra_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

/******************************************************************************
 * Query 2: Temporal Projection
 * Time when an item is assigned to any brand
 *****************************************************************************/

-- Q2_MobDB ------------------------------------------

  QueryId = 'Q2_MobDB';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT * FROM Q2_MobDB
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Algebra Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Algebra Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO talgebra_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q2_TDW ------------------------------------------

  QueryId = 'Q2_TDW';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT * FROM Q2_TDW
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Algebra Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Algebra Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO talgebra_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q2_SCD ------------------------------------------

/* 
 * Execute the OLAP version only for SFs < 100 since it takes more than 1h at SF 100
 * Query: Q2_SCD, Run: 1, Total Duration: 01:07:34.326571, Seconds: 4054.326571, Number of Rows: 101997
*/
  IF current_sf < 100 THEN
    QueryId = 'Q2_SCD';
    StartTime := clock_timestamp();

    EXPLAIN (ANALYZE, FORMAT JSON)
    SELECT * FROM Q2_SCD
    INTO J;

    PlanningTime := (J->0->>'Planning Time')::float;
    ExecutionTime := (J->0->>'Execution Time')::float;
    Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
    Seconds := EXTRACT(epoch FROM Duration);
    NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
    IF detailed THEN
      RAISE INFO 'Algebra Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
    ELSE
      RAISE INFO 'Algebra Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
        trim(QueryId), RunId, Duration, Seconds, NumberRows;
    END IF;
    INSERT INTO talgebra_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);
    END IF;

/******************************************************************************
 * Query 3: Temporal Join
 * Time when an item is assigned to brand B and its price is greater that 80
 *****************************************************************************/

-- Q3_MobDB ------------------------------------------

  QueryId = 'Q3_MobDB';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT * FROM Q3_MobDB
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Algebra Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Algebra Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO talgebra_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);
  
-- Q3_TDW ------------------------------------------

  QueryId = 'Q3_TDW';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT * FROM Q3_TDW
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Algebra Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Algebra Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO talgebra_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q3_SCD ------------------------------------------

  QueryId = 'Q3_SCD';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT * FROM Q3_SCD
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Algebra Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Algebra Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO talgebra_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

/******************************************************************************
 * Query 4: Temporal Union
 * Time when an item is assigned to brand B or its price is greater than 80 
 *****************************************************************************/

-- Q4_MobDB ------------------------------------------

  QueryId = 'Q4_MobDB';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT * FROM Q4_MobDB
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Algebra Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Algebra Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO talgebra_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q4_TDW ------------------------------------------

  QueryId = 'Q4_TDW';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT * FROM Q4_TDW
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Algebra Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Algebra Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO talgebra_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q4_SCD ------------------------------------------

  QueryId = 'Q4_SCD';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT * FROM Q4_SCD
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Algebra Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Algebra Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO talgebra_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

/******************************************************************************
 * Query 5: Temporal Difference
 * Time when an item is assigned to brand B and its price is not greater than 
 * 80 
 *****************************************************************************/

-- Q5_MobDB ------------------------------------------

  QueryId = 'Q5_MobDB';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT * FROM Q5_MobDB
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Algebra Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Algebra Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO talgebra_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q5_TDW ------------------------------------------

  QueryId = 'Q5_TDW';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT * FROM Q5_TDW
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Algebra Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Algebra Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO talgebra_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q5_SCD ------------------------------------------

  QueryId = 'Q5_SCD';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT * FROM Q5_SCD
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Algebra Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Algebra Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO talgebra_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

/******************************************************************************
 * Query 6: Temporal Aggregation
 * Time when a category has assigned to it more than 3 items
 *****************************************************************************/

-- Q6_MobDB ------------------------------------------

  QueryId = 'Q6_MobDB';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT * FROM Q6_MobDB
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Algebra Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Algebra Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO talgebra_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q6_TDW ------------------------------------------

  QueryId = 'Q6_TDW';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT * FROM Q6_TDW
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Algebra Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Algebra Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO talgebra_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q6_SCD ------------------------------------------

/* 
 * Execute the algebra version only for SFs < 10 since it takes more than 3h at SF 10
 * Query: Q6_SCD, Run: 1, Total Duration: 03:30:28.625489, Seconds: 12628.625489, Number of Rows: 10
 */
  IF current_sf < 10 THEN
    QueryId = 'Q6_SCD';
    StartTime := clock_timestamp();

    EXPLAIN (ANALYZE, FORMAT JSON)
    SELECT * FROM Q6_SCD
    INTO J;

    PlanningTime := (J->0->>'Planning Time')::float;
    ExecutionTime := (J->0->>'Execution Time')::float;
    Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
    Seconds := EXTRACT(epoch FROM Duration);
    NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
    IF detailed THEN
      RAISE INFO 'Algebra Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
    ELSE
      RAISE INFO 'Algebra Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
        trim(QueryId), RunId, Duration, Seconds, NumberRows;
    END IF;
    INSERT INTO talgebra_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);
  END IF;

-------------------------------------------------------------------------------

END LOOP;

  SELECT clock_timestamp() INTO FinalTime;

  RAISE INFO 'Execution Start: %, Execution End: %, Total Duration: %',
    InitialTime, FinalTime, FinalTime - InitialTime;

  -- Reset the timeout of queries to 0
  set statement_timeout=0;

  RETURN 'The End';
END;
$$ LANGUAGE 'plpgsql';


/*****************************************************************************/
