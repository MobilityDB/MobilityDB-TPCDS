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
 * @brief Scripts used to benchmark the temporal OLAP queries on alternative
 * implementations of a temporal data warehouse using an excerpt of the
 * TPC-DS benchmark https://www.tpc.org/tpcds/.
 * @note According to 
 * https://stackoverflow.com/questions/37873517/how-we-can-make-statement-timeout-work-inside-a-function
 * it is not possible to set a timeout for queries that stops a query and
 * continues executing a function. For this reason, the queries that exceed
 * the time out have been commented out in the code below.
 */

DROP FUNCTION IF EXISTS tolap_queries;
CREATE OR REPLACE FUNCTION tolap_queries(times integer,
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
  DROP TABLE IF EXISTS tolap_queries;
  CREATE TABLE tolap_queries (
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
 * Time when an item has brand B
 *****************************************************************************/

-- Q1_MobDB ------------------------------------------

  QueryId := 'Q1_MobDB';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_item_id, q.i_brandB_vt, SUM(s.ss_net_paid) AS TotalSales
  FROM store_sales s, date_dim d, Q1_MobDB q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = q.i_item_id AND
    q.i_brandB_vt * d.d_datespan IS NOT NULL
  GROUP BY q.i_item_id, q.i_brandB_vt
  ORDER BY q.i_item_id
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'OLAP Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'OLAP Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q1_TDW ------------------------------------------

  QueryId := 'Q1_TDW';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_item_id, q.FromDate, q.ToDate, SUM(s.ss_net_paid) AS TotalSales
  FROM store_sales s, date_dim d, Q1_TDW q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = q.i_item_id AND
    q.FromDate <= d.d_date AND d.d_date < q.ToDate
  GROUP BY q.i_item_id, q.FromDate, q.ToDate
  ORDER BY q.i_item_id, q.FromDate
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'OLAP Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'OLAP Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q1_SCD ------------------------------------------

  QueryId := 'Q1_SCD';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  -- Adding an i_item_id column to the fact table enables avoiding coalescing 
  SELECT q.i_item_id, q.FromDate, q.ToDate, SUM(s.ss_net_paid) AS TotalSales
  FROM store_sales s, date_dim d, Q1_SCD q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = q.i_item_id AND
    q.FromDate <= d.d_date AND d.d_date < q.ToDate
  GROUP BY q.i_item_id, q.FromDate, q.ToDate
  ORDER BY q.i_item_id, q.FromDate  
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'OLAP Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'OLAP Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

/******************************************************************************
 * Query 2: Temporal Projection
 * Total sales and time when an item has any brand
 *****************************************************************************/

-- Q2_MobDB ------------------------------------------

  QueryId := 'Q2_MobDB';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_item_id, q.b_item_anyBrand_vt, SUM(s.ss_net_paid) AS TotalSales
  FROM store_sales s, date_dim d, Q2_MobDB q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = q.i_item_id AND
    q.b_item_anyBrand_vt * d.d_datespan IS NOT NULL
  GROUP BY q.i_item_id, q.b_item_anyBrand_vt
  ORDER BY q.i_item_id
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'OLAP Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'OLAP Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q2_TDW ------------------------------------------

  QueryId := 'Q2_TDW';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_item_id, q.FromDate, q.ToDate, SUM(s.ss_net_paid) AS TotalSales
  FROM store_sales s, date_dim d, Q2_TDW q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = q.i_item_id AND
    q.FromDate <= d.d_date AND d.d_date < q.ToDate
  GROUP BY q.i_item_id, q.FromDate, q.ToDate
  ORDER BY q.i_item_id, q.FromDate
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'OLAP Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'OLAP Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q2_SCD ------------------------------------------

/* 
 * Execute the OLAP version only for SFs < 100 since it takes more than 1h at SF 100
 * Query: Q2_SCD, Run: 1, Total Duration: 01:07:34.326571, Seconds: 4054.326571, Number of Rows: 101997
*/
  IF current_sf < 100 THEN
    QueryId := 'Q2_SCD';
    StartTime := clock_timestamp();

    EXPLAIN (ANALYZE, FORMAT JSON)
    -- Adding an i_item_id column to the fact table enables avoiding coalescing 
    SELECT q.i_item_id, q.FromDate, q.ToDate, SUM(s.ss_net_paid) AS TotalSales
    FROM store_sales s, date_dim d, Q2_SCD q
    WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = q.i_item_id AND
      q.FromDate <= d.d_date AND d.d_date < q.ToDate
    GROUP BY q.i_item_id, q.FromDate, q.ToDate
    ORDER BY q.i_item_id, q.FromDate
    INTO J;

    PlanningTime := (J->0->>'Planning Time')::float;
    ExecutionTime := (J->0->>'Execution Time')::float;
    Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
    Seconds := EXTRACT(epoch FROM Duration);
    NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
    IF detailed THEN
      RAISE INFO 'OLAP Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
    ELSE
      RAISE INFO 'OLAP Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
        trim(QueryId), RunId, Duration, Seconds, NumberRows;
    END IF;
    INSERT INTO tolap_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);
  END IF;

/******************************************************************************
 * Query 3: Temporal Join
 * Total sales and time when an item has brand B and its price is
 * greater that 80
 *****************************************************************************/

-- Q3_MobDB ------------------------------------------

  QueryId := 'Q3_MobDB';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_item_id, i_brandBAndPriceGT80_vt, SUM(s.ss_net_paid) AS TotalSales
  FROM store_sales s, date_dim d, Q3_MobDB q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = q.i_item_id AND
    q.i_brandBAndPriceGT80_vt * d.d_datespan IS NOT NULL
  GROUP BY q.i_item_id, q.i_brandBAndPriceGT80_vt
  ORDER BY q.i_item_id
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'OLAP Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'OLAP Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);
  
-- Q3_TDW ------------------------------------------

  QueryId := 'Q3_TDW';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_item_id, q.FromDate, q.ToDate, SUM(s.ss_net_paid) AS TotalSales
  FROM store_sales s, date_dim d, Q3_TDW q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = q.i_item_id AND
    q.FromDate <= d.d_date AND d.d_date < q.ToDate
  GROUP BY q.i_item_id, q.FromDate, q.ToDate
  ORDER BY q.i_item_id, q.FromDate
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'OLAP Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'OLAP Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q3_SCD ------------------------------------------

  QueryId := 'Q3_SCD';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  -- Adding an i_item_id column to the fact table enables avoiding coalescing 
  SELECT q.i_item_id, q.FromDate, q.ToDate, SUM(s.ss_net_paid) AS TotalSales
  FROM store_sales s, date_dim d, Q3_SCD q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = q.i_item_id AND
    q.FromDate <= d.d_date AND d.d_date < q.ToDate
  GROUP BY q.i_item_id, q.FromDate, q.ToDate
  ORDER BY q.i_item_id, q.FromDate  
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'OLAP Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'OLAP Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

/******************************************************************************
 * Query 4: Temporal Union
 * Total sales and ime when an item has brand B or its price is
 * greater than 80 
 *****************************************************************************/

-- Q4_MobDB ------------------------------------------

  QueryId := 'Q4_MobDB';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  -- Adding an i_item_id column to the fact table enables avoiding coalescing 
  SELECT q.i_item_id, q.i_brandBOrPriceGT80_vt,
    SUM(s.ss_net_paid) AS TotalSales
  FROM store_sales s, date_dim d, Q4_MobDB q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = q.i_item_id AND
    q.i_brandBOrPriceGT80_vt * d.d_datespan IS NOT NULL
  GROUP BY q.i_item_id, i_brandBOrPriceGT80_vt
  ORDER BY q.i_item_id
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'OLAP Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'OLAP Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q4_TDW ------------------------------------------

  QueryId := 'Q4_TDW';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_item_id, q.FromDate, q.ToDate, SUM(s.ss_net_paid) AS TotalSales
  FROM store_sales s, date_dim d, Q4_TDW q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = q.i_item_id AND
    q.FromDate <= d.d_date AND d.d_date < q.ToDate
  GROUP BY q.i_item_id, q.FromDate, q.ToDate
  ORDER BY q.i_item_id, q.FromDate
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'OLAP Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'OLAP Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q4_SCD ------------------------------------------

  QueryId := 'Q4_SCD';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  -- Adding an i_item_id column to the fact table enables avoiding coalescing 
  SELECT q.i_item_id, q.FromDate, q.ToDate, SUM(s.ss_net_paid) AS TotalSales
  FROM store_sales s, date_dim d, Q4_SCD q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = q.i_item_id AND
    q.FromDate <= d.d_date AND d.d_date < q.ToDate
  GROUP BY q.i_item_id, q.FromDate, q.ToDate
  ORDER BY q.i_item_id, q.FromDate  
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'OLAP Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'OLAP Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

/******************************************************************************
 * Query 5: Temporal Difference
 * Total sales and time when an item has brand B and its price is
 * not greater than 80 
 *****************************************************************************/

-- Q5_MobDB ------------------------------------------

  QueryId := 'Q5_MobDB';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_item_id, q.i_brandBAndNotPriceGT80_vt,
    SUM(s.ss_net_paid) AS TotalSales
  FROM store_sales s, date_dim d, Q5_MobDB q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = q.i_item_id
  GROUP BY q.i_item_id, q.i_brandBAndNotPriceGT80_vt
  ORDER BY q.i_item_id
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'OLAP Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'OLAP Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q5_TDW ------------------------------------------

  QueryId := 'Q5_TDW';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_item_id, q.FromDate, q.ToDate, SUM(s.ss_net_paid) AS TotalSales 
  FROM store_sales s, date_dim d, Q5_TDW q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = q.i_item_id AND
    q.FromDate <= d.d_date AND d.d_date < q.ToDate 
  GROUP BY q.i_item_id, q.FromDate, q.ToDate
  ORDER BY q.i_item_id, q.FromDate
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'OLAP Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'OLAP Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q5_SCD ------------------------------------------

  QueryId := 'Q5_SCD';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  -- Adding an i_item_id column to the fact table enables avoiding coalescing 
  SELECT q.i_item_id, q.FromDate, q.ToDate, SUM(s.ss_net_paid) AS TotalSales 
  FROM store_sales s, date_dim d, Q5_SCD q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = q.i_item_id AND
    q.FromDate <= d.d_date AND d.d_date < q.ToDate 
  GROUP BY q.i_item_id, q.FromDate, q.ToDate
  ORDER BY q.i_item_id, q.FromDate
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'OLAP Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'OLAP Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

/******************************************************************************
 * Query 6: Temporal Aggregation
 * Total sales and time when a category has assigned to it more than 3 items
 *****************************************************************************/

-- Q6_MobDB ------------------------------------------

/* 
 * Execute the OLAP version only for SFs < 100 since it takes more than 1h at SF 100
 */
  IF current_sf < 100 THEN
    QueryId := 'Q6_MobDB';
    StartTime := clock_timestamp();

    EXPLAIN (ANALYZE, FORMAT JSON)
    SELECT q.i_category_id, ic.i_item_category_vt * q.i_gt3items_vt AS
      i_gt3items_vt, SUM(s.ss_net_paid) AS TotalSales
    FROM store_sales s, date_dim d, mobdb_item_category ic, Q6_MobDB q
    WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = ic.i_item_id AND
      ic.i_category_id = q.i_category_id AND 
      q.i_gt3items_vt * d.d_datespan IS NOT NULL AND
      -- Temporal join between item_category and Q6_MobDB
      ic.i_item_category_vt * q.i_gt3items_vt IS NOT NULL
    GROUP BY q.i_category_id, ic.i_item_category_vt * q.i_gt3items_vt
    ORDER BY q.i_category_id
    INTO J;

    PlanningTime := (J->0->>'Planning Time')::float;
    ExecutionTime := (J->0->>'Execution Time')::float;
    Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
    Seconds := EXTRACT(epoch FROM Duration);
    NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
    IF detailed THEN
      RAISE INFO 'OLAP Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
    ELSE
      RAISE INFO 'OLAP Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
        trim(QueryId), RunId, Duration, Seconds, NumberRows;
    END IF;
    INSERT INTO tolap_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);
  END IF;

-- Q6_TDW ------------------------------------------

  QueryId := 'Q6_TDW';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_category_id, greatest(ic.FromDate, q.FromDate) AS FromDate,
    least(ic.ToDate, q.ToDate) AS ToDate, SUM(s.ss_net_paid) AS TotalSales
  FROM store_sales s, date_dim d, tdw_item_category ic, Q6_TDW q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = ic.i_item_id AND 
    ic.i_category_id = q.i_category_id AND
    -- Temporal join between item_category and Q6_TDW
    greatest(ic.FromDate, q.FromDate) < least(ic.ToDate, q.ToDate) AND
    q.FromDate <= d.d_date AND d.d_date < q.ToDate
  GROUP BY q.i_category_id, ic.FromDate, ic.toDate, q.FromDate, q.toDate
  ORDER BY q.i_category_id, q.FromDate
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'OLAP Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'OLAP Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
    (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q6_SCD ------------------------------------------

/* 
 * Execute the OLAP version only for SFs < 10 since it takes more than 1h at SF 10
 * Query: Q6_SCD, Run: 1, Total Duration: 03:30:28.625489, Seconds: 12628.625489, Number of Rows: 10
 */
  IF current_sf < 10 THEN
    QueryId := 'Q6_SCD';
    StartTime := clock_timestamp();

    EXPLAIN (ANALYZE, FORMAT JSON)
    SELECT q.i_category_id, greatest(ic.FromDate, q.FromDate) AS FromDate,
      least(ic.ToDate, q.ToDate) AS ToDate, SUM(s.ss_net_paid) AS TotalSales
    FROM store_sales s, date_dim d, tdw_item_category ic, Q6_SCD q
    WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = ic.i_item_id AND 
      ic.i_category_id = q.i_category_id AND
      -- Temporal join between item_category and Q6_TDW
      greatest(ic.FromDate, q.FromDate) < least(ic.ToDate, q.ToDate) AND
      q.FromDate <= d.d_date AND d.d_date < q.ToDate
    GROUP BY q.i_category_id, ic.FromDate, ic.toDate, q.FromDate, q.toDate
    ORDER BY q.i_category_id, q.FromDate
    INTO J;

    PlanningTime := (J->0->>'Planning Time')::float;
    ExecutionTime := (J->0->>'Execution Time')::float;
    Duration := make_interval(secs := (PlanningTime + ExecutionTime) / 1000);
    Seconds := EXTRACT(epoch FROM Duration);
    NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
    IF detailed THEN
      RAISE INFO 'OLAP Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
    ELSE
      RAISE INFO 'OLAP Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
        trim(QueryId), RunId, Duration, Seconds, NumberRows;
    END IF;
    INSERT INTO tolap_queries VALUES
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
