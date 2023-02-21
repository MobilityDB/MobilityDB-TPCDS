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

-- Set the timeout of queries to 5 minutes
set statement_timeout=300000;

SELECT clock_timestamp() INTO InitialTime;

FOR RunId IN 1..times
LOOP
  SET log_error_verbosity to terse;


-------------------------------------------------------------------------------
-- Query 1: Temporal Selection
-- Time when an item has price between €5 and €10
-------------------------------------------------------------------------------

-- Q1_MobDB ------------------------------------------

  QueryId := 'Q1_MobDB';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_item_id, q.i_price5to10_vt, SUM(s.ss_sales_price) AS TotalSales
  FROM store_sales s, date_dim d, Q1_MobDB q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = q.i_item_id AND
    q.i_price5to10_vt @> d.d_date
  GROUP BY q.i_item_id, q.i_price5to10_vt
  ORDER BY q.i_item_id
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q1_TDW ------------------------------------------

  QueryId := 'Q1_TDW';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_item_id, q.FromDate, q.ToDate, SUM(s.ss_sales_price) AS TotalSales
  FROM store_sales s, date_dim d, Q1_TDW q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = q.i_item_id AND
    q.FromDate <= d.d_date AND d.d_date < q.ToDate
  GROUP BY q.i_item_id, q.FromDate, q.ToDate
  ORDER BY q.i_item_id, q.FromDate
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q1_SCD ------------------------------------------

  QueryId := 'Q1_SCD';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_item_id, q.FromDate, q.ToDate, SUM(s.ss_sales_price) AS TotalSales
  FROM store_sales s, date_dim d, scd_item i, Q1_SCD q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_sk = i.i_item_sk AND
    i.i_item_id = q.i_item_id AND q.FromDate <= d.d_date AND d.d_date < q.ToDate
  GROUP BY q.i_item_id, q.FromDate, q.ToDate
  ORDER BY q.i_item_id, q.FromDate
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-------------------------------------------------------------------------------
-- Query 2: Temporal Projection
-- Time when a brand was assigned to any category
-------------------------------------------------------------------------------

-- Q2_MobDB ------------------------------------------

  QueryId := 'Q2_MobDB';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_brand_id, q.b_brand_anycat_vt, SUM(s.ss_sales_price) AS TotalSales
  FROM store_sales s, date_dim d, mobdb_item i, mobdb_item_brand ib,
    Q2_MobDB q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = i.i_item_id AND
    i.i_item_id = ib.i_item_id AND ib.i_brand_id = q.i_brand_id AND 
    q.b_brand_anycat_vt @> d.d_date 
  GROUP BY q.i_brand_id, q.b_brand_anycat_vt
  ORDER BY q.i_brand_id
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q2_TDW ------------------------------------------

  QueryId := 'Q2_TDW';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_brand_id, q.FromDate, q.ToDate, SUM(s.ss_sales_price) AS TotalSales
  FROM store_sales s, date_dim d, tdw_item i, tdw_item_brand ib,
    Q2_TDW q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = i.i_item_id AND
    i.i_item_id = ib.i_item_id AND ib.i_brand_id = q.i_brand_id AND 
    q.FromDate <= d.d_date AND d.d_date < q.ToDate
  GROUP BY q.i_brand_id, q.FromDate, q.ToDate
  ORDER BY q.i_brand_id, q.FromDate
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q2_SCD ------------------------------------------

/*
  TIME OUT WITH 5 minutes

  QueryId := 'Q2_SCD';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_brand_id, q.FromDate, q.ToDate, SUM(s.ss_sales_price) AS TotalSales
  FROM store_sales s, date_dim d, scd_item i, Q2_SCD q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_sk = i.i_item_sk AND
    i.i_brand_id = q.i_brand_id AND
    q.FromDate <= d.d_date AND d.d_date < q.ToDate
  GROUP BY q.i_brand_id, q.FromDate, q.ToDate
  ORDER BY q.i_brand_id, q.FromDate
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);
*/

-------------------------------------------------------------------------------
-- Query 3: Temporal Join
-- Time when an item has a given price and a given brand
-------------------------------------------------------------------------------

-- Q3_MobDB ------------------------------------------

  QueryId := 'Q3_MobDB';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_item_id, q.i_item_price, q.i_brand_id, q.i_price_brand_vt,
    SUM(s.ss_sales_price) AS TotalSales
  FROM store_sales s, date_dim d, mobdb_item i, Q3_MobDB q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = i.i_item_id AND
    i.i_item_id = q.i_item_id AND q.i_price_brand_vt @> d.d_date
  GROUP BY q.i_item_id, q.i_item_price, q.i_brand_id, q.i_price_brand_vt
  ORDER BY q.i_item_id, q.i_price_brand_vt
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);
  
-- Q3_TDW ------------------------------------------

  QueryId := 'Q3_TDW';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_item_id, q.i_item_price, q.i_brand_id, q.FromDate, q.ToDate,
    SUM(s.ss_sales_price) AS TotalSales
  FROM store_sales s, date_dim d, Q3_TDW q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = q.i_item_id AND
    q.FromDate <= d.d_date AND d.d_date < q.ToDate
  GROUP BY q.i_item_id, q.i_item_price, q.i_brand_id, q.FromDate, q.ToDate
  ORDER BY q.i_item_id, q.FromDate
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q3_SCD ------------------------------------------

  QueryId := 'Q3_SCD';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_item_id, q.i_item_price, q.i_brand_id, q.FromDate, q.ToDate,
    SUM(s.ss_sales_price) AS SalesAmount
  FROM store_sales s, date_dim d, scd_item i, Q3_SCD q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_sk = i.i_item_sk AND
    i.i_item_id = q.i_item_id AND q.FromDate <= d.d_date AND d.d_date < q.ToDate
  GROUP BY q.i_item_id, q.i_item_price, q.i_brand_id, q.FromDate, q.ToDate
  ORDER BY q.i_item_id, q.FromDate
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-------------------------------------------------------------------------------
-- Query 4: Temporal Union
-- Time when items are assigned to brand A or when its price is greater than €20 
-------------------------------------------------------------------------------

-- Q4_MobDB ------------------------------------------

  QueryId := 'Q4_MobDB';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_item_id, q.i_brandAOrPriceGT20_vt,
    SUM(s.ss_sales_price) AS TotalSales
  FROM store_sales s, date_dim d, mobdb_item i, Q4_MobDB q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = i.i_item_id AND
    i.i_item_id = q.i_item_id AND q.i_brandAOrPriceGT20_vt @> d.d_date
  GROUP BY q.i_item_id, i_brandAOrPriceGT20_vt
  ORDER BY q.i_item_id
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q4_TDW ------------------------------------------

  QueryId := 'Q4_TDW';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_item_id, q.FromDate, q.ToDate, SUM(s.ss_sales_price) AS TotalSales
  FROM store_sales s, date_dim d, Q4_TDW q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = q.i_item_id AND
    q.FromDate <= d.d_date AND d.d_date < q.ToDate
  GROUP BY q.i_item_id, q.FromDate, q.ToDate
  ORDER BY q.i_item_id, q.FromDate
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q4_SCD ------------------------------------------

  QueryId := 'Q4_SCD';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_item_id, q.FromDate, q.ToDate, SUM(s.ss_sales_price) AS TotalSales
  FROM store_sales s, date_dim d, scd_item i, Q4_SCD q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_sk = i.i_item_sk AND
    i.i_item_id = q.i_item_id AND q.FromDate <= d.d_date AND d.d_date <= q.ToDate
  GROUP BY q.i_item_id, q.FromDate, q.ToDate
  ORDER BY q.i_item_id, q.FromDate
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-------------------------------------------------------------------------------
-- Query 5: Temporal Difference
-- Time when a brand was assigned to a single category
-------------------------------------------------------------------------------

-- Q5_MobDB ------------------------------------------

  QueryId := 'Q5_MobDB';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_brand_id, q.i_onecat_vt, SUM(s.ss_sales_price) AS TotalSales
  FROM store_sales s, date_dim d, mobdb_item i, mobdb_item_brand ib,
    Q5_MobDB q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = i.i_item_id AND
    i.i_item_id = ib.i_item_id AND ib.i_brand_id = q.i_brand_id
    -- AND ib.i_item_brand_vt * q.i_onecat_vt IS NOT NULL
  GROUP BY q.i_brand_id, q.i_onecat_vt
  ORDER BY q.i_brand_id
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q5_TDW ------------------------------------------

/*
  TIME OUT WITH 5 minutes
  QueryId := 'Q5_TDW';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_brand_id, q.FromDate, q.ToDate, SUM(s.ss_sales_price) AS TotalSales 
  FROM store_sales s, date_dim d, tdw_item i, tdw_item_brand ib, Q5_TDW q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = i.i_item_id AND
    i.i_item_id = ib.i_item_id AND ib.i_brand_id = q.i_brand_id AND
    q.FromDate <= d.d_date AND d.d_date < q.ToDate 
  GROUP BY q.i_brand_id, q.FromDate, q.ToDate
  ORDER BY q.i_brand_id, q.FromDate
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);
*/

-- Q5_SCD ------------------------------------------

/*
  TIME OUT WITH 5 minutes

  QueryId := 'Q5_SCD';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_brand_id, q.FromDate, q.ToDate, SUM(s.ss_sales_price)
  FROM store_sales s, date_dim d, scd_item i, Q5_SCD q 
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_sk = i.i_item_sk AND
    i.i_brand_id = q.i_brand_id AND q.FromDate <= d.d_date AND d.d_date < q.ToDate 
  GROUP BY q.i_brand_id, q.FromDate, q.ToDate
  ORDER BY q.i_brand_id, q.FromDate
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);
*/

-------------------------------------------------------------------------------
-- Query 6: Temporal Aggregation
-- Total sales and time when a brand has assigned at least 5 items
-------------------------------------------------------------------------------

-- Q6_MobDB ------------------------------------------

  QueryId := 'Q6_MobDB';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_brand_id, q.i_gt5items_vt, SUM(s.ss_sales_price) AS TotalSales
  FROM store_sales s, date_dim d, mobdb_item_brand ib, Q6_MobDB q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = ib.i_item_id AND 
    ib.i_brand_id = q.i_brand_id AND q.i_gt5items_vt @> d.d_date
  GROUP BY q.i_brand_id, q.i_gt5items_vt
  ORDER BY q.i_brand_id
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  Seconds := EXTRACT(MILLISECONDS FROM Duration)/1000;
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q6_TDW ------------------------------------------

  QueryId := 'Q6_TDW';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_brand_id, q.FromDate, q.toDate, SUM(s.ss_sales_price) AS TotalSales
  FROM store_sales s, date_dim d, tdw_item_brand ib, Q6_TDW q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_id = ib.i_item_id AND 
    ib.i_brand_id = q.i_brand_id AND q.FromDate <= d.d_date AND
    d.d_date < q.ToDate
  GROUP BY q.i_brand_id, q.FromDate, q.toDate
  ORDER BY q.i_brand_id, q.FromDate
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

-- Q6_SCD ------------------------------------------

  QueryId := 'Q6_SCD';
  StartTime := clock_timestamp();

  EXPLAIN (ANALYZE, FORMAT JSON)
  SELECT q.i_brand_id, q.FromDate, q.toDate, SUM(s.ss_sales_price) AS TotalSales
  FROM store_sales s, date_dim d, scd_item i, Q6_SCD q
  WHERE s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_sk = i.i_item_sk AND 
    i.i_brand_id = q.i_brand_id AND q.FromDate <= d.d_date AND
    d.d_date < q.ToDate
  GROUP BY q.i_brand_id, q.FromDate, q.toDate
  ORDER BY q.i_brand_id, q.FromDate, q.toDate
  INTO J;

  PlanningTime := (J->0->>'Planning Time')::float;
  ExecutionTime := (J->0->>'Execution Time')::float;
  Duration := make_interval(secs := PlanningTime + ExecutionTime);
  Seconds := EXTRACT(epoch FROM Duration);
  NumberRows := (J->0->'Plan'->>'Actual Rows')::bigint;
  IF detailed THEN
    RAISE INFO 'Query: %, Run: %, Start Time: %, Planning Time: % milisecs, Execution Time: % secs, Total Duration: %, Seconds: %, Number of Rows: %',
    trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows;
  ELSE
    RAISE INFO 'Query: %, Run: %, Total Duration: %, Seconds: %, Number of Rows: %',
      trim(QueryId), RunId, Duration, Seconds, NumberRows;
  END IF;
  INSERT INTO tolap_queries VALUES
      (trim(QueryId), RunId, StartTime, PlanningTime, ExecutionTime, Duration, Seconds, NumberRows, J);

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
