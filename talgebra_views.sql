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
 * @brief Scripts defining the views used to benchmark the temporal algebra
 * and the temporal OLAP queries on alternative implementations of a temporal
 * data warehouse using an excerpt of the TPC-DS benchmark
 * https://www.tpc.org/tpcds/.
 *
 * The MobilityDB queries have two alternative aggregations, one of which is
 * commented out, e.g., in Query 1 below
 * - tunion(i_item_price_vt)
 * - unnest(spans(tunion(i_item_price_vt)))
 * The first version returns a period set and is the standard MobilityDB 
 * answer. The second version is used for verifying that the number of
 * rows obtained in the MobilityDB and the TDW and SCD versions coincide.
 */
 
/******************************************************************************
 * Query 1: Temporal Selection
 * Time when an item has price between €5 and €10
 *****************************************************************************/

-- Q1_MobDB ------------------------------------------

DROP VIEW IF EXISTS Q1_MobDB;
CREATE VIEW Q1_MobDB(i_item_id, i_price5to10_vt) AS 
SELECT i_item_id, 
  tunion(i_item_price_vt)
  -- unnest(spans(tunion(i_item_price_vt)))
FROM mobdb_item_price
WHERE i_item_price > 5 AND i_item_price < 10
GROUP BY i_item_id
ORDER BY i_item_id;

-- Q1_TDW ------------------------------------------

DROP VIEW IF EXISTS Q1_TDW;
CREATE VIEW Q1_TDW(i_item_id, FromDate, ToDate) AS 
WITH ItemPrice5to10(i_item_id, FromDate, ToDate) AS (
  SELECT i_item_id, FromDate, ToDate
  FROM tdw_item_price
  WHERE i_item_price > 5 AND i_item_price < 10 
)
-- Coalesce the result
SELECT DISTINCT f.i_item_id, f.FromDate, l.ToDate 
FROM ItemPrice5to10 f, ItemPrice5to10 l 
WHERE f.i_item_id = l.i_item_id AND
  f.FromDate < l.ToDate AND NOT EXISTS (
  SELECT * 
  FROM ItemPrice5to10 m1 
  WHERE f.i_item_id = m1.i_item_id AND f.FromDate < m1.FromDate AND
    m1.FromDate <= l.ToDate AND NOT EXISTS (
      SELECT * 
      FROM ItemPrice5to10 m2 
      WHERE f.i_item_id = m2.i_item_id 
      AND m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
  NOT EXISTS (
    SELECT * 
  FROM ItemPrice5to10 m 
  WHERE f.i_item_id = m.i_item_id AND 
  ( ( m.FromDate < f.FromDate AND f.FromDate <= m.ToDate ) OR 
    ( m.FromDate <= l.ToDate AND l.ToDate < m.ToDate ) ) )
ORDER BY f.i_item_id, f.FromDate;

-- Q1_SCD ------------------------------------------

DROP VIEW IF EXISTS Q1_SCD;
CREATE VIEW Q1_SCD(i_item_id, FromDate, ToDate) AS 
WITH ItemPrice5to10(i_item_id, FromDate, ToDate) AS (
  SELECT i_item_id, i_rec_start_date, i_rec_end_date
  FROM scd_item
  WHERE i_current_price > 5 AND i_current_price < 10 
)
-- Coalesce the table above
SELECT DISTINCT f.i_item_id, f.FromDate, l.ToDate 
FROM ItemPrice5to10 f, ItemPrice5to10 l 
WHERE f.i_item_id = l.i_item_id AND
  f.FromDate < l.ToDate AND NOT EXISTS (
  SELECT * 
  FROM ItemPrice5to10 m1 
  WHERE f.i_item_id = m1.i_item_id AND f.FromDate < m1.FromDate AND
    m1.FromDate <= l.ToDate AND NOT EXISTS (
      SELECT * 
      FROM ItemPrice5to10 m2 
      WHERE f.i_item_id = m2.i_item_id 
      AND m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
  NOT EXISTS (
    SELECT * 
  FROM ItemPrice5to10 m 
  WHERE f.i_item_id = m.i_item_id AND 
  ( ( m.FromDate < f.FromDate AND f.FromDate <= m.ToDate ) OR 
    ( m.FromDate <= l.ToDate AND l.ToDate < m.ToDate ) ) )
ORDER BY f.i_item_id, f.FromDate;

/******************************************************************************
 * Query 2: Temporal Projection
 * Time when a brand was assigned to at least one category
 *****************************************************************************/

-- Q2_MobDB ------------------------------------------

DROP VIEW IF EXISTS Q2_MobDB;
CREATE VIEW Q2_MobDB(i_brand_id, b_brand_anycat_vt) AS 
SELECT i_brand_id, 
  tunion(i_brand_category_vt)
  -- unnest(spans(tunion(i_brand_category_vt)))
FROM mobdb_brand_category
GROUP BY i_brand_id
ORDER BY i_brand_id;

-- (949 rows) -> (955 rows)
-- Time: 6.316 ms

-- Q2_TDW ------------------------------------------

DROP VIEW IF EXISTS Q2_TDW;
CREATE VIEW Q2_TDW(i_brand_id, FromDate, ToDate) AS 
WITH BrandAnyCatAll(i_brand_id, FromDate, ToDate) AS (
  SELECT i_brand_id, FromDate, ToDate
  FROM tdw_brand_category 
)
-- Coalesce the previous table
SELECT DISTINCT f.i_brand_id, f.FromDate, l.ToDate 
FROM BrandAnyCatAll f, BrandAnyCatAll l 
WHERE f.i_brand_id = l.i_brand_id AND f.FromDate < l.ToDate AND NOT EXISTS (
  SELECT * 
  FROM BrandAnyCatAll m1 
  WHERE f.i_brand_id = m1.i_brand_id AND f.FromDate < m1.FromDate AND
    m1.FromDate <= l.ToDate AND NOT EXISTS (
      SELECT * 
      FROM BrandAnyCatAll m2 
      WHERE f.i_brand_id = m2.i_brand_id 
      AND m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
  NOT EXISTS (
    SELECT * 
  FROM BrandAnyCatAll m 
  WHERE f.i_brand_id = m.i_brand_id AND 
  ( ( m.FromDate < f.FromDate AND f.FromDate <= m.ToDate ) OR 
    ( m.FromDate <= l.ToDate AND l.ToDate < m.ToDate ) ) )
ORDER BY f.i_brand_id, f.FromDate;

-- Q2_SCD ------------------------------------------

DROP VIEW IF EXISTS Q2_SCD;
CREATE VIEW Q2_SCD(i_brand_id, FromDate, ToDate) AS 
WITH BrandAnyCatAll(i_brand_id, FromDate, ToDate) AS (
  SELECT i_brand_id, i_rec_start_date, i_rec_end_date
  FROM scd_item
)
-- Coalesce the previous table
SELECT DISTINCT f.i_brand_id, f.FromDate, l.ToDate 
FROM BrandAnyCatAll f, BrandAnyCatAll l 
WHERE f.i_brand_id = l.i_brand_id AND f.FromDate < l.ToDate AND NOT EXISTS (
  SELECT * 
  FROM BrandAnyCatAll m1 
  WHERE f.i_brand_id = m1.i_brand_id AND f.FromDate < m1.FromDate AND
    m1.FromDate <= l.ToDate AND NOT EXISTS (
      SELECT * 
      FROM BrandAnyCatAll m2 
      WHERE f.i_brand_id = m2.i_brand_id 
      AND m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
  NOT EXISTS (
    SELECT * 
  FROM BrandAnyCatAll m 
  WHERE f.i_brand_id = m.i_brand_id AND 
  ( ( m.FromDate < f.FromDate AND f.FromDate <= m.ToDate ) OR 
    ( m.FromDate <= l.ToDate AND l.ToDate < m.ToDate ) ) )
ORDER BY f.i_brand_id, f.FromDate;

/******************************************************************************
 * Query 3: Temporal Join
 * Time when an item has a given price and a given brand
 *****************************************************************************/

-- Q3_MobDB ------------------------------------------

DROP VIEW IF EXISTS Q3_MobDB;
CREATE VIEW Q3_MobDB(i_item_id, i_item_price, i_brand_id, i_price_brand_vt) AS 
SELECT p.i_item_id, p.i_item_price, b.i_brand_id, 
  p.i_item_price_vt * b.i_item_brand_vt AS i_price_brand_vt
  -- unnest(spans(p.i_item_price_vt * b.i_item_brand_vt)) AS i_price_brand_vt
FROM mobdb_item_price p, mobdb_item_brand b
WHERE p.i_item_id = b.i_item_id AND
  p.i_item_price_vt * b.i_item_brand_vt IS NOT NULL
ORDER BY i_item_id, i_price_brand_vt;
  
-- Q3_TDW ------------------------------------------

DROP VIEW IF EXISTS Q3_TDW;
CREATE VIEW Q3_TDW(i_item_id, i_item_price, i_brand_id, FromDate, ToDate) AS 
SELECT DISTINCT p.i_item_id, p.i_item_price, b.i_brand_id,
  greatest(p.FromDate, b.FromDate), least(p.ToDate, b.ToDate)
FROM tdw_item_price p, tdw_item_brand b
WHERE p.i_item_id = b.i_item_id AND
  greatest(p.FromDate, b.FromDate) < least(p.ToDate, b.ToDate)
ORDER BY i_item_id, greatest(p.FromDate, b.FromDate);

-- Q3_SCD ------------------------------------------

DROP VIEW IF EXISTS Q3_SCD;
CREATE VIEW Q3_SCD(i_item_id, i_item_price, i_brand_id, FromDate, ToDate) AS
WITH ItemPriceBrandAll(i_item_id, i_item_price, i_brand_id, FromDate, ToDate) AS (
  SELECT i_item_id, i_current_price, i_brand_id, i_rec_start_date, i_rec_end_date
  FROM scd_item
)
SELECT DISTINCT f.i_item_id, f.i_item_price, f.i_brand_id, f.FromDate, l.ToDate
FROM ItemPriceBrandAll f, ItemPriceBrandAll l
WHERE f.i_item_id = l.i_item_id AND f.i_item_price = l.i_item_price AND
  f.i_brand_id = l.i_brand_id AND f.FromDate < l.ToDate AND NOT EXISTS (
    SELECT *
    FROM ItemPriceBrandAll m1
    WHERE f.i_item_id = m1.i_item_id AND f.i_item_price = m1.i_item_price AND
      f.i_brand_id = m1.i_brand_id AND
      f.FromDate < m1.FromDate AND m1.FromDate <= l.ToDate AND 
      NOT EXISTS (
        SELECT *
        FROM ItemPriceBrandAll m2
        WHERE f.i_item_id = m2.i_item_id AND f.i_item_price = m2.i_item_price AND
          m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
  NOT EXISTS (
    SELECT *
    FROM ItemPriceBrandAll m
    WHERE f.i_item_id = m.i_item_id AND f.i_item_price = m.i_item_price AND
      f.i_brand_id = m.i_brand_id AND
      ( (m.FromDate < f.FromDate AND f.FromDate <= m.ToDate) OR
        (m.FromDate <= l.ToDate AND l.ToDate < m.ToDate) ) )
ORDER BY i_item_id, f.FromDate; 

/******************************************************************************
 * Query 4: Temporal Union
 * Time when items are assigned to brand A or when its price is greater than 20 
 *****************************************************************************/

-- Q4_MobDB ------------------------------------------

DROP VIEW IF EXISTS Q4_MobDB;
CREATE VIEW Q4_MobDB(i_item_id, i_brandAOrPriceGT20_vt) AS
WITH brandAOrPriceGT20(i_item_id, i_brandAOrPriceGT20_vt) AS (
  SELECT i_item_id, i_item_brand_vt
  FROM mobdb_item_brand
  WHERE i_brand_id = 5004001
  UNION
  SELECT i_item_id, i_item_price_vt
  FROM mobdb_item_price
  WHERE i_item_price > 20
)
SELECT i_item_id, 
  tunion(i_brandAOrPriceGT20_vt)
  -- unnest(spans(tunion(i_brandAOrPriceGT20_vt)))
FROM brandAOrPriceGT20
GROUP BY i_item_id
ORDER BY i_item_id;

-- Q4_TDW ------------------------------------------

DROP VIEW IF EXISTS Q4_TDW;
CREATE VIEW Q4_TDW(i_item_id, FromDate, ToDate) AS
WITH BrandAOrPriceGT20All(i_item_id, FromDate, ToDate) AS (
  SELECT i_item_id, ib.FromDate, ib.ToDate
  FROM tdw_item_brand ib, tdw_brand b
  WHERE ib.i_brand_id = b.i_brand_id AND b.i_brand_id = 5004001
  UNION
  SELECT i_item_id, FromDate, ToDate
  FROM tdw_item_price
  WHERE i_item_price > 20
)
-- Coalesce the above
SELECT DISTINCT f.i_item_id, f.FromDate, l.ToDate
FROM brandAOrPriceGT20All f, brandAOrPriceGT20All l
WHERE f.i_item_id = l.i_item_id AND f.FromDate < l.ToDate AND
  NOT EXISTS (
    SELECT *
    FROM brandAOrPriceGT20All m1
    WHERE f.i_item_id = m1.i_item_id AND f.FromDate < m1.FromDate AND
      m1.FromDate <= l.ToDate AND NOT EXISTS (
        SELECT *
        FROM brandAOrPriceGT20All m2
        WHERE f.i_item_id = m2.i_item_id AND m2.FromDate < m1.FromDate AND
          m1.FromDate <= m2.ToDate ) ) AND
  NOT EXISTS (
    SELECT *
    FROM brandAOrPriceGT20All m
    WHERE f.i_item_id = m.i_item_id AND
    ( (m.FromDate < f.FromDate AND f.FromDate <= m.ToDate) OR
      (m.FromDate <= l.ToDate AND l.ToDate < m.ToDate) ) )
ORDER BY f.i_item_id, f.FromDate; 

-- Q4_SCD ------------------------------------------

DROP VIEW IF EXISTS Q4_SCD;
CREATE VIEW Q4_SCD(i_item_id, FromDate, ToDate) AS
WITH BrandAOrPriceGT20All(i_item_id, FromDate, ToDate) AS (
  SELECT i_item_id, i_rec_start_date, i_rec_end_date
  FROM scd_item
  WHERE (i_brand_id = 5004001 OR i_current_price > 20)
)
-- Coalesce the above
SELECT DISTINCT f.i_item_id, f.FromDate, l.ToDate
FROM BrandAOrPriceGT20All f, BrandAOrPriceGT20All l
WHERE f.i_item_id = l.i_item_id AND f.FromDate < l.ToDate AND
  NOT EXISTS (
    SELECT *
    FROM BrandAOrPriceGT20All m1
    WHERE f.i_item_id = m1.i_item_id AND f.FromDate < m1.FromDate AND
      m1.FromDate <= l.ToDate AND NOT EXISTS (
        SELECT *
        FROM BrandAOrPriceGT20All m2
        WHERE f.i_item_id = m2.i_item_id AND m2.FromDate < m1.FromDate AND
          m1.FromDate <= m2.ToDate ) ) AND
  NOT EXISTS (
    SELECT *
    FROM BrandAOrPriceGT20All m
    WHERE f.i_item_id = m.i_item_id AND
      ( (m.FromDate < f.FromDate AND f.FromDate <= m.ToDate) OR
        (m.FromDate <= l.ToDate AND l.ToDate < m.ToDate) ) ) 
ORDER BY f.i_item_id, f.FromDate;

/******************************************************************************
 * Query 5: Temporal Difference
 * Time when a brand was assigned to a single category
 *****************************************************************************/

-- Q5_MobDB ------------------------------------------

DROP VIEW IF EXISTS Q5_MobDB;
CREATE VIEW Q5_MobDB(i_brand_id, i_onecat_vt) AS
WITH BrandNoItems(i_brand_id, noItems) AS (
  SELECT i_brand_id, tcount(i_brand_category_vt)
  FROM mobdb_brand_category
  GROUP BY i_brand_id
)
SELECT i_brand_id, 
  whenTrue(noItems #= 1)
  -- unnest(spans(whenTrue(noItems #= 1)))
FROM BrandNoItems
WHERE whenTrue(noItems #= 1) IS NOT NULL
ORDER BY i_brand_id;

-- Q5_TDW ------------------------------------------

DROP VIEW IF EXISTS Q5_TDW;
CREATE VIEW Q5_TDW(i_brand_id, FromDate, ToDate) AS
-- Time when a brand was assigned to at least one category
WITH BrandAnyCatAll(i_brand_id, FromDate, ToDate) AS (
  SELECT i_brand_id, FromDate, ToDate
  FROM tdw_brand_category 
),
-- Coalesce the above
BrandAnyCat(i_brand_id, FromDate, ToDate) AS (
  SELECT DISTINCT f.i_brand_id, f.FromDate, l.ToDate 
  FROM BrandAnyCatAll f, BrandAnyCatAll l 
  WHERE f.i_brand_id = l.i_brand_id AND f.FromDate < l.ToDate AND NOT EXISTS (
    SELECT * 
    FROM BrandAnyCatAll m1 
    WHERE f.i_brand_id = m1.i_brand_id AND f.FromDate < m1.FromDate AND
      m1.FromDate <= l.ToDate AND NOT EXISTS (
        SELECT * 
        FROM BrandAnyCatAll m2 
        WHERE f.i_brand_id = m2.i_brand_id 
        AND m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
    NOT EXISTS (
      SELECT * 
    FROM BrandAnyCatAll m 
    WHERE f.i_brand_id = m.i_brand_id AND 
    ( ( m.FromDate < f.FromDate AND f.FromDate <= m.ToDate ) OR 
      ( m.FromDate <= l.ToDate AND l.ToDate < m.ToDate ) ) )
),
-- Time when a brand was assigned to at least two categories
BrandTwoCatAll(i_brand_id, FromDate, ToDate) AS (
  SELECT b1.i_brand_id, 
    greatest(b1.FromDate, b2.FromDate),
    least(b1.ToDate, b2.ToDate)
  FROM tdw_brand_category b1, tdw_brand_category b2
  WHERE b1.i_brand_id = b2.i_brand_id AND b1.i_category_id <> b2.i_category_id AND
    greatest(b1.FromDate, b2.FromDate) < least(b1.ToDate, b2.ToDate)
),
-- Coalesce the above
BrandTwoCat(i_brand_id, FromDate, ToDate) AS (
  SELECT DISTINCT f.i_brand_id, f.FromDate, l.ToDate 
  FROM BrandTwoCatAll f, BrandTwoCatAll l 
  WHERE f.i_brand_id = l.i_brand_id AND f.FromDate < l.ToDate AND NOT EXISTS (
    SELECT * 
    FROM BrandTwoCatAll m1 
    WHERE f.i_brand_id = m1.i_brand_id AND f.FromDate < m1.FromDate AND
      m1.FromDate <= l.ToDate AND NOT EXISTS (
        SELECT * 
        FROM BrandTwoCatAll m2 
        WHERE f.i_brand_id = m2.i_brand_id 
        AND m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
    NOT EXISTS (
      SELECT * 
    FROM BrandTwoCatAll m 
    WHERE f.i_brand_id = m.i_brand_id AND 
    ( ( m.FromDate < f.FromDate AND f.FromDate <= m.ToDate ) OR 
      ( m.FromDate <= l.ToDate AND l.ToDate < m.ToDate ) ) )
),
-- Temporal difference between BrandAnyCat and BrandTwoCat
BrandOneCatAll(i_brand_id, FromDate, ToDate) AS (
  -- Case 1
  -- X----B1----|
  --      X----B2----|
  --   |----B3----|
  SELECT b1.i_brand_id, b1.FromDate, b2.FromDate
  FROM BrandAnyCat b1, BrandTwoCat b2
  WHERE b1.i_brand_id = b2.i_brand_id AND b1.FromDate < b2.FromDate AND
    NOT EXISTS (
      SELECT *
      FROM BrandTwoCat b3
      WHERE b1.i_brand_id = b3.i_brand_id AND
        b1.FromDate < b3.ToDate AND b3.FromDate < b2.FromDate )
  UNION
  -- Case 2
  --      |----B1----X
  -- |----B2----X
  --   |----B3----|
  SELECT b1.i_brand_id, b2.ToDate, b1.ToDate
  FROM BrandAnyCat b1, BrandTwoCat b2
  WHERE b1.i_brand_id = b2.i_brand_id AND b2.ToDate < b1.ToDate AND
    NOT EXISTS (
      SELECT *
      FROM BrandTwoCat b3
      WHERE b1.i_brand_id = b3.i_brand_id AND 
        b2.ToDate < b3.ToDate AND b3.FromDate < b1.ToDate )
  UNION
  -- Case 3
  -- |-----------B1-----------|
  --   |--B2--X     X--B3--|
  --      |--B4--|
  -- 
  SELECT b1.i_brand_id, b2.ToDate, b3.FromDate
  FROM BrandAnyCat b1, BrandTwoCat b2, BrandTwoCat b3
  WHERE b2.ToDate < b3.FromDate AND
    b1.i_brand_id = b2.i_brand_id AND b1.i_brand_id = b3.i_brand_id AND 
    NOT EXISTS (
      SELECT *
      FROM BrandTwoCat b4
      WHERE b1.i_brand_id = b4.i_brand_id AND
        b2.ToDate < b4.ToDate AND b4.FromDate < b3.FromDate ) -- 4 rows
  UNION
  -- Case 4
  -- |----B1----|
  -- |----B2----|
  SELECT i_brand_id, FromDate, ToDate
  FROM BrandAnyCat b1 
  WHERE NOT EXISTS (
    SELECT *
    FROM BrandTwoCat b2
    WHERE b1.i_brand_id = b2.i_brand_id AND 
      b1.FromDate < b2.ToDate AND b2.FromDate < b1.ToDate )
),
-- Coalesce the above
BrandOneCat(i_brand_id, FromDate, ToDate) AS (
  SELECT DISTINCT f.i_brand_id, f.FromDate, l.ToDate
  FROM BrandOneCatAll f, BrandOneCatAll l
  WHERE f.i_brand_id = l.i_brand_id AND f.FromDate < l.ToDate AND NOT EXISTS (
    SELECT *
    FROM BrandOneCatAll m1
    WHERE f.i_brand_id = m1.i_brand_id AND f.FromDate < m1.FromDate AND
      m1.FromDate <= l.ToDate AND NOT EXISTS (
        SELECT *
        FROM BrandOneCatAll m2
        WHERE f.i_brand_id = m2.i_brand_id AND
          m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
  NOT EXISTS (
    SELECT *
    FROM BrandOneCatAll m
    WHERE f.i_brand_id = m.i_brand_id AND 
    ( (m.FromDate < f.FromDate AND f.FromDate <= m.ToDate) OR
      (m.FromDate <= l.ToDate AND l.ToDate < m.ToDate) ) ) )
SELECT * FROM BrandOneCat
ORDER BY i_brand_id, FromDate;

-- Q5_SCD ------------------------------------------

DROP VIEW IF EXISTS Q5_SCD;
CREATE VIEW Q5_SCD(i_brand_id, FromDate, ToDate) AS
-- Time when a brand was assigned to at least one category
WITH BrandAnyCatAll(i_brand_id, FromDate, ToDate) AS (
  SELECT i_brand_id, i_rec_start_date, i_rec_end_date
  FROM scd_item 
),
-- Coalesce the above
BrandAnyCat(i_brand_id, FromDate, ToDate) AS (
  SELECT DISTINCT f.i_brand_id, f.FromDate, l.ToDate 
  FROM BrandAnyCatAll f, BrandAnyCatAll l 
  WHERE f.i_brand_id = l.i_brand_id AND f.FromDate < l.ToDate AND NOT EXISTS (
    SELECT * 
    FROM BrandAnyCatAll m1 
    WHERE f.i_brand_id = m1.i_brand_id AND f.FromDate < m1.FromDate AND
      m1.FromDate <= l.ToDate AND NOT EXISTS (
        SELECT * 
        FROM BrandAnyCatAll m2 
        WHERE f.i_brand_id = m2.i_brand_id 
        AND m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
    NOT EXISTS (
      SELECT * 
    FROM BrandAnyCatAll m 
    WHERE f.i_brand_id = m.i_brand_id AND 
    ( ( m.FromDate < f.FromDate AND f.FromDate <= m.ToDate ) OR 
      ( m.FromDate <= l.ToDate AND l.ToDate < m.ToDate ) ) )
),
-- Time when a brand was assigned to at least two categories
BrandTwoCatAll(i_brand_id, FromDate, ToDate) AS (
  SELECT b1.i_brand_id, 
    greatest(b1.i_rec_start_date, b2.i_rec_end_date),
    least(b1.i_rec_end_date, b2.i_rec_end_date)
  FROM scd_item b1, scd_item b2
  WHERE b1.i_brand_id = b2.i_brand_id AND b1.i_category_id <> b2.i_category_id AND
    greatest(b1.i_rec_start_date, b2.i_rec_start_date) < 
      least(b1.i_rec_end_date, b2.i_rec_end_date)
),
-- Coalesce the above
BrandTwoCat(i_brand_id, FromDate, ToDate) AS (
  SELECT DISTINCT f.i_brand_id, f.FromDate, l.ToDate 
  FROM BrandTwoCatAll f, BrandTwoCatAll l 
  WHERE f.i_brand_id = l.i_brand_id AND f.FromDate < l.ToDate AND NOT EXISTS (
    SELECT * 
    FROM BrandTwoCatAll m1 
    WHERE f.i_brand_id = m1.i_brand_id AND f.FromDate < m1.FromDate AND
      m1.FromDate <= l.ToDate AND NOT EXISTS (
        SELECT * 
        FROM BrandTwoCatAll m2 
        WHERE f.i_brand_id = m2.i_brand_id 
        AND m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
    NOT EXISTS (
      SELECT * 
    FROM BrandTwoCatAll m 
    WHERE f.i_brand_id = m.i_brand_id AND 
    ( ( m.FromDate < f.FromDate AND f.FromDate <= m.ToDate ) OR 
      ( m.FromDate <= l.ToDate AND l.ToDate < m.ToDate ) ) )
),
-- Temporal difference between BrandAnyCat and BrandTwoCat
BrandOneCatAll(i_brand_id, FromDate, ToDate) AS (
  -- Case 1
  -- X----B1----|
  --      X----B2----|
  --   |----B4----|
  SELECT b1.i_brand_id, b1.FromDate, b2.FromDate
  FROM BrandAnyCat b1, BrandTwoCat b2
  WHERE b1.i_brand_id = b2.i_brand_id AND b1.FromDate < b2.FromDate AND
    NOT EXISTS (
      SELECT *
      FROM BrandTwoCat b4
      WHERE b1.i_brand_id = b4.i_brand_id AND
        b1.FromDate < b4.ToDate AND b4.FromDate < b2.FromDate ) -- 670
  UNION
  -- Case 2
  --      |----B1----X
  -- |----B2----X
  --   |----B4----|
  SELECT b1.i_brand_id, b2.ToDate, b1.ToDate
  FROM BrandAnyCat b1, BrandTwoCat b2
  WHERE b1.i_brand_id = b2.i_brand_id AND b2.ToDate < b1.ToDate AND
    NOT EXISTS (
      SELECT *
      FROM BrandTwoCat b4
      WHERE b1.i_brand_id = b4.i_brand_id AND 
        b2.ToDate < b4.ToDate AND b4.FromDate < b1.ToDate ) -- 43
  UNION
  -- Case 3
  -- |-----------B1-----------|
  --   |--B2--X     X--B3--|
  --      |--B4--|
  -- 
  SELECT b1.i_brand_id, b2.ToDate, b3.FromDate
  FROM BrandAnyCat b1, BrandTwoCat b2, BrandTwoCat b3
  WHERE b2.ToDate < b3.FromDate AND -- b1.i_brand_id = 7003003 AND
    b1.i_brand_id = b2.i_brand_id AND b1.i_brand_id = b3.i_brand_id AND 
    NOT EXISTS (
      SELECT *
      FROM BrandTwoCat b4
      WHERE b1.i_brand_id = b4.i_brand_id AND
        b2.ToDate < b4.ToDate AND b4.FromDate < b3.FromDate ) -- 4 rows
  UNION
  -- Case 4
  -- |----B1----|
  -- |----B4----|
  SELECT i_brand_id, FromDate, ToDate
  FROM BrandAnyCat b1 
  WHERE NOT EXISTS (
    SELECT *
    FROM BrandTwoCat b4
    WHERE b1.i_brand_id = b4.i_brand_id AND 
      b1.FromDate < b4.ToDate AND b4.FromDate < b1.ToDate )
)
-- Coalesce the above
SELECT DISTINCT f.i_brand_id, f.FromDate, l.ToDate
FROM BrandOneCatAll f, BrandOneCatAll l
WHERE f.i_brand_id = l.i_brand_id AND f.FromDate < l.ToDate AND NOT EXISTS (
  SELECT *
  FROM BrandOneCatAll m1
  WHERE f.i_brand_id = m1.i_brand_id AND f.FromDate < m1.FromDate AND
    m1.FromDate <= l.ToDate AND NOT EXISTS (
      SELECT *
      FROM BrandOneCatAll m2
      WHERE f.i_brand_id = m2.i_brand_id AND
        m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
NOT EXISTS (
  SELECT *
  FROM BrandOneCatAll m
  WHERE f.i_brand_id = m.i_brand_id AND 
  ( (m.FromDate < f.FromDate AND f.FromDate <= m.ToDate) OR
    (m.FromDate <= l.ToDate AND l.ToDate < m.ToDate) ) )
ORDER BY f.i_brand_id, f.FromDate;

/******************************************************************************
 * Query 6: Temporal Aggregation
 * Time when a brand has assigned at least 5 items
 *****************************************************************************/

-- Q6_MobDB ------------------------------------------

DROP VIEW IF EXISTS Q6_MobDB;
CREATE VIEW Q6_MobDB(i_brand_id, i_gt5items_vt) AS
WITH BrandNoItems AS (
  SELECT i_brand_id, tcount(i_item_brand_vt)
  FROM mobdb_item_brand
  GROUP BY i_brand_id
)
SELECT i_brand_id,
  whenTrue(tcount #>= 5)
  -- unnest(spans(whenTrue(tcount #>= 5)))
FROM BrandNoItems
WHERE whenTrue(tcount #>= 5) IS NOT NULL
ORDER BY i_brand_id;

/*

-- Same query as above but that outputs standard SQL data types
-- to compare the result with the SQL version below
WITH temp AS (
  SELECT i_brand_id, unnest(segments(tcount(i_item_brand_vt))) AS seg
  FROM mobdb_item_brand
  GROUP BY i_brand_id )
SELECT *
FROM ( SELECT i_brand_id, unnest(getvalues(seg)) AS val, 
    startTimestamp(seg) AS FromDate, endTimestamp(seg) AS ToDate
  FROM temp ) AS T
WHERE val >= 5
ORDER BY i_brand_id, FromDate;


-- Using the UNNEST operation in MobilityDB
WITH temp AS (
  -- This in the UNNEST operation for arrays in PostgreSQL
  SELECT i_brand_id, (pair).value , unnest(periods((pair).time)) AS p
  -- Using the UNNEST operation in MobilityDB
  FROM ( SELECT i_brand_id, unnest(tcount(i_item_brand_vt)) AS pair
    FROM mobdb_item_brand
    GROUP BY i_brand_id ) AS T )
SELECT i_brand_id, value, lower(p) AS FromDate, upper(p) AS ToDate
FROM temp
WHERE value >= 5
ORDER BY i_brand_id, FromDate;

*/

-- Q6_TDW ------------------------------------------

DROP VIEW IF EXISTS Q6_TDW;
CREATE VIEW Q6_TDW(i_brand_id, FromDate, ToDate) AS
-- Days when a the assignment of an item to brand changes 
WITH BrandChanges(i_brand_id, Day) AS (
  SELECT i_brand_id, FromDate FROM tdw_item_brand
  UNION
  SELECT i_brand_id, ToDate FROM tdw_item_brand 
),
-- Per brand, split the time line according to the days in BrandChanges
BrandPeriods(i_brand_id, FromDate, ToDate) AS (
  SELECT *
  FROM ( SELECT i_brand_id, Day AS FromDate,
      LEAD(Day) OVER (PARTITION BY i_brand_id ORDER BY Day) AS ToDate
    FROM BrandChanges ) AS t
  WHERE ToDate IS NOT NULL
),
-- Select the brands that have five items assigned to a brand for each period in BrandPeriods
BrandGT5ItemsAll(i_brand_id, FromDate, ToDate) AS (
  SELECT ib.i_brand_id, b.FromDate, b.ToDate
  FROM tdw_item_brand ib, BrandPeriods b
  WHERE ib.i_brand_id = b.i_brand_id AND
    ib.FromDate <= b.FromDate AND b.ToDate <= ib.ToDate
  GROUP BY ib.i_brand_id, b.FromDate, b.ToDate
  HAVING COUNT(*) >= 5
)
-- Coalesce the result
SELECT DISTINCT f.i_brand_id, f.FromDate, l.ToDate AS XXX
FROM BrandGT5ItemsAll f, BrandGT5ItemsAll l
WHERE f.i_brand_id = l.i_brand_id AND f.FromDate < l.ToDate AND NOT EXISTS (
    SELECT *
    FROM BrandGT5ItemsAll m1
    WHERE f.i_brand_id = m1.i_brand_id AND
      f.FromDate < m1.FromDate AND m1.FromDate <= l.ToDate AND
      NOT EXISTS (
        SELECT *
        FROM BrandGT5ItemsAll m2
        WHERE f.i_brand_id = m2.i_brand_id AND
          m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
  NOT EXISTS (
    SELECT *
    FROM BrandGT5ItemsAll m
    WHERE f.i_brand_id = m.i_brand_id AND
      ( (m.FromDate < f.FromDate AND f.FromDate <= m.ToDate) OR
        (m.FromDate <= l.ToDate AND l.ToDate < m.ToDate) ) )
ORDER BY f.i_brand_id, f.FromDate;

-- Q6_SCD ------------------------------------------

DROP VIEW IF EXISTS Q6_SCD;
CREATE VIEW Q6_SCD(i_brand_id, FromDate, ToDate) AS
WITH ItemBrandAll(i_item_id, i_brand_id, FromDate, ToDate) AS (
  SELECT i_item_id, i_brand_id, i_rec_start_date, i_rec_end_date
  FROM scd_item
),
ItemBrand(i_item_id, i_brand_id, FromDate, ToDate) AS (
  SELECT DISTINCT f.i_item_id, f.i_brand_id, f.FromDate, l.ToDate
  FROM ItemBrandAll f, ItemBrandAll l
  WHERE f.i_item_id = l.i_item_id AND f.i_brand_id = l.i_brand_id AND
    f.FromDate < l.ToDate AND NOT EXISTS (
      SELECT *
      FROM ItemBrandAll m1
      WHERE f.i_item_id = m1.i_item_id AND f.i_brand_id = m1.i_brand_id AND
        f.FromDate < m1.FromDate AND m1.FromDate <= l.ToDate AND
        NOT EXISTS (
          SELECT *
          FROM ItemBrandAll m2
          WHERE f.i_item_id = m2.i_item_id AND f.i_brand_id = m2.i_brand_id AND
            m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
    NOT EXISTS (
      SELECT *
      FROM ItemBrandAll m
      WHERE f.i_item_id = m.i_item_id AND f.i_brand_id = m.i_brand_id AND
        ( (m.FromDate < f.FromDate AND f.FromDate <= m.ToDate) OR
          (m.FromDate <= l.ToDate AND l.ToDate < m.ToDate) ) )
),
BrandChanges(i_brand_id, Day) AS (
  SELECT i_brand_id, FromDate FROM ItemBrand
  UNION
  SELECT i_brand_id, ToDate FROM ItemBrand
),
BrandPeriods(i_brand_id, FromDate, ToDate) AS (
  SELECT *
  FROM ( SELECT i_brand_id, Day AS FromDate,
      LEAD(Day) OVER (PARTITION BY i_brand_id ORDER BY Day) AS ToDate
    FROM BrandChanges ) AS t
  WHERE ToDate IS NOT NULL
),
BrandGT5ItemsAll(i_brand_id, FromDate, ToDate) AS (
  SELECT ib.i_brand_id, b.FromDate, b.ToDate
  FROM ItemBrand ib, BrandPeriods b
  WHERE ib.i_brand_id = b.i_brand_id AND
    ib.FromDate <= b.FromDate AND b.ToDate <= ib.ToDate
  GROUP BY ib.i_brand_id, b.FromDate, b.ToDate
  HAVING COUNT(*) >= 5
)
-- Coalesce the above
SELECT DISTINCT f.i_brand_id, f.FromDate, l.ToDate
FROM BrandGT5ItemsAll f, BrandGT5ItemsAll l
WHERE f.i_brand_id = l.i_brand_id AND f.FromDate < l.ToDate AND NOT EXISTS (
  SELECT *
  FROM BrandGT5ItemsAll m1
  WHERE f.i_brand_id = m1.i_brand_id AND
    f.FromDate < m1.FromDate AND m1.FromDate <= l.ToDate AND
    NOT EXISTS (
      SELECT *
      FROM BrandGT5ItemsAll m2
      WHERE f.i_brand_id = m2.i_brand_id AND
        m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
NOT EXISTS (
  SELECT *
  FROM BrandGT5ItemsAll m
  WHERE f.i_brand_id = m.i_brand_id AND
    ( (m.FromDate < f.FromDate AND f.FromDate <= m.ToDate) OR
      (m.FromDate <= l.ToDate AND l.ToDate < m.ToDate) ) )
ORDER BY i_brand_id, FromDate;

/*****************************************************************************/
