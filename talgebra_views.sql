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
 * - spanUnion(i_item_price_vt)
 * - unnest(spans(spanUnion(i_item_price_vt)))
 * The first version returns a period set and is the standard MobilityDB 
 * answer. The second version is used for verifying that the number of
 * rows obtained in the MobilityDB and the TDW and SCD versions coincide.
 */
 
/******************************************************************************
 * Query 1: Temporal Selection
 * Time when an item is assigned to brand B
 *****************************************************************************/

-- Q1_MobDB ------------------------------------------

DROP VIEW IF EXISTS Q1_MobDB;
CREATE VIEW Q1_MobDB(i_item_id, i_brandB_vt) AS 
SELECT i_item_id,
  i_item_brand_vt
  -- unnest(spans(i_item_brand_vt))
FROM mobdb_item_brand
WHERE i_brand_id = 5004001
ORDER BY i_item_id;

-- Q1_TDW ------------------------------------------

DROP VIEW IF EXISTS Q1_TDW;
CREATE VIEW Q1_TDW(i_item_id, FromDate, ToDate) AS 
WITH ItemBrandB(i_item_id, FromDate, ToDate) AS (
  SELECT i_item_id, FromDate, ToDate
  FROM tdw_item_brand
  WHERE i_brand_id = 5004001 )
-- Coalesce the result
SELECT DISTINCT f.i_item_id, f.FromDate, l.ToDate 
FROM ItemBrandB f, ItemBrandB l 
WHERE f.i_item_id = l.i_item_id AND
  f.FromDate < l.ToDate AND NOT EXISTS (
  SELECT * 
  FROM ItemBrandB m1 
  WHERE f.i_item_id = m1.i_item_id AND f.FromDate < m1.FromDate AND
    m1.FromDate <= l.ToDate AND NOT EXISTS (
      SELECT * 
      FROM ItemBrandB m2 
      WHERE f.i_item_id = m2.i_item_id 
      AND m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
  NOT EXISTS (
    SELECT * 
  FROM ItemBrandB m 
  WHERE f.i_item_id = m.i_item_id AND 
  ( ( m.FromDate < f.FromDate AND f.FromDate <= m.ToDate ) OR 
    ( m.FromDate <= l.ToDate AND l.ToDate < m.ToDate ) ) )
ORDER BY f.i_item_id, f.FromDate;

-- Q1_SCD ------------------------------------------

DROP VIEW IF EXISTS Q1_SCD;
CREATE VIEW Q1_SCD(i_item_id, FromDate, ToDate) AS 
WITH ItemBrandBAll(i_item_id, FromDate, ToDate) AS (
  SELECT i_item_id, i_rec_start_date, i_rec_end_date
  FROM scd_item
  WHERE i_brand_id = 5004001 )
-- Coalesce the table above
SELECT DISTINCT f.i_item_id, f.FromDate, l.ToDate 
FROM ItemBrandBAll f, ItemBrandBAll l 
WHERE f.i_item_id = l.i_item_id AND
  f.FromDate < l.ToDate AND NOT EXISTS (
  SELECT * 
  FROM ItemBrandBAll m1 
  WHERE f.i_item_id = m1.i_item_id AND f.FromDate < m1.FromDate AND
    m1.FromDate <= l.ToDate AND NOT EXISTS (
      SELECT * 
      FROM ItemBrandBAll m2 
      WHERE f.i_item_id = m2.i_item_id 
      AND m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
  NOT EXISTS (
    SELECT * 
  FROM ItemBrandBAll m 
  WHERE f.i_item_id = m.i_item_id AND 
  ( ( m.FromDate < f.FromDate AND f.FromDate <= m.ToDate ) OR 
    ( m.FromDate <= l.ToDate AND l.ToDate < m.ToDate ) ) )
ORDER BY f.i_item_id, f.FromDate;

/******************************************************************************
 * Query 2: Temporal Projection
 * Time when an item is assigned to any brand
 *****************************************************************************/

-- Q2_MobDB ------------------------------------------

DROP VIEW IF EXISTS Q2_MobDB;
CREATE VIEW Q2_MobDB(i_item_id, b_item_anyBrand_vt) AS 
SELECT i_item_id, 
  spanUnion(i_item_brand_vt)
  -- unnest(spans(spanUnion(i_item_brand_vt)))
FROM mobdb_item_brand
GROUP BY i_item_id
ORDER BY i_item_id;

-- Q2_TDW ------------------------------------------

DROP VIEW IF EXISTS Q2_TDW;
CREATE VIEW Q2_TDW(i_item_id, FromDate, ToDate) AS 
WITH ItemAnyBrandAll(i_item_id, FromDate, ToDate) AS (
  SELECT i_item_id, FromDate, ToDate
  FROM tdw_item_brand )
-- Coalesce the previous table
SELECT DISTINCT f.i_item_id, f.FromDate, l.ToDate 
FROM ItemAnyBrandAll f, ItemAnyBrandAll l 
WHERE f.i_item_id = l.i_item_id AND f.FromDate < l.ToDate AND NOT EXISTS (
  SELECT * 
  FROM ItemAnyBrandAll m1 
  WHERE f.i_item_id = m1.i_item_id AND f.FromDate < m1.FromDate AND
    m1.FromDate <= l.ToDate AND NOT EXISTS (
      SELECT * 
      FROM ItemAnyBrandAll m2 
      WHERE f.i_item_id = m2.i_item_id 
      AND m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
  NOT EXISTS (
    SELECT * 
  FROM ItemAnyBrandAll m 
  WHERE f.i_item_id = m.i_item_id AND 
  ( ( m.FromDate < f.FromDate AND f.FromDate <= m.ToDate ) OR 
    ( m.FromDate <= l.ToDate AND l.ToDate < m.ToDate ) ) )
ORDER BY f.i_item_id, f.FromDate;

-- Q2_SCD ------------------------------------------

DROP VIEW IF EXISTS Q2_SCD;
CREATE VIEW Q2_SCD(i_item_id, FromDate, ToDate) AS 
WITH ItemAnyBrandAll(i_item_id, FromDate, ToDate) AS (
  SELECT i_item_id, i_rec_start_date, i_rec_end_date
  FROM scd_item )
-- Coalesce the previous table
SELECT DISTINCT f.i_item_id, f.FromDate, l.ToDate 
FROM ItemAnyBrandAll f, ItemAnyBrandAll l 
WHERE f.i_item_id = l.i_item_id AND f.FromDate < l.ToDate AND NOT EXISTS (
  SELECT * 
  FROM ItemAnyBrandAll m1 
  WHERE f.i_item_id = m1.i_item_id AND f.FromDate < m1.FromDate AND
    m1.FromDate <= l.ToDate AND NOT EXISTS (
      SELECT * 
      FROM ItemAnyBrandAll m2 
      WHERE f.i_item_id = m2.i_item_id 
      AND m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
  NOT EXISTS (
    SELECT * 
  FROM ItemAnyBrandAll m 
  WHERE f.i_item_id = m.i_item_id AND 
  ( ( m.FromDate < f.FromDate AND f.FromDate <= m.ToDate ) OR 
    ( m.FromDate <= l.ToDate AND l.ToDate < m.ToDate ) ) )
ORDER BY f.i_item_id, f.FromDate;

/******************************************************************************
 * Query 3: Temporal Join
 * Total sales and time when an item is assigned to brand B and its price is
 * greater that 80
 *****************************************************************************/

-- Q3_MobDB ------------------------------------------

DROP VIEW IF EXISTS Q3_MobDB;
CREATE VIEW Q3_MobDB(i_item_id, i_brandBAndPriceGT80_vt) AS
SELECT i_item_id, 
  spanUnion(i_brandBAndPriceGT80_vt)
  -- unnest(spans(spanUnion(i_brandBAndPriceGT80_vt)))
FROM (
  SELECT ib.i_item_id, ib.i_item_brand_vt * ip.i_item_price_vt AS
    i_brandBAndPriceGT80_vt
  FROM mobdb_item_brand ib, mobdb_item_price ip
  WHERE ib.i_item_id = ip.i_item_id AND
    ib.i_brand_id = 5004001 AND ip.i_item_price > 80 AND
    ib.i_item_brand_vt * ip.i_item_price_vt IS NOT NULL
)
GROUP BY i_item_id
ORDER BY i_item_id;
  
-- Q3_TDW ------------------------------------------

DROP VIEW IF EXISTS Q3_TDW;
CREATE VIEW Q3_TDW(i_item_id, FromDate, ToDate) AS
WITH BrandBAndPriceGT80All AS (
  SELECT DISTINCT ib.i_item_id, greatest(ib.FromDate, ip.FromDate) AS FromDate,
    least(ib.ToDate, ip.ToDate) AS ToDate
  FROM tdw_item_brand ib, tdw_item_price ip
  WHERE ib.i_item_id = ip.i_item_id AND
    ib.i_brand_id = 5004001 AND ip.i_item_price > 80 AND
    greatest(ib.FromDate, ip.FromDate) < least(ib.ToDate, ip.ToDate) )
-- Coalesce the previous table
SELECT DISTINCT f.i_item_id, f.FromDate, l.ToDate 
FROM BrandBAndPriceGT80All f, BrandBAndPriceGT80All l 
WHERE f.i_item_id = l.i_item_id AND f.FromDate < l.ToDate AND NOT EXISTS (
  SELECT * 
  FROM BrandBAndPriceGT80All m1 
  WHERE f.i_item_id = m1.i_item_id AND f.FromDate < m1.FromDate AND
    m1.FromDate <= l.ToDate AND NOT EXISTS (
      SELECT * 
      FROM BrandBAndPriceGT80All m2 
      WHERE f.i_item_id = m2.i_item_id 
      AND m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
  NOT EXISTS (
    SELECT * 
  FROM BrandBAndPriceGT80All m 
  WHERE f.i_item_id = m.i_item_id AND 
  ( ( m.FromDate < f.FromDate AND f.FromDate <= m.ToDate ) OR 
    ( m.FromDate <= l.ToDate AND l.ToDate < m.ToDate ) ) )
ORDER BY f.i_item_id, f.FromDate;

-- Q3_SCD ------------------------------------------

DROP VIEW IF EXISTS Q3_SCD;
CREATE VIEW Q3_SCD(i_item_id, FromDate, ToDate) AS
WITH BrandBAndPriceGT80All(i_item_id, FromDate, ToDate) AS (
  SELECT i_item_id, i_rec_start_date, i_rec_end_date
  FROM scd_item
  WHERE i_brand_id = 5004001 AND i_current_price > 80 )
-- Coalesce the previous table
SELECT DISTINCT f.i_item_id, f.FromDate, l.ToDate
FROM BrandBAndPriceGT80All f, BrandBAndPriceGT80All l
WHERE f.i_item_id = l.i_item_id AND f.FromDate < l.ToDate AND NOT EXISTS (
    SELECT *
    FROM BrandBAndPriceGT80All m1
    WHERE f.i_item_id = m1.i_item_id AND 
      f.FromDate < m1.FromDate AND m1.FromDate <= l.ToDate AND 
      NOT EXISTS (
        SELECT *
        FROM BrandBAndPriceGT80All m2
        WHERE f.i_item_id = m2.i_item_id AND
          m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
  NOT EXISTS (
    SELECT *
    FROM BrandBAndPriceGT80All m
    WHERE f.i_item_id = m.i_item_id AND 
      ( (m.FromDate < f.FromDate AND f.FromDate <= m.ToDate) OR
        (m.FromDate <= l.ToDate AND l.ToDate < m.ToDate) ) )
ORDER BY i_item_id;

/******************************************************************************
 * Query 4: Temporal Union
 * Time when an item is assigned to brand B or its price is greater than 80 
 *****************************************************************************/

-- Q4_MobDB ------------------------------------------

DROP VIEW IF EXISTS Q4_MobDB;
CREATE VIEW Q4_MobDB(i_item_id, i_brandBOrPriceGT80_vt) AS
SELECT i_item_id, 
  spanUnion(i_brandBOrPriceGT80_vt)
  -- unnest(spans(spanUnion(i_brandBOrPriceGT80_vt)))
FROM (
  SELECT i_item_id, i_item_brand_vt AS i_brandBOrPriceGT80_vt
  FROM mobdb_item_brand
  WHERE i_brand_id = 5004001
  UNION
  SELECT i_item_id, i_item_price_vt
  FROM mobdb_item_price
  WHERE i_item_price > 80 )
GROUP BY i_item_id
ORDER BY i_item_id;

-- Q4_TDW ------------------------------------------

DROP VIEW IF EXISTS Q4_TDW;
CREATE VIEW Q4_TDW(i_item_id, FromDate, ToDate) AS
WITH BrandBOrPriceGT20All(i_item_id, FromDate, ToDate) AS (
  SELECT i_item_id, ib.FromDate, ib.ToDate
  FROM tdw_item_brand ib, tdw_brand b
  WHERE ib.i_brand_id = b.i_brand_id AND b.i_brand_id = 5004001
  UNION
  SELECT i_item_id, FromDate, ToDate
  FROM tdw_item_price
  WHERE i_item_price > 80 )
-- Coalesce the above
SELECT DISTINCT f.i_item_id, f.FromDate, l.ToDate
FROM BrandBOrPriceGT20All f, BrandBOrPriceGT20All l
WHERE f.i_item_id = l.i_item_id AND f.FromDate < l.ToDate AND
  NOT EXISTS (
    SELECT *
    FROM BrandBOrPriceGT20All m1
    WHERE f.i_item_id = m1.i_item_id AND f.FromDate < m1.FromDate AND
      m1.FromDate <= l.ToDate AND NOT EXISTS (
        SELECT *
        FROM BrandBOrPriceGT20All m2
        WHERE f.i_item_id = m2.i_item_id AND m2.FromDate < m1.FromDate AND
          m1.FromDate <= m2.ToDate ) ) AND
  NOT EXISTS (
    SELECT *
    FROM BrandBOrPriceGT20All m
    WHERE f.i_item_id = m.i_item_id AND
    ( (m.FromDate < f.FromDate AND f.FromDate <= m.ToDate) OR
      (m.FromDate <= l.ToDate AND l.ToDate < m.ToDate) ) )
ORDER BY f.i_item_id, f.FromDate; 

-- Q4_SCD ------------------------------------------

DROP VIEW IF EXISTS Q4_SCD;
CREATE VIEW Q4_SCD(i_item_id, FromDate, ToDate) AS
WITH BrandBOrPriceGT20All(i_item_id, FromDate, ToDate) AS (
  SELECT i_item_id, i_rec_start_date, i_rec_end_date
  FROM scd_item
  WHERE (i_brand_id = 5004001 OR i_current_price > 80) )
-- Coalesce the above
SELECT DISTINCT f.i_item_id, f.FromDate, l.ToDate
FROM BrandBOrPriceGT20All f, BrandBOrPriceGT20All l
WHERE f.i_item_id = l.i_item_id AND f.FromDate < l.ToDate AND
  NOT EXISTS (
    SELECT *
    FROM BrandBOrPriceGT20All m1
    WHERE f.i_item_id = m1.i_item_id AND f.FromDate < m1.FromDate AND
      m1.FromDate <= l.ToDate AND NOT EXISTS (
        SELECT *
        FROM BrandBOrPriceGT20All m2
        WHERE f.i_item_id = m2.i_item_id AND m2.FromDate < m1.FromDate AND
          m1.FromDate <= m2.ToDate ) ) AND
  NOT EXISTS (
    SELECT *
    FROM BrandBOrPriceGT20All m
    WHERE f.i_item_id = m.i_item_id AND
      ( (m.FromDate < f.FromDate AND f.FromDate <= m.ToDate) OR
        (m.FromDate <= l.ToDate AND l.ToDate < m.ToDate) ) ) 
ORDER BY f.i_item_id, f.FromDate;

/******************************************************************************
 * Query 5: Temporal Difference
 * Time when an item is assigned to brand B and its price is not greater than 
 * 80 
 *****************************************************************************/

-- Q5_MobDB ------------------------------------------

DROP VIEW IF EXISTS Q5_MobDB;
CREATE VIEW Q5_MobDB(i_item_id, i_brandBAndNotPriceGT80_vt) AS
SELECT i_item_id, 
  spanUnion(i_brandBAndNotPriceGT80_vt)
  -- unnest(spans(spanUnion(i_brandBAndNotPriceGT80_vt)))
FROM (
  SELECT ib.i_item_id, ib.i_item_brand_vt - ip.i_item_price_vt AS
    i_brandBAndNotPriceGT80_vt
  FROM mobdb_item_brand ib, mobdb_item_price ip
  WHERE ib.i_item_id = ip.i_item_id AND
    ib.i_brand_id = 5004001 AND ip.i_item_price > 80 AND
    ib.i_item_brand_vt - ip.i_item_price_vt IS NOT NULL )
GROUP BY i_item_id
ORDER BY i_item_id;

-- Q5_TDW ------------------------------------------

DROP VIEW IF EXISTS Q5_TDW;
CREATE VIEW Q5_TDW(i_item_id, FromDate, ToDate) AS
WITH ItemBrandBAll(i_item_id, FromDate, ToDate) AS (
  SELECT i_item_id, FromDate, ToDate
  FROM tdw_item_brand
  WHERE i_brand_id = 5004001 ),
-- Coalesce the table above
ItemBrandB(i_item_id, FromDate, ToDate) AS (
  SELECT DISTINCT f.i_item_id, f.FromDate, l.ToDate 
  FROM ItemBrandBAll f, ItemBrandBAll l 
  WHERE f.i_item_id = l.i_item_id AND f.FromDate < l.ToDate AND NOT EXISTS (
    SELECT * 
    FROM ItemBrandBAll m1 
    WHERE f.i_item_id = m1.i_item_id AND f.FromDate < m1.FromDate AND
      m1.FromDate <= l.ToDate AND NOT EXISTS (
        SELECT * 
        FROM ItemBrandBAll m2 
        WHERE f.i_item_id = m2.i_item_id 
        AND m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
    NOT EXISTS (
      SELECT * 
    FROM ItemBrandBAll m 
    WHERE f.i_item_id = m.i_item_id AND 
    ( ( m.FromDate < f.FromDate AND f.FromDate <= m.ToDate ) OR 
      ( m.FromDate <= l.ToDate AND l.ToDate < m.ToDate ) ) ) ),
ItemPriceGT80All(i_item_id, FromDate, ToDate) AS (
  SELECT i_item_id, FromDate, ToDate
  FROM tdw_item_price
  WHERE i_item_price > 80 ),
-- Coalesce the table above
ItemPriceGT80(i_item_id, FromDate, ToDate) AS (
  SELECT DISTINCT f.i_item_id, f.FromDate, l.ToDate 
  FROM ItemPriceGT80All f, ItemPriceGT80All l 
  WHERE f.i_item_id = l.i_item_id AND f.FromDate < l.ToDate AND NOT EXISTS (
    SELECT * 
    FROM ItemPriceGT80All m1 
    WHERE f.i_item_id = m1.i_item_id AND f.FromDate < m1.FromDate AND
      m1.FromDate <= l.ToDate AND NOT EXISTS (
        SELECT * 
        FROM ItemPriceGT80All m2 
        WHERE f.i_item_id = m2.i_item_id 
        AND m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
    NOT EXISTS (
      SELECT * 
    FROM ItemPriceGT80All m 
    WHERE f.i_item_id = m.i_item_id AND 
    ( ( m.FromDate < f.FromDate AND f.FromDate <= m.ToDate ) OR 
      ( m.FromDate <= l.ToDate AND l.ToDate < m.ToDate ) ) ) ),
-- Temporal difference between ItemBrandB and ItemPriceGT80
ItemBrandBAndNotPriceGT80All(i_item_id, FromDate, ToDate) AS (
  -- Case 1
  -- X----B1----|
  --      X----B2----|
  --   |----B3----|
  SELECT b1.i_item_id, b1.FromDate, b2.FromDate
  FROM ItemBrandB b1, ItemPriceGT80 b2
  WHERE b1.i_item_id = b2.i_item_id AND b1.FromDate < b2.FromDate AND
    NOT EXISTS (
      SELECT *
      FROM ItemPriceGT80 b3
      WHERE b1.i_item_id = b3.i_item_id AND
        b1.FromDate < b3.FromDate AND b3.FromDate < b2.FromDate )
  UNION
  -- Case 2
  --      |----B1----X
  -- |----B2----X
  --   |----B3----|
  SELECT b1.i_item_id, b2.ToDate, b1.ToDate
  FROM ItemBrandB b1, ItemPriceGT80 b2
  WHERE b1.i_item_id = b2.i_item_id AND b2.ToDate < b1.ToDate AND
    NOT EXISTS (
      SELECT *
      FROM ItemPriceGT80 b3
      WHERE b1.i_item_id = b3.i_item_id AND 
        b2.ToDate < b3.ToDate AND b3.ToDate < b1.ToDate )
  UNION
  -- Case 3
  -- |-----------B1-----------|
  --   |--B2--X     X--B3--|
  --      |--B4--|
  -- 
  SELECT b1.i_item_id, b2.ToDate, b3.FromDate
  FROM ItemBrandB b1, ItemPriceGT80 b2, ItemPriceGT80 b3
  WHERE b2.ToDate < b3.FromDate AND
    b1.i_item_id = b2.i_item_id AND b1.i_item_id = b3.i_item_id AND 
    NOT EXISTS (
      SELECT *
      FROM ItemPriceGT80 b4
      WHERE b1.i_item_id = b4.i_item_id AND
        b2.ToDate < b4.ToDate AND b4.ToDate < b3.FromDate )
  /* The next condition should NOT be added to really ensure that price > 80 
  UNION
  -- Case 4
  -- |----B1----|
  -- |----B2----|
  SELECT i_item_id, FromDate, ToDate
  FROM ItemBrandB b1 
  WHERE NOT EXISTS (
    SELECT *
    FROM ItemPriceGT80 b2
    WHERE b1.i_item_id = b2.i_item_id AND 
      b1.FromDate < b2.ToDate AND b2.FromDate < b1.ToDate )
  */
)
-- Coalesce the table above
SELECT DISTINCT f.i_item_id, f.FromDate, l.ToDate
FROM ItemBrandBAndNotPriceGT80All f, ItemBrandBAndNotPriceGT80All l
WHERE f.i_item_id = l.i_item_id AND f.FromDate < l.ToDate AND NOT EXISTS (
  SELECT *
  FROM ItemBrandBAndNotPriceGT80All m1
  WHERE f.i_item_id = m1.i_item_id AND f.FromDate < m1.FromDate AND
    m1.FromDate <= l.ToDate AND NOT EXISTS (
      SELECT *
      FROM ItemBrandBAndNotPriceGT80All m2
      WHERE f.i_item_id = m2.i_item_id AND
        m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
NOT EXISTS (
  SELECT *
  FROM ItemBrandBAndNotPriceGT80All m
  WHERE f.i_item_id = m.i_item_id AND 
  ( (m.FromDate < f.FromDate AND f.FromDate <= m.ToDate) OR
    (m.FromDate <= l.ToDate AND l.ToDate < m.ToDate) ) )
ORDER BY f.i_item_id, f.FromDate;

-- Q5_SCD ------------------------------------------

DROP VIEW IF EXISTS Q5_SCD;
CREATE VIEW Q5_SCD(i_item_id, FromDate, ToDate) AS
-- Time when an item has category B
WITH ItemBrandBAll(i_item_id, FromDate, ToDate) AS (
  SELECT i_item_id, i_rec_start_date, i_rec_end_date
  FROM scd_item 
  WHERE i_brand_id = 5004001 ),
ItemBrandB(i_item_id, FromDate, ToDate) AS (
  SELECT DISTINCT f.i_item_id, f.FromDate, l.ToDate 
  FROM ItemBrandBAll f, ItemBrandBAll l 
  WHERE f.i_item_id = l.i_item_id AND f.FromDate < l.ToDate AND NOT EXISTS (
    SELECT * 
    FROM ItemBrandBAll m1 
    WHERE f.i_item_id = m1.i_item_id AND f.FromDate < m1.FromDate AND
      m1.FromDate <= l.ToDate AND NOT EXISTS (
        SELECT * 
        FROM ItemBrandBAll m2 
        WHERE f.i_item_id = m2.i_item_id 
        AND m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
    NOT EXISTS (
      SELECT * 
    FROM ItemBrandBAll m 
    WHERE f.i_item_id = m.i_item_id AND 
    ( ( m.FromDate < f.FromDate AND f.FromDate <= m.ToDate ) OR 
      ( m.FromDate <= l.ToDate AND l.ToDate < m.ToDate ) ) ) ),
-- Time when an item has price greater than 80
ItemPriceGT80All(i_item_id, FromDate, ToDate) AS (
  SELECT i_item_id, i_rec_start_date, i_rec_end_date
  FROM scd_item
  WHERE i_current_price > 80 ),
-- Coalesce the above
ItemPriceGT80(i_item_id, FromDate, ToDate) AS (
  SELECT DISTINCT f.i_item_id, f.FromDate, l.ToDate 
  FROM ItemPriceGT80All f, ItemPriceGT80All l 
  WHERE f.i_item_id = l.i_item_id AND f.FromDate < l.ToDate AND NOT EXISTS (
    SELECT * 
    FROM ItemPriceGT80All m1 
    WHERE f.i_item_id = m1.i_item_id AND f.FromDate < m1.FromDate AND
      m1.FromDate <= l.ToDate AND NOT EXISTS (
        SELECT * 
        FROM ItemPriceGT80All m2 
        WHERE f.i_item_id = m2.i_item_id 
        AND m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
    NOT EXISTS (
      SELECT * 
    FROM ItemPriceGT80All m 
    WHERE f.i_item_id = m.i_item_id AND 
    ( ( m.FromDate < f.FromDate AND f.FromDate <= m.ToDate ) OR 
      ( m.FromDate <= l.ToDate AND l.ToDate < m.ToDate ) ) ) ),
-- Temporal difference between ItemBrandB and ItemPriceGT80
ItemBrandBAndNotPriceGT80All(i_item_id, FromDate, ToDate) AS (
  -- Case 1
  -- X----B1----|
  --      X----B2----|
  --   |----B4----|
  SELECT b1.i_item_id, b1.FromDate, b2.FromDate
  FROM ItemBrandB b1, ItemPriceGT80 b2
  WHERE b1.i_item_id = b2.i_item_id AND b1.FromDate < b2.FromDate AND
    NOT EXISTS (
      SELECT *
      FROM ItemPriceGT80 b4
      WHERE b1.i_item_id = b4.i_item_id AND
        b1.FromDate < b4.ToDate AND b4.FromDate < b2.FromDate ) -- 670
  UNION
  -- Case 2
  --      |----B1----X
  -- |----B2----X
  --   |----B4----|
  SELECT b1.i_item_id, b2.ToDate, b1.ToDate
  FROM ItemBrandB b1, ItemPriceGT80 b2
  WHERE b1.i_item_id = b2.i_item_id AND b2.ToDate < b1.ToDate AND
    NOT EXISTS (
      SELECT *
      FROM ItemPriceGT80 b4
      WHERE b1.i_item_id = b4.i_item_id AND 
        b2.ToDate < b4.ToDate AND b4.FromDate < b1.ToDate ) -- 43
  UNION
  -- Case 3
  -- |-----------B1-----------|
  --   |--B2--X     X--B3--|
  --      |--B4--|
  -- 
  SELECT b1.i_item_id, b2.ToDate, b3.FromDate
  FROM ItemBrandB b1, ItemPriceGT80 b2, ItemPriceGT80 b3
  WHERE b2.ToDate < b3.FromDate AND -- b1.i_item_id = 7003003 AND
    b1.i_item_id = b2.i_item_id AND b1.i_item_id = b3.i_item_id AND 
    NOT EXISTS (
      SELECT *
      FROM ItemPriceGT80 b4
      WHERE b1.i_item_id = b4.i_item_id AND
        b2.ToDate < b4.ToDate AND b4.FromDate < b3.FromDate )
  /* The next condition should NOT be added to really ensure that price > 80 
  UNION
  -- Case 4
  -- |----B1----|
  -- |----B4----|
  SELECT i_item_id, FromDate, ToDate
  FROM ItemBrandB b1 
  WHERE NOT EXISTS (
    SELECT *
    FROM ItemPriceGT80 b4
    WHERE b1.i_item_id = b4.i_item_id AND 
      b1.FromDate < b4.ToDate AND b4.FromDate < b1.ToDate )
  */
)
-- Coalesce the above
SELECT DISTINCT f.i_item_id, f.FromDate, l.ToDate
FROM ItemBrandBAndNotPriceGT80All f, ItemBrandBAndNotPriceGT80All l
WHERE f.i_item_id = l.i_item_id AND f.FromDate < l.ToDate AND NOT EXISTS (
  SELECT *
  FROM ItemBrandBAndNotPriceGT80All m1
  WHERE f.i_item_id = m1.i_item_id AND f.FromDate < m1.FromDate AND
    m1.FromDate <= l.ToDate AND NOT EXISTS (
      SELECT *
      FROM ItemBrandBAndNotPriceGT80All m2
      WHERE f.i_item_id = m2.i_item_id AND
        m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
NOT EXISTS (
  SELECT *
  FROM ItemBrandBAndNotPriceGT80All m
  WHERE f.i_item_id = m.i_item_id AND 
  ( (m.FromDate < f.FromDate AND f.FromDate <= m.ToDate) OR
    (m.FromDate <= l.ToDate AND l.ToDate < m.ToDate) ) )
ORDER BY f.i_item_id, f.FromDate;

/******************************************************************************
 * Query 6: Temporal Aggregation
 * Time when a category has assigned to it more than 3 items
 *****************************************************************************/

-- Q6_MobDB ------------------------------------------

DROP VIEW IF EXISTS Q6_MobDB;
CREATE VIEW Q6_MobDB(i_category_id, i_gt3items_vt) AS
WITH ItemNoCats AS (
  SELECT i_category_id, tcount(i_item_category_vt::tstzspanset)
  FROM mobdb_item_category
  GROUP BY i_category_id )
SELECT i_category_id,
  whenTrue(tcount #> 3)::datespanset
  -- unnest(spans(whenTrue(tcount #> 3)))
FROM ItemNoCats
WHERE whenTrue(tcount #> 3) IS NOT NULL
ORDER BY i_category_id;

-- Q6_TDW ------------------------------------------

DROP VIEW IF EXISTS Q6_TDW;
CREATE VIEW Q6_TDW(i_category_id, FromDate, ToDate) AS
-- Days when the assignment of an item to a category changes 
WITH CategoryChanges(i_category_id, Day) AS (
  SELECT i_category_id, FromDate FROM tdw_item_category
  UNION
  SELECT i_category_id, ToDate FROM tdw_item_category ),
-- Per category, split the time line according to the days in CategoryChanges
CategoryPeriods(i_category_id, FromDate, ToDate) AS (
  SELECT *
  FROM ( SELECT i_category_id, Day AS FromDate,
      LEAD(Day) OVER (PARTITION BY i_category_id ORDER BY Day) AS ToDate
    FROM CategoryChanges ) AS t
  WHERE ToDate IS NOT NULL ),
-- Select the categories that have at least 3 items assigned to it for each period in CategoryPeriods
CategoryGT3ItemsAll(i_category_id, FromDate, ToDate) AS (
  SELECT ic.i_category_id, b.FromDate, b.ToDate
  FROM tdw_item_category ic, CategoryPeriods b
  WHERE ic.i_category_id = b.i_category_id AND
    ic.FromDate <= b.FromDate AND b.ToDate <= ic.ToDate
  GROUP BY ic.i_category_id, b.FromDate, b.ToDate
  HAVING COUNT(*) >= 3 )
-- Coalesce the result
SELECT DISTINCT f.i_category_id, f.FromDate, l.ToDate
FROM CategoryGT3ItemsAll f, CategoryGT3ItemsAll l
WHERE f.i_category_id = l.i_category_id AND f.FromDate < l.ToDate AND NOT EXISTS (
    SELECT *
    FROM CategoryGT3ItemsAll m1
    WHERE f.i_category_id = m1.i_category_id AND
      f.FromDate < m1.FromDate AND m1.FromDate <= l.ToDate AND
      NOT EXISTS (
        SELECT *
        FROM CategoryGT3ItemsAll m2
        WHERE f.i_category_id = m2.i_category_id AND
          m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
  NOT EXISTS (
    SELECT *
    FROM CategoryGT3ItemsAll m
    WHERE f.i_category_id = m.i_category_id AND
      ( (m.FromDate < f.FromDate AND f.FromDate <= m.ToDate) OR
        (m.FromDate <= l.ToDate AND l.ToDate < m.ToDate) ) )
ORDER BY f.i_category_id, f.FromDate;

-- Q6_SCD ------------------------------------------

DROP VIEW IF EXISTS Q6_SCD;
CREATE VIEW Q6_SCD(i_category_id, FromDate, ToDate) AS
WITH ItemCategoryAll(i_item_id, i_category_id, FromDate, ToDate) AS (
  SELECT i_item_id, i_category_id, i_rec_start_date, i_rec_end_date
  FROM scd_item ),
-- Coalesce the above table
ItemCategory(i_item_id, i_category_id, FromDate, ToDate) AS (
  SELECT DISTINCT f.i_item_id, f.i_category_id, f.FromDate, l.ToDate
  FROM ItemCategoryAll f, ItemCategoryAll l
  WHERE f.i_item_id = l.i_item_id AND f.i_category_id = l.i_category_id AND 
    f.FromDate < l.ToDate AND NOT EXISTS (
      SELECT *
      FROM ItemCategoryAll m1
      WHERE f.i_item_id = l.i_item_id AND f.i_category_id = l.i_category_id AND 
        f.FromDate < m1.FromDate AND m1.FromDate <= l.ToDate AND
        NOT EXISTS (
          SELECT *
          FROM ItemCategoryAll m2
          WHERE f.i_item_id = l.i_item_id AND f.i_category_id = l.i_category_id AND 
            m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
    NOT EXISTS (
      SELECT *
      FROM ItemCategoryAll m
      WHERE f.i_item_id = l.i_item_id AND f.i_category_id = l.i_category_id AND 
        ( (m.FromDate < f.FromDate AND f.FromDate <= m.ToDate) OR
          (m.FromDate <= l.ToDate AND l.ToDate < m.ToDate) ) )
  ORDER BY f.i_category_id, f.FromDate ),
CategoryChanges(i_category_id, Day) AS (
  SELECT i_category_id, FromDate FROM ItemCategory
  UNION
  SELECT i_category_id, ToDate FROM ItemCategory ),
CategoryPeriods(i_category_id, FromDate, ToDate) AS (
  SELECT *
  FROM ( SELECT i_category_id, Day AS FromDate,
      LEAD(Day) OVER (PARTITION BY i_category_id ORDER BY Day) AS ToDate
    FROM CategoryChanges ) AS t
  WHERE ToDate IS NOT NULL ),
CategroyGT3ItemsAll(i_category_id, FromDate, ToDate) AS (
  SELECT ic.i_category_id, c.FromDate, c.ToDate
  FROM ItemCategory ic, CategoryPeriods c
  WHERE ic.i_category_id = c.i_category_id AND
    ic.FromDate <= c.FromDate AND c.ToDate <= ic.ToDate
  GROUP BY ic.i_category_id, c.FromDate, c.ToDate
  HAVING COUNT(*) >= 3 )
-- Coalesce the above
SELECT DISTINCT f.i_category_id, f.FromDate, l.ToDate
FROM CategroyGT3ItemsAll f, CategroyGT3ItemsAll l
WHERE f.i_category_id = l.i_category_id AND f.FromDate < l.ToDate AND NOT EXISTS (
  SELECT *
  FROM CategroyGT3ItemsAll m1
  WHERE f.i_category_id = m1.i_category_id AND
    f.FromDate < m1.FromDate AND m1.FromDate <= l.ToDate AND
    NOT EXISTS (
      SELECT *
      FROM CategroyGT3ItemsAll m2
      WHERE f.i_category_id = m2.i_category_id AND
        m2.FromDate < m1.FromDate AND m1.FromDate <= m2.ToDate ) ) AND
NOT EXISTS (
  SELECT *
  FROM CategroyGT3ItemsAll m
  WHERE f.i_category_id = m.i_category_id AND
    ( (m.FromDate < f.FromDate AND f.FromDate <= m.ToDate) OR
      (m.FromDate <= l.ToDate AND l.ToDate < m.ToDate) ) )
ORDER BY f.i_category_id, FromDate;

/*****************************************************************************/
