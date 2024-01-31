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
 * @brief Excerpt of the TPC-DS bechmark https://www.tpc.org/tpcds/ for
 * analyzing alternative implementations of a temporal data warehouse to
 * assess temporal algebra and temporal OLAP queries.
 *
 * The three implementations are
 * - The traditional Kimball's Slowly Changing Dimension (SCD) implementation
 * - A temporal data warehouse (TDW) implementation proposed by Ahmed et al. in
 *   the article https://www.igi-global.com/gateway/article/265260
 * - A MobilityDB (MobDB) implementation 
 * 
 * This script supposes that the MobilityDB extension has been created before
 * with the following command
 * @code
 * CREATE EXTENSION MobilityDB CASCADE;
 * @endcode
 * This script creates the three schemas on the SAME database and loads the
 * data from the CSV files in a given Scale Factor (SF). The SF size, currently
 * tested with SFs 1, 10, and 100, is given as a parameter to call the function
 * loading the data. The CSV files are expected to be in subdirectories of the
 * current repository, e.g., ..../sf1/scd_item.csv.
 *
 * There are three CSV files, namely, scd_item.csv, store_sales.csv, and
 * date_dim.csv. These files correspond to the SCD implementation. From the
 * same files, the TDW and the MobilityDB implementation are derived.
 *
 * The tables are UNLOGGED to avoid using the Write-Ahead Log (WAL). As stated 
 * in the PostgreSQL manual
 * https://www.postgresql.org/docs/current/sql-createtable.html
 *   "Data written to unlogged tables is not written to the write-ahead log,
 *   which makes them considerably faster than ordinary tables. However, they
 *   are not crash-safe: an unlogged table is automatically truncated after a
 *   crash or unclean shutdown. The contents of an unlogged table are also not
 *   replicated to standby servers. Any indexes created on an unlogged table
 *   are automatically unlogged as well."
 * The number of lines of the fact table (the larger one) is as follows:
 * - sf1: 2 490 397
 * - sf10: 24 906 845
 * - sf100: 249 018 773
 * For this reason the indexes and foreign key constraints are also created
 * AFTER the whole table has been created from the CSV file. In the SF 1
 * the speed up of these optimizations wrt a more traditional method WHERE
 * the tables are logged and the constraints are in checked during the COPY
 * are a
 * - traditional method: Time: 151818.781 ms (02:31.819)
 * - optimized method: Time: 62924.914 ms (01:02.925)
 * The following are the loading time for the three scale factors on a desktop
 * machine with an AMD Ryzen 9 3900X 12-Core Processor 3.79 GHz and 64 G of RAM
 * sf1: Time: 30770.830 ms (00:30.771)
 * sf10: Time: 369864.800 ms (06:09.865)
 * sf100: Time: 6158453.478 ms (01:42:38.453)
 */

DROP FUNCTION IF EXISTS tdw_load;
CREATE OR REPLACE FUNCTION tdw_load(SF integer)
RETURNS text AS $$
DECLARE
  Path text;
BEGIN
  Path := '/home/esteban/src/MobilityDB-TPCDS/sf' || SF || '/';

/******************************************************************************
 * item
 *****************************************************************************/

DROP TABLE IF EXISTS scd_item CASCADE;
CREATE TABLE scd_item(
  i_item_sk int PRIMARY KEY,
  i_item_id char(16) NOT NULL,
  i_rec_start_date date NULL,
  i_rec_end_date date NULL,
  i_item_desc varchar(200) NULL,
  i_current_price decimal(7, 2) NULL,
  i_wholesale_cost decimal(7, 2) NULL,
  i_brand_id int NULL,
  i_brand char(50) NULL,
  i_class_id int NULL,
  i_class char(50) NULL,
  i_category_id int NULL,
  i_category char(50) NULL,
  i_manufact_id int NULL,
  i_manufact char(50) NULL,
  i_size char(20) NULL,
  i_formulation char(20) NULL,
  i_color char(20) NULL,
  i_units char(10) NULL,
  i_container char(10) NULL,
  i_manager_id int NULL,
  i_product_name char(50) NULL,
  UNIQUE (i_item_id, i_rec_start_date)
);

EXECUTE format('COPY scd_item FROM ''%sscd_item.csv'' DELIMITER '',''  CSV HEADER', Path);

DROP TABLE IF EXISTS mobdb_item CASCADE;
CREATE TABLE mobdb_item(
  i_item_id char(16) PRIMARY KEY,
  i_item_desc varchar(200) NULL,
  i_item_vt datespanset
);

-- In the table scd_item there are multiple i_item_desc values for a single
-- i_item_id. Since we do not keep the evolution of i_item_desc, we take the
-- latest values of them.
INSERT INTO mobdb_item
WITH temp AS (
  SELECT i_item_id, i_item_desc, RANK() OVER (PARTITION BY i_item_id
    ORDER BY i_rec_start_date DESC) AS desc_rank
  FROM scd_item )
SELECT i_item_id, i_item_desc
FROM temp
WHERE desc_rank = 1;

UPDATE mobdb_item I
SET i_item_vt = (
  SELECT spanUnion(span(i_rec_start_date, i_rec_end_date))
  FROM scd_item S
  WHERE S.i_item_id = I.i_item_id );

DROP TABLE IF EXISTS tdw_item CASCADE;
CREATE TABLE tdw_item(
  i_item_id char(16) PRIMARY KEY,
  i_item_desc varchar(200) NULL
);

INSERT INTO tdw_item
SELECT i_item_id, i_item_desc
FROM mobdb_item;

DROP TABLE IF EXISTS tdw_item_ls CASCADE;
CREATE TABLE tdw_item_ls(
  i_item_id char(16) NOT NULL,
  FromDate date NOT NULL,
  ToDate date NOT NULL,
  PRIMARY KEY (i_item_id, FromDate),
  FOREIGN KEY (i_item_id) REFERENCES tdw_item(i_item_id)
);

INSERT INTO tdw_item_ls(i_item_id, FromDate, ToDate)
WITH temp(i_item_id, i_item_vt) AS (
  SELECT i_item_id, unnest(spans(i_item_vt))
  FROM mobdb_item
  GROUP BY i_item_id )
SELECT i_item_id, lower(i_item_vt), upper(i_item_vt)
FROM temp
ORDER BY i_item_id;

/******************************************************************************
 * brand
 *****************************************************************************/

DROP TABLE IF EXISTS mobdb_brand CASCADE;
CREATE TABLE mobdb_brand(
  i_brand_id int PRIMARY KEY,
  i_brand char(50),
  i_brand_vt datespanset
);

-- In the table scd_item there are multiple i_brand_id values for a single
-- i_brand_id. Since we do not keep the evolution of i_brand_id, we take the
-- latest values of them.
INSERT INTO mobdb_brand(i_brand_id, i_brand)
WITH temp AS (
  SELECT i_brand_id, i_brand, RANK() OVER (PARTITION BY i_brand_id
    ORDER BY i_item_id, i_rec_start_date DESC) AS brand_rank
  FROM scd_item )
SELECT i_brand_id, i_brand
FROM temp
WHERE brand_rank = 1;

UPDATE mobdb_brand I
SET i_brand_vt = (
  SELECT spanUnion(span(i_rec_start_date, i_rec_end_date))
  FROM scd_item S
  WHERE S.i_brand_id = I.i_brand_id );

DROP TABLE IF EXISTS tdw_brand CASCADE;
CREATE TABLE tdw_brand(
  i_brand_id int PRIMARY KEY,
  i_brand char(50)
);

INSERT INTO tdw_brand
SELECT i_brand_id, i_brand
FROM mobdb_brand;

/******************************************************************************
 * category
 *****************************************************************************/

DROP TABLE IF EXISTS mobdb_category CASCADE;
CREATE TABLE mobdb_category(
  i_category_id int PRIMARY KEY,
  i_category char(50),
  i_category_vt datespanset
);

-- In the table scd_item there are multiple i_category_id values for a single
-- i_category_id. Since we do not keep the evolution of i_category_id, we take the
-- latest values of them.
INSERT INTO mobdb_category(i_category_id, i_category)
WITH temp AS (
  SELECT i_category_id, i_category, RANK() OVER (PARTITION BY i_category_id
    ORDER BY i_item_id, i_rec_start_date DESC) AS category_rank
  FROM scd_item )
SELECT i_category_id, i_category
FROM temp
WHERE category_rank = 1;

UPDATE mobdb_category I
SET i_category_vt = (
  SELECT spanUnion(span(i_rec_start_date, i_rec_end_date))
  FROM scd_item S
  WHERE S.i_category_id = I.i_category_id );

DROP TABLE IF EXISTS tdw_category CASCADE;
CREATE TABLE tdw_category(
  i_category_id int PRIMARY KEY,
  i_category char(50) NULL
);

INSERT INTO tdw_category
SELECT i_category_id, i_category
FROM mobdb_category;

DROP TABLE IF EXISTS tdw_category_vt CASCADE;
CREATE TABLE tdw_category_vt(
  i_category_id int NOT NULL,
  FromDate date NOT NULL,
  ToDate date NULL,
  PRIMARY KEY (i_category_id, FromDate),
  FOREIGN KEY (i_category_id) REFERENCES tdw_category(i_category_id)
);

/******************************************************************************
 * item_category
 *****************************************************************************/

DROP TABLE IF EXISTS mobdb_item_category CASCADE;
CREATE TABLE mobdb_item_category(
  i_item_id char(16) NOT NULL,
  i_category_id int NOT NULL,
  i_item_category_vt datespanset,
  PRIMARY KEY (i_item_id, i_category_id),
  FOREIGN KEY (i_item_id) REFERENCES mobdb_item (i_item_id),
  FOREIGN KEY (i_category_id) REFERENCES mobdb_category (i_category_id)
);

INSERT INTO mobdb_item_category(i_item_id, i_category_id, i_item_category_vt)
SELECT i_item_id, i_category_id,
  spanUnion(span(i_rec_start_date, i_rec_end_date))
FROM scd_item
GROUP BY i_item_id, i_category_id
ORDER BY i_item_id;

DROP TABLE IF EXISTS tdw_item_category CASCADE;
CREATE TABLE tdw_item_category(
  i_item_id char(16) NOT NULL,
  i_category_id int NOT NULL,
  FromDate date NOT NULL,
  ToDate date NULL,
  PRIMARY KEY (i_item_id, i_category_id, FromDate),
  FOREIGN KEY (i_item_id) REFERENCES tdw_item(i_item_id),
  FOREIGN KEY (i_category_id) REFERENCES tdw_category(i_category_id)
);

INSERT INTO tdw_item_category(i_item_id, i_category_id, FromDate, ToDate)
WITH temp(i_item_id, i_category_id, i_item_category_vt) AS (
  SELECT i_item_id, i_category_id, unnest(spans(i_item_category_vt))
  FROM mobdb_item_category )
SELECT i_item_id, i_category_id, lower(i_item_category_vt), upper(i_item_category_vt)
FROM temp
ORDER BY i_item_id;

/******************************************************************************
 * item_brand
 *****************************************************************************/

DROP TABLE IF EXISTS mobdb_item_brand CASCADE;
CREATE TABLE mobdb_item_brand(
  i_item_id char(16) NOT NULL,
  i_brand_id int NOT NULL,
  i_item_brand_vt datespanset,
  PRIMARY KEY (i_item_id, i_brand_id),
  FOREIGN KEY (i_item_id) REFERENCES mobdb_item (i_item_id),
  FOREIGN KEY (i_brand_id) REFERENCES mobdb_brand (i_brand_id)
);

INSERT INTO mobdb_item_brand(i_item_id, i_brand_id, i_item_brand_vt)
SELECT i_item_id, i_brand_id,
  spanUnion(span(i_rec_start_date, i_rec_end_date))
FROM scd_item
GROUP BY i_item_id, i_brand_id
ORDER BY i_item_id;

DROP TABLE IF EXISTS tdw_item_brand CASCADE;
CREATE TABLE tdw_item_brand(
  i_item_id char(16) NOT NULL,
  i_brand_id int NOT NULL,
  FromDate date NOT NULL,
  ToDate date NULL,
  PRIMARY KEY (i_item_id, i_brand_id, FromDate),
  FOREIGN KEY (i_item_id) REFERENCES tdw_item(i_item_id),
  FOREIGN KEY (i_brand_id) REFERENCES tdw_brand(i_brand_id)
);

INSERT INTO tdw_item_brand(i_item_id, i_brand_id, FromDate, ToDate)
WITH temp(i_item_id, i_brand_id, i_item_brand_vt) AS (
  SELECT i_item_id, i_brand_id, unnest(spans(i_item_brand_vt))
  FROM mobdb_item_brand )
SELECT i_item_id, i_brand_id, lower(i_item_brand_vt), upper(i_item_brand_vt)
FROM temp
ORDER BY i_item_id;

/******************************************************************************
 * item_price
 *****************************************************************************/

DROP TABLE IF EXISTS mobdb_item_price CASCADE;
CREATE TABLE mobdb_item_price(
  i_item_id char(16) NOT NULL,
  i_item_price decimal(7, 2) NULL,
  i_item_price_vt datespanset,
  PRIMARY KEY (i_item_id, i_item_price),
  FOREIGN KEY (i_item_id) REFERENCES tdw_item (i_item_id)
);

INSERT INTO mobdb_item_price(i_item_id, i_item_price, i_item_price_vt)
SELECT i_item_id, i_current_price,
  spanUnion(span(i_rec_start_date, i_rec_end_date))
FROM scd_item
GROUP BY i_item_id, i_current_price
ORDER BY i_item_id, i_current_price;

DROP TABLE IF EXISTS tdw_item_price CASCADE;
CREATE TABLE tdw_item_price(
  i_item_id char(16) NOT NULL,
  i_item_price decimal(7, 2) NULL,
  FromDate date NOT NULL,
  ToDate date NOT NULL,
  PRIMARY KEY (i_item_id, i_item_price, FromDate, ToDate),
  FOREIGN KEY (i_item_id) REFERENCES tdw_item (i_item_id)
);

INSERT INTO tdw_item_price(i_item_id, i_item_price, FromDate, ToDate)
WITH temp(i_item_id, i_item_price, i_item_price_vt) AS (
  SELECT i_item_id, i_item_price, unnest(spans(i_item_price_vt))
  FROM mobdb_item_price )
SELECT i_item_id, i_item_price, lower(i_item_price_vt), upper(i_item_price_vt)
FROM temp
ORDER BY i_item_id, i_item_price;

/******************************************************************************
 * Date
 ******************************************************************************/

DROP TABLE IF EXISTS date_dim CASCADE;
CREATE TABLE date_dim(
  d_date_sk int PRIMARY KEY,
  d_date_id char(16) NOT NULL,
  d_date date NULL,
  d_month_seq int NULL,
  d_week_seq int NULL,
  d_quarter_seq int NULL,
  d_year int NULL,
  d_dow int NULL,
  d_moy int NULL,
  d_dom int NULL,
  d_qoy int NULL,
  d_fy_year int NULL,
  d_fy_quarter_seq int NULL,
  d_fy_week_seq int NULL,
  d_day_name char(9) NULL,
  d_quarter_name char(6) NULL,
  d_holiday char(1) NULL,
  d_weekend char(1) NULL,
  d_following_holiday char(1) NULL,
  d_first_dom int NULL,
  d_last_dom int NULL,
  d_same_day_ly int NULL,
  d_same_day_lq int NULL,
  d_current_day char(1) NULL,
  d_current_week char(1) NULL,
  d_current_month char(1) NULL,
  d_current_quarter char(1) NULL,
  d_current_year char(1) NULL
);

EXECUTE format('COPY date_dim FROM ''%sdate_dim.csv'' DELIMITER '',''  CSV HEADER', Path);

CREATE INDEX date_dim_bree_idx ON date_dim USING BTREE(d_date);

/* We need to add a column with type datespan for MobilityDB since it does
 * not currently have a datespan data type */
ALTER TABLE date_dim ADD COLUMN d_datespan datespan;
UPDATE date_dim SET d_datespan = span(d_date);
CREATE INDEX date_dim_gist_idx ON date_dim USING GIST(d_datespan);

/******************************************************************************
 * store_sales
 ******************************************************************************/

/* The table is UNLOGGED to speed up the loading for large scale factors */
DROP TABLE IF EXISTS store_sales CASCADE;
CREATE UNLOGGED TABLE store_sales(
  ss_sold_date_sk int NOT NULL,
  ss_sold_time_sk int NULL,
  ss_item_sk int NULL,
  ss_customer_sk int NULL,
  ss_cdemo_sk int NULL,
  ss_hdemo_sk int NULL,
  ss_addr_sk int NULL,
  ss_store_sk int NULL,
  ss_promo_sk int NULL,
  ss_ticket_number int NOT NULL,
  ss_quantity int NULL,
  ss_wholesale_cost decimal(7, 2) NULL,
  ss_list_price decimal(7, 2) NULL,
  ss_sales_price decimal(7, 2) NULL,
  ss_ext_discount_amt decimal(7, 2) NULL,
  ss_ext_sales_price decimal(7, 2) NULL,
  ss_ext_wholesale_cost decimal(7, 2) NULL,
  ss_ext_list_price decimal(7, 2) NULL,
  ss_ext_tax decimal(7, 2) NULL,
  ss_coupon_amt decimal(7, 2) NULL,
  ss_net_paid decimal(7, 2) NULL,
  ss_net_paid_inc_tax decimal(7, 2) NULL,
  ss_net_profit decimal(7, 2) NULL
);

EXECUTE format('COPY store_sales FROM ''%sstore_sales.csv'' DELIMITER '',''  CSV HEADER', Path);

/* In MobilityDB there is no SK for items, we use the ss_item_id key in the Items table */
ALTER TABLE store_sales ADD COLUMN ss_item_id char(16);

UPDATE store_sales s
SET ss_item_id = i.i_item_id
FROM scd_item i
WHERE s.ss_item_sk = i.i_item_sk;

/* Add the constraints after the fact tables have been created */

ALTER TABLE store_sales ADD CONSTRAINT store_sales_pk
  PRIMARY KEY (ss_item_sk, ss_sold_date_sk, ss_ticket_number);
ALTER TABLE store_sales ADD CONSTRAINT store_sales_fk_date
  FOREIGN KEY(ss_sold_date_sk) REFERENCES date_dim (d_date_sk);
ALTER TABLE store_sales ADD CONSTRAINT store_sales_fk_item
  FOREIGN KEY(ss_item_sk) REFERENCES scd_item (i_item_sk);
ALTER TABLE store_sales ADD CONSTRAINT fk_item_id
 FOREIGN KEY(ss_item_id) REFERENCES tdw_item (i_item_id);

ALTER TABLE store_sales SET LOGGED;

-- ANALYZE;

/******************************************************************************/

  RETURN 'The End';
END;
$$ LANGUAGE 'plpgsql';

/*****************************************************************************/