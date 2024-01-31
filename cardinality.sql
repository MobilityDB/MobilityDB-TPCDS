------------------------------------------------------------------------------

SELECT 'store_sales' AS "table", COUNT(*),
  pg_size_pretty(pg_total_relation_size('store_sales')) AS "size" FROM store_sales;
SELECT 'date_dim' AS "table", COUNT(*),
  pg_size_pretty(pg_total_relation_size('date_dim')) AS "size" FROM date_dim;

SELECT 'scd_item' AS "table", COUNT(*),
  pg_size_pretty(pg_total_relation_size('scd_item')) AS "size" FROM scd_item;

SELECT 'tdw_item' AS "table", COUNT(*),
  pg_size_pretty(pg_total_relation_size('tdw_item')) AS "size" FROM tdw_item;
SELECT 'tdw_item_ls' AS "table", COUNT(*),
  pg_size_pretty(pg_total_relation_size('tdw_item_ls')) AS "size" FROM tdw_item_ls;
SELECT 'tdw_item_price' AS "table", COUNT(*),
  pg_size_pretty(pg_total_relation_size('tdw_item_price')) AS "size" FROM tdw_item_price;
SELECT 'tdw_brand' AS "table", COUNT(*),
  pg_size_pretty(pg_total_relation_size('tdw_brand')) AS "size" FROM tdw_brand;
SELECT 'tdw_category' AS "table", COUNT(*),
  pg_size_pretty(pg_total_relation_size('tdw_category')) AS "size" FROM tdw_category;
SELECT 'tdw_item_brand' AS "table", COUNT(*),
  pg_size_pretty(pg_total_relation_size('tdw_item_brand')) AS "size" FROM tdw_item_brand;
SELECT 'tdw_item_category' AS "table", COUNT(*),
  pg_size_pretty(pg_total_relation_size('tdw_item_category')) AS "size" FROM tdw_item_category;

SELECT 'mobdb_item' AS "table", COUNT(*),
  pg_size_pretty(pg_total_relation_size('mobdb_item')) AS "size" FROM mobdb_item;
SELECT 'mobdb_item_price' AS "table", COUNT(*),
  pg_size_pretty(pg_total_relation_size('mobdb_item_price')) AS "size" FROM mobdb_item_price;
SELECT 'mobdb_brand' AS "table", COUNT(*),
  pg_size_pretty(pg_total_relation_size('mobdb_brand')) AS "size" FROM mobdb_brand;
SELECT 'mobdb_category' AS "table", COUNT(*),
  pg_size_pretty(pg_total_relation_size('mobdb_category')) AS "size" FROM mobdb_category;
SELECT 'mobdb_item_brand' AS "table", COUNT(*),
  pg_size_pretty(pg_total_relation_size('mobdb_item_brand')) AS "size" FROM mobdb_item_brand;
SELECT 'mobdb_item_category' AS "table", COUNT(*),
  pg_size_pretty(pg_total_relation_size('mobdb_item_category')) AS "size" FROM mobdb_item_category;

------------------------------------------------------------------------------
