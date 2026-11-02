CREATE OR REPLACE VIEW dw.v_food_price AS
SELECT f.date_key, d.month_start, d.year, d.quarter, d.year_month,
       m.market_name, m.admin1, m.latitude, m.longitude,
       p.product_name, p.food_group, p.unit,
       f.price, f.currency, f.run_id
FROM dw.fact_food_price f
JOIN meta.published_run pr ON pr.run_id = f.run_id
JOIN dw.dim_date d    USING (date_key)
JOIN dw.dim_market m  USING (market_key)
JOIN dw.dim_product p USING (product_key);

CREATE OR REPLACE VIEW dw.v_lineage AS
SELECT run_id, source_name, source_sha256, git_commit, status,
       started_at, finished_at, rows_extracted, rows_clean,
       rows_rejected, rows_loaded
FROM meta.etl_run;