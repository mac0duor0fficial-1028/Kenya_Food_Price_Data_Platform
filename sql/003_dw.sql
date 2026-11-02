CREATE TABLE IF NOT EXISTS dw.dim_date (
  date_key     INTEGER PRIMARY KEY,         -- yyyymm01, e.g. 20210101
  month_start  DATE NOT NULL UNIQUE,
  year         SMALLINT NOT NULL,
  quarter      SMALLINT NOT NULL,
  month        SMALLINT NOT NULL,
  month_name   TEXT NOT NULL,
  year_month   TEXT NOT NULL                -- '2021-01'
);

CREATE TABLE IF NOT EXISTS dw.dim_market (
  market_key   SERIAL PRIMARY KEY,
  market_name  TEXT NOT NULL,
  admin1       TEXT NOT NULL DEFAULT '',
  latitude     NUMERIC(9,6),
  longitude    NUMERIC(9,6),
  country_iso3 CHAR(3) NOT NULL DEFAULT 'KEN',
  UNIQUE (market_name, admin1)
);

CREATE TABLE IF NOT EXISTS dw.dim_product (
  product_key   SERIAL PRIMARY KEY,
  product_name  TEXT NOT NULL UNIQUE,
  food_group    TEXT,                       -- optional, from a seed mapping file
  unit          TEXT
);

CREATE TABLE IF NOT EXISTS dw.fact_food_price (
  date_key     INTEGER NOT NULL REFERENCES dw.dim_date(date_key),
  market_key   INTEGER NOT NULL REFERENCES dw.dim_market(market_key),
  product_key  INTEGER NOT NULL REFERENCES dw.dim_product(product_key),
  price        NUMERIC(14,4) NOT NULL CHECK (price > 0),
  currency     CHAR(3) NOT NULL,
  -- lineage
  run_id       UUID NOT NULL REFERENCES meta.etl_run(run_id),
  source_row   INTEGER NOT NULL,
  loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (date_key, market_key, product_key)
);
CREATE INDEX IF NOT EXISTS ix_fact_product 
ON dw.fact_food_price (product_key, date_key);

CREATE INDEX IF NOT EXISTS ix_fact_market  
ON dw.fact_food_price (market_key, date_key);