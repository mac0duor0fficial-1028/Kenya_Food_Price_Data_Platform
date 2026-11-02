import json
import pandas as pd
from sqlalchemy import text

def _nn(df):
    return df.astype(object).where(df.notna(), None)

def load_dims(engine, clean_path: str):
    df = pd.read_parquet(clean_path)
    months = (df[["month_start"]].drop_duplicates().assign(
        date_key=lambda x: x.month_start.dt.strftime("%Y%m01").astype(int),
        year=lambda x: x.month_start.dt.year, quarter=lambda x: x.month_start.dt.quarter,
        month=lambda x: x.month_start.dt.month,
        month_name=lambda x: x.month_start.dt.month_name(),
        year_month=lambda x: x.month_start.dt.strftime("%Y-%m")))
    months["month_start"] = months["month_start"].dt.date
    mk = df[["market_name", "admin1", "latitude", "longitude"]].drop_duplicates(["market_name", "admin1"])
    pr = df[["product_name", "unit"]].drop_duplicates("product_name")
    with engine.begin() as c:
        c.execute(text("""INSERT INTO dw.dim_date(date_key,month_start,year,quarter,month,month_name,year_month)
            VALUES (:date_key,:month_start,:year,:quarter,:month,:month_name,:year_month)
            ON CONFLICT (date_key) DO NOTHING"""), _nn(months).to_dict("records"))
        c.execute(text("""INSERT INTO dw.dim_market(market_name,admin1,latitude,longitude)
            VALUES (:market_name,:admin1,:latitude,:longitude)
            ON CONFLICT (market_name, admin1) DO UPDATE
              SET latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude"""),
            _nn(mk).to_dict("records"))
        c.execute(text("""INSERT INTO dw.dim_product(product_name,unit)
            VALUES (:product_name,:unit) ON CONFLICT (product_name) DO NOTHING"""),
            _nn(pr).to_dict("records"))

def load_fact(engine, clean_path: str, rejects_path: str, run_id: str) -> int:
    df = pd.read_parquet(clean_path)
    with engine.begin() as c:
        mkeys = pd.read_sql("SELECT market_key, market_name, admin1 FROM dw.dim_market", c)
        pkeys = pd.read_sql("SELECT product_key, product_name FROM dw.dim_product", c)
        df = (df.merge(mkeys, on=["market_name", "admin1"], how="left")
                .merge(pkeys, on="product_name", how="left"))
        cols = ["date_key", "market_key", "product_key", "price", "currency", "run_id", "source_row"]
        rows = _nn(df[cols]).to_dict("records")
        c.execute(text("""INSERT INTO dw.fact_food_price
              (date_key,market_key,product_key,price,currency,run_id,source_row)
            VALUES (:date_key,:market_key,:product_key,:price,:currency,:run_id,:source_row)
            ON CONFLICT (date_key,market_key,product_key) DO UPDATE
              SET price=EXCLUDED.price, currency=EXCLUDED.currency,
                  run_id=EXCLUDED.run_id, source_row=EXCLUDED.source_row, loaded_at=now()"""), rows)
        rj = pd.read_parquet(rejects_path)
        if len(rj):
            c.execute(text("""INSERT INTO meta.reject_row(run_id,source_row,reason,payload)
               VALUES (:r,:s,:why,CAST(:p AS JSONB)) ON CONFLICT DO NOTHING"""),
               [dict(r=run_id, s=int(x.source_row), why=x.reason, p=json.dumps(x._asdict(), default=str))
                for x in rj.itertuples(index=False)])
    return len(rows)