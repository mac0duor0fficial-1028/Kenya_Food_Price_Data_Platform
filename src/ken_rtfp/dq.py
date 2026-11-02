import json
import pandas as pd
from sqlalchemy import text
from .config import load_yaml

def run_checks(engine, run_id: str, clean_path: str, extracted_rows: int, rejected_rows: int) -> bool:
    rules = load_yaml("dq_rules.yaml")["checks"]
    df = pd.read_parquet(clean_path)
    results = []

    def rec(rule, passed, failed=0, **detail):
        results.append((rule["name"], rule["severity"], bool(passed), int(failed), detail))

    for r in rules:
        n = r["name"]
        if n == "row_count_min":
            rec(r, len(df) >= r["min_rows"], 0, rows=len(df), min=r["min_rows"])
        elif n == "not_null_keys":
            bad = int(df[r["columns"]].isna().any(axis=1).sum()); rec(r, bad == 0, bad)
        elif n == "price_positive":
            bad = int((df["price"] <= 0).sum()); rec(r, bad == 0, bad)
        elif n == "unique_grain":
            bad = int(df.duplicated(r["grain"]).sum()); rec(r, bad == 0, bad)
        elif n == "date_range":
            bad = int(((df.month_start < r["min"]) | (df.month_start > r["max"])).sum()); rec(r, bad == 0, bad)
        elif n == "price_outlier_iqr":
            g = df.groupby(r["group_by"])["price"]
            q1, q3 = g.transform(lambda s: s.quantile(.25)), g.transform(lambda s: s.quantile(.75))
            iqr = q3 - q1
            bad = int(((df.price < q1 - r["k"] * iqr) | (df.price > q3 + r["k"] * iqr)).sum()); rec(r, bad == 0, bad)
        elif n == "coords_in_kenya":
            lat, lon = df["latitude"], df["longitude"]
            bad = int((lat.notna() & ~lat.between(*r["lat"])).sum() + (lon.notna() & ~lon.between(*r["lon"])).sum()); rec(r, bad == 0, bad)
        elif n == "fk_orphans":
            with engine.connect() as c:
                bad = c.execute(text("""SELECT count(*) FROM dw.fact_food_price f
                  LEFT JOIN dw.dim_market m USING(market_key) LEFT JOIN dw.dim_product p USING(product_key)
                  LEFT JOIN dw.dim_date d USING(date_key)
                  WHERE m.market_key IS NULL OR p.product_key IS NULL OR d.date_key IS NULL""")).scalar()
            rec(r, bad == 0, bad)
        elif n == "rowcount_vs_source":
            with engine.connect() as c:
                loaded = c.execute(text("SELECT count(*) FROM dw.fact_food_price WHERE run_id=:r"), dict(r=run_id)).scalar()
            rec(r, loaded + rejected_rows == extracted_rows, abs(extracted_rows - loaded - rejected_rows),
                extracted=extracted_rows, loaded=loaded, rejected=rejected_rows)

    with engine.begin() as c:
        for name, sev, ok, failed, detail in results:
            c.execute(text("""INSERT INTO meta.dq_result(run_id,check_name,severity,passed,failed_rows,detail)
                VALUES (:r,:n,:s,:p,:f,CAST(:d AS JSONB))"""),
                dict(r=run_id, n=name, s=sev, p=ok, f=failed, d=json.dumps(detail)))
    return all(ok for _, sev, ok, _, _ in results if sev == "error")