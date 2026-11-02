import pandas as pd
from .config import ROOT, load_yaml

def transform(staging: str, run_id: str) -> tuple[str, str]:
    cfg = load_yaml("schema_map.yaml")
    cols, const, dcfg = cfg["columns"], cfg["constants"], cfg["date"]
    raw = pd.read_parquet(staging).replace({"nan": None, "None": None, "": None})
    raw["source_row"] = raw["source_row"].astype(int)
    df = raw.rename(columns={v: k for k, v in cols.items() if v in raw.columns})

    for c in ["market_name", "admin1", "product_name", "unit", "currency"]:
        if c not in df:
            df[c] = None
        df[c] = df[c].astype("string").str.strip().str.replace(r"\s+", " ", regex=True)
    df["market_name"] = df["market_name"].str.title()
    df["product_name"] = df["product_name"].str.lower().str.capitalize()
    df["admin1"] = df["admin1"].fillna("")

    for c in ["price", "latitude", "longitude"]:
        df[c] = pd.to_numeric(df[c], errors="coerce") if c in df else pd.NA

    if dcfg["mode"] == "year_month":
        y = pd.to_numeric(raw[dcfg["year_col"]], errors="coerce")
        m = pd.to_numeric(raw[dcfg["month_col"]], errors="coerce")
        d = pd.to_datetime(dict(year=y, month=m, day=1), errors="coerce")
    else:
        d = pd.to_datetime(df["price_date"], format=dcfg.get("format"), errors="coerce")
    df["month_start"] = d.dt.to_period("M").dt.to_timestamp()
    df["date_key"] = df["month_start"].dt.strftime("%Y%m01").astype("Int64")

    df["currency"] = df["currency"].fillna(const["currency_default"]).str.upper()
    df["unit"] = df["unit"].fillna(const["price_unit_default"])

    reasons = pd.Series("", index=df.index)
    reasons[df["price"].isna() | (df["price"] <= 0)] += "bad_price;"
    reasons[df["month_start"].isna()] += "bad_date;"
    reasons[df["market_name"].isna() | df["product_name"].isna()] += "missing_key;"
    rejects = df[reasons != ""].assign(reason=reasons[reasons != ""])
    clean = df[reasons == ""].copy()
    clean["run_id"] = run_id

    out_c, out_r = ROOT / "data/clean/clean.parquet", ROOT / "data/clean/rejects.parquet"
    out_c.parent.mkdir(parents=True, exist_ok=True)
    clean.to_parquet(out_c, index=False)
    rejects.astype(str).to_parquet(out_r, index=False)
    return str(out_c), str(out_r)