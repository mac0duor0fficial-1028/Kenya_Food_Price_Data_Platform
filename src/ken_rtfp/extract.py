import pandas as pd
from pathlib import Path
from .config import ROOT, load_yaml

def extract() -> Path:
    cfg = load_yaml("schema_map.yaml")["source"]
    src = ROOT / cfg["file"]
    if not src.exists():
        raise FileNotFoundError(f"Download the dataset to {src} (see Section 4).")
    readers = {
        "csv":   lambda: pd.read_csv(src, encoding=cfg.get("encoding", "utf-8"), dtype=str),
        "stata": lambda: pd.read_stata(src, convert_categoricals=False).astype(object),
        "excel": lambda: pd.read_excel(src, dtype=str),
    }
    df = readers[cfg["format"]]()
    df.insert(0, "source_row", range(2, len(df) + 2))   # 1-based, header = row 1
    out = ROOT / "data/staging/raw.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.astype(str).to_parquet(out, index=False)
    return out