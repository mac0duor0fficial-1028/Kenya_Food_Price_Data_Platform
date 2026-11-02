# tests/test_transform.py
import pandas as pd
from ken_rtfp.transform import transform

def test_nonpositive_price_is_rejected(tmp_path):
    # Build a small raw parquet using the source column names from schema_map.yaml,
    # include one row with price 0, call transform(), and assert that row
    # appears in the rejects file with reason 'bad_price;' and not in the clean file.
    ...