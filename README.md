# Kenya_Food_Price_Data_Platform
---
This is a reproducible, data-integrity-focused batch ETL pipeline that automates the ingestion, transformation, and optimization of sub-national market price tracking datasets. Built using Apache Airflow, pandas, and a PostgreSQL star schema, this platform structures fragile real-time development indicators into analytics-ready dimensions and facts for reporting inside Power BI.
> Airflow → pandas → PostgreSQL → Power BI

A reproducible batch pipeline for the World Bank dataset
**Monthly food price estimates by product and market, Kenya** (`KEN_2021_RTFP_v02_M`).
Source: <https://microdata.worldbank.org/catalog/6167>

```mermaid
graph LR
    %% Styles
    classDef step fill:#1e1e1e,stroke:#888,stroke-width:1px,color:#fff;
    classDef db fill:#1e1e1e,stroke:#888,stroke-width:1px,color:#fff;
    classDef decision fill:#1e1e1e,stroke:#888,stroke-width:1px,color:#fff;

    %% Nodes
    RawFile[("Raw file")]:::db
    Extract["Extract"]:::step
    Transform["Transform"]:::step
    Load["Load<br>dims + fact"]:::step
    DQ{"DQ checks"}:::decision
    Publish["Publish"]:::step
    Blocked["Blocked"]:::step
    PowerBI["Power BI"]:::step
    Postgres[("PostgreSQL<br>star schema")]:::db
    Lineage[("Lineage<br>meta.etl_run, reject_row,<br>dq_result")]:::db

    %% Main ETL Flow
    RawFile --> Extract
    Extract --> Transform
    Transform --> Load
    Load --> DQ

    %% DQ Conditional Branches
    DQ -- pass --> Publish
    DQ -- error --> Blocked

    %% Outputs from Publish
    Publish --> PowerBI
    Publish --> Postgres

    %% Lineage and Metadata Dotted Connections
    Extract -.-> Lineage
    Transform -.-> Lineage
    Load -.-> Lineage
    Load -.-> Postgres
    DQ -.-> Lineage
    Blocked -.-> Lineage
    Publish -.-> Lineage
```

- **Orchestration:** Apache Airflow (one DAG, 7 tasks)
- **Transform:** pandas + Parquet staging files
- **Warehouse:** PostgreSQL star schema (`dim_date`, `dim_market`, `dim_product`, `fact_food_price`)
- **Lineage:** every run records the source file SHA-256, git commit, `uv.lock` hash and row counts; every fact row stores its `run_id` and `source_row`
- **Data quality:** rule-driven checks; error-severity failures block publishing
- **Environment:** [uv](https://docs.astral.sh/uv/)

> **Status note.** The code in this guide was written without access to the real data file. Column names in `config/schema_map.yaml` are placeholders: Step 4 shows how to fix them in a few minutes. Expect to adjust the config on first run.

---
## Repository layout

```text
.
├── README.md
├── pyproject.toml / uv.lock / .python-version
├── constraints/airflow-2.10.5-py3.11.txt
├── docker-compose.yml
├── .env.example
├── run_local.py
├── config/            schema_map.yaml, dq_rules.yaml
├── sql/               001_schemas … 004_views
├── src/ken_rtfp/      config, lineage, extract, transform, load, dq
├── dags/              ken_rtfp_dag.py
├── tests/
├── data/raw/SHA256SUMS      (raw files themselves are git-ignored)
├── docs/images/
└── powerbi/           .pbip project
```

## 1. Requirements

| Tool | Version | Why |
|---|---|---|
| Python | 3.11 (installed by uv) | Airflow 2.10 supports 3.8–3.12; 3.11 is pinned here |
| [uv](https://docs.astral.sh/uv/getting-started/installation/) | recent | environments and lockfile |
| Docker + Docker Compose | recent | PostgreSQL |
| Power BI Desktop | recent (Windows) | dashboard |
| Git | any | lineage records the commit |

## 2. Python libraries

**Pipeline (always needed)**

| Library | Purpose |
|---|---|
| `pandas` | transform |
| `pyarrow` | Parquet staging files |
| `sqlalchemy` | database access |
| `psycopg2-binary` | PostgreSQL driver |
| `pyyaml` | read the YAML config |
| `pydantic-settings` | read `.env` settings (installs `pydantic`) |
| `openpyxl` | *only if* the source file is Excel |

**Orchestration:** `apache-airflow==2.10.5` (installed against its official constraints file, see Step 3).

**Development:** `pytest`, `ruff`.

> **Why `psycopg2` and not `psycopg` (v3)?** Airflow 2.10 pins SQLAlchemy 1.4, and the `postgresql+psycopg` (v3) dialect needs SQLAlchemy 2. The code here is written to work on both SQLAlchemy 1.4 and 2.x, so use `psycopg2-binary` and the DSN `postgresql+psycopg2://...`.

## 3. Setup

### 3.1 Create the project

```bash
git clone <your-repo-url> ken-rtfp && cd ken-rtfp      # or: mkdir ken-rtfp && cd ken-rtfp && git init

uv init --package --name ken-rtfp --python 3.11         # skip if pyproject.toml already exists
uv python pin 3.11

uv add pandas pyarrow sqlalchemy psycopg2-binary pyyaml pydantic-settings
uv add --dev pytest ruff
# uv add openpyxl            # only for an Excel source file
```

### 3.2 Install Airflow (with constraints)

Airflow must be installed against its constraints file, otherwise dependency resolution can break.

```bash
mkdir -p constraints
curl -sSL https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt \
  -o constraints/airflow-2.10.5-py3.11.txt

source .venv/bin/activate                 # Windows: .venv\Scripts\activate
uv pip install "apache-airflow==2.10.5" -c constraints/airflow-2.10.5-py3.11.txt
```

Two rules to keep Airflow in place:

- After this step, run commands **inside the activated venv** (plain `python`, `airflow`, `pytest`), not `uv run`, because `uv run` re-syncs the environment to `uv.lock` and removes Airflow.
- To re-sync later without removing it, use `uv sync --inexact`.

Reproducibility trade-off: `uv.lock` pins the pipeline libraries; the Airflow install is pinned by the committed constraints file and version number. Commit both.

### 3.3 Folders and ignore rules

```bash
mkdir -p config data/raw data/staging data/clean sql dags tests src/ken_rtfp
cat > .gitignore <<'EOF'
.venv/
.airflow/
.env
data/raw/
data/staging/
data/clean/
__pycache__/
*.pyc
EOF
```

### 3.4 Add the code

Copy these files from the companion guide (the Claude doc *Reproducible Batch Pipeline: World Bank Kenya Food Prices (RTFP) to Power BI*) into the repo:

| File | Section of the guide |
|---|---|
| `docker-compose.yml`, `.env.example` | 3 |
| `config/schema_map.yaml`, `config/dq_rules.yaml` | 5 |
| `src/ken_rtfp/config.py` | 5 |
| `sql/001_schemas.sql` … `sql/004_views.sql` | 6 |
| `src/ken_rtfp/lineage.py`, `extract.py`, `transform.py`, `load.py`, `dq.py` | 7 |
| `dags/ken_rtfp_dag.py` | 8 |

Make sure `src/ken_rtfp/__init__.py` exists (it can be empty).

**Edit `.env.example` and `config.py` to use the psycopg2 driver:**

```
PG_DSN=postgresql+psycopg2://rtfp:rtfp@localhost:5433/rtfp
```

and in `config.py` set the default `pg_dsn` to the same string. Then:

```bash
cp .env.example .env
```

### 3.5 Start PostgreSQL

```bash
docker compose up -d
docker compose ps                         # wait until "healthy"/"running"
psql postgresql://rtfp:rtfp@localhost:5433/rtfp -c "\dt meta.*"
```

You should see `etl_run`, `dq_result`, `reject_row`, `published_run`. If the tables are missing, the init scripts did not run (they only run on a **fresh** volume). Reset with `docker compose down -v && docker compose up -d`.

No `psql` installed? Use: `docker compose exec warehouse psql -U rtfp -d rtfp -c "\dt meta.*"`.

## 4. Get the data and map the columns

1. Log in to the [World Bank Microdata Library](https://microdata.worldbank.org/catalog/6167), accept the terms, and download the data file and the codebook.
2. Save the data file **unchanged** into `data/raw/`.
3. Record its checksum (commit this file; the data itself is git-ignored because of the licence):

   ```bash
   sha256sum data/raw/KEN_2021_RTFP_v02_M.* > data/raw/SHA256SUMS
   ```
4. Inspect the real columns:

   ```bash
   python - <<'PY'
   import pandas as pd, glob
   f = glob.glob("data/raw/KEN_2021_RTFP_v02_M.*")[0]
   df = pd.read_csv(f) if f.endswith(".csv") else pd.read_stata(f)
   print(df.dtypes); print(df.head(10).to_string()); print(df.nunique())
   PY
   ```
5. Edit `config/schema_map.yaml`:
   - `source.file` and `source.format` (`csv`, `stata` or `excel`)
   - the right-hand side of every entry under `columns:` must be an actual column name in your file
   - `date.mode`: `single` if there is one date column; `year_month` if year and month are separate columns (then set `year_col` and `month_col`)
   - if a required field (e.g. unit, currency, region) does not exist, delete that line; the defaults in `constants:` are used

   **Check the grain:** the fact table holds one price per *market × product × month*. If the file has several rows at that grain (e.g. different price types), the run will fail at the duplicate check. Add the distinguishing column as a dimension rather than dropping rows.

## 5. Run the pipeline to completion

### 5.1 Fast path: run all steps in one command (recommended first)

Create `run_local.py` in the repo root. It runs exactly the same task functions as the DAG, in order, without Airflow:

```python
"""Run the full pipeline once, without the Airflow scheduler."""
import pandas as pd
from sqlalchemy import create_engine, text

from ken_rtfp.config import Settings, ROOT, load_yaml
from ken_rtfp import extract, transform, load, dq, lineage

engine = create_engine(Settings().pg_dsn)
src = ROOT / load_yaml("schema_map.yaml")["source"]["file"]

run_id = lineage.start_run(engine, src, dag_id="local", airflow_run_id="manual")
print("run_id:", run_id)

try:
    staging = extract.extract()
    rows = len(pd.read_parquet(staging))
    clean, rejects = transform.transform(str(staging), run_id)
    n_clean, n_rej = len(pd.read_parquet(clean)), len(pd.read_parquet(rejects))
    print(f"extracted={rows} clean={n_clean} rejected={n_rej}")

    load.load_dims(engine, clean)
    loaded = load.load_fact(engine, clean, rejects, run_id)
    counts = dict(rows_extracted=rows, rows_clean=n_clean, rows_rejected=n_rej, rows_loaded=loaded)

    if not dq.run_checks(engine, run_id, clean, rows, n_rej):
        lineage.finish_run(engine, run_id, "blocked_by_dq", **counts)
        raise SystemExit("DQ errors. See: SELECT * FROM meta.dq_result WHERE NOT passed;")

    with engine.begin() as c:
        c.execute(text("UPDATE meta.published_run SET run_id=:r WHERE id=1"), dict(r=run_id))
    lineage.finish_run(engine, run_id, "succeeded", **counts)
    print("SUCCESS: published", run_id)
except SystemExit:
    raise
except Exception:
    lineage.finish_run(engine, run_id, "failed")
    raise
```

Run it:

```bash
source .venv/bin/activate
python run_local.py
```

Expected output ends with `SUCCESS: published <uuid>`.

### 5.2 Verify the result

```bash
PSQL="psql postgresql://rtfp:rtfp@localhost:5433/rtfp"

$PSQL -c "SELECT status, rows_extracted, rows_clean, rows_rejected, rows_loaded FROM meta.etl_run ORDER BY started_at DESC LIMIT 1;"
$PSQL -c "SELECT check_name, severity, passed, failed_rows FROM meta.dq_result WHERE run_id=(SELECT run_id FROM meta.published_run);"
$PSQL -c "SELECT count(*) FROM dw.v_food_price;"
$PSQL -c "SELECT reason, count(*) FROM meta.reject_row GROUP BY 1;"
```

A correct run satisfies all of these:

- `status = succeeded`
- `rows_extracted = rows_loaded + rows_rejected`
- every `severity = 'error'` check has `passed = t`
- `count(*)` from `dw.v_food_price` equals `rows_loaded`

### 5.3 Run it through Airflow

```bash
source .venv/bin/activate
export AIRFLOW_HOME=$PWD/.airflow
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# (a) test the whole DAG once, no scheduler needed
airflow dags test ken_rtfp_monthly 2026-11-01

# (b) or run the UI + scheduler (prints an admin password on first start)
airflow standalone          # open http://localhost:8080, unpause and trigger ken_rtfp_monthly
```

If the DAG does not appear: `airflow dags list-import-errors`.

### 5.4 Re-running

Loads are upserts keyed on `(date, market, product)`, so re-running the same file is safe and produces the same data (with a new `run_id`). A new release of the file: replace it in `data/raw/`, refresh `SHA256SUMS`, run again.

## 6. Power BI dashboard

1. **Get Data → PostgreSQL database**, server `localhost:5433`, database `rtfp`, mode **Import**. (Windows may require the Npgsql provider.)
2. Load `dw.dim_date`, `dw.dim_market`, `dw.dim_product`, `dw.v_lineage`, and the fact data via **Advanced options → SQL statement**:

   ```sql
   SELECT f.* FROM dw.fact_food_price f
   JOIN meta.published_run p ON p.run_id = f.run_id;
   ```
3. Relationships (many-to-one, single direction): fact → dims on `date_key`, `market_key`, `product_key`. Mark `dim_date[month_start]` as the date table.
4. Measures:

   ```dax
   Avg Price = AVERAGE ( fact_food_price[price] )
   Avg Price Prev Year = CALCULATE ( [Avg Price], DATEADD ( dim_date[month_start], -1, YEAR ) )
   YoY % = DIVIDE ( [Avg Price] - [Avg Price Prev Year], [Avg Price Prev Year] )
   Markets Reporting = DISTINCTCOUNT ( fact_food_price[market_key] )
   ```
5. Suggested pages: **Overview** (KPIs, price over time), **Markets** (map using `latitude`/`longitude`), **Products** (matrix product × year), **Lineage** (table from `v_lineage`).
6. Save as a **`.pbip`** project (Options → Preview features → Power BI Project) so it is text and diff-able. Do not average prices across products with different units.

## 7. Tests and lint

```bash
source .venv/bin/activate
pytest -q
ruff check .
```

## 8. Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| `FileNotFoundError: Download the dataset to ...` | File not in `data/raw/`, or `source.file` in `schema_map.yaml` is wrong |
| `KeyError: 'price'` (or another column) in transform | A name under `columns:` doesn't match the file. Re-run the profiling snippet in Step 4 |
| Everything rejected with `bad_date;` | Wrong `date.format` or `date.mode`. Check how dates appear in the profile output |
| `connection refused` on port 5433 | `docker compose up -d` not run, or port in use. Check `docker compose ps` |
| `relation "meta.etl_run" does not exist` | Init scripts didn't run. `docker compose down -v && docker compose up -d` |
| `Can't load plugin: sqlalchemy.dialects:postgresql.psycopg` | DSN uses the psycopg v3 driver. Use `postgresql+psycopg2://` |
| `ModuleNotFoundError: airflow` after `uv run` | `uv run` removed it. Use the activated venv, or `uv sync --inexact` |
| `ON CONFLICT DO UPDATE command cannot affect row a second time` | Duplicate rows at the grain. Add the missing dimension (see Step 4) |
| `rowcount_vs_source` fails | Rows were lost or duplicated between extract and load. Inspect `meta.reject_row` and `meta.dq_result.detail` |
| DAG missing in the UI | `airflow dags list-import-errors`; confirm `AIRFLOW__CORE__DAGS_FOLDER` and that `ken_rtfp` is installed in the venv (`pip show ken-rtfp`) |

## License

This project is open-source and licensed under the terms of the [MIT LICENSE](MIT).
