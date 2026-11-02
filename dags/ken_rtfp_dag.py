from datetime import datetime
import pandas as pd
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from sqlalchemy import create_engine, text

from ken_rtfp.config import Settings, ROOT, load_yaml
from ken_rtfp import extract as ex, transform as tr, load as ld, dq, lineage

def engine():
    return create_engine(Settings().pg_dsn)

@dag(dag_id="ken_rtfp_monthly", start_date=datetime(2024, 1, 1),
     schedule="@monthly", catchup=False, max_active_runs=1,
     default_args={"retries": 1}, tags=["worldbank", "food-prices"])
def ken_rtfp():

    @task
    def register_run(**ctx) -> str:
        src = ROOT / load_yaml("schema_map.yaml")["source"]["file"]
        return lineage.start_run(engine(), src, ctx["dag"].dag_id, ctx["run_id"], ctx["logical_date"].date())

    @task
    def extract(run_id: str) -> dict:
        path = ex.extract()
        return {"run_id": run_id, "staging": str(path), "rows": len(pd.read_parquet(path))}

    @task
    def transform(x: dict) -> dict:
        clean, rej = tr.transform(x["staging"], x["run_id"])
        return {**x, "clean": clean, "rejects": rej,
                "n_clean": len(pd.read_parquet(clean)), "n_rej": len(pd.read_parquet(rej))}

    @task
    def load_dims(x: dict) -> dict:
        ld.load_dims(engine(), x["clean"]); return x

    @task
    def load_fact(x: dict) -> dict:
        n = ld.load_fact(engine(), x["clean"], x["rejects"], x["run_id"]); return {**x, "loaded": n}

    @task
    def dq_checks(x: dict) -> dict:
        ok = dq.run_checks(engine(), x["run_id"], x["clean"], x["rows"], x["n_rej"])
        if not ok:
            lineage.finish_run(engine(), x["run_id"], "blocked_by_dq",
                rows_extracted=x["rows"], rows_clean=x["n_clean"], rows_rejected=x["n_rej"], rows_loaded=x["loaded"])
            raise AirflowFailException("Error-severity DQ checks failed; see meta.dq_result")
        return x

    @task
    def publish(x: dict):
        with engine().begin() as c:
            c.execute(text("UPDATE meta.published_run SET run_id=:r WHERE id=1"), dict(r=x["run_id"]))
        lineage.finish_run(engine(), x["run_id"], "succeeded",
            rows_extracted=x["rows"], rows_clean=x["n_clean"], rows_rejected=x["n_rej"], rows_loaded=x["loaded"])

    r = register_run()
    publish(dq_checks(load_fact(load_dims(transform(extract(r))))))

ken_rtfp()