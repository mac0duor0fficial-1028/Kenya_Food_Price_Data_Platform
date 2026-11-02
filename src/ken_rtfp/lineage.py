import hashlib, subprocess, uuid
from pathlib import Path
from sqlalchemy import text

def sha256_file(path: Path, chunk=1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda: f.read(chunk), b""):
            h.update(b)
    return h.hexdigest()

def git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"

def start_run(engine, src: Path, dag_id=None, airflow_run_id=None, logical_date=None) -> str:
    run_id = str(uuid.uuid4())
    lock = Path("uv.lock")
    with engine.begin() as c:
        c.execute(text("""
          INSERT INTO meta.etl_run(run_id, dag_id, airflow_run_id, logical_date,
            source_name, source_url, source_sha256, source_bytes, git_commit, uv_lock_sha256)
          VALUES (:r,:d,:a,:l,:n,:u,:s,:b,:g,:k)"""),
          dict(r=run_id, d=dag_id, a=airflow_run_id, l=logical_date, n=src.name,
               u="https://microdata.worldbank.org/catalog/6167",
               s=sha256_file(src), b=src.stat().st_size, g=git_commit(),
               k=sha256_file(lock) if lock.exists() else None))
    return run_id

def finish_run(engine, run_id: str, status: str, **counts):
    sets = "".join(f", {k}=:{k}" for k in counts)
    sql = f"UPDATE meta.etl_run SET status=:status, finished_at=now(){sets} WHERE run_id=:run_id"
    with engine.begin() as c:
        c.execute(text(sql), dict(status=status, run_id=run_id, **counts))