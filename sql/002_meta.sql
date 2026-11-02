CREATE TABLE IF NOT EXISTS meta.etl_run (
  run_id          UUID PRIMARY KEY,
  dag_id          TEXT,
  airflow_run_id  TEXT,
  logical_date    DATE,
  source_name     TEXT NOT NULL,
  source_url      TEXT,
  source_sha256   CHAR(64) NOT NULL,
  source_bytes    BIGINT,
  git_commit      TEXT,
  uv_lock_sha256  CHAR(64),
  started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at     TIMESTAMPTZ,
  status          TEXT NOT NULL DEFAULT 'running'
                  CHECK (status IN ('running','succeeded','failed','blocked_by_dq')),
  rows_extracted  INTEGER,
  rows_clean      INTEGER,
  rows_rejected   INTEGER,
  rows_loaded     INTEGER
);

CREATE TABLE IF NOT EXISTS meta.dq_result (
  dq_id       BIGSERIAL PRIMARY KEY,
  run_id      UUID NOT NULL REFERENCES meta.etl_run(run_id),
  check_name  TEXT NOT NULL,
  severity    TEXT NOT NULL CHECK (severity IN ('error','warn')),
  passed      BOOLEAN NOT NULL,
  failed_rows INTEGER NOT NULL DEFAULT 0,
  detail      JSONB,
  checked_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS meta.reject_row (
  run_id      UUID NOT NULL REFERENCES meta.etl_run(run_id),
  source_row  INTEGER NOT NULL,
  reason      TEXT NOT NULL,
  payload     JSONB,
  PRIMARY KEY (run_id, source_row, reason)
);

CREATE TABLE IF NOT EXISTS meta.published_run (
  id     INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  run_id UUID REFERENCES meta.etl_run(run_id)
);
INSERT INTO meta.published_run(id) VALUES (1) ON CONFLICT DO NOTHING;