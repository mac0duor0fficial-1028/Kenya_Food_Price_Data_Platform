from pathlib import Path
import yaml
from pydantic_settings import BaseSettings

ROOT = Path(__file__).resolve().parents[2]

class Settings(BaseSettings):
    pg_dsn: str = "postgresql+psycopg://rtfp:rtfp@localhost:5433/rtfp"
    model_config = {"env_file": ".env", "extra": "ignore"}

def load_yaml(name: str) -> dict:
    return yaml.safe_load((ROOT / "config" / name).read_text())