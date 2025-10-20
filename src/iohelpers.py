from pathlib import Path
import json
import pandas as pd


_BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = (_BASE_DIR / "data").resolve()
PROC = DATA_DIR / "processed"
RAW = DATA_DIR / "raw"

for directory in (DATA_DIR, PROC, RAW):
    directory.mkdir(parents=True, exist_ok=True)


def _resolve_path(path: str | Path) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    if candidate.parts and candidate.parts[0] == "data":
        return (_BASE_DIR / candidate).resolve()
    return (DATA_DIR / candidate).resolve()


def write_parquet(df: pd.DataFrame, path: str):
    target = _resolve_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(target, index=False)


def read_parquet(path: str) -> pd.DataFrame:
    target = _resolve_path(path)
    return pd.read_parquet(target)


def write_manifest(info: dict, path: str = "data/processed/manifest.json"):
    target = _resolve_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w", encoding="utf-8") as fh:
        json.dump(info, fh, indent=2, default=str)
