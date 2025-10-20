from pathlib import Path
import json
import pandas as pd

DATA_DIR = Path("data")
PROC = DATA_DIR / "processed"
RAW = DATA_DIR / "raw"
for path in (DATA_DIR, PROC, RAW):
    path.mkdir(parents=True, exist_ok=True)

def write_parquet(df: pd.DataFrame, path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)

def read_parquet(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)

def write_manifest(info: dict, path: str = "data/processed/manifest.json"):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(info, fh, indent=2, default=str)
