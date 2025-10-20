from typing import Tuple, Type
from pydantic import BaseModel, ValidationError
import pandas as pd


class OTPDaily(BaseModel):
    date: object
    airport: str
    dep_count: int
    arr_count: int
    movements: int


def check_schema(df: pd.DataFrame, model: Type[BaseModel]) -> Tuple[bool, str]:
    if df is None or df.empty:
        return False, "Empty dataframe"
    try:
        model(**df.iloc[0].to_dict())
        return True, "OK"
    except ValidationError as exc:
        return False, f"Schema error: {exc.errors()[0]['loc']}"


def coverage_pct(df: pd.DataFrame, date_col: str = "date") -> float:
    if df.empty:
        return 0.0
    dates = pd.to_datetime(df[date_col])
    span = (dates.max() - dates.min()).days + 1
    return round(100 * dates.nunique() / max(span, 1), 2)


def duplicates(df: pd.DataFrame, keys: list[str]) -> int:
    return int(df.duplicated(subset=keys).sum())


def nonnegatives(df: pd.DataFrame, cols: list[str]) -> bool:
    return all((col in df.columns and (df[col] >= 0).all()) for col in cols)
