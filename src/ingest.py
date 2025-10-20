import io
import os
import zipfile
from datetime import date, datetime, timedelta
from typing import Iterable

import pandas as pd
import requests
import streamlit as st
from requests import Session, TooManyRedirects
from tenacity import retry, stop_after_attempt, wait_exponential

from src.display import kpi_card, line_fig, status_badge
from src.iohelpers import write_manifest, write_parquet
from src.validate import (
    OTPDaily,
    check_schema,
    coverage_pct,
    duplicates,
    nonnegatives,
)

BTS_URL = "https://transtats.bts.gov/PREZIP/On_Time_Reporting_{Y}_{M}.zip"
TSA_URL = "https://www.tsa.gov/sites/default/files/tsa_checkpoint_travel_numbers.csv"
METAR_URL = "https://aviationweather.gov/api/data/metar"

IATA2ICAO = {
    "ATL": "KATL",
    "DFW": "KDFW",
    "DEN": "KDEN",
    "ORD": "KORD",
    "LAX": "KLAX",
    "JFK": "KJFK",
    "SFO": "KSFO",
    "SEA": "KSEA",
}


DEFAULT_HEADERS = {
    "User-Agent": os.getenv(
        "HTTP_USER_AGENT",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ),
    "Accept": "application/json,text/csv,*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

_SESSION = Session()
_SESSION.headers.update(DEFAULT_HEADERS)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=4))
def _http_get(url, **kwargs):
    headers = {**_SESSION.headers}
    if "headers" in kwargs and kwargs["headers"]:
        headers.update(kwargs.pop("headers"))
    try:
        response = _SESSION.get(
            url,
            timeout=60,
            allow_redirects=True,
            headers=headers,
            **kwargs,
        )
        response.raise_for_status()
        return response
    except TooManyRedirects as exc:
        raise RuntimeError(
            "Too many redirects encountered when requesting "
            f"{url}. Please verify the URL accessibility."
        ) from exc


def _month_urls(start: date, end: date) -> Iterable[str]:
    year, month = start.year, start.month
    while (year < end.year) or (year == end.year and month <= end.month):
        yield BTS_URL.format(Y=year, M=f"{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1


def pull_bts_otp(start: date, end: date, airports_iata: list[str]) -> pd.DataFrame:
    if not airports_iata:
        return pd.DataFrame(columns=["date", "airport", "dep_count", "arr_count", "movements"])

    usecols = ["FL_DATE", "ORIGIN", "DEST", "CANCELLED", "DIVERTED"]
    frames = []
    for url in _month_urls(start, end):
        content = _http_get(url).content
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            csv_name = next(
                (name for name in archive.namelist() if name.lower().endswith(".csv")),
                None,
            )
            if not csv_name:
                continue
            frames.append(
                pd.read_csv(
                    archive.open(csv_name),
                    usecols=usecols,
                    dtype={"ORIGIN": "string", "DEST": "string"},
                )
            )

    if not frames:
        raise RuntimeError("No BTS OTP data retrieved for the requested window.")

    raw = pd.concat(frames, ignore_index=True)
    raw = raw.rename(
        columns={
            "FL_DATE": "date",
            "ORIGIN": "dep_iata",
            "DEST": "arr_iata",
            "CANCELLED": "cancelled",
            "DIVERTED": "diverted",
        }
    )
    raw["date"] = pd.to_datetime(raw["date"]).dt.date

    selected = raw[
        raw["dep_iata"].isin(airports_iata) | raw["arr_iata"].isin(airports_iata)
    ]

    outputs = []
    for airport in airports_iata:
        departures = (
            selected[selected["dep_iata"].eq(airport)]
            .groupby("date")
            .size()
            .rename("dep_count")
        )
        arrivals = (
            selected[selected["arr_iata"].eq(airport)]
            .groupby("date")
            .size()
            .rename("arr_count")
        )
        daily = (
            pd.concat([departures, arrivals], axis=1)
            .fillna(0)
            .astype(int)
            .reset_index()
        )
        daily["airport"] = IATA2ICAO.get(airport, airport)
        daily["movements"] = daily["dep_count"] + daily["arr_count"]
        outputs.append(daily)

    return pd.concat(outputs, ignore_index=True).sort_values(["airport", "date"])


def pull_tsa() -> pd.DataFrame:
    response = _http_get(TSA_URL, headers={"Accept": "text/csv"})
    df = pd.read_csv(io.StringIO(response.text))
    df = df.rename(columns=lambda col: col.strip())
    year_cols = sorted([col for col in df.columns if col.isdigit()], reverse=True)
    if not year_cols:
        raise ValueError("TSA CSV missing yearly columns")
    latest_col = year_cols[0]
    out = df[["Date", latest_col]].rename(columns={"Date": "date", latest_col: "tsa_travelers"})
    out["date"] = pd.to_datetime(out["date"]).dt.date
    return out.dropna()


def pull_metar_recent(icao: str, days: int, user_agent: str) -> pd.DataFrame:
    start = datetime.utcnow() - timedelta(days=days)
    params = {
        "ids": icao,
        "format": "json",
        "start": start.strftime("%Y-%m-%dT%H:%MZ"),
    }
    response = _http_get(
        METAR_URL,
        params=params,
        headers={"User-Agent": user_agent},
    )
    data = response.json() if response.text.strip() else []
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df.get("obsTime"), errors="coerce")
    df["date"] = df["timestamp"].dt.date

    def to_numeric(column):
        return pd.to_numeric(df.get(column), errors="coerce")

    df["windSpdKt"] = to_numeric("windSpdKt")
    df["windGustKt"] = to_numeric("windGustKt")
    df["visSm"] = to_numeric("visSm")
    df["ceilFt"] = to_numeric("ceilFt")
    weather = df.get("wx", pd.Series([""] * len(df))).fillna("")
    df["precip_flag"] = weather.str.contains("RA|SN|DZ|PL", regex=True, na=False)
    df["ts_flag"] = weather.str.contains("TS", na=False)

    aggregated = (
        df.groupby("date")
        .agg(
            wind_mean=("windSpdKt", "mean"),
            gust_max=("windGustKt", "max"),
            vis_min=("visSm", "min"),
            ceiling_min=("ceilFt", "min"),
            precip_any=("precip_flag", "max"),
            ts_any=("ts_flag", "max"),
        )
        .reset_index()
    )
    aggregated["ifr_any"] = (
        (aggregated["ceiling_min"] <= 1000) | (aggregated["vis_min"] <= 3)
    ).astype(int)
    aggregated["airport"] = icao
    return aggregated


def render_ingest():
    st.caption(
        "Public sources: BTS TranStats (bulk CSV), NOAA AWC (recent METAR), TSA daily throughput."
    )
    airports = st.multiselect(
        "Airports (IATA)",
        list(IATA2ICAO.keys()),
        default=["ATL", "DFW", "DEN"],
    )
    days_back = st.slider("Days back (OTP window)", 90, 1095, 730, step=30)
    metar_days = st.slider("METAR recent days (≤15)", 1, 15, 15)
    user_agent = (os.getenv("NOAA_USER_AGENT", "") or "").strip()
    st.write(
        status_badge(
            "NOAA User-Agent", "ok" if ("@" in user_agent and "." in user_agent) else "missing"
        )
    )

    if st.button("Run Ingest", type="primary", use_container_width=True):
        if not airports:
            st.error("Select at least one airport before running ingest.")
            return

        with st.status("Ingest running…", expanded=True) as status:
            try:
                status.update(label="Step 1/4 • Downloading & aggregating BTS OTP …")
                end = date.today()
                start = end - timedelta(days=days_back)
                otp_daily = pull_bts_otp(start, end, airports)

                ok, message = check_schema(otp_daily, OTPDaily)
                st.write("OTP schema:", message)
                coverage = coverage_pct(otp_daily)
                dupes = duplicates(otp_daily, ["date", "airport"])
                sane = nonnegatives(otp_daily, ["dep_count", "arr_count", "movements"])
                st.write({"coverage_%": coverage, "duplicates": dupes, "nonnegatives": sane})

                status.update(label="Step 2/4 • Fetching METAR (recent) …")
                wx_frames = []
                for airport in airports:
                    icao = IATA2ICAO.get(airport, airport)
                    wx_frames.append(
                        pull_metar_recent(icao, metar_days, user_agent or "you@example.com")
                    )
                wx_daily = pd.concat(wx_frames, ignore_index=True) if wx_frames else pd.DataFrame()

                status.update(label="Step 3/4 • Fetching TSA CSV …")
                tsa_daily = pull_tsa()

                status.update(label="Step 4/4 • Writing Parquet & manifest …")
                write_parquet(otp_daily, "data/processed/otp_daily.parquet")
                if not wx_daily.empty:
                    write_parquet(wx_daily, "data/processed/wx_daily.parquet")
                write_parquet(tsa_daily, "data/processed/tsa_daily.parquet")
                write_manifest(
                    {
                        "airports": airports,
                        "start": start,
                        "end": end,
                        "rows": {
                            "otp_daily": len(otp_daily),
                            "wx_daily": len(wx_daily),
                            "tsa": len(tsa_daily),
                        },
                        "coverage_pct": coverage,
                        "dupes": dupes,
                        "nonnegatives": sane,
                    }
                )

                status.update(label="Done", state="complete")
                st.success("Ingest complete ✔")

                col1, col2, col3 = st.columns(3)
                col1.plotly_chart(
                    kpi_card("OTP daily rows", len(otp_daily)), use_container_width=True
                )
                col2.plotly_chart(
                    kpi_card("Coverage %", coverage, "%"), use_container_width=True
                )
                col3.plotly_chart(
                    kpi_card("Airports", len(airports)), use_container_width=True
                )
                st.plotly_chart(
                    line_fig(
                        otp_daily,
                        "date",
                        "movements",
                        color="airport",
                        title="Daily Movements — quick look",
                    ),
                    use_container_width=True,
                )

            except Exception as exc:
                st.error(f"Ingest failed: {exc}")
