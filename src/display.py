import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import pandas as pd

from src.iohelpers import read_parquet


def kpi_card(title, value, suffix=""):
    fig = go.Figure(
        go.Indicator(
            mode="number",
            value=value,
            number={"suffix": f" {suffix}"},
            title={"text": title},
        )
    )
    fig.update_layout(
        template="plotly_dark",
        height=140,
        margin=dict(l=10, r=10, t=40, b=10),
    )
    return fig


def line_fig(df, x, y, color=None, title=""):
    fig = px.line(df, x=x, y=y, color=color, title=title)
    fig.update_layout(
        template="plotly_dark",
        height=420,
        margin=dict(l=10, r=10, t=50, b=10),
    )
    return fig


def status_badge(label, state):
    emoji = "🟢" if state == "ok" else ("🟡" if state == "warn" else "🔴")
    return f"{emoji} **{label}**: `{state}`"


def render_dashboard():
    st.subheader("Dashboard")
    try:
        otp = read_parquet("data/processed/otp_daily.parquet")
    except Exception:
        st.warning("No processed data found. Run Ingest first.")
        return

    otp["date"] = pd.to_datetime(otp["date"])
    airports = sorted(otp["airport"].unique())
    selected = st.multiselect("Airports (ICAO)", airports, default=airports[:2])
    if not selected:
        st.info("Select at least one airport to view data.")
        return

    min_date = otp["date"].min().date()
    max_date = otp["date"].max().date()
    date_range = st.slider(
        "Date range",
        min_value=min_date,
        max_value=max_date,
        value=(max(min_date, max_date.replace(day=1)), max_date),
    )
    filtered = otp[
        otp["airport"].isin(selected)
        & otp["date"].between(pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1]))
    ]

    if filtered.empty:
        st.warning("No data for the selected filters.")
        return

    col1, col2, col3, col4 = st.columns(4)
    last_date = filtered["date"].max()
    last7 = filtered[filtered["date"] >= last_date - pd.Timedelta(days=7)]["movements"].mean()
    last28 = filtered[filtered["date"] >= last_date - pd.Timedelta(days=28)]["movements"].mean()
    ytd = filtered[filtered["date"].dt.year == last_date.year]["movements"].sum()
    wow = (
        filtered[filtered["date"] >= last_date - pd.Timedelta(days=7)]["movements"].sum()
        - filtered[
            (filtered["date"] < last_date - pd.Timedelta(days=7))
            & (filtered["date"] >= last_date - pd.Timedelta(days=14))
        ]["movements"].sum()
    )

    col1.metric("Avg last 7d", f"{last7:.0f}")
    col2.metric("Avg last 28d", f"{last28:.0f}")
    col3.metric("YTD movements", f"{ytd:,.0f}")
    col4.metric("Δ WoW (sum)", f"{wow:+,.0f}")

    st.plotly_chart(
        line_fig(filtered, "date", "movements", color="airport", title="Daily Movements"),
        use_container_width=True,
    )
