# US Aviation — Public Data Explorer

A Streamlit app that ingests public US aviation datasets and visualises key metrics in an interactive dashboard.

## Features
- Ingest tab downloads BTS On-Time Performance bulk CSVs, NOAA METAR recent observations, and TSA daily throughput.
- Built-in validations: schema, coverage, duplicates, and non-negative checks with clear feedback in the UI.
- Writes processed Parquet files and a manifest for downstream use.
- Dashboard tab reads processed data and displays Plotly-based KPIs and time-series charts in a dark, blue/green theme.

## Quick start
1. Create and activate a virtual environment.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy the example environment file and set a real email for the NOAA request header:
   ```bash
   cp .env.example .env
   # edit .env to set NOAA_USER_AGENT to your contact email
   ```
4. Launch the Streamlit app:
   ```bash
   streamlit run app.py
   ```
5. Use the **Ingest** tab to pull data first, then explore metrics in the **Dashboard** tab.
