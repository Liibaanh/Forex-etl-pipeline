# Forex ETL Pipeline

A production-grade data pipeline that ingests real **EUR/USD Forex data**
from Alpha Vantage, processes it through a **Medallion architecture**
(Bronze → Silver → Gold), and automatically detects **Fair Value Gaps (FVG)**
— a core concept in ICT trading strategy.

![CI](https://github.com/liibaanh/forex-etl-pipeline/actions/workflows/ci.yml/badge.svg)

---

## What is a Fair Value Gap?

A Fair Value Gap (FVG) is a price inefficiency left when markets move
aggressively. It occurs across three consecutive candles:
```
Bullish FVG:                    Bearish FVG:
                                
  next_low  ────                  prev_low  ────
     GAP    ████  ← buy zone         GAP   ████  ← sell zone
  prev_high ────               next_high   ────
```

**Bullish FVG**: `prev_high < next_low` — price gapped up, leaving a zone
price is likely to return to (buy opportunity).

**Bearish FVG**: `prev_low > next_high` — price gapped down, leaving a zone
price is likely to return to (sell opportunity).

This pipeline automatically detects, sizes, and tracks all FVGs on EUR/USD
daily data — analysis that traders typically do manually on charts.

---

## Architecture
```
Alpha Vantage API
      │
      ▼
Raw JSON (local / S3)
      │
      ▼
Bronze Delta Table    ← faithful copy of source, no transforms
      │
      ▼
Silver Delta Table    ← typed, cleaned, daily_range, daily_return
      │
      ▼
Gold Delta Table      ← FVG detection, gap sizing, fill tracking
      │
      ▼
SQL Analytics         ← fill rate, volatility, monthly activity
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Source | Alpha Vantage API (real EUR/USD OHLC) |
| Compute | Databricks (AWS) |
| Storage | AWS S3 + Delta Lake |
| Transformation | PySpark |
| Orchestration | Databricks Workflows |
| Data Quality | Great Expectations |
| CI/CD | GitHub Actions |
| Containerization | Docker |

---

## Project Structure
```
forex-etl-pipeline/
├── ingestion/
│   └── alpha_vantage.py     # Fetches real EUR/USD OHLC from Alpha Vantage
├── transform/
│   ├── bronze/              # Raw JSON → Bronze Delta (no transforms)
│   ├── silver/              # Bronze → Silver (typed, daily_range, daily_return)
│   └── gold/                # Silver → Gold (FVG detection + fill tracking)
├── utils/
│   ├── spark_session.py     # SparkSession factory (local + Databricks)
│   └── schema_registry.py   # Central schema definitions for all layers
├── sql/queries/
│   └── fvg_analysis.sql     # FVG fill rate, volatility, monthly activity
├── tests/unit/              # Pytest unit tests (9/9 passing)
├── .github/workflows/       # CI pipeline
└── docker/                  # Dockerfile + docker-compose
```

---

## Pipeline Steps

### 1. Ingest — Alpha Vantage API
Fetches daily OHLC (Open, High, Low, Close) for EUR/USD.
Saves raw JSON locally, mimicking an S3 landing zone.
```bash
python -m ingestion.alpha_vantage --from-symbol EUR --to-symbol USD --outputsize full
```

### 2. Bronze — Raw Ingestion
Reads raw JSON and writes to Delta Lake with no transformations.
Idempotent — safe to re-run without creating duplicates.
```bash
python -m transform.bronze.raw_to_bronze --from-symbol EUR --to-symbol USD
```

### 3. Silver — Clean and Enrich
Casts types, validates data, and adds derived fields:
- `daily_range` = high - low (measures daily volatility)
- `daily_return` = % change close-to-close (measures momentum)
```bash
python -m transform.silver.bronze_to_silver --from-symbol EUR --to-symbol USD
```

### 4. Gold — Fair Value Gap Detection
Automatically detects bullish and bearish FVGs using window functions.
Sizes each gap in pips and tracks whether it has been filled.
```bash
python -m transform.gold.silver_to_gold --from-symbol EUR --to-symbol USD
```

---

## Sample Output
```
FVG SUMMARY — EUR/USD
==================================================
Total FVGs detected :  47
Bullish FVGs        :  24
Bearish FVGs        :  23
Filled gaps         :  31
Unfilled gaps       :  16 ← potential trade zones
==================================================

Largest FVGs by size (pips):
+----------+---------+--------+--------+--------------+---------+
|date      |fvg_type |fvg_high|fvg_low |fvg_size_pips |is_filled|
+----------+---------+--------+--------+--------------+---------+
|2026-03-02|bearish  |1.17060 |1.17850 |79.00         |0        |
|2026-02-09|bullish  |1.18860 |1.18260 |60.00         |0        |
+----------+---------+--------+--------+--------------+---------+
```

---

## Key Design Decisions

**Why Delta Lake for Forex data?**
Delta Lake provides ACID transactions and time-travel. If a bad API response
corrupts a day's data, we can roll back to a previous version instantly
without re-fetching from the API.

**Why window functions for FVG detection?**
FVG requires comparing three consecutive candles. PySpark window functions
(`LAG` and `LEAD`) handle this elegantly at scale — the same logic works
on 100 rows locally or 100 million rows in Databricks.

**Why track `is_filled`?**
An unfilled FVG is an active trade zone in ICT theory. By tracking fill
status, analysts can filter for currently active setups without manually
scanning charts.

**Why `daily_range` and `daily_return` in Silver?**
Large `daily_range` values often precede FVG formation — high volatility
days leave inefficiencies. Storing these in Silver enables correlation
analysis between volatility and FVG size in Gold.

---

## Getting Started

### Prerequisites
- Python 3.11+
- Java 21 (required by PySpark)
- Alpha Vantage API key (free at [alphavantage.co](https://alphavantage.co))

### Run locally
```bash
git clone https://github.com/liibaanh/forex-etl-pipeline
cd forex-etl-pipeline

# Install dependencies
pip install -e ".[dev]"

# Add your Alpha Vantage API key
cp .env.example .env
# Edit .env and add ALPHA_VANTAGE_API_KEY

# Run full pipeline
python -m ingestion.alpha_vantage --from-symbol EUR --to-symbol USD
python -m transform.bronze.raw_to_bronze --from-symbol EUR --to-symbol USD
python -m transform.silver.bronze_to_silver --from-symbol EUR --to-symbol USD
python -m transform.gold.silver_to_gold --from-symbol EUR --to-symbol USD
```

### Run tests
```bash
pytest tests/unit/ -v
```

---

## Notes

> This pipeline uses **real market data** from the Alpha Vantage API.
> The free tier provides 25 API calls per day and full historical data
> going back 20+ years. No simulated or synthetic data is used.