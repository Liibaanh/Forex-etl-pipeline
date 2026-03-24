"""
Alpha Vantage API client for Forex OHLC data.

Fetches daily EUR/USD OHLC (Open, High, Low, Close) data
and saves it as JSON to the local raw zone, mimicking S3 structure.

Alpha Vantage free tier:
  - 25 API calls per day
  - Daily OHLC data (not intraday on free tier)
  - Data goes back 20+ years

Usage:
    python -m ingestion.alpha_vantage --from-symbol EUR --to-symbol USD --outputsize full
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

BASE_URL    = "https://www.alphavantage.co/query"
OUTPUT_BASE = Path("data/raw/forex")


def fetch_daily_ohlc(
    from_symbol: str,
    to_symbol: str,
    outputsize: str = "compact",
    api_key: str | None = None,
) -> dict:
    """
    Fetch daily OHLC data from Alpha Vantage.

    Args:
        from_symbol: Base currency (e.g. EUR)
        to_symbol:   Quote currency (e.g. USD)
        outputsize:  'compact' = last 100 days, 'full' = 20+ years
        api_key:     Alpha Vantage API key (defaults to env var)

    Returns:
        Raw API response as dict.

    Raises:
        ValueError: If API key is missing or API returns an error.
        requests.HTTPError: If the HTTP request fails.
    """
    key = api_key or os.getenv("ALPHA_VANTAGE_API_KEY")
    if not key:
        raise ValueError(
            "Alpha Vantage API key not found. "
            "Set ALPHA_VANTAGE_API_KEY in your .env file."
        )

    params = {
        "function":    "FX_DAILY",
        "from_symbol": from_symbol.upper(),
        "to_symbol":   to_symbol.upper(),
        "outputsize":  outputsize,
        "apikey":      key,
    }

    logger.info(
        "Fetching %s/%s OHLC data (outputsize=%s)...",
        from_symbol, to_symbol, outputsize,
    )

    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    # Alpha Vantage returns error messages inside the JSON body — not HTTP errors
    if "Error Message" in data:
        raise ValueError(f"Alpha Vantage API error: {data['Error Message']}")

    if "Note" in data:
        # Rate limit warning — free tier allows 25 calls/day
        logger.warning("Alpha Vantage rate limit warning: %s", data["Note"])

    if "Time Series FX (Daily)" not in data:
        raise ValueError(f"Unexpected API response format: {data}")

    logger.info(
        "Fetched %d days of %s/%s data.",
        len(data["Time Series FX (Daily)"]),
        from_symbol,
        to_symbol,
    )

    return data


def parse_ohlc(raw_data: dict) -> list[dict]:
    """
    Parse raw Alpha Vantage response into a clean list of OHLC records.

    Alpha Vantage format:
        "2024-01-15": {
            "1. open": "1.09230",
            "2. high": "1.09510",
            "3. low":  "1.08910",
            "4. close":"1.09340"
        }

    Returns a flat list of dicts with consistent field names.
    """
    meta        = raw_data["Meta Data"]
    time_series = raw_data["Time Series FX (Daily)"]

    from_symbol = meta["2. From Symbol"]
    to_symbol   = meta["3. To Symbol"]
    ingest_ts   = datetime.now(timezone.utc).isoformat()

    records = []
    for date_str, values in time_series.items():
        records.append(
            {
                "date":              date_str,
                "open":              values["1. open"],
                "high":              values["2. high"],
                "low":               values["3. low"],
                "close":             values["4. close"],
                "from_symbol":       from_symbol,
                "to_symbol":         to_symbol,
                "_ingest_timestamp": ingest_ts,
                "_source":           "alpha_vantage",
            }
        )

    # Sort by date ascending — oldest first
    records.sort(key=lambda r: r["date"])
    return records


def save_raw(records: list[dict], from_symbol: str, to_symbol: str) -> Path:
    """
    Save parsed records to local raw zone.
    Structure mirrors S3: data/raw/forex/pair=EURUSD/data.json

    Returns path to saved file.
    """
    pair       = f"{from_symbol.upper()}{to_symbol.upper()}"
    output_dir = OUTPUT_BASE / f"pair={pair}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "data.json"
    with open(output_file, "w") as f:
        json.dump(records, f, indent=2)

    logger.info(
        "Saved %d records to %s",
        len(records),
        output_file,
    )
    return output_file


def run(
    from_symbol: str = "EUR",
    to_symbol: str = "USD",
    outputsize: str = "compact",
) -> Path:
    """
    Full ingestion flow:
    1. Fetch from Alpha Vantage
    2. Parse into clean records
    3. Save to raw zone

    Returns path to saved file.
    """
    raw_data = fetch_daily_ohlc(
        from_symbol=from_symbol,
        to_symbol=to_symbol,
        outputsize=outputsize,
    )
    records     = parse_ohlc(raw_data)
    output_file = save_raw(records, from_symbol, to_symbol)
    return output_file


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    parser = argparse.ArgumentParser(description="Fetch Forex OHLC data from Alpha Vantage")
    parser.add_argument("--from-symbol", default="EUR",     help="Base currency (default: EUR)")
    parser.add_argument("--to-symbol",   default="USD",     help="Quote currency (default: USD)")
    parser.add_argument("--outputsize",  default="compact", choices=["compact", "full"],
                        help="compact = last 100 days, full = 20+ years")
    args = parser.parse_args()

    run(
        from_symbol=args.from_symbol,
        to_symbol=args.to_symbol,
        outputsize=args.outputsize,
    )
    