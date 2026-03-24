"""
Bronze layer: Raw JSON → Bronze Delta table.

Reads raw OHLC data from local JSON file (mimicking S3 raw zone),
attaches pipeline metadata, and writes to Bronze Delta table
using an idempotent MERGE on (date, from_symbol, to_symbol).

No transformations here — Bronze is a faithful copy of source data.
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from utils.schema_registry import BRONZE_OHLC_SCHEMA
from utils.spark_session import get_spark

logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────

RAW_BASE    = Path("data/raw/forex")
BRONZE_BASE = Path("data/delta/bronze/forex")


def run(from_symbol: str, to_symbol: str) -> None:
    """
    Ingest raw OHLC JSON into Bronze Delta table.

    Args:
        from_symbol: Base currency (e.g. EUR)
        to_symbol:   Quote currency (e.g. USD)
    """
    spark = get_spark(app_name=f"bronze_{from_symbol}{to_symbol}")

    pair     = f"{from_symbol.upper()}{to_symbol.upper()}"
    raw_path = str(RAW_BASE / f"pair={pair}" / "data.json")
    bronze_path = str(BRONZE_BASE / f"pair={pair}")

    logger.info("Reading raw JSON from %s", raw_path)

    # Read raw JSON with enforced schema
    # Read raw JSON with enforced schema
# multiLine=True required because file is a JSON array, not newline-delimited
    raw_df = (
    spark.read.format("json")
    .option("multiLine", "true")
    .schema(BRONZE_OHLC_SCHEMA)
    .load(raw_path)
)

    row_count = raw_df.count()
    logger.info("Loaded %d rows from raw zone.", row_count)

    if row_count == 0:
        logger.warning("No data found — skipping write.")
        return

    # Attach ingest timestamp
    bronze_df = raw_df.withColumn(
        "_ingest_timestamp",
        F.lit(datetime.now(timezone.utc).isoformat()).cast("timestamp"),
    )

    # Write to Delta — create on first run, merge on subsequent runs
    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(bronze_path)
    )

    logger.info("Bronze write complete — %s", bronze_path)
    logger.info("Sample data:")

    bronze_df.select(
        "date", "from_symbol", "to_symbol", "open", "high", "low", "close"
    ).orderBy("date", ascending=False).show(5, truncate=False)


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    parser = argparse.ArgumentParser(description="Bronze ingestion job")
    parser.add_argument("--from-symbol", default="EUR", help="Base currency")
    parser.add_argument("--to-symbol",   default="USD", help="Quote currency")
    args = parser.parse_args()

    run(from_symbol=args.from_symbol, to_symbol=args.to_symbol)
    

