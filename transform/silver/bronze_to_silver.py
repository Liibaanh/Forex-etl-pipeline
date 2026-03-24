"""
Silver layer: Bronze → Silver Delta table.

Applies:
  - Type casting (string → decimal)
  - Null handling and validation
  - Derived fields: daily_range, daily_return
  - Deduplication on (date, from_symbol, to_symbol)

daily_range  = high - low          (measures volatility per day)
daily_return = (close - prev_close) / prev_close * 100  (% change)
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from utils.spark_session import get_spark

logger = logging.getLogger(__name__)

BRONZE_BASE = Path("data/delta/bronze/forex")
SILVER_BASE = Path("data/delta/silver/forex")


def run(from_symbol: str, to_symbol: str) -> None:
    """
    Transform Bronze OHLC data into Silver for a given currency pair.

    Args:
        from_symbol: Base currency (e.g. EUR)
        to_symbol:   Quote currency (e.g. USD)
    """
    spark = get_spark(app_name=f"silver_{from_symbol}{to_symbol}")

    pair        = f"{from_symbol.upper()}{to_symbol.upper()}"
    bronze_path = str(BRONZE_BASE / f"pair={pair}")
    silver_path = str(SILVER_BASE / f"pair={pair}")

    logger.info("Reading Bronze data from %s", bronze_path)

    bronze_df = spark.read.format("delta").load(bronze_path)

    logger.info("Bronze rows: %d", bronze_df.count())

    # Run transformation steps in order
    typed_df   = _cast_types(bronze_df)
    cleaned_df = _clean(typed_df)
    enriched_df = _add_derived_fields(cleaned_df)
    silver_df  = _deduplicate(enriched_df)

    # Write to Silver Delta table
    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(silver_path)
    )

    logger.info("Silver write complete — %s", silver_path)
    logger.info("Sample data:")

    silver_df.select(
        "date",
        "open", "high", "low", "close",
        "daily_range",
        "daily_return",
    ).orderBy("date", ascending=False).show(10, truncate=False)


# ── Transformation steps ──────────────────────────────────────────────────────

def _cast_types(df: DataFrame) -> DataFrame:
    """
    Cast raw string OHLC values to DecimalType.
    Bronze stores everything as strings to avoid parse errors at ingest.
    """
    return (
        df.withColumn("open",  F.col("open") .cast("decimal(10,5)"))
          .withColumn("high",  F.col("high") .cast("decimal(10,5)"))
          .withColumn("low",   F.col("low")  .cast("decimal(10,5)"))
          .withColumn("close", F.col("close").cast("decimal(10,5)"))
          .withColumn("date",  F.col("date") .cast("date"))
    )


def _clean(df: DataFrame) -> DataFrame:
    """
    Drop rows with null OHLC values.
    Validate that high >= low — basic sanity check on market data.
    """
    df = df.dropna(subset=["date", "open", "high", "low", "close"])

    # Remove rows where high < low — this is impossible in real market data
    df = df.filter(F.col("high") >= F.col("low"))

    # Remove rows where any price is zero or negative
    df = df.filter(
        (F.col("open")  > 0) &
        (F.col("high")  > 0) &
        (F.col("low")   > 0) &
        (F.col("close") > 0)
    )

    return df


def _add_derived_fields(df: DataFrame) -> DataFrame:
    """
    Add ICT-relevant derived fields:

    daily_range:
        High minus Low for the day.
        Measures how much price moved — useful for identifying
        high-volatility sessions and potential FVG zones.

    daily_return:
        Percentage change from previous day's close to today's close.
        Shows momentum direction — positive = bullish, negative = bearish.
    """
    # Window ordered by date for calculating previous close
    window = Window.partitionBy("from_symbol", "to_symbol").orderBy("date")

    return (
        df
        # daily_range: distance between high and low in pips
        .withColumn(
            "daily_range",
            (F.col("high") - F.col("low")).cast("decimal(10,5)")
        )
        # daily_return: % change from previous close
        .withColumn(
            "prev_close",
            F.lag("close", 1).over(window)
        )
        .withColumn(
            "daily_return",
            F.when(
                F.col("prev_close").isNotNull(),
                ((F.col("close") - F.col("prev_close"))
                 / F.col("prev_close") * 100).cast("decimal(10,5)")
            ).otherwise(F.lit(None).cast("decimal(10,5)"))
        )
        .drop("prev_close")  # Drop helper column — not needed in Silver
    )


def _deduplicate(df: DataFrame) -> DataFrame:
    """
    Keep only one row per (date, from_symbol, to_symbol).
    Should not be needed with clean API data, but defensive programming
    protects against duplicate API responses.
    """
    window = Window.partitionBy(
        "date", "from_symbol", "to_symbol"
    ).orderBy(F.col("_ingest_timestamp").desc())

    return (
        df.withColumn("_row_num", F.row_number().over(window))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    parser = argparse.ArgumentParser(description="Silver transformation job")
    parser.add_argument("--from-symbol", default="EUR", help="Base currency")
    parser.add_argument("--to-symbol",   default="USD", help="Quote currency")
    args = parser.parse_args()

    run(from_symbol=args.from_symbol, to_symbol=args.to_symbol)