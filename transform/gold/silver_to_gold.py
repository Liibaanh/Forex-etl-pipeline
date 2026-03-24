"""
Gold layer: Silver → Gold Fair Value Gap (FVG) analysis.

A Fair Value Gap (FVG) is a core ICT concept. It occurs when price moves
so aggressively that it leaves an inefficiency (gap) between candles.

Detection logic (requires 3 consecutive candles):
  Bullish FVG: candle[i-1].high < candle[i+1].low
    → Price gapped UP — gap zone between high[i-1] and low[i+1]
    → Price often returns to fill this gap (buy opportunity)

  Bearish FVG: candle[i-1].low > candle[i+1].high
    → Price gapped DOWN — gap zone between low[i-1] and high[i+1]
    → Price often returns to fill this gap (sell opportunity)

FVG size is measured in pips (0.0001 = 1 pip for EUR/USD).
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from utils.spark_session import get_spark

logger = logging.getLogger(__name__)

SILVER_BASE = Path("data/delta/silver/forex")
GOLD_BASE   = Path("data/delta/gold/forex")


def run(from_symbol: str, to_symbol: str) -> None:
    """
    Detect Fair Value Gaps from Silver OHLC data.

    Args:
        from_symbol: Base currency (e.g. EUR)
        to_symbol:   Quote currency (e.g. USD)
    """
    spark = get_spark(app_name=f"gold_fvg_{from_symbol}{to_symbol}")

    pair        = f"{from_symbol.upper()}{to_symbol.upper()}"
    silver_path = str(SILVER_BASE / f"pair={pair}")
    gold_path   = str(GOLD_BASE   / f"pair={pair}")

    logger.info("Reading Silver data from %s", silver_path)

    silver_df = spark.read.format("delta").load(silver_path)

    logger.info("Silver rows: %d", silver_df.count())

    fvg_df = _detect_fvg(silver_df)
    fvg_df = _mark_filled_gaps(fvg_df, silver_df)

    # Attach pipeline metadata
    fvg_df = fvg_df.withColumn(
        "_updated_at",
        F.lit(datetime.now(timezone.utc).isoformat()).cast("timestamp"),
    )

    # Write to Gold Delta table
    (
        fvg_df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(gold_path)
    )

    logger.info("Gold write complete — %s", gold_path)

    # Show summary
    _print_summary(fvg_df)


# ── FVG Detection ─────────────────────────────────────────────────────────────

def _detect_fvg(df: DataFrame) -> DataFrame:
    """
    Detect bullish and bearish Fair Value Gaps.

    Uses window functions to look at 3 consecutive candles:
      prev_high = candle[i-1].high  (one day before)
      prev_low  = candle[i-1].low
      next_high = candle[i+1].high  (one day after)
      next_low  = candle[i+1].low

    A FVG exists on candle[i] if there is a gap between
    candle[i-1] and candle[i+1].
    """
    window = Window.partitionBy(
        "from_symbol", "to_symbol"
    ).orderBy("date")

    # Get previous and next candle values using LAG and LEAD
    df = (
        df
        .withColumn("prev_high", F.lag("high", 1).over(window))
        .withColumn("prev_low",  F.lag("low",  1).over(window))
        .withColumn("next_high", F.lead("high", 1).over(window))
        .withColumn("next_low",  F.lead("low",  1).over(window))
    )

    # Detect bullish FVG: gap between prev_high and next_low
    bullish = (
        df.filter(
            F.col("prev_high").isNotNull() &
            F.col("next_low").isNotNull() &
            (F.col("prev_high") < F.col("next_low"))  # Gap exists!
        )
        .withColumn("fvg_type", F.lit("bullish"))
        .withColumn("fvg_low",  F.col("prev_high"))  # Bottom of gap
        .withColumn("fvg_high", F.col("next_low"))   # Top of gap
    )

    # Detect bearish FVG: gap between next_high and prev_low
    bearish = (
        df.filter(
            F.col("prev_low").isNotNull() &
            F.col("next_high").isNotNull() &
            (F.col("prev_low") > F.col("next_high"))  # Gap exists!
        )
        .withColumn("fvg_type", F.lit("bearish"))
        .withColumn("fvg_high", F.col("prev_low"))   # Top of gap
        .withColumn("fvg_low",  F.col("next_high"))  # Bottom of gap
    )

    # Combine bullish and bearish FVGs
    fvg_df = bullish.union(bearish)

    # Calculate gap size in pips (1 pip = 0.0001 for EUR/USD)
    fvg_df = fvg_df.withColumn(
        "fvg_size_pips",
        ((F.col("fvg_high") - F.col("fvg_low")) * 10000)
        .cast("decimal(10,2)")
    )

    return fvg_df.select(
        "date",
        "from_symbol",
        "to_symbol",
        "fvg_type",
        "fvg_high",
        "fvg_low",
        "fvg_size_pips",
        "open", "high", "low", "close",  # Keep OHLC for context
    )


def _mark_filled_gaps(fvg_df: DataFrame, silver_df: DataFrame) -> DataFrame:
    """
    Check if each FVG has been filled (price returned to the gap zone).

    A bullish FVG is filled when a future candle's low <= fvg_low.
    A bearish FVG is filled when a future candle's high >= fvg_high.

    is_filled = 1 means the gap has been mitigated.
    is_filled = 0 means the gap is still open — potential trade zone.
    """
    # Get max date in dataset to check if gap could have been filled
    max_date = silver_df.agg(F.max("date")).collect()[0][0]

    # For simplicity: mark as filled if the candle's own close
    # returned into the gap zone (conservative fill definition)
    fvg_df = fvg_df.withColumn(
        "is_filled",
        F.when(
            # Bullish gap filled: close drops back into gap
            (F.col("fvg_type") == "bullish") &
            (F.col("close") <= F.col("fvg_high")),
            F.lit(1)
        ).when(
            # Bearish gap filled: close rises back into gap
            (F.col("fvg_type") == "bearish") &
            (F.col("close") >= F.col("fvg_low")),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    return fvg_df


def _print_summary(fvg_df: DataFrame) -> None:
    """Print a readable summary of detected FVGs."""
    total    = fvg_df.count()
    bullish  = fvg_df.filter(F.col("fvg_type") == "bullish").count()
    bearish  = fvg_df.filter(F.col("fvg_type") == "bearish").count()
    filled   = fvg_df.filter(F.col("is_filled") == 1).count()
    unfilled = fvg_df.filter(F.col("is_filled") == 0).count()

    logger.info("=" * 50)
    logger.info("FVG SUMMARY — EUR/USD")
    logger.info("=" * 50)
    logger.info("Total FVGs detected : %d", total)
    logger.info("Bullish FVGs        : %d", bullish)
    logger.info("Bearish FVGs        : %d", bearish)
    logger.info("Filled gaps         : %d", filled)
    logger.info("Unfilled gaps       : %d ← potential trade zones", unfilled)
    logger.info("=" * 50)

    logger.info("Largest FVGs by size (pips):")
    fvg_df.select(
        "date", "fvg_type", "fvg_high", "fvg_low", "fvg_size_pips", "is_filled"
    ).orderBy(F.col("fvg_size_pips").desc()).show(10, truncate=False)

    logger.info("Unfilled FVGs — active trade zones:")
    fvg_df.filter(F.col("is_filled") == 0).select(
        "date", "fvg_type", "fvg_high", "fvg_low", "fvg_size_pips"
    ).orderBy("date", ascending=False).show(10, truncate=False)


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    parser = argparse.ArgumentParser(description="Gold FVG detection job")
    parser.add_argument("--from-symbol", default="EUR", help="Base currency")
    parser.add_argument("--to-symbol",   default="USD", help="Quote currency")
    args = parser.parse_args()

    run(from_symbol=args.from_symbol, to_symbol=args.to_symbol)