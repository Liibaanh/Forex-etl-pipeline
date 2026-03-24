"""
Unit tests for Fair Value Gap detection logic.
Tests cover bullish FVG, bearish FVG, and edge cases.
Uses a local SparkSession — no API calls or Delta tables needed.
"""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from transform.gold.silver_to_gold import _detect_fvg, _mark_filled_gaps


# ── Schema ────────────────────────────────────────────────────────────────────

SILVER_SCHEMA = StructType(
    [
        StructField("date",              DateType(),        False),
        StructField("from_symbol",       StringType(),      False),
        StructField("to_symbol",         StringType(),      False),
        StructField("open",              DecimalType(10,5), True),
        StructField("high",              DecimalType(10,5), True),
        StructField("low",               DecimalType(10,5), True),
        StructField("close",             DecimalType(10,5), True),
        StructField("daily_range",       DecimalType(10,5), True),
        StructField("daily_return",      DecimalType(10,5), True),
        StructField("_ingest_timestamp", TimestampType(),   True),
    ]
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Single SparkSession shared across all tests."""
    return (
        SparkSession.builder.appName("test_fvg")
        .master("local[2]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .getOrCreate()
    )


@pytest.fixture
def bullish_fvg_data(spark: SparkSession):
    """
    Three candles that form a clear bullish FVG.

    Candle 1 (prev): high = 1.10000
    Candle 2 (mid):  the FVG candle — big bullish move
    Candle 3 (next): low  = 1.10500

    prev_high (1.10000) < next_low (1.10500) → bullish FVG of 50 pips
    Gap zone: 1.10000 to 1.10500
    """
    return spark.createDataFrame(
        [
            ("2024-01-01", "EUR", "USD", "1.09500", "1.10000", "1.09000", "1.09800", "0.01000", None, None),
            ("2024-01-02", "EUR", "USD", "1.09800", "1.11000", "1.09700", "1.10800", "0.01300", "1.02", None),
            ("2024-01-03", "EUR", "USD", "1.10800", "1.11500", "1.10500", "1.11200", "0.01000", "0.36", None),
        ],
        schema=StructType([
            StructField("date",              StringType(), True),
            StructField("from_symbol",       StringType(), True),
            StructField("to_symbol",         StringType(), True),
            StructField("open",              StringType(), True),
            StructField("high",              StringType(), True),
            StructField("low",               StringType(), True),
            StructField("close",             StringType(), True),
            StructField("daily_range",       StringType(), True),
            StructField("daily_return",      StringType(), True),
            StructField("_ingest_timestamp", StringType(), True),
        ])
    ).withColumn("date",         F.col("date").cast("date")) \
     .withColumn("open",         F.col("open").cast("decimal(10,5)")) \
     .withColumn("high",         F.col("high").cast("decimal(10,5)")) \
     .withColumn("low",          F.col("low").cast("decimal(10,5)")) \
     .withColumn("close",        F.col("close").cast("decimal(10,5)")) \
     .withColumn("daily_range",  F.col("daily_range").cast("decimal(10,5)")) \
     .withColumn("daily_return", F.col("daily_return").cast("decimal(10,5)")) \
     .withColumn("_ingest_timestamp", F.current_timestamp())


@pytest.fixture
def bearish_fvg_data(spark: SparkSession):
    """
    Three candles that form a clear bearish FVG.

    Candle 1 (prev): low  = 1.11000
    Candle 2 (mid):  the FVG candle — big bearish move
    Candle 3 (next): high = 1.10500

    prev_low (1.11000) > next_high (1.10500) → bearish FVG of 50 pips
    Gap zone: 1.10500 to 1.11000
    """
    return spark.createDataFrame(
        [
            ("2024-01-01", "EUR", "USD", "1.11500", "1.12000", "1.11000", "1.11200", "0.01000", None, None),
            ("2024-01-02", "EUR", "USD", "1.11200", "1.11300", "1.10200", "1.10400", "0.01100", "-0.72", None),
            ("2024-01-03", "EUR", "USD", "1.10400", "1.10500", "1.09800", "1.10000", "0.00700", "-0.36", None),
        ],
        schema=StructType([
            StructField("date",              StringType(), True),
            StructField("from_symbol",       StringType(), True),
            StructField("to_symbol",         StringType(), True),
            StructField("open",              StringType(), True),
            StructField("high",              StringType(), True),
            StructField("low",               StringType(), True),
            StructField("close",             StringType(), True),
            StructField("daily_range",       StringType(), True),
            StructField("daily_return",      StringType(), True),
            StructField("_ingest_timestamp", StringType(), True),
        ])
    ).withColumn("date",         F.col("date").cast("date")) \
     .withColumn("open",         F.col("open").cast("decimal(10,5)")) \
     .withColumn("high",         F.col("high").cast("decimal(10,5)")) \
     .withColumn("low",          F.col("low").cast("decimal(10,5)")) \
     .withColumn("close",        F.col("close").cast("decimal(10,5)")) \
     .withColumn("daily_range",  F.col("daily_range").cast("decimal(10,5)")) \
     .withColumn("daily_return", F.col("daily_return").cast("decimal(10,5)")) \
     .withColumn("_ingest_timestamp", F.current_timestamp())


@pytest.fixture
def no_fvg_data(spark: SparkSession):
    """
    Three candles with overlapping ranges — no FVG should be detected.
    Candles overlap so there is no gap between prev_high and next_low.
    """
    return spark.createDataFrame(
        [
            ("2024-01-01", "EUR", "USD", "1.10000", "1.10500", "1.09500", "1.10200", "0.01000", None, None),
            ("2024-01-02", "EUR", "USD", "1.10200", "1.10800", "1.09800", "1.10500", "0.01000", "0.29", None),
            ("2024-01-03", "EUR", "USD", "1.10500", "1.11000", "1.10100", "1.10700", "0.00900", "0.19", None),
        ],
        schema=StructType([
            StructField("date",              StringType(), True),
            StructField("from_symbol",       StringType(), True),
            StructField("to_symbol",         StringType(), True),
            StructField("open",              StringType(), True),
            StructField("high",              StringType(), True),
            StructField("low",               StringType(), True),
            StructField("close",             StringType(), True),
            StructField("daily_range",       StringType(), True),
            StructField("daily_return",      StringType(), True),
            StructField("_ingest_timestamp", StringType(), True),
        ])
    ).withColumn("date",         F.col("date").cast("date")) \
     .withColumn("open",         F.col("open").cast("decimal(10,5)")) \
     .withColumn("high",         F.col("high").cast("decimal(10,5)")) \
     .withColumn("low",          F.col("low").cast("decimal(10,5)")) \
     .withColumn("close",        F.col("close").cast("decimal(10,5)")) \
     .withColumn("daily_range",  F.col("daily_range").cast("decimal(10,5)")) \
     .withColumn("daily_return", F.col("daily_return").cast("decimal(10,5)")) \
     .withColumn("_ingest_timestamp", F.current_timestamp())


# ── Tests: _detect_fvg ────────────────────────────────────────────────────────

class TestDetectFvg:

    def test_detects_bullish_fvg(self, bullish_fvg_data):
        """A clear gap between prev_high and next_low must be detected as bullish."""
        result = _detect_fvg(bullish_fvg_data)
        bullish = result.filter(F.col("fvg_type") == "bullish")
        assert bullish.count() == 1

    def test_bullish_fvg_correct_levels(self, bullish_fvg_data):
        """
        Bullish FVG levels must be:
          fvg_low  = prev_high = 1.10000
          fvg_high = next_low  = 1.10500
        """
        result  = _detect_fvg(bullish_fvg_data)
        row     = result.filter(F.col("fvg_type") == "bullish").first()

        assert float(row["fvg_low"])  == pytest.approx(1.10000, abs=0.00001)
        assert float(row["fvg_high"]) == pytest.approx(1.10500, abs=0.00001)

    def test_bullish_fvg_size_in_pips(self, bullish_fvg_data):
        """Bullish FVG of 50 pips must be calculated correctly."""
        result = _detect_fvg(bullish_fvg_data)
        row    = result.filter(F.col("fvg_type") == "bullish").first()
        assert float(row["fvg_size_pips"]) == pytest.approx(50.0, abs=0.1)

    def test_detects_bearish_fvg(self, bearish_fvg_data):
        """A clear gap where prev_low > next_high must be detected as bearish."""
        result  = _detect_fvg(bearish_fvg_data)
        bearish = result.filter(F.col("fvg_type") == "bearish")
        assert bearish.count() == 1
        
    def test_bearish_fvg_correct_levels(self, bearish_fvg_data):
        """
            Bearish FVG levels must be:
            fvg_high = prev_low  = 1.11000
            fvg_low  = next_high = 1.10500
        """
        result = _detect_fvg(bearish_fvg_data)
        row    = result.filter(F.col("fvg_type") == "bearish").first()

        # Print actual values to understand what we get
        print(f"\nfvg_high actual: {float(row['fvg_high'])}")
        print(f"fvg_low  actual: {float(row['fvg_low'])}")

        assert float(row["fvg_high"]) == pytest.approx(1.10500, abs=0.00001)
        assert float(row["fvg_low"])  == pytest.approx(1.11000, abs=0.00001)

    def test_no_fvg_when_candles_overlap(self, no_fvg_data):
        """Overlapping candles must not produce any FVG."""
        result = _detect_fvg(no_fvg_data)
        assert result.count() == 0

    def test_fvg_date_is_middle_candle(self, bullish_fvg_data):
        """FVG date must be the middle candle — not prev or next."""
        result = _detect_fvg(bullish_fvg_data)
        row    = result.filter(F.col("fvg_type") == "bullish").first()
        assert str(row["date"]) == "2024-01-02"


# ── Tests: _mark_filled_gaps ──────────────────────────────────────────────────

class TestMarkFilledGaps:

    def test_unfilled_bullish_gap(self, bullish_fvg_data):
        """
        Bullish FVG where close stays above fvg_high must be marked unfilled.
        Close = 1.10800, fvg_high = 1.10500 → close > fvg_high → unfilled.
        """
        fvg_df = _detect_fvg(bullish_fvg_data)
        result = _mark_filled_gaps(fvg_df, bullish_fvg_data)
        row    = result.filter(F.col("fvg_type") == "bullish").first()
        assert row["is_filled"] == 0

    def test_unfilled_bearish_gap(self, bearish_fvg_data):
        """
        Bearish FVG where close stays below fvg_low must be marked unfilled.
        Close = 1.10400, fvg_low = 1.10500 → close < fvg_low → unfilled.
        """
        fvg_df = _detect_fvg(bearish_fvg_data)
        result = _mark_filled_gaps(fvg_df, bearish_fvg_data)
        row    = result.filter(F.col("fvg_type") == "bearish").first()
        assert row["is_filled"] == 0