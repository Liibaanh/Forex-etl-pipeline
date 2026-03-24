"""
Central schema registry.
All Bronze / Silver / Gold schemas defined here for EUR/USD OHLC data.
"""
from __future__ import annotations

from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Bronze ────────────────────────────────────────────────────────────────────
# Raw OHLC data exactly as received from Alpha Vantage API.
# All fields nullable — we never reject data at this layer.

BRONZE_OHLC_SCHEMA = StructType(
    [
        StructField("date",        StringType(), nullable=True),  # Raw string
        StructField("open",        StringType(), nullable=True),  # Raw string
        StructField("high",        StringType(), nullable=True),  # Raw string
        StructField("low",         StringType(), nullable=True),  # Raw string
        StructField("close",       StringType(), nullable=True),  # Raw string
        StructField("from_symbol", StringType(), nullable=True),  # e.g. EUR
        StructField("to_symbol",   StringType(), nullable=True),  # e.g. USD
        StructField("_ingest_timestamp", TimestampType(), nullable=False),
        StructField("_source",     StringType(), nullable=True),  # API source
    ]
)

# ── Silver ────────────────────────────────────────────────────────────────────
# Cleaned and typed OHLC data. Ready for analysis.

SILVER_OHLC_SCHEMA = StructType(
    [
        StructField("date",        StringType(),       nullable=False),
        StructField("from_symbol", StringType(),       nullable=False),
        StructField("to_symbol",   StringType(),       nullable=False),
        StructField("open",        DecimalType(10, 5), nullable=False),
        StructField("high",        DecimalType(10, 5), nullable=False),
        StructField("low",         DecimalType(10, 5), nullable=False),
        StructField("close",       DecimalType(10, 5), nullable=False),
        # Derived fields
        StructField("daily_range", DecimalType(10, 5), nullable=False),  # high - low
        StructField("daily_return",DecimalType(10, 5), nullable=True),   # % change close-to-close
        StructField("_ingest_timestamp", TimestampType(), nullable=False),
    ]
)

# ── Gold ──────────────────────────────────────────────────────────────────────
# Fair Value Gap (FVG) analysis — core ICT concept.
# A FVG exists when candle[i-1].high < candle[i+1].low (bullish)
# or candle[i-1].low > candle[i+1].high (bearish)

GOLD_FVG_SCHEMA = StructType(
    [
        StructField("date",         StringType(),       nullable=False),
        StructField("from_symbol",  StringType(),       nullable=False),
        StructField("to_symbol",    StringType(),       nullable=False),
        StructField("fvg_type",     StringType(),       nullable=False),  # bullish | bearish
        StructField("fvg_high",     DecimalType(10, 5), nullable=False),  # Top of gap
        StructField("fvg_low",      DecimalType(10, 5), nullable=False),  # Bottom of gap
        StructField("fvg_size",     DecimalType(10, 5), nullable=False),  # Gap size in pips
        StructField("is_filled",    IntegerType(),      nullable=False),  # 1 = price returned to gap
        StructField("_updated_at",  TimestampType(),    nullable=False),
    ]
)

# ── Allowed values ────────────────────────────────────────────────────────────

VALID_SYMBOLS = {"EUR", "USD", "GBP", "NOK", "JPY", "CHF"}